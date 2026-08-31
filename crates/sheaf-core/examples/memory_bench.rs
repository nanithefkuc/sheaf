//! Stage-by-stage resident-memory attribution for the daemon's state —
//! the scalpel for "why does sheafd use 600 MB".
//!
//! Usage: cargo run -p sheaf-core --release --example memory_bench -- <project-root>

use std::path::PathBuf;
use std::time::Instant;

fn proc_status() -> (u64, u64) {
    // (VmRSS, VmHWM) in KiB
    let raw = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let mut rss = 0u64;
    let mut hwm = 0u64;
    for line in raw.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            rss = rest.trim_end_matches("kB").trim().parse().unwrap_or(0);
        }
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            hwm = rest.trim_end_matches("kB").trim().parse().unwrap_or(0);
        }
    }
    (rss, hwm)
}

fn mib(kib: u64) -> f64 {
    kib as f64 / 1024.0
}

fn stage(label: &str, start: &mut u64) {
    let (rss, hwm) = proc_status();
    println!(
        "{label:<42} rss {:>7.1} MiB (Δ{:+7.1})  peak {:>7.1} MiB",
        mib(rss),
        mib(rss.saturating_sub(*start)),
        mib(hwm)
    );
    *start = rss;
}

extern "C" {
    fn malloc_trim(pad: usize) -> std::os::raw::c_int;
}

fn trim(label: &str, start: &mut u64) {
    unsafe { malloc_trim(0) };
    stage(label, start);
}

fn req(needle: &str) -> sheaf_core::store::GrepRequest {
    sheaf_core::store::GrepRequest {
        query: sheaf_core::store::GrepQuery::literal(needle),
        mode: sheaf_core::store::GrepMode::History,
        at: None,
        anchor: None,
        from: None,
        to: None,
        path: None,
        follow: false,
        all: false,
        every_capture: false,
        extent: sheaf_core::store::SelectionExtent::Match,
        budget: Default::default(),
        cursor: None,
    }
}

fn main() -> anyhow::Result<()> {
    let root = PathBuf::from(std::env::args().nth(1).expect("project root"));

    let mut mark = proc_status().0;
    stage("baseline", &mut mark);

    // Stage 1: the daemon's resident writer state — full Loro doc (oplog
    // + doc state) via recovery, plus the writable grep sidecar handles.
    let t = Instant::now();
    let store = sheaf_core::store::ProjectStore::open(&root, Default::default())?;
    println!("  (open took {:?})", t.elapsed());
    stage("ProjectStore::open (doc + ledger)", &mut mark);

    // Stage 2: a broad history grep — fills the warm caches exactly the
    // way the daemon's writer thread does across queries.
    let t = Instant::now();
    let report = store.grep(&req("fn "))?;
    println!(
        "  (grep 'fn ' took {:?}: {} hits, stop {:?})",
        t.elapsed(),
        report.hits.len(),
        report.stop_reason
    );
    stage("grep 'fn ' (warm caches fill)", &mut mark);

    // Stage 3: a second distinct needle — more scan outcomes.
    let t = Instant::now();
    let report = store.grep(&req("let "))?;
    println!(
        "  (grep 'let ' took {:?}: {} hits, stop {:?})",
        t.elapsed(),
        report.hits.len(),
        report.stop_reason
    );
    stage("grep 'let ' (more scan outcomes)", &mut mark);

    // Stage 4: the doctor transient — a fresh reader (full second doc
    // import), then dropped. Watch peak vs post-drop: the gap is
    // allocator retention.
    {
        let t = Instant::now();
        let reader = sheaf_core::store::TimelineReader::open(&root)?;
        let n = reader.captures(true, None, false, usize::MAX)?.len();
        println!("  (reader open + captures({n}) took {:?})", t.elapsed());
        stage("TimelineReader::open (transient doc)", &mut mark);
    }
    stage("reader dropped", &mut mark);
    trim("after malloc_trim", &mut mark);

    // Stage 5: a grep after the trim — steady state with everything warm.
    let _ = store.grep(&req("use "))?;
    stage("grep 'use ' (steady state)", &mut mark);

    // Stage 6: captures walk on the live store (log path).
    let _ = store.captures(true, None, false, usize::MAX)?;
    stage("captures(all) on live store", &mut mark);
    trim("final malloc_trim", &mut mark);

    Ok(())
}
