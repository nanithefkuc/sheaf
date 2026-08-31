//! Phase-by-phase timing of the doctor sweep against a project root —
//! the performance scalpel for "why is `sheaf doctor` slow".
//!
//! Usage: cargo run -p sheaf-core --release --example doctor_bench -- <project-root>

use std::path::PathBuf;
use std::time::Instant;

fn ms(t: std::time::Duration) -> f64 {
    t.as_secs_f64() * 1000.0
}

fn main() -> anyhow::Result<()> {
    let root = PathBuf::from(std::env::args().nth(1).expect("project root"));

    // Phase 1: raw journal read + CRC scan approximation (page-cached read
    // + crc32c over the bytes, mirroring scan_intact_prefix's cost).
    let sdir = root.join(".sheaf/store");
    let t = Instant::now();
    let mut crc_bytes = 0u64;
    for (_idx, path) in sheaf_core::store::list_segments(&sdir) {
        let bytes = std::fs::read(&path)?;
        crc_bytes += bytes.len() as u64;
        let _ = crc32c::crc32c(&bytes);
    }
    println!(
        "journal crc scan ({crc_bytes} B): {:.1} ms",
        ms(t.elapsed())
    );

    // Phase 2: TimelineReader::open (snapshot import + journal replay).
    let t = Instant::now();
    let reader = sheaf_core::store::TimelineReader::open(&root)?;
    println!("reader open (replay):            {:.1} ms", ms(t.elapsed()));

    // Phase 3: branch tips (frontier iteration + capture_id_at per tip).
    let t = Instant::now();
    let tips = reader.branch_tips()?;
    println!(
        "branch tips ({}):            {:.1} ms",
        tips.len(),
        ms(t.elapsed())
    );

    // Phase 4: reachable blob digests — tree_events deep-value iteration.
    let t = Instant::now();
    let mut events = 0usize;
    let mut digests = std::collections::BTreeSet::new();
    reader.doc().get_list("tree_events").for_each(|value| {
        events += 1;
        if let Ok(raw) = value.get_deep_value().into_string() {
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&raw) {
                if let Some(d) = parsed["event"]["binary"].as_str() {
                    digests.insert(d.to_owned());
                }
            }
        }
    });
    reader.doc().get_map("binaries").for_each(|_, value| {
        if let Ok(raw) = value.get_deep_value().into_string() {
            if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&raw) {
                if let Some(d) = parsed["hash"].as_str() {
                    digests.insert(d.to_owned());
                }
            }
        }
    });
    println!(
        "tree_events iter ({events} events, {} binary digests): {:.1} ms",
        digests.len(),
        ms(t.elapsed())
    );

    // Phase 5: full captures walk with all branches (report's final count
    // path: captures_from + mark_current_lineage + annotate_checkpoints).
    let t = Instant::now();
    let all = reader.captures(true, None, false, usize::MAX)?;
    println!(
        "captures(all branches) -> {}:    {:.1} ms",
        all.len(),
        ms(t.elapsed())
    );

    // Phase 6: current-lineage walk (the ledger_state check's shape).
    let t = Instant::now();
    let cur = reader.captures(false, None, false, usize::MAX)?;
    println!(
        "captures(lineage) -> {}:         {:.1} ms",
        cur.len(),
        ms(t.elapsed())
    );

    // Phase 7: grep-cache facts approximation.
    let t = Instant::now();
    let cache_dir = root.join(".sheaf/state/cache/grep-v1");
    let mut rows = 0usize;
    if let Ok(bytes) = std::fs::read(cache_dir.join("mappings.jsonl")) {
        for line in bytes.split(|b| *b == b'\n') {
            if line.is_empty() {
                continue;
            }
            if serde_json::from_slice::<serde_json::Value>(line).is_ok() {
                rows += 1;
            }
        }
    }
    let mut content_files = 0usize;
    if let Ok(rd) = std::fs::read_dir(cache_dir.join("content")) {
        for f in rd.flatten() {
            let _ = f.metadata();
            content_files += 1;
        }
    }
    let _ = std::fs::read(cache_dir.join("index.bin"));
    println!(
        "grep cache facts ({rows} rows, {content_files} content): {:.1} ms",
        ms(t.elapsed())
    );

    // Phase 8: replay cost breakdown — per-record sequential import (what
    // TimelineReader::open does today) vs one import_batch call, over the
    // identical classified payloads.
    let paths = sheaf_core::store::list_segments(&sdir);
    let mut updates: Vec<Vec<u8>> = Vec::new();
    let mut records = 0usize;
    let mut update_bytes = 0usize;
    sheaf_core::store::read_records(&paths)
        .into_iter()
        .for_each(|r| {
            if let Ok(rec) = r {
                match sheaf_core::store::classify_payload(&rec.payload) {
                    Some(sheaf_core::store::Frame::Update(delta)) => {
                        update_bytes += delta.len();
                        updates.push(delta);
                    }
                    Some(sheaf_core::store::Frame::Record(_)) => records += 1,
                    None => {}
                }
            }
        });
    println!(
        "frames: {} updates ({:.2} MiB), {} ledger records",
        updates.len(),
        update_bytes as f64 / 1048576.0,
        records
    );

    let t = Instant::now();
    let doc = loro::LoroDoc::new();
    for delta in &updates {
        doc.import(delta)?;
    }
    let seq_import = ms(t.elapsed());
    println!(
        "sequential imports ({}):   {:.1} ms ({:.0} µs/frame)",
        updates.len(),
        seq_import,
        seq_import * 1000.0 / updates.len() as f64
    );

    let t = Instant::now();
    let doc2 = loro::LoroDoc::new();
    doc2.import_batch(&updates)?;
    let batch_import = ms(t.elapsed());
    println!(
        "import_batch (one call):    {:.1} ms ({:.0}x faster)",
        batch_import,
        seq_import / batch_import.max(0.001)
    );

    // Chunked batch: same payloads in bounded chunks (frame count cap),
    // mirroring a memory-bounded streaming loader.
    for chunk_frames in [32usize, 128, 512] {
        let t = Instant::now();
        let doc3 = loro::LoroDoc::new();
        for chunk in updates.chunks(chunk_frames) {
            doc3.import_batch(chunk)?;
        }
        let chunked = ms(t.elapsed());
        println!(
            "import_batch (chunks of {chunk_frames:>3}): {:.1} ms ({:.0}x faster)",
            chunked,
            seq_import / chunked.max(0.001)
        );
    }

    Ok(())
}
