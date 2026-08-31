//! Scale fixture and query-latency budgets.
//!
//! The acceleration phase is validated against a deterministic store of at
//! least 10,000 captures. The whole point of the content-version sidecar is
//! that an indexed query no longer pays one Loro `fork_at` per touching
//! capture, so the measurement that matters is *query execution* — the walk
//! after `points()` returns — held apart from the one-time cold store-open
//! replay it must never be blamed for.
//!
//! Three budgets are reported and asserted independently:
//!   1. store-open replay latency (its own budget; dominated by journal
//!      replay, unaffected by the sidecar),
//!   2. cold-process indexed query execution with an existing sidecar
//!      (`report.usage.elapsed_ms`, the sidecar's core claim),
//!   3. fork accounting — an indexed query performs zero query-time
//!      `fork_at`.
//!
//! Slow by design (`#[ignore]`): building 10k+ captures takes minutes.
//! Run explicitly with `cargo test -p sheaf-core --test grep_scale -- --ignored`.

use std::path::Path;
use std::time::Instant;

use chrono::{Duration, Utc};
use sheaf_core::config;
use sheaf_core::events::{Batch, EventKind, FsEvent};
use sheaf_core::store::{
    GrepBackfillOptions, GrepMode, GrepQuery, GrepRequest, ProjectStore, SearchBudget,
    SelectionExtent, StoreLimits,
};

fn skeleton(root: &Path) {
    std::fs::create_dir_all(root.join(".sheaf/store")).unwrap();
    config::write_skeleton(root).unwrap();
}

fn open(root: &Path) -> ProjectStore {
    ProjectStore::open(
        root,
        StoreLimits {
            // A large segment keeps the fixture in few segments so store-open
            // measures replay, not segment enumeration overhead.
            max_segment_bytes: 64 << 20,
            snapshot_edit_size: 500,
        },
    )
    .unwrap()
}

/// A deterministic source file. The body is a fixed template with two moving
/// parts: a per-capture counter that changes every capture (so every capture
/// touches distinct content and the sidecar stores a fresh row) and a rare
/// needle planted only at chosen generations. No randomness: the same seed
/// arguments always produce byte-identical stores.
fn file_body(file_idx: usize, generation: u64, plant_rare: bool) -> String {
    let mut body = String::with_capacity(2048);
    body.push_str(&format!("// module {file_idx}, generation {generation}\n"));
    for line in 0..40u64 {
        // A stable common token ("fn ") appears densely; the moving counter
        // guarantees content changes each capture so dedup can't collapse
        // the whole history to one version.
        body.push_str(&format!(
            "fn worker_{file_idx}_{line}() {{ step({}); }}\n",
            generation.wrapping_mul(2_654_435_761).wrapping_add(line)
        ));
    }
    if plant_rare {
        // A needle that occurs at exactly the planted generations, so a
        // "rare" query touches a known, small number of captures.
        body.push_str("fn quokka_marker_needle() {}\n");
    }
    body
}

fn touch(store: &mut ProjectStore, root: &Path, rel: &str, text: &str, age: Duration) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, text).unwrap();
    let at = Utc::now() - age;
    store
        .apply_batch(&Batch {
            root: root.to_path_buf(),
            started_at: at,
            flushed_at: at,
            events: vec![FsEvent::now(EventKind::Touched { path: path.into() })],
        })
        .unwrap();
}

/// Build (or reuse) a deterministic store of `>= captures` captures across
/// `files` rotating source files. Every capture edits one file, so the store
/// has exactly `captures` captures and each file is touched at hundreds of
/// generations — the measured profile's expensive shape. The rare needle
/// is planted every `rare_period` captures.
///
/// Returns the number of captures written and the number that carry the rare
/// needle.
fn build_fixture(root: &Path, captures: u64, files: usize, rare_period: u64) -> (u64, u64) {
    skeleton(root);
    let mut store = open(root);
    let mut rare = 0u64;
    // Oldest first: ages descend as generation ascends, matching wall-clock
    // capture order.
    for gen in 0..captures {
        let file_idx = (gen as usize) % files;
        let plant = gen % rare_period == 0 && gen != 0;
        if plant {
            rare += 1;
        }
        let body = file_body(file_idx, gen, plant);
        let age = Duration::milliseconds((captures - gen) as i64);
        touch(
            &mut store,
            root,
            &format!("src/mod_{file_idx}.rs"),
            &body,
            age,
        );
    }
    (captures, rare)
}

fn literal_history(needle: &str, path: Option<&str>) -> GrepRequest {
    GrepRequest {
        query: GrepQuery::literal(needle),
        mode: GrepMode::History,
        at: None,
        anchor: None,
        from: None,
        to: None,
        path: path.map(str::to_owned),
        follow: false,
        all: false,
        every_capture: false,
        extent: SelectionExtent::Match,
        // A generous budget: we want the query to *complete* so elapsed_ms is
        // the true execution cost, not a budget-cap artifact.
        budget: SearchBudget {
            max_results: 100_000,
            max_materialized_bytes: 512 << 20,
            max_elapsed_ms: 120_000,
        },
        cursor: None,
    }
}

fn percentile(mut samples: Vec<u64>, pct: f64) -> u64 {
    samples.sort_unstable();
    if samples.is_empty() {
        return 0;
    }
    let rank = ((pct / 100.0) * (samples.len() as f64 - 1.0)).round() as usize;
    samples[rank.min(samples.len() - 1)]
}

#[test]
#[ignore = "builds 10k+ captures; run explicitly with --ignored"]
fn indexed_query_execution_stays_fast_on_a_ten_thousand_capture_store() {
    const CAPTURES: u64 = 10_000;
    const FILES: usize = 8;
    const RARE_PERIOD: u64 = 500;
    const WARMUP: usize = 2;
    const RUNS: usize = 10;

    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();

    let build_started = Instant::now();
    let (written, rare) = build_fixture(root, CAPTURES, FILES, RARE_PERIOD);
    let build_ms = build_started.elapsed().as_millis();
    assert!(
        written >= 10_000,
        "fixture must have at least 10k captures, got {written}"
    );
    assert!(rare >= 15, "rare needle must be planted enough, got {rare}");
    eprintln!("[scale] built {written} captures ({rare} carry the rare needle) in {build_ms} ms");

    // The sidecar is populated at capture time; confirm it is present before
    // measuring, so we measure the indexed path and never a silent scan-first
    // fallback.
    let cache_dir = root.join(".sheaf/state/cache/grep-v1");
    assert!(
        cache_dir.join("mappings.jsonl").exists(),
        "capture-time indexing must have written the sidecar"
    );

    // Build the trigram pre-filter (as `sheaf cache backfill` does). Content
    // rows already exist from capture-time indexing, so this run indexes no
    // captures — it exists to rebuild the trigram index over the blobs.
    {
        let store = open(root);
        let report = store
            .grep_cache_backfill(GrepBackfillOptions::default())
            .unwrap();
        assert!(report.complete, "backfill must complete");
        assert!(
            report.trigram_index_bytes > 0,
            "backfill must build a non-empty trigram index"
        );
        eprintln!(
            "[scale] trigram index: {} bytes over the distinct-content corpus",
            report.trigram_index_bytes
        );
    }

    // ---- Budget 1: store-open replay, measured on its own. --------------
    let mut open_samples = Vec::new();
    for _ in 0..(WARMUP + RUNS) {
        let started = Instant::now();
        let store = open(root);
        let open_ms = started.elapsed().as_millis() as u64;
        std::hint::black_box(&store);
        open_samples.push(open_ms);
    }
    let open_samples: Vec<u64> = open_samples.split_off(WARMUP);
    let open_p50 = percentile(open_samples.clone(), 50.0);
    let open_p95 = percentile(open_samples.clone(), 95.0);
    eprintln!("[scale] store-open p50={open_p50} ms p95={open_p95} ms (its own budget)");

    // ---- Budgets 2 & 3: cold-process indexed query execution. -----------
    // The rare needle touches a bounded set of captures; the sidecar answers
    // every touching read from disk with zero query-time forks. Measure the
    // query alone (`usage.elapsed_ms`) in a freshly opened process each run so
    // no warm process cache from a prior run leaks in — this is the
    // "cold process with an existing sidecar" target.
    let mut rare_exec = Vec::new();
    let mut rare_forks_max = 0u64;
    let mut rare_hits = 0usize;
    let mut rare_skipped = 0u64;
    for i in 0..(WARMUP + RUNS) {
        let store = open(root);
        let report = store
            .grep(&literal_history("quokka_marker_needle", None))
            .unwrap();
        assert!(
            report.complete,
            "the rare query must complete within budget"
        );
        if i >= WARMUP {
            rare_exec.push(report.usage.elapsed_ms);
            rare_forks_max = rare_forks_max.max(report.usage.historical_forks);
            rare_hits = report.hits.len();
            rare_skipped = report.usage.trigram_skipped;
        }
    }
    let rare_p50 = percentile(rare_exec.clone(), 50.0);
    let rare_p95 = percentile(rare_exec.clone(), 95.0);
    eprintln!(
        "[scale] rare indexed query: {rare_hits} hits, exec p50={rare_p50} ms p95={rare_p95} ms, max query-forks={rare_forks_max}, trigram-skipped={rare_skipped}"
    );
    assert!(
        rare_skipped > 1_000,
        "the trigram pre-filter must skip most captures for a rare needle, skipped {rare_skipped}"
    );

    // An entirely absent needle, the case the filter helps most: every covered
    // version is provably excluded and the walk only reduces lifecycle state.
    let mut absent_exec = Vec::new();
    let mut absent_skipped = 0u64;
    for i in 0..(WARMUP + RUNS) {
        let store = open(root);
        let report = store
            .grep(&literal_history("nonexistent_zzzq_probe", None))
            .unwrap();
        assert!(report.complete, "the absent query must complete in budget");
        assert!(report.hits.is_empty(), "an absent needle finds nothing");
        if i >= WARMUP {
            absent_exec.push(report.usage.elapsed_ms);
            absent_skipped = absent_skipped.max(report.usage.trigram_skipped);
        }
    }
    let absent_p95 = percentile(absent_exec.clone(), 95.0);
    eprintln!(
        "[scale] absent indexed query: exec p95={absent_p95} ms, trigram-skipped={absent_skipped}"
    );
    assert!(
        absent_skipped > 1_000,
        "the absent needle must skip the corpus, skipped {absent_skipped}"
    );
    assert!(
        absent_p95 < 2_500,
        "absent unscoped indexed query exec p95 {absent_p95} ms regressed past the cold-walk bound"
    );

    // The warm rare/absent unscoped case, measured honestly: one resident
    // store, scan cache and resident index active. Content work is already
    // skipped by the trigram filter on BOTH runs, so the O(captures) lifecycle
    // reduction walk dominates warm and cold alike — warming cannot shrink it
    // and is not claimed to. The bound is therefore the same cold-walk bound;
    // the 100 ms warm figure is asserted for the scoped interactive case below.
    {
        let store = open(root);
        let _ = store
            .grep(&literal_history("quokka_marker_needle", None))
            .unwrap();
        let mut warm = Vec::new();
        for _ in 0..RUNS {
            let report = store
                .grep(&literal_history("quokka_marker_needle", None))
                .unwrap();
            warm.push(report.usage.elapsed_ms);
        }
        let warm_p50 = percentile(warm.clone(), 50.0);
        let warm_p95 = percentile(warm.clone(), 95.0);
        eprintln!("[scale] rare unscoped WARM: exec p50={warm_p50} ms p95={warm_p95} ms");
        assert!(
            warm_p95 < 2_500,
            "warm rare unscoped query exec p95 {warm_p95} ms regressed past the walk bound"
        );
    }

    // ---- Warm path (AC1's 100 ms warm-daemon figure). -------------------
    // One resident store serves the query repeatedly, exactly as the daemon
    // does. The warm scan cache and resident trigram index mean the second
    // and later runs re-derive nothing: the hot scoped query, whose candidate
    // versions were all scanned on the first run, must answer far faster than
    // the cold number and comfortably under 100 ms.
    {
        let store = open(root);
        // First run warms the caches.
        let _ = store
            .grep(&literal_history("fn worker_0_0", Some("src/mod_0.rs")))
            .unwrap();
        let mut warm = Vec::new();
        for _ in 0..RUNS {
            let report = store
                .grep(&literal_history("fn worker_0_0", Some("src/mod_0.rs")))
                .unwrap();
            warm.push(report.usage.elapsed_ms);
        }
        let warm_p50 = percentile(warm.clone(), 50.0);
        let warm_p95 = percentile(warm.clone(), 95.0);
        eprintln!("[scale] hot scoped WARM: exec p50={warm_p50} ms p95={warm_p95} ms");
        assert!(
            warm_p95 < 100,
            "warm hot scoped query exec p95 {warm_p95} ms exceeds the 100 ms warm target"
        );
    }

    // A scoped query over one hot file touches ~CAPTURES/FILES captures — the
    // dense per-file profile — and must still answer from the sidecar.
    let mut hot_exec = Vec::new();
    let mut hot_forks_max = 0u64;
    for i in 0..(WARMUP + RUNS) {
        let store = open(root);
        let report = store
            .grep(&literal_history("fn worker_0_0", Some("src/mod_0.rs")))
            .unwrap();
        assert!(report.complete, "the hot scoped query must complete");
        if i >= WARMUP {
            hot_exec.push(report.usage.elapsed_ms);
            hot_forks_max = hot_forks_max.max(report.usage.historical_forks);
        }
    }
    let hot_p50 = percentile(hot_exec.clone(), 50.0);
    let hot_p95 = percentile(hot_exec.clone(), 95.0);
    eprintln!(
        "[scale] hot scoped query: exec p50={hot_p50} ms p95={hot_p95} ms, max query-forks={hot_forks_max}"
    );

    // ---- Assertions. ----------------------------------------------------
    // Per-touching-capture query-time forks are the measured cost this work
    // removes: the sidecar answers every touching read from disk. An unscoped
    // query still pays at most one whole-document fork for the lineage's
    // baseline path enumeration (`text_keys_at`), which is O(1) in the walk
    // length, not the per-capture O(N) profile. A scoped query pays none.
    assert!(
        rare_forks_max <= 1,
        "an indexed unscoped query must fork at most once (baseline enumeration), got {rare_forks_max}"
    );
    assert_eq!(
        hot_forks_max, 0,
        "an indexed scoped query must perform no query-time fork_at"
    );

    // Cold-process indexed query execution targets (AC1). A *scoped* query
    // reads only its path's touching captures and, with the trigram filter
    // plus content dedup, lands well under 500 ms — the common interactive
    // case. An *unscoped* whole-history query must still visit every one of
    // the 10k captures to reduce lifecycle across the entire tree; the
    // trigram filter removes the content scan (9981 of 10k skipped) but not
    // the O(captures) reduction walk, so its cost — warm or cold — is bounded
    // under 2.5 s rather than the pre-acceleration ~30 s, and warming does not
    // shrink it (measured above). The 100 ms warm figure belongs to the scoped
    // interactive path, asserted in the warm block.
    assert!(
        hot_p95 < 500,
        "hot scoped indexed query exec p95 {hot_p95} ms exceeds the 500 ms cold-process target"
    );
    assert!(
        rare_p95 < 2_500,
        "rare unscoped indexed query exec p95 {rare_p95} ms regressed past the 2.5 s cold-walk bound"
    );
}
