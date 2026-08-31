//! Memory ceiling for dense point-mode enumeration.
//!
//! The occurrence-centered grep must enumerate millions of collapsed
//! occurrences without materializing them: point mode is windowed (skip/take
//! over byte ranges), so a 32 MiB dense corpus paged to completion must keep
//! the process's high-water RSS within a fixed ceiling of the corpus size
//! while still returning every occurrence. Slow by design; `#[ignore]` keeps
//! it out of routine runs (`cargo test -- --ignored`).

use std::path::Path;

use chrono::{Duration, Utc};
use sheaf_core::config;
use sheaf_core::events::{Batch, EventKind, FsEvent};
use sheaf_core::store::{
    GrepMode, GrepQuery, GrepRequest, ProjectStore, SearchBudget, SelectionExtent, StoreLimits,
};

fn skeleton(root: &Path) {
    std::fs::create_dir_all(root.join(".sheaf/store")).unwrap();
    config::write_skeleton(root).unwrap();
}

fn open(root: &Path) -> ProjectStore {
    ProjectStore::open(
        root,
        StoreLimits {
            max_segment_bytes: 64 << 20,
            snapshot_edit_size: 1000,
        },
    )
    .unwrap()
}

fn vm_hwm_bytes() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    let line = status
        .lines()
        .find(|l| l.starts_with("VmHWM:"))
        .expect("VmHWM present on Linux");
    line.split_whitespace()
        .nth(1)
        .and_then(|kb| kb.parse::<u64>().ok())
        .map(|kb| kb * 1024)
        .unwrap_or(0)
}

/// Authoritative occurrence count: non-overlapping literal scan, the same
/// contract as the engine's enumeration.
fn authoritative_matches(text: &str, needle: &str) -> u64 {
    let mut count = 0u64;
    let mut cursor = 0usize;
    while let Some(offset) = text[cursor..].find(needle) {
        count += 1;
        cursor += offset + needle.len();
    }
    count
}

#[test]
#[ignore = "dense 32 MiB fixture; run explicitly with --ignored"]
fn dense_point_query_pages_within_the_rss_ceiling() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);

    // 32 MiB of 7-byte cells, one ONE-BYTE match per cell (~4.8M
    // occurrences), split across 32 files because files above
    // TEXT_MAX_BYTES (1 MiB) are content-addressed blobs, not searchable
    // text — and ingested in 4 batches so each capture's NEW-text admission
    // (TEXT_BATCH_MAX_BYTES, 8 MiB) keeps every file searchable text.
    const CELL: &str = "zxxxxx\n";
    let cells = (1 << 20) / CELL.len();
    let mut text = String::with_capacity(cells * CELL.len());
    for _ in 0..cells {
        text.push_str(CELL);
    }
    let expected = 32 * authoritative_matches(&text, "z");
    assert!(
        expected > 4_000_000,
        "fixture must be dense, got {expected}"
    );

    let dense_files: Vec<_> = (0..32)
        .map(|i| root.join(format!("dense{i:02}.txt")))
        .collect();
    {
        let mut store = open(root);
        let at = Utc::now() - Duration::hours(1);
        for chunk in dense_files.chunks(8) {
            for p in chunk {
                std::fs::write(p, &text).unwrap();
            }
            store
                .apply_batch(&Batch {
                    root: root.to_path_buf(),
                    started_at: at,
                    flushed_at: at,
                    events: chunk
                        .iter()
                        .map(|p| {
                            FsEvent::now(EventKind::Touched {
                                path: p.clone().into(),
                            })
                        })
                        .collect(),
                })
                .unwrap();
        }
    }

    // Baseline after opening the store but before any dense query: the
    // ceiling must catch occurrence-count-proportional allocation, which a
    // post-warm-up baseline would silently absorb into its floor.
    let store = open(root);

    let baseline_hwm = vm_hwm_bytes();

    // Page the dense query to completion.
    let mut cursor = None;
    let mut pages = 0u32;
    let mut collected = 0u64;
    let mut seen_cursors = std::collections::BTreeSet::new();
    loop {
        let req = request(1_000, cursor.clone());
        let page = store.grep(&req).unwrap();
        assert!(
            page.hits.len() <= 1_000,
            "a page must respect max_results, got {}",
            page.hits.len()
        );
        collected += page.hits.len() as u64;
        pages += 1;
        match page.cursor {
            Some(next) => {
                // Cursors must strictly advance (no page served twice).
                let token = format!(
                    "{}::{:?}::{}",
                    next.after_capture_id, next.resume_capture_id, next.record_index
                );
                assert!(
                    seen_cursors.insert(token),
                    "cursor repeated (page served twice)"
                );
                cursor = Some(next);
            }
            None => break,
        }
    }

    let peak_delta = vm_hwm_bytes().saturating_sub(baseline_hwm);
    assert!(pages > 1, "a dense query must page, got {pages} page(s)");
    assert_eq!(
        collected, expected,
        "paged enumeration must return every occurrence exactly once"
    );
    // Peak above the warm state must stay at corpus + page scale: 4x the
    // 32 MiB corpus is generous for page-sized transients while still
    // excluding anything proportional to the ~4.8M occurrence count.
    assert!(
        peak_delta <= (128 << 20),
        "peak RSS delta {peak_delta} bytes exceeded the 128 MiB ceiling for a 32 MiB corpus"
    );
}

fn request(max_results: usize, cursor: Option<sheaf_core::store::SearchCursor>) -> GrepRequest {
    GrepRequest {
        query: GrepQuery::literal("z"),
        mode: GrepMode::Point,
        at: None,
        anchor: None,
        from: None,
        to: None,
        path: None,
        follow: false,
        all: false,
        every_capture: false,
        extent: SelectionExtent::Match,
        budget: SearchBudget {
            max_results,
            ..SearchBudget::default()
        },
        cursor,
    }
}
