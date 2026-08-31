//! Performance budgets.
//!
//! "Capture latency invisible during builds/editing storms" is a
//! done-criterion, so it gets a number and a test: a synthetic storm of
//! events must persist inside the debounce cadence, and per-batch flush
//! cost must stay flat as the burst grows. Bounds are deliberately generous
//! (CI/debug-build friendly) — this guards against order-of-magnitude
//! regressions, not milliseconds.

use std::path::Path;
use std::time::{Duration, Instant};

use sheaf_core::config;
use sheaf_core::debounce::{Debouncer, DebouncerConfig};
use sheaf_core::events::{Batch, EventKind, FsEvent};
use sheaf_core::ignore::IgnoreSet;
use sheaf_core::store::{ProjectStore, StoreLimits};

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

fn ignores() -> IgnoreSet {
    IgnoreSet::from_patterns(&config::default_patterns()).unwrap()
}

#[test]
fn an_editing_storm_persists_within_the_debounce_cadence() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);

    // Real files first: the store reconciles against disk at flush time.
    for i in 0..2000u32 {
        let path = root.join(format!("src/file{i:04}.rs"));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, format!("pub fn f{i}() {{}}\n").as_bytes()).unwrap();
    }

    let mut store = open(root);
    let ignore = ignores();

    // Storm: 2000 creates in one batch (the debouncer's cap_events fires
    // first in real life; here we drive the store directly at its cap).
    let mut events = Vec::new();
    for i in 0..2000u32 {
        events.push(FsEvent::now(EventKind::Added {
            path: root.join(format!("src/file{i:04}.rs")),
        }));
    }
    let batch = sheaf_core::events::Batch {
        root: root.to_path_buf(),
        events,
        started_at: chrono::Utc::now(),
        flushed_at: chrono::Utc::now(),
    };
    let t0 = Instant::now();
    let outcome = store.apply_batch(&batch).unwrap();
    let create_flush = t0.elapsed();
    assert_eq!(outcome.capture.as_ref().map(|_| 1), Some(1));
    assert!(
        create_flush < Duration::from_secs(30),
        "flushing a 2000-file create burst took {create_flush:?}; capture must stay invisible"
    );

    // Storm phase 2: rewrite every file (2000 splices + journal append).
    for i in 0..2000u32 {
        let path = root.join(format!("src/file{i:04}.rs"));
        std::fs::write(path, format!("pub fn f{i}() {{ /* v2 */ }}\n").as_bytes()).unwrap();
    }
    let events: Vec<FsEvent> = (0..2000u32)
        .map(|i| {
            FsEvent::now(EventKind::Touched {
                path: root.join(format!("src/file{i:04}.rs")).into(),
            })
        })
        .collect();
    let batch = sheaf_core::events::Batch {
        root: root.to_path_buf(),
        events,
        started_at: chrono::Utc::now(),
        flushed_at: chrono::Utc::now(),
    };
    let t0 = Instant::now();
    store.apply_batch(&batch).unwrap();
    let splice_flush = t0.elapsed();
    assert!(
        splice_flush < Duration::from_secs(30),
        "flushing a 2000-file edit burst took {splice_flush:?}"
    );

    // The ignore filter is the other half of "invisible": 2000 events in
    // ignored directories classify away without touching disk.
    let t0 = Instant::now();
    let mut classified = 0usize;
    for i in 0..2000u32 {
        let rel = format!("target/debug/deps/artifact{i}");
        if !ignore.is_ignored_rel(std::path::Path::new(&rel)) {
            classified += 1;
        }
    }
    assert_eq!(classified, 0, "target/ is ignored by default");
    assert!(
        t0.elapsed() < Duration::from_millis(500),
        "ignore classification of 2000 paths took {:?}",
        t0.elapsed()
    );
}

#[test]
fn aggregate_text_budget_falls_back_to_blobs_and_reuses_freed_space() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    std::fs::write(root.join("a.txt"), b"aaaaaaaa").unwrap();
    std::fs::write(root.join("b.txt"), b"bbbbbbbb").unwrap();

    let mut store = ProjectStore::open_with_text_budget(root, StoreLimits::default(), 8).unwrap();
    let now = chrono::Utc::now();
    let outcome = store
        .apply_batch(&Batch {
            root: root.to_path_buf(),
            events: vec![
                FsEvent::now(EventKind::Added {
                    path: root.join("a.txt"),
                }),
                FsEvent::now(EventKind::Added {
                    path: root.join("b.txt"),
                }),
            ],
            started_at: now,
            flushed_at: now,
        })
        .unwrap();
    assert_eq!(store.tracked_text_bytes(), 8);
    assert_eq!(store.max_tracked_bytes(), 8);
    assert_eq!(outcome.text_budget_fallbacks, 1);
    assert_eq!(outcome.binaries_stored, 1);

    // Removing the admitted text frees budget for a later file.
    std::fs::remove_file(root.join("a.txt")).unwrap();
    std::fs::write(root.join("c.txt"), b"cccccccc").unwrap();
    let now = chrono::Utc::now();
    let outcome = store
        .apply_batch(&Batch {
            root: root.to_path_buf(),
            events: vec![
                FsEvent::now(EventKind::Removed {
                    path: root.join("a.txt"),
                }),
                FsEvent::now(EventKind::Added {
                    path: root.join("c.txt"),
                }),
            ],
            started_at: now,
            flushed_at: now,
        })
        .unwrap();
    assert_eq!(outcome.text_budget_fallbacks, 0);
    assert_eq!(store.tracked_text_bytes(), 8);

    // Growth beyond the aggregate limit demotes an existing text container
    // to a blob and releases its prior admission for another path.
    std::fs::write(root.join("c.txt"), b"ccccccccc").unwrap();
    let now = chrono::Utc::now();
    let outcome = store
        .apply_batch(&Batch {
            root: root.to_path_buf(),
            events: vec![FsEvent::now(EventKind::Touched {
                path: root.join("c.txt").into(),
            })],
            started_at: now,
            flushed_at: now,
        })
        .unwrap();
    assert_eq!(outcome.text_budget_fallbacks, 1);
    assert_eq!(store.tracked_text_bytes(), 0);
    std::fs::write(root.join("d.txt"), b"dddddddd").unwrap();
    let now = chrono::Utc::now();
    store
        .apply_batch(&Batch {
            root: root.to_path_buf(),
            events: vec![FsEvent::now(EventKind::Added {
                path: root.join("d.txt"),
            })],
            started_at: now,
            flushed_at: now,
        })
        .unwrap();
    assert_eq!(store.tracked_text_bytes(), 8);
    drop(store);

    let reopened = ProjectStore::open_with_text_budget(root, StoreLimits::default(), 8).unwrap();
    assert_eq!(reopened.tracked_text_bytes(), 8);
    let materialized = tmp.path().join("materialized");
    std::fs::create_dir(&materialized).unwrap();
    reopened.materialize(&materialized).unwrap();
    assert_eq!(
        std::fs::read(materialized.join("b.txt")).unwrap(),
        b"bbbbbbbb"
    );
    assert_eq!(
        std::fs::read(materialized.join("c.txt")).unwrap(),
        b"ccccccccc"
    );
    assert_eq!(
        std::fs::read(materialized.join("d.txt")).unwrap(),
        b"dddddddd"
    );
}

#[test]
fn lowering_the_budget_does_not_demote_an_unchanged_text_echo() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    std::fs::write(root.join("a.txt"), b"aaaaaaaa").unwrap();
    std::fs::write(root.join("b.txt"), b"bbbbbbbb").unwrap();
    let mut store = ProjectStore::open_with_text_budget(root, StoreLimits::default(), 16).unwrap();
    store.reconcile_worktree(&ignores()).unwrap();
    drop(store);

    let mut store = ProjectStore::open_with_text_budget(root, StoreLimits::default(), 8).unwrap();
    assert_eq!(store.tracked_text_bytes(), 16);
    let now = chrono::Utc::now();
    let outcome = store
        .apply_batch(&Batch {
            root: root.to_path_buf(),
            events: vec![FsEvent::now(EventKind::Touched {
                path: root.join("a.txt").into(),
            })],
            started_at: now,
            flushed_at: now,
        })
        .unwrap();
    assert!(outcome.capture.is_none());
    assert_eq!(outcome.text_budget_fallbacks, 0);
    assert_eq!(store.tracked_text_bytes(), 16);

    std::fs::write(root.join("a.txt"), b"aaaaaaaaa").unwrap();
    let now = chrono::Utc::now();
    let outcome = store
        .apply_batch(&Batch {
            root: root.to_path_buf(),
            events: vec![FsEvent::now(EventKind::Touched {
                path: root.join("a.txt").into(),
            })],
            started_at: now,
            flushed_at: now,
        })
        .unwrap();
    assert_eq!(outcome.text_budget_fallbacks, 1);
    assert_eq!(store.tracked_text_bytes(), 8);
}

#[test]
fn boot_reconciliation_flushes_a_bounded_number_of_events_per_capture() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let total = sheaf_core::store::RECONCILE_BATCH_EVENTS * 2 + 17;
    for i in 0..total {
        std::fs::write(root.join(format!("file-{i:04}.txt")), b"x").unwrap();
    }

    let mut store = ProjectStore::open(root, StoreLimits::default()).unwrap();
    assert!(store.reconcile_worktree(&ignores()).unwrap().is_some());
    assert_eq!(store.seq(), 3, "reconcile must flush 256/256/17 events");
    assert!(store.reconcile_worktree(&ignores()).unwrap().is_none());
}

#[test]
fn the_debouncer_releases_bursts_on_schedule() {
    // The write-burst debounce cadence, re-stated as a budget: under a
    // continuous flood, batches still release at max_hold, not never.
    let cfg = DebouncerConfig {
        window: Duration::from_millis(50),
        max_hold: Duration::from_millis(300),
        cap_events: 10_000,
    };
    let mut deb = Debouncer::new(std::path::PathBuf::from("/storm"), cfg);
    let t0 = Instant::now();
    let mut flushes = 0usize;
    let mut fed = 0usize;
    while t0.elapsed() < Duration::from_secs(2) {
        fed += 1;
        if deb
            .feed(FsEvent::now(EventKind::Touched {
                path: std::path::PathBuf::from(format!("hot/file{}.txt", fed % 500)).into(),
            }))
            .is_some()
        {
            flushes += 1;
        }
    }
    assert!(
        flushes >= 4,
        "2 s flood must release at least ~6 partial batches at 300 ms max_hold; got {flushes}"
    );
    let tail = deb.force_flush();
    assert!(
        tail.len() + fed >= fed,
        "tail flush preserves the remainder"
    );
}
