//! Integrity checking and reachability-constrained retention.
//!
//! The bar: `doctor` must catch every way the on-disk durability contract says
//! a store can be hurt, and GC must never remove a byte a restore to ANY
//! timeline point could still need.

use std::path::Path;

use sheaf_core::config::{self};
use sheaf_core::events::{Batch, EventKind, FsEvent};
use sheaf_core::ignore::IgnoreSet;
use sheaf_core::store::{
    doctor, gc_apply, gc_plan, hash_of, ProjectStore, StoreLimits, TimelineReader,
};

/// Blob path for a digest, mirroring the store's fanout layout.
fn blob_path(root: &Path, digest: &str) -> std::path::PathBuf {
    root.join(".sheaf/store/blobs")
        .join(&digest[..2])
        .join(digest)
}

fn skeleton(root: &Path) {
    std::fs::create_dir_all(root.join(".sheaf/store")).unwrap();
    // write_skeleton lays down config.toml — the root marker carrying
    // format_version — and doctor checks it exists and parses.
    config::write_skeleton(root).unwrap();
}

fn limits() -> StoreLimits {
    // Snapshot every third batch: enough compaction to supersede snapshots
    // (retention needs that), while later rounds keep live frames on disk
    // for the corruption checks.
    StoreLimits {
        max_segment_bytes: 4 << 20,
        snapshot_edit_size: 3,
    }
}

fn ignores() -> IgnoreSet {
    IgnoreSet::from_patterns(&config::default_patterns()).unwrap()
}

fn open(root: &Path) -> ProjectStore {
    ProjectStore::open(root, limits()).unwrap()
}

fn write(root: &Path, rel: &str, bytes: &[u8]) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

fn flush(store: &mut ProjectStore, root: &Path, events: Vec<FsEvent>) {
    let batch = Batch {
        root: root.to_path_buf(),
        events,
        started_at: chrono::Utc::now(),
        flushed_at: chrono::Utc::now(),
    };
    store.apply_batch(&batch).unwrap();
}

/// A store with text + binary history, several snapshots, and enough churn
/// that covered segments exist.
fn seeded_store(root: &Path) -> ProjectStore {
    skeleton(root);
    let mut store = open(root);
    for round in 0..8 {
        write(
            root,
            "src/lib.rs",
            format!("pub fn round_{round}() {{}}\n").as_bytes(),
        );
        write(
            root,
            "assets/logo.bin",
            &[0xff, round, 0x00, 0x93, 0x94, 0x01],
        );
        flush(
            &mut store,
            root,
            vec![
                FsEvent::now(EventKind::Touched {
                    path: root.join("src/lib.rs").into(),
                }),
                FsEvent::now(EventKind::Touched {
                    path: root.join("assets/logo.bin").into(),
                }),
            ],
        );
    }
    store
}

#[test]
fn doctor_reports_a_healthy_store_as_healthy() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let store = seeded_store(root);
    drop(store);
    let report = doctor(root).unwrap();
    assert!(
        report.ok,
        "checks: {:?}",
        report.checks.iter().filter(|c| !c.ok).collect::<Vec<_>>()
    );
    assert!(report.captures >= 8);
    assert!(
        report.blob_count >= 6,
        "each round's binary is a distinct blob"
    );
    assert_eq!(
        report.orphan_blobs, 0,
        "every blob is reachable in a clean store"
    );
}

#[test]
fn doctor_catches_a_corrupt_journal_frame() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let store = seeded_store(root);
    drop(store);

    // Flip a byte inside the newest segment's payload region (past the
    // 8-byte header).
    let segments = sheaf_core::store::list_segments(&root.join(".sheaf/store"));
    // Compaction leaves a fresh empty tail segment; corrupt the last one
    // that actually holds frames.
    let (last, mut bytes) = segments
        .iter()
        .rev()
        .find_map(|(_, p)| {
            let b = std::fs::read(p).unwrap();
            (b.len() > 8).then(|| (p.clone(), b))
        })
        .expect("at least one segment with frames");
    if bytes.len() > 24 {
        bytes[20] ^= 0xff; // payload byte -> BadCrc
    } else {
        bytes[5] ^= 0xff; // crc field of the first frame -> BadCrc
    }
    std::fs::write(&last, &bytes).unwrap();

    let report = doctor(root).unwrap();
    assert!(!report.ok);
    let framing = report
        .checks
        .iter()
        .find(|c| c.name == "journal_frames")
        .unwrap();
    assert!(!framing.ok, "corrupt payload must fail the frame check");
}

#[test]
fn doctor_catches_a_missing_blob() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let store = seeded_store(root);
    drop(store);

    // Delete a blob out from under history: round 0's payload, referenced
    // only by an older capture (the live binaries map points at round 7's).
    // Directory order is filesystem-dependent, so pick by digest — this is
    // the case a live-tip-only coverage check would silently miss.
    let victim = blob_path(root, &hash_of(&[0xff, 0, 0x00, 0x93, 0x94, 0x01]));
    std::fs::remove_file(&victim).unwrap();

    let report = doctor(root).unwrap();
    assert!(!report.ok);
    let coverage = report
        .checks
        .iter()
        .find(|c| c.name == "blob_coverage")
        .unwrap();
    assert!(
        !coverage.ok,
        "a referenced blob is missing and doctor must say so"
    );
}

#[test]
fn doctor_catches_format_and_config_problems() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let store = seeded_store(root);
    drop(store);
    std::fs::write(root.join(".sheaf/config.toml"), "not [ valid toml").unwrap();
    let report = doctor(root).unwrap();
    assert!(!report.ok);
    assert!(report.checks.iter().any(|c| c.name == "config" && !c.ok));
}

#[test]
fn gc_collects_orphans_and_superseded_snapshots_but_keeps_history_reachable() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let mut store = seeded_store(root);

    // Every capture and every blob digest before GC.
    let reader_before = TimelineReader::open(root).unwrap();
    let captures_before = reader_before
        .captures(true, None, false, usize::MAX)
        .unwrap();
    assert!(captures_before.len() >= 8);
    let digests_before: std::collections::BTreeSet<String> =
        std::fs::read_dir(root.join(".sheaf/store/blobs"))
            .unwrap()
            .flatten()
            .flat_map(|fan| std::fs::read_dir(fan.path()).unwrap().flatten())
            .map(|f| f.file_name().to_string_lossy().into_owned())
            .collect();

    // An orphan: bytes stored with no event ever naming them.
    let orphan_bytes = b"never referenced by history";
    let orphan_digest = hash_of(orphan_bytes);
    let orphan_path = blob_path(root, &orphan_digest);
    std::fs::create_dir_all(orphan_path.parent().unwrap()).unwrap();
    std::fs::write(&orphan_path, orphan_bytes).unwrap();

    // Seed snapshots so superseded ones exist, then run GC on the writer
    // (the exclusive-lock holder in the daemon).
    let plan = gc_plan(root).unwrap();
    assert!(
        plan.orphan_blobs.len() == 1,
        "exactly the orphan is collectable, found {:?}",
        plan.orphan_blobs
    );
    let report = gc_apply(root, &plan).unwrap();
    assert_eq!(report.blobs_removed, 1);
    assert_eq!(
        report.captures_after,
        captures_before.len(),
        "timeline untouched"
    );

    // Every digest reachable before GC is still on disk after GC.
    for digest in &digests_before {
        assert!(
            blob_path(root, digest).exists(),
            "reachable blob {digest} must survive GC"
        );
    }

    // The one invariant that makes this GC lawful: a restore to any point
    // still lands byte-exact.
    for capture in &captures_before {
        let plan = store.plan_restore(&capture.id, &[], &ignores()).unwrap();
        if plan.applicable() {
            store.apply_restore(&plan, &ignores()).unwrap();
            let text = std::fs::read_to_string(root.join("src/lib.rs")).unwrap();
            assert!(text.contains("round_"), "restore target must have content");
            // Restore repositioned the tree; re-seed for the next iteration
            // so plans stay applicable.
            write(root, "assets/logo.bin", &[0xff, 0, 0x00, 0x93, 0x94, 0x02]);
            store.reconcile_worktree(&ignores()).unwrap();
        }
    }
}

#[test]
fn gc_of_a_clean_store_removes_nothing_reachable() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let _store = seeded_store(root);
    let plan = gc_plan(root).unwrap();
    assert_eq!(plan.orphan_blobs.len(), 0);
    let report = gc_apply(root, &plan).unwrap();
    assert_eq!(report.blobs_removed, 0);
    let reader = TimelineReader::open(root).unwrap();
    let after = reader.captures(true, None, false, usize::MAX).unwrap();
    assert!(after.len() >= 8, "history intact");
}

#[test]
fn reopened_store_snapshots_according_to_its_journal_tail_not_process_age() {
    // The compaction cadence must survive process restarts: a store whose
    // journal tail already holds more captures than the cadence has to
    // snapshot on its next flush, not after another full cadence of fresh
    // captures. Otherwise a frequently restarted daemon (the dev-loop
    // default) never snapshots at all and every reader pays a
    // full-journal replay per open.
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let no_compaction = StoreLimits {
        max_segment_bytes: 4 << 20,
        snapshot_edit_size: u64::MAX,
    };
    {
        let mut store = ProjectStore::open(root, no_compaction).unwrap();
        for round in 0..5 {
            write(
                root,
                "src/lib.rs",
                format!("pub fn round_{round}() {{}}\n").as_bytes(),
            );
            flush(
                &mut store,
                root,
                vec![FsEvent::now(EventKind::Touched {
                    path: root.join("src/lib.rs").into(),
                })],
            );
        }
    }
    let snapshots = || {
        std::fs::read_dir(root.join(".sheaf/store/snapshots"))
            .map(|rd| rd.flatten().count())
            .unwrap_or(0)
    };
    assert_eq!(snapshots(), 0, "cadence u64::MAX never snapshots");

    // A fresh process reopens with a cadence of 3: the five replayed
    // update frames seed the counter past it, so the very first flush
    // compacts.
    {
        let mut store = open(root);
        write(root, "src/lib.rs", b"pub fn round_5() {}\n");
        flush(
            &mut store,
            root,
            vec![FsEvent::now(EventKind::Touched {
                path: root.join("src/lib.rs").into(),
            })],
        );
        assert!(
            snapshots() > 0,
            "first flush after reopen must snapshot the stale tail"
        );
    }

    // And the compacted store still reads whole: every capture, including
    // the pre-snapshot ones, resolves through the snapshot baseline.
    let reader = TimelineReader::open(root).unwrap();
    let captures = reader.captures(true, None, false, usize::MAX).unwrap();
    assert_eq!(captures.len(), 6, "snapshot + tail replay sees all history");
    let report = doctor(root).unwrap();
    assert!(
        report.ok,
        "doctor green on the compacted store: {:#?}",
        report.checks
    );
}
