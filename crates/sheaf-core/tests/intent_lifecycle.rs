//! Restore-intent lifecycle.
//!
//! A durable intent is a promise the daemon keeps across restarts — but an
//! intent a week old must never silently rewind a tree the user has been
//! working in since. These tests pin the operator path: staleness gating,
//! forced resume, and abandon-with-reconciliation.

use std::path::Path;

use sheaf_core::config;
use sheaf_core::events::{Batch, EventKind, FsEvent};
use sheaf_core::ignore::IgnoreSet;
use sheaf_core::store::{ProjectStore, RestoreIntent, RestoreMode, StoreLimits};

fn skeleton(root: &Path) {
    std::fs::create_dir_all(root.join(".sheaf/store")).unwrap();
    config::write_skeleton(root).unwrap();
}

fn limits() -> StoreLimits {
    StoreLimits {
        max_segment_bytes: 64 << 20,
        snapshot_edit_size: 1000,
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

fn read(root: &Path, rel: &str) -> Vec<u8> {
    std::fs::read(root.join(rel)).unwrap()
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

fn plant_intent(root: &Path, token: &str, target_frontier: &str, started_ms: i64) {
    let intent = RestoreIntent {
        token: token.to_owned(),
        mode: RestoreMode::Full,
        scope: vec![],
        target: sheaf_core::store::ResolvedPoint {
            capture_id: None,
            frontier: target_frontier.to_owned(),
        },
        started_ms,
        fragment: None,
    };
    std::fs::create_dir_all(root.join(".sheaf/state")).unwrap();
    std::fs::write(
        root.join(".sheaf/state/restore.intent"),
        serde_json::to_vec_pretty(&intent).unwrap(),
    )
    .unwrap();
}

#[test]
fn a_fresh_intent_auto_resumes_and_completes() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();

    write(root, "src/lib.rs", b"pub fn good() {}\n");
    write(root, "src/util.rs", b"pub fn util() {}\n");
    let mut store = open(root);
    flush(
        &mut store,
        root,
        vec![
            FsEvent::now(EventKind::Added {
                path: root.join("src/lib.rs"),
            }),
            FsEvent::now(EventKind::Added {
                path: root.join("src/util.rs"),
            }),
        ],
    );
    let reader = sheaf_core::store::TimelineReader::open(root).unwrap();
    let target = reader.captures(false, None, false, 1).unwrap()[0].clone();

    // Crash mid-apply: intent fresh, one file already reverted.
    plant_intent(
        root,
        "tok",
        &target.frontier,
        chrono::Utc::now().timestamp_millis(),
    );
    write(root, "src/lib.rs", b"pub fn good() {}\n"); // landed
    write(root, "src/util.rs", b"pub fn WRECKED() {}\n"); // not yet
    drop(store);

    let mut restarted = open(root);
    assert!(restarted.pending_restore().is_some());
    let outcome = restarted
        .resume_restore(
            &ignore,
            false,
            config::RestoreConfig::default().max_resume_age_ms,
        )
        .unwrap()
        .expect("fresh intent auto-resumes");
    assert!(outcome.resumed);
    assert_eq!(read(root, "src/util.rs"), b"pub fn util() {}\n");
    assert!(restarted.pending_restore().is_none());
}

#[test]
fn a_stale_intent_waits_for_the_operator_instead_of_replaying() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();

    write(root, "src/lib.rs", b"pub fn good() {}\n");
    let mut store = open(root);
    flush(
        &mut store,
        root,
        vec![FsEvent::now(EventKind::Added {
            path: root.join("src/lib.rs"),
        })],
    );
    let reader = sheaf_core::store::TimelineReader::open(root).unwrap();
    let target = reader.captures(false, None, false, 1).unwrap()[0].clone();
    drop(store);

    // A week-old intent: auto-replay would rewind a tree the user has been
    // working in for days. It must NOT run on boot.
    let week_ago = chrono::Utc::now().timestamp_millis() - 7 * 24 * 3600 * 1000 - 60_000;
    plant_intent(root, "tok", &target.frontier, week_ago);
    write(root, "src/lib.rs", b"pub fn good() {}\n");
    write(root, "later.txt", b"user kept working\n");

    let max_age = config::RestoreConfig::default().max_resume_age_ms;
    let mut booted = open(root);
    let intent = booted.pending_restore().expect("intent survives boot");
    assert!(intent.is_stale(max_age));
    assert!(
        booted
            .resume_restore(&ignore, false, max_age)
            .unwrap()
            .is_none(),
        "stale intent must not auto-replay"
    );
    // The user's later work stands, untouched.
    assert_eq!(read(root, "later.txt"), b"user kept working\n");
    assert!(
        booted.pending_restore().is_some(),
        "still pending, still visible"
    );

    // The operator asks for it by name: now it runs.
    let outcome = booted
        .resume_restore(&ignore, true, max_age)
        .unwrap()
        .expect("forced resume executes");
    assert!(outcome.resumed);
    assert!(booted.pending_restore().is_none());
}

#[test]
fn abandoning_keeps_the_worktree_and_captures_what_the_crash_left() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();

    write(root, "src/lib.rs", b"pub fn good() {}\n");
    write(root, "src/extra.rs", b"pub fn extra() {}\n");
    let mut store = open(root);
    flush(
        &mut store,
        root,
        vec![
            FsEvent::now(EventKind::Added {
                path: root.join("src/lib.rs"),
            }),
            FsEvent::now(EventKind::Added {
                path: root.join("src/extra.rs"),
            }),
        ],
    );
    let reader = sheaf_core::store::TimelineReader::open(root).unwrap();
    let target = reader.captures(false, None, false, 1).unwrap()[0].clone();
    drop(store);

    // Crash mid-apply: lib.rs landed back, extra.rs (not in the target
    // state) already deleted — a half-applied tree.
    plant_intent(
        root,
        "tok",
        &target.frontier,
        chrono::Utc::now().timestamp_millis(),
    );
    std::fs::remove_file(root.join("src/extra.rs")).unwrap();

    let mut booted = open(root);
    let capture = booted.abandon_restore(&ignore).unwrap();
    // The intent is gone, the worktree is exactly as the crash left it —
    // but that state is now ordinary history (the reconciliation capture),
    // so nothing on disk is uncaptured.
    assert!(booted.pending_restore().is_none());
    assert!(!root.join(".sheaf/state/restore.intent").exists());
    assert!(
        !root.join("src/extra.rs").exists(),
        "abandon does not finish the restore"
    );
    assert!(
        capture.is_some(),
        "the half-applied state must be captured, not left invisible"
    );
    // And a later capture still records forward work normally.
    write(root, "after.txt", b"new work\n");
    flush(
        &mut booted,
        root,
        vec![FsEvent::now(EventKind::Added {
            path: root.join("after.txt"),
        })],
    );
    assert_eq!(read(root, "after.txt"), b"new work\n");
}

#[test]
fn abandoning_with_no_intent_is_a_clean_noop() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    write(root, "a.txt", b"x\n");
    let mut store = open(root);
    flush(
        &mut store,
        root,
        vec![FsEvent::now(EventKind::Added {
            path: root.join("a.txt"),
        })],
    );
    let capture = store.abandon_restore(&ignore).unwrap();
    assert!(
        capture.is_none(),
        "nothing half-applied, nothing to reconcile"
    );
    assert!(store.pending_restore().is_none());
}
