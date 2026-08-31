//! `doctor --fix` — bounded repair of the safe classes,
//! refusal with guidance for everything else.
//!
//! The bar: a torn journal tail truncates to exactly what replay already
//! reads (and the writer resumes on the truncated segment), superseded
//! snapshots and quarantined intents clear, a pending restore intent is
//! operator state and is never touched, and ambiguity (a missing blob) is
//! refused with guidance rather than guessed at.

use std::io::Write;
use std::path::Path;

use chrono::{Duration, Utc};
use sheaf_core::config;
use sheaf_core::events::{Batch, EventKind, FsEvent};
use sheaf_core::store::{doctor, doctor_fix, ProjectStore, StoreLimits};

fn skeleton(root: &Path) {
    std::fs::create_dir_all(root.join(".sheaf/store")).unwrap();
    config::write_skeleton(root).unwrap();
}

fn open(root: &Path) -> ProjectStore {
    ProjectStore::open(root, StoreLimits::default()).unwrap()
}

fn write(root: &Path, rel: &str, bytes: &[u8]) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

fn flush(store: &mut ProjectStore, root: &Path, rel: &str, bytes: &[u8]) {
    write(root, rel, bytes);
    let now = Utc::now();
    let batch = Batch {
        root: root.to_path_buf(),
        events: vec![FsEvent {
            at: now,
            kind: EventKind::Touched {
                path: sheaf_core::events::TouchedPath(root.join(rel)),
            },
        }],
        started_at: now,
        flushed_at: now,
    };
    store.apply_batch(&batch).unwrap();
}

fn active_segment(root: &Path) -> std::path::PathBuf {
    let dir = root.join(".sheaf/store/journal");
    let mut segs: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "op"))
        .collect();
    segs.sort();
    segs.pop().expect("at least one journal segment")
}

#[test]
fn torn_tail_truncated_writer_resumes_and_history_intact() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    {
        let mut store = open(root);
        flush(&mut store, root, "a.txt", b"v1\n");
        flush(&mut store, root, "a.txt", b"v2\n");
    }
    let before_captures = doctor(root).unwrap().captures;
    assert_eq!(before_captures, 2);

    // Simulate a crash mid-append: a frame header claiming a payload that
    // never landed.
    let seg = active_segment(root);
    let intact_len = std::fs::metadata(&seg).unwrap().len();
    {
        let mut f = std::fs::OpenOptions::new().append(true).open(&seg).unwrap();
        f.write_all(&1234u32.to_le_bytes()).unwrap(); // bogus length
        f.write_all(&0u32.to_le_bytes()).unwrap(); // bogus crc
        f.write_all(b"half a payload").unwrap();
    }
    let report = doctor(root).unwrap();
    assert!(!report.ok, "torn tail must fail the sweep");
    assert!(report
        .checks
        .iter()
        .any(|c| c.name == "journal_frames" && !c.ok));

    let outcome = doctor_fix(root).unwrap();
    assert!(outcome
        .applied
        .iter()
        .any(|f| f.action == "truncate-journal"));
    assert!(
        outcome.healthy(),
        "re-run sweep is green: {:#?}",
        outcome.after.checks
    );
    assert_eq!(
        std::fs::metadata(&seg).unwrap().len(),
        intact_len,
        "segment is exactly its intact prefix"
    );

    // Replay semantics did not change: same captures, and the writer
    // resumes appending on the truncated segment.
    assert_eq!(outcome.after.captures, 2);
    {
        let mut store = open(root);
        flush(&mut store, root, "a.txt", b"v3\n");
    }
    assert_eq!(doctor(root).unwrap().captures, 3);
}

#[test]
fn superseded_snapshots_and_quarantine_and_stage_clear() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    {
        let mut store = open(root);
        flush(&mut store, root, "a.txt", b"v1\n");
        store.compact().unwrap();
        flush(&mut store, root, "a.txt", b"v2\n");
        store.compact().unwrap();
    }
    // Plain compaction does not prune its predecessors (gc owns that);
    // two compactions leave a naturally superseded pair to repair.
    let snaps = root.join(".sheaf/store/snapshots");
    let live: Vec<_> = std::fs::read_dir(&snaps)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    let newest_idx: u64 = live
        .iter()
        .filter_map(|n| {
            n.strip_prefix("snap-")?
                .strip_suffix(".manifest.json")?
                .parse()
                .ok()
        })
        .max()
        .unwrap();
    let stale_idx = newest_idx - 1;
    assert!(
        live.iter()
            .any(|n| n.contains(&format!("snap-{stale_idx:06}"))),
        "precondition: superseded pair present"
    );

    // Quarantined intent + leftover staging.
    let state = root.join(".sheaf/state");
    std::fs::create_dir_all(&state).unwrap();
    std::fs::write(state.join("restore.intent.bad"), b"{ not json").unwrap();
    let stage = root.join(".sheaf/store/restore-stage");
    std::fs::create_dir_all(stage.join("sub")).unwrap();
    std::fs::write(stage.join("sub/debris.bin"), b"x").unwrap();

    let report = doctor(root).unwrap();
    assert!(!report.ok, "quarantined intent must fail the sweep");
    assert_eq!(
        report.superseded_snapshots, 1,
        "the stale pair below the newest manifest"
    );

    let outcome = doctor_fix(root).unwrap();
    assert!(outcome.healthy(), "{:#?}", outcome.after.checks);
    let actions: Vec<&str> = outcome.applied.iter().map(|f| f.action.as_str()).collect();
    assert!(actions.contains(&"remove-superseded"));
    assert!(actions.contains(&"remove-quarantine"));
    assert!(actions.contains(&"remove-stage"));
    assert!(!snaps.join(format!("snap-{stale_idx:06}.snapshot")).exists());
    assert!(!state.join("restore.intent.bad").exists());
    assert!(!stage.exists());
}

#[test]
fn missing_blob_is_refused_with_guidance() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    {
        let mut store = open(root);
        flush(&mut store, root, "data.bin", &[0xFFu8; 64 * 1024]); // non-UTF8 ⇒ content-addressed blob path
    }
    // Remove the one blob payload: data loss doctor must not paper over.
    let blobs = root.join(".sheaf/store/blobs");
    let victim: std::path::PathBuf = std::fs::read_dir(&blobs)
        .unwrap()
        .flatten()
        .map(|e| e.path())
        .find(|p| p.is_dir())
        .and_then(|fan| {
            std::fs::read_dir(fan)
                .unwrap()
                .flatten()
                .map(|e| e.path())
                .find(|p| p.is_file())
        })
        .expect("one stored blob payload");
    std::fs::remove_file(&victim).unwrap();

    let report = doctor(root).unwrap();
    assert!(!report.ok);

    let outcome = doctor_fix(root).unwrap();
    assert!(!outcome.healthy(), "missing payload stays a failure");
    let refusal = outcome
        .refused
        .iter()
        .find(|r| r.check == "blob_coverage")
        .expect("refusal names blob_coverage");
    assert!(
        refusal.reason.contains("cannot be synthesized"),
        "{}",
        refusal.reason
    );
    assert!(
        !outcome
            .applied
            .iter()
            .any(|f| f.action == "truncate-journal"),
        "nothing else was touched"
    );
}

#[test]
fn pending_intent_is_operator_state_never_touched() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    {
        let mut store = open(root);
        flush(&mut store, root, "a.txt", b"v1\n");
    }
    // A real pending intent: `restore.resume`/`abandon` own it; --fix
    // must leave it exactly where it is (opening the store again must
    // keep it, not quarantine it).
    let intent = sheaf_core::store::RestoreIntent {
        token: "token-p4".into(),
        mode: sheaf_core::store::RestoreMode::Full,
        scope: vec![],
        target: sheaf_core::store::ResolvedPoint {
            frontier: "0-0".into(),
            capture_id: None,
        },
        started_ms: (Utc::now() - Duration::hours(8)).timestamp_millis(),
        fragment: None,
    };
    let intent_path = root.join(".sheaf/state/restore.intent");
    std::fs::write(&intent_path, serde_json::to_string(&intent).unwrap()).unwrap();

    let outcome = doctor_fix(root).unwrap();
    assert!(
        outcome.applied.is_empty(),
        "a healthy store with a pending intent gets no repair: {:#?}",
        outcome.applied
    );
    assert!(intent_path.exists(), "pending intent untouched");
    assert!(outcome.after.pending_restore.is_some());
}
