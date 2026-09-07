use std::path::Path;

use chrono::Utc;
use sheaf_core::events::{Batch, EventKind, FsEvent};
use sheaf_core::store::{
    hash_of, CaptureOrigin, HistoricalPathContent, OriginKind, ProjectStore, StoreLimits,
};

fn open(root: &Path) -> ProjectStore {
    sheaf_core::config::write_skeleton(root).unwrap();
    ProjectStore::open(root, StoreLimits::default()).unwrap()
}

fn capture_disk(store: &mut ProjectStore, root: &Path, path: &Path) {
    let now = Utc::now();
    store
        .apply_batch(&Batch {
            root: root.to_path_buf(),
            events: vec![FsEvent {
                kind: EventKind::Added {
                    path: path.to_path_buf(),
                },
                at: now,
            }],
            started_at: now,
            flushed_at: now,
        })
        .unwrap();
}

fn editor_origin(source: Option<String>) -> CaptureOrigin {
    CaptureOrigin {
        kind: OriginKind::Editor,
        source,
        target: None,
        scope: vec!["note.txt".into()],
        selections: Vec::new(),
    }
}

#[test]
fn unsaved_snapshot_is_durable_without_mutating_disk() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let path = root.join("note.txt");
    std::fs::write(&path, "one\n").unwrap();
    let mut store = open(root);
    capture_disk(&mut store, root, &path);

    let source = store.editor_cursor(&path).unwrap();
    let outcome = store
        .apply_editor_snapshot(&path, "one two\n", Utc::now(), editor_origin(source))
        .unwrap();

    assert!(outcome.capture.is_some());
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "one\n");
    assert_eq!(
        store.historical_path_content("@", "note.txt").unwrap(),
        HistoricalPathContent::Text("one two\n".into())
    );
    assert_eq!(
        outcome.capture.unwrap().origin.unwrap().kind,
        OriginKind::Editor
    );
}

#[test]
fn delayed_saved_echo_does_not_regress_newer_unsaved_text() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let path = root.join("note.txt");
    std::fs::write(&path, "saved\n").unwrap();
    let mut store = open(root);
    capture_disk(&mut store, root, &path);

    store
        .register_saved_editor_digest(&path, &hash_of(b"saved\n"))
        .unwrap();
    let source = store.editor_cursor(&path).unwrap();
    store
        .apply_editor_snapshot(&path, "newer\n", Utc::now(), editor_origin(source))
        .unwrap();

    let before = store
        .captures(false, None, false, usize::MAX)
        .unwrap()
        .len();
    capture_disk(&mut store, root, &path);
    capture_disk(&mut store, root, &path);
    let after = store
        .captures(false, None, false, usize::MAX)
        .unwrap()
        .len();

    assert_eq!(before, after);
    assert_eq!(
        store.historical_path_content("@", "note.txt").unwrap(),
        HistoricalPathContent::Text("newer\n".into())
    );
}

#[test]
fn logical_cursor_walks_across_editor_undo_and_redo_captures() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let path = root.join("note.txt");
    std::fs::write(&path, "one\n").unwrap();
    let mut store = open(root);
    capture_disk(&mut store, root, &path);
    let one = store.editor_cursor(&path).unwrap().unwrap();

    let two_capture = store
        .apply_editor_snapshot(&path, "two\n", Utc::now(), editor_origin(Some(one.clone())))
        .unwrap()
        .capture
        .unwrap();
    let two = two_capture.id;
    assert_eq!(
        store.previous_editor_cursor(&two, &path).unwrap(),
        Some(one.clone())
    );

    store
        .apply_editor_snapshot(
            &path,
            "one\n",
            Utc::now(),
            CaptureOrigin {
                kind: OriginKind::EditorUndo,
                source: Some(two.clone()),
                target: Some(one.clone()),
                scope: vec!["note.txt".into()],
                selections: Vec::new(),
            },
        )
        .unwrap();
    assert_eq!(store.editor_cursor(&path).unwrap(), Some(one.clone()));

    store
        .apply_editor_snapshot(
            &path,
            "two\n",
            Utc::now(),
            CaptureOrigin {
                kind: OriginKind::EditorRedo,
                source: Some(one.clone()),
                target: Some(two.clone()),
                scope: vec!["note.txt".into()],
                selections: Vec::new(),
            },
        )
        .unwrap();
    assert_eq!(store.editor_cursor(&path).unwrap(), Some(two.clone()));
    assert_eq!(
        store.previous_editor_cursor(&two, &path).unwrap(),
        Some(one)
    );
}

#[test]
fn different_external_content_is_not_a_saved_echo() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let path = root.join("note.txt");
    std::fs::write(&path, "first\n").unwrap();
    let mut store = open(root);
    capture_disk(&mut store, root, &path);

    store
        .register_saved_editor_digest(&path, &hash_of(b"first\n"))
        .unwrap();
    let before = store
        .captures(false, None, false, usize::MAX)
        .unwrap()
        .len();
    // A genuinely different external write still enters the timeline.
    std::fs::write(&path, "external\n").unwrap();
    capture_disk(&mut store, root, &path);
    let after = store
        .captures(false, None, false, usize::MAX)
        .unwrap()
        .len();
    assert_eq!(after, before + 1);
    assert_eq!(
        store.historical_path_content("@", "note.txt").unwrap(),
        HistoricalPathContent::Text("external\n".into())
    );
}

#[test]
fn multibyte_editor_text_reconstructs_exactly() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let path = root.join("note.txt");
    std::fs::write(&path, "café\n").unwrap();
    let mut store = open(root);
    capture_disk(&mut store, root, &path);
    let source = store.editor_cursor(&path).unwrap();
    store
        .apply_editor_snapshot(
            &path,
            "café → naïve 🌱\n",
            Utc::now(),
            editor_origin(source),
        )
        .unwrap();
    assert_eq!(
        store.historical_path_content("@", "note.txt").unwrap(),
        HistoricalPathContent::Text("café → naïve 🌱\n".into())
    );
}

#[test]
fn oversized_editor_snapshot_is_rejected_as_unsupported() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let path = root.join("note.txt");
    std::fs::write(&path, "seed\n").unwrap();
    let mut store = open(root);
    capture_disk(&mut store, root, &path);
    let huge = "a".repeat((sheaf_core::store::TEXT_MAX_BYTES + 1) as usize);
    let error = store
        .apply_editor_snapshot(&path, &huge, Utc::now(), editor_origin(None))
        .unwrap_err();
    assert_eq!(error.code(), "editor.unsupported");
}

#[test]
fn capture_origin_deserializes_without_the_editor_source_field() {
    let legacy = serde_json::json!({"kind": "restore", "target": "abc"});
    let origin: CaptureOrigin = serde_json::from_value(legacy).unwrap();
    assert_eq!(origin.kind, OriginKind::Restore);
    assert!(origin.source.is_none());
}

#[test]
fn previous_cursor_stops_at_the_start_of_text_history() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let path = root.join("note.txt");
    std::fs::write(&path, "only\n").unwrap();
    let mut store = open(root);
    capture_disk(&mut store, root, &path);
    let cursor = store.editor_cursor(&path).unwrap().unwrap();
    assert_eq!(store.previous_editor_cursor(&cursor, &path).unwrap(), None);
}

fn open_small_budget(root: &Path) -> ProjectStore {
    sheaf_core::config::write_skeleton(root).unwrap();
    ProjectStore::open_with_text_budget(root, StoreLimits::default(), 8).unwrap()
}

#[test]
fn editor_path_rejects_relative_symlink_and_untracked_inputs() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let path = root.join("note.txt");
    std::fs::write(&path, "one\n").unwrap();
    let mut store = open(root);
    capture_disk(&mut store, root, &path);

    // A relative path is refused before any filesystem work.
    let relative = store
        .apply_editor_snapshot(Path::new("note.txt"), "x\n", Utc::now(), editor_origin(None))
        .unwrap_err();
    assert_eq!(relative.code(), "editor.unsupported");

    // A symlink is not a regular tracked file.
    let link = root.join("link.txt");
    std::os::unix::fs::symlink(&path, &link).unwrap();
    let symlink = store
        .apply_editor_snapshot(&link, "x\n", Utc::now(), editor_origin(None))
        .unwrap_err();
    assert_eq!(symlink.code(), "editor.unsupported");

    // editor_text on a never-tracked path is unsupported, not a panic.
    let untracked = root.join("fresh.txt");
    std::fs::write(&untracked, "hi\n").unwrap();
    assert_eq!(store.editor_text(&untracked).unwrap_err().code(), "editor.unsupported");
}

#[test]
fn editor_snapshot_over_the_text_budget_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let path = root.join("note.txt");
    std::fs::write(&path, "ab\n").unwrap();
    let mut store = open_small_budget(root);
    capture_disk(&mut store, root, &path);
    let error = store
        .apply_editor_snapshot(&path, "much longer than eight bytes\n", Utc::now(), editor_origin(None))
        .unwrap_err();
    assert_eq!(error.code(), "editor.unsupported");
}

#[test]
fn validate_editor_cursor_accepts_current_lineage_and_rejects_unknown() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let path = root.join("note.txt");
    std::fs::write(&path, "one\n").unwrap();
    let mut store = open(root);
    capture_disk(&mut store, root, &path);
    let cursor = store.editor_cursor(&path).unwrap().unwrap();
    assert_eq!(store.validate_editor_cursor(&cursor).unwrap(), cursor);
    assert!(store.validate_editor_cursor("does-not-exist").is_err());
}

#[test]
fn identical_editor_snapshot_records_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let path = root.join("note.txt");
    std::fs::write(&path, "same\n").unwrap();
    let mut store = open(root);
    capture_disk(&mut store, root, &path);
    let before = store.captures(false, None, false, usize::MAX).unwrap().len();
    let outcome = store
        .apply_editor_snapshot(&path, "same\n", Utc::now(), editor_origin(None))
        .unwrap();
    assert!(outcome.capture.is_none());
    let after = store.captures(false, None, false, usize::MAX).unwrap().len();
    assert_eq!(before, after);
}
