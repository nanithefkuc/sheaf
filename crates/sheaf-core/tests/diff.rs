//! Diff engine over real trees — worktree vs point, point vs
//! point (branches included), renamed paths, type changes, binary content,
//! path scopes, and the degraded read-only view.

use std::path::Path;

use chrono::Utc;
use sheaf_core::config;
use sheaf_core::events::{Batch, EventKind, FsEvent};
use sheaf_core::ignore::IgnoreSet;
use sheaf_core::store::SideContent;
use sheaf_core::store::{DiffKind, ProjectStore, StoreLimits, TimelineReader};

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

fn write(root: &Path, rel: &str, bytes: &[u8]) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

fn capture_now(store: &mut ProjectStore) -> sheaf_core::store::Capture {
    store
        .reconcile_worktree(&ignores())
        .unwrap()
        .expect("expected a capture")
}

/// Feed an explicit structural event (renames are first-class).
fn structural(
    store: &mut ProjectStore,
    root: &Path,
    kind: EventKind,
) -> sheaf_core::store::Capture {
    let now = Utc::now();
    let outcome = store
        .apply_batch(&Batch {
            root: root.to_path_buf(),
            started_at: now,
            flushed_at: now,
            events: vec![FsEvent::now(kind)],
        })
        .unwrap();
    outcome
        .capture
        .expect("structural batch produced a capture")
}

fn entry<'a>(
    outcome: &'a sheaf_core::store::DiffOutcome,
    path: &str,
) -> &'a sheaf_core::store::FileDiff {
    outcome
        .entries
        .iter()
        .find(|e| e.path == path)
        .unwrap_or_else(|| panic!("no diff entry for {path}"))
}

#[test]
fn worktree_diff_shows_uncaptured_edits() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = ProjectStore::open(root, limits()).unwrap();
    let ignore = ignores();

    write(root, "src/lib.rs", b"fn one() {}\nfn two() {}\n");
    write(root, "README.md", b"notes\n");
    write(root, "img.bin", &[0xff, 0xfe, 0x00]);
    capture_now(&mut store);

    // An uncaptured afternoon: an edit, a deletion, an addition, and a
    // binary rewrite — none of it flushed yet.
    write(root, "src/lib.rs", b"fn one() {}\nfn TWO() {}\n");
    std::fs::remove_file(root.join("README.md")).unwrap();
    write(root, "src/new.rs", b"fresh\n");
    write(root, "img.bin", &[0x94, 0x94, 0x94, 0x94]);

    let outcome = store.diff("@", None, &[], &ignore).unwrap();
    assert_eq!(outcome.to.kind, "worktree");
    let lib = entry(&outcome, "src/lib.rs");
    assert_eq!(lib.kind, DiffKind::Modified);
    assert_eq!((lib.added_lines, lib.removed_lines), (1, 1));
    assert!(lib.hunks.iter().any(|h| h.contains("-fn two() {}")));
    assert!(lib.hunks.iter().any(|h| h.contains("+fn TWO() {}")));
    assert_eq!(entry(&outcome, "README.md").kind, DiffKind::Deleted);
    assert_eq!(entry(&outcome, "src/new.rs").kind, DiffKind::Added);
    let bin = entry(&outcome, "img.bin");
    assert_eq!(bin.kind, DiffKind::Modified);
    assert!(matches!(bin.old, SideContent::Binary { .. }));
    assert!(bin.hunks.is_empty(), "binary pairs carry no hunks");

    // Once captured, the worktree agrees with head: empty diff.
    capture_now(&mut store);
    let settled = store.diff("@", None, &[], &ignore).unwrap();
    assert!(settled.is_empty(), "expected no differences");
    assert!(settled.render_patch().is_empty());
}

#[test]
fn point_vs_point_diff_spans_captures() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = ProjectStore::open(root, limits()).unwrap();
    let ignore = ignores();

    write(root, "a.txt", b"v1 line\n");
    let first = capture_now(&mut store);
    write(root, "a.txt", b"v2 line\n");
    write(root, "b.txt", b"added later\n");
    let second = capture_now(&mut store);

    let outcome = store
        .diff(&first.id, Some(&second.id), &[], &ignore)
        .unwrap();
    assert_eq!(outcome.from.capture_id.as_deref(), Some(first.id.as_str()));
    assert_eq!(outcome.to.capture_id.as_deref(), Some(second.id.as_str()));
    assert_eq!(entry(&outcome, "a.txt").kind, DiffKind::Modified);
    assert_eq!(entry(&outcome, "b.txt").kind, DiffKind::Added);

    // Reversed: the same change read backwards.
    let back = store
        .diff(&second.id, Some(&first.id), &[], &ignore)
        .unwrap();
    assert_eq!(entry(&back, "b.txt").kind, DiffKind::Deleted);
}

#[test]
fn recorded_renames_pair_even_when_content_changed() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = ProjectStore::open(root, limits()).unwrap();
    let ignore = ignores();

    write(root, "old.rs", b"fn same() {}\n");
    let first = capture_now(&mut store);
    std::fs::rename(root.join("old.rs"), root.join("new.rs")).unwrap();
    write(root, "new.rs", b"fn same() {}\nfn extra() {}\n");
    structural(
        &mut store,
        root,
        EventKind::Renamed {
            from: root.join("old.rs"),
            to: root.join("new.rs"),
        },
    );

    let outcome = store.diff(&first.id, None, &[], &ignore).unwrap();
    let renamed = outcome
        .entries
        .iter()
        .find(|e| e.kind == DiffKind::Renamed)
        .expect("rename should pair through the recorded event");
    assert_eq!(renamed.path, "new.rs");
    assert_eq!(renamed.old_path.as_deref(), Some("old.rs"));
    assert_eq!((renamed.added_lines, renamed.removed_lines), (1, 0));
    assert!(renamed.hunks.iter().any(|h| h.contains("+fn extra() {}")));

    let patch = String::from_utf8(outcome.render_patch()).unwrap();
    assert!(patch.contains("rename from old.rs"));
    assert!(patch.contains("rename to new.rs"));
    assert!(!patch.contains("diff --sheaf a/old.rs b/old.rs"));
}

#[test]
fn unflushed_renames_pair_by_content_identity() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = ProjectStore::open(root, limits()).unwrap();
    let ignore = ignores();

    write(root, "before.txt", b"identical bytes\n");
    capture_now(&mut store);
    // A rename the watcher has not recorded yet: delete+create with equal
    // content on the live side must still read as one move, the same way
    // the restore engine would pair it.
    std::fs::rename(root.join("before.txt"), root.join("after.txt")).unwrap();

    let outcome = store.diff("@", None, &[], &ignore).unwrap();
    let renamed = outcome
        .entries
        .iter()
        .find(|e| e.kind == DiffKind::Renamed)
        .expect("identity pairing should catch the unflushed move");
    assert_eq!(renamed.path, "after.txt");
    assert_eq!(renamed.old_path.as_deref(), Some("before.txt"));
    assert!(renamed.hunks.is_empty());
}

#[test]
fn type_changes_and_binary_rewrites_are_classified() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = ProjectStore::open(root, limits()).unwrap();
    let ignore = ignores();

    write(root, "t.txt", b"text\n");
    write(root, "b.bin", &[0xff, 0x01, 0x02]);
    capture_now(&mut store);
    write(root, "t.txt", &[0xff, 0xfe, 0x00]); // text -> binary
    write(root, "b.bin", &[0x94, 0x95, 0x96]); // binary -> binary

    let outcome = store.diff("@", None, &[], &ignore).unwrap();
    let changed = entry(&outcome, "t.txt");
    assert_eq!(changed.kind, DiffKind::TypeChanged);
    assert!(matches!(changed.old, SideContent::Text { .. }));
    assert!(matches!(changed.new, SideContent::Binary { .. }));
    let binmod = entry(&outcome, "b.bin");
    assert_eq!(binmod.kind, DiffKind::Modified);
    let patch = String::from_utf8(outcome.render_patch()).unwrap();
    assert!(patch.contains("Binary files a/t.txt and b/t.txt differ"));
    assert!(patch.contains("Binary files a/b.bin and b/b.bin differ"));
}

#[test]
fn diff_scopes_narrow_to_paths() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = ProjectStore::open(root, limits()).unwrap();
    let ignore = ignores();

    write(root, "src/one.rs", b"one\n");
    write(root, "src/two.rs", b"two\n");
    write(root, "docs/guide.md", b"guide\n");
    let first = capture_now(&mut store);
    write(root, "src/one.rs", b"ONE\n");
    write(root, "src/two.rs", b"TWO\n");
    write(root, "docs/guide.md", b"GUIDE\n");
    capture_now(&mut store);

    let src_only = store
        .diff(&first.id, None, &["src".to_string()], &ignore)
        .unwrap();
    assert_eq!(src_only.entries.len(), 2);
    assert!(src_only.entries.iter().all(|e| e.path.starts_with("src/")));

    let one = store
        .diff(&first.id, None, &["src/one.rs".to_string()], &ignore)
        .unwrap();
    assert_eq!(one.entries.len(), 1);
    assert_eq!(one.entries[0].path, "src/one.rs");
}

#[test]
fn cross_branch_point_diff_sees_both_sides() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = ProjectStore::open(root, limits()).unwrap();
    let ignore = ignores();

    write(root, "shared.txt", b"base\n");
    let base = capture_now(&mut store);
    write(root, "future.txt", b"abandoned future\n");
    let abandoned = capture_now(&mut store);

    // Roll back, then diverge: the abandoned future is now off-lineage, but
    // a diff against it must still work — frontiers are just addresses.
    let plan = store.plan_restore(&base.id, &[], &ignore).unwrap();
    store.apply_restore(&plan, &ignore).unwrap();
    write(root, "new.txt", b"the new future\n");
    let divergent = capture_now(&mut store);

    let outcome = store
        .diff(&divergent.id, Some(&abandoned.id), &[], &ignore)
        .unwrap();
    assert_eq!(entry(&outcome, "future.txt").kind, DiffKind::Added);
    assert_eq!(entry(&outcome, "new.txt").kind, DiffKind::Deleted);
}

#[test]
fn degraded_reader_diff_matches_the_live_store() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = ProjectStore::open(root, limits()).unwrap();
    let ignore = ignores();

    write(root, "a.txt", b"one\n");
    let first = capture_now(&mut store);
    write(root, "a.txt", b"two\n");
    let second = capture_now(&mut store);
    drop(store);

    let reader = TimelineReader::open(root).unwrap();
    let live_style = reader
        .diff(&first.id, Some(&second.id), &[], &ignore)
        .unwrap();
    assert!(live_style.degraded);
    assert_eq!(entry(&live_style, "a.txt").kind, DiffKind::Modified);
    let patch = String::from_utf8(live_style.render_patch()).unwrap();
    assert!(patch.contains("-one"));
    assert!(patch.contains("+two"));
}

#[test]
fn diff_bad_references_fail_closed() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = ProjectStore::open(root, limits()).unwrap();
    let ignore = ignores();
    write(root, "a.txt", b"one\n");
    capture_now(&mut store);

    let err = store.diff("deadbeef", None, &[], &ignore).unwrap_err();
    assert_eq!(err.code(), "state.bad_reference");
    let err = store
        .diff("@", Some("checkpoint:missing"), &[], &ignore)
        .unwrap_err();
    assert_eq!(err.code(), "state.bad_reference");
}
