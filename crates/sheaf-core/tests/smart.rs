//! Smart-squash planning over a real store. These tests cover
//! the pure planning layer: git is absent by design — HEAD-side content
//! arrives as a map, exactly the way the daemon's two-phase IPC supplies
//! it. The git orchestration itself (staging, commit, frames) is covered
//! end-to-end in the sheaf-cli integration tests.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use sheaf_core::config;
use sheaf_core::events::{Batch, EventKind, FsEvent};
use sheaf_core::store::{
    ByteRange, HistoricalPathContent, ProjectStore, SelectionExtent, SelectionHandle, StoreLimits,
};

fn tmp(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "sheaf-smart-{}-{tag}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

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

fn added(root: &Path, rel: &str) -> FsEvent {
    FsEvent::now(EventKind::Added {
        path: root.join(rel),
    })
}

fn modified(root: &Path, rel: &str) -> FsEvent {
    FsEvent::now(EventKind::Touched {
        path: root.join(rel).into(),
    })
}

/// Build a match-extent handle over `needle` in `path` at `reference`,
/// exactly the way timeline grep builds one.
fn handle_at(store: &ProjectStore, reference: &str, path: &str, needle: &str) -> SelectionHandle {
    let point = store.resolve(reference).unwrap();
    let text = match store.historical_path_content(reference, path).unwrap() {
        HistoricalPathContent::Text(text) => text,
        other => panic!("expected text at {reference}:{path}, got {other:?}"),
    };
    let start = text
        .find(needle)
        .unwrap_or_else(|| panic!("`{needle}` not found in {path} at {reference}:\n{text}"));
    SelectionHandle::from_source(
        point.frontier,
        point.capture_id,
        path,
        SelectionExtent::Match,
        ByteRange {
            start,
            end: start + needle.len(),
        },
        &text,
        format!("literal:{needle}"),
        None,
    )
    .unwrap()
}

/// Build a line-extent handle over the line containing `needle`, exactly
/// the way `sheaf grep --extent line` cuts one (the extent excludes the
/// trailing newline).
fn line_handle_at(
    store: &ProjectStore,
    reference: &str,
    path: &str,
    needle: &str,
) -> SelectionHandle {
    let point = store.resolve(reference).unwrap();
    let text = match store.historical_path_content(reference, path).unwrap() {
        HistoricalPathContent::Text(text) => text,
        other => panic!("expected text at {reference}:{path}, got {other:?}"),
    };
    let at = text
        .find(needle)
        .unwrap_or_else(|| panic!("`{needle}` not found in {path} at {reference}:\n{text}"));
    let start = text[..at].rfind('\n').map(|n| n + 1).unwrap_or(0);
    let end = text[at..].find('\n').map(|n| at + n).unwrap_or(text.len());
    SelectionHandle::from_source(
        point.frontier,
        point.capture_id,
        path,
        SelectionExtent::Line,
        ByteRange { start, end },
        &text,
        format!("literal:{needle}"),
        None,
    )
    .unwrap()
}

fn heads(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
    entries
        .iter()
        .map(|(p, t)| (p.to_string(), t.to_string()))
        .collect()
}

const GOOD: &str = "fn alpha() -> u32 {\n    1\n}\n\nfn beta() -> u32 {\n    2\n}\n";
const BOTH_DIRTY: &str = "fn alpha() -> u64 {\n    99\n}\n\nfn beta() -> u32 {\n    4200\n}\n";

#[test]
fn replace_stages_only_the_selected_unit_with_a_dirty_neighbor() {
    let root = tmp("replace");
    skeleton(&root);
    let mut store = open(&root);
    flush(&mut store, &root, vec![added(&root, "src/lib.rs")]);
    write(&root, "src/lib.rs", BOTH_DIRTY.as_bytes());
    flush(&mut store, &root, vec![modified(&root, "src/lib.rs")]);

    // The handle is bound at the tip: alpha in its edited form.
    let handle = handle_at(&store, "@", "src/lib.rs", "fn alpha() -> u64 {\n    99\n}");
    let plan = store
        .plan_smart_with_heads(&[handle], &heads(&[("src/lib.rs", GOOD)]))
        .unwrap();
    assert!(plan.applicable(), "{:#?}", plan.conflicts);
    assert_eq!(plan.files.len(), 1);
    let file = &plan.files[0];
    // The staged tree is HEAD with alpha's region replaced: alpha's edit
    // is staged, beta's edit stays dirty in the worktree only.
    assert_eq!(
        file.staged_text,
        "fn alpha() -> u64 {\n    99\n}\n\nfn beta() -> u32 {\n    2\n}\n"
    );
    assert!(file.staged_text.contains("fn alpha() -> u64 {\n    99\n}"));
    assert!(
        !file.staged_text.contains("4200"),
        "beta must stay uncommitted"
    );
    assert_eq!(
        plan.selections[0].kind,
        sheaf_core::store::SmartKind::Replace
    );
    // And the live worktree still holds both edits.
    assert_eq!(
        std::fs::read_to_string(root.join("src/lib.rs")).unwrap(),
        BOTH_DIRTY
    );
}

#[test]
fn insert_and_delete_infer_from_the_two_sides() {
    let root = tmp("insdel");
    skeleton(&root);
    let mut store = open(&root);
    let with_gamma = "fn alpha() -> u32 {\n    1\n}\n\nfn gamma() -> u32 {\n    3\n}\n\nfn beta() -> u32 {\n    2\n}\n";
    // The timeline captures the CURRENT state; the "HEAD" side is the
    // caller-supplied older content without gamma.
    write(&root, "src/lib.rs", with_gamma.as_bytes());
    flush(&mut store, &root, vec![added(&root, "src/lib.rs")]);

    let handle = handle_at(&store, "@", "src/lib.rs", "fn gamma() -> u32 {\n    3\n}");
    let plan = store
        .plan_smart_with_heads(&[handle], &heads(&[("src/lib.rs", GOOD)]))
        .unwrap();
    assert!(plan.applicable(), "{:#?}", plan.conflicts);
    assert_eq!(
        plan.selections[0].kind,
        sheaf_core::store::SmartKind::Insert
    );
    // The selection names gamma's own lines; the blank line AFTER gamma
    // belongs to the worktree's uncommitted formatting and stays there.
    assert_eq!(
        plan.files[0].staged_text,
        "fn alpha() -> u32 {\n    1\n}\n\nfn gamma() -> u32 {\n    3\n}\nfn beta() -> u32 {\n    2\n}\n"
    );

    // Delete: the worktree drops beta; the handle still names beta (bound
    // at the tip before the deletion), and the head side still has it.
    let root = tmp("del");
    skeleton(&root);
    let mut store = open(&root);
    write(&root, "src/lib.rs", GOOD.as_bytes());
    flush(&mut store, &root, vec![added(&root, "src/lib.rs")]);
    // The needle includes beta's trailing newline so the after-context is
    // the EOF boundary — a deleted unit at EOF still anchors.
    let handle = handle_at(&store, "@", "src/lib.rs", "fn beta() -> u32 {\n    2\n}\n");
    // Remove exactly beta: the blank line before it stays, so the
    // before-context still anchors and the unit reads as deleted.
    let without_beta = &GOOD[..GOOD.find("fn beta").unwrap()];
    write(&root, "src/lib.rs", without_beta.as_bytes());
    let plan = store
        .plan_smart_with_heads(&[handle], &heads(&[("src/lib.rs", GOOD)]))
        .unwrap();
    assert!(plan.applicable(), "{:#?}", plan.conflicts);
    assert_eq!(
        plan.selections[0].kind,
        sheaf_core::store::SmartKind::Delete
    );
    assert_eq!(plan.files[0].staged_text, without_beta);
}

#[test]
fn noop_selections_report_an_empty_patch() {
    let root = tmp("noop");
    skeleton(&root);
    let mut store = open(&root);
    write(&root, "src/lib.rs", GOOD.as_bytes());
    flush(&mut store, &root, vec![added(&root, "src/lib.rs")]);
    let handle = handle_at(&store, "@", "src/lib.rs", "fn alpha() -> u32 {\n    1\n}");
    let plan = store
        .plan_smart_with_heads(&[handle], &heads(&[("src/lib.rs", GOOD)]))
        .unwrap();
    assert!(!plan.applicable(), "{plan:#?}");
    assert_eq!(plan.unchanged, 1);
    assert!(plan
        .conflicts
        .iter()
        .any(|c| c.condition == sheaf_core::store::SmartCondition::EmptyPatch));
}

#[test]
fn symbol_and_hunk_extents_are_refused_for_mutation() {
    let root = tmp("extents");
    skeleton(&root);
    let mut store = open(&root);
    write(&root, "src/lib.rs", GOOD.as_bytes());
    flush(&mut store, &root, vec![added(&root, "src/lib.rs")]);
    for extent in [SelectionExtent::Symbol, SelectionExtent::Hunk] {
        let mut handle = handle_at(&store, "@", "src/lib.rs", "fn alpha");
        handle.extent = extent;
        let plan = store
            .plan_smart_with_heads(&[handle], &heads(&[("src/lib.rs", GOOD)]))
            .unwrap();
        assert!(!plan.applicable());
        assert!(
            plan.conflicts
                .iter()
                .any(|c| { c.condition == sheaf_core::store::SmartCondition::UnsupportedExtent }),
            "{extent:?}"
        );
    }
}

#[test]
fn new_files_and_deleted_files_are_ordinary_squash_territory() {
    let root = tmp("wholefile");
    skeleton(&root);
    let mut store = open(&root);
    write(&root, "src/lib.rs", GOOD.as_bytes());
    flush(&mut store, &root, vec![added(&root, "src/lib.rs")]);

    // Whole-file add: no HEAD text for the path.
    let handle = handle_at(&store, "@", "src/lib.rs", "fn alpha");
    let plan = store
        .plan_smart_with_heads(&[handle], &BTreeMap::new())
        .unwrap();
    assert!(matches!(
        plan.conflicts[0].condition,
        sheaf_core::store::SmartCondition::NewFileSinceHead
    ));

    // Whole-file removal: the worktree file is gone.
    let root = tmp("gone");
    skeleton(&root);
    let mut store = open(&root);
    write(&root, "src/lib.rs", GOOD.as_bytes());
    flush(&mut store, &root, vec![added(&root, "src/lib.rs")]);
    let handle = handle_at(&store, "@", "src/lib.rs", "fn alpha");
    std::fs::remove_file(root.join("src/lib.rs")).unwrap();
    let plan = store
        .plan_smart_with_heads(&[handle], &heads(&[("src/lib.rs", GOOD)]))
        .unwrap();
    assert!(matches!(
        plan.conflicts[0].condition,
        sheaf_core::store::SmartCondition::FileDeletedInWorktree
    ));
}

#[test]
fn overlapping_selections_refuse() {
    let root = tmp("overlap");
    skeleton(&root);
    let mut store = open(&root);
    write(&root, "src/lib.rs", BOTH_DIRTY.as_bytes());
    flush(&mut store, &root, vec![added(&root, "src/lib.rs")]);
    let alpha = handle_at(&store, "@", "src/lib.rs", "fn alpha() -> u64 {\n    99\n}");
    let whole = handle_at(
        &store,
        "@",
        "src/lib.rs",
        "fn alpha() -> u64 {\n    99\n}\n\nfn beta",
    );
    let plan = store
        .plan_smart_with_heads(&[alpha, whole], &heads(&[("src/lib.rs", GOOD)]))
        .unwrap();
    assert!(!plan.applicable());
    assert!(plan
        .conflicts
        .iter()
        .any(|c| c.condition == sheaf_core::store::SmartCondition::Overlap));
    // A refused plan stages nothing.
    assert!(plan.files.is_empty());
}

#[test]
fn duplicate_contexts_on_the_worktree_side_refuse() {
    let root = tmp("dup");
    skeleton(&root);
    let mut store = open(&root);
    // Two identical units each flanked by >64 bytes of identical padding
    // on BOTH sides: selected bytes AND contexts match at two places, so
    // planning must refuse rather than pick one.
    let unit = "fn u() {\n    1\n}\n";
    let pad = "pad line\n".repeat(9); // 81 bytes
    let text = format!("{pad}{unit}{pad}{unit}{pad}");
    write(&root, "src/lib.rs", text.as_bytes());
    flush(&mut store, &root, vec![added(&root, "src/lib.rs")]);
    let handle = handle_at(&store, "@", "src/lib.rs", unit);
    let plan = store
        .plan_smart_with_heads(&[handle], &heads(&[("src/lib.rs", &text)]))
        .unwrap();
    assert!(!plan.applicable(), "{:#?}", plan.conflicts);
    assert!(
        plan.conflicts.iter().any(|c| {
            c.condition == sheaf_core::store::SmartCondition::Ambiguous
                || c.condition == sheaf_core::store::SmartCondition::Missing
        }),
        "{:#?}",
        plan.conflicts
    );
}

#[test]
fn renames_since_head_refuse_with_a_whole_file_hint() {
    let root = tmp("rename");
    skeleton(&root);
    let mut store = open(&root);
    write(&root, "src/old.rs", GOOD.as_bytes());
    flush(&mut store, &root, vec![added(&root, "src/old.rs")]);
    // Rename recorded in the timeline...
    let handle = handle_at(&store, "@", "src/old.rs", "fn alpha");
    std::fs::rename(root.join("src/old.rs"), root.join("src/new.rs")).unwrap();
    flush(
        &mut store,
        &root,
        vec![FsEvent::now(EventKind::Renamed {
            from: root.join("src/old.rs"),
            to: root.join("src/new.rs"),
        })],
    );
    // The handle's historical path is the pre-rename name; the worktree
    // side follows the rename to `src/new.rs`, but HEAD still knows the
    // old path only.
    let plan = store
        .plan_smart_with_heads(&[handle], &heads(&[("src/old.rs", GOOD)]))
        .unwrap();
    assert!(!plan.applicable());
    assert!(plan
        .conflicts
        .iter()
        .any(|c| c.condition == sheaf_core::store::SmartCondition::RenamedSinceHead));
}

#[test]
fn destination_paths_phase_names_rename_followed_candidates() {
    let root = tmp("paths");
    skeleton(&root);
    let mut store = open(&root);
    write(&root, "src/old.rs", GOOD.as_bytes());
    flush(&mut store, &root, vec![added(&root, "src/old.rs")]);
    let handle = handle_at(&store, "@", "src/old.rs", "fn alpha");
    std::fs::rename(root.join("src/old.rs"), root.join("src/new.rs")).unwrap();
    flush(
        &mut store,
        &root,
        vec![FsEvent::now(EventKind::Renamed {
            from: root.join("src/old.rs"),
            to: root.join("src/new.rs"),
        })],
    );
    let paths = store.smart_destination_paths(&[handle]);
    assert!(paths.contains(&"src/old.rs".to_string()));
    assert!(paths.contains(&"src/new.rs".to_string()));
}

#[test]
fn boundary_inside_one_insertion_is_exact() {
    let root = tmp("insert-seam");
    skeleton(&root);
    let mut store = open(&root);
    // Two lines inserted where HEAD had none; selecting only the first
    // line is still exact — an insertion consumes no HEAD lines, so the
    // seam is unique whichever inserted lines the boundary splits.
    let head = "a\nb\n";
    let worktree = "a\nX\nY\nb\n";
    write(&root, "f.txt", worktree.as_bytes());
    flush(&mut store, &root, vec![added(&root, "f.txt")]);
    let handle = handle_at(&store, "@", "f.txt", "X\n");
    let plan = store
        .plan_smart_with_heads(&[handle], &heads(&[("f.txt", head)]))
        .unwrap();
    assert!(plan.applicable(), "{:#?}", plan.conflicts);
    assert_eq!(plan.files[0].staged_text, "a\nX\nb\n");
    assert_eq!(
        plan.selections[0].kind,
        sheaf_core::store::SmartKind::Insert
    );
}

#[test]
fn planning_reads_the_live_worktree_not_the_tip() {
    let root = tmp("live");
    skeleton(&root);
    let mut store = open(&root);
    write(&root, "src/lib.rs", GOOD.as_bytes());
    flush(&mut store, &root, vec![added(&root, "src/lib.rs")]);
    // The unit is edited AFTER the tip is captured; planning must read the
    // live file (the commit patches the worktree, not the timeline).
    write(&root, "src/lib.rs", BOTH_DIRTY.as_bytes());
    let handle = handle_at(&store, "@", "src/lib.rs", "fn alpha");
    // The handle's source no longer matches the live text: contexts of
    // `fn alpha` (the signature line alone) still anchor at the file head.
    let plan = store
        .plan_smart_with_heads(&[handle], &heads(&[("src/lib.rs", GOOD)]))
        .unwrap();
    // The after-context of `fn alpha` names the OLD body; the live file
    // holds the edited one, so the worktree anchor refuses — exactly, and
    // without reading anything but the live bytes.
    assert!(!plan.applicable());
    assert!(
        plan.conflicts.iter().any(|c| {
            matches!(
                c.condition,
                sheaf_core::store::SmartCondition::Missing
                    | sheaf_core::store::SmartCondition::Ambiguous
            ) && c.side == Some(sheaf_core::store::SmartSide::Worktree)
        }),
        "{:#?}",
        plan.conflicts
    );
}

#[test]
fn same_anchor_inserts_compose_in_worktree_order() {
    // The `grep | jq .hits` pipeline hands selections to squash in
    // ascending worktree order; a block of new lines selected per line
    // all anchor at one empty HEAD seam. Splicing at one coordinate
    // lands each splice's bytes before the ones already there, so only
    // descending-worktree application composes the block back into
    // worktree order — the staged tree must be identical whatever order
    // the handles arrived in.
    let root = tmp("anchor-order");
    skeleton(&root);
    let mut store = open(&root);
    let head = "fn a() {\n    1\n}\nfn b() {\n    2\n}\n";
    let block = "let extra_one = 1;\nlet extra_two = 2;\nlet extra_three = 3;\n";
    let worktree = format!("fn a() {{\n    1\n}}\n{block}fn b() {{\n    2\n}}\n");
    write(&root, "src/lib.rs", worktree.as_bytes());
    flush(&mut store, &root, vec![added(&root, "src/lib.rs")]);

    let one = line_handle_at(&store, "@", "src/lib.rs", "extra_one");
    let two = line_handle_at(&store, "@", "src/lib.rs", "extra_two");
    let three = line_handle_at(&store, "@", "src/lib.rs", "extra_three");
    let expected = format!("fn a() {{\n    1\n}}\n{block}fn b() {{\n    2\n}}\n");

    for order in [
        vec![one.clone(), two.clone(), three.clone()], // grep ascending
        vec![three.clone(), two.clone(), one.clone()], // descending
        vec![two.clone(), three.clone(), one.clone()], // shuffled
    ] {
        let plan = store
            .plan_smart_with_heads(&order, &heads(&[("src/lib.rs", head)]))
            .unwrap();
        assert!(plan.applicable(), "{:#?}", plan.conflicts);
        assert_eq!(
            plan.files[0].staged_text, expected,
            "input order must never leak into the staged tree"
        );
        for selection in &plan.selections {
            assert_eq!(
                selection.kind,
                sheaf_core::store::SmartKind::Insert,
                "{selection:#?}"
            );
            assert_eq!(
                selection.head.start, selection.head.end,
                "every inserted line anchors at one empty seam"
            );
        }
        assert_eq!(plan.files[0].added_bytes, block.len());
    }
}

#[test]
fn same_anchor_overlapping_worktree_extents_refuse() {
    // Two selections covering the same live line (a match extent and the
    // line extent around it) both anchor at the same empty seam; the
    // head-side overlap check cannot see it (both extents are empty), so
    // staging must refuse on the worktree side before the patch splices
    // those bytes twice.
    let root = tmp("anchor-overlap");
    skeleton(&root);
    let mut store = open(&root);
    let head = "fn a() {\n    1\n}\nfn b() {\n    2\n}\n";
    let worktree = "fn a() {\n    1\n}\nlet x = one();\nfn b() {\n    2\n}\n";
    write(&root, "src/lib.rs", worktree.as_bytes());
    flush(&mut store, &root, vec![added(&root, "src/lib.rs")]);

    let call = handle_at(&store, "@", "src/lib.rs", "one();");
    let whole_line = line_handle_at(&store, "@", "src/lib.rs", "let x = one();");
    for order in [
        vec![call.clone(), whole_line.clone()],
        vec![whole_line, call],
    ] {
        let plan = store
            .plan_smart_with_heads(&order, &heads(&[("src/lib.rs", head)]))
            .unwrap();
        assert!(!plan.applicable(), "{plan:#?}");
        assert!(
            plan.conflicts
                .iter()
                .any(|c| c.condition == sheaf_core::store::SmartCondition::Overlap),
            "{:#?}",
            plan.conflicts
        );
        assert!(plan.files.is_empty(), "a refused plan stages nothing");
    }
}

#[test]
fn smart_attribution_counts_only_selection_paths() {
    let root = tmp("attr");
    skeleton(&root);
    let mut store = open(&root);
    write(&root, "src/lib.rs", GOOD.as_bytes());
    flush(&mut store, &root, vec![added(&root, "src/lib.rs")]);
    let other_capture = {
        write(&root, "src/other.rs", b"fn z() {}\n");
        flush(&mut store, &root, vec![added(&root, "src/other.rs")]);
        store.resolve("@").unwrap().capture_id.unwrap()
    };
    write(&root, "src/lib.rs", BOTH_DIRTY.as_bytes());
    flush(&mut store, &root, vec![modified(&root, "src/lib.rs")]);
    let _tip = store.resolve("@").unwrap().capture_id.unwrap();
    let _ = other_capture;

    // Walk the lineage newest-first (wire order) and filter by the
    // selection path.
    let captures = store.captures(false, None, false, usize::MAX).unwrap();
    let newest_first: Vec<_> = captures.into_iter().take(3).collect();
    assert!(newest_first.len() >= 2);
    let paths = std::collections::BTreeSet::from(["src/lib.rs".to_string()]);
    let attribution = sheaf_core::store::smart_attribution(&newest_first, &paths);
    // The tip (lib.rs) counts; the other.rs-only capture must not.
    assert!(attribution.captures >= 1, "{attribution:?}");
    assert!(newest_first
        .iter()
        .any(|c| !c.paths.iter().any(|p| paths.contains(p))));
}
