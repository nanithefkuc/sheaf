//! Snapshot-bound fragment restore.
//!
//! The bar: applying a historical selection changes exactly that extent
//! (byte hashes prove everything else untouched), ambiguity of any kind is
//! a typed conflict that writes nothing, a live edit after preview stales
//! the token, apply records undoable pre-restore history plus one forward
//! capture naming the selection, and a kill at any restore-intent boundary
//! converges on the complete splice or the untouched pre-intent state.

use std::path::Path;

use chrono::{Duration, Utc};
use sheaf_core::config;
use sheaf_core::events::{Batch, EventKind, FsEvent};
use sheaf_core::ignore::IgnoreSet;
use sheaf_core::store::{
    CaptureOrigin, FragmentActionKind, FragmentCondition, FragmentMode, OriginKind, ProjectStore,
    RestoreIntent, RestoreMode, StoreLimits, TimelineReader,
};

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
        started_at: Utc::now(),
        flushed_at: Utc::now(),
    };
    store.apply_batch(&batch).unwrap();
}

fn added(root: &Path, rel: &str) -> FsEvent {
    FsEvent::now(EventKind::Added {
        path: root.join(rel),
    })
}

fn touched(root: &Path, rel: &str) -> FsEvent {
    FsEvent::now(EventKind::Touched {
        path: root.join(rel).into(),
    })
}

/// Build a handle over `needle` in `path` at `reference`, exactly the way
/// timeline grep builds one: extent Match, context-bound, fingerprinted.
fn handle_at(
    store: &ProjectStore,
    reference: &str,
    path: &str,
    needle: &str,
) -> sheaf_core::store::SelectionHandle {
    use sheaf_core::store::{HistoricalPathContent, SelectionExtent, SelectionHandle};
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
        sheaf_core::store::ByteRange {
            start,
            end: start + needle.len(),
        },
        &text,
        format!("literal:{needle}"),
        None,
    )
    .unwrap()
}

/// Build a parser-backed symbol handle the way a symbol-mode grep would:
/// extent Symbol, semantic identity from the selection adapter seam. This is
/// the handle type that can rebind across content changes.
fn symbol_handle_at(
    store: &ProjectStore,
    reference: &str,
    path: &str,
    name: &str,
) -> sheaf_core::store::SelectionHandle {
    use sheaf_core::store::{
        HistoricalPathContent, RustPrototypeParser, SelectionExtent, SelectionHandle, SymbolParser,
    };
    let point = store.resolve(reference).unwrap();
    let text = match store.historical_path_content(reference, path).unwrap() {
        HistoricalPathContent::Text(text) => text,
        other => panic!("expected text at {reference}:{path}, got {other:?}"),
    };
    let parser = RustPrototypeParser;
    let symbols = parser
        .parse_symbols(std::path::Path::new(path), &text)
        .unwrap();
    let symbol = symbols
        .iter()
        .find(|s| s.identity.qualified_name == name)
        .unwrap_or_else(|| panic!("symbol `{name}` not found in {path} at {reference}"));
    SelectionHandle::from_source(
        point.frontier,
        point.capture_id,
        path,
        SelectionExtent::Symbol,
        symbol.range,
        &text,
        format!("symbol:{name}"),
        Some(symbol.identity.clone()),
    )
    .unwrap()
}

/// The canonical two-function fixture: one wreckable, one bystander.
const GOOD: &str = "fn alpha() -> u32 {\n    1\n}\nfn beta() -> u32 {\n    2\n}\n";
const WRECKED: &str = "fn alpha() -> u64 {\n    99\n}\nfn beta() -> u32 {\n    2\n}\n";

fn seed_two_files(root: &Path, store: &mut ProjectStore) {
    write(root, "src/lib.rs", GOOD.as_bytes());
    write(root, "src/other.rs", b"pub const X: u32 = 1;\n");
    flush(
        store,
        root,
        vec![added(root, "src/lib.rs"), added(root, "src/other.rs")],
    );
}

#[test]
fn replace_splices_exactly_the_selected_extent() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);
    seed_two_files(root, &mut store);

    write(root, "src/lib.rs", WRECKED.as_bytes());
    flush(&mut store, root, vec![touched(root, "src/lib.rs")]);
    let pre_bytes = read(root, "src/lib.rs");

    // A grep-emitted text handle flows through planning too: the beta
    // line survived the wreck unchanged, so its replace plan is an
    // acknowledged no-op ("already there"), not a conflict.
    use sheaf_core::store::{GrepQuery, GrepRequest, LifecycleKind, SelectionExtent};
    let report = store
        .grep(&GrepRequest {
            query: GrepQuery::literal("fn beta() -> u32 {"),
            mode: sheaf_core::store::GrepMode::Point,
            at: None,
            from: None,
            to: None,
            path: None,
            follow: false,
            all: false,
            every_capture: false,
            extent: SelectionExtent::Line,
            budget: Default::default(),
            cursor: None,
            anchor: None,
        })
        .unwrap();
    // A point-discovery hit AT the current point holds bytes and context the
    // destination already mirrors: its replace plan is an acknowledged
    // no-op -- the grep-to-fragment-plan pipeline in one motion.
    let hit = report
        .hits
        .iter()
        .rev()
        .find(|h| h.on_current && h.kind == LifecycleKind::Present)
        .expect("point discovery finds the bystander at @");
    assert_eq!(hit.handle.extent, SelectionExtent::Line);
    let bystander_plan = store
        .plan_fragment_restore(std::slice::from_ref(&hit.handle), FragmentMode::Replace)
        .unwrap();
    assert!(bystander_plan.applicable());
    assert_eq!(bystander_plan.unchanged, 1, "unchanged bytes are a no-op");
    assert!(bystander_plan.is_noop());

    // The wrecked function itself needs the symbol handle: semantic
    // identity rebinds across the content change, where exact
    // text correctly reports the historical bytes as gone.
    let good_point = store.resolve("@~1").unwrap().capture_id.unwrap();
    let handle = symbol_handle_at(&store, &good_point, "src/lib.rs", "alpha");
    let text_plan = store
        .plan_fragment_restore(
            &[handle_at(
                &store,
                &good_point,
                "src/lib.rs",
                "fn alpha() -> u32 {\n    1\n}",
            )],
            FragmentMode::Replace,
        )
        .unwrap();
    assert!(matches!(
        text_plan.conflicts[0].condition,
        FragmentCondition::Missing
    ));

    let plan = store
        .plan_fragment_restore(std::slice::from_ref(&handle), FragmentMode::Replace)
        .unwrap();
    assert!(plan.applicable(), "conflicts: {:#?}", plan.conflicts);
    assert_eq!(plan.files.len(), 1);
    assert_eq!(plan.files[0].path, "src/lib.rs");
    assert_eq!(plan.files[0].actions.len(), 1);
    let action = &plan.files[0].actions[0];
    assert_eq!(action.kind, FragmentActionKind::Replace);
    assert_eq!(
        String::from_utf8_lossy(&pre_bytes[action.range.start..action.range.end]),
        "fn alpha() -> u64 {\n    99\n}"
    );

    let pre_apply_head = store.resolve("@").unwrap().capture_id;
    let outcome = store.apply_fragment_restore(&plan, &ignore).unwrap();

    // Exactly the selected extent moved: splicing the planned new bytes
    // into the pre bytes at the planned range reproduces the result, which
    // is the acceptance-level "out-of-range bytes unchanged" proof.
    let post = read(root, "src/lib.rs");
    // Byte-exact out-of-range proof: prefix and suffix around the action's
    // range are identical, and only the action's new bytes were inserted.
    let new_fragment = b"fn alpha() -> u32 {\n    1\n}";
    assert_eq!(
        post[..action.range.start],
        pre_bytes[..action.range.start],
        "bytes before the range are untouched"
    );
    assert_eq!(
        post[action.range.start + new_fragment.len()..],
        pre_bytes[action.range.end..],
        "bytes after the range are untouched"
    );
    assert_eq!(String::from_utf8_lossy(&post), GOOD);

    // Provenance: one forward capture names the selection. Everything was
    // already captured here, so the undo reference is the pre-apply head
    // and no extra safety capture was needed (the live-edit test covers
    // the uncaptured path).
    let forward = store
        .capture_info(&outcome.restore_capture.clone().unwrap())
        .unwrap();
    let origin = forward
        .capture
        .origin
        .expect("forward capture carries origin");
    assert_eq!(origin.kind, OriginKind::FragmentRestore);
    assert_eq!(origin.selections, vec![handle.id()]);
    assert_eq!(origin.scope, vec!["src/lib.rs".to_string()]);
    assert!(outcome.pre_restore_capture.is_none());
    assert_eq!(outcome.undo.capture_id, pre_apply_head);
    // The bystander file and everything outside the range are untouched.
    assert_eq!(read(root, "src/other.rs"), b"pub const X: u32 = 1;\n");
}

#[test]
fn conflicts_fail_closed_and_write_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);
    seed_two_files(root, &mut store);

    // A byte-identical duplicate of alpha: the text rebind finds two
    // context-identical candidates and refuses to guess.
    let handle = handle_at(&store, "@", "src/lib.rs", "fn alpha() -> u32 {\n    1\n}");
    let alpha_text = "fn alpha() -> u32 {\n    1\n}";
    let duplicated = format!("{alpha_text}\n{GOOD}");
    write(root, "src/lib.rs", duplicated.as_bytes());
    flush(&mut store, root, vec![touched(root, "src/lib.rs")]);

    let plan = store
        .plan_fragment_restore(std::slice::from_ref(&handle), FragmentMode::Replace)
        .unwrap();
    assert!(!plan.applicable());
    assert!(plan.is_noop());
    match &plan.conflicts[0].condition {
        FragmentCondition::Ambiguous => {
            assert!(plan.conflicts[0].candidates.len() >= 2);
        }
        other => panic!("expected ambiguous, got {other:?}"),
    }
    let err = store.apply_fragment_restore(&plan, &ignore).unwrap_err();
    assert!(matches!(err, sheaf_core::SheafError::RestoreObstructed(_)));
    assert_eq!(read(root, "src/lib.rs"), duplicated.as_bytes());

    // A deleted unit without --insert is `missing`, with the hint.
    write(
        root,
        "src/lib.rs",
        "fn beta() -> u32 {\n    2\n}\n".as_bytes(),
    );
    flush(&mut store, root, vec![touched(root, "src/lib.rs")]);
    let plan = store
        .plan_fragment_restore(std::slice::from_ref(&handle), FragmentMode::Replace)
        .unwrap();
    assert!(matches!(
        plan.conflicts[0].condition,
        FragmentCondition::Missing
    ));
    assert!(plan.conflicts[0].detail.contains("--insert"));

    // Insert mode against a present unit contradicts the destination state.
    write(root, "src/lib.rs", GOOD.as_bytes());
    flush(&mut store, root, vec![touched(root, "src/lib.rs")]);
    let plan = store
        .plan_fragment_restore(std::slice::from_ref(&handle), FragmentMode::Insert)
        .unwrap();
    assert!(matches!(
        plan.conflicts[0].condition,
        FragmentCondition::UnexpectedState
    ));

    // A duplicated SYMBOL surfaces as ambiguous through the parser seam,
    // even though a fingerprint match exists for both copies.
    let symbol_handle = symbol_handle_at(&store, "@", "src/lib.rs", "alpha");
    let text = read(root, "src/lib.rs");
    let text = String::from_utf8(text).unwrap();
    write(
        root,
        "src/lib.rs",
        format!("{text}{alpha_text}\n").as_bytes(),
    );
    flush(&mut store, root, vec![touched(root, "src/lib.rs")]);
    let plan = store
        .plan_fragment_restore(&[symbol_handle], FragmentMode::Delete)
        .unwrap();
    assert!(!plan.applicable());
    assert!(matches!(
        plan.conflicts[0].condition,
        FragmentCondition::Ambiguous
    ));
}

#[test]
fn live_edit_stales_the_token_but_unrelated_files_do_not() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);
    seed_two_files(root, &mut store);

    write(root, "src/lib.rs", WRECKED.as_bytes());
    flush(&mut store, root, vec![touched(root, "src/lib.rs")]);
    let handle = symbol_handle_at(&store, "@~1", "src/lib.rs", "alpha");

    let plan = store
        .plan_fragment_restore(std::slice::from_ref(&handle), FragmentMode::Replace)
        .unwrap();
    assert!(plan.applicable());

    // A live edit to the containing file after preview: token goes stale,
    // apply writes nothing.
    let wrecked_more = "fn alpha() -> u64 {\n    100\n}\nfn beta() -> u32 {\n    2\n}\n";
    write(root, "src/lib.rs", wrecked_more.as_bytes());
    let err = store.apply_fragment_restore(&plan, &ignore).unwrap_err();
    assert!(matches!(err, sheaf_core::SheafError::RestorePlanStale(_)));
    assert_eq!(read(root, "src/lib.rs"), wrecked_more.as_bytes());
    assert!(
        store.pending_restore().is_none(),
        "a rejected apply leaves no intent behind"
    );

    // Re-plan and confirm the edit is now part of the plan's old bytes.
    let plan = store
        .plan_fragment_restore(std::slice::from_ref(&handle), FragmentMode::Replace)
        .unwrap();
    assert!(plan.applicable());

    // An edit to an unrelated file does not stale the fragment token, and
    // the uncaptured edit becomes the undoable pre-restore capture.
    write(root, "src/other.rs", b"pub const X: u32 = 2;\n");
    let outcome = store.apply_fragment_restore(&plan, &ignore).unwrap();
    assert_eq!(String::from_utf8_lossy(&read(root, "src/lib.rs")), GOOD);
    assert_eq!(read(root, "src/other.rs"), b"pub const X: u32 = 2;\n");
    let pre_restore = store
        .capture_info(&outcome.pre_restore_capture.clone().unwrap())
        .unwrap();
    assert_eq!(
        pre_restore.capture.origin.map(|o| o.kind),
        Some(OriginKind::PreRestore)
    );
    assert_eq!(outcome.undo.capture_id, outcome.pre_restore_capture);
}

#[test]
fn insert_mode_reinserts_at_a_unique_deletion_scar() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);
    seed_two_files(root, &mut store);

    let handle = handle_at(&store, "@", "src/lib.rs", "fn alpha() -> u32 {\n    1\n}");

    // Delete exactly the selected extent: what remains is the deletion
    // scar — the joined before/after context (here: nothing + the rest).
    let scarred = "\nfn beta() -> u32 {\n    2\n}\n";
    write(root, "src/lib.rs", scarred.as_bytes());
    flush(&mut store, root, vec![touched(root, "src/lib.rs")]);

    let plan = store
        .plan_fragment_restore(std::slice::from_ref(&handle), FragmentMode::Insert)
        .unwrap();
    assert!(plan.applicable(), "conflicts: {:#?}", plan.conflicts);
    let action = &plan.files[0].actions[0];
    assert_eq!(action.kind, FragmentActionKind::Insert);
    assert_eq!(action.range.start, action.range.end);

    store.apply_fragment_restore(&plan, &ignore).unwrap();
    assert_eq!(String::from_utf8_lossy(&read(root, "src/lib.rs")), GOOD);

    // A scar occurring twice never authorizes a guess: two concatenated
    // copies of the scar text give exactly two placements.
    let two_scars = "\nfn beta() -> u32 {\n    2\n}\n\nfn beta() -> u32 {\n    2\n}\n";
    write(root, "src/lib.rs", two_scars.as_bytes());
    flush(&mut store, root, vec![touched(root, "src/lib.rs")]);
    let plan = store
        .plan_fragment_restore(&[handle], FragmentMode::Insert)
        .unwrap();
    assert!(!plan.applicable());
    assert!(matches!(
        plan.conflicts[0].condition,
        FragmentCondition::Ambiguous
    ));
    assert_eq!(plan.conflicts[0].candidates.len(), 2);
}

/// Build a line-extent handle over the line containing `needle`, exactly
/// the way `sheaf grep --extent line` cuts one: the extent excludes the
/// trailing newline.
fn line_handle_at(
    store: &ProjectStore,
    reference: &str,
    path: &str,
    needle: &str,
) -> sheaf_core::store::SelectionHandle {
    use sheaf_core::store::{HistoricalPathContent, SelectionExtent, SelectionHandle};
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
        sheaf_core::store::ByteRange { start, end },
        &text,
        format!("literal:{needle}"),
        None,
    )
    .unwrap()
}

#[test]
fn insert_mode_reinserts_a_whole_deleted_line() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);
    seed_two_files(root, &mut store);

    // A line-extent handle over beta's signature line: the extent is the
    // line WITHOUT its trailing newline.
    let handle = line_handle_at(&store, "@", "src/lib.rs", "fn beta() -> u32 {");

    // A normal editor line deletion takes the newline with the line, so
    // the scar is the joined before-context and after-context-minus-its
    // newline — not the exact-extent join.
    let deleted = "fn alpha() -> u32 {\n    1\n}\n    2\n}\n";
    write(root, "src/lib.rs", deleted.as_bytes());
    flush(&mut store, root, vec![touched(root, "src/lib.rs")]);

    let plan = store
        .plan_fragment_restore(std::slice::from_ref(&handle), FragmentMode::Insert)
        .unwrap();
    assert!(plan.applicable(), "conflicts: {:#?}", plan.conflicts);
    let action = &plan.files[0].actions[0];
    assert_eq!(action.kind, FragmentActionKind::Insert);
    assert_eq!(action.range.start, action.range.end);
    assert!(
        action.line_glue,
        "the whole-line scar must glue the terminator to the splice"
    );
    assert_eq!(action.new_bytes, "fn beta() -> u32 {".len() + 1);

    store.apply_fragment_restore(&plan, &ignore).unwrap();
    assert_eq!(String::from_utf8_lossy(&read(root, "src/lib.rs")), GOOD);
}

#[test]
fn insert_mode_refuses_when_both_scar_variants_match() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    seed_two_files(root, &mut store);

    let handle = line_handle_at(&store, "@", "src/lib.rs", "fn beta() -> u32 {");
    // One copy of the whole-line scar (newline went with the line) and
    // one copy of the exact scar (newline stayed): the destination is
    // consistent with two different deletions, so insert refuses rather
    // than pick either.
    let both = "fn alpha() -> u32 {\n    1\n}\n    2\n}\n\
                fn alpha() -> u32 {\n    1\n}\n\n    2\n}\n";
    write(root, "src/lib.rs", both.as_bytes());
    flush(&mut store, root, vec![touched(root, "src/lib.rs")]);

    let plan = store
        .plan_fragment_restore(std::slice::from_ref(&handle), FragmentMode::Insert)
        .unwrap();
    assert!(!plan.applicable());
    assert!(matches!(
        plan.conflicts[0].condition,
        FragmentCondition::Ambiguous
    ));
    assert_eq!(plan.conflicts[0].candidates.len(), 2);
}

#[test]
fn delete_mode_removes_exactly_the_unit() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);
    seed_two_files(root, &mut store);

    let handle = handle_at(&store, "@", "src/lib.rs", "fn alpha() -> u32 {\n    1\n}");
    let plan = store
        .plan_fragment_restore(&[handle], FragmentMode::Delete)
        .unwrap();
    assert!(plan.applicable());
    let action = &plan.files[0].actions[0];
    assert_eq!(action.kind, FragmentActionKind::Delete);
    assert_eq!(action.new_bytes, 0);

    store.apply_fragment_restore(&plan, &ignore).unwrap();
    // Exactly the selected bytes are gone; beta and all whitespace outside
    // the extent survive untouched.
    let expected = {
        let start = GOOD.find("fn alpha").unwrap();
        let end = start + "fn alpha() -> u32 {\n    1\n}".len();
        format!("{}{}", &GOOD[..start], &GOOD[end..])
    };
    assert_eq!(String::from_utf8_lossy(&read(root, "src/lib.rs")), expected);
}

#[test]
fn rebind_follows_renames_to_the_new_path() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);
    seed_two_files(root, &mut store);

    let handle = symbol_handle_at(&store, "@", "src/lib.rs", "alpha");

    // First-class rename through the batch engine, then wreck the function
    // at its new name.
    std::fs::rename(root.join("src/lib.rs"), root.join("src/renamed.rs")).unwrap();
    flush(
        &mut store,
        root,
        vec![FsEvent::now(EventKind::Renamed {
            from: root.join("src/lib.rs"),
            to: root.join("src/renamed.rs"),
        })],
    );
    write(root, "src/renamed.rs", WRECKED.as_bytes());
    flush(&mut store, root, vec![touched(root, "src/renamed.rs")]);

    let plan = store
        .plan_fragment_restore(&[handle], FragmentMode::Replace)
        .unwrap();
    assert!(plan.applicable(), "conflicts: {:#?}", plan.conflicts);
    assert_eq!(plan.files[0].path, "src/renamed.rs");
    store.apply_fragment_restore(&plan, &ignore).unwrap();
    assert_eq!(String::from_utf8_lossy(&read(root, "src/renamed.rs")), GOOD);
    assert!(!root.join("src/lib.rs").exists());
}

#[test]
fn branch_source_restores_into_the_current_lineage() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);
    seed_two_files(root, &mut store);

    // Wreck alpha, capture, then rewind the whole tree: the wrecked capture
    // lands on an abandoned branch.
    write(root, "src/lib.rs", WRECKED.as_bytes());
    flush(&mut store, root, vec![touched(root, "src/lib.rs")]);
    let branch_capture = store.resolve("@").unwrap().capture_id.unwrap();
    let good_id = store.resolve("@~1").unwrap().capture_id.unwrap();
    let rewind = store
        .plan_restore(
            &good_id, // whole tree → repositions head; edits diverge from here
            &[],
            &ignore,
        )
        .unwrap();
    store.apply_restore(&rewind, &ignore).unwrap();

    // Wreck alpha differently on the current lineage.
    let second_wreck = "fn alpha() -> i128 {\n    7\n}\nfn beta() -> u32 {\n    2\n}\n";
    write(root, "src/lib.rs", second_wreck.as_bytes());
    flush(&mut store, root, vec![touched(root, "src/lib.rs")]);

    // A handle whose source frontier is the abandoned branch's capture.
    let handle = symbol_handle_at(&store, &branch_capture, "src/lib.rs", "alpha");
    let plan = store
        .plan_fragment_restore(&[handle], FragmentMode::Replace)
        .unwrap();
    assert!(plan.applicable(), "conflicts: {:#?}", plan.conflicts);
    store.apply_fragment_restore(&plan, &ignore).unwrap();
    // The branch's fragment landed on the current lineage's file.
    assert_eq!(
        String::from_utf8_lossy(&read(root, "src/lib.rs")),
        "fn alpha() -> u64 {\n    99\n}\nfn beta() -> u32 {\n    2\n}\n"
    );
}

#[test]
fn shallow_retention_reads_retained_sources_and_refuses_pruned() {
    use sheaf_core::store::{gc_plan, gc_run_store, GcOutcome};
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);

    // v1 (ancient), v2 (checkpoint-pinned), v3 (young, live) — then trim
    // below the pinned boundary so the document goes shallow.
    let hour = Duration::hours(1);
    let v1 = b"fn alpha() -> u32 {\n    1\n}\nfn beta() {}\n";
    let v2 = b"fn alpha() -> u32 {\n    11\n}\nfn beta() {}\n";
    let v3 = b"fn alpha() -> u64 {\n    99\n}\nfn beta() {}\n";
    flush_aged(&mut store, root, "src/lib.rs", v1, hour * 5);
    let v1_id = store.resolve("@").unwrap().capture_id.unwrap();
    let v1_handle = symbol_handle_at(&store, "@", "src/lib.rs", "alpha");
    flush_aged(&mut store, root, "src/lib.rs", v2, hour * 4);
    store.create_checkpoint("pin", None).unwrap();
    flush_aged(&mut store, root, "src/lib.rs", v3, Duration::zero());

    config::set_retention_expiry(root, "2h").unwrap();
    let report = match gc_run_store(&mut store, true).unwrap() {
        GcOutcome::Applied(report) => report,
        other => panic!("expected applied gc, got {other:?}"),
    };
    assert_eq!(report.trimmed, 1, "the pre-pin capture is reclaimed");
    let _ = gc_plan(root).unwrap();
    drop(store);

    // On the shallow store, a handle bound at the retained pinned point
    // still plans and applies byte-exactly (state-only snapshot round trip).
    let mut store = open(root);
    store.resolve("checkpoint:pin").unwrap();
    let handle = symbol_handle_at(&store, "checkpoint:pin", "src/lib.rs", "alpha");
    let plan = store
        .plan_fragment_restore(std::slice::from_ref(&handle), FragmentMode::Replace)
        .unwrap();
    assert!(plan.applicable(), "conflicts: {:#?}", plan.conflicts);
    store.apply_fragment_restore(&plan, &ignore).unwrap();
    assert_eq!(
        read(root, "src/lib.rs"),
        b"fn alpha() -> u32 {\n    11\n}\nfn beta() {}\n"
    );

    // A handle whose source frontier was pruned surfaces as a typed
    // unsupported-source conflict — never fabricated content — and the
    // pruned reference itself explains why it no longer resolves.
    let pruned_point = store.resolve(&v1_id).unwrap_err();
    assert!(
        format!("{pruned_point}").contains("pruned"),
        "pruned reference explains itself: {pruned_point}"
    );
    let plan = store
        .plan_fragment_restore(&[v1_handle], FragmentMode::Replace)
        .unwrap();
    assert!(!plan.applicable());
    assert!(
        matches!(
            plan.conflicts[0].condition,
            FragmentCondition::UnsupportedSource
        ),
        "conflict: {:#?}",
        plan.conflicts[0].condition
    );
}

fn flush_aged(store: &mut ProjectStore, root: &Path, rel: &str, bytes: &[u8], age: Duration) {
    write(root, rel, bytes);
    let now = Utc::now();
    let events = vec![FsEvent {
        at: now - age,
        kind: EventKind::Touched {
            path: sheaf_core::events::TouchedPath(root.join(rel)),
        },
    }];
    let batch = Batch {
        root: root.to_path_buf(),
        events,
        started_at: now - age,
        flushed_at: now - age,
    };
    store.apply_batch(&batch).unwrap();
}

#[test]
fn crash_resume_converges_from_every_intent_boundary() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);

    // Two files, each with one wrecked extent: the plan spans two files so
    // a crash between installs is representable.
    let good_a = "fn alpha() -> u32 {\n    1\n}\nfn beta() {}\n";
    let good_b = "pub fn bee() -> u8 {\n    2\n}\n";
    write(root, "a.rs", good_a.as_bytes());
    write(root, "b.rs", good_b.as_bytes());
    flush(
        &mut store,
        root,
        vec![added(root, "a.rs"), added(root, "b.rs")],
    );

    write(
        root,
        "a.rs",
        b"fn alpha() -> u64 {\n    9\n}\nfn beta() {}\n",
    );
    write(root, "b.rs", b"pub fn bee() -> u8 {\n    3\n}\n");
    flush(
        &mut store,
        root,
        vec![touched(root, "a.rs"), touched(root, "b.rs")],
    );

    let handle_a = symbol_handle_at(&store, "@~1", "a.rs", "alpha");
    let handle_b = symbol_handle_at(&store, "@~1", "b.rs", "bee");
    let plan = store
        .plan_fragment_restore(&[handle_a, handle_b], FragmentMode::Replace)
        .unwrap();
    assert!(plan.applicable());
    assert_eq!(plan.files.len(), 2);
    let paths: Vec<&String> = plan.files.iter().map(|f| &f.path).collect();
    assert_eq!(paths, vec!["a.rs", "b.rs"]);

    // Simulate a crash after the FIRST file's install: hand-write the
    // durable intent, land file a at its planned result, leave b at pre.
    let intent = RestoreIntent {
        token: plan.token.clone(),
        mode: RestoreMode::Fragment,
        scope: plan.destination_paths(),
        target: plan.base.clone(),
        started_ms: Utc::now().timestamp_millis(),
        fragment: Some(Box::new(plan.clone())),
    };
    std::fs::create_dir_all(root.join(".sheaf/state")).unwrap();
    std::fs::write(
        root.join(".sheaf/state/restore.intent"),
        serde_json::to_vec_pretty(&intent).unwrap(),
    )
    .unwrap();
    let a_plan = &plan.files[0];
    write(root, "a.rs", &splice_result(root, a_plan, &store));
    drop(store);

    // Fresh writer: auto-resume drives b to its planned result, skips a
    // (already there), clears the intent, and records the forward capture.
    let mut resumed = open(root);
    let outcome = resumed
        .resume_restore(
            &ignore,
            false,
            config::RestoreConfig::default().max_resume_age_ms,
        )
        .unwrap()
        .expect("fresh fragment intent auto-resumes");
    assert!(outcome.resumed);
    assert_eq!(String::from_utf8_lossy(&read(root, "a.rs")), good_a);
    assert_eq!(String::from_utf8_lossy(&read(root, "b.rs")), good_b);
    assert!(resumed.pending_restore().is_none());
    assert!(outcome.restore_capture.is_some());

    // The untouched-pre-intent boundary: plant a fresh intent against a
    // never-applied state and resume converges on the complete splice.
    write(root, "b.rs", b"pub fn bee() -> u8 {\n    3\n}\n");
    flush(&mut resumed, root, vec![touched(root, "b.rs")]);
    let handle_b2 = symbol_handle_at(&resumed, "@~1", "b.rs", "bee");
    let plan2 = resumed
        .plan_fragment_restore(&[handle_b2], FragmentMode::Replace)
        .unwrap();
    let intent2 = RestoreIntent {
        token: plan2.token.clone(),
        mode: RestoreMode::Fragment,
        scope: plan2.destination_paths(),
        target: plan2.base.clone(),
        started_ms: Utc::now().timestamp_millis(),
        fragment: Some(Box::new(plan2.clone())),
    };
    std::fs::write(
        root.join(".sheaf/state/restore.intent"),
        serde_json::to_vec_pretty(&intent2).unwrap(),
    )
    .unwrap();
    drop(resumed);
    let mut again = open(root);
    again
        .resume_restore(
            &ignore,
            false,
            config::RestoreConfig::default().max_resume_age_ms,
        )
        .unwrap()
        .expect("second intent resumes");
    assert_eq!(String::from_utf8_lossy(&read(root, "b.rs")), good_b);
    assert!(again.pending_restore().is_none());

    // Staleness still gates fragment intents exactly like whole-tree ones.
    let stale = RestoreIntent {
        token: "tok".into(),
        mode: RestoreMode::Fragment,
        scope: vec![],
        target: again.resolve("@").unwrap(),
        started_ms: (Utc::now() - Duration::days(30)).timestamp_millis(),
        fragment: None, // staleness is decided before the payload matters
    };
    std::fs::write(
        root.join(".sheaf/state/restore.intent"),
        serde_json::to_vec_pretty(&stale).unwrap(),
    )
    .unwrap();
    let outcome = again
        .resume_restore(
            &ignore,
            false,
            config::RestoreConfig::default().max_resume_age_ms,
        )
        .unwrap();
    assert!(
        outcome.is_none(),
        "stale fragment intents do not auto-replay"
    );
}

/// Apply a file plan's splices by hand — the crash fixture's "what the
/// daemon would have written" oracle.
fn splice_result(
    root: &Path,
    file: &sheaf_core::store::FragmentFilePlan,
    store: &ProjectStore,
) -> Vec<u8> {
    use sheaf_core::store::HistoricalPathContent;
    let mut out = read(root, &file.path);
    for action in &file.actions {
        let source_ref = action
            .handle
            .source_capture_id
            .as_deref()
            .expect("test handles carry their capture id");
        let text = match store
            .historical_path_content(source_ref, &action.handle.historical_path)
            .unwrap()
        {
            HistoricalPathContent::Text(text) => text,
            other => panic!("source unreadable: {other:?}"),
        };
        let new = text.as_bytes()[action.handle.range.start..action.handle.range.end].to_vec();
        out.splice(action.range.start..action.range.end, new);
    }
    out
}

#[test]
fn degraded_plan_matches_the_live_plan() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);
    seed_two_files(root, &mut store);
    write(root, "src/lib.rs", WRECKED.as_bytes());
    flush(&mut store, root, vec![touched(root, "src/lib.rs")]);
    let handle = symbol_handle_at(&store, "@~1", "src/lib.rs", "alpha");

    let live = store
        .plan_fragment_restore(std::slice::from_ref(&handle), FragmentMode::Replace)
        .unwrap();
    drop(store);

    let reader = TimelineReader::open(root).unwrap();
    let degraded = reader
        .plan_fragment_restore(&[handle], FragmentMode::Replace)
        .unwrap();
    assert!(degraded.degraded);
    assert!(!live.degraded);
    assert_eq!(live.token, degraded.token);
    assert_eq!(live.files, degraded.files);
    assert_eq!(live.conflicts, degraded.conflicts);
    let _ = ignore;
}

/// Selection provenance is additive: an origin without `selections` (the
/// older, pre-fragment-restore shape) still deserializes.
#[test]
fn capture_origin_selections_field_is_additive() {
    let legacy = serde_json::json!({
        "kind": "restore",
        "target": "abc",
        "scope": ["src"]
    });
    let origin: CaptureOrigin = serde_json::from_value(legacy).unwrap();
    assert_eq!(origin.kind, OriginKind::Restore);
    assert!(origin.selections.is_empty());
}
