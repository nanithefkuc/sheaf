//! Restore engine behaviour on real trees.
//!
//! The bar is concrete — "a fat-fingered refactor afternoon
//! is fully recovered on a real tree, chars included" and "restores never
//! corrupt or trim the log" — so these tests wreck a populated worktree and
//! assert byte-exact recovery plus intact history.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use chrono::Utc;
use sheaf_core::config;
use sheaf_core::events::{Batch, EventKind, FsEvent};
use sheaf_core::ignore::IgnoreSet;
use sheaf_core::store::{
    Obstacle, OriginKind, ProjectStore, RestoreIntent, RestoreMode, StoreLimits, TimelineReader,
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

fn write(root: &Path, rel: &str, bytes: &[u8]) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, bytes).unwrap();
}

/// Every non-store file in the tree, keyed by root-relative path.
fn snapshot_tree(root: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut out = BTreeMap::new();
    for entry in walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(std::result::Result::ok)
    {
        if !entry.file_type().is_file() {
            continue;
        }
        let rel = entry.path().strip_prefix(root).unwrap();
        if rel.starts_with(".sheaf") {
            continue;
        }
        out.insert(
            rel.to_string_lossy().replace('\\', "/"),
            std::fs::read(entry.path()).unwrap(),
        );
    }
    out
}

/// The afternoon's worth of work every test starts from.
fn seed(root: &Path) {
    write(
        root,
        "src/lib.rs",
        b"pub fn greet() -> &'static str {\n    \"h\xc3\xa9llo w\xc3\xb6rld \xf0\x9f\x8c\x8d\"\n}\n",
    );
    write(root, "src/util/mod.rs", b"pub mod strings;\n");
    write(root, "src/util/strings.rs", b"pub fn trim() {}\n");
    write(root, "README.md", "# project\n\nnotes\n".as_bytes());
    write(
        root,
        "assets/logo.bin",
        &[0xff, 0xfe, 0x00, 0x93, 0x94, 0x01],
    );
}

fn open(root: &Path) -> ProjectStore {
    ProjectStore::open(root, limits()).unwrap()
}

#[test]
fn full_restore_recovers_a_fat_fingered_afternoon_byte_exact() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    let ignore = ignores();

    seed(root);
    let good = store.reconcile_worktree(&ignore).unwrap().unwrap();
    let good_tree = snapshot_tree(root);

    // The afternoon: a multibyte edit, a botched rename, a deletion, a new
    // file, and a corrupted binary.
    write(
        root,
        "src/lib.rs",
        b"pub fn greet() -> &'static str {\n    \"OOPS\"\n}\n",
    );
    std::fs::rename(root.join("src/util/strings.rs"), root.join("src/strs.rs")).unwrap();
    std::fs::remove_file(root.join("README.md")).unwrap();
    write(root, "src/scratch.rs", b"// half-finished\n");
    write(root, "assets/logo.bin", &[0x00, 0x00]);
    let wrecked = store.reconcile_worktree(&ignore).unwrap().unwrap();
    let wrecked_tree = snapshot_tree(root);
    assert_ne!(good_tree, wrecked_tree);

    let plan = store.plan_restore(good.short_id(), &[], &ignore).unwrap();
    assert_eq!(plan.mode, RestoreMode::Full);
    assert!(plan.applicable() && !plan.is_noop());
    let before_plan = snapshot_tree(root);
    assert_eq!(
        before_plan, wrecked_tree,
        "planning must not touch the tree"
    );

    let outcome = store.apply_restore(&plan, &ignore).unwrap();
    assert_eq!(outcome.mode, RestoreMode::Full);
    assert_eq!(snapshot_tree(root), good_tree, "tree recovered byte-exact");
    assert_eq!(outcome.result.frontier, plan.target.frontier);
    // Empty directories left by the restore are cleaned up, not orphaned.
    assert!(root.join("src/util").is_dir());

    // Nothing was trimmed: both earlier points still resolve exactly.
    assert_eq!(
        store.resolve(good.short_id()).unwrap().frontier,
        good.frontier
    );
    assert_eq!(
        store.resolve(wrecked.short_id()).unwrap().frontier,
        wrecked.frontier
    );

    // The undo reference returns the afternoon, wreckage and all.
    let undo = store
        .plan_restore(&outcome.undo.capture_id.clone().unwrap(), &[], &ignore)
        .unwrap();
    store.apply_restore(&undo, &ignore).unwrap();
    assert_eq!(snapshot_tree(root), wrecked_tree, "restore is reversible");
}

#[test]
fn full_restore_diverges_and_keeps_the_abandoned_future_reachable() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);

    seed(root);
    let good = store.reconcile_worktree(&ignore).unwrap().unwrap();
    write(root, "src/lib.rs", b"pub fn greet() {}\n// abandoned\n");
    let abandoned = store.reconcile_worktree(&ignore).unwrap().unwrap();

    let plan = store.plan_restore(good.short_id(), &[], &ignore).unwrap();
    store.apply_restore(&plan, &ignore).unwrap();
    assert_eq!(
        store.branch_tips().unwrap().len(),
        1,
        "restore alone authors nothing"
    );

    // New work after the rollback becomes an implicit divergence branch.
    write(
        root,
        "src/lib.rs",
        b"pub fn greet() {}\n// the new future\n",
    );
    let divergent = store.reconcile_worktree(&ignore).unwrap().unwrap();
    assert_eq!(store.branch_tips().unwrap().len(), 2);
    assert_eq!(
        store.resolve(abandoned.short_id()).unwrap().frontier,
        abandoned.frontier,
        "the overwritten work stays addressable"
    );
    let lineage = store.captures(false, None, false, 50).unwrap();
    assert!(lineage.iter().any(|c| c.id == divergent.id));
    assert!(!lineage.iter().any(|c| c.id == abandoned.id));
    drop(store);

    // Reopening must not silently merge the abandoned future back in: the
    // head file names the lineage the worktree actually holds.
    let mut reopened = open(root);
    let lineage = reopened.captures(false, None, false, 50).unwrap();
    assert!(lineage.iter().any(|c| c.id == divergent.id));
    assert!(!lineage.iter().any(|c| c.id == abandoned.id));
    write(
        root,
        "src/lib.rs",
        b"pub fn greet() {}\n// still on the branch\n",
    );
    let further = reopened.reconcile_worktree(&ignore).unwrap().unwrap();
    assert_eq!(
        reopened.captures(false, None, false, 2).unwrap()[1].id,
        divergent.id,
        "the capture after reopen continues the restored lineage"
    );
    assert_eq!(reopened.branch_tips().unwrap().len(), 2);
    assert!(reopened.resolve(further.short_id()).is_ok());
}

#[test]
fn scoped_restore_appends_one_forward_capture_and_spares_the_rest() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);

    seed(root);
    let good = store.reconcile_worktree(&ignore).unwrap().unwrap();
    let original_lib = std::fs::read(root.join("src/lib.rs")).unwrap();

    write(
        root,
        "src/lib.rs",
        b"pub fn greet() {\n    unimplemented!()\n}\n",
    );
    write(root, "README.md", b"# project\n\nkeep this new note\n");
    store.reconcile_worktree(&ignore).unwrap().unwrap();
    let readme_now = std::fs::read(root.join("README.md")).unwrap();

    let plan = store
        .plan_restore(good.short_id(), &["src/lib.rs".to_string()], &ignore)
        .unwrap();
    assert_eq!(plan.mode, RestoreMode::Scoped);
    assert_eq!(plan.actions.len(), 1, "only the scoped path is touched");
    assert_eq!(plan.actions[0].path, "src/lib.rs");
    assert!(plan.actions[0].local_modified.eq(&false));

    let outcome = store.apply_restore(&plan, &ignore).unwrap();
    assert_eq!(
        std::fs::read(root.join("src/lib.rs")).unwrap(),
        original_lib
    );
    assert_eq!(
        std::fs::read(root.join("README.md")).unwrap(),
        readme_now,
        "out-of-scope work is untouched"
    );
    assert!(outcome.restore_capture.is_some());
    assert_eq!(
        store.branch_tips().unwrap().len(),
        1,
        "a scoped restore is forward work, not a divergence"
    );
    let head = store.captures(false, None, false, 1).unwrap();
    assert_eq!(head[0].id, outcome.restore_capture.unwrap());
    assert_eq!(head[0].paths, vec!["src/lib.rs".to_string()]);
}

#[test]
fn scoped_restore_replays_a_move_as_a_first_class_rename() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);

    seed(root);
    store.reconcile_worktree(&ignore).unwrap().unwrap();
    // Move the file, capture the move, then restore the subtree.
    std::fs::rename(
        root.join("src/util/strings.rs"),
        root.join("src/util/text.rs"),
    )
    .unwrap();
    let moved = store.reconcile_worktree(&ignore).unwrap().unwrap();

    let plan = store
        .plan_restore(moved.short_id(), &["src/util".to_string()], &ignore)
        .unwrap();
    assert!(
        plan.is_noop(),
        "the worktree already holds the moved layout"
    );

    // Now undo the move through a restore of the subtree.
    let before_move = store.captures(false, None, false, 2).unwrap()[1].clone();
    let plan = store
        .plan_restore(before_move.short_id(), &["src/util".to_string()], &ignore)
        .unwrap();
    store.apply_restore(&plan, &ignore).unwrap();
    assert!(root.join("src/util/strings.rs").is_file());
    assert!(!root.join("src/util/text.rs").exists());

    let renames: Vec<_> = store
        .tree_events()
        .into_iter()
        .filter(|e| e["event"]["kind"] == "renamed")
        .collect();
    let last = renames
        .last()
        .expect("restore recorded a structural rename");
    assert_eq!(last["event"]["from"], "src/util/text.rs");
    assert_eq!(last["event"]["to"], "src/util/strings.rs");
}

#[test]
fn binaries_restore_from_content_addressed_blobs() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);

    let payload: Vec<u8> = (0u8..=255).cycle().take(9000).collect();
    write(root, "assets/logo.bin", &payload);
    let good = store.reconcile_worktree(&ignore).unwrap().unwrap();
    std::fs::remove_file(root.join("assets/logo.bin")).unwrap();
    store.reconcile_worktree(&ignore).unwrap().unwrap();

    let plan = store.plan_restore(good.short_id(), &[], &ignore).unwrap();
    store.apply_restore(&plan, &ignore).unwrap();
    assert_eq!(
        std::fs::read(root.join("assets/logo.bin")).unwrap(),
        payload
    );
}

#[test]
fn a_missing_blob_blocks_the_plan_instead_of_writing_half_a_tree() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);

    write(root, "keep.txt", b"kept\n");
    write(root, "assets/logo.bin", &[0xff, 0xfe, 0x00, 0x01]);
    let good = store.reconcile_worktree(&ignore).unwrap().unwrap();
    std::fs::remove_file(root.join("assets/logo.bin")).unwrap();
    write(root, "keep.txt", b"changed\n");
    store.reconcile_worktree(&ignore).unwrap().unwrap();

    // Simulate blob loss (the on-disk layout has no blob GC, but disks lie).
    let blobs = root.join(".sheaf/store/blobs");
    for entry in walkdir::WalkDir::new(&blobs)
        .into_iter()
        .filter_map(std::result::Result::ok)
    {
        if entry.file_type().is_file() {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }

    let plan = store.plan_restore(good.short_id(), &[], &ignore).unwrap();
    assert!(!plan.applicable());
    assert_eq!(plan.obstructions[0].obstacle, Obstacle::MissingBlob);
    let err = store.apply_restore(&plan, &ignore).unwrap_err();
    assert_eq!(err.code(), "restore.obstructed");
    assert_eq!(
        std::fs::read(root.join("keep.txt")).unwrap(),
        b"changed\n",
        "a blocked restore writes nothing at all"
    );
}

#[test]
fn a_directory_in_the_way_blocks_the_path_it_occupies() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);

    write(root, "notes", b"a file today\n");
    let good = store.reconcile_worktree(&ignore).unwrap().unwrap();
    std::fs::remove_file(root.join("notes")).unwrap();
    write(root, "notes/inner.txt", b"a directory tomorrow\n");
    store.reconcile_worktree(&ignore).unwrap().unwrap();

    let plan = store.plan_restore(good.short_id(), &[], &ignore).unwrap();
    assert_eq!(plan.obstructions[0].path, "notes");
    assert_eq!(plan.obstructions[0].obstacle, Obstacle::DirectoryInTheWay);
    assert!(store.apply_restore(&plan, &ignore).is_err());
}

#[test]
fn a_plan_whose_worktree_moved_underneath_it_fails_closed() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);

    write(root, "a.txt", b"one\n");
    let good = store.reconcile_worktree(&ignore).unwrap().unwrap();
    write(root, "a.txt", b"two\n");
    store.reconcile_worktree(&ignore).unwrap().unwrap();
    let plan = store.plan_restore(good.short_id(), &[], &ignore).unwrap();

    // Someone keeps typing between plan and apply.
    write(root, "b.txt", b"brand new\n");
    let err = store.apply_restore(&plan, &ignore).unwrap_err();
    assert_eq!(err.code(), "restore.plan_stale");
    assert_eq!(std::fs::read(root.join("a.txt")).unwrap(), b"two\n");

    // Uncaptured edits alone do not invalidate a plan: apply captures them
    // first, and that safety capture must not fight its own token.
    write(root, "b.txt", b"brand new\n");
    let plan = store.plan_restore(good.short_id(), &[], &ignore).unwrap();
    write(root, "b.txt", b"brand new\n");
    let outcome = store.apply_restore(&plan, &ignore).unwrap();
    assert_eq!(std::fs::read(root.join("a.txt")).unwrap(), b"one\n");
    assert!(outcome.pre_restore_capture.is_some());
}

#[test]
fn an_interrupted_restore_resumes_to_exactly_the_same_worktree() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);

    seed(root);
    let good = store.reconcile_worktree(&ignore).unwrap().unwrap();
    let good_tree = snapshot_tree(root);
    write(root, "src/lib.rs", b"pub fn greet() {}\n");
    std::fs::remove_file(root.join("README.md")).unwrap();
    write(root, "src/scratch.rs", b"// junk\n");
    store.reconcile_worktree(&ignore).unwrap().unwrap();

    let plan = store.plan_restore(good.short_id(), &[], &ignore).unwrap();

    // Fake a `kill -9` in the middle of apply: the intent is durable, one
    // file landed, the rest did not.
    let intent = RestoreIntent {
        token: plan.token.clone(),
        mode: plan.mode,
        scope: plan.scope.clone(),
        target: plan.target.clone(),
        started_ms: chrono::Utc::now().timestamp_millis(),
        fragment: None,
    };
    std::fs::write(
        root.join(".sheaf/state/restore.intent"),
        serde_json::to_vec_pretty(&intent).unwrap(),
    )
    .unwrap();
    write(root, "src/lib.rs", &good_tree["src/lib.rs"]);
    drop(store);

    let mut restarted = open(root);
    assert!(restarted.pending_restore().is_some());
    let outcome = restarted
        .resume_restore(
            &ignore,
            false,
            sheaf_core::config::RestoreConfig::default().max_resume_age_ms,
        )
        .unwrap()
        .expect("an outstanding fresh intent resumes");
    assert!(outcome.resumed);
    assert_eq!(snapshot_tree(root), good_tree);
    assert!(restarted.pending_restore().is_none());
    // Resuming again is a no-op, not a second restore.
    assert!(restarted
        .resume_restore(
            &ignore,
            false,
            sheaf_core::config::RestoreConfig::default().max_resume_age_ms
        )
        .unwrap()
        .is_none());
}

#[test]
fn a_full_restore_plans_untracked_files_it_will_remove_and_spares_ignored_ones() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);

    write(root, "a.txt", b"one\n");
    let good = store.reconcile_worktree(&ignore).unwrap().unwrap();

    // Never captured, born after the target point.
    write(root, "notes/scratch.md", b"typed just now\n");
    // Ignored by config: restore has no business touching build output.
    write(root, "target/debug/artifact", b"build output\n");

    let plan = store.plan_restore(good.short_id(), &[], &ignore).unwrap();
    let planned: Vec<&str> = plan.actions.iter().map(|a| a.path.as_str()).collect();
    assert_eq!(planned, vec!["notes/scratch.md"], "a dry-run hides nothing");
    assert!(plan.actions[0].local_modified, "it is uncaptured work");

    let outcome = store.apply_restore(&plan, &ignore).unwrap();
    assert!(!root.join("notes/scratch.md").exists());
    assert!(!root.join("notes").exists(), "emptied directory is pruned");
    assert!(root.join("target/debug/artifact").is_file());

    // Uncaptured work is never lost: it lives in the pre-restore capture.
    let saved = outcome.pre_restore_capture.expect("safety capture");
    let back = store.plan_restore(&saved, &[], &ignore).unwrap();
    store.apply_restore(&back, &ignore).unwrap();
    assert_eq!(
        std::fs::read(root.join("notes/scratch.md")).unwrap(),
        b"typed just now\n"
    );
}

#[test]
fn a_scoped_restore_declares_itself_in_the_timeline() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);

    write(root, "a.txt", b"one\n");
    write(root, "b.txt", b"untouched\n");
    let good = store.reconcile_worktree(&ignore).unwrap().unwrap();
    write(root, "a.txt", b"two\n");
    store.reconcile_worktree(&ignore).unwrap().unwrap();

    let plan = store
        .plan_restore(good.short_id(), &["a.txt".to_string()], &ignore)
        .unwrap();
    store.apply_restore(&plan, &ignore).unwrap();

    // It stays on the current lineage — but the capture says what it is, so
    // the log never presents a rollback as ordinary typing.
    assert_eq!(store.branch_tips().unwrap().len(), 1);
    let head = store.captures(false, None, false, 1).unwrap()[0].clone();
    let origin = head.origin.expect("restore captures carry provenance");
    assert_eq!(origin.kind, OriginKind::Restore);
    assert_eq!(origin.target.as_deref(), Some(good.id.as_str()));
    assert_eq!(origin.scope, vec!["a.txt".to_string()]);

    // Ordinary watcher work stays unmarked.
    write(root, "b.txt", b"typed by hand\n");
    let plain = store.reconcile_worktree(&ignore).unwrap().unwrap();
    assert!(plain.origin.is_none());
}

/// The window this closes is genuinely concurrent, so a single-threaded test
/// cannot land an edit *between* apply's safety capture and a given path's
/// turn in the install loop. What it can prove is the promise both mechanisms
/// exist to keep: bytes that were on disk and in no capture when a restore
/// began are recoverable afterwards, and they are labelled as such. The live
/// concurrent-writer case is exercised in `scripts/e2e_restore.sh`.
#[test]
fn bytes_a_restore_overwrites_are_captured_and_labelled_first() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);

    write(root, "a.txt", b"one\n");
    let good = store.reconcile_worktree(&ignore).unwrap().unwrap();
    write(root, "a.txt", b"two\n");
    store.reconcile_worktree(&ignore).unwrap().unwrap();

    let plan = store
        .plan_restore(good.short_id(), &["a.txt".to_string()], &ignore)
        .unwrap();
    // Stand in for a save that lands after apply's safety capture but before
    // this path's turn in the install loop: the bytes exist only on disk.
    write(root, "a.txt", b"typed while the restore was running\n");
    let outcome = store.apply_restore(&plan, &ignore).unwrap();

    assert_eq!(std::fs::read(root.join("a.txt")).unwrap(), b"one\n");
    let rescued = store
        .captures(false, None, false, 20)
        .unwrap()
        .into_iter()
        .find(|c| {
            c.origin
                .as_ref()
                .is_some_and(|o| o.kind == OriginKind::PreRestore)
        })
        .expect("the concurrent edit became a capture");
    let back = store
        .plan_restore(rescued.short_id(), &["a.txt".to_string()], &ignore)
        .unwrap();
    store.apply_restore(&back, &ignore).unwrap();
    assert_eq!(
        std::fs::read(root.join("a.txt")).unwrap(),
        b"typed while the restore was running\n",
        "bytes overwritten mid-restore stay recoverable"
    );
    assert!(outcome
        .progress_log
        .iter()
        .any(|line| line.contains("pre-restore")));
    assert_eq!(rescued.origin.unwrap().kind, OriginKind::PreRestore);
}

#[test]
fn structural_echoes_that_mean_nothing_never_become_captures() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);

    write(root, "src/a.txt", b"one\n");
    store.reconcile_worktree(&ignore).unwrap().unwrap();
    let before = store.captures(false, None, false, 50).unwrap().len();

    // Exactly what the watcher reports while a restore installs and prunes:
    // a bare directory create, and the removal of a directory the document
    // never modelled. Neither is history.
    std::fs::create_dir_all(root.join("src/fresh")).unwrap();
    let now = chrono::Utc::now();
    let outcome = store
        .apply_batch(&Batch {
            root: root.to_path_buf(),
            started_at: now,
            flushed_at: now,
            events: vec![
                FsEvent::now(EventKind::Added {
                    path: root.join("src/fresh"),
                }),
                FsEvent::now(EventKind::Removed {
                    path: root.join("gone-dir"),
                }),
            ],
        })
        .unwrap();
    assert!(outcome.capture.is_none(), "no capture for a no-op batch");
    assert_eq!(
        store.captures(false, None, false, 50).unwrap().len(),
        before
    );

    // A real removal still lands.
    std::fs::remove_file(root.join("src/a.txt")).unwrap();
    let now = chrono::Utc::now();
    let real = store
        .apply_batch(&Batch {
            root: root.to_path_buf(),
            started_at: now,
            flushed_at: now,
            events: vec![FsEvent::now(EventKind::Removed {
                path: root.join("src/a.txt"),
            })],
        })
        .unwrap();
    assert!(real.capture.is_some());
}

#[test]
fn an_unreadable_intent_is_quarantined_rather_than_retried_forever() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);
    write(root, "a.txt", b"one\n");
    store.reconcile_worktree(&ignore).unwrap().unwrap();

    std::fs::write(root.join(".sheaf/state/restore.intent"), b"{ truncated").unwrap();
    assert!(store.pending_restore().is_none());
    assert!(store
        .resume_restore(
            &ignore,
            false,
            sheaf_core::config::RestoreConfig::default().max_resume_age_ms
        )
        .unwrap()
        .is_none());
    assert!(!root.join(".sheaf/state/restore.intent").exists());
    assert!(
        root.join(".sheaf/state/restore.intent.bad").exists(),
        "the evidence is kept, not deleted"
    );
}

#[test]
fn degraded_readers_can_plan_but_the_plan_reports_its_staleness() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);

    write(root, "a.txt", b"one\n");
    let good = store.reconcile_worktree(&ignore).unwrap().unwrap();
    write(root, "a.txt", b"two\n");
    store.reconcile_worktree(&ignore).unwrap().unwrap();
    drop(store);

    let reader = TimelineReader::open(root).unwrap();
    let plan = reader.plan_restore(good.short_id(), &[], &ignore).unwrap();
    assert!(plan.degraded);
    assert_eq!(plan.actions.len(), 1);
    assert_eq!(plan.actions[0].path, "a.txt");
    assert_eq!(
        std::fs::read(root.join("a.txt")).unwrap(),
        b"two\n",
        "planning is pure computation"
    );
}

#[test]
fn scope_keys_resolve_relative_to_the_invocation_directory() {
    let root = PathBuf::from("/projects/sheaf");
    let cwd = root.join("crates/sheaf-core");
    assert_eq!(
        sheaf_core::store::scope_key(&root, &cwd, "src/store").unwrap(),
        "crates/sheaf-core/src/store"
    );
    assert!(sheaf_core::store::scope_key(&root, &cwd, "/etc/hosts").is_err());
}

#[test]
fn scoped_restore_across_a_rename_speaks_both_names() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);

    write(root, "src/strings.rs", b"pub fn trim() {}\n");
    let before = store.reconcile_worktree(&ignore).unwrap().unwrap();
    // The rename, recorded as the first-class event it is.
    std::fs::rename(root.join("src/strings.rs"), root.join("src/strs.rs")).unwrap();
    store
        .apply_batch(&Batch {
            root: root.to_path_buf(),
            started_at: Utc::now(),
            flushed_at: Utc::now(),
            events: vec![FsEvent::now(EventKind::Renamed {
                from: root.join("src/strings.rs"),
                to: root.join("src/strs.rs"),
            })],
        })
        .unwrap();

    // Naming the CURRENT path must restore its former name, not delete it.
    let by_new = store
        .plan_restore(before.short_id(), &["src/strs.rs".to_string()], &ignore)
        .unwrap();
    assert_eq!(by_new.mode, RestoreMode::Scoped);
    assert!(
        by_new.actions.iter().any(|a| a.path == "src/strings.rs"),
        "the target-side name must be materialized: {:?}",
        by_new.actions
    );
    assert!(
        by_new.actions.iter().any(|a| a.path == "src/strs.rs"),
        "the current name must be removed"
    );
    store.apply_restore(&by_new, &ignore).unwrap();
    assert_eq!(
        std::fs::read(root.join("src/strings.rs")).unwrap(),
        b"pub fn trim() {}\n"
    );
    assert!(!root.join("src/strs.rs").exists());

    // Rename back so the second direction starts from the same shape.
    std::fs::rename(root.join("src/strings.rs"), root.join("src/strs.rs")).unwrap();
    store
        .apply_batch(&Batch {
            root: root.to_path_buf(),
            started_at: Utc::now(),
            flushed_at: Utc::now(),
            events: vec![FsEvent::now(EventKind::Renamed {
                from: root.join("src/strings.rs"),
                to: root.join("src/strs.rs"),
            })],
        })
        .unwrap();

    // Naming the FORMER path must also converge: restore the old name,
    // remove its successor, never leave both copies behind.
    let by_old = store
        .plan_restore(before.short_id(), &["src/strings.rs".to_string()], &ignore)
        .unwrap();
    store.apply_restore(&by_old, &ignore).unwrap();
    assert!(root.join("src/strings.rs").exists());
    assert!(!root.join("src/strs.rs").exists(), "no duplicate copies");
}

#[test]
fn scoped_restore_flags_paths_nothing_has_ever_held() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();
    let mut store = open(root);

    write(root, "real.txt", b"one\n");
    let before = store.reconcile_worktree(&ignore).unwrap().unwrap();
    write(root, "real.txt", b"two\n");
    store.reconcile_worktree(&ignore).unwrap().unwrap();

    let plan = store
        .plan_restore(before.short_id(), &["src/tpyo.rs".to_string()], &ignore)
        .unwrap();
    assert_eq!(plan.scope_missing, vec!["src/tpyo.rs".to_string()]);
    assert!(plan.is_noop(), "nothing to do for a never-seen path");

    // A real path must never be flagged.
    let ok = store
        .plan_restore(before.short_id(), &["real.txt".to_string()], &ignore)
        .unwrap();
    assert!(ok.scope_missing.is_empty());
}
