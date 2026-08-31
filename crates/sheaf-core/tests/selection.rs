//! Executable fixtures for snapshot-bound selections.

use std::path::Path;

use chrono::{Duration, Utc};
use sheaf_core::config;
use sheaf_core::events::{Batch, EventKind, FsEvent};
use sheaf_core::store::{
    append_frame, gc_run_store, hash_of, CommitFrame, FrameKind, HistoricalPathContent,
    ProjectStore, Projection, StoreLimits, TimelineReader,
};

fn skeleton(root: &Path) {
    std::fs::create_dir_all(root.join(".sheaf/store")).unwrap();
    config::write_skeleton(root).unwrap();
}

fn open(root: &Path) -> ProjectStore {
    ProjectStore::open(
        root,
        StoreLimits {
            max_segment_bytes: 4 << 20,
            snapshot_edit_size: 1_000,
        },
    )
    .unwrap()
}

fn capture(store: &mut ProjectStore, root: &Path, rel: &str, text: &str, age: Duration) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, text).unwrap();
    let at = Utc::now() - age;
    store
        .apply_batch(&Batch {
            root: root.to_path_buf(),
            started_at: at,
            flushed_at: at,
            events: vec![FsEvent::now(EventKind::Touched { path: path.into() })],
        })
        .unwrap();
}

#[test]
fn single_path_reads_survive_shallow_retention_byte_exact() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);

    capture(
        &mut store,
        root,
        "src/lib.rs",
        "fn ancient() {}\n",
        Duration::hours(5),
    );
    capture(
        &mut store,
        root,
        "src/lib.rs",
        "fn selected() { println!(\"kept\"); }\n",
        Duration::hours(1),
    );
    store.create_checkpoint("selection-source", None).unwrap();
    let expected = store
        .historical_path_content("checkpoint:selection-source", "src/lib.rs")
        .unwrap();
    capture(
        &mut store,
        root,
        "src/lib.rs",
        "fn current() {}\n",
        Duration::zero(),
    );

    config::set_retention_expiry(root, "2h").unwrap();
    gc_run_store(&mut store, true).unwrap();
    assert_eq!(
        store
            .historical_path_content("checkpoint:selection-source", "src/lib.rs")
            .unwrap(),
        expected
    );
    drop(store);

    let reader = TimelineReader::open(root).unwrap();
    assert!(reader.doc().is_shallow());
    assert_eq!(
        reader
            .historical_path_content("checkpoint:selection-source", "src/lib.rs")
            .unwrap(),
        expected
    );
    assert!(matches!(expected, HistoricalPathContent::Text(text) if text.contains("selected")));
}

#[test]
fn single_path_reads_address_both_divergent_futures() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);

    capture(
        &mut store,
        root,
        "f.rs",
        "fn base() {}\n",
        Duration::hours(3),
    );
    let base = store.captures(false, None, false, 1).unwrap()[0].clone();
    capture(
        &mut store,
        root,
        "f.rs",
        "fn abandoned() {}\n",
        Duration::hours(2),
    );
    let abandoned = store.captures(false, None, false, 1).unwrap()[0].clone();
    store.checkout_for_branch(&base.frontier).unwrap();
    capture(
        &mut store,
        root,
        "f.rs",
        "fn current() {}\n",
        Duration::hours(1),
    );
    let current = store.captures(false, None, false, 1).unwrap()[0].clone();

    assert_eq!(
        store
            .historical_path_content(&abandoned.id, "f.rs")
            .unwrap(),
        HistoricalPathContent::Text("fn abandoned() {}\n".into())
    );
    assert_eq!(
        store.historical_path_content(&current.id, "f.rs").unwrap(),
        HistoricalPathContent::Text("fn current() {}\n".into())
    );
    assert_eq!(store.branch_tips().unwrap().len(), 2);
}

#[test]
fn interleaved_partial_commit_has_no_equal_timeline_frontier_and_never_anchors() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let mut store = open(root);
    let states = [
        "fn a() { 0 }\nfn b() { 0 }\n",
        "fn a() { 1 }\nfn b() { 0 }\n",
        "fn a() { 1 }\nfn b() { 1 }\n",
        "fn a() { 2 }\nfn b() { 1 }\n",
    ];
    for (index, state) in states.iter().enumerate() {
        capture(
            &mut store,
            root,
            "src/lib.rs",
            state,
            Duration::hours((states.len() - index) as i64),
        );
    }
    let captures = store.captures(false, None, false, 10).unwrap();
    let partial_tree = "fn a() { 2 }\nfn b() { 0 }\n";
    for capture in &captures {
        assert_ne!(
            store
                .historical_path_content(&capture.id, "src/lib.rs")
                .unwrap(),
            HistoricalPathContent::Text(partial_tree.into()),
            "subset commit must not be represented by capture {}",
            capture.short_id()
        );
    }

    let tip = &captures[0];
    let anchor = &captures[captures.len() - 1];
    let partial = CommitFrame {
        v: 1,
        sha: "partial-commit".into(),
        short_sha: "partial".into(),
        anchor_capture_id: Some(anchor.id.clone()),
        anchor_ref: None,
        tip_capture_id: None,
        committed_at_ms: Utc::now().timestamp_millis(),
        stamped_at_ms: Utc::now().timestamp_millis(),
        captures: captures.len(),
        files: 1,
        added: 1,
        removed: 1,
        restores_crossed: 0,
        subject: "smart-squash a".into(),
        kind: FrameKind::Partial,
        projection: Some(Projection {
            parent_sha: "baseline".into(),
            git_tree_before: hash_of(states[0].as_bytes()),
            git_tree_after: hash_of(partial_tree.as_bytes()),
            selection_ids: vec!["selection-a".into()],
            patch_sha256: hash_of(b"a:0->2"),
            tip_capture_id: tip.id.clone(),
        }),
    };
    assert!(!partial.is_anchor_eligible());
    assert_eq!(partial.anchor_eligible_name(), None);
    assert_eq!(partial.validate_projection(), Ok(()));
    append_frame(root, &partial).unwrap();
    assert!(store
        .checkpoints()
        .iter()
        .all(|checkpoint| checkpoint.name != "git-partial"));
}
