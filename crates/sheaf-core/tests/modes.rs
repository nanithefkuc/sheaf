//! File-mode modelling.
//!
//! History records the exec bit per path; capture records it, chmod is
//! visible history, and restore materializes it. A restored executable that
//! comes back 0644 is unrecoverable data loss — the whole point of this
//! suite.

use std::collections::BTreeMap;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use sheaf_core::config;
use sheaf_core::events::{Batch, EventKind, FsEvent};
use sheaf_core::ignore::IgnoreSet;
use sheaf_core::store::{ProjectStore, StoreLimits};

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

fn chmod_x(path: &Path) {
    let mut perms = std::fs::metadata(path).unwrap().permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(path, perms).unwrap();
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

/// Watcher events carry absolute paths (the daemon's backend resolves them);
/// tests must mirror that or the store would read against the process CWD.
fn touched(root: &Path, rel: &str) -> FsEvent {
    FsEvent::now(EventKind::Touched {
        path: root.join(rel).into(),
    })
}

fn added(root: &Path, rel: &str) -> FsEvent {
    FsEvent::now(EventKind::Added {
        path: root.join(rel),
    })
}

/// mode-aware tree snapshot for assertions.
fn tree_with_modes(root: &Path) -> BTreeMap<String, u32> {
    let mut out = BTreeMap::new();
    for entry in walkdir::WalkDir::new(root)
        .into_iter()
        .filter_map(Result::ok)
    {
        if !entry.file_type().is_file() {
            continue;
        }
        let rel = entry.path().strip_prefix(root).unwrap();
        if rel.starts_with(".sheaf") {
            continue;
        }
        let mode = std::fs::metadata(entry.path())
            .unwrap()
            .permissions()
            .mode();
        out.insert(rel.to_string_lossy().replace('\\', "/"), mode & 0o111);
    }
    out
}

#[test]
fn an_executable_file_is_captured_and_restored_executable() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();

    write(root, "scripts/build.sh", b"#!/bin/sh\necho hi\n");
    chmod_x(&root.join("scripts/build.sh"));
    write(root, "src/main.rs", b"fn main() {}\n");

    let mut store = open(root);
    flush(
        &mut store,
        root,
        vec![added(root, "scripts/build.sh"), added(root, "src/main.rs")],
    );

    // Wreck both files and lose the mode entirely.
    write(root, "scripts/build.sh", b"corrupted\n");
    let plain = std::fs::Permissions::from_mode(0o644);
    std::fs::set_permissions(root.join("scripts/build.sh"), plain).unwrap();
    write(root, "src/main.rs", b"// broken\n");

    let reader = sheaf_core::store::TimelineReader::open(root).unwrap();
    let tip = reader.captures(false, None, false, 1).unwrap();
    let target = tip[0].clone();
    let plan = store.plan_restore(&target.id, &[], &ignore).unwrap();
    store.apply_restore(&plan, &ignore).unwrap();

    let tree = tree_with_modes(root);
    assert_eq!(
        std::fs::read(root.join("scripts/build.sh")).unwrap(),
        b"#!/bin/sh\necho hi\n"
    );
    assert_eq!(tree["scripts/build.sh"] & 0o111, 0o111, "exec bit restored");
    assert_eq!(tree["src/main.rs"] & 0o111, 0, "plain file stays plain");
}

#[test]
fn chmod_on_unchanged_content_is_real_history() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();

    write(root, "tool.sh", b"#!/bin/sh\nexit 0\n");
    let mut store = open(root);
    flush(&mut store, root, vec![added(root, "tool.sh")]);
    let first = store.captures(false, None, false, 10).unwrap().len();

    // chmod +x: content identical, mode changed. That IS a capture.
    chmod_x(&root.join("tool.sh"));
    flush(&mut store, root, vec![touched(root, "tool.sh")]);
    let after_chmod = store.captures(false, None, false, 10).unwrap();
    assert!(
        after_chmod.len() > first,
        "a chmod must become history, not noise"
    );

    // And it is recoverable: break the content, restore, both come back.
    write(root, "tool.sh", b"# wrecked\n");
    let plain = std::fs::Permissions::from_mode(0o644);
    std::fs::set_permissions(root.join("tool.sh"), plain.clone()).unwrap();
    let reader = sheaf_core::store::TimelineReader::open(root).unwrap();
    let tip = reader.captures(false, None, false, 1).unwrap();
    let plan = store.plan_restore(&tip[0].id, &[], &ignore).unwrap();
    store.apply_restore(&plan, &ignore).unwrap();
    assert_eq!(
        std::fs::read(root.join("tool.sh")).unwrap(),
        b"#!/bin/sh\nexit 0\n"
    );
    assert_eq!(
        std::fs::metadata(root.join("tool.sh"))
            .unwrap()
            .permissions()
            .mode()
            & 0o111,
        0o111
    );
}

#[test]
fn chmod_minus_x_is_history_too() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let ignore = ignores();

    write(root, "run.sh", b"#!/bin/sh\nexit 0\n");
    chmod_x(&root.join("run.sh"));
    let mut store = open(root);
    flush(&mut store, root, vec![added(root, "run.sh")]);

    // Exec -> plain on identical bytes: another real change.
    let plain = std::fs::Permissions::from_mode(0o644);
    std::fs::set_permissions(root.join("run.sh"), plain).unwrap();
    let before = store.captures(false, None, false, 100).unwrap().len();
    flush(&mut store, root, vec![touched(root, "run.sh")]);
    let after = store.captures(false, None, false, 100).unwrap().len();
    assert!(after > before, "losing the exec bit must be recorded");

    // Restore to the ORIGINAL capture (newest-first, so the last entry is
    // the one where the file was still executable).
    let reader = sheaf_core::store::TimelineReader::open(root).unwrap();
    let caps = reader.captures(false, None, false, 100).unwrap();
    let original = caps.last().unwrap();
    let plan = store.plan_restore(&original.id, &[], &ignore).unwrap();
    store.apply_restore(&plan, &ignore).unwrap();
    assert_eq!(
        std::fs::metadata(root.join("run.sh"))
            .unwrap()
            .permissions()
            .mode()
            & 0o111,
        0o111
    );
}

#[test]
fn a_pure_mtime_echo_models_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    skeleton(root);
    let _ignore = ignores();

    write(root, "doc.txt", b"stable bytes\n");
    let mut store = open(root);
    flush(&mut store, root, vec![added(root, "doc.txt")]);
    let captures = store.captures(false, None, false, 100).unwrap().len();

    // Same bytes, same mode, only mtime moves (touch(1), backup tools):
    // not a user action, not history. The next flush must be dropped as a
    // batch that models nothing.
    let path = root.join("doc.txt");
    let file = std::fs::File::options().append(true).open(&path).unwrap();
    file.set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_000_000))
        .unwrap();
    drop(file);
    flush(&mut store, root, vec![touched(root, "doc.txt")]);
    assert_eq!(
        store.captures(false, None, false, 100).unwrap().len(),
        captures,
        "a pure mtime echo must not become a capture"
    );
}
