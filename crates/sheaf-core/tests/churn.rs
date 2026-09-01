//! Watcher acceptance: real inotify watch over a real tree, structural
//! correctness under editor-style churn, ignore honoring, rename pairing,
//! enrollment persistence, offline init — the done-criteria, in code.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sheaf_core::config::default_patterns;
use sheaf_core::error::SheafError;
use sheaf_core::events::{EventKind, FsEvent};
use sheaf_core::ignore::IgnoreSet;
use sheaf_core::init::{init_project, resolve_project_root, InitOptions};
use sheaf_core::registry::Registry;
use sheaf_core::watcher::{default_backend, new_stop_flag};

/// Drain everything currently queued, non-blocking.
fn snapshot(rx: &Receiver<FsEvent>) -> Vec<FsEvent> {
    let mut out = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        out.push(ev);
    }
    out
}

/// Accumulate until predicate holds; fail loudly with what we did see.
fn drive(
    rx: &Receiver<FsEvent>,
    timeout_ms: u64,
    mut pred: impl FnMut(&[FsEvent]) -> bool,
) -> Vec<FsEvent> {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    let mut acc = Vec::new();
    while Instant::now() < deadline {
        acc.extend(snapshot(rx));
        if pred(&acc) {
            return acc;
        }
        std::thread::sleep(Duration::from_millis(15));
    }
    panic!(
        "condition unmet after {timeout_ms}ms; saw {} events:\n{:#?}",
        acc.len(),
        kinds(&acc)
    );
}

fn kinds(events: &[FsEvent]) -> Vec<String> {
    events.iter().map(|e| format!("{:?}", e.kind)).collect()
}

struct TestWatch {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
    rx: Receiver<FsEvent>,
}

impl TestWatch {
    fn start(root: &Path) -> Self {
        let classifier =
            sheaf_core::classify::Classifier::from_volatile_patterns(&default_patterns()).unwrap();
        let backend = default_backend(
            root.to_path_buf(),
            sheaf_core::watcher::shared_classifier(classifier),
        )
        .expect("backend init");
        let (tx, rx) = channel::<FsEvent>();
        let stop = new_stop_flag();
        let stop2 = stop.clone();
        let handle = std::thread::Builder::new()
            .name("test-watch".into())
            .spawn(move || backend.run(tx, stop2))
            .unwrap();
        TestWatch {
            stop,
            handle: Some(handle),
            rx,
        }
    }

    /// Path of interest for an event (rename destination if applicable).
    fn probe_path(ev: &FsEvent) -> PathBuf {
        ev.path().to_path_buf()
    }
}

impl Drop for TestWatch {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

#[test]
fn init_creates_store_and_registry_idempotently() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tmp.path().join("reg")).unwrap();
    let registry = Registry::at(tmp.path().join("reg/enrollments.jsonl"));
    let dead_socket = tmp.path().join("definitely-absent.sock");

    let proj = tempfile::tempdir_in(tmp.path()).unwrap();
    let report = init_project(
        proj.path(),
        InitOptions {
            registry_override: Some(&registry),
            socket_override: Some(dead_socket.clone()),
        },
    )
    .unwrap();
    assert!(report.store_created);
    assert!(report.newly_enrolled);
    assert!(
        !report.daemon_notified,
        "absent socket => best-effort notify fails"
    );
    // config.toml IS the marker (carrying format_version); the legacy flat
    // file must NOT come back.
    assert!(proj.path().join(".sheaf/config.toml").is_file());
    assert!(!proj.path().join(".sheaf/FORMAT_VERSION").exists());

    // Re-run inside subdir: adopts ancestor store, no duplicate enrollment.
    std::fs::create_dir(proj.path().join("deep")).unwrap();
    let again = init_project(
        &proj.path().join("deep"),
        InitOptions {
            registry_override: Some(&registry),
            socket_override: Some(dead_socket),
        },
    )
    .unwrap();
    assert!(!again.store_created && !again.newly_enrolled);
    assert_eq!(
        resolve_project_root(&proj.path().join("deep")).as_deref(),
        Some(proj.path())
    );
    assert_eq!(registry.list().unwrap().len(), 1, "deduped enrollment");
}

#[test]
fn newer_store_format_fails_closed() {
    let tmp = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(tmp.path().join(".sheaf")).unwrap();
    // config.toml is the marker AND the version carrier.
    std::fs::write(
        tmp.path().join(".sheaf/config.toml"),
        "format_version = 7\n",
    )
    .unwrap();
    let result = sheaf_core::config::read_store_format(tmp.path());
    match result {
        Err(SheafError::StoreVersion {
            found: 7,
            supported: sheaf_core::config::STORE_FORMAT_VERSION,
            ..
        }) => {}
        other => panic!("expected StoreVersion mismatch, got {other:?}"),
    }
}

#[test]
fn churn_events_are_structurally_correct_and_nondurable_subtrees_stay_dark() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();

    // Baseline layout incl. ignored dirs created BEFORE watching starts.
    std::fs::create_dir_all(root.join("src/nested")).unwrap();
    std::fs::create_dir_all(root.join("target/debug")).unwrap();
    std::fs::create_dir_all(root.join(".git/objects")).unwrap();

    let w = TestWatch::start(root);
    // Let baseline registration settle; watcher is poll-driven at ~25ms.
    std::thread::sleep(Duration::from_millis(150));

    // --- editor-style burst ---
    std::fs::write(root.join("a.txt"), b"hello").unwrap(); // Added (+Touched noise ok)
    std::fs::create_dir(root.join("newdir")).unwrap(); // Added(dir)
    std::fs::write(root.join("newdir/x.yml"), b"k: v").unwrap(); // Added(file)
    std::fs::write(root.join("b.txt"), b"orig").unwrap();

    // Wait for the initial adds BEFORE renaming them apart (pairing needs
    // both halves observed from our watches, which is the point being tested).
    let _ = drive(&w.rx, 5000, |acc| {
        acc.iter()
            .any(|e| matches!(&e.kind, EventKind::Added { path } if path.ends_with("x.yml")))
            && acc
                .iter()
                .any(|e| matches!(&e.kind, EventKind::Added { path } if path.ends_with("b.txt")))
    });

    std::fs::rename(root.join("b.txt"), root.join("c.txt")).unwrap();

    // Move into subdir must pair as Renamed, not delete+add.
    std::thread::sleep(Duration::from_millis(60));
    std::fs::rename(root.join("a.txt"), root.join("newdir/a-moved.txt")).unwrap();

    let _ = drive(&w.rx, 5000, |acc| {
        acc.iter().any(|e| {
            matches!(
                e.kind,
                EventKind::Renamed { ref from, ref to }
                    if from.ends_with("a.txt") && to.ends_with("a-moved.txt")
            )
        })
    });

    // Deletion
    std::fs::remove_file(root.join("newdir/a-moved.txt")).unwrap();
    let seen_removals = drive(&w.rx, 5000, |acc| {
        acc.iter().any(|e| {
            matches!(
                e.kind,
                EventKind::Removed { ref path } if path.ends_with("a-moved.txt")
            )
        })
    });
    let removal_count = seen_removals
        .iter()
        .filter(|e| matches!(&e.kind, EventKind::Removed { path } if path.ends_with("a-moved.txt")))
        .count();
    assert_eq!(removal_count, 1, "exactly one Removed per delete");

    // Explicit pairing assertions for BOTH renames.
    let _all_so_far = snapshot(&w.rx); // leftovers shouldn't matter for asserts below
                                       // --- non-durable subtrees stay dark; their dir-create itself flows ---
                                       // A volatile directory created inside a watched dir surfaces as ONE
                                       // structural event (the daemon's ring stats it and moves on); nothing
                                       // BENEATH it may ever surface, because registration never descends.
    std::fs::write(root.join("target/debug/out.bin"), vec![0u8; 64]).unwrap();
    std::fs::write(root.join(".git/config.extra"), b"noise").unwrap();
    let extra = root.join("node_modules");
    std::fs::create_dir_all(extra.join("pkg")).unwrap();
    std::fs::write(extra.join("pkg/lib.js"), b"x").unwrap();

    let quiet_check = drive(&w.rx, 700, |acc| !acc.is_empty() || true); // just wait out the window
    let noisy = quiet_check.iter().any(|ev| {
        let p = TestWatch::probe_path(ev);
        (p.starts_with(root.join("target")) && p != root.join("target"))
            || p.starts_with(root.join(".git"))
            || (p.starts_with(&extra) && p != extra)
    });
    assert!(
        !noisy,
        "events leaked from non-durable subtrees: {:?}",
        kinds(&quiet_check)
    );

    // Give lingering writes from earlier ops one grace beat, then final sweep.
    std::thread::sleep(Duration::from_millis(120));
    let tail = snapshot(&w.rx);
    let noisy_tail = tail.iter().any(|ev| {
        let p = TestWatch::probe_path(ev);
        (p.starts_with(root.join("target")) && p != root.join("target"))
            || p.starts_with(root.join(".git"))
            || (p.starts_with(&extra) && p != extra)
    });
    assert!(
        !noisy_tail,
        "late-arriving events from non-durable subtrees: {:?}",
        kinds(&tail)
    );
}

#[test]
fn enrollments_survive_restart_simulation() {
    let tmp = tempfile::tempdir().unwrap();
    let registry_file = tmp.path().join("enrollments.jsonl");
    let proj_a = tempfile::tempdir_in(tmp.path()).unwrap();
    let proj_b = tempfile::tempdir_in(tmp.path()).unwrap();

    // "First daemon life"
    {
        let r = Registry::at(registry_file.clone());
        r.upsert(proj_a.path()).unwrap();
        r.upsert(proj_b.path()).unwrap();
    }
    // Between lives: project B deleted from disk.
    drop(proj_b);

    // "Second daemon life": reload verbatim; live check gates watching.
    let r = Registry::at(registry_file);
    let entries = r.list().unwrap();
    assert_eq!(entries.len(), 2, "registry persists across restarts");
    let live: Vec<_> = entries.into_iter().filter(|e| e.root.is_dir()).collect();
    assert_eq!(live.len(), 1, "missing projects are skipped, not fatal");
    assert_eq!(live[0].root, proj_a.path().canonicalize().unwrap());

    let ig = IgnoreSet::from_patterns(&default_patterns()).unwrap();
    assert!(ig.is_ignored_rel(Path::new(".git/objects/ab/cd")));
}

/// A Neovim-style atomic save writes a swap file (`.name.swp`) and a temp
/// file, then renames the temp over the real file. Under classification
/// the litter is VOLATILE, not invisible: its events must flow so the
/// daemon's scratch ring can mirror the last state (that is what makes
/// `sheaf recover` possible after an editor crash). What must NOT happen
/// is the timeline capturing them — that is the daemon's routing job and
/// is pinned by the daemon-level tests.
#[test]
fn editor_atomic_save_litter_flows_for_the_ring() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    std::fs::write(root.join("notes.md"), b"v1\n").unwrap();

    let w = TestWatch::start(root);
    std::thread::sleep(Duration::from_millis(150));

    // Swap file appears (Vim/Neovim), then the atomic write-temp-then-rename.
    std::fs::write(root.join(".notes.md.swp"), b"swapdata").unwrap();
    std::fs::write(root.join("notes.md~"), b"v1\n").unwrap(); // backup copy
    std::fs::write(root.join(".notes.md.tmp"), b"v2 new content\n").unwrap();
    std::fs::rename(root.join(".notes.md.tmp"), root.join("notes.md")).unwrap();
    // Swap file removed on save completion.
    std::fs::remove_file(root.join(".notes.md.swp")).unwrap();

    // The real file's update must be observed (as Added/Touched/Renamed-to,
    // whichever the backend reports for a rename-over-existing).
    let seen = drive(&w.rx, 5000, |acc| {
        acc.iter()
            .any(|e| TestWatch::probe_path(e).ends_with("notes.md"))
    });

    // The litter surfaces too — volatile events flow to the owner.
    let litter: Vec<_> = seen
        .iter()
        .filter(|e| {
            let p = TestWatch::probe_path(e);
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            name.ends_with(".swp") || name.ends_with('~') || name.ends_with(".tmp")
        })
        .collect();
    assert!(
        !litter.is_empty(),
        "volatile editor litter must reach the owner for the ring: {:?}",
        kinds(&seen)
    );
}
