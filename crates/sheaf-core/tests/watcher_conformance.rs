//! The watcher seam under stress (Linux-first inotify backend).
//!
//! The inotify backend must satisfy the documented `WatchBackend` contract,
//! and a synthetic loopback backend must satisfy it too — proving the suite
//! itself stays mechanism-agnostic, which is what keeps FSEvents/USN
//! plausible later without touching core design.

use std::path::{Path, PathBuf};
use std::sync::mpsc::Sender;
use std::time::Duration;

use sheaf_core::classify::Classifier;
use sheaf_core::config;
use sheaf_core::events::FsEvent;
use sheaf_core::watcher::{conformance, shared_classifier, InotifySource, StopFlag, WatchBackend};

fn classifier() -> Classifier {
    // `conf-ignored/` is the suite's silent-subtree canary;
    // `conf-noise-*.log` is its flows-to-owner volatile canary.
    let mut patterns = config::default_volatile_patterns();
    patterns.push("conf-ignored/".into());
    patterns.push(conformance::VOLATILE_CANARY.into());
    Classifier::from_volatile_patterns(&patterns).unwrap()
}

fn inotify_factory() -> impl FnMut(&Path, Classifier) -> anyhow::Result<Box<dyn WatchBackend>> {
    |root: &Path, classifier: Classifier| {
        Ok(Box::new(InotifySource::new(
            root.to_path_buf(),
            shared_classifier(classifier),
        )?) as Box<dyn WatchBackend>)
    }
}

#[test]
fn the_real_inotify_backend_satisfies_the_contract() {
    let tmp = tempfile::tempdir().unwrap();
    let mut factory = inotify_factory();
    conformance::run_suite(&mut factory, tmp.path(), classifier());
}

/// A synthetic backend that emits a scripted, correct event sequence. It
/// validates the suite's tolerance boundaries: pairing renames is optional
/// (this one degrades a rename to Removed+Added) and directory Adds are
/// optional (this one skips them) — the contract is about what must be
/// observable, never about how.
struct Loopback {
    root: PathBuf,
    script: Vec<FsEvent>,
    stop_after: Duration,
}

impl WatchBackend for Loopback {
    fn root(&self) -> &Path {
        &self.root
    }

    fn run(self: Box<Self>, tx: Sender<FsEvent>, stop: StopFlag) {
        for ev in self.script {
            if stop.load(std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            let _ = tx.send(ev);
        }
        // Hold the thread alive briefly so stop-responsiveness has something
        // to observe, then exit well inside the 2 s bound.
        let deadline = std::time::Instant::now() + self.stop_after;
        while std::time::Instant::now() < deadline {
            if stop.load(std::sync::atomic::Ordering::SeqCst) {
                return;
            }
        }
    }
}

#[test]
fn the_conformance_suite_accepts_a_correct_synthetic_backend() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_path_buf();
    let mut factory = move |r: &Path, _classifier: Classifier| {
        Ok(Box::new(Loopback {
            root: r.to_path_buf(),
            script: vec![
                // volatile canary in a watched dir: must flow to the owner
                FsEvent::now(sheaf_core::events::EventKind::Added {
                    path: root.join("conf-noise-7.log"),
                }),
                // create
                FsEvent::now(sheaf_core::events::EventKind::Added {
                    path: root.join("conf-a.txt"),
                }),
                // modify
                FsEvent::now(sheaf_core::events::EventKind::Touched {
                    path: root.join("conf-a.txt").into(),
                }),
                // rename degraded to remove+add (allowed by the contract)
                FsEvent::now(sheaf_core::events::EventKind::Removed {
                    path: root.join("conf-a.txt"),
                }),
                FsEvent::now(sheaf_core::events::EventKind::Added {
                    path: root.join("conf-b.txt"),
                }),
                // delete
                FsEvent::now(sheaf_core::events::EventKind::Removed {
                    path: root.join("conf-b.txt"),
                }),
                // new-dir child discovery
                FsEvent::now(sheaf_core::events::EventKind::Added {
                    path: root.join("conf-newdir").join("child.txt"),
                }),
            ],
            stop_after: Duration::from_secs(30),
        }) as Box<dyn WatchBackend>)
    };
    conformance::run_suite(&mut factory, tmp.path(), classifier());
}
