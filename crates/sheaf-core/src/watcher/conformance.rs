//! Backend conformance suite: the executable contract every watch backend
//! must satisfy.
//!
//! The [`WatchBackend`] trait is the portability seam: FSEvents, USN, or
//! whatever comes next must slot in *without touching core design*. That
//! holds only if every backend honors the same observable contract, so the
//! contract lives here as an executable suite rather than as prose in a
//! comment. `sheaf-core`'s own tests run it against the inotify backend and
//! against a synthetic loopback backend; a new platform implements
//! [`WatchBackend`] and calls [`run_suite`] with its constructor.
//!
//! The suite is deliberately tolerant on mechanism and strict on semantics:
//! rename pairing may degrade to Removed+Added, Added may or may not
//! accompany directory creations, but a created-then-modified file MUST be
//! reported as touched, an ignored path MUST be silent, and a stop flag MUST
//! be honored within the documented bound.

use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use super::{StopFlag, Watch, WatchBackend};
use crate::classify::Classifier;
use crate::events::{EventKind, FsEvent};

/// How long any single scenario waits for its evidence before failing.
const SCENARIO_TIMEOUT: Duration = Duration::from_secs(10);

/// A volatile canary the suite relies on: files matching it live in watched
/// (durable) directories, so their events MUST flow to the owner.
pub const VOLATILE_CANARY: &str = "conf-noise-*.log";

/// Factory: construct a fresh backend rooted at `root` honoring `classifier`.
pub type BackendFactory<'a> =
    dyn FnMut(&Path, Classifier) -> anyhow::Result<Box<dyn WatchBackend>> + 'a;

/// Run every scenario against a backend produced by `factory`. Panics with a
/// named scenario on the first violation — this is a test suite, not a
/// library API.
pub fn run_suite(factory: &mut BackendFactory, root: &Path, classifier: Classifier) {
    let backend = factory(root, classifier.clone())
        .unwrap_or_else(|e| panic!("factory failed to construct a backend: {e}"));
    scenario_stop_responsiveness(backend);

    let backend = factory(root, classifier.clone())
        .unwrap_or_else(|e| panic!("factory failed to construct a backend: {e}"));
    scenario_basic_lifecycle(backend, root, &classifier);

    let backend = factory(root, classifier)
        .unwrap_or_else(|e| panic!("factory failed to construct a backend: {e}"));
    scenario_new_dir_discovery(backend, root);
}

/// One running backend with its event collector. The `Watch` guard keeps the
/// thread owned for the scenario's lifetime; stopping it explicitly would
/// mask contract violations, so it is dropped (and joined) last.
struct Harness {
    #[allow(dead_code)] // lifetime guard; scenarios only consume events
    watch: Watch,
    rx: Receiver<FsEvent>,
    root: PathBuf,
}

impl Harness {
    fn start(backend: Box<dyn WatchBackend>) -> Harness {
        let root = backend.root().to_path_buf();
        let (tx, rx) = channel();
        let watch = Watch::start(backend, tx).expect("backend thread starts");
        Harness { watch, rx, root }
    }

    /// Collect events until `pred` is satisfied or the timeout lapses.
    fn wait_for(&self, pred: impl Fn(&FsEvent) -> bool, timeout: Duration) -> Vec<FsEvent> {
        let deadline = Instant::now() + timeout;
        let mut seen = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                panic!(
                    "timed out after {timeout:?}; collected: {:?}",
                    seen.iter().map(kind_name).collect::<Vec<_>>()
                );
            }
            match self
                .rx
                .recv_timeout(remaining.min(Duration::from_millis(100)))
            {
                Ok(ev) => {
                    let hit = pred(&ev);
                    seen.push(ev);
                    if hit {
                        return seen;
                    }
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => panic!("backend hung up its channel"),
            }
        }
    }

    fn drain_available(&self) -> Vec<FsEvent> {
        let mut out = Vec::new();
        while let Ok(ev) = self.rx.try_recv() {
            out.push(ev);
        }
        out
    }

    fn rel(&self, rel: &str) -> PathBuf {
        self.root.join(rel)
    }
}

fn kind_name(ev: &FsEvent) -> &'static str {
    match &ev.kind {
        EventKind::Added { .. } => "added",
        EventKind::Touched { .. } => "touched",
        EventKind::Renamed { .. } => "renamed",
        EventKind::Removed { .. } => "removed",
    }
}

/// 1. Stop flag honored within the documented ~2 s bound.
fn scenario_stop_responsiveness(backend: Box<dyn WatchBackend>) {
    let stop: StopFlag = super::new_stop_flag();
    let stop_for_thread = stop.clone();
    let (tx, _rx) = channel();
    let handle = std::thread::spawn(move || backend.run(tx, stop_for_thread));
    std::thread::sleep(Duration::from_millis(150));
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    let deadline = Instant::now() + Duration::from_secs(2);
    while !handle.is_finished() {
        if Instant::now() >= deadline {
            panic!("stop_responsiveness: backend did not exit within 2 s of the stop flag");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let _ = handle.join();
}

/// 2. Create → touch → rename → delete all surface, ignored paths stay
///    silent, and every path is absolute under root.
fn scenario_basic_lifecycle(backend: Box<dyn WatchBackend>, root: &Path, classifier: &Classifier) {
    let h = Harness::start(backend);
    std::thread::sleep(Duration::from_millis(120)); // let baseline settle

    // Volatile files in watched directories FLOW — the owner routes them
    // to the recovery ring. The canary matches VOLATILE_CANARY.
    let noisy = h.rel("conf-noise-7.log");
    std::fs::write(&noisy, "ring fodder").expect("write volatile canary");
    h.wait_for(
        |ev| {
            matches!(&ev.kind, EventKind::Added { path } if path == &noisy)
                || matches!(&ev.kind, EventKind::Touched { path } if path.0 == noisy)
        },
        SCENARIO_TIMEOUT,
    );
    assert_eq!(
        classifier.classify_rel(std::path::Path::new("conf-noise-7.log")),
        crate::classify::PathClass::Volatile,
        "suite canary must actually be volatile in the passed classifier"
    );

    // Never paths are silent even though the root itself is watched.
    let store_dir = root.join(".sheaf");
    std::fs::create_dir_all(&store_dir).expect("mkdir .sheaf");
    std::fs::write(store_dir.join("never.txt"), "self-observation").expect("write never");
    std::thread::sleep(Duration::from_millis(300));
    let a = h.rel("conf-a.txt");
    std::fs::write(&a, "hello").expect("create a");
    let evs = h.wait_for(
        |ev| {
            matches!(&ev.kind, EventKind::Added { path } if path == &a)
                || matches!(&ev.kind, EventKind::Touched { path } if path.0 == a)
        },
        SCENARIO_TIMEOUT,
    );

    std::fs::write(&a, "hello world").expect("modify a");
    let evs2 = h.wait_for(
        |ev| matches!(&ev.kind, EventKind::Touched { path } if path.0 == a),
        SCENARIO_TIMEOUT,
    );

    let b = h.rel("conf-b.txt");
    std::fs::rename(&a, &b).expect("rename a to b");
    let evs3 = h.wait_for(|ev| h_reported_to(ev, &b), SCENARIO_TIMEOUT);

    std::fs::remove_file(&b).expect("remove b");
    let evs4 = h.wait_for(|ev| h_vanished(ev, &b), SCENARIO_TIMEOUT);

    // Ignored paths: never reported, even across kinds.
    let ignored_dir = root.join("conf-ignored");
    std::fs::create_dir_all(&ignored_dir).expect("mkdir ignored");
    std::thread::sleep(Duration::from_millis(300));
    std::fs::write(ignored_dir.join("x.txt"), "noise").expect("write ignored");
    std::thread::sleep(Duration::from_millis(400));
    let all: Vec<FsEvent> = evs
        .into_iter()
        .chain(evs2)
        .chain(evs3)
        .chain(evs4)
        .chain(h.drain_available())
        .collect();
    for ev in &all {
        let p = ev.path();
        let ok_under_root = p.starts_with(root);
        assert!(
            ok_under_root,
            "basic_lifecycle: event path {p:?} escapes the watched root"
        );
        assert!(
            !(p.starts_with(&ignored_dir) && p != ignored_dir),
            "basic_lifecycle: non-durable subtree leaked an event ({p:?}, {})",
            kind_name(ev)
        );
        assert!(
            !p.starts_with(&store_dir),
            "basic_lifecycle: Never path {p:?} leaked an event ({}) — the watcher must never observe the store",
            kind_name(ev)
        );
        // Evidence check: every path the suite created outside ignores was seen.
    }
    let reported_a = all.iter().any(|ev| match &ev.kind {
        EventKind::Added { path } => path == &a,
        EventKind::Touched { path } => path.0 == a,
        EventKind::Renamed { to, .. } => to == &a,
        _ => false,
    });
    assert!(
        reported_a,
        "basic_lifecycle: creation of {a:?} never reported"
    );
    let reported_b = all.iter().any(|ev| match &ev.kind {
        EventKind::Removed { path } => path == &b,
        EventKind::Renamed { from, .. } => from == &b,
        _ => false,
    });
    assert!(
        reported_b,
        "basic_lifecycle: disappearance of {b:?} never reported"
    );
}

fn h_reported_to(ev: &FsEvent, path: &Path) -> bool {
    matches!(&ev.kind, EventKind::Renamed { to, .. } if to == path)
        || matches!(&ev.kind, EventKind::Added { path: p } if p == path)
        || matches!(&ev.kind, EventKind::Touched { path: p } if p.0 == path)
}

fn h_vanished(ev: &FsEvent, path: &Path) -> bool {
    match &ev.kind {
        EventKind::Removed { path: p } => p == path,
        EventKind::Renamed { from, .. } => from == path,
        _ => false,
    }
}

/// 3. A file landing inside a NEWLY CREATED subdirectory is reported — the
///    backend must close its own registration gap.
fn scenario_new_dir_discovery(backend: Box<dyn WatchBackend>, _root: &Path) {
    let h = Harness::start(backend);
    std::thread::sleep(Duration::from_millis(120));

    let sub = h.rel("conf-newdir");
    let file = sub.join("child.txt");
    std::fs::create_dir_all(&sub).expect("mkdir new dir");
    std::fs::write(&file, "child").expect("write child");

    let evs = h.wait_for(
        |ev| {
            matches!(&ev.kind, EventKind::Added { path } if path == &file)
                || matches!(&ev.kind, EventKind::Touched { path } if path.0 == file)
        },
        SCENARIO_TIMEOUT,
    );
    let _ = evs;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{EventKind, FsEvent, TouchedPath};

    #[test]
    fn helper_classification_and_path_predicates_cover_all_event_kinds() {
        let a = PathBuf::from("/tmp/a");
        let b = PathBuf::from("/tmp/b");
        let events = [
            FsEvent::now(EventKind::Added { path: a.clone() }),
            FsEvent::now(EventKind::Removed { path: a.clone() }),
            FsEvent::now(EventKind::Renamed {
                from: a.clone(),
                to: b.clone(),
            }),
            FsEvent::now(EventKind::Touched {
                path: TouchedPath(b.clone()),
            }),
        ];
        assert_eq!(
            events.iter().map(kind_name).collect::<Vec<_>>(),
            vec!["added", "removed", "renamed", "touched"]
        );
        assert!(h_reported_to(&events[0], &a));
        assert!(h_reported_to(&events[2], &b));
        assert!(h_vanished(&events[1], &a));
        assert!(h_vanished(&events[2], &a));
        assert!(!h_vanished(&events[0], &a));
    }
}
