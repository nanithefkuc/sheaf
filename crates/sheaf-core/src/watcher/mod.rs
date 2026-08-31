//! Watcher abstraction: the only seam allowed to know a backend
//! exists. Core ships the raw-inotify backend; FSEvents/USN land behind this
//! same trait later.

/// Executable contract every watch backend must pass.
pub mod conformance;
/// The Linux inotify watch backend.
pub mod inotify_source;

use std::path::PathBuf;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use crate::error::{Result, SheafError};
use crate::events::FsEvent;
use crate::ignore::IgnoreSet;
pub use inotify_source::InotifySource;

/// Stop flag shared with watcher threads.
pub type StopFlag = Arc<std::sync::atomic::AtomicBool>;

/// Create a fresh, un-set stop flag to share with a watcher thread.
pub fn new_stop_flag() -> StopFlag {
    Arc::new(std::sync::atomic::AtomicBool::new(false))
}

/// A running watch over one project root.
pub struct Watch {
    pub root: PathBuf,
    stop: StopFlag,
    handle: Option<JoinHandle<()>>,
}

impl Watch {
    /// Spawn the backend thread. Events flow through `tx` until `stop()`.
    pub fn start(backend: Box<dyn WatchBackend>, tx: Sender<FsEvent>) -> Result<Watch> {
        let stop = new_stop_flag();
        let root = backend.root().to_path_buf();
        let stop_for_thread = stop.clone();
        let handle = std::thread::Builder::new()
            .name(format!("watch:{}", root.display()))
            .spawn(move || backend.run(tx, stop_for_thread))
            .map_err(|e| SheafError::WatchInit {
                root: root.clone(),
                message: e.to_string(),
            })?;
        Ok(Watch {
            root,
            stop,
            handle: Some(handle),
        })
    }

    /// Ask politely; caller decides how long to wait for the tail.
    pub fn request_stop(&self) {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Wait up to `timeout` for the loop to exit.
    pub fn join(&mut self, timeout: Duration) -> bool {
        if let Some(h) = self.handle.take() {
            let deadline = std::time::Instant::now() + timeout;
            while !h.is_finished() {
                if std::time::Instant::now() >= deadline {
                    // Leaked-by-design on timeout: process teardown reclaims.
                    tracing::warn!(root = %self.root.display(), "watch thread did not stop in time");
                    return false;
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            let _ = h.join();
        }
        true
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        self.request_stop();
        let _ = self.join(Duration::from_millis(250));
    }
}

/// Backend constructor contract. Every implementation must satisfy
/// the observable contract [`conformance`] exercises; running that suite is
/// the porting checklist for the next backend (FSEvents, USN).
///
/// Contract, independent of OS mechanism:
///
/// 1. **Paths**: every emitted [`FsEvent`] path is absolute and lies under
///    `root`. Events name *files*; directory creations may additionally be
///    reported as `Added`, but directory churn alone must not flood events.
/// 2. **Ignores**: paths matching the provided `IgnoreSet` are never
///    reported, in any event kind. (`.sheaf/` is always included by the
///    caller's set — the watcher never observes its own store.)
/// 3. **Discoverability**: new files under NEW subdirectories surface
///    without an out-of-band rescan, even when the file landed before the
///    backend could register the directory (the classic inotify
///    registration gap; each backend must close it its own way).
/// 4. **Truthfulness over pairing**: a rename SHOULD surface as
///    [`EventKind::Renamed`] when the platform gives the two halves; when it
///    cannot pair them, an honest `Removed` + `Added` is correct. Never
///    fabricate a pairing, never drop one side silently.
/// 5. **Stop**: once the stop flag is set, `run` returns within ~2 s. The
///    daemon's shutdown grace depends on it.
/// 6. **Burst coalescing is NOT the backend's job**: the debouncer owns
///    batching. Backends emit what the OS told them.
pub trait WatchBackend: Send + 'static {
    fn root(&self) -> &std::path::Path;

    fn run(self: Box<Self>, tx: Sender<FsEvent>, stop: StopFlag);
}

/// The ignore set the backend filters against, shared with its owner.
///
/// Rule sources are files (`.gitignore`, `.git/info/exclude`, config) that
/// users edit while the daemon runs, and a snapshot taken at watch start
/// silently goes stale the moment they do. Both sides hold this handle: the
/// backend read-locks it per event/scan, and the daemon swaps in a rebuilt
/// set when it observes a rule-file edit. A `RwLock` (not `Mutex`) because
/// the backend's read side sits on the hot event path.
pub type SharedIgnores = std::sync::Arc<std::sync::RwLock<IgnoreSet>>;

/// Wrap a compiled set in the shared handle.
pub fn shared_ignores(set: IgnoreSet) -> SharedIgnores {
    std::sync::Arc::new(std::sync::RwLock::new(set))
}

/// Convenience: build the platform default backend.
pub fn default_backend(root: PathBuf, ignores: SharedIgnores) -> Result<Box<dyn WatchBackend>> {
    Ok(Box::new(InotifySource::new(root, ignores)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::channel;
    use std::thread;

    struct QuietBackend {
        root: PathBuf,
    }

    impl WatchBackend for QuietBackend {
        fn root(&self) -> &std::path::Path {
            &self.root
        }

        fn run(self: Box<Self>, _tx: Sender<FsEvent>, stop: StopFlag) {
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                thread::sleep(Duration::from_millis(1));
            }
        }
    }
    #[test]
    fn stop_flag_and_shared_ignores_have_expected_defaults() {
        let stop = new_stop_flag();
        assert!(!stop.load(std::sync::atomic::Ordering::SeqCst));
        let shared = shared_ignores(IgnoreSet::empty());
        assert!(shared.read().is_ok());
    }

    #[test]
    fn watch_start_request_stop_join_and_drop() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().to_path_buf();
        let (tx, _rx) = channel();
        let mut watch = Watch::start(Box::new(QuietBackend { root: root.clone() }), tx).unwrap();
        assert_eq!(watch.root, root);
        watch.request_stop();
        assert!(watch.join(Duration::from_secs(1)));
        assert!(watch.join(Duration::from_millis(1)));
    }

    #[test]
    fn default_backend_rejects_non_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("file");
        std::fs::write(&file, "x").unwrap();
        let result = default_backend(file, shared_ignores(IgnoreSet::empty()));
        assert!(matches!(result, Err(SheafError::WatchInit { .. })));
    }

    struct SlowBackend {
        root: PathBuf,
    }
    impl WatchBackend for SlowBackend {
        fn root(&self) -> &std::path::Path {
            &self.root
        }
        fn run(self: Box<Self>, _tx: Sender<FsEvent>, _stop: StopFlag) {
            thread::sleep(Duration::from_millis(80));
        }
    }

    #[test]
    fn join_reports_timeout_then_reaps_finished_thread() {
        let tmp = tempfile::tempdir().unwrap();
        let (tx, _rx) = channel();
        let mut watch = Watch::start(
            Box::new(SlowBackend {
                root: tmp.path().into(),
            }),
            tx,
        )
        .unwrap();
        assert!(!watch.join(Duration::from_millis(1)));
        assert!(watch.join(Duration::from_millis(200)));
    }
}
