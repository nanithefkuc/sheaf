//! Raw inotify backend: recursive directory watching with explicit rename
//! cookie pairing, behind [`crate::watcher::WatchBackend`].
//!
//! Design notes:
//! - Non-blocking reads with a small sleep poll: trivially interruptible,
//!   CPU negligible relative to the debounce window.
//! - Directories get watches at creation/baseline; renames pair through
//!   inotify's `cookie`, with a timeout sweep so unpaired halves decay.
//! - The classic inotify registration gap (contents landing before a new
//!   directory's watch exists; wholesale move-ins) is closed by synthesizing
//!   `Added` events during runtime registrations — see `Sweep` below.
//! - `.sheaf/` is hard-ignored in addition to configured patterns: the
//!   watcher never observes its own store.

use std::collections::HashMap;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::mpsc::Sender;
use std::time::{Duration, Instant};

use chrono::Utc;
use inotify::{EventMask, Inotify, WatchMask};
use walkdir::WalkDir;

use crate::error::{Result, SheafError};
use crate::events::{EventKind, FsEvent, TouchedPath};

const READ_BUF_SIZE: usize = 64 * 1024;
/// Upper bound on one blocking poll of the inotify fd. The backend is idle
/// most of its life; poll(2) means that idleness costs zero wakeups until an
/// event or this timeout lands (the timeout exists so the stop flag and the
/// cookie-expiry sweep stay responsive while keeping idle CPU cost at zero).
const POLL_IDLE_MS: libc::c_int = 250;
/// How long an unpaired MOVED_FROM survives before resolving honestly as
/// Removed. Same-batch sweeps claim orphans immediately, so this bound
/// mostly caps truth-latency of differing-name splits, not pairing odds.
const COOKIE_TTL: Duration = Duration::from_millis(750);

/// The Linux inotify watch backend for one project root, carrying the ignore
/// set it filters emitted events against.
#[derive(Debug)]
pub struct InotifySource {
    root: PathBuf,
    ignores: crate::watcher::SharedIgnores,
}

const WATCH_MASK: WatchMask = WatchMask::CREATE
    .union(WatchMask::DELETE)
    .union(WatchMask::MOVED_FROM)
    .union(WatchMask::MOVED_TO)
    .union(WatchMask::MODIFY)
    .union(WatchMask::ATTRIB)
    .union(WatchMask::DELETE_SELF)
    .union(WatchMask::MOVE_SELF)
    .union(WatchMask::ONLYDIR);

impl InotifySource {
    /// Construct a backend rooted at `root`, failing if it is not a directory.
    pub fn new(root: PathBuf, ignores: crate::watcher::SharedIgnores) -> Result<Self> {
        if !root.is_dir() {
            return Err(SheafError::WatchInit {
                message: "project root missing or not a directory".into(),
                root,
            });
        }
        Ok(InotifySource { root, ignores })
    }
}

struct RunState {
    root: PathBuf,
    ignores: crate::watcher::SharedIgnores,
    ino: Inotify,
    tx: Sender<FsEvent>,
    /// watch descriptor -> absolute watched directory
    dirs: HashMap<inotify::WatchDescriptor, PathBuf>,
    /// rename cookie -> origin path awaiting its MOVED_TO half
    pending_moves: HashMap<u32, (PathBuf, Instant)>,
    /// Directories whose inhabitants need reporting once the current kernel
    /// batch finishes draining. Deferral matters: a MOVED_FROM sitting later
    /// in the SAME batch as the CREATE of the destination directory is the
    /// cross-watch rename race, and claiming it requires pending_moves to be
    /// fully populated first.
    pending_sweeps: Vec<PathBuf>,
}

impl RunState {
    fn add_watch(&mut self, dir: &Path) {
        if let Ok(rel) = dir.strip_prefix(&self.root) {
            if !rel.as_os_str().is_empty() && self.ignores.read().unwrap().is_ignored_rel(rel) {
                return;
            }
        }
        match self.ino.watches().add(dir, WATCH_MASK) {
            Ok(wd) => {
                self.dirs.insert(wd, dir.to_path_buf());
            }
            Err(e)
                if e.raw_os_error() == Some(17 /*EEXIST*/)
                    || e.kind() == std::io::ErrorKind::AlreadyExists =>
            {
                tracing::trace!(dir = %dir.display(), "watch already present");
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::debug!(dir = %dir.display(), "vanished before watching");
            }
            Err(e) => {
                tracing::warn!(dir = %dir.display(), error = %e, "failed to add watch");
            }
        }
    }

    fn register_tree_at(&mut self, base: &Path) {
        // Phase 1: collect registration targets under a read-only borrow.
        let targets: Vec<PathBuf> = {
            let ig = self.ignores.read().unwrap().clone();
            let root = self.root.clone();
            WalkDir::new(base)
                .follow_links(false)
                .into_iter()
                .filter_entry(move |e| match e.path().strip_prefix(&root) {
                    Ok(rel) => e.depth() == 0 || !ig.is_ignored_rel(rel),
                    Err(_) => false,
                })
                .filter_map(|e| e.ok())
                .filter(|e| e.file_type().is_dir())
                .map(|e| e.into_path())
                .collect()
        };
        // Phase 2: mutate — watches go in top-down so parents already observe
        // stragglers beneath children as those children register.
        for dir in &targets {
            self.add_watch(dir);
        }
    }

    /// Surface previously-invisible inhabitants of a freshly-watched
    /// directory tree. Runs at END OF BATCH (see `pending_sweeps`): the
    /// cross-watch rename race hides MOVED_FROM twins that may sit later
    /// in the same kernel batch as the destination's CREATE event.
    /// Discovered files claiming such an orphan become Renamed instead of
    /// Added, keeping renames first-class rather than surfacing as
    /// delete-plus-create.
    fn sweep_report(&mut self, base: &Path) {
        let discovered: Vec<PathBuf> = {
            let ig = self.ignores.read().unwrap().clone();
            let root = self.root.clone();
            let base_owned = base.to_path_buf();
            WalkDir::new(base)
                .follow_links(false)
                .into_iter()
                // NOTE: filter_entry prunes whole subtrees on false, so do
                // NOT gate on depth here — the base itself must pass or
                // nothing beneath it is ever visited.
                .filter_entry(move |e| match e.path().strip_prefix(&root) {
                    Ok(rel) => rel.as_os_str().is_empty() || !ig.is_ignored_rel(rel),
                    Err(_) => false,
                })
                .filter_map(|e| e.ok())
                .filter(move |e| e.depth() > 0 && e.path() != base_owned)
                .map(|e| e.into_path()) // files AND nested dirs alike
                .collect()
        };
        for p in discovered {
            match self.claim_orphan_move(&p) {
                Some(from) => self.emit(EventKind::Renamed { from, to: p }),
                None => self.emit(EventKind::Added { path: p }),
            }
        }
    }

    /// If a pending MOVED_FROM explains this swept path, consume and return
    /// its origin. Strictly same-filename + source-gone + fresh-cookie:
    /// mispairing renames would poison the append-only timeline, so the
    /// heuristic stays conservative — different-name moves decompose into
    /// Removed+Added via orphan expiry instead of being guessed at.
    fn claim_orphan_move(&mut self, discovered: &Path) -> Option<PathBuf> {
        let want = discovered.file_name()?.to_os_string();
        let now = Instant::now();
        let mut best: Option<(u32, PathBuf)> = None;
        for (&cookie, (origin, seen_at)) in &self.pending_moves {
            if origin.file_name().as_ref()? != &want {
                continue;
            }
            if origin == discovered || origin.exists() {
                continue; // still there — not the moved-away twin
            }
            if now.duration_since(*seen_at) > COOKIE_TTL {
                continue;
            }
            match best {
                Some((c, _)) if c <= cookie => {}
                _ => best = Some((cookie, origin.clone())),
            }
        }
        let (cookie, origin) = best?;
        self.pending_moves.remove(&cookie);
        Some(origin)
    }

    /// Resolve cookies whose twin never arrived: each expired MOVED_FROM
    /// becomes an honest Removed so structural truth never silently decays.
    fn expire_stale_moves(&mut self) {
        if self.pending_moves.is_empty() {
            return;
        }
        let now = Instant::now();
        let expired: Vec<(u32, PathBuf)> = self
            .pending_moves
            .iter()
            .filter(|(_, (_, t))| now.duration_since(*t) > COOKIE_TTL)
            .map(|(c, (p, _))| (*c, p.clone()))
            .collect();
        for (cookie, origin) in expired {
            self.pending_moves.remove(&cookie);
            self.emit(EventKind::Removed { path: origin });
        }
    }

    fn purge_descendants(&mut self, dir: &Path) {
        self.dirs.retain(|_, p| !p.starts_with(dir));
    }

    /// Emit one event through ignore filtering. Receiver-gone (shutdown)
    /// silently drops — correct behavior, not an error.
    fn emit(&mut self, kind: EventKind) {
        let probe: &Path = match &kind {
            EventKind::Added { path } => path,
            EventKind::Removed { path } => path,
            EventKind::Renamed { to, .. } => to,
            EventKind::Touched { path } => &path.0,
        };
        if let Ok(rel) = probe.strip_prefix(&self.root) {
            if self.ignores.read().unwrap().is_ignored_rel(rel) {
                return;
            }
        }
        let _ = self.tx.send(FsEvent {
            kind,
            at: Utc::now(),
        });
    }

    fn handle_event(
        &mut self,
        wd_dir: PathBuf,
        name: Option<std::ffi::OsString>,
        mask: EventMask,
        cookie: u32,
    ) {
        if mask.contains(EventMask::Q_OVERFLOW) {
            tracing::warn!(root = %self.root.display(), "kernel queue overflow: rescanning");
            self.pending_moves.clear();
            let base = self.root.clone();
            self.register_tree_at(&base);
            return;
        }
        if mask.contains(EventMask::IGNORED) {
            // Descriptor already removed during map lookup phase.
            return;
        }
        if mask.contains(EventMask::DELETE_SELF) || mask.contains(EventMask::MOVE_SELF) {
            self.purge_descendants(&wd_dir);
            return; // parent observes structural truth via child events
        }
        let Some(name) = name else { return };
        let path = wd_dir.join(&name);
        let now = Instant::now();

        if mask.contains(EventMask::MOVED_FROM) {
            self.pending_moves.insert(cookie, (path, now));
        } else if mask.contains(EventMask::MOVED_TO) {
            if let Some((from, _)) = self.pending_moves.remove(&cookie) {
                self.emit(EventKind::Renamed {
                    from,
                    to: path.clone(),
                });
            } else {
                self.emit(EventKind::Added { path: path.clone() });
            }
            if path.is_dir() {
                // Moved-in subtree: interior was never watched at its old
                // location relative to us — sweep its real contents in.
                self.register_tree_at(&path);
                self.pending_sweeps.push(path);
            }
        } else if mask.contains(EventMask::CREATE) {
            let is_dir = path.is_dir();
            self.emit(EventKind::Added { path: path.clone() });
            if is_dir {
                self.register_tree_at(&path);
                self.pending_sweeps.push(path);
            }
        } else if mask.contains(EventMask::DELETE) {
            if path.is_dir() {
                self.purge_descendants(&path);
            }
            self.emit(EventKind::Removed { path });
        } else if mask.contains(EventMask::MODIFY) {
            self.emit(EventKind::Touched {
                path: TouchedPath(path),
            });
        } else if mask.contains(EventMask::ATTRIB) {
            // Metadata changed: chmod/chown/timestamps. Content-affecting
            // metadata (the exec bit) is history-worthy — the store dedupes
            // pure mtime noise by comparing content and mode at flush time,
            // so emitting here is safe and keeps `chmod +x` recoverable.
            if path.is_file() {
                self.emit(EventKind::Touched {
                    path: TouchedPath(path),
                });
            }
        }
    }
}

impl Drop for InotifySource {
    fn drop(&mut self) {}
}

impl crate::watcher::WatchBackend for InotifySource {
    fn root(&self) -> &Path {
        &self.root
    }

    fn run(self: Box<Self>, tx: Sender<FsEvent>, stop: crate::watcher::StopFlag) {
        let ino = match Inotify::init() {
            Ok(i) => i,
            Err(e) => {
                tracing::error!(root = %self.root.display(), error = %e, "inotify init failed");
                return;
            }
        };
        // inotify 0.10 instances are born non-blocking; read_events()
        // returns WouldBlock when the queue is empty.

        let mut st = RunState {
            root: self.root.clone(),
            ignores: self.ignores.clone(),
            ino,
            tx,
            dirs: HashMap::new(),
            pending_moves: HashMap::new(),
            pending_sweeps: Vec::new(),
        };

        let baseline_root = st.root.clone();
        st.register_tree_at(&baseline_root);
        tracing::debug!(
            root = %st.root.display(),
            dirs_watched = st.dirs.len(),
            "baseline watch established"
        );

        let mut buf = vec![0u8; READ_BUF_SIZE];

        loop {
            if stop.load(Ordering::SeqCst) {
                tracing::debug!(root = %st.root.display(), "watch stopping");
                break;
            }

            // Expiry first: runs every cycle including idle ones (the
            // WouldBlock continue below must never starve it).
            st.expire_stale_moves();

            // Block on the inotify fd instead of spin-sleeping: no events
            // and no stop request means this thread costs nothing. The
            // timeout keeps the expiry sweep and the stop check serviced
            // (keeps idle CPU at zero; stop latency stays ≤ 250 ms).
            let wait_rc = unsafe {
                libc::poll(
                    &mut libc::pollfd {
                        fd: st.ino.as_raw_fd(),
                        events: libc::POLLIN,
                        revents: 0,
                    },
                    1,
                    POLL_IDLE_MS,
                )
            };
            if wait_rc < 0 {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::Interrupted {
                    continue;
                }
                tracing::error!(error = %err, "inotify poll failed; watch aborting");
                break;
            }

            let events = match st.ino.read_events(&mut buf) {
                Ok(evts) => evts.collect::<Vec<_>>(),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    tracing::error!(error = %e, "inotify read failed; watch aborting");
                    break;
                }
            };

            // Snapshot only the raw event data (wd ids are opaque handles);
            // path resolution happens per-event against the LIVE map, because
            // an earlier event in this very batch may have registered the
            // directory a later event refers to (mkdir+populate within one
            // poll cycle). Resolving up-front drops those silently.
            let raw: Vec<(
                inotify::WatchDescriptor,
                Option<std::ffi::OsString>,
                EventMask,
                u32,
            )> = events
                .into_iter()
                .map(|ev| {
                    (
                        ev.wd.clone(),
                        ev.name.map(|n| n.to_os_string()),
                        ev.mask,
                        ev.cookie,
                    )
                })
                .collect();
            for (wd, name, mask, cookie) in raw {
                if mask.contains(EventMask::IGNORED) {
                    st.dirs.remove(&wd);
                    continue;
                }
                let Some(dir) = st.dirs.get(&wd).cloned() else {
                    tracing::trace!(
                        ?mask,
                        "event for untracked descriptor; awaiting sweep repair"
                    );
                    continue;
                };
                st.handle_event(dir, name, mask, cookie);
            }

            // End of kernel batch: report deferred discoveries now that
            // pending_moves is fully populated for this batch.
            for base in std::mem::take(&mut st.pending_sweeps) {
                st.sweep_report(&base);
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::channel;

    fn state(
        root: &Path,
        ignores: crate::watcher::SharedIgnores,
    ) -> (RunState, std::sync::mpsc::Receiver<FsEvent>) {
        let (tx, rx) = channel();
        let ino = Inotify::init().unwrap();
        (
            RunState {
                root: root.to_path_buf(),
                ignores,
                ino,
                tx,
                dirs: HashMap::new(),
                pending_moves: HashMap::new(),
                pending_sweeps: Vec::new(),
            },
            rx,
        )
    }

    fn shared(patterns: &[&str]) -> crate::watcher::SharedIgnores {
        crate::watcher::shared_ignores(
            crate::ignore::IgnoreSet::from_patterns(
                &patterns.iter().map(|p| (*p).into()).collect::<Vec<_>>(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn rejects_missing_root() {
        let tmp = tempfile::tempdir().unwrap();
        let err = InotifySource::new(tmp.path().join("gone"), shared(&[])).unwrap_err();
        assert!(matches!(err, SheafError::WatchInit { .. }));
    }
    #[test]
    fn handles_edge_masks_and_filters_ignored_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        std::fs::create_dir(root.join("ignored")).unwrap();
        let (mut st, rx) = state(root, shared(&["ignored/"]));
        st.add_watch(root);
        st.add_watch(root);
        let watched = st.dirs.len();
        st.add_watch(&root.join("ignored"));
        assert_eq!(st.dirs.len(), watched);
        st.add_watch(&root.join("vanished"));
        let file = root.join("file.txt");
        std::fs::write(&file, "x").unwrap();
        st.add_watch(&file);
        st.handle_event(
            root.to_path_buf(),
            Some("file.txt".into()),
            EventMask::CREATE,
            0,
        );
        st.handle_event(
            root.to_path_buf(),
            Some("file.txt".into()),
            EventMask::MODIFY,
            0,
        );
        st.handle_event(
            root.to_path_buf(),
            Some("file.txt".into()),
            EventMask::ATTRIB,
            0,
        );
        st.handle_event(
            root.to_path_buf(),
            Some("ignored".into()),
            EventMask::CREATE,
            0,
        );
        st.handle_event(root.to_path_buf(), None, EventMask::CREATE, 0);
        st.handle_event(root.to_path_buf(), None, EventMask::Q_OVERFLOW, 0);
        st.handle_event(root.to_path_buf(), None, EventMask::IGNORED, 0);
        st.handle_event(root.to_path_buf(), None, EventMask::DELETE_SELF, 0);
        let moved = root.join("moved");
        std::fs::create_dir(&moved).unwrap();
        st.handle_event(
            root.to_path_buf(),
            Some("moved".into()),
            EventMask::MOVED_TO,
            42,
        );
        st.handle_event(
            root.to_path_buf(),
            Some("moved".into()),
            EventMask::ATTRIB,
            0,
        );
        let events: Vec<_> = rx.try_iter().collect();
        assert!(events
            .iter()
            .any(|e| matches!(e.kind, EventKind::Added { .. })));
        assert!(events
            .iter()
            .any(|e| matches!(e.kind, EventKind::Touched { .. })));
        assert!(!events
            .iter()
            .any(|e| e.path().starts_with(root.join("ignored"))));
    }

    #[test]
    fn pairs_swept_orphans_and_expires_unpaired_moves() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let (mut st, rx) = state(root, shared(&[]));
        let origin = root.join("old.txt");
        let destination = root.join("new");
        std::fs::create_dir(&destination).unwrap();
        st.pending_moves.insert(7, (origin.clone(), Instant::now()));
        std::fs::write(destination.join("old.txt"), "x").unwrap();
        st.sweep_report(&destination);
        assert!(
            matches!(rx.try_recv().unwrap().kind, EventKind::Renamed { from, to } if from == origin && to.ends_with("old.txt"))
        );

        let expired = root.join("expired.txt");
        st.pending_moves.insert(
            8,
            (
                expired.clone(),
                Instant::now() - COOKIE_TTL - Duration::from_millis(1),
            ),
        );
        st.expire_stale_moves();
        assert!(
            matches!(rx.try_recv().unwrap().kind, EventKind::Removed { path } if path == expired)
        );

        let direct = root.join("direct.txt");
        std::fs::write(&direct, "x").unwrap();
        st.handle_event(
            root.to_path_buf(),
            Some("direct.txt".into()),
            EventMask::MOVED_TO,
            99,
        );
        assert!(matches!(rx.try_recv().unwrap().kind, EventKind::Added { path } if path == direct));
        let dir = root.join("nested");
        std::fs::create_dir(&dir).unwrap();
        st.handle_event(
            root.to_path_buf(),
            Some("nested".into()),
            EventMask::CREATE,
            0,
        );
        st.handle_event(
            root.to_path_buf(),
            Some("nested".into()),
            EventMask::DELETE,
            0,
        );
        assert!(st.claim_orphan_move(&direct).is_none());
        let present = root.join("present.txt");
        std::fs::write(&present, "x").unwrap();
        st.pending_moves
            .insert(10, (present.clone(), Instant::now()));
        assert!(st.claim_orphan_move(&present).is_none());
        let stale = root.join("stale.txt");
        st.pending_moves.insert(
            11,
            (
                stale.clone(),
                Instant::now() - COOKIE_TTL - Duration::from_millis(1),
            ),
        );
        assert!(st.claim_orphan_move(&stale).is_none());
    }
}
