//! sheafd — the sheaf daemon.
//!
//! Responsibilities:
//!   - Socket lifecycle: bind the per-user control socket, probe and
//!     replace a stale incumbent, and serve clients over it.
//!   - Collector loop: own the single writer thread that drains watcher
//!     events, debounces write bursts, and applies every timeline mutation
//!     so persistence is never contended.
//!   - Capture pipeline: turn debounced worktree changes into persisted
//!     captures, including boot replay of edits made while the daemon was
//!     down and periodic snapshot compaction.
//!   - IPC dispatch: decode client requests and route them to the
//!     collector, covering timeline, checkpoint, diff, grep, restore, and
//!     squash methods.

use std::collections::{BTreeMap, HashMap};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{bail, Context as _, Result};
use clap::Parser;
use serde_json::json;

use sheaf_core::config::sheaf_dir;
use sheaf_core::config::{self, ProjectConfig};
use sheaf_core::debounce::{Debouncer, DebouncerConfig};
use sheaf_core::ipc::{self, IpcError, Request, Response, MAX_ENVELOPE, PROTO_MAJOR, PROTO_MINOR};
use sheaf_core::registry::Registry;
use sheaf_core::store::{ProjectStore, StoreLimits};
use sheaf_core::watcher::{self, StopFlag};

/// Concurrent connection ceiling.
const MAX_CONNS: usize = 32;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(2);
const REQUEST_SOFT: Duration = Duration::from_secs(10);
/// `restore.apply` hard deadline.
const RESTORE_HARD: Duration = Duration::from_secs(120);
/// Diffing a large tree materializes two whole points and runs a line
/// differ per changed file — a legitimately heavier read than a log walk.
const DIFF_HARD: Duration = Duration::from_secs(30);
/// Grep budget ceilings the daemon clamps client requests to.
const GREP_MAX_RESULTS: usize = 10_000;
const GREP_MAX_MATERIALIZED_BYTES: u64 = 256 * 1024 * 1024;
const GREP_MAX_ELAPSED_MS: u64 = 15_000;
/// How long watcher threads may take to notice the stop flag.
const WATCH_STOP_GRACE: Duration = Duration::from_millis(1500);
/// How long the collector may take to flush its debounce tail on shutdown.
/// The final drain fsyncs a journal frame per pending batch; a burst that
/// started a second before SIGTERM must not be sacrificed at the finish line.
const TAIL_FLUSH_GRACE: Duration = Duration::from_secs(15);
/// How long the writer disregards the watcher echo of its own restore.
const RESTORE_MUTE: Duration = Duration::from_secs(5);
/// Plans a collector remembers so `restore.apply` can name one by token.
const PLAN_CACHE: usize = 16;

#[derive(Parser)]
#[command(name = "sheafd", version, about = "sheaf daemon")]
struct Args {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(clap::Subcommand)]
enum Cmd {
    /// Run the daemon in the foreground.
    Run {
        /// Override control socket location (tests).
        #[arg(long)]
        socket: Option<PathBuf>,
    },
}

fn main() -> Result<()> {
    let args = Args::parse();
    match args.cmd {
        Cmd::Run { socket } => run_daemon(socket),
    }
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    // Loro's export/block-encode diagnostics are per-chunk INFO spam that
    // floods journald during any sizeable capture; our own INFO stays.
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,loro=warn,loro_internal=warn"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}

// ------------------------------------------------------------------ state

type WatchTable = Arc<Mutex<HashMap<PathBuf, WatchEntry>>>;

struct WatchEntry {
    stop: StopFlag,
    /// True until the project's store has been opened on this daemon run.
    /// Cold projects watch the worktree (kernel-side, cheap) but hold no
    /// Loro document, no grep caches, and no writer flock in memory — the
    /// store opens on the first worktree event or IPC command.
    cold: Arc<AtomicBool>,
    /// False while boot reconciliation owns the writer before the command
    /// loop starts. IPC verbs fail explicitly instead of silently queueing
    /// (cold projects queue: their open is triggered by the wake channel).
    ready: Arc<AtomicBool>,
    /// Watcher thread(s): event producers. They exit on the stop flag.
    watch_handles: Vec<std::thread::JoinHandle<()>>,
    /// Every physical worktree feeds the same collector through this channel.
    events: Sender<sheaf_core::events::FsEvent>,

    /// The project's sole writer. It exits when the event channel hangs up,
    /// then flushes its debounce tail — hence the separate, longer grace.
    collector: Option<std::thread::JoinHandle<()>>,
    control: Sender<StoreCommand>,
    /// Lazy-open trigger: IPC commands to a cold project send here so the
    /// collector wakes, opens the store, and only then drains the command.
    wake: Sender<()>,
}

/// One message from the collector's grep walk to the connection writer:
/// an incrementally finalized record, or the terminal outcome.
enum GrepStreamItem {
    Record(sheaf_core::store::GrepStreamRecord),
    Done(std::result::Result<sheaf_core::store::GrepReport, sheaf_core::SheafError>),
}

/// What a dispatched method leaves behind for the connection: a fully
/// computed body, or a live channel whose items become body frames as
/// they arrive.
enum IpcBody {
    Bytes(Vec<u8>),
    Stream(Receiver<GrepStreamItem>),
}

enum StoreCommand {
    /// Run one command with the document checked out at this physical
    /// worktree's advisory head.
    InWorktree {
        root: PathBuf,
        command: Box<StoreCommand>,
    },

    TimelineLog {
        all: bool,
        branch: Option<String>,
        path: Option<PathBuf>,
        follow: bool,
        limit: usize,
        reply: Sender<
            std::result::Result<(Vec<sheaf_core::store::Capture>, usize), sheaf_core::SheafError>,
        >,
    },
    CaptureLogDetails {
        references: Vec<String>,
        reply: Sender<
            std::result::Result<Vec<sheaf_core::store::CaptureLogDetail>, sheaf_core::SheafError>,
        >,
    },
    CaptureInfo {
        reference: String,
        reply: Sender<std::result::Result<sheaf_core::store::CaptureInfo, sheaf_core::SheafError>>,
    },
    ListCheckpoints {
        reply:
            Sender<std::result::Result<Vec<sheaf_core::store::Checkpoint>, sheaf_core::SheafError>>,
    },
    CreateCheckpoint {
        name: String,
        reference: Option<String>,
        reply: Sender<std::result::Result<sheaf_core::store::Checkpoint, sheaf_core::SheafError>>,
    },
    ListBranches {
        reply: Sender<std::result::Result<Vec<sheaf_core::store::Branch>, sheaf_core::SheafError>>,
    },
    BranchGraph {
        reply: Sender<std::result::Result<sheaf_core::store::BranchGraph, sheaf_core::SheafError>>,
    },
    CreateBranch {
        name: String,
        reference: Option<String>,
        metadata: BTreeMap<String, String>,
        reply: Sender<std::result::Result<sheaf_core::store::Branch, sheaf_core::SheafError>>,
    },
    RenameBranch {
        old_name: String,
        new_name: String,
        reply: Sender<std::result::Result<sheaf_core::store::Branch, sheaf_core::SheafError>>,
    },
    DeleteBranch {
        name: String,
        reply: Sender<std::result::Result<sheaf_core::store::Branch, sheaf_core::SheafError>>,
    },
    /// Dry-run a restore. Pure computation; never touches the worktree.
    PlanRestore {
        reference: String,
        scope: Vec<String>,
        reply: Sender<std::result::Result<sheaf_core::store::RestorePlan, sheaf_core::SheafError>>,
    },
    /// Execute a plan this collector previously handed out: apply takes the
    /// token, never ad-hoc arguments, so it can only re-run a plan the
    /// daemon itself computed.
    ApplyRestore {
        token: String,
        reply:
            Sender<std::result::Result<sheaf_core::store::RestoreOutcome, sheaf_core::SheafError>>,
    },
    /// Operator verb: finish an interrupted restore on demand, overriding
    /// the staleness bound that gates automatic boot replay.
    ResumeRestore {
        reply:
            Sender<std::result::Result<sheaf_core::store::RestoreOutcome, sheaf_core::SheafError>>,
    },
    /// Operator verb: drop an outstanding intent and keep the worktree
    /// exactly as it stands, reconciling whatever the interrupted restore
    /// already wrote into ordinary history.
    AbandonRestore {
        reply:
            Sender<std::result::Result<Option<sheaf_core::store::Capture>, sheaf_core::SheafError>>,
    },
    /// Retention: compute (or apply) the reachability-constrained GC plan
    /// on the writer thread, where the store is exclusively ours. Apply
    /// includes the shallow-snapshot compaction retention trim.
    Gc {
        apply: bool,
        reply: Sender<std::result::Result<sheaf_core::store::GcOutcome, sheaf_core::SheafError>>,
    },
    /// Retention: record an explicit `gc mark <ref>` on the writer thread,
    /// where the ledger append is exclusive. An explicit mark is the one
    /// path that bypasses reachability-bound automatic expiry.
    Mark {
        reference: String,
        reply:
            Sender<std::result::Result<sheaf_core::store::MarkedCapture, sheaf_core::SheafError>>,
    },
    /// Integrity sweep; with `fix`, the bounded repair verb. Runs in the
    /// collector thread so it never races the writer's own appends —
    /// journal truncation under a live appender would corrupt.
    Doctor {
        fix: bool,
        reply: Sender<std::result::Result<sheaf_core::store::DoctorReply, sheaf_core::SheafError>>,
    },
    /// Compare a point against another point or the live worktree.
    Diff {
        from: String,
        to: Option<String>,
        paths: Vec<String>,
        reply: Sender<std::result::Result<sheaf_core::store::DiffOutcome, sheaf_core::SheafError>>,
    },
    /// Read-only literal timeline grep. Runs on the collector like
    /// every other document read; hits stream out as NDJSON body chunks.
    /// Since proto 1.5 the chunks are produced incrementally: each finalized
    /// record is sent the moment the walk emits it (GNU-grep liveness), with
    /// the summary as the last record before the terminator.
    Grep {
        request: Box<sheaf_core::store::GrepRequest>,
        reply: Sender<GrepStreamItem>,
    },
    /// Explicitly backfill/rebuild the derived grep cache on the
    /// collector thread, which owns the store. Bounded per call by the
    /// handler so one request cannot occupy the collector indefinitely;
    /// callers loop on the report's `complete` flag.
    CacheBackfill {
        opts: sheaf_core::store::GrepBackfillOptions,
        reply: Sender<
            std::result::Result<sheaf_core::store::GrepBackfillReport, sheaf_core::SheafError>,
        >,
    },
    /// Dry-run a selection-scoped fragment restore. Pure computation;
    /// never touches the worktree.
    PlanFragment {
        selections: Vec<sheaf_core::store::SelectionHandle>,
        mode: sheaf_core::store::FragmentMode,
        reply: Sender<std::result::Result<sheaf_core::store::FragmentPlan, sheaf_core::SheafError>>,
    },
    /// Execute a fragment plan this collector previously handed out, under
    /// the same token discipline as `ApplyRestore`.
    ApplyFragment {
        token: String,
        reply:
            Sender<std::result::Result<sheaf_core::store::RestoreOutcome, sheaf_core::SheafError>>,
    },
    /// Smart-squash planning. Two phases over one method: with
    /// no `head_texts` the collector answers the candidate destination
    /// paths whose HEAD content the caller must fetch; with them, the
    /// plan itself. Pure computation.
    PlanSmart {
        selections: Vec<sheaf_core::store::SelectionHandle>,
        head_texts: Option<std::collections::BTreeMap<String, String>>,
        reply: Sender<std::result::Result<SmartPlanReply, sheaf_core::SheafError>>,
    },
    ListWorktrees {
        reply: Sender<
            std::result::Result<Vec<sheaf_core::store::WorktreeInfo>, sheaf_core::SheafError>,
        >,
    },
    AddWorktree {
        reference: String,
        destination: PathBuf,
        reply: Sender<std::result::Result<sheaf_core::store::WorktreeInfo, sheaf_core::SheafError>>,
    },
    PlanMerge {
        source: String,
        reply: Sender<std::result::Result<sheaf_core::store::MergePlan, sheaf_core::SheafError>>,
    },
    ApplyMerge {
        token: String,
        reply: Sender<std::result::Result<sheaf_core::store::MergeOutcome, sheaf_core::SheafError>>,
    },
    ResumeMerge {
        reply: Sender<std::result::Result<sheaf_core::store::MergeOutcome, sheaf_core::SheafError>>,
    },
}

/// The collector side of `smart.plan`: phase one names candidate paths,
/// phase two returns the plan.
enum SmartPlanReply {
    Paths(Vec<String>),
    Plan(Box<sheaf_core::store::SmartPlan>),
}

struct Shared {
    table: WatchTable,
    stopping: AtomicBool,
    conns: AtomicUsize,
    socket_path: PathBuf,
    /// Write end of the shutdown self-pipe. Any thread that flips `stopping`
    /// (the `shutdown` IPC verb, the accept loop on signal) writes one byte
    /// here so the poll in the accept loop wakes immediately — no busy-wait,
    /// no missed wakeup, keeping the idle daemon off the CPU.
    wake_fd: std::os::unix::io::RawFd,
}

impl Shared {
    fn request_stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
        // Best-effort wake; the byte content is irrelevant.
        let byte = [1u8];
        let _ = unsafe { libc::write(self.wake_fd, byte.as_ptr().cast(), 1) };
    }

    fn watching(&self, root: &Path) -> bool {
        self.table.lock().unwrap().contains_key(&normalize(root))
    }

    fn cold(&self, root: &Path) -> bool {
        self.table
            .lock()
            .unwrap()
            .get(&normalize(root))
            .is_some_and(|entry| entry.cold.load(Ordering::Acquire))
    }

    fn ready(&self, root: &Path) -> bool {
        self.table
            .lock()
            .unwrap()
            .get(&normalize(root))
            .is_some_and(|entry| entry.ready.load(Ordering::Acquire))
    }

    fn registered_anywhere(&self, root: &Path) -> anyhow::Result<bool> {
        let reg = Registry::global()?;
        Ok(reg.list()?.iter().any(|e| same_root(&e.root, root)))
    }
}

fn normalize(p: &Path) -> PathBuf {
    p.canonicalize().unwrap_or_else(|_| p.to_path_buf())
}

fn same_root(a: &Path, b: &Path) -> bool {
    normalize(a) == normalize(b)
}

// ------------------------------------------------------------------ daemon

fn run_daemon(socket_override: Option<PathBuf>) -> Result<()> {
    init_tracing();
    disable_thp_for_this_process();

    let socket_path = socket_override.unwrap_or_else(sheaf_core::paths::control_socket_path);
    // One daemon per enrollment registry, regardless of socket path, before
    // any socket is probed or bound. Held until the process exits.
    let _registry_singleton = acquire_registry_singleton(&socket_path)?;
    let runtime_dir = socket_path
        .parent()
        .ok_or_else(|| anyhow::anyhow!("socket path has no parent"))?
        .to_path_buf();
    std::fs::create_dir_all(&runtime_dir)?;
    set_mode(&runtime_dir, 0o700)?;

    // Stale-socket protocol: probe incumbent, replace if dead.
    if socket_path.exists() {
        match probe_incumbent(&socket_path) {
            Ok(_) => bail!("daemon already running on {}", socket_path.display()),
            Err(_) => {
                tracing::info!("removing dead socket");
                let _ = std::fs::remove_file(&socket_path);
            }
        }
    }

    let listener = UnixListener::bind(&socket_path)
        .with_context(|| format!("bind {}", socket_path.display()))?;
    listener.set_nonblocking(true)?;
    set_mode(&socket_path, 0o600)?;

    // Shutdown self-pipe: SIGINT/SIGTERM write a byte through signal-hook's
    // async-signal-safe registration; the accept loop polls listener + pipe
    // and sleeps only when there is genuinely nothing to do. Idle CPU then
    // measures zero periodic wakeups, which is the idle-CPU budget.
    let mut pipe_fds = [0 as libc::c_int; 2];
    if unsafe { libc::pipe(pipe_fds.as_mut_ptr()) } != 0 {
        return Err(anyhow::anyhow!(
            "shutdown pipe: {}",
            std::io::Error::last_os_error()
        ));
    }
    let (pipe_read, pipe_write) = (pipe_fds[0], pipe_fds[1]);
    // Each registration takes ownership of (and may close) its own dup'd fd.
    for sig in [signal_hook::consts::SIGINT, signal_hook::consts::SIGTERM] {
        let fd = unsafe { libc::dup(pipe_write) };
        if fd < 0 {
            return Err(anyhow::anyhow!(
                "dup shutdown pipe: {}",
                std::io::Error::last_os_error()
            ));
        }
        signal_hook::low_level::pipe::register(sig, fd)
            .map_err(|e| anyhow::anyhow!("signal registration: {e}"))?;
    }

    let shared = Arc::new(Shared {
        table: Arc::new(Mutex::new(HashMap::new())),
        stopping: AtomicBool::new(false),
        conns: AtomicUsize::new(0),
        socket_path: socket_path.clone(),
        wake_fd: pipe_write,
    });

    // Resume enrollment across restarts. A registry entry whose
    // root no longer exists on disk is a deleted project (a scratch or
    // eval checkout that has been cleaned up): it can never capture
    // anything again, and dead entries otherwise accumulate forever. Prune
    // them with a loud log so `sheaf init` can always re-enroll. The
    // boundary is deliberate: a root that EXISTS but has a damaged or
    // unreadable store is only warned about, never forgotten — damage may
    // be repairable, and that call belongs to the operator, not startup.
    let registry = Registry::global()?;
    let (resumed, pruned) = resume_enrollments(&shared, &registry);
    tracing::info!(
        socket = %socket_path.display(),
        projects_resumed = resumed,
        projects_pruned = pruned,
        "sheafd listening"
    );

    // Accept loop: poll(2) on {listener, shutdown pipe}. Blocks indefinitely
    // while idle — signals and the shutdown verb both write the pipe, so
    // every wake has work waiting (the idle-CPU budget).
    use std::os::unix::io::AsRawFd;
    loop {
        if shared.stopping.load(Ordering::SeqCst) {
            return graceful_shutdown(shared, listener);
        }
        let mut fds = [
            libc::pollfd {
                fd: listener.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: pipe_read,
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        let ready = unsafe { libc::poll(fds.as_mut_ptr(), 2, -1) };
        if ready < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(anyhow::anyhow!("accept poll failed: {err}"));
        }
        if fds[1].revents & libc::POLLIN != 0 {
            // Drain the pipe (multiple signals may have coalesced).
            let mut buf = [0u8; 64];
            unsafe { libc::read(pipe_read, buf.as_mut_ptr().cast(), buf.len()) };
            tracing::info!("shutdown signal received");
            return graceful_shutdown(shared, listener);
        }
        if fds[0].revents & libc::POLLIN != 0 {
            // Accept everything currently queued; the fd is non-blocking.
            loop {
                match listener.accept() {
                    Ok((stream, _addr)) => {
                        if shared.conns.load(Ordering::SeqCst) >= MAX_CONNS {
                            tracing::debug!("connection cap reached; dropping client");
                            drop(stream);
                            continue;
                        }
                        let sh = shared.clone();
                        shared.conns.fetch_add(1, Ordering::SeqCst);
                        std::thread::Builder::new()
                            .name("conn".into())
                            .spawn(move || {
                                let _ = serve_connection(sh.clone(), stream);
                                sh.conns.fetch_sub(1, Ordering::SeqCst);
                            })
                            .ok();
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(e) => {
                        tracing::warn!(error = %e, "accept failed");
                        break;
                    }
                }
            }
        }
    }
}

fn graceful_shutdown(shared: Arc<Shared>, listener: UnixListener) -> Result<()> {
    shared.stopping.store(true, Ordering::SeqCst);

    // Phase 1: stop the event producers so no new batch can start.
    {
        let mut table = shared.table.lock().unwrap();
        for entry in table.values_mut() {
            entry.stop.store(true, Ordering::SeqCst);
            for h in entry.watch_handles.drain(..) {
                wait_bounded(h, WATCH_STOP_GRACE, "watch");
            }
        }
    }
    // Phase 2: give every collector its debounce tail. This is the "clean
    // shutdown flush" contract: a burst that started before SIGTERM still
    // lands in the journal with its fsync, instead of dying at the socket.
    {
        let mut table = shared.table.lock().unwrap();
        for (root, mut entry) in table.drain() {
            let collector = entry.collector.take();
            // Release this entry's stored event sender before waiting. The
            // watcher threads (joined in phase 1) already dropped their
            // clones; this `events` handle is the last sender keeping the
            // collector's channel open. Held here, the collector never sees
            // its channel disconnect and blocks the full tail-flush grace on
            // every shutdown. Dropping the entry frees it so the collector
            // drains its tail and exits at once.
            drop(entry);
            if let Some(h) = collector {
                wait_bounded(h, TAIL_FLUSH_GRACE, &root.display().to_string());
            }
        }
    }
    drop(listener);
    // Socket removal last so probes never see live-but-exiting states.
    let _ = std::fs::remove_file(&shared.socket_path);
    tracing::info!("bye");
    std::process::exit(0);
}

fn wait_bounded(h: std::thread::JoinHandle<()>, limit: Duration, what: &str) {
    let deadline = Instant::now() + limit;
    while !h.is_finished() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(5));
    }
    if !h.is_finished() {
        tracing::warn!(
            what,
            grace_ms = limit.as_millis() as u64,
            "thread exceeded grace; abandoning to process teardown"
        );
    }
}

fn probe_incumbent(socket: &Path) -> Result<(u32, u32)> {
    let mut c = ipc::Client::connect(socket, Duration::from_millis(500))?;
    let (major, minor, _) = c.ping()?;
    Ok((major, minor))
}

/// Registry-scoped daemon singleton, held for the daemon's whole lifetime.
///
/// The per-socket incumbency probe above only guards one socket path, but
/// enrollment is scoped to the data home: two daemons started with
/// different `--socket` overrides (or different runtime dirs) share one
/// enrollment registry and would both watch — and contend to write — the
/// same projects. The per-store writer flock still keeps each journal
/// single-writer, but the duplicate watchers, the open/refuse/park churn
/// under load, and two processes each replaying a large store (tens of
/// GiB of RSS) are exactly the failure this lock prevents. The kernel
/// releases the flock on exit or crash, so a leftover lock file can never
/// block a restart.
fn acquire_registry_singleton(socket_path: &Path) -> Result<std::fs::File> {
    acquire_registry_singleton_at(&sheaf_core::paths::data_sheaf_dir()?, socket_path)
}

/// Path-injected core of [`acquire_registry_singleton`] so tests can pin
/// the registry scope without touching process-global env.
fn acquire_registry_singleton_at(dir: &Path, socket_path: &Path) -> Result<std::fs::File> {
    std::fs::create_dir_all(dir)?;
    let lock_path = dir.join("daemon.lock");
    let lock = match sheaf_core::store::try_lock_exclusive(&lock_path) {
        Ok(lock) => lock.ok_or_else(|| anyhow::anyhow!("daemon lock vanished under us"))?,
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
            // Advisory identity written by the holder; best-effort.
            let incumbent = std::fs::read_to_string(&lock_path)
                .map(|body| {
                    body.lines()
                        .find(|l| l.starts_with("pid=") || l.starts_with("socket="))
                        .map(|l| format!(" [{l}]"))
                })
                .ok()
                .flatten()
                .unwrap_or_default();
            bail!(
                "another sheafd is already serving this enrollment registry{incumbent}; \
                 one daemon per data home — stop it first, or point XDG_DATA_HOME at \
                 an isolated registry"
            );
        }
        Err(e) => {
            bail!("flock {}: {e}", lock_path.display());
        }
    };
    let _ = std::fs::write(
        &lock_path,
        format!(
            "pid={}\nsocket={}\n",
            std::process::id(),
            socket_path.display()
        ),
    );
    Ok(lock)
}

// ------------------------------------------------------------- watch setup

/// Take the writer flock and open recovery state; both travel together so
/// a second daemon instance can never double-write a project's journal.
fn open_store_locked(
    root: &Path,
    limits: StoreLimits,
    max_tracked_bytes: u64,
) -> anyhow::Result<(ProjectStore, std::fs::File)> {
    let lock_path = sheaf_dir(root).join("lock");
    let lock_file = sheaf_core::store::try_lock_exclusive(&lock_path)
        .map_err(|e| anyhow::anyhow!("flock {}: {e}", lock_path.display()))?
        .ok_or_else(|| anyhow::anyhow!("lock vanished under us"))?;
    let store = ProjectStore::open_with_text_budget(root, limits, max_tracked_bytes)?;
    Ok((store, lock_file))
}

/// Finish a restore that a crash interrupted, before anything else observes
/// the tree. `resume_restore` reconciles the live worktree into history
/// first, so edits made in the dead gap survive the replay: a
/// partially-applied restore must never be left observable.
///
/// An intent past the project's staleness bound is NOT replayed
/// automatically — a tree the user has worked in for days must never rewind
/// after a reboot. It stays pending, visible in `project.status`, until the
/// operator resumes or abandons it explicitly.
fn resume_interrupted_restore(
    root: &Path,
    store: &mut ProjectStore,
    ignore: &dyn sheaf_core::ignore::ExcludesRel,
    max_resume_age_ms: i64,
) -> Option<sheaf_core::store::RestoreOutcome> {
    match store.resume_restore(ignore, false, max_resume_age_ms) {
        Ok(None) => None,
        Ok(Some(outcome)) => {
            tracing::warn!(
                root = %root.display(),
                written = outcome.files_written,
                deleted = outcome.files_deleted,
                "interrupted restore completed on startup"
            );
            Some(outcome)
        }
        Err(e) => {
            tracing::error!(root = %root.display(), error = %e, "restore resume FAILED");
            None
        }
    }
}

/// Build+register a watch task; false when the project is unwatchable now.
/// Resume every enrollment: live roots get a watch, roots deleted from
/// disk are pruned from the registry (see the call site for the policy
/// boundary), and roots that exist but fail to start are only warned
/// about. Returns (resumed, pruned) for the startup log line.
fn resume_enrollments(shared: &Shared, registry: &Registry) -> (usize, usize) {
    let mut resumed = 0usize;
    let mut pruned = 0usize;
    let Ok(entries) = registry.list() else {
        tracing::error!("enrollment registry unreadable; nothing resumed");
        return (0, 0);
    };
    for entry in entries {
        if !entry.root.is_dir() {
            match registry.forget(&entry.root) {
                Ok(true) => {
                    pruned += 1;
                    tracing::info!(
                        root = %entry.root.display(),
                        "enrollment pruned: root is gone from disk (re-run `sheaf init` if this was a mistake)"
                    );
                }
                Ok(false) => {}
                Err(e) => tracing::warn!(
                    root = %entry.root.display(),
                    error = %e,
                    "root is gone but the enrollment could not be pruned"
                ),
            }
            continue;
        }
        if spawn_watch(shared, &entry.root) {
            resumed += 1;
        } else {
            tracing::warn!(root = %entry.root.display(), "enrolled project skipped");
        }
    }
    (resumed, pruned)
}

/// When a project's store gets opened. Registry-resumed projects are Lazy:
/// an untouched project costs only a watcher thread, and the doc, caches,
/// and writer flock materialize on first activity. Fresh enrollments and
/// projects with a pending restore intent are Eager: the former is
/// activity by definition (`sheaf init` expects its baseline capture),
/// and the latter has a staleness deadline that waiting could let lapse.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OpenPolicy {
    Eager,
    Lazy,
}

fn effective_policy(root: &Path, requested: OpenPolicy) -> OpenPolicy {
    if matches!(requested, OpenPolicy::Lazy)
        && sheaf_core::store::pending_restore_at(root).is_some()
    {
        OpenPolicy::Eager
    } else {
        requested
    }
}

fn spawn_watch(shared: &Shared, root: &Path) -> bool {
    spawn_watch_policy(shared, root, OpenPolicy::Lazy)
}

/// Git's machine-global ignore file, when it exists in a default location
/// (`$XDG_CONFIG_HOME/git/ignore`, else `~/.config/git/ignore`). A custom
/// `core.excludesFile` is not resolved — honoring it would mean parsing
/// gitconfig, and the default locations cover the standard setups.
fn global_git_ignore_candidates() -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Some(x) = std::env::var_os("XDG_CONFIG_HOME") {
        if !x.is_empty() {
            out.push(PathBuf::from(x).join("git").join("ignore"));
        }
    }
    if let Some(home) = std::env::var_os("HOME") {
        out.push(
            PathBuf::from(home)
                .join(".config")
                .join("git")
                .join("ignore"),
        );
    }
    out.into_iter().filter(|p| p.is_file()).collect()
}

/// Build the effective classifier for a project root, leniently: config
/// (`[classify]` + legacy `[ignore]`) ∪ repository gitignore sources ∪
/// git's machine-global ignore. A build failure keeps the watch alive with
/// an all-durable classifier (watch everything) — dropping work silently is
/// the worse failure; the enrollment path still fails closed.
fn classifier_for(root: &Path) -> sheaf_core::classify::Classifier {
    let cfg = config::load(root).unwrap_or_default();
    match sheaf_core::classify::Classifier::for_project_with(
        root,
        &cfg,
        &global_git_ignore_candidates(),
    ) {
        Ok(classifier) => classifier,
        Err(error) => {
            tracing::warn!(
                root = %root.display(),
                %error,
                "classification failed; treating every path as durable until the rules parse"
            );
            sheaf_core::classify::Classifier::all_durable()
        }
    }
}

/// Rebuild a project's classification and swap it into the shared handle
/// the watcher backend filters against. Config is re-read too, so edits to
/// `[classify]`/`[ignore]` in `config.toml` land without a restart.
///
/// Refresh is event-driven (a `.gitignore` save); `.git/info/exclude` and
/// the global file live outside the watched tree and piggyback on any
/// `.gitignore`-triggered rebuild plus daemon restarts.
fn refresh_classifications(root: &Path, shared: &watcher::SharedClassifier) {
    let classifier = classifier_for(root);
    *shared.write() = classifier;
    tracing::info!(root = %root.display(), "classification refreshed");
}

fn spawn_watch_policy(shared: &Shared, root: &Path, policy: OpenPolicy) -> bool {
    let root_n = normalize(root);
    {
        let table = shared.table.lock().unwrap();
        if table.contains_key(&root_n) {
            return true; // idempotent
        }
    }
    if config::read_store_format(&root_n).is_err() {
        return false;
    } // We are about to own this store's writer side; retire the legacy flat
      // marker file older builds left next to config.toml.
    let _ = config::migrate_legacy_format_file(&root_n);
    let cfg = match config::load(&root_n) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(root = %root_n.display(), error = %e, "config unreadable; using defaults");
            ProjectConfig {
                format_version: config::STORE_FORMAT_VERSION,
                ..Default::default()
            }
        }
    };
    // Effective rules = config ([classify] + legacy [ignore]) ∪ repository
    // gitignore sources ∪ git's global ignore (default locations only).
    // The global file is a daemon-level input on purpose: its content
    // varies per machine, and library callers (tests, degraded CLI) must
    // stay deterministic.
    let classifier = {
        let compiled = match sheaf_core::classify::Classifier::for_project_with(
            &root_n,
            &cfg,
            &global_git_ignore_candidates(),
        ) {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(root = %root_n.display(), error = %e, "bad classify patterns");
                return false;
            }
        };
        watcher::shared_classifier(compiled)
    };

    let backend = match watcher::default_backend(root_n.clone(), classifier.clone()) {
        Ok(b) => b,
        Err(e) => {
            tracing::error!(root = %root_n.display(), error = %e, "backend init failed");
            return false;
        }
    };

    let (tx_ev, rx_ev) = channel::<sheaf_core::events::FsEvent>();
    // One stop flag shared by both threads of this project's task pair.
    let stop_flag = watcher::new_stop_flag();
    let backend_stop = stop_flag.clone();

    let watch_thread = {
        let root_log = root_n.display().to_string();
        let event_tx = tx_ev.clone();
        std::thread::Builder::new()
            .name(format!("inotify:{root_log}"))
            .spawn(move || backend.run(event_tx, backend_stop))
            .expect("spawn watch thread")
    };
    let mut watch_handles = vec![watch_thread];
    for linked in sheaf_core::store::linked_worktrees(&root_n)
        .unwrap_or_default()
        .into_iter()
        .filter(|item| item.present)
    {
        let linked_classifier = match sheaf_core::classify::Classifier::for_project_with(
            &linked.path,
            &cfg,
            &global_git_ignore_candidates(),
        ) {
            Ok(c) => watcher::shared_classifier(c),
            Err(error) => {
                tracing::warn!(
                    root = %linked.path.display(),
                    %error,
                    "managed worktree classification failed; worktree not watched"
                );
                continue;
            }
        };
        let linked_backend = match watcher::default_backend(linked.path.clone(), linked_classifier)
        {
            Ok(backend) => backend,
            Err(error) => {
                tracing::warn!(
                    root = %linked.path.display(),
                    %error,
                    "managed worktree backend failed"
                );
                continue;
            }
        };
        let linked_stop = stop_flag.clone();
        let linked_tx = tx_ev.clone();
        let root_log = linked.path.display().to_string();
        match std::thread::Builder::new()
            .name(format!("inotify:{root_log}"))
            .spawn(move || linked_backend.run(linked_tx, linked_stop))
        {
            Ok(handle) => watch_handles.push(handle),
            Err(error) => {
                tracing::warn!(root = %root_log, %error, "managed worktree thread failed")
            }
        }
    }

    let deb_cfg = DebouncerConfig {
        window: Duration::from_millis(cfg.watch.debounce_ms as u64),
        max_hold: Duration::from_millis(cfg.watch.max_hold_ms as u64),
        cap_events: cfg.watch.max_events,
    };
    // Eager: take the store now so a failure to open is reported by this
    // call (project not watched), exactly as before lazy loading. Lazy:
    // open nothing yet; the collector thread does it on first activity.
    let policy = effective_policy(&root_n, policy);
    let store_limits = cfg.store.clone();
    let eager_pair = if matches!(policy, OpenPolicy::Eager) {
        match open_store_locked(&root_n, store_limits.clone(), cfg.watch.max_tracked_bytes) {
            Ok(pair) => Some(pair),
            Err(e) => {
                tracing::error!(root = %root_n.display(), error = %e, "store unavailable");
                return false;
            }
        }
    } else {
        None
    };

    // Reconciliation runs on the project's writer thread, not on the IPC
    // enrollment/startup path. This makes `sheaf init` return promptly while
    // the bounded initial capture continues in the background.
    let max_resume_age_ms = cfg.restore.max_resume_age_ms;
    let max_tracked_bytes = cfg.watch.max_tracked_bytes;
    let (control_tx, control_rx) = channel::<StoreCommand>();
    let (wake_tx, wake_rx) = channel::<()>();
    let ready = Arc::new(AtomicBool::new(false));
    // Eager entries hold an open store from the start; only lazy ones are
    // cold. Keeping the distinction in the initial value means the warming
    // gate alone governs the eager boot window (fail-fast, never queue),
    // while cold is exclusively the lazy parked state.
    let cold = Arc::new(AtomicBool::new(matches!(policy, OpenPolicy::Lazy)));
    let collector_stop = stop_flag.clone();
    let scratch_cfg = cfg.scratch.clone();
    let collector_thread = {
        let root_n2 = root_n.clone();
        let classifier2 = classifier.clone();
        let ready2 = ready.clone();
        let cold2 = cold.clone();
        std::thread::Builder::new()
            .name(format!("collect:{}", root_n.display()))
            .spawn(move || {
                let (store, lock_file, initial_mute) = match eager_pair {
                    Some((mut store, lock_file)) => {
                        let cls = classifier2.read().clone();
                        let mute =
                            boot_reconcile_store(&root_n2, &mut store, &cls, max_resume_age_ms);
                        ready2.store(true, Ordering::Release);
                        (store, lock_file, mute)
                    }
                    None => {
                        let Some((mut store, lock_file)) = collect_cold(
                            &root_n2,
                            &rx_ev,
                            &wake_rx,
                            &collector_stop,
                            store_limits,
                            max_tracked_bytes,
                        ) else {
                            return; // stopped before any activity; nothing to flush
                        };
                        let cls = classifier2.read().clone();
                        let mute =
                            boot_reconcile_store(&root_n2, &mut store, &cls, max_resume_age_ms);
                        cold2.store(false, Ordering::Release);
                        ready2.store(true, Ordering::Release);
                        // The wake channel has served its purpose; letting it
                        // hang would buffer every post-open wake forever.
                        drop(wake_rx);
                        (store, lock_file, mute)
                    }
                };
                // Journal replay is the most allocation-heavy thing this
                // process ever does, and nothing in the steady state needs
                // what it transiently allocated. Without an explicit trim,
                // glibc holds the freed arenas indefinitely: a store that
                // took GiB to open idled hundreds of MiB above its true
                // footprint until the first memory-heavy command happened
                // to trim. Return the replay's pages now.
                trim_process_heap();
                collect_loop(
                    root_n2,
                    rx_ev,
                    control_rx,
                    deb_cfg,
                    store,
                    classifier2,
                    scratch_cfg,
                    initial_mute,
                    lock_file,
                    max_resume_age_ms,
                )
            })
            .expect("spawn collector thread")
    };

    shared.table.lock().unwrap().insert(
        root_n.clone(),
        WatchEntry {
            stop: stop_flag,
            cold,
            ready,
            watch_handles,
            events: tx_ev,
            collector: Some(collector_thread),
            control: control_tx,
            wake: wake_tx,
        },
    );
    tracing::info!(root = %root_n.display(), cold = matches!(policy, OpenPolicy::Lazy), "watching");
    true
}

/// Resume crash-safe mutations and reconcile every physical worktree before
/// the project becomes ready. One writer walks the heads serially.
fn boot_reconcile_store(
    root: &Path,
    store: &mut ProjectStore,
    primary_classifier: &sheaf_core::classify::Classifier,
    max_resume_age_ms: i64,
) -> Option<RestoreMute> {
    let mut roots = vec![root.to_path_buf()];
    roots.extend(
        sheaf_core::store::linked_worktrees(root)
            .unwrap_or_default()
            .into_iter()
            .filter(|worktree| worktree.present)
            .map(|worktree| worktree.path),
    );
    let mut primary_mute = None;
    for worktree in roots {
        if let Err(error) = store.activate_worktree(&worktree) {
            tracing::error!(root = %worktree.display(), %error, "worktree activation failed");
            continue;
        }
        let owned_classifier;
        let classifier: &sheaf_core::classify::Classifier = if worktree == root {
            primary_classifier
        } else {
            owned_classifier = classifier_for(&worktree);
            &owned_classifier
        };
        if sheaf_core::store::pending_merge_at(&worktree).is_some() {
            match store.resume_merge() {
                Ok(Some(outcome)) => tracing::warn!(
                    root = %worktree.display(),
                    written = outcome.files_written,
                    deleted = outcome.files_deleted,
                    "interrupted merge completed on startup"
                ),
                Ok(None) => {}
                Err(error) => {
                    tracing::error!(
                        root = %worktree.display(),
                        %error,
                        "merge resume blocked; leaving intent pending and skipping reconciliation"
                    );
                    continue;
                }
            }
        }
        let resumed = resume_interrupted_restore(&worktree, store, classifier, max_resume_age_ms);
        match store.reconcile_worktree(classifier) {
            Ok(None) => {}
            Ok(Some(capture)) => tracing::info!(
                root = %worktree.display(),
                capture = capture.short_id(),
                "boot reconciliation complete"
            ),
            Err(error) => tracing::warn!(
                root = %worktree.display(),
                %error,
                "boot reconciliation failed"
            ),
        }
        if worktree == root {
            primary_mute = resumed
                .as_ref()
                .map(|outcome| RestoreMute::new(root, outcome));
        }
    }
    let _ = store.activate_worktree(root);
    primary_mute
}

/// How long the cold collector sleeps between wake-channel polls. Only
/// pays while a project sits untouched; worktree events interrupt
/// `recv_timeout` immediately.
const COLD_POLL: Duration = Duration::from_millis(50);

/// Backoff after a failed lazy open (e.g. an offline `doctor --fix`
/// holding the writer flock): without it, queued events would retrigger
/// the open in a tight loop for the repair's whole duration.
const OPEN_RETRY_BACKOFF: Duration = Duration::from_secs(1);

/// The cold half of a lazy project's life: wait for the first sign of
/// activity, then open the store (flock + replay) and hand it to the
/// normal boot sequence. The triggering fs event is deliberately
/// consumed and dropped — the open-time `reconcile_worktree` scans the
/// live tree, so whatever that event represented is captured by the
/// reconcile itself, and anything arriving during the open stays queued
/// in the event channel for `collect_loop`. Returns None when the daemon
/// is stopping before any activity happened (a cold store has no debounce
/// tail to flush).
fn collect_cold(
    root: &Path,
    rx: &Receiver<sheaf_core::events::FsEvent>,
    wake: &Receiver<()>,
    stop: &StopFlag,
    limits: StoreLimits,
    max_tracked_bytes: u64,
) -> Option<(ProjectStore, std::fs::File)> {
    loop {
        // Phase 1: parked until an event, an IPC wake, or shutdown.
        loop {
            if stop.load(Ordering::SeqCst) {
                return None;
            }
            if wake.try_recv().is_ok() {
                break;
            }
            match rx.recv_timeout(COLD_POLL) {
                Ok(_) => break, // absorbed by the reconcile in phase 2
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return None,
            }
        }
        // Phase 2: open. A failure (flock held, disk error) parks us again —
        // the next event or command retries, and any command that woke us
        // stays queued for whenever the open finally succeeds.
        match open_store_locked(root, limits.clone(), max_tracked_bytes) {
            Ok(pair) => {
                tracing::info!(root = %root.display(), "store opened on first activity");
                return Some(pair);
            }
            Err(e) => {
                tracing::error!(
                    root = %root.display(),
                    error = %e,
                    "lazy store open failed; retrying on next activity"
                );
                std::thread::sleep(OPEN_RETRY_BACKOFF);
            }
        }
    }
}

/// The writer's own restore writes come back through inotify. Swallow that
/// echo — but only while the bytes on disk still say exactly what the restore
/// put there, so a user typing into a restored file is never silenced.
struct RestoreMute {
    until: Instant,
    written: std::collections::BTreeSet<PathBuf>,
    deleted: std::collections::BTreeSet<PathBuf>,
}

impl RestoreMute {
    fn new(root: &Path, outcome: &sheaf_core::store::RestoreOutcome) -> RestoreMute {
        RestoreMute {
            until: Instant::now() + RESTORE_MUTE,
            written: outcome.written_paths.iter().map(|p| root.join(p)).collect(),
            deleted: outcome.deleted_paths.iter().map(|p| root.join(p)).collect(),
        }
    }

    fn swallows(&self, event: &sheaf_core::events::FsEvent, store: &ProjectStore) -> bool {
        if Instant::now() >= self.until {
            return false;
        }
        let path = event.path();
        if self.deleted.contains(path) {
            return !path.exists();
        }
        if self.written.contains(path) {
            return store.content_differs(path) == Some(false);
        }
        false
    }
}

/// Per-project debouncer sink: batches persist through the Loro-backed
/// store, falling back to log-only when persistence fails. This
/// thread is the project's sole writer, so restores execute here too.
///
/// Events route by classification: `Durable` feeds the debouncer (and so
/// the timeline), `Volatile` feeds the scratch ring (never the timeline),
/// `Never` is dropped defensively — the backend already refuses to emit
/// it. The ring flushes when durable work flushes, on its own cadence
/// (`[scratch] flush_ms`), and at shutdown.
#[allow(clippy::too_many_arguments)]
fn collect_loop(
    root: PathBuf,
    rx: Receiver<sheaf_core::events::FsEvent>,
    control: Receiver<StoreCommand>,
    cfg: DebouncerConfig,
    mut store: ProjectStore,
    classifier: watcher::SharedClassifier,
    scratch_cfg: sheaf_core::config::ScratchConfig,
    initial_mute: Option<RestoreMute>,
    _lock_guard: std::fs::File,
    max_resume_age_ms: i64,
) {
    use sheaf_core::classify::PathClass;
    use sheaf_core::events::EventKind;
    use sheaf_core::scratch::ScratchWriter;
    use std::collections::BTreeSet;

    let poll = (cfg.window / 4).max(Duration::from_millis(20));
    let mut debouncers = HashMap::from([(root.clone(), Debouncer::new(root.clone(), cfg.clone()))]);
    let mut classifiers = HashMap::from([(root.clone(), classifier)]);
    let mut mutes = HashMap::new();
    if let Some(mute) = initial_mute {
        mutes.insert(root.clone(), mute);
    }
    // The recovery ring lives under the STORE's `.sheaf/scratch/` (shared
    // by every worktree of the project; records are tagged by the worktree
    // that observed them).
    let scratch_dir = sheaf_dir(&root).join("scratch");
    let mut scratch = if scratch_cfg.enabled {
        ScratchWriter::open(
            &scratch_dir,
            scratch_cfg.max_bytes,
            scratch_cfg.max_file_bytes,
        )
    } else {
        ScratchWriter::disabled()
    };
    // Volatile paths awaiting a ring flush: (worktree root, absolute path).
    let mut scratch_dirty: BTreeSet<(PathBuf, PathBuf)> = BTreeSet::new();
    let mut last_scratch_flush = Instant::now();
    let scratch_period = Duration::from_millis(scratch_cfg.flush_ms.max(1));

    let flush_scratch = |scratch: &mut ScratchWriter, dirty: &mut BTreeSet<(PathBuf, PathBuf)>| {
        for (event_root, abs) in dirty.iter() {
            if let Ok(rel) = abs.strip_prefix(event_root) {
                scratch.snapshot(event_root, abs, &rel.to_string_lossy());
            }
        }
        dirty.clear();
        scratch.flush();
    };

    // Plans handed out but not yet applied. Bounded and collector-local:
    // the single writer is the only thing entitled to honour a token.
    let mut plans: Vec<sheaf_core::store::RestorePlan> = Vec::new();
    let mut fragment_plans: Vec<sheaf_core::store::FragmentPlan> = Vec::new();
    let mut merge_plans: Vec<sheaf_core::store::MergePlan> = Vec::new();

    loop {
        while let Ok(command) = control.try_recv() {
            let command_root = command.worktree_root().unwrap_or(&root).to_path_buf();
            if !debouncers.contains_key(&command_root) {
                debouncers.insert(
                    command_root.clone(),
                    Debouncer::new(command_root.clone(), cfg.clone()),
                );
            }
            let mut flush_error = None;
            let memory_heavy = command.is_memory_heavy();
            if command.crosses_debounce_boundary() {
                let pending = debouncers
                    .get_mut(&command_root)
                    .expect("command debouncer exists")
                    .force_flush();
                if !pending.is_empty() {
                    flush_error = persist_batch_checked(&mut store, &pending).err();
                }
                // A boundary means "everything up to now" — durable or not.
                // Immediate markers (volatile disappearances) sitting in the
                // ring's buffer reach disk here even with no durable batch.
                flush_scratch(&mut scratch, &mut scratch_dirty);
                last_scratch_flush = Instant::now();
            }
            let cls = classifiers[&command_root].read().clone();
            if let Some(outcome) = handle_store_command(
                &mut store,
                &cls,
                &mut plans,
                &mut fragment_plans,
                &mut merge_plans,
                command,
                flush_error,
                max_resume_age_ms,
            ) {
                mutes.insert(
                    command_root.clone(),
                    RestoreMute::new(&command_root, &outcome),
                );
            }
            if memory_heavy {
                trim_process_heap();
            }
        }
        match rx.recv_timeout(poll) {
            Ok(ev) => {
                let start = ev.path().parent().unwrap_or_else(|| ev.path());
                let Some(event_root) =
                    sheaf_core::init::resolve_project_root(start).map(|path| normalize(&path))
                else {
                    tracing::warn!(path = %ev.path().display(), "event has no project root");
                    continue;
                };
                if !store.is_registered_worktree(&event_root).unwrap_or(false) {
                    tracing::warn!(
                        root = %event_root.display(),
                        path = %ev.path().display(),
                        "event from unregistered worktree ignored"
                    );
                    continue;
                }
                if !debouncers.contains_key(&event_root) {
                    debouncers.insert(
                        event_root.clone(),
                        Debouncer::new(event_root.clone(), cfg.clone()),
                    );
                }
                if !classifiers.contains_key(&event_root) {
                    let compiled = watcher::shared_classifier(classifier_for(&event_root));
                    classifiers.insert(event_root.clone(), compiled);
                }
                if ev
                    .path()
                    .file_name()
                    .is_some_and(|name| name == ".gitignore")
                {
                    refresh_classifications(&event_root, &classifiers[&event_root]);
                }
                // Route by classification. The probe path is the event's
                // primary path (rename destinations); a rename whose
                // SOURCE was volatile leaves a `gone` marker so the ring
                // records the disappearance, not just the arrival.
                let class = classifiers[&event_root]
                    .read()
                    .classify_event_path(&event_root, ev.path());
                match (class, &ev.kind) {
                    (PathClass::Never, _) => {}
                    (PathClass::Volatile, EventKind::Removed { path }) => {
                        if let Ok(rel) = path.strip_prefix(&event_root) {
                            scratch.gone(&event_root, &rel.to_string_lossy());
                        }
                    }
                    (PathClass::Volatile, EventKind::Renamed { from, .. }) => {
                        let from_class = classifiers[&event_root]
                            .read()
                            .classify_event_path(&event_root, from);
                        if from_class == PathClass::Volatile {
                            if let Ok(rel) = from.strip_prefix(&event_root) {
                                scratch.gone(&event_root, &rel.to_string_lossy());
                            }
                        }
                        scratch_dirty.insert((event_root.clone(), ev.path().to_path_buf()));
                    }
                    (PathClass::Volatile, _) => {
                        scratch_dirty.insert((event_root.clone(), ev.path().to_path_buf()));
                    }
                    (PathClass::Durable, _) => {
                        let swallowed = store.activate_worktree(&event_root).is_ok()
                            && mutes
                                .get(&event_root)
                                .is_some_and(|mute| mute.swallows(&ev, &store));
                        if !swallowed {
                            if let Some(batch) = debouncers
                                .get_mut(&event_root)
                                .expect("event debouncer exists")
                                .feed(ev)
                            {
                                persist_batch(&mut store, &batch);
                                flush_scratch(&mut scratch, &mut scratch_dirty);
                                last_scratch_flush = Instant::now();
                            }
                        }
                    }
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        }
        let ready: Vec<_> = debouncers
            .values_mut()
            .filter_map(Debouncer::take_if_quiescent)
            .collect();
        for batch in ready {
            persist_batch(&mut store, &batch);
            flush_scratch(&mut scratch, &mut scratch_dirty);
            last_scratch_flush = Instant::now();
        }
        // Cadence: volatile-only activity still reaches the ring at least
        // every `flush_ms`, so an editor crash loses at most one window.
        if last_scratch_flush.elapsed() >= scratch_period && !scratch_dirty.is_empty() {
            flush_scratch(&mut scratch, &mut scratch_dirty);
            last_scratch_flush = Instant::now();
        }
        mutes.retain(|_, mute| Instant::now() < mute.until);
    }
    let tails: Vec<_> = debouncers
        .values_mut()
        .map(Debouncer::force_flush)
        .filter(|batch| !batch.is_empty())
        .collect();
    for tail in tails {
        tracing::info!(root = %tail.root.display(), events = tail.len(), "final drain on shutdown");
        persist_batch(&mut store, &tail);
    }
    flush_scratch(&mut scratch, &mut scratch_dirty);
}

impl StoreCommand {
    fn worktree_root(&self) -> Option<&Path> {
        match self {
            StoreCommand::InWorktree { root, .. } => Some(root),
            _ => None,
        }
    }

    /// Mutations that mean "everything up to now" must first close the open
    /// debounce window.
    fn crosses_debounce_boundary(&self) -> bool {
        match self {
            StoreCommand::InWorktree { command, .. } => command.crosses_debounce_boundary(),
            StoreCommand::CreateCheckpoint { .. }
            | StoreCommand::CreateBranch { .. }
            | StoreCommand::RenameBranch { .. }
            | StoreCommand::DeleteBranch { .. }
            | StoreCommand::ApplyRestore { .. }
            | StoreCommand::ResumeRestore { .. }
            | StoreCommand::ApplyFragment { .. }
            | StoreCommand::AddWorktree { .. }
            | StoreCommand::ApplyMerge { .. }
            | StoreCommand::ResumeMerge { .. } => true,
            _ => false,
        }
    }

    /// Commands whose work allocates large transients: doctor and gc open
    /// a whole second document (fresh reader), diff and grep build forked
    /// history views, backfill decompresses content. glibc retains freed
    /// arena memory, and with kernel THP policy `always` the retained
    /// pages pin 2 MiB chunks — so the daemon's RSS ratchets upward after
    /// each one and never comes back down. The reply has been sent by the
    /// time this matters, so returning the freed pages immediately after
    /// keeps the resident set honest without costing the caller anything.
    fn is_memory_heavy(&self) -> bool {
        match self {
            StoreCommand::InWorktree { command, .. } => command.is_memory_heavy(),
            StoreCommand::Doctor { .. }
            | StoreCommand::Gc { .. }
            | StoreCommand::CaptureLogDetails { .. }
            | StoreCommand::Diff { .. }
            | StoreCommand::Grep { .. }
            | StoreCommand::CacheBackfill { .. }
            | StoreCommand::PlanMerge { .. }
            | StoreCommand::AddWorktree { .. } => true,
            _ => false,
        }
    }

    fn send_error(self, error: sheaf_core::SheafError) {
        match self {
            StoreCommand::InWorktree { command, .. } => command.send_error(error),
            StoreCommand::TimelineLog { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::CaptureLogDetails { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::ListCheckpoints { reply } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::CaptureInfo { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::CreateCheckpoint { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::ListBranches { reply } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::BranchGraph { reply } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::CreateBranch { reply, .. }
            | StoreCommand::RenameBranch { reply, .. }
            | StoreCommand::DeleteBranch { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::PlanRestore { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::ApplyRestore { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::ResumeRestore { reply } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::AbandonRestore { reply } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::Gc { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::Mark { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::Doctor { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::Diff { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::Grep { reply, .. } => {
                let _ = reply.send(GrepStreamItem::Done(Err(error)));
            }
            StoreCommand::CacheBackfill { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::PlanFragment { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::ApplyFragment { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::PlanSmart { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::ListWorktrees { reply } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::AddWorktree { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::PlanMerge { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::ApplyMerge { reply, .. } => {
                let _ = reply.send(Err(error));
            }
            StoreCommand::ResumeMerge { reply } => {
                let _ = reply.send(Err(error));
            }
        }
    }
}

/// Best-effort return of freed heap pages to the OS. Cheap (walks free
/// lists, madvises whole free extents); called after memory-heavy writer
/// commands, not on a timer, so an idle daemon does zero extra work.
fn trim_process_heap() {
    unsafe {
        libc::malloc_trim(0);
    }
}

/// Forbid transparent huge pages for this process and its threads.
///
/// The daemon is long-lived with a churny heap: every doctor/gc/diff/grep
/// allocates large transients that are then freed, and glibc does not
/// rush to return mid-heap pages. Under kernel THP policy `always`, benchmark
/// measurements showed those retained regions pinning whole 2 MiB huge pages:
/// 208 MiB of a 689 MiB resident set was `anon_thp` that ordinary trimming
/// could not split. PR_SET_THP_DISABLE
/// trades a little allocation throughput for a resident set that actually
/// follows live data. Threads spawned later inherit the flag.
fn disable_thp_for_this_process() {
    // PR_SET_THP_DISABLE is 41 on Linux; older libc crate versions may
    // lack the constant, so spell it rather than depend on the bind.
    const PR_SET_THP_DISABLE: libc::c_int = 41;
    if unsafe { libc::prctl(PR_SET_THP_DISABLE, 1, 0, 0, 0) } != 0 {
        tracing::debug!(
            error = %std::io::Error::last_os_error(),
            "could not disable THP for this process (non-fatal)"
        );
    }
}

/// Returns the restore that just landed, if any, so the caller can mute its
/// filesystem echo.
fn handle_store_command(
    store: &mut ProjectStore,
    ignore: &dyn sheaf_core::ignore::ExcludesRel,
    plans: &mut Vec<sheaf_core::store::RestorePlan>,
    fragment_plans: &mut Vec<sheaf_core::store::FragmentPlan>,
    merge_plans: &mut Vec<sheaf_core::store::MergePlan>,
    command: StoreCommand,
    flush_error: Option<sheaf_core::SheafError>,
    max_resume_age_ms: i64,
) -> Option<sheaf_core::store::RestoreOutcome> {
    let command = match command {
        StoreCommand::InWorktree { root, command } => {
            if let Err(error) = store.activate_worktree(&root) {
                command.send_error(error);
                return None;
            }
            *command
        }
        command => command,
    };
    match command {
        StoreCommand::TimelineLog {
            all,
            branch,
            path,
            follow,
            limit,
            reply,
        } => {
            let tips = store.branch_tips().map(|t| t.len()).unwrap_or(1);
            let captures = match branch {
                Some(branch) => store.captures_for_branch(&branch, path.as_deref(), follow, limit),
                None => store.captures(all, path.as_deref(), follow, limit),
            };
            let _ = reply.send(captures.map(|captures| (captures, tips)));
        }
        StoreCommand::CaptureLogDetails { references, reply } => {
            let details = references
                .iter()
                .map(|reference| store.capture_log_detail(reference))
                .collect();
            let _ = reply.send(details);
        }
        StoreCommand::CaptureInfo { reference, reply } => {
            let _ = reply.send(store.capture_info(&reference));
        }
        StoreCommand::ListCheckpoints { reply } => {
            let _ = reply.send(Ok(store.checkpoints()));
        }
        StoreCommand::CreateCheckpoint {
            name,
            reference,
            reply,
        } => {
            let result = match flush_error {
                Some(error) => Err(error),
                None => store.create_checkpoint(&name, reference.as_deref()),
            };
            let _ = reply.send(result);
        }
        StoreCommand::ListBranches { reply } => {
            let _ = reply.send(Ok(store.branches()));
        }
        StoreCommand::BranchGraph { reply } => {
            let _ = reply.send(store.branch_graph());
        }
        StoreCommand::CreateBranch {
            name,
            reference,
            metadata,
            reply,
        } => {
            let result = match flush_error {
                Some(error) => Err(error),
                None => store.create_branch(&name, reference.as_deref(), metadata),
            };
            let _ = reply.send(result);
        }
        StoreCommand::RenameBranch {
            old_name,
            new_name,
            reply,
        } => {
            let result = match flush_error {
                Some(error) => Err(error),
                None => store.rename_branch(&old_name, &new_name),
            };
            let _ = reply.send(result);
        }
        StoreCommand::DeleteBranch { name, reply } => {
            let result = match flush_error {
                Some(error) => Err(error),
                None => store.delete_branch(&name),
            };
            let _ = reply.send(result);
        }
        StoreCommand::PlanRestore {
            reference,
            scope,
            reply,
        } => {
            let result = store.plan_restore(&reference, &scope, ignore);
            if let Ok(plan) = &result {
                plans.retain(|p| p.token != plan.token);
                plans.push(plan.clone());
                if plans.len() > PLAN_CACHE {
                    plans.remove(0);
                }
            }
            let _ = reply.send(result);
        }
        StoreCommand::ApplyRestore { token, reply } => {
            let result = match flush_error {
                Some(error) => Err(error),
                None => match plans.iter().find(|p| p.token == token).cloned() {
                    Some(plan) => store.apply_restore(&plan, ignore),
                    None => Err(sheaf_core::SheafError::RestorePlanStale(format!(
                        "unknown or expired plan token `{token}`; re-plan the restore"
                    ))),
                },
            };
            // Any surviving plan describes a worktree that no longer exists.
            if result.is_ok() {
                plans.clear();
                fragment_plans.clear();
            }
            let outcome = result.as_ref().ok().cloned();
            let _ = reply.send(result);
            return outcome;
        }
        StoreCommand::PlanFragment {
            selections,
            mode,
            reply,
        } => {
            let result = store.plan_fragment_restore(&selections, mode);
            if let Ok(plan) = &result {
                fragment_plans.retain(|p| p.token != plan.token);
                fragment_plans.push(plan.clone());
                if fragment_plans.len() > PLAN_CACHE {
                    fragment_plans.remove(0);
                }
            }
            let _ = reply.send(result);
        }
        StoreCommand::PlanSmart {
            selections,
            head_texts,
            reply,
        } => {
            let result = match head_texts {
                None => Ok(SmartPlanReply::Paths(
                    store.smart_destination_paths(&selections),
                )),
                Some(head_texts) => store
                    .plan_smart_with_heads(&selections, &head_texts)
                    .map(|plan| SmartPlanReply::Plan(Box::new(plan))),
            };
            let _ = reply.send(result);
        }
        StoreCommand::ApplyFragment { token, reply } => {
            let result = match flush_error {
                Some(error) => Err(error),
                None => match fragment_plans.iter().find(|p| p.token == token).cloned() {
                    Some(plan) => store.apply_fragment_restore(&plan, ignore),
                    None => Err(sheaf_core::SheafError::RestorePlanStale(format!(
                        "unknown or expired fragment plan token `{token}`; re-plan the restore"
                    ))),
                },
            };
            if result.is_ok() {
                plans.clear();
                fragment_plans.clear();
            }
            let outcome = result.as_ref().ok().cloned();
            let _ = reply.send(result);
            return outcome;
        }
        StoreCommand::ResumeRestore { reply } => {
            // The operator asked by name: staleness bound overridden.
            let result = match flush_error {
                Some(error) => Err(error),
                None => match store.resume_restore(ignore, true, max_resume_age_ms) {
                    Ok(Some(outcome)) => Ok(outcome),
                    Ok(None) => Err(sheaf_core::SheafError::RestorePlanStale(
                        "no pending restore intent to resume".into(),
                    )),
                    Err(e) => Err(e),
                },
            };
            if result.is_ok() {
                plans.clear();
                fragment_plans.clear();
            }
            let outcome = result.as_ref().ok().cloned();
            let _ = reply.send(result);
            return outcome;
        }
        StoreCommand::AbandonRestore { reply } => {
            let result = store.abandon_restore(ignore);
            if result.is_ok() {
                plans.clear();
            }
            let _ = reply.send(result);
        }
        StoreCommand::Gc { apply, reply } => {
            let result = sheaf_core::store::gc_run_store(store, apply);
            let _ = reply.send(result);
        }
        StoreCommand::Mark { reference, reply } => {
            let result = sheaf_core::store::retention_mark(store, &reference);
            let _ = reply.send(result);
        }
        StoreCommand::Doctor { fix, reply } => {
            let result = if fix {
                sheaf_core::store::doctor_fix(store.root())
                    .map(|o| sheaf_core::store::DoctorReply::Repair(Box::new(o)))
            } else {
                sheaf_core::store::doctor(store.root())
                    .map(|r| sheaf_core::store::DoctorReply::Report(Box::new(r)))
            };
            let _ = reply.send(result);
        }
        StoreCommand::Diff {
            from,
            to,
            paths,
            reply,
        } => {
            let _ = reply.send(store.diff(&from, to.as_deref(), &paths, ignore));
        }
        StoreCommand::Grep { request, reply } => {
            let mut sink_record = |record: sheaf_core::store::GrepStreamRecord| {
                // A hung-up client just stops receiving; the walk is budgeted
                // either way, so a dead receiver costs nothing but the sends.
                let _ = reply.send(GrepStreamItem::Record(record));
            };
            let result = store.grep_streaming(&request, &mut Some(&mut sink_record));
            let _ = reply.send(GrepStreamItem::Done(result));
        }
        StoreCommand::CacheBackfill { opts, reply } => {
            let _ = reply.send(store.grep_cache_backfill(opts));
        }
        StoreCommand::ListWorktrees { reply } => {
            let _ = reply.send(store.worktrees());
        }
        StoreCommand::AddWorktree {
            reference,
            destination,
            reply,
        } => {
            let result = match flush_error {
                Some(error) => Err(error),
                None => store.add_worktree(&reference, &destination),
            };
            let _ = reply.send(result);
        }
        StoreCommand::PlanMerge { source, reply } => {
            let result = store.plan_merge(&source);
            if let Ok(plan) = &result {
                merge_plans.retain(|cached| cached.token != plan.token);
                merge_plans.push(plan.clone());
                if merge_plans.len() > PLAN_CACHE {
                    merge_plans.remove(0);
                }
            }
            let _ = reply.send(result);
        }
        StoreCommand::ApplyMerge { token, reply } => {
            let result = match flush_error {
                Some(error) => Err(error),
                None => match merge_plans.iter().find(|plan| plan.token == token).cloned() {
                    Some(plan) => store.apply_merge(&plan, ignore),
                    None => Err(sheaf_core::SheafError::MergePlanStale(format!(
                        "unknown or expired merge plan token `{token}`; re-plan the merge"
                    ))),
                },
            };
            if result.is_ok() {
                merge_plans.clear();
                plans.clear();
                fragment_plans.clear();
            }
            let _ = reply.send(result);
        }
        StoreCommand::ResumeMerge { reply } => {
            let result = match flush_error {
                Some(error) => Err(error),
                None => match store.resume_merge() {
                    Ok(Some(outcome)) => Ok(outcome),
                    Ok(None) => Err(sheaf_core::SheafError::MergePlanStale(
                        "no pending merge intent to resume".into(),
                    )),
                    Err(error) => Err(error),
                },
            };
            let _ = reply.send(result);
        }
        StoreCommand::InWorktree { .. } => unreachable!("worktree wrapper was unwrapped"),
    }
    None
}

fn persist_batch(store: &mut ProjectStore, batch: &sheaf_core::events::Batch) {
    let _ = persist_batch_checked(store, batch);
}

fn persist_batch_checked(
    store: &mut ProjectStore,
    batch: &sheaf_core::events::Batch,
) -> std::result::Result<(), sheaf_core::SheafError> {
    store.activate_worktree(&batch.root)?;

    match store.apply_batch(batch) {
        Ok(o) => {
            tracing::info!(
                root = %batch.root.display(),
                seq = o.seq,
                events = o.events_applied,
                text_spliced = o.text_ops_spliced,
                text_created = o.text_created,
                binaries = o.binaries_stored,
                update_bytes = o.update_bytes,
                snapshotted = o.snapshotted,
                "batch persisted"
            );
            Ok(())
        }
        Err(e) => {
            tracing::error!(root = %batch.root.display(), error = %e, "persist FAILED; window lost");
            Err(e)
        }
    }
}

fn set_mode(path: &Path, mode: u32) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(path)?.permissions();
    perms.set_mode(mode);
    std::fs::set_permissions(path, perms)
}

// ------------------------------------------------------------ connections

fn serve_connection(shared: Arc<Shared>, stream: UnixStream) -> Result<()> {
    // Kernel-enforced same-user policy: the socket only serves peers
    // running as the same uid as the daemon.
    if let Some(peer) = peer_uid(&stream) {
        if peer != current_uid() {
            tracing::warn!(peer_uid = peer, "cross-user connection refused");
            return Ok(());
        }
    }
    let mut stream = stream;
    stream.set_read_timeout(Some(REQUEST_SOFT))?;
    stream.set_write_timeout(Some(CONNECT_TIMEOUT))?;

    loop {
        if shared.stopping.load(Ordering::SeqCst) {
            return Ok(());
        }
        let env_bytes = match sheaf_core::ipc::read_frame(&mut stream, MAX_ENVELOPE) {
            Ok(b) => b,
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
            Err(ref e)
                if e.kind() == std::io::ErrorKind::UnexpectedEof
                    || e.kind() == std::io::ErrorKind::ConnectionReset =>
            {
                return Ok(())
            }
            Err(e) => {
                tracing::debug!(error = %e, "frame read failed");
                return Ok(());
            }
        };
        let req: Request = match serde_json::from_slice(&env_bytes) {
            Ok(r) => r,
            Err(e) => {
                let resp = Response::err(
                    "?",
                    IpcError::new("bad.request", format!("unparseable envelope: {e}")),
                );
                write_response(&mut stream, &resp, &[])?;
                continue;
            }
        };

        let mut shutting_down = false;
        let (resp, body) = dispatch(&shared, &req, &mut shutting_down);
        match body {
            IpcBody::Bytes(bytes) => write_response(&mut stream, &resp, &bytes)?,
            IpcBody::Stream(records) => write_streamed_response(&mut stream, &resp, records)?,
        }
        if shutting_down {
            shared.request_stop();
            return Ok(());
        }
    }
}

/// Envelope first, then any body chunks the envelope announces (the
/// download framing). An oversized envelope becomes `result.too_large` and
/// loses its body: framing is not recoverable, so a stated error beats a
/// dropped connection the client would misread as transport failure.
fn write_response(stream: &mut UnixStream, resp: &Response, body: &[u8]) -> std::io::Result<()> {
    let mut resp = resp.clone();
    if !body.is_empty() {
        let chunks = body.len().div_ceil(ipc::MAX_CHUNK).max(1) as u32;
        resp.body = Some(ipc::BodyInfo { chunks });
    }
    let payload = serde_json::to_vec(&resp)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    if payload.len() > MAX_ENVELOPE {
        tracing::warn!(
            bytes = payload.len(),
            cap = MAX_ENVELOPE,
            method_id = %resp.id,
            "response exceeds the envelope cap; replying with `result.too_large`"
        );
        let replacement = Response::err(
            resp.id.clone(),
            IpcError::new(
                "result.too_large",
                format!(
                    "the answer is {} bytes, over the {MAX_ENVELOPE}-byte envelope cap;                      narrow the request (for a restore, pass explicit paths)",
                    payload.len()
                ),
            ),
        );
        let payload = serde_json::to_vec(&replacement)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        return ipc::write_frame(stream, &payload, MAX_ENVELOPE);
    }
    ipc::write_frame(stream, &payload, MAX_ENVELOPE)?;
    for chunk in body.chunks(ipc::MAX_CHUNK) {
        ipc::write_frame(stream, chunk, ipc::MAX_CHUNK)?;
    }
    Ok(())
}

fn dispatch(shared: &Shared, req: &Request, shutting_down: &mut bool) -> (Response, IpcBody) {
    let rid = req.id.clone();
    if req.v != PROTO_MAJOR {
        return (
            Response::err(
                rid,
                IpcError::new(
                    "store.version_mismatch",
                    format!("client proto v{}, daemon supports v{}", req.v, PROTO_MAJOR),
                ),
            ),
            IpcBody::Bytes(Vec::new()),
        );
    }
    let plain = |resp: Response| (resp, IpcBody::Bytes(Vec::new()));
    // Methods that precompute their whole body in one go.
    let bytes = |(resp, body): (Response, Vec<u8>)| (resp, IpcBody::Bytes(body));
    match req.method.as_str() {
        "ping" => plain(Response::ok(
            rid,
            json!({
                "proto": {"major": PROTO_MAJOR, "minor": PROTO_MINOR},
                "daemon_version": env!("CARGO_PKG_VERSION"),
                "capabilities": [
                    "timeline.log",
                    "timeline.log.branch",
                    "timeline.log.details",
                    "timeline.info",
                    "checkpoint.list",
                    "checkpoint.create",
                    "branch.list",
                    "branch.graph",
                    "branch.create",
                    "branch.rename",
                    "branch.delete",
                    "restore.plan",
                    "restore.apply",
                    "restore.resume",
                    "restore.abandon",
                    "fragment.plan",
                    "fragment.apply",
                    "smart.plan",
                    "store.gc","store.doctor",
                    "diff",
                    "timeline.grep",
                    "timeline.grep.occurrences",
                    "timeline.grep.anchors",
                    "cache.backfill",
                    "worktree.list",
                    "worktree.add",
                    "merge.plan",
                    "merge.apply",
                    "merge.resume",

                ],
            }),
        )),
        "project.status" => plain(project_status(shared, req, rid)),
        "timeline.log" => bytes(timeline_log(shared, req, rid)),
        "timeline.info" => plain(timeline_info(shared, req, rid)),
        "checkpoint.list" => plain(checkpoint_list(shared, req, rid)),
        "checkpoint.create" => plain(checkpoint_create(shared, req, rid)),
        "branch.list" => plain(branch_list(shared, req, rid)),
        "branch.graph" => plain(branch_graph(shared, req, rid)),
        "branch.create" => plain(branch_create(shared, req, rid)),
        "branch.rename" => plain(branch_rename(shared, req, rid)),
        "branch.delete" => plain(branch_delete(shared, req, rid)),
        // Plans stream through the body-chunk channel: the envelope carries
        // a bounded summary, the full plan the bytes.
        "restore.plan" => bytes(restore_plan_streamed(shared, req, rid)),
        "restore.apply" => plain(restore_apply(shared, req, rid)),
        "restore.resume" => plain(restore_resume(shared, req, rid)),
        "restore.abandon" => plain(restore_abandon(shared, req, rid)),
        "fragment.plan" => bytes(fragment_plan_streamed(shared, req, rid)),
        "fragment.apply" => plain(fragment_apply(shared, req, rid)),
        "smart.plan" => plain(smart_plan(shared, req, rid)),
        "store.doctor" => plain(store_doctor(shared, req, rid)),
        "store.gc" => plain(store_gc(shared, req, rid)),
        "diff" => bytes(diff(shared, req, rid)),
        "timeline.grep" => grep(shared, req, rid),
        "cache.backfill" => plain(cache_backfill(shared, req, rid)),
        "worktree.list" => plain(worktree_list(shared, req, rid)),
        "worktree.add" => plain(worktree_add(shared, req, rid)),
        "merge.plan" => plain(merge_plan(shared, req, rid)),
        "merge.apply" => plain(merge_apply(shared, req, rid)),
        "merge.resume" => plain(merge_resume(shared, req, rid)),

        "enroll.notify" => plain(enroll_notify(shared, req, rid)),
        "shutdown" => {
            *shutting_down = true;
            plain(Response::ok(rid, json!({"graceful": true})))
        }
        other => plain(Response::err(
            rid,
            IpcError::new("bad.method", format!("unknown method `{other}`")),
        )),
    }
}

fn require_project<'a>(req: &'a Request, rid: &str) -> Result<&'a Path, Response> {
    let Some(p) = &req.project else {
        return Err(Response::err(
            rid.to_owned(),
            IpcError::new("bad.params", "`project` (canonical root) is required"),
        ));
    };
    Ok(p.as_path())
}

fn project_status(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(p) => normalize(p),
        Err(resp) => return resp,
    };
    let store_root = normalize(&config::store_root(&root));
    let linked_registered = root == store_root
        || sheaf_core::store::linked_worktrees(&store_root)
            .is_ok_and(|items| items.into_iter().any(|item| normalize(&item.path) == root));
    let registered =
        linked_registered && matches!(shared.registered_anywhere(&store_root), Ok(true));
    if !registered {
        return Response::err(
            rid,
            IpcError::new(
                "project.not_enrolled",
                format!("{} is not enrolled", root.display()),
            ),
        );
    }

    let format = config::read_store_format(&root).ok();
    // A restore that could not be finished is otherwise invisible: the tree
    // looks merely odd. Surfacing it here — with its age and whether it has
    // blown the staleness bound — is what makes it actionable.
    let max_age = config::load(&root)
        .map(|c| c.restore.max_resume_age_ms)
        .unwrap_or_else(|_| sheaf_core::config::RestoreConfig::default().max_resume_age_ms);
    let pending = sheaf_core::store::pending_restore_at(&root).map(|intent| {
        let stale = intent.is_stale(max_age);
        json!({
            "target": intent.target,
            "scope": intent.scope,
            "mode": intent.mode,
            "token": intent.token,
            "started_ms": intent.started_ms,
            "age_ms": intent.age_ms(),
            "stale": stale,
            "auto_resume": !stale,
        })
    });
    let pending_merge = sheaf_core::store::pending_merge_at(&root);

    Response::ok(
        rid,
        json!({
            "root": root.display().to_string(),
            "store_root": store_root.display().to_string(),
            "worktree_id": config::worktree_id(&root),
            "registered": true,
            "watching": shared.watching(&store_root),
            "ready": shared.ready(&store_root),
            "cold": shared.cold(&store_root),
            "store_format": format,
            "pending_restore": pending,
            "pending_merge": pending_merge,
        }),
    )
}

/// How long a command to a cold (lazily parked) project waits for the
/// store to open before falling back to the warming error. Must stay
/// under the CLI's default 2s call timeout so the client sees a proper
/// error, and keeps the warm-up contract intact: a mutation is never
/// queued to execute after its caller has given up on it.
const COLD_OPEN_BUDGET: Duration = Duration::from_millis(1250);
#[derive(Clone, Debug)]

struct ProjectControl {
    sender: Sender<StoreCommand>,
    worktree: PathBuf,
}

impl ProjectControl {
    fn send(
        &self,
        command: StoreCommand,
    ) -> std::result::Result<(), std::sync::mpsc::SendError<StoreCommand>> {
        self.sender.send(StoreCommand::InWorktree {
            root: self.worktree.clone(),
            command: Box::new(command),
        })
    }
}

fn project_control(
    shared: &Shared,
    root: &Path,
    rid: &str,
) -> std::result::Result<ProjectControl, Response> {
    let worktree = normalize(root);
    let store_root = normalize(&config::store_root(&worktree));
    let registered_worktree = worktree == store_root
        || sheaf_core::store::linked_worktrees(&store_root).is_ok_and(|items| {
            items
                .into_iter()
                .any(|item| item.present && normalize(&item.path) == worktree)
        });
    if !registered_worktree {
        return Err(Response::err(
            rid.to_owned(),
            IpcError::new(
                "project.not_enrolled",
                "worktree is not registered with this store",
            ),
        ));
    }
    let (cold, ready, wake, control) = {
        let table = shared.table.lock().unwrap();
        let Some(entry) = table.get(&store_root) else {
            return Err(Response::err(
                rid.to_owned(),
                IpcError::new("project.not_enrolled", "project is not currently watched"),
            ));
        };
        (
            entry.cold.clone(),
            entry.ready.clone(),
            entry.wake.clone(),
            entry.control.clone(),
        )
        // Lock released before any waiting: the table must never be held
        // across a cold-open wait, or one slow project would stall every
        // other IPC operation.
    };

    if cold.load(Ordering::Acquire) {
        // Lazy project: trigger the open and give it a bounded head start.
        // The collector boot-reconciles before draining commands, so a
        // command issued after this wait never observes a half-open store.
        let deadline = Instant::now() + COLD_OPEN_BUDGET;
        while cold.load(Ordering::Acquire) && Instant::now() < deadline {
            let _ = wake.send(());
            std::thread::sleep(Duration::from_millis(25));
        }
    }
    if !ready.load(Ordering::Acquire) {
        let detail = if cold.load(Ordering::Acquire) {
            "store is opening (lazy project); retry shortly"
        } else {
            "initial worktree capture is still in progress; retry shortly"
        };
        return Err(Response::err(
            rid.to_owned(),
            IpcError::new("project.warming", detail),
        ));
    }
    Ok(ProjectControl {
        sender: control,
        worktree,
    })
}

fn timeline_log(shared: &Shared, req: &Request, rid: String) -> (Response, Vec<u8>) {
    let root = match require_project(req, &rid) {
        Ok(p) => normalize(p),
        Err(resp) => return (resp, Vec::new()),
    };
    let limit = req
        .params
        .get("limit")
        .and_then(|v| v.as_u64())
        .unwrap_or(50)
        .min(1000) as usize;
    let all = req
        .params
        .get("all")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let branch = req
        .params
        .get("branch")
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    if all && branch.is_some() {
        return (
            Response::err(
                rid,
                IpcError::new("bad.params", "`all` and `branch` cannot be combined"),
            ),
            Vec::new(),
        );
    }
    let follow = req
        .params
        .get("follow")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let path = req
        .params
        .get("path")
        .and_then(|v| v.as_str())
        .map(PathBuf::from);
    let before = req
        .params
        .get("before")
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    let details = req
        .params
        .get("details")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let patch = req
        .params
        .get("patch")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    // A walk that only needs identity/time/provenance (the squash span
    // stats) sets this so per-capture `paths` — unbounded for bulk-change
    // captures — never bloats the envelope past its 1 MiB cap.
    let omit_paths = req
        .params
        .get("omit_paths")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return (response, Vec::new()),
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::TimelineLog {
            all,
            branch,
            path,
            follow,
            limit: if before.is_some() { usize::MAX } else { limit },
            reply: reply_tx,
        })
        .is_err()
    {
        return (
            Response::err(rid, IpcError::new("internal", "project writer stopped")),
            Vec::new(),
        );
    }
    let (mut entries, tips) = match reply_rx.recv_timeout(REQUEST_SOFT) {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => return (core_error(rid, e), Vec::new()),
        Err(_) => {
            return (
                Response::err(rid, IpcError::new("internal", "timeline request timed out")),
                Vec::new(),
            )
        }
    };
    if let Some(before) = before.as_deref() {
        if before.len() < 6 {
            return (
                Response::err(
                    rid,
                    IpcError::new(
                        "state.bad_reference",
                        "capture cursor prefixes require at least 6 hex characters",
                    ),
                ),
                Vec::new(),
            );
        }
        let exact = entries.iter().position(|entry| entry.id == before);
        let matches: Vec<_> = entries
            .iter()
            .enumerate()
            .filter(|(_, entry)| entry.id.starts_with(before))
            .map(|(index, _)| index)
            .collect();
        let pos = if let Some(index) = exact {
            index
        } else {
            match matches.as_slice() {
                [index] => *index,
                [] => {
                    return (
                        Response::err(
                            rid,
                            IpcError::new(
                                "state.bad_reference",
                                format!("unknown cursor `{before}`"),
                            ),
                        ),
                        Vec::new(),
                    )
                }
                _ => {
                    return (
                        Response::err(
                            rid,
                            IpcError::new(
                                "state.bad_reference",
                                format!("ambiguous cursor `{before}`"),
                            ),
                        ),
                        Vec::new(),
                    )
                }
            }
        };
        entries.drain(..=pos);
    }
    entries.truncate(limit);

    let detail_rows = if details || patch {
        let (detail_tx, detail_rx) = channel();
        let references = entries.iter().map(|entry| entry.id.clone()).collect();
        if control
            .send(StoreCommand::CaptureLogDetails {
                references,
                reply: detail_tx,
            })
            .is_err()
        {
            return (
                Response::err(rid, IpcError::new("internal", "project writer stopped")),
                Vec::new(),
            );
        }
        match detail_rx.recv_timeout(REQUEST_SOFT) {
            Ok(Ok(rows)) => Some(rows),
            Ok(Err(error)) => return (core_error(rid, error), Vec::new()),
            Err(_) => {
                return (
                    Response::err(
                        rid,
                        IpcError::new("internal", "timeline detail request timed out"),
                    ),
                    Vec::new(),
                )
            }
        }
    } else {
        None
    };

    let body = match detail_rows {
        Some(rows) => {
            let patches: Vec<String> = if patch {
                rows.iter()
                    .map(|detail| {
                        detail
                            .diff
                            .as_ref()
                            .map(|diff| String::from_utf8_lossy(&diff.render_patch()).into_owned())
                            .unwrap_or_default()
                    })
                    .collect()
            } else {
                Vec::new()
            };
            match serde_json::to_vec(&json!({"details": rows, "patches": patches})) {
                Ok(body) => body,
                Err(error) => {
                    return (
                        Response::err(
                            rid,
                            IpcError::new("internal", format!("serialize log details: {error}")),
                        ),
                        Vec::new(),
                    )
                }
            }
        }
        None => Vec::new(),
    };
    if omit_paths {
        for entry in &mut entries {
            entry.paths.clear();
        }
    }
    let result = json!({"entries": entries, "tips": tips, "degraded": false});
    (Response::ok(rid, result), body)
}

fn timeline_info(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(p) => normalize(p),
        Err(resp) => return resp,
    };
    let Some(reference) = req.params.get("reference").and_then(|v| v.as_str()) else {
        return Response::err(rid, IpcError::new("bad.params", "`reference` is required"));
    };
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return response,
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::CaptureInfo {
            reference: reference.to_owned(),
            reply: reply_tx,
        })
        .is_err()
    {
        return Response::err(rid, IpcError::new("internal", "project writer stopped"));
    }
    match reply_rx.recv_timeout(REQUEST_SOFT) {
        Ok(Ok(info)) => Response::ok(rid, json!({"info": info, "degraded": false})),
        Ok(Err(e)) => core_error(rid, e),
        Err(_) => Response::err(
            rid,
            IpcError::new("internal", "timeline info request timed out"),
        ),
    }
}

fn checkpoint_list(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(p) => normalize(p),
        Err(resp) => return resp,
    };
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return response,
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::ListCheckpoints { reply: reply_tx })
        .is_err()
    {
        return Response::err(rid, IpcError::new("internal", "project writer stopped"));
    }
    match reply_rx.recv_timeout(REQUEST_SOFT) {
        Ok(Ok(checkpoints)) => {
            Response::ok(rid, json!({"checkpoints": checkpoints, "degraded": false}))
        }
        Ok(Err(error)) => core_error(rid, error),
        Err(_) => Response::err(rid, IpcError::new("internal", "checkpoint list timed out")),
    }
}

fn checkpoint_create(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(p) => normalize(p),
        Err(resp) => return resp,
    };
    let Some(name) = req.params.get("name").and_then(|v| v.as_str()) else {
        return Response::err(rid, IpcError::new("bad.params", "`name` is required"));
    };
    let reference = req
        .params
        .get("at")
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return response,
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::CreateCheckpoint {
            name: name.to_owned(),
            reference,
            reply: reply_tx,
        })
        .is_err()
    {
        return Response::err(rid, IpcError::new("internal", "project writer stopped"));
    }
    match reply_rx.recv_timeout(REQUEST_SOFT) {
        Ok(Ok(checkpoint)) => Response::ok(rid, json!({"checkpoint": checkpoint})),
        Ok(Err(e)) => core_error(rid, e),
        Err(_) => Response::err(
            rid,
            IpcError::new("internal", "checkpoint request timed out"),
        ),
    }
}
fn branch_list(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(project) => normalize(project),
        Err(response) => return response,
    };
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return response,
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::ListBranches { reply: reply_tx })
        .is_err()
    {
        return Response::err(rid, IpcError::new("internal", "project writer stopped"));
    }
    match reply_rx.recv_timeout(REQUEST_SOFT) {
        Ok(Ok(branches)) => Response::ok(rid, json!({"branches": branches, "degraded": false})),
        Ok(Err(error)) => core_error(rid, error),
        Err(_) => Response::err(rid, IpcError::new("internal", "branch list timed out")),
    }
}

fn branch_graph(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(project) => normalize(project),
        Err(response) => return response,
    };
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return response,
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::BranchGraph { reply: reply_tx })
        .is_err()
    {
        return Response::err(rid, IpcError::new("internal", "project writer stopped"));
    }
    match reply_rx.recv_timeout(REQUEST_SOFT) {
        Ok(Ok(graph)) => Response::ok(rid, json!({"graph": graph, "degraded": false})),
        Ok(Err(error)) => core_error(rid, error),
        Err(_) => Response::err(rid, IpcError::new("internal", "branch graph timed out")),
    }
}

fn branch_create(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(project) => normalize(project),
        Err(response) => return response,
    };
    let Some(name) = req.params.get("name").and_then(|value| value.as_str()) else {
        return Response::err(rid, IpcError::new("bad.params", "`name` is required"));
    };
    let reference = req
        .params
        .get("at")
        .and_then(|value| value.as_str())
        .map(str::to_owned);
    let metadata = match req.params.get("metadata") {
        None | Some(serde_json::Value::Null) => BTreeMap::new(),
        Some(value) => match serde_json::from_value(value.clone()) {
            Ok(metadata) => metadata,
            Err(_) => {
                return Response::err(
                    rid,
                    IpcError::new("bad.params", "`metadata` must be a string-to-string object"),
                )
            }
        },
    };
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return response,
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::CreateBranch {
            name: name.to_owned(),
            reference,
            metadata,
            reply: reply_tx,
        })
        .is_err()
    {
        return Response::err(rid, IpcError::new("internal", "project writer stopped"));
    }
    match reply_rx.recv_timeout(REQUEST_SOFT) {
        Ok(Ok(branch)) => Response::ok(rid, json!({"branch": branch})),
        Ok(Err(error)) => core_error(rid, error),
        Err(_) => Response::err(rid, IpcError::new("internal", "branch create timed out")),
    }
}

fn branch_rename(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(project) => normalize(project),
        Err(response) => return response,
    };
    let Some(old_name) = req.params.get("old_name").and_then(|value| value.as_str()) else {
        return Response::err(rid, IpcError::new("bad.params", "`old_name` is required"));
    };
    let Some(new_name) = req.params.get("new_name").and_then(|value| value.as_str()) else {
        return Response::err(rid, IpcError::new("bad.params", "`new_name` is required"));
    };
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return response,
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::RenameBranch {
            old_name: old_name.to_owned(),
            new_name: new_name.to_owned(),
            reply: reply_tx,
        })
        .is_err()
    {
        return Response::err(rid, IpcError::new("internal", "project writer stopped"));
    }
    match reply_rx.recv_timeout(REQUEST_SOFT) {
        Ok(Ok(branch)) => Response::ok(rid, json!({"branch": branch})),
        Ok(Err(error)) => core_error(rid, error),
        Err(_) => Response::err(rid, IpcError::new("internal", "branch rename timed out")),
    }
}

fn branch_delete(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(project) => normalize(project),
        Err(response) => return response,
    };
    let Some(name) = req.params.get("name").and_then(|value| value.as_str()) else {
        return Response::err(rid, IpcError::new("bad.params", "`name` is required"));
    };
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return response,
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::DeleteBranch {
            name: name.to_owned(),
            reply: reply_tx,
        })
        .is_err()
    {
        return Response::err(rid, IpcError::new("internal", "project writer stopped"));
    }
    match reply_rx.recv_timeout(REQUEST_SOFT) {
        Ok(Ok(branch)) => Response::ok(rid, json!({"branch": branch})),
        Ok(Err(error)) => core_error(rid, error),
        Err(_) => Response::err(rid, IpcError::new("internal", "branch delete timed out")),
    }
}

fn restore_plan_streamed(shared: &Shared, req: &Request, rid: String) -> (Response, Vec<u8>) {
    let root = match require_project(req, &rid) {
        Ok(p) => normalize(p),
        Err(resp) => return (resp, Vec::new()),
    };
    let Some(reference) = req.params.get("at").and_then(|v| v.as_str()) else {
        return (
            Response::err(
                rid,
                IpcError::new("bad.params", "`at` (a timeline reference) is required"),
            ),
            Vec::new(),
        );
    };
    let scope = match string_list(req.params.get("paths")) {
        Ok(list) => list,
        Err(message) => {
            return (
                Response::err(rid, IpcError::new("bad.params", message)),
                Vec::new(),
            )
        }
    };
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return (response, Vec::new()),
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::PlanRestore {
            reference: reference.to_owned(),
            scope,
            reply: reply_tx,
        })
        .is_err()
    {
        return (
            Response::err(rid, IpcError::new("internal", "project writer stopped")),
            Vec::new(),
        );
    }
    match reply_rx.recv_timeout(REQUEST_SOFT) {
        Ok(Ok(plan)) => {
            // The envelope keeps a bounded summary so thin clients (and the
            // error path) never need the body; the full plan rides body
            // chunks, lifting the ~8k-path ceiling the envelope imposed.
            let body = serde_json::to_vec(&plan).unwrap_or_default();
            let summary = plan_summary(&plan);
            (
                Response::ok(
                    rid,
                    json!({"plan_summary": summary, "degraded": plan.degraded}),
                ),
                body,
            )
        }
        Ok(Err(e)) => (core_error(rid, e), Vec::new()),
        Err(_) => (
            Response::err(rid, IpcError::new("internal", "restore plan timed out")),
            Vec::new(),
        ),
    }
}

/// Envelope-side summary of a streamed plan: everything a UI needs to show
/// intent without listing ten thousand actions.
fn plan_summary(plan: &sheaf_core::store::RestorePlan) -> serde_json::Value {
    json!({
        "token": plan.token,
        "mode": plan.mode,
        "scope": plan.scope,
        "base": plan.base,
        "target": plan.target,
        "actions_total": plan.actions.len(),
        "writes": plan.writes(),
        "deletes": plan.deletes(),
        "unchanged": plan.unchanged,
        "locally_modified": plan.locally_modified,
        "obstructions": plan.obstructions,
        "scope_missing": plan.scope_missing,
        "created_at_ms": plan.created_at_ms,
        "applicable": plan.applicable(),
        "noop": plan.is_noop(),
    })
}

fn restore_resume(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(p) => normalize(p),
        Err(resp) => return resp,
    };
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return response,
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::ResumeRestore { reply: reply_tx })
        .is_err()
    {
        return Response::err(rid, IpcError::new("internal", "project writer stopped"));
    }
    match reply_rx.recv_timeout(RESTORE_HARD) {
        Ok(Ok(outcome)) => Response::ok(rid, json!({"outcome": outcome})),
        Ok(Err(e)) => core_error(rid, e),
        Err(_) => Response::err(rid, IpcError::new("internal", "restore resume timed out")),
    }
}

fn restore_abandon(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(p) => normalize(p),
        Err(resp) => return resp,
    };
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return response,
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::AbandonRestore { reply: reply_tx })
        .is_err()
    {
        return Response::err(rid, IpcError::new("internal", "project writer stopped"));
    }
    match reply_rx.recv_timeout(RESTORE_HARD) {
        Ok(Ok(capture)) => Response::ok(
            rid,
            json!({
                "abandoned": true,
                // The reconciliation capture, when the half-applied state
                // needed preserving before the intent was dropped.
                "reconciled_as": capture.map(|c| c.id),
            }),
        ),
        Ok(Err(e)) => core_error(rid, e),
        Err(_) => Response::err(rid, IpcError::new("internal", "restore abandon timed out")),
    }
}

fn store_gc(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(p) => normalize(p),
        Err(resp) => return resp,
    };
    let apply = req
        .params
        .get("apply")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let mark = req
        .params
        .get("mark")
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return response,
    };
    if let Some(reference) = mark {
        let (reply_tx, reply_rx) = channel();
        if control
            .send(StoreCommand::Mark {
                reference,
                reply: reply_tx,
            })
            .is_err()
        {
            return Response::err(rid, IpcError::new("internal", "project writer stopped"));
        }
        return match reply_rx.recv_timeout(DIFF_HARD) {
            Ok(Ok(marked)) => Response::ok(rid, json!({"mark": marked})),
            Ok(Err(e)) => core_error(rid, e),
            Err(_) => Response::err(rid, IpcError::new("internal", "mark request timed out")),
        };
    }
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::Gc {
            apply,
            reply: reply_tx,
        })
        .is_err()
    {
        return Response::err(rid, IpcError::new("internal", "project writer stopped"));
    }
    match reply_rx.recv_timeout(DIFF_HARD) {
        Ok(Ok(outcome)) => Response::ok(rid, json!({"gc": outcome})),
        Ok(Err(e)) => core_error(rid, e),
        Err(_) => Response::err(rid, IpcError::new("internal", "gc request timed out")),
    }
}

fn store_doctor(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(p) => normalize(p),
        Err(resp) => return resp,
    };
    let fix = req
        .params
        .get("fix")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return response,
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::Doctor {
            fix,
            reply: reply_tx,
        })
        .is_err()
    {
        return Response::err(rid, IpcError::new("internal", "project writer stopped"));
    }
    match reply_rx.recv_timeout(DIFF_HARD) {
        Ok(Ok(reply)) => Response::ok(rid, json!({"report": reply})),
        Ok(Err(e)) => core_error(rid, e),
        Err(_) => Response::err(rid, IpcError::new("internal", "doctor request timed out")),
    }
}

fn restore_apply(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(p) => normalize(p),
        Err(resp) => return resp,
    };
    let Some(token) = req.params.get("token").and_then(|v| v.as_str()) else {
        return Response::err(
            rid,
            IpcError::new("bad.params", "`token` from `restore.plan` is required"),
        );
    };
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return response,
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::ApplyRestore {
            token: token.to_owned(),
            reply: reply_tx,
        })
        .is_err()
    {
        return Response::err(rid, IpcError::new("internal", "project writer stopped"));
    }
    // Past its fsync line a restore always runs to completion, so the hard
    // deadline bounds waiting for the answer, never the work itself.
    match reply_rx.recv_timeout(RESTORE_HARD) {
        // `progress_log` rides inside the outcome; duplicating it at the top
        // level only spent envelope budget.
        Ok(Ok(outcome)) => Response::ok(rid, json!({"outcome": outcome})),
        Ok(Err(e)) => core_error(rid, e),
        Err(_) => Response::err(rid, IpcError::new("internal", "restore timed out")),
    }
}

fn string_list(value: Option<&serde_json::Value>) -> std::result::Result<Vec<String>, String> {
    match value {
        None | Some(serde_json::Value::Null) => Ok(Vec::new()),
        Some(serde_json::Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| "`paths` must contain only strings".to_owned())
            })
            .collect(),
        Some(_) => Err("`paths` must be an array of strings".to_owned()),
    }
}

/// `fragment.plan`: dry-run a selection-scoped restore. The
/// envelope carries a bounded summary; the full plan streams as one body
/// chunk, mirroring `restore.plan`'s framing.
fn fragment_plan_streamed(shared: &Shared, req: &Request, rid: String) -> (Response, Vec<u8>) {
    let root = match require_project(req, &rid) {
        Ok(p) => normalize(p),
        Err(resp) => return (resp, Vec::new()),
    };
    let Some(raw_selections) = req.params.get("selections") else {
        return (
            Response::err(
                rid,
                IpcError::new(
                    "bad.params",
                    "`selections` (an array of selection handles from `timeline.grep`) is required",
                ),
            ),
            Vec::new(),
        );
    };
    let selections: Vec<sheaf_core::store::SelectionHandle> =
        match serde_json::from_value(raw_selections.clone()) {
            Ok(selections) => selections,
            Err(error) => {
                return (
                    Response::err(
                        rid,
                        IpcError::new("bad.params", format!("invalid selection handle: {error}")),
                    ),
                    Vec::new(),
                )
            }
        };
    if selections.is_empty() {
        return (
            Response::err(
                rid,
                IpcError::new("bad.params", "`selections` must hold at least one handle"),
            ),
            Vec::new(),
        );
    }
    let mode = match req.params.get("mode").and_then(|v| v.as_str()) {
        None | Some("replace") => sheaf_core::store::FragmentMode::Replace,
        Some(raw) => match sheaf_core::store::FragmentMode::parse(raw) {
            Ok(mode) => mode,
            Err(error) => {
                return (
                    Response::err(rid, IpcError::new("bad.params", error.to_string())),
                    Vec::new(),
                )
            }
        },
    };
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return (response, Vec::new()),
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::PlanFragment {
            selections,
            mode,
            reply: reply_tx,
        })
        .is_err()
    {
        return (
            Response::err(rid, IpcError::new("internal", "project writer stopped")),
            Vec::new(),
        );
    }
    match reply_rx.recv_timeout(REQUEST_SOFT) {
        Ok(Ok(plan)) => {
            let body = serde_json::to_vec(&plan).unwrap_or_default();
            let summary = json!({
                "token": plan.token,
                "mode": plan.mode,
                "files": plan.files.len(),
                "actions": plan.files.iter().map(|f| f.actions.len()).sum::<usize>(),
                "conflicts": plan.conflicts.len(),
                "unchanged": plan.unchanged,
                "applicable": plan.applicable(),
                "noop": plan.is_noop(),
                "paths": plan.destination_paths(),
                "created_at_ms": plan.created_at_ms,
                "degraded": plan.degraded,
            });
            (Response::ok(rid, json!({"plan_summary": summary})), body)
        }
        Ok(Err(e)) => (core_error(rid, e), Vec::new()),
        Err(_) => (
            Response::err(rid, IpcError::new("internal", "fragment plan timed out")),
            Vec::new(),
        ),
    }
}

/// `fragment.apply`: execute a fragment plan by token, under the
/// same collector-local token discipline as `restore.apply`.
fn fragment_apply(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(p) => normalize(p),
        Err(resp) => return resp,
    };
    let Some(token) = req.params.get("token").and_then(|v| v.as_str()) else {
        return Response::err(
            rid,
            IpcError::new("bad.params", "`token` from `fragment.plan` is required"),
        );
    };
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return response,
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::ApplyFragment {
            token: token.to_owned(),
            reply: reply_tx,
        })
        .is_err()
    {
        return Response::err(rid, IpcError::new("internal", "project writer stopped"));
    }
    match reply_rx.recv_timeout(RESTORE_HARD) {
        Ok(Ok(outcome)) => Response::ok(rid, json!({"outcome": outcome})),
        Ok(Err(e)) => core_error(rid, e),
        Err(_) => Response::err(rid, IpcError::new("internal", "fragment apply timed out")),
    }
}

/// `smart.plan`: selection-scoped squash planning. With no `head_texts`
/// the reply names the candidate destination paths whose HEAD content the
/// caller must fetch (git stays on the CLI side, never in the daemon);
/// with `head_texts`, the reply is the plan. Read-only either way.
fn smart_plan(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(p) => normalize(p),
        Err(resp) => return resp,
    };
    let Some(raw_selections) = req.params.get("selections") else {
        return Response::err(
            rid,
            IpcError::new(
                "bad.params",
                "`selections` (an array of selection handles from `timeline.grep`) is required",
            ),
        );
    };
    let selections: Vec<sheaf_core::store::SelectionHandle> =
        match serde_json::from_value(raw_selections.clone()) {
            Ok(selections) => selections,
            Err(error) => {
                return Response::err(
                    rid,
                    IpcError::new("bad.params", format!("invalid selection handle: {error}")),
                )
            }
        };
    if selections.is_empty() {
        return Response::err(
            rid,
            IpcError::new("bad.params", "`selections` must hold at least one handle"),
        );
    }
    let head_texts = match req.params.get("head_texts") {
        None => None,
        Some(value) => match serde_json::from_value(value.clone()) {
            Ok(map) => Some(map),
            Err(error) => {
                return Response::err(
                    rid,
                    IpcError::new(
                        "bad.params",
                        format!("`head_texts` must map path → text: {error}"),
                    ),
                )
            }
        },
    };
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return response,
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::PlanSmart {
            selections,
            head_texts,
            reply: reply_tx,
        })
        .is_err()
    {
        return Response::err(rid, IpcError::new("internal", "project writer stopped"));
    }
    match reply_rx.recv_timeout(RESTORE_HARD) {
        Ok(Ok(SmartPlanReply::Paths(paths))) => {
            Response::ok(rid, json!({ "phase": "resolve", "paths": paths }))
        }
        Ok(Ok(SmartPlanReply::Plan(plan))) => {
            let summary = json!({
                "phase": "plan",
                "files": plan.files.len(),
                "conflicts": plan.conflicts.len(),
                "unchanged": plan.unchanged,
                "patch_sha256": plan.patch_sha256,
            });
            Response::ok(rid, json!({ "plan_summary": summary, "plan": *plan }))
        }
        Ok(Err(e)) => core_error(rid, e),
        Err(_) => Response::err(rid, IpcError::new("internal", "smart plan timed out")),
    }
}

/// `diff` is a pure-read verb, but the live document belongs to the
/// collector thread, so it executes there like every other doc read. The
/// rendered patch rides body chunks; the envelope carries the
/// machine-readable entry list without hunks.
fn diff(shared: &Shared, req: &Request, rid: String) -> (Response, Vec<u8>) {
    let root = match require_project(req, &rid) {
        Ok(p) => normalize(p),
        Err(resp) => return (resp, Vec::new()),
    };
    let Some(from) = req.params.get("from").and_then(|v| v.as_str()) else {
        return (
            Response::err(
                rid,
                IpcError::new("bad.params", "`from` (a timeline reference) is required"),
            ),
            Vec::new(),
        );
    };
    let to = req
        .params
        .get("to")
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    let paths = match string_list(req.params.get("paths")) {
        Ok(list) => list,
        Err(message) => {
            return (
                Response::err(rid, IpcError::new("bad.params", message)),
                Vec::new(),
            )
        }
    };
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return (response, Vec::new()),
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::Diff {
            from: from.to_owned(),
            to,
            paths,
            reply: reply_tx,
        })
        .is_err()
    {
        return (
            Response::err(rid, IpcError::new("internal", "project writer stopped")),
            Vec::new(),
        );
    }
    match reply_rx.recv_timeout(DIFF_HARD) {
        Ok(Ok(outcome)) => {
            let patch = outcome.render_patch();
            (
                Response::ok(rid, json!({"diff": outcome, "degraded": false})),
                patch,
            )
        }
        Ok(Err(e)) => (core_error(rid, e), Vec::new()),
        Err(_) => (
            Response::err(rid, IpcError::new("internal", "diff request timed out")),
            Vec::new(),
        ),
    }
}

/// `timeline.grep` is a read-only verb. It runs
/// on the collector like `diff`, returns a bounded summary in the envelope,
/// and streams hit/event records as NDJSON body chunks so the 1 MiB envelope
/// cap never truncates results.
fn grep(shared: &Shared, req: &Request, rid: String) -> (Response, IpcBody) {
    let root = match require_project(req, &rid) {
        Ok(p) => normalize(p),
        Err(resp) => return (resp, IpcBody::Bytes(Vec::new())),
    };
    let mut request: sheaf_core::store::GrepRequest =
        match serde_json::from_value(req.params.clone()) {
            Ok(request) => request,
            Err(e) => {
                return (
                    Response::err(
                        rid,
                        IpcError::new("bad.params", format!("invalid grep request: {e}")),
                    ),
                    IpcBody::Bytes(Vec::new()),
                )
            }
        };
    // Reject malformed requests here, before any bytes are streamed: once
    // the streamed envelope is out, failures travel as terminal body
    // records, and a bad extent deserves a real IPC error.
    if let Err(e) = request.validate() {
        return (core_error(rid, e), IpcBody::Bytes(Vec::new()));
    }
    // Clamp client budgets to the daemon's configured maxima: a client may
    // request less, never an unbounded scan.
    request.budget.max_results = request.budget.max_results.min(GREP_MAX_RESULTS);
    request.budget.max_materialized_bytes = request
        .budget
        .max_materialized_bytes
        .min(GREP_MAX_MATERIALIZED_BYTES);
    request.budget.max_elapsed_ms = request.budget.max_elapsed_ms.min(GREP_MAX_ELAPSED_MS);
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return (response, IpcBody::Bytes(Vec::new())),
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::Grep {
            request: Box::new(request),
            reply: reply_tx,
        })
        .is_err()
    {
        return (
            Response::err(rid, IpcError::new("internal", "project writer stopped")),
            IpcBody::Bytes(Vec::new()),
        );
    }
    // The envelope only acknowledges the stream (proto 1.5): records and
    // the final summary arrive as body frames, each flushed the moment the
    // walk finalizes it, terminated by one empty frame.
    let mut resp = Response::ok(rid, json!({"streamed": true, "method": "timeline.grep"}));
    resp.body = Some(ipc::BodyInfo {
        chunks: ipc::STREAMED_BODY_SENTINEL,
    });
    (resp, IpcBody::Stream(reply_rx))
}

/// Envelope with the streamed-body sentinel, then one flushed frame per
/// record as the walk produces it, the summary (or error) as the last
/// non-empty frame, and the empty terminator frame. A stalled or dead
/// collector still terminates the body so a client never hangs.
fn write_streamed_response(
    stream: &mut UnixStream,
    resp: &Response,
    records: Receiver<GrepStreamItem>,
) -> std::io::Result<()> {
    let payload = serde_json::to_vec(resp)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    ipc::write_frame(stream, &payload, MAX_ENVELOPE)?;
    // The last non-empty frame (summary or error), then the terminator.
    fn finish_with(stream: &mut UnixStream, last: serde_json::Value) -> std::io::Result<()> {
        let mut line = serde_json::to_vec(&last)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        line.push(b'\n');
        ipc::write_frame(stream, &line, ipc::MAX_CHUNK)?;
        ipc::write_frame(stream, &[], ipc::MAX_CHUNK)
    }
    loop {
        match records.recv_timeout(DIFF_HARD) {
            Ok(GrepStreamItem::Record(record)) => {
                let mut line = serde_json::to_vec(&record)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                line.push(b'\n');
                ipc::write_frame(stream, &line, ipc::MAX_CHUNK)?;
            }
            Ok(GrepStreamItem::Done(Ok(report))) => {
                return finish_with(stream, json!({"type": "summary", "report": report}));
            }
            Ok(GrepStreamItem::Done(Err(e))) => {
                let code = e.code();
                return finish_with(
                    stream,
                    json!({
                        "type": "error",
                        "code": code,
                        "message": e.to_string(),
                    }),
                );
            }
            Err(_) => {
                tracing::error!("grep record stream stalled past the reply timeout");
                return finish_with(
                    stream,
                    json!({
                        "type": "error",
                        "code": "internal",
                        "message": "grep record stream stalled",
                    }),
                );
            }
        }
    }
}

fn core_error(rid: String, error: sheaf_core::SheafError) -> Response {
    Response::err(rid, IpcError::new(error.code(), error.to_string()))
}

/// Per-call ceilings for daemon-served backfill: at most this many
/// incomplete captures AND at most this much wall time per request, so
/// the collector always returns well inside the reply timeout. Callers
/// loop on `complete` for more; the CLI does.
const CACHE_BACKFILL_MAX_BATCH: u32 = 512;
const CACHE_BACKFILL_PAGE_MS: u64 = 20_000;

/// `cache.backfill`: explicit, idempotent population of the
/// derived grep cache on the store-owning thread. A `rebuild` request
/// wipes first; the reply reports progress and coverage so a caller can
/// page through a large history with repeated bounded calls.
fn cache_backfill(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(p) => normalize(p),
        Err(resp) => return resp,
    };
    let mut opts: sheaf_core::store::GrepBackfillOptions =
        match serde_json::from_value(req.params.clone()) {
            Ok(opts) => opts,
            Err(e) => {
                return Response::err(
                    rid,
                    IpcError::new("bad.params", format!("invalid backfill options: {e}")),
                )
            }
        };
    // Bound every daemon-served run by count and time: materializing a
    // cold page is fork-bound and would otherwise hold the collector past
    // the reply timeout. Offline callers reach the writer directly for
    // unbounded runs.
    opts.limit = Some(
        opts.limit
            .unwrap_or(CACHE_BACKFILL_MAX_BATCH)
            .min(CACHE_BACKFILL_MAX_BATCH),
    );
    opts.max_elapsed_ms = Some(
        opts.max_elapsed_ms
            .unwrap_or(CACHE_BACKFILL_PAGE_MS)
            .min(CACHE_BACKFILL_PAGE_MS),
    );
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return response,
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::CacheBackfill {
            opts,
            reply: reply_tx,
        })
        .is_err()
    {
        return Response::err(rid, IpcError::new("internal", "project writer stopped"));
    }
    match reply_rx.recv_timeout(DIFF_HARD) {
        Ok(Ok(report)) => Response::ok(rid, json!({"backfill": report})),
        Ok(Err(e)) => core_error(rid, e),
        Err(_) => Response::err(rid, IpcError::new("internal", "backfill request timed out")),
    }
}
fn worktree_list(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(path) => normalize(path),
        Err(response) => return response,
    };
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return response,
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::ListWorktrees { reply: reply_tx })
        .is_err()
    {
        return Response::err(rid, IpcError::new("internal", "project writer stopped"));
    }
    match reply_rx.recv_timeout(REQUEST_SOFT) {
        Ok(Ok(worktrees)) => Response::ok(rid, json!({"worktrees": worktrees})),
        Ok(Err(error)) => core_error(rid, error),
        Err(_) => Response::err(rid, IpcError::new("internal", "worktree list timed out")),
    }
}

fn attach_linked_watch(shared: &Shared, store_root: &Path, worktree: &Path) -> Result<()> {
    let cfg = config::load(store_root)?;
    let classifier = watcher::shared_classifier(
        sheaf_core::classify::Classifier::for_project_with(
            worktree,
            &cfg,
            &global_git_ignore_candidates(),
        )
        .map_err(anyhow::Error::msg)?,
    );
    let backend = watcher::default_backend(worktree.to_path_buf(), classifier)?;
    let mut table = shared.table.lock().unwrap();
    let entry = table
        .get_mut(&normalize(store_root))
        .ok_or_else(|| anyhow::anyhow!("primary project is no longer watched"))?;
    let stop = entry.stop.clone();
    let events = entry.events.clone();
    let name = worktree.display().to_string();
    let handle = std::thread::Builder::new()
        .name(format!("inotify:{name}"))
        .spawn(move || backend.run(events, stop))
        .with_context(|| format!("spawn managed worktree watcher for {name}"))?;
    entry.watch_handles.push(handle);
    Ok(())
}

fn worktree_add(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(path) => normalize(path),
        Err(response) => return response,
    };
    let Some(reference) = req.params.get("reference").and_then(|value| value.as_str()) else {
        return Response::err(rid, IpcError::new("bad.params", "`reference` is required"));
    };
    let Some(destination) = req
        .params
        .get("destination")
        .and_then(|value| value.as_str())
    else {
        return Response::err(
            rid,
            IpcError::new("bad.params", "`destination` is required"),
        );
    };
    let destination = PathBuf::from(destination);
    if !destination.is_absolute() {
        return Response::err(
            rid,
            IpcError::new("bad.params", "`destination` must be an absolute path"),
        );
    }
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return response,
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::AddWorktree {
            reference: reference.to_owned(),
            destination,
            reply: reply_tx,
        })
        .is_err()
    {
        return Response::err(rid, IpcError::new("internal", "project writer stopped"));
    }
    match reply_rx.recv_timeout(RESTORE_HARD) {
        Ok(Ok(worktree)) => {
            let store_root = config::store_root(&root);
            match attach_linked_watch(shared, &store_root, &worktree.path) {
                Ok(()) => Response::ok(rid, json!({"worktree": worktree, "watching": true})),
                Err(error) => Response::err(
                    rid,
                    IpcError::new(
                        "unsupported",
                        format!(
                            "worktree was created at {}, but its live watcher failed: {error}; restart sheafd to retry",
                            worktree.path.display()
                        ),
                    ),
                ),
            }
        }
        Ok(Err(error)) => core_error(rid, error),
        Err(_) => Response::err(rid, IpcError::new("internal", "worktree add timed out")),
    }
}

fn merge_plan(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(path) => normalize(path),
        Err(response) => return response,
    };
    let Some(source) = req.params.get("source").and_then(|value| value.as_str()) else {
        return Response::err(rid, IpcError::new("bad.params", "`source` is required"));
    };
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return response,
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::PlanMerge {
            source: source.to_owned(),
            reply: reply_tx,
        })
        .is_err()
    {
        return Response::err(rid, IpcError::new("internal", "project writer stopped"));
    }
    match reply_rx.recv_timeout(DIFF_HARD) {
        Ok(Ok(plan)) => Response::ok(rid, json!({"plan": plan})),
        Ok(Err(error)) => core_error(rid, error),
        Err(_) => Response::err(rid, IpcError::new("internal", "merge plan timed out")),
    }
}

fn merge_apply(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(path) => normalize(path),
        Err(response) => return response,
    };
    let Some(token) = req.params.get("token").and_then(|value| value.as_str()) else {
        return Response::err(rid, IpcError::new("bad.params", "`token` is required"));
    };
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return response,
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::ApplyMerge {
            token: token.to_owned(),
            reply: reply_tx,
        })
        .is_err()
    {
        return Response::err(rid, IpcError::new("internal", "project writer stopped"));
    }
    match reply_rx.recv_timeout(RESTORE_HARD) {
        Ok(Ok(outcome)) => Response::ok(rid, json!({"outcome": outcome})),
        Ok(Err(error)) => core_error(rid, error),
        Err(_) => Response::err(rid, IpcError::new("internal", "merge apply timed out")),
    }
}

fn merge_resume(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(path) => normalize(path),
        Err(response) => return response,
    };
    let control = match project_control(shared, &root, &rid) {
        Ok(control) => control,
        Err(response) => return response,
    };
    let (reply_tx, reply_rx) = channel();
    if control
        .send(StoreCommand::ResumeMerge { reply: reply_tx })
        .is_err()
    {
        return Response::err(rid, IpcError::new("internal", "project writer stopped"));
    }
    match reply_rx.recv_timeout(RESTORE_HARD) {
        Ok(Ok(outcome)) => Response::ok(rid, json!({"outcome": outcome})),
        Ok(Err(error)) => core_error(rid, error),
        Err(_) => Response::err(rid, IpcError::new("internal", "merge resume timed out")),
    }
}

fn enroll_notify(shared: &Shared, req: &Request, rid: String) -> Response {
    let root = match require_project(req, &rid) {
        Ok(p) => normalize(p),
        Err(resp) => return resp,
    };
    if !root.is_dir() || config::read_store_format(&root).is_err() {
        return Response::err(
            rid,
            IpcError::new(
                "project.not_enrolled",
                format!("{} has no valid .sheaf store", root.display()),
            ),
        );
    }
    let spawned = spawn_watch_policy(shared, &root, OpenPolicy::Eager);
    Response::ok(rid, json!({"watching": spawned}))
}

/// Peer uid via SO_PEERCRED (std's peer_cred is still feature-gated).
fn peer_uid(stream: &UnixStream) -> Option<u32> {
    use std::os::unix::io::AsRawFd;
    let mut cred = libc::ucred {
        pid: 0,
        gid: 0,
        uid: 0,
    };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    let ret = unsafe {
        libc::getsockopt(
            stream.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            (&mut cred as *mut libc::ucred).cast(),
            &mut len,
        )
    };
    (ret == 0).then_some(cred.uid)
}

fn current_uid() -> u32 {
    // Std exposes uid through peer_cred only; for self-check reuse the
    // /proc probe from core paths (same convention as paths.rs).
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| {
                    l.strip_prefix("Uid:")
                        .map(|r| r.split_whitespace().next().map(str::to_owned))
                })
                .flatten()
        })
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_prunes_roots_deleted_from_disk_and_keeps_live_ones() {
        let tmp = tempfile::tempdir().unwrap();
        let live = tmp.path().join("live-proj");
        std::fs::create_dir_all(live.join(".sheaf/store")).unwrap();
        sheaf_core::config::write_skeleton(&live).unwrap();

        let reg = Registry::at(tmp.path().join("reg/enrollments.jsonl"));
        reg.upsert(&live).unwrap();
        // Enroll-then-delete so the registry holds the canonical path of a
        // root that no longer exists — exactly what /tmp scratch checkouts
        // leave behind.
        let scratch = tmp.path().join("scratch-proj");
        std::fs::create_dir_all(&scratch).unwrap();
        reg.upsert(&scratch).unwrap();
        std::fs::remove_dir_all(&scratch).unwrap();

        let shared = Shared {
            table: Arc::new(Mutex::new(HashMap::new())),
            stopping: AtomicBool::new(false),
            conns: AtomicUsize::new(0),
            socket_path: PathBuf::from("/nonexistent"),
            wake_fd: -1,
        };
        let (resumed, pruned) = resume_enrollments(&shared, &reg);
        assert_eq!(resumed, 1, "the live project resumes");
        assert_eq!(pruned, 1, "the deleted root is pruned exactly once");

        let remaining = reg.list().unwrap();
        assert_eq!(remaining.len(), 1, "only the live enrollment survives");
        assert!(remaining[0].root.ends_with("live-proj"));
        assert!(shared.watching(&live), "live root is in the watch table");
    }

    #[test]
    fn resume_keeps_a_damaged_but_present_root() {
        // Present-but-broken must NEVER be forgotten: damage may be
        // repairable, and that call belongs to the operator.
        let tmp = tempfile::tempdir().unwrap();
        let present = tmp.path().join("present-but-broken");
        std::fs::create_dir_all(&present).unwrap(); // no .sheaf at all
        let reg = Registry::at(tmp.path().join("reg/enrollments.jsonl"));
        reg.upsert(&present).unwrap();

        let shared = Shared {
            table: Arc::new(Mutex::new(HashMap::new())),
            stopping: AtomicBool::new(false),
            conns: AtomicUsize::new(0),
            socket_path: PathBuf::from("/nonexistent"),
            wake_fd: -1,
        };
        let (resumed, pruned) = resume_enrollments(&shared, &reg);
        assert_eq!((resumed, pruned), (0, 0), "skipped, not pruned");
        assert_eq!(reg.list().unwrap().len(), 1, "enrollment survives damage");
    }

    fn test_shared() -> Arc<Shared> {
        Arc::new(Shared {
            table: Arc::new(Mutex::new(HashMap::new())),
            stopping: AtomicBool::new(false),
            conns: AtomicUsize::new(0),
            socket_path: PathBuf::from("/nonexistent"),
            wake_fd: -1,
        })
    }

    fn wait_until(deadline_ms: u64, f: impl Fn() -> bool) -> bool {
        let deadline = std::time::Instant::now() + Duration::from_millis(deadline_ms);
        while std::time::Instant::now() < deadline {
            if f() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        f()
    }

    fn lock_is_free(root: &Path) -> bool {
        let lock_path = sheaf_core::config::sheaf_dir(root).join("lock");
        sheaf_core::store::try_lock_exclusive(&lock_path)
            .ok()
            .flatten()
            .is_some()
    }

    fn skeleton_project(base: &Path, name: &str) -> PathBuf {
        let root = base.join(name);
        std::fs::create_dir_all(root.join(".sheaf/store")).unwrap();
        sheaf_core::config::write_skeleton(&root).unwrap();
        root
    }

    #[test]
    fn lazy_project_holds_no_store_until_worktree_activity() {
        let tmp = tempfile::tempdir().unwrap();
        let live = skeleton_project(tmp.path(), "lazy-event");
        let shared = test_shared();
        assert!(spawn_watch(&shared, &live));

        // Cold: watched, but no store is open and the writer flock is free.
        assert!(shared.watching(&live));
        assert!(!shared.ready(&live));
        assert!(
            lock_is_free(&live),
            "a cold project must not hold the writer flock"
        );

        // First worktree event: open, reconcile, become hot. The write must
        // land AFTER the watcher's baseline registration (inotify is not
        // retrospective), hence the pause.
        std::thread::sleep(Duration::from_millis(300));
        std::fs::write(live.join("hello.txt"), "first activity").unwrap();
        assert!(
            wait_until(10_000, || shared.ready(&live)),
            "store never opened on first activity"
        );
        assert!(!shared.cold(&live));
        assert!(!lock_is_free(&live), "hot project holds the writer flock");
    }

    #[test]
    fn lazy_project_opens_on_first_ipc_command_and_queues_behind_boot() {
        let tmp = tempfile::tempdir().unwrap();
        let live = skeleton_project(tmp.path(), "lazy-ipc");
        // Present from the start, so the open-time reconcile has a baseline
        // to capture when the IPC command wakes the store.
        std::fs::write(live.join("tracked.txt"), "content").unwrap();
        let shared = test_shared();
        assert!(spawn_watch(&shared, &live));

        let control = match project_control(&shared, &live, "test-rid") {
            Ok(sender) => sender,
            Err(response) => panic!("cold open did not finish within the budget: {response:?}"),
        };

        let (reply_tx, reply_rx) = channel();
        control
            .send(StoreCommand::TimelineLog {
                all: false,
                branch: None,
                path: None,
                follow: false,
                limit: 10,
                reply: reply_tx,
            })
            .unwrap();
        let answer = reply_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("command queued behind the lazy open must still be answered");
        let (captures, _total) = answer.expect("timeline log over a freshly opened store");
        assert!(
            !captures.is_empty(),
            "the open-time reconcile captures the baseline"
        );
        assert!(shared.ready(&live));
        assert!(!shared.cold(&live));
    }

    #[test]
    fn pending_restore_intent_forces_the_eager_policy() {
        let tmp = tempfile::tempdir().unwrap();
        let root = skeleton_project(tmp.path(), "pending");
        // No intent: lazy.
        assert_eq!(effective_policy(&root, OpenPolicy::Lazy), OpenPolicy::Lazy);
        // A pending restore has a staleness deadline; waiting for activity
        // could let it lapse, so such projects must open eagerly.
        let intent = serde_json::json!({
            "token": "test-token",
            "mode": "full",
            "scope": [],
            "target": {"frontier": "f1", "capture_id": null},
            "started_ms": 0_i64,
        });
        std::fs::create_dir_all(root.join(".sheaf/state")).unwrap();
        std::fs::write(
            root.join(".sheaf/state/restore.intent"),
            serde_json::to_string(&intent).unwrap(),
        )
        .unwrap();
        assert_eq!(effective_policy(&root, OpenPolicy::Lazy), OpenPolicy::Eager);
        assert_eq!(
            effective_policy(&root, OpenPolicy::Eager),
            OpenPolicy::Eager
        );
    }

    // ------------------------------------------------------- pure helpers

    #[test]
    fn normalize_and_same_root_handle_canonical_and_missing_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let nested = tmp.path().join("sub");
        std::fs::create_dir_all(&nested).unwrap();
        assert_eq!(normalize(&nested), nested.canonicalize().unwrap());
        let missing = tmp.path().join("does-not-exist");
        assert_eq!(normalize(&missing), missing, "missing paths pass through");
        assert!(same_root(&nested, &nested.canonicalize().unwrap()));
        assert!(same_root(&missing, &missing));
        assert!(!same_root(&nested, &missing));
    }

    #[test]
    fn string_list_accepts_missing_null_and_string_arrays_and_rejects_other_json() {
        assert_eq!(string_list(None).unwrap(), Vec::<String>::new());
        assert_eq!(
            string_list(Some(&serde_json::Value::Null)).unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(
            string_list(Some(&json!(["a", "b"]))).unwrap(),
            vec!["a".to_owned(), "b".to_owned()]
        );
        let err = string_list(Some(&json!([1]))).unwrap_err();
        assert!(err.contains("only strings"));
        assert!(string_list(Some(&json!("x"))).is_err());
    }

    /// Construct every `StoreCommand` variant so the classification helpers
    /// are exercised over the full enum, not just the arms one test happens
    /// to drive.
    #[test]
    fn store_commands_classify_debounce_boundaries_and_memory_weight() {
        let commands: Vec<StoreCommand> = {
            let (tx_log, _) = channel::<
                std::result::Result<
                    (Vec<sheaf_core::store::Capture>, usize),
                    sheaf_core::SheafError,
                >,
            >();
            let (tx_ck, _) = channel::<
                std::result::Result<Vec<sheaf_core::store::Checkpoint>, sheaf_core::SheafError>,
            >();

            let (tx_info, _) = channel::<
                std::result::Result<sheaf_core::store::CaptureInfo, sheaf_core::SheafError>,
            >();
            let (tx_ck_create, _) = channel::<
                std::result::Result<sheaf_core::store::Checkpoint, sheaf_core::SheafError>,
            >();
            let (tx_plan, _) = channel::<
                std::result::Result<sheaf_core::store::RestorePlan, sheaf_core::SheafError>,
            >();
            let (tx_outcome_apply, _) = channel::<
                std::result::Result<sheaf_core::store::RestoreOutcome, sheaf_core::SheafError>,
            >();
            let (tx_outcome_resume, _) = channel::<
                std::result::Result<sheaf_core::store::RestoreOutcome, sheaf_core::SheafError>,
            >();
            let (tx_outcome_frag, _) = channel::<
                std::result::Result<sheaf_core::store::RestoreOutcome, sheaf_core::SheafError>,
            >();
            let (tx_opt_capture, _) = channel::<
                std::result::Result<Option<sheaf_core::store::Capture>, sheaf_core::SheafError>,
            >();
            let (tx_gc, _) = channel::<
                std::result::Result<sheaf_core::store::GcOutcome, sheaf_core::SheafError>,
            >();
            let (tx_mark, _) = channel::<
                std::result::Result<sheaf_core::store::MarkedCapture, sheaf_core::SheafError>,
            >();
            let (tx_doctor, _) = channel::<
                std::result::Result<sheaf_core::store::DoctorReply, sheaf_core::SheafError>,
            >();
            let (tx_diff, _) = channel::<
                std::result::Result<sheaf_core::store::DiffOutcome, sheaf_core::SheafError>,
            >();
            let (tx_stream, _) = channel::<GrepStreamItem>();
            let (tx_backfill, _) = channel::<
                std::result::Result<sheaf_core::store::GrepBackfillReport, sheaf_core::SheafError>,
            >();
            let (tx_frag, _) = channel::<
                std::result::Result<sheaf_core::store::FragmentPlan, sheaf_core::SheafError>,
            >();
            let (tx_smart, _) =
                channel::<std::result::Result<SmartPlanReply, sheaf_core::SheafError>>();
            let grep_request = point_grep("needle");
            vec![
                StoreCommand::TimelineLog {
                    all: false,
                    branch: None,
                    path: None,
                    follow: false,
                    limit: 1,
                    reply: tx_log,
                },
                StoreCommand::ListCheckpoints { reply: tx_ck },
                StoreCommand::CaptureInfo {
                    reference: "@".into(),
                    reply: tx_info,
                },
                StoreCommand::CreateCheckpoint {
                    name: "cp".into(),
                    reference: None,
                    reply: tx_ck_create,
                },
                StoreCommand::PlanRestore {
                    reference: "@".into(),
                    scope: vec![],
                    reply: tx_plan,
                },
                StoreCommand::ApplyRestore {
                    token: "t".into(),
                    reply: tx_outcome_apply,
                },
                StoreCommand::ResumeRestore {
                    reply: tx_outcome_resume,
                },
                StoreCommand::AbandonRestore {
                    reply: tx_opt_capture,
                },
                StoreCommand::Gc {
                    apply: false,
                    reply: tx_gc,
                },
                StoreCommand::Mark {
                    reference: "@".into(),
                    reply: tx_mark,
                },
                StoreCommand::Doctor {
                    fix: false,
                    reply: tx_doctor,
                },
                StoreCommand::Diff {
                    from: "@".into(),
                    to: None,
                    paths: vec![],
                    reply: tx_diff,
                },
                StoreCommand::Grep {
                    request: Box::new(grep_request),
                    reply: tx_stream,
                },
                StoreCommand::CacheBackfill {
                    opts: sheaf_core::store::GrepBackfillOptions::default(),
                    reply: tx_backfill,
                },
                StoreCommand::PlanFragment {
                    selections: vec![],
                    mode: sheaf_core::store::FragmentMode::Replace,
                    reply: tx_frag,
                },
                StoreCommand::ApplyFragment {
                    token: "t".into(),
                    reply: tx_outcome_frag,
                },
                StoreCommand::PlanSmart {
                    selections: vec![],
                    head_texts: None,
                    reply: tx_smart,
                },
            ]
        };
        let boundaries: Vec<bool> = commands
            .iter()
            .map(|c| c.crosses_debounce_boundary())
            .collect();
        assert_eq!(
            boundaries,
            vec![
                false, // TimelineLog
                false, // ListCheckpoints
                false, // CaptureInfo
                true,  // CreateCheckpoint
                false, // PlanRestore
                true,  // ApplyRestore
                true,  // ResumeRestore
                false, // AbandonRestore
                false, // Gc
                false, // Mark
                false, // Doctor
                false, // Diff
                false, // Grep
                false, // CacheBackfill
                false, // PlanFragment
                true,  // ApplyFragment
                false, // PlanSmart
            ]
        );
        let heavy: Vec<bool> = commands.iter().map(|c| c.is_memory_heavy()).collect();
        assert_eq!(
            heavy,
            vec![
                false, // TimelineLog
                false, // ListCheckpoints
                false, // CaptureInfo
                false, // CreateCheckpoint
                false, // PlanRestore
                false, // ApplyRestore
                false, // ResumeRestore
                false, // AbandonRestore
                true,  // Gc
                false, // Mark
                true,  // Doctor
                true,  // Diff
                true,  // Grep
                true,  // CacheBackfill
                false, // PlanFragment
                false, // ApplyFragment
                false, // PlanSmart
            ]
        );
    }

    #[test]
    fn wait_bounded_returns_for_finished_threads_and_abandons_hangs_past_grace() {
        let finished = std::thread::spawn(|| {});
        wait_bounded(finished, Duration::from_secs(5), "quick-finish");
        let hanging = std::thread::spawn(|| {
            std::thread::sleep(Duration::from_millis(250));
        });
        let start = Instant::now();
        wait_bounded(hanging, Duration::ZERO, "hung-thread");
        assert!(
            start.elapsed() < Duration::from_millis(200),
            "an exceeded grace must not be waited out"
        );
    }

    #[test]
    fn set_mode_applies_unix_modes_and_fails_on_missing_paths() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("modeled");
        std::fs::write(&file, "x").unwrap();
        set_mode(&file, 0o600).unwrap();
        assert_eq!(
            std::fs::metadata(&file).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(set_mode(&tmp.path().join("missing"), 0o600).is_err());
    }

    #[test]
    fn shared_predicates_reflect_the_watch_table_and_stop_wakes_the_self_pipe() {
        let mut fds = [0 as libc::c_int; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        let (read_fd, write_fd) = (fds[0], fds[1]);
        let shared = Arc::new(Shared {
            table: Arc::new(Mutex::new(HashMap::new())),
            stopping: AtomicBool::new(false),
            conns: AtomicUsize::new(0),
            socket_path: PathBuf::from("/nonexistent"),
            wake_fd: write_fd,
        });
        let root = PathBuf::from("/tmp/sheafd-predicates-root");
        assert!(!shared.watching(&root));
        assert!(!shared.cold(&root));
        assert!(!shared.ready(&root));

        let (control_tx, _control_rx) = channel::<StoreCommand>();
        let (wake_tx, _wake_rx) = channel::<()>();
        let (events, _events_rx) = channel();

        shared.table.lock().unwrap().insert(
            normalize(&root),
            WatchEntry {
                stop: watcher::new_stop_flag(),
                cold: Arc::new(AtomicBool::new(true)),
                ready: Arc::new(AtomicBool::new(false)),
                watch_handles: Vec::new(),
                collector: None,
                control: control_tx,
                events,

                wake: wake_tx,
            },
        );
        assert!(shared.watching(&root));
        assert!(shared.cold(&root));
        assert!(!shared.ready(&root));

        shared.request_stop();
        assert!(shared.stopping.load(Ordering::SeqCst));
        let mut byte = [0u8; 1];
        let n = unsafe { libc::read(read_fd, byte.as_mut_ptr().cast(), 1) };
        assert_eq!(n, 1, "request_stop must wake the shutdown self-pipe");
        unsafe {
            libc::close(read_fd);
            libc::close(write_fd);
        }
    }

    #[test]
    fn peer_uid_identifies_self_over_a_socketpair() {
        let (a, b) = UnixStream::pair().unwrap();
        assert_eq!(peer_uid(&a), Some(current_uid()));
        assert_eq!(peer_uid(&b), Some(current_uid()));
    }

    #[test]
    fn tracing_and_thp_setup_are_safe_inside_a_process() {
        init_tracing();
        disable_thp_for_this_process();
        trim_process_heap();
    }

    #[test]
    fn probe_incumbent_fails_when_no_daemon_listens() {
        let tmp = tempfile::tempdir().unwrap();
        let socket = tmp.path().join("nothing.sock");
        assert!(probe_incumbent(&socket).is_err());
    }

    #[test]
    fn registry_singleton_refuses_a_second_daemon_and_releases_on_drop() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("registry");
        let socket_a = tmp.path().join("a/control.sock");
        let socket_b = tmp.path().join("b/control.sock");

        let first = acquire_registry_singleton_at(&dir, &socket_a).expect("first holder locks");
        let identity = std::fs::read_to_string(dir.join("daemon.lock")).unwrap();
        assert!(
            identity.contains(&format!("pid={}", std::process::id())),
            "lock file must record the holder: {identity}"
        );
        assert!(
            identity.contains(&socket_a.display().to_string()),
            "lock file must record the socket: {identity}"
        );

        let refusal = acquire_registry_singleton_at(&dir, &socket_b)
            .expect_err("a second daemon on the same registry must be refused");
        let msg = format!("{refusal:#}");
        assert!(
            msg.contains("another sheafd") && msg.contains("pid="),
            "refusal must name the incumbent: {msg}"
        );

        drop(first);
        let second = acquire_registry_singleton_at(&dir, &socket_b)
            .expect("the lock releases with its holder");
        drop(second);
    }

    // ------------------------------------------------ store-backed paths

    /// Reply budget for store-backed unit tests; generous, but bounded so a
    /// broken writer surfaces as a timeout instead of a hang.
    const TEN_SECS: Duration = Duration::from_secs(10);

    fn point_grep(needle: &str) -> sheaf_core::store::GrepRequest {
        sheaf_core::store::GrepRequest {
            query: sheaf_core::store::GrepQuery::literal(needle),
            mode: sheaf_core::store::GrepMode::Point,
            at: None,
            anchor: None,
            from: None,
            to: None,
            path: None,
            follow: false,
            all: false,
            every_capture: false,
            extent: sheaf_core::store::SelectionExtent::Match,
            budget: sheaf_core::store::SearchBudget::default(),
            cursor: None,
        }
    }

    fn run_command(
        store: &mut ProjectStore,
        ignore: &dyn sheaf_core::ignore::ExcludesRel,
        plans: &mut Vec<sheaf_core::store::RestorePlan>,
        frags: &mut Vec<sheaf_core::store::FragmentPlan>,
        command: StoreCommand,
    ) -> Option<sheaf_core::store::RestoreOutcome> {
        let mut merge_plans = Vec::new();
        handle_store_command(
            store,
            ignore,
            plans,
            frags,
            &mut merge_plans,
            command,
            None,
            60_000,
        )
    }

    /// Open a real writer-locked store over a skeleton project.
    fn opened_store(
        root: &Path,
    ) -> (
        ProjectStore,
        sheaf_core::classify::Classifier,
        std::fs::File,
    ) {
        let classifier = sheaf_core::classify::Classifier::from_volatile_patterns(&[]).unwrap();
        let (store, lock) =
            open_store_locked(root, StoreLimits::default(), 8 * 1024 * 1024).unwrap();
        (store, classifier, lock)
    }

    #[test]
    fn open_store_locked_fails_while_the_flock_is_held_then_succeeds() {
        let tmp = tempfile::tempdir().unwrap();
        let root = skeleton_project(tmp.path(), "flock");
        let lock_path = sheaf_dir(&root).join("lock");
        let held = sheaf_core::store::try_lock_exclusive(&lock_path)
            .unwrap()
            .expect("free lock is acquired");
        assert!(
            open_store_locked(&root, StoreLimits::default(), 8 * 1024 * 1024).is_err(),
            "a second writer must be refused while the flock is held"
        );
        drop(held);
        let (_store, _ignore, lock) = opened_store(&root);
        drop(lock);
    }

    #[test]
    fn boot_resume_is_noop_without_an_intent_and_reports_failures_as_none() {
        let tmp = tempfile::tempdir().unwrap();
        let root = skeleton_project(tmp.path(), "boot-resume");
        std::fs::write(root.join("seed.txt"), "seed\n").unwrap();
        let (mut store, ignore, _lock) = opened_store(&root);

        // No intent at all: plain Ok(None) — nothing to resume.
        let resumed = resume_interrupted_restore(&root, &mut store, &ignore, 60_000);
        assert!(resumed.is_none());

        // A fresh intent pointing at a bogus frontier cannot be replayed:
        // the failure is logged and surfaced as None, never a panic.
        let intent = serde_json::json!({
            "token": "bogus-token",
            "mode": "full",
            "scope": [],
            "target": {"frontier": "f1", "capture_id": null},
            "started_ms": chrono::Utc::now().timestamp_millis(),
        });
        std::fs::create_dir_all(root.join(".sheaf/state")).unwrap();
        std::fs::write(
            root.join(".sheaf/state/restore.intent"),
            serde_json::to_string(&intent).unwrap(),
        )
        .unwrap();
        let resumed = resume_interrupted_restore(&root, &mut store, &ignore, 60_000);
        assert!(
            resumed.is_none(),
            "a failed resume must not fabricate an outcome"
        );
    }

    #[test]
    fn store_dispatch_answers_read_and_write_verbs_over_a_live_store() {
        let tmp = tempfile::tempdir().unwrap();
        let root = skeleton_project(tmp.path(), "read-verbs");
        std::fs::write(root.join("seed.txt"), "seed content\n").unwrap();
        let (mut store, ignore, _lock) = opened_store(&root);
        store.reconcile_worktree(&ignore).unwrap();
        let mut plans = Vec::new();
        let mut frags = Vec::new();

        // timeline.log: entries plus the branch-tip count.
        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::TimelineLog {
                all: false,
                branch: None,
                path: None,
                follow: false,
                limit: 10,
                reply: tx,
            },
        );
        let (captures, tips) = rx.recv_timeout(TEN_SECS).unwrap().unwrap();
        assert!(!captures.is_empty(), "the reconcile captured the seed file");
        assert!(tips >= 1);

        // checkpoint.list starts empty; checkpoint.create lands one.
        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::ListCheckpoints { reply: tx },
        );
        assert!(rx.recv_timeout(TEN_SECS).unwrap().unwrap().is_empty());

        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::CreateCheckpoint {
                name: "cp-one".into(),
                reference: None,
                reply: tx,
            },
        );
        assert_eq!(rx.recv_timeout(TEN_SECS).unwrap().unwrap().name, "cp-one");

        // A lost debounce tail poisons the mutation: the flush error wins.
        let (tx, rx) = channel();
        handle_store_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            &mut Vec::new(),
            StoreCommand::CreateCheckpoint {
                name: "cp-two".into(),
                reference: None,
                reply: tx,
            },
            Some(sheaf_core::SheafError::Config("tail lost".into())),
            60_000,
        );
        assert!(rx.recv_timeout(TEN_SECS).unwrap().is_err());

        // timeline.info: good reference answers, bad one errors.
        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::CaptureInfo {
                reference: "@".into(),
                reply: tx,
            },
        );
        assert!(rx.recv_timeout(TEN_SECS).unwrap().is_ok());
        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::CaptureInfo {
                reference: "bogus-ref".into(),
                reply: tx,
            },
        );
        assert!(rx.recv_timeout(TEN_SECS).unwrap().is_err());

        // diff: the live worktree against @ is empty but answerable; a
        // nonsense reference errors.
        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::Diff {
                from: "@".into(),
                to: None,
                paths: vec![],
                reply: tx,
            },
        );
        assert!(rx.recv_timeout(TEN_SECS).unwrap().is_ok());
        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::Diff {
                from: "bogus-ref".into(),
                to: None,
                paths: vec![],
                reply: tx,
            },
        );
        assert!(rx.recv_timeout(TEN_SECS).unwrap().is_err());

        // store.gc report (read-only) answers on a fresh store.
        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::Gc {
                apply: false,
                reply: tx,
            },
        );
        assert!(rx.recv_timeout(TEN_SECS).unwrap().is_ok());

        // Retention marks refuse the current head.
        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::Mark {
                reference: "@".into(),
                reply: tx,
            },
        );
        assert!(
            rx.recv_timeout(TEN_SECS).unwrap().is_err(),
            "marking the head is the present, not restorable history"
        );

        // store.doctor (report) and cache.backfill (rebuild) answer.
        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::Doctor {
                fix: false,
                reply: tx,
            },
        );
        assert!(rx.recv_timeout(TEN_SECS).unwrap().is_ok());
        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::CacheBackfill {
                opts: sheaf_core::store::GrepBackfillOptions {
                    rebuild: true,
                    limit: Some(16),
                    max_elapsed_ms: Some(1_000),
                    ..Default::default()
                },
                reply: tx,
            },
        );
        assert!(rx.recv_timeout(TEN_SECS).unwrap().is_ok());

        // timeline.grep streams finalized records, then a Done outcome.
        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::Grep {
                request: Box::new(point_grep("seed")),
                reply: tx,
            },
        );
        let mut records = Vec::new();
        let done = loop {
            match rx.recv_timeout(TEN_SECS).unwrap() {
                GrepStreamItem::Record(record) => records.push(record),
                GrepStreamItem::Done(done) => break done,
            }
        };
        assert!(
            done.is_ok(),
            "point grep over the seeded store must succeed"
        );
        assert!(
            !records.is_empty(),
            "the literal `seed` occurs in tracked content"
        );

        // A zero budget can not make progress: the walk ends in an error
        // record instead of looping forever.
        let mut zero_budget = point_grep("seed");
        zero_budget.budget.max_results = 0;
        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::Grep {
                request: Box::new(zero_budget),
                reply: tx,
            },
        );
        loop {
            match rx.recv_timeout(TEN_SECS).unwrap() {
                GrepStreamItem::Record(_) => {}
                GrepStreamItem::Done(done) => {
                    assert!(done.is_err(), "a zero budget must fail, not stream");
                    break;
                }
            }
        }

        // Plan/apply token discipline without a real plan.
        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::PlanRestore {
                reference: "bogus-ref".into(),
                scope: vec![],
                reply: tx,
            },
        );
        assert!(rx.recv_timeout(TEN_SECS).unwrap().is_err());
        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::ApplyRestore {
                token: "unknown-token".into(),
                reply: tx,
            },
        );
        let err = rx.recv_timeout(TEN_SECS).unwrap().unwrap_err();
        assert!(err.to_string().contains("unknown or expired plan token"));

        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::ApplyFragment {
                token: "unknown-token".into(),
                reply: tx,
            },
        );
        assert!(rx
            .recv_timeout(TEN_SECS)
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("unknown or expired fragment plan token"));

        // Operator verbs with no pending intent: resume refuses explicitly,
        // abandon reconciles to Ok(None).
        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::ResumeRestore { reply: tx },
        );
        assert!(rx
            .recv_timeout(TEN_SECS)
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("no pending restore intent"));
        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::AbandonRestore { reply: tx },
        );
        assert_eq!(rx.recv_timeout(TEN_SECS).unwrap().unwrap(), None);

        // A lost debounce tail also poisons resume, like any mutation.
        let (tx, rx) = channel();
        handle_store_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            &mut Vec::new(),
            StoreCommand::ResumeRestore { reply: tx },
            Some(sheaf_core::SheafError::Config("tail lost".into())),
            60_000,
        );
        assert!(rx.recv_timeout(TEN_SECS).unwrap().is_err());

        // smart.plan phase one names destination paths (empty for no
        // selections); a phase-two query over an unresolvable handle still
        // answers — conflicts ride inside the plan instead of failing the
        // request.
        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::PlanSmart {
                selections: vec![],
                head_texts: None,
                reply: tx,
            },
        );
        match rx.recv_timeout(TEN_SECS).unwrap().unwrap() {
            SmartPlanReply::Paths(paths) => assert!(paths.is_empty()),
            SmartPlanReply::Plan(_) => panic!("phase one must not answer with a plan"),
        }
        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::PlanSmart {
                selections: vec![bogus_selection_handle()],
                head_texts: Some(Default::default()),
                reply: tx,
            },
        );
        match rx.recv_timeout(TEN_SECS).unwrap().unwrap() {
            SmartPlanReply::Paths(_) => panic!("phase two must not answer with paths"),
            SmartPlanReply::Plan(_) => {}
        }

        // fragment.plan over an unresolvable handle answers with a
        // conflict-bearing plan (or a hard error); it never fabricates an
        // applicable plan.
        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::PlanFragment {
                selections: vec![bogus_selection_handle()],
                mode: sheaf_core::store::FragmentMode::Replace,
                reply: tx,
            },
        );
        if let Ok(plan) = rx.recv_timeout(TEN_SECS).unwrap() {
            assert!(
                !plan.applicable() || !plan.conflicts.is_empty(),
                "an unresolvable selection must not plan as applicable"
            );
        }
    }

    fn bogus_selection_handle() -> sheaf_core::store::SelectionHandle {
        sheaf_core::store::SelectionHandle {
            version: 1,
            source_frontier: "0000000000000000000000000000000000000000000000000000000000000000"
                .into(),
            source_capture_id: None,
            historical_path: "never-tracked.txt".into(),
            extent: sheaf_core::store::SelectionExtent::Match,
            range: sheaf_core::store::ByteRange { start: 0, end: 4 },
            selected_text_sha256: "0".repeat(64),
            before_context_sha256: "0".repeat(64),
            after_context_sha256: "0".repeat(64),
            query_fingerprint: "fingerprint".into(),
            semantic: None,
        }
    }

    #[test]
    fn plan_then_apply_restore_clears_plan_caches_and_mutes_its_own_writes() {
        let tmp = tempfile::tempdir().unwrap();
        let root = skeleton_project(tmp.path(), "restore-cycle");
        std::fs::write(root.join("seed.txt"), "v1\n").unwrap();
        let (mut store, ignore, _lock) = opened_store(&root);
        store.reconcile_worktree(&ignore).unwrap();
        std::fs::write(root.join("seed.txt"), "v2\n").unwrap();
        std::fs::write(root.join("extra.txt"), "added later\n").unwrap();
        store.reconcile_worktree(&ignore).unwrap();
        let mut plans = Vec::new();
        let mut frags = Vec::new();

        // Plan: the cache keeps exactly one entry for the token.
        let (tx, rx) = channel();
        run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::PlanRestore {
                reference: "@~1".into(),
                scope: vec![],
                reply: tx,
            },
        );
        let plan = rx.recv_timeout(TEN_SECS).unwrap().unwrap();
        assert_eq!(plans.len(), 1, "the handed-out plan is cached by token");
        let summary = plan_summary(&plan);
        assert_eq!(summary["token"], plan.token.as_str());
        assert!(
            !plan.is_noop(),
            "restoring one capture back rewrites seed.txt"
        );

        // Apply by token: the answer comes back and both plan caches reset —
        // any surviving plan describes a worktree that no longer exists.
        let outcome = run_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            StoreCommand::ApplyRestore {
                token: plan.token.clone(),
                reply: channel().0,
            },
        )
        .expect("apply of a fresh plan succeeds and returns the outcome");
        assert!(plans.is_empty() && frags.is_empty());
        assert_eq!(
            std::fs::read(root.join("seed.txt")).unwrap(),
            b"v1\n",
            "the worktree actually moved back"
        );
        assert!(
            !root.join("extra.txt").exists(),
            "the later file is deleted"
        );

        // The writer's own echo is swallowed only while the bytes on disk
        // still say exactly what the restore put there.
        let mute = RestoreMute::new(&root, &outcome);
        let written = sheaf_core::events::FsEvent::now(sheaf_core::events::EventKind::Touched {
            path: sheaf_core::events::TouchedPath::from(root.join("seed.txt")),
        });
        assert!(
            mute.swallows(&written, &store),
            "identical bytes are the restore's echo"
        );
        std::fs::write(root.join("seed.txt"), "user edit\n").unwrap();
        assert!(
            !mute.swallows(&written, &store),
            "a user typing into a restored file is never silenced"
        );
        let deleted = sheaf_core::events::FsEvent::now(sheaf_core::events::EventKind::Removed {
            path: root.join("extra.txt"),
        });
        assert!(
            mute.swallows(&deleted, &store),
            "the missing file is the restore's own delete"
        );
        std::fs::write(root.join("extra.txt"), "recreated\n").unwrap();
        assert!(
            !mute.swallows(&deleted, &store),
            "a deleted path that exists again is new user work"
        );
        let expired = RestoreMute {
            until: Instant::now() - Duration::from_secs(1),
            written: outcome.written_paths.iter().map(|p| root.join(p)).collect(),
            deleted: outcome.deleted_paths.iter().map(|p| root.join(p)).collect(),
        };
        std::fs::write(root.join("seed.txt"), "v1\n").unwrap();
        assert!(
            !expired.swallows(&written, &store),
            "past the mute window every event is live user work"
        );
    }

    #[test]
    fn collect_loop_answers_commands_then_final_drains_and_exits() {
        let tmp = tempfile::tempdir().unwrap();
        let root = skeleton_project(tmp.path(), "collect-loop");
        std::fs::write(root.join("seed.txt"), "seed content\n").unwrap();
        let (mut store, ignore, lock) = opened_store(&root);
        store.reconcile_worktree(&ignore).unwrap();

        let (event_tx, event_rx) = channel::<sheaf_core::events::FsEvent>();
        let (control_tx, control_rx) = channel::<StoreCommand>();
        let loop_root = root.clone();
        let collector = std::thread::spawn(move || {
            collect_loop(
                loop_root,
                event_rx,
                control_rx,
                DebouncerConfig {
                    window: Duration::from_millis(40),
                    max_hold: Duration::from_millis(80),
                    cap_events: 100,
                },
                store,
                watcher::shared_classifier(
                    sheaf_core::classify::Classifier::from_volatile_patterns(&[]).unwrap(),
                ),
                sheaf_core::config::ScratchConfig::default(),
                None,
                lock,
                60_000,
            )
        });

        // A command is answered from the loop's drain branch.
        let (tx, rx) = channel();
        control_tx
            .send(StoreCommand::TimelineLog {
                all: false,
                branch: None,
                path: None,
                follow: false,
                limit: 10,
                reply: tx,
            })
            .unwrap();
        let (entries, _) = rx.recv_timeout(TEN_SECS).unwrap().unwrap();
        assert!(!entries.is_empty());

        // A watcher event flows through the debouncer into a persisted batch.
        event_tx
            .send(sheaf_core::events::FsEvent::now(
                sheaf_core::events::EventKind::Touched {
                    path: sheaf_core::events::TouchedPath::from(root.join("seed.txt")),
                },
            ))
            .unwrap();
        std::thread::sleep(Duration::from_millis(300));

        // Dropping both senders disconnects the loop; the final drain runs
        // and the thread exits instead of leaking.
        drop(control_tx);
        drop(event_tx);
        collector.join().unwrap();
    }

    /// The classification contract end to end: durable events reach the
    /// timeline, volatile events reach the scratch ring and ONLY the ring,
    /// and a volatile disappearance leaves a `gone` marker instead of a
    /// timeline removal. This is the daemon-level guarantee the watcher
    /// tests defer to ("litter flows; routing is the daemon's job").
    #[test]
    fn volatile_events_feed_the_ring_and_never_the_timeline() {
        let tmp = tempfile::tempdir().unwrap();
        let root = skeleton_project(tmp.path(), "scratch-routing");
        std::fs::write(root.join("real.txt"), "seed\n").unwrap();
        std::fs::write(root.join("notes.md.swp"), "unsaved buffer\n").unwrap();
        let (mut store, _baseline, lock) = opened_store(&root);
        let classifier = sheaf_core::classify::Classifier::from_volatile_patterns(
            &sheaf_core::config::default_volatile_patterns(),
        )
        .unwrap();
        store.reconcile_worktree(&classifier).unwrap();

        let (event_tx, event_rx) = channel::<sheaf_core::events::FsEvent>();
        let (control_tx, control_rx) = channel::<StoreCommand>();
        let loop_root = root.clone();
        let shared = watcher::shared_classifier(classifier);
        let collector = std::thread::spawn(move || {
            collect_loop(
                loop_root,
                event_rx,
                control_rx,
                DebouncerConfig {
                    window: Duration::from_millis(40),
                    max_hold: Duration::from_millis(80),
                    cap_events: 100,
                },
                store,
                shared,
                sheaf_core::config::ScratchConfig {
                    enabled: true,
                    max_bytes: 1 << 20,
                    max_file_bytes: 1 << 20,
                    flush_ms: 50,
                },
                None,
                lock,
                60_000,
            )
        });

        // One burst: a durable edit plus editor litter.
        event_tx
            .send(sheaf_core::events::FsEvent::now(
                sheaf_core::events::EventKind::Touched {
                    path: sheaf_core::events::TouchedPath::from(root.join("real.txt")),
                },
            ))
            .unwrap();
        event_tx
            .send(sheaf_core::events::FsEvent::now(
                sheaf_core::events::EventKind::Added {
                    path: root.join("notes.md.swp"),
                },
            ))
            .unwrap();
        event_tx
            .send(sheaf_core::events::FsEvent::now(
                sheaf_core::events::EventKind::Touched {
                    path: sheaf_core::events::TouchedPath::from(root.join("notes.md.swp")),
                },
            ))
            .unwrap();

        // Let the burst route and the quiescence window (40 ms) close
        // before commanding: control commands are drained ahead of events,
        // so an immediate checkpoint could flush an empty window.
        std::thread::sleep(Duration::from_millis(250));

        // A checkpoint crosses the debounce boundary: the durable batch (and
        // the piggybacked ring flush) complete before its reply.
        let (tx, rx) = channel();
        control_tx
            .send(StoreCommand::CreateCheckpoint {
                name: "after-burst".into(),
                reference: None,
                reply: tx,
            })
            .unwrap();
        rx.recv_timeout(TEN_SECS).unwrap().unwrap();

        // Timeline: the durable edit landed; the swap never appears.
        let (tx, rx) = channel();
        control_tx
            .send(StoreCommand::TimelineLog {
                all: false,
                branch: None,
                path: None,
                follow: false,
                limit: 50,
                reply: tx,
            })
            .unwrap();
        let (entries, _) = rx.recv_timeout(TEN_SECS).unwrap().unwrap();
        assert!(
            entries
                .iter()
                .any(|c| c.paths.iter().any(|p| p == "real.txt")),
            "the durable edit must be captured"
        );
        assert!(
            entries
                .iter()
                .all(|c| !c.paths.iter().any(|p| p.contains("notes.md.swp"))),
            "volatile litter must never reach the timeline: {:?}",
            entries.iter().map(|c| c.paths.clone()).collect::<Vec<_>>()
        );

        // Ring: the swap's snapshot round-trips.
        let ring = sheaf_dir(&root).join("scratch");
        let latest = sheaf_core::scratch::latest_snapshot(&ring, &root, "notes.md.swp")
            .expect("the ring holds the swap's snapshot");
        std::thread::sleep(Duration::from_millis(150));

        // A volatile disappearance leaves a gone marker, never a removal.
        event_tx
            .send(sheaf_core::events::FsEvent::now(
                sheaf_core::events::EventKind::Removed {
                    path: root.join("notes.md.swp"),
                },
            ))
            .unwrap();
        let (tx, rx) = channel();
        control_tx
            .send(StoreCommand::CreateCheckpoint {
                name: "after-gone".into(),
                reference: None,
                reply: tx,
            })
            .unwrap();
        rx.recv_timeout(TEN_SECS).unwrap().unwrap();
        let history = sheaf_core::scratch::history(&ring, &root, "notes.md.swp");
        assert!(
            history.iter().any(|r| r.gone),
            "disappearance must be recorded as a gone marker: {history:?}"
        );

        drop(control_tx);
        drop(event_tx);
        collector.join().unwrap();
    }

    // ------------------------------------------- worktree + merge writers

    /// Capture one file event into the active worktree and return the
    /// resulting capture id. Mirrors the core merge tests' `capture_file`.
    fn capture(store: &mut ProjectStore, root: &Path, path: &str, added: bool) -> String {
        let now = chrono::Utc::now();
        let kind = if added {
            sheaf_core::events::EventKind::Added {
                path: root.join(path),
            }
        } else {
            sheaf_core::events::EventKind::Touched {
                path: sheaf_core::events::TouchedPath::from(root.join(path)),
            }
        };
        store
            .apply_batch(&sheaf_core::events::Batch {
                root: root.to_path_buf(),
                started_at: now,
                flushed_at: now,
                events: vec![sheaf_core::events::FsEvent::now(kind)],
            })
            .unwrap()
            .capture
            .unwrap()
            .id
    }

    /// A locked store with a primary worktree and one linked worktree that
    /// carries a source-only capture on its own branch. Returned activated
    /// at the primary, ready to plan a merge of `source` back onto it.
    fn branched_store(
        base: &Path,
    ) -> (
        ProjectStore,
        sheaf_core::classify::Classifier,
        std::fs::File,
        PathBuf,
        PathBuf,
        String,
    ) {
        let primary = skeleton_project(base, "primary");
        std::fs::write(primary.join("base.txt"), "base\n").unwrap();
        let (mut store, ignore, lock) = opened_store(&primary);
        capture(&mut store, &primary, "base.txt", true);
        store.create_checkpoint("base", None).unwrap();
        let linked = base.join("linked");
        store.add_worktree("base", &linked).unwrap();

        std::fs::write(primary.join("target.txt"), "target\n").unwrap();
        capture(&mut store, &primary, "target.txt", true);

        store.activate_worktree(&linked).unwrap();
        std::fs::write(linked.join("source.txt"), "source\n").unwrap();
        let source = capture(&mut store, &linked, "source.txt", true);
        store.activate_worktree(&primary).unwrap();
        (store, ignore, lock, primary, linked, source)
    }

    #[test]
    fn worktree_and_merge_verbs_answer_and_surface_typed_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut store, ignore, _lock, primary, linked, source) = branched_store(tmp.path());
        let mut plans = Vec::new();
        let mut frags = Vec::new();
        let mut merges = Vec::new();

        // worktree.list names the primary and the one present linked worktree.
        let (tx, rx) = channel();
        handle_store_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            &mut merges,
            StoreCommand::ListWorktrees { reply: tx },
            None,
            60_000,
        );
        let worktrees = rx.recv_timeout(TEN_SECS).unwrap().unwrap();
        assert_eq!(worktrees.len(), 2, "primary plus one linked worktree");
        assert!(worktrees.iter().any(|w| w.primary));
        assert!(worktrees.iter().any(|w| !w.primary && w.present));

        // worktree.add onto an existing directory is refused by the store.
        let (tx, rx) = channel();
        handle_store_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            &mut merges,
            StoreCommand::AddWorktree {
                reference: "base".into(),
                destination: linked.clone(),
                reply: tx,
            },
            None,
            60_000,
        );
        assert!(rx
            .recv_timeout(TEN_SECS)
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("already exists"));

        // A lost debounce tail poisons the mutation before the store is touched.
        let (tx, rx) = channel();
        handle_store_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            &mut merges,
            StoreCommand::AddWorktree {
                reference: "base".into(),
                destination: tmp.path().join("linked2"),
                reply: tx,
            },
            Some(sheaf_core::SheafError::Config("tail lost".into())),
            60_000,
        );
        assert!(rx.recv_timeout(TEN_SECS).unwrap().is_err());
        assert!(
            !tmp.path().join("linked2").exists(),
            "poisoned add wrote nothing"
        );

        // merge.plan caches exactly one entry keyed by token; the source-only
        // change is the single action and there are no conflicts.
        let (tx, rx) = channel();
        handle_store_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            &mut merges,
            StoreCommand::PlanMerge {
                source: source.clone(),
                reply: tx,
            },
            None,
            60_000,
        );
        let plan = rx.recv_timeout(TEN_SECS).unwrap().unwrap();
        assert_eq!(
            merges.len(),
            1,
            "the handed-out merge plan is cached by token"
        );
        assert!(plan.conflicts.is_empty());
        assert_eq!(
            plan.actions
                .iter()
                .map(|action| action.path.as_str())
                .collect::<Vec<_>>(),
            vec!["source.txt"]
        );
        let token = plan.token.clone();

        // merge.apply with an unknown token is a stale-plan error.
        let (tx, rx) = channel();
        handle_store_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            &mut merges,
            StoreCommand::ApplyMerge {
                token: "no-such-token".into(),
                reply: tx,
            },
            None,
            60_000,
        );
        assert!(rx
            .recv_timeout(TEN_SECS)
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("unknown or expired merge plan token"));

        // A lost tail also poisons apply, leaving the cached plan intact.
        let (tx, rx) = channel();
        handle_store_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            &mut merges,
            StoreCommand::ApplyMerge {
                token: token.clone(),
                reply: tx,
            },
            Some(sheaf_core::SheafError::Config("tail lost".into())),
            60_000,
        );
        assert!(rx.recv_timeout(TEN_SECS).unwrap().is_err());
        assert_eq!(merges.len(), 1, "a poisoned apply keeps the plan cached");

        // merge.apply by token squashes the source change onto the primary as
        // one capture and clears every plan cache.
        let (tx, rx) = channel();
        handle_store_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            &mut merges,
            StoreCommand::ApplyMerge {
                token: token.clone(),
                reply: tx,
            },
            None,
            60_000,
        );
        let outcome = rx.recv_timeout(TEN_SECS).unwrap().unwrap();
        assert_eq!(outcome.files_written, 1);
        assert!(merges.is_empty() && plans.is_empty() && frags.is_empty());
        assert_eq!(
            std::fs::read_to_string(primary.join("source.txt")).unwrap(),
            "source\n",
            "the merge actually wrote the source file"
        );

        // merge.resume with no pending intent refuses by name, and a lost
        // tail poisons it like any mutation.
        let (tx, rx) = channel();
        handle_store_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            &mut merges,
            StoreCommand::ResumeMerge { reply: tx },
            None,
            60_000,
        );
        assert!(rx
            .recv_timeout(TEN_SECS)
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("no pending merge intent"));
        let (tx, rx) = channel();
        handle_store_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            &mut merges,
            StoreCommand::ResumeMerge { reply: tx },
            Some(sheaf_core::SheafError::Config("tail lost".into())),
            60_000,
        );
        assert!(rx.recv_timeout(TEN_SECS).unwrap().is_err());
    }

    #[test]
    fn merge_apply_reports_path_conflicts_without_writing() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut store, ignore, _lock, primary, linked, _source) = branched_store(tmp.path());
        // Diverge one path on both branches: the target edits base.txt and
        // the source edits it differently.
        std::fs::write(primary.join("base.txt"), "target edit\n").unwrap();
        capture(&mut store, &primary, "base.txt", false);
        store.activate_worktree(&linked).unwrap();
        std::fs::write(linked.join("base.txt"), "source edit\n").unwrap();
        let source = capture(&mut store, &linked, "base.txt", false);
        store.activate_worktree(&primary).unwrap();

        let mut plans = Vec::new();
        let mut frags = Vec::new();
        let mut merges = Vec::new();
        let (tx, rx) = channel();
        handle_store_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            &mut merges,
            StoreCommand::PlanMerge { source, reply: tx },
            None,
            60_000,
        );
        let plan = rx.recv_timeout(TEN_SECS).unwrap().unwrap();
        assert_eq!(plan.conflicts.len(), 1);
        assert_eq!(plan.conflicts[0].path, "base.txt");

        let (tx, rx) = channel();
        handle_store_command(
            &mut store,
            &ignore,
            &mut plans,
            &mut frags,
            &mut merges,
            StoreCommand::ApplyMerge {
                token: plan.token.clone(),
                reply: tx,
            },
            None,
            60_000,
        );
        assert!(rx
            .recv_timeout(TEN_SECS)
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("conflict"));
        assert_eq!(
            std::fs::read_to_string(primary.join("base.txt")).unwrap(),
            "target edit\n",
            "a conflicting merge leaves the target untouched"
        );
    }

    #[test]
    fn inworktree_wrapper_delegates_classification_and_error_routing() {
        let root = PathBuf::from("/tmp/sheafd-inworktree");
        // worktree_root answers only for the wrapper; the delegated
        // classification is exactly the inner command's.
        let (tx, _rx) = channel::<
            std::result::Result<Vec<sheaf_core::store::WorktreeInfo>, sheaf_core::SheafError>,
        >();
        let wrapped = StoreCommand::InWorktree {
            root: root.clone(),
            command: Box::new(StoreCommand::ListWorktrees { reply: tx }),
        };
        assert_eq!(wrapped.worktree_root(), Some(root.as_path()));
        assert!(!wrapped.crosses_debounce_boundary());
        assert!(!wrapped.is_memory_heavy());

        // A bare command has no worktree root.
        let (tx, _rx) = channel::<
            std::result::Result<sheaf_core::store::MergeOutcome, sheaf_core::SheafError>,
        >();
        assert_eq!(
            StoreCommand::ResumeMerge { reply: tx }.worktree_root(),
            None
        );

        // Classification of the new worktree/merge variants.
        let (tx, _rx) = channel::<
            std::result::Result<sheaf_core::store::WorktreeInfo, sheaf_core::SheafError>,
        >();
        let add = StoreCommand::AddWorktree {
            reference: "base".into(),
            destination: root.clone(),
            reply: tx,
        };
        assert!(add.crosses_debounce_boundary() && add.is_memory_heavy());
        let (tx, _rx) =
            channel::<std::result::Result<sheaf_core::store::MergePlan, sheaf_core::SheafError>>();
        let plan = StoreCommand::PlanMerge {
            source: "@".into(),
            reply: tx,
        };
        assert!(!plan.crosses_debounce_boundary() && plan.is_memory_heavy());
        let (tx, _rx) = channel::<
            std::result::Result<sheaf_core::store::MergeOutcome, sheaf_core::SheafError>,
        >();
        let apply = StoreCommand::ApplyMerge {
            token: "t".into(),
            reply: tx,
        };
        assert!(apply.crosses_debounce_boundary() && !apply.is_memory_heavy());

        // send_error routes through the wrapper to the inner reply, and each
        // new variant's own arm delivers a typed error to its receiver.
        let (tx, rx) = channel();
        StoreCommand::InWorktree {
            root,
            command: Box::new(StoreCommand::ApplyMerge {
                token: "t".into(),
                reply: tx,
            }),
        }
        .send_error(sheaf_core::SheafError::StoreCorrupt("boom".into()));
        assert!(
            rx.recv().unwrap().is_err(),
            "wrapper routes to the inner reply"
        );

        let (tx, rx) = channel();
        StoreCommand::ListWorktrees { reply: tx }
            .send_error(sheaf_core::SheafError::StoreCorrupt("boom".into()));
        assert!(rx.recv().unwrap().is_err());
        let (tx, rx) = channel();
        StoreCommand::AddWorktree {
            reference: "base".into(),
            destination: PathBuf::from("/tmp/x"),
            reply: tx,
        }
        .send_error(sheaf_core::SheafError::StoreCorrupt("boom".into()));
        assert!(rx.recv().unwrap().is_err());
        let (tx, rx) = channel();
        StoreCommand::PlanMerge {
            source: "@".into(),
            reply: tx,
        }
        .send_error(sheaf_core::SheafError::StoreCorrupt("boom".into()));
        assert!(rx.recv().unwrap().is_err());
        let (tx, rx) = channel();
        StoreCommand::ResumeMerge { reply: tx }
            .send_error(sheaf_core::SheafError::StoreCorrupt("boom".into()));
        assert!(rx.recv().unwrap().is_err());
    }

    #[test]
    fn boot_reconcile_resumes_an_interrupted_merge_and_reconciles_every_worktree() {
        let tmp = tempfile::tempdir().unwrap();
        let (store, _ignore, lock, primary, linked, source) = branched_store(tmp.path());

        // Park a merge intent exactly as a crash mid-apply would leave it:
        // planned and written, but never finished.
        let plan = store.plan_merge(&source).unwrap();
        let intent = sheaf_core::store::MergeIntent {
            plan,
            worktree: store.root().to_path_buf(),
            started_ms: chrono::Utc::now().timestamp_millis(),
        };
        let intent_path = sheaf_core::config::sheaf_dir(&primary)
            .join("state")
            .join("merge.intent");
        std::fs::write(&intent_path, serde_json::to_string(&intent).unwrap()).unwrap();
        assert!(sheaf_core::store::pending_merge_at(&primary).is_some());

        // Uncaptured edits on both physical worktrees; boot reconciliation
        // must fold them into history.
        std::fs::write(primary.join("late.txt"), "late primary\n").unwrap();
        std::fs::write(linked.join("late-linked.txt"), "late linked\n").unwrap();

        // Reopen exactly as a fresh daemon boot would.
        drop(store);
        drop(lock);
        let (mut store, ignore, _lock) = opened_store(&primary);
        boot_reconcile_store(&primary, &mut store, &ignore, 60_000);

        // The interrupted merge finished: source.txt landed and the intent
        // was cleared.
        assert_eq!(
            std::fs::read_to_string(primary.join("source.txt")).unwrap(),
            "source\n"
        );
        assert!(sheaf_core::store::pending_merge_at(&primary).is_none());

        // Both worktrees are already reconciled: a second pass finds nothing.
        store.activate_worktree(&primary).unwrap();
        assert!(
            store.reconcile_worktree(&ignore).unwrap().is_none(),
            "the primary's late edit was captured at boot"
        );
        store.activate_worktree(&linked).unwrap();
        assert!(
            store.reconcile_worktree(&ignore).unwrap().is_none(),
            "the linked worktree's late edit was captured at boot"
        );
    }

    #[test]
    fn boot_reconcile_resumes_a_pending_restore_in_a_linked_worktree() {
        let tmp = tempfile::tempdir().unwrap();
        let (mut store, ignore, lock, primary, linked, _source) = branched_store(tmp.path());

        // Plan a restore of the linked branch back one capture (dropping
        // source.txt) and park its intent, byte-for-byte what a crashed
        // restore leaves behind.
        store.activate_worktree(&linked).unwrap();
        let plan = store.plan_restore("@~1", &[], &ignore).unwrap();
        assert!(
            !plan.is_noop(),
            "restoring one capture back drops source.txt"
        );
        let plan_json = serde_json::to_value(&plan).unwrap();
        let id = sheaf_core::config::worktree_id(&linked).expect("linked worktree has an id");
        let intent_path = sheaf_core::config::worktree_head_path(&linked)
            .parent()
            .unwrap()
            .join(format!("{id}.restore.intent"));
        std::fs::write(
            &intent_path,
            serde_json::json!({
                "token": plan_json["token"],
                "mode": "full",
                "scope": [],
                "target": plan_json["target"],
                "started_ms": chrono::Utc::now().timestamp_millis(),
            })
            .to_string(),
        )
        .unwrap();
        assert!(sheaf_core::store::pending_restore_at(&linked).is_some());
        store.activate_worktree(&primary).unwrap();

        // Reopen and boot: the primary has no intent, but the linked
        // worktree's restore is resumed independently.
        drop(store);
        drop(lock);
        let (mut store, ignore, _lock) = opened_store(&primary);
        boot_reconcile_store(&primary, &mut store, &ignore, 60_000);

        assert!(
            !linked.join("source.txt").exists(),
            "the resumed restore removed the source-only file"
        );
        assert!(
            sheaf_core::store::pending_restore_at(&linked).is_none(),
            "a completed restore clears its intent"
        );
        // The primary is untouched by the linked worktree's restore.
        assert!(primary.join("target.txt").exists());
    }

    #[test]
    fn dropping_the_watch_entry_lets_its_only_collector_exit_promptly() {
        // graceful_shutdown's phase 2 takes the collector handle, then drops
        // the WatchEntry so its stored `events` sender — the last one alive
        // once the watcher threads have joined — hangs up. Without that drop
        // the collector blocks the full tail-flush grace on every shutdown.
        let tmp = tempfile::tempdir().unwrap();
        let root = skeleton_project(tmp.path(), "shutdown-exit");
        std::fs::write(root.join("seed.txt"), "seed\n").unwrap();
        let (mut store, ignore, lock) = opened_store(&root);
        store.reconcile_worktree(&ignore).unwrap();

        let (events, event_rx) = channel::<sheaf_core::events::FsEvent>();
        let (control, control_rx) = channel::<StoreCommand>();
        let (wake, _wake_rx) = channel::<()>();
        let loop_root = root.clone();
        let collector = std::thread::spawn(move || {
            collect_loop(
                loop_root,
                event_rx,
                control_rx,
                DebouncerConfig {
                    window: Duration::from_millis(40),
                    max_hold: Duration::from_millis(80),
                    cap_events: 100,
                },
                store,
                watcher::shared_classifier(
                    sheaf_core::classify::Classifier::from_volatile_patterns(&[]).unwrap(),
                ),
                sheaf_core::config::ScratchConfig::default(),
                None,
                lock,
                60_000,
            )
        });

        let mut entry = WatchEntry {
            stop: watcher::new_stop_flag(),
            cold: Arc::new(AtomicBool::new(false)),
            ready: Arc::new(AtomicBool::new(true)),
            watch_handles: Vec::new(),
            collector: Some(collector),
            control,
            events,
            wake,
        };

        // Take the collector, then drop the entry: dropping frees the last
        // event/control senders so the collector disconnects, drains its
        // tail, and exits well within the grace.
        let collector = entry.collector.take().unwrap();
        drop(entry);
        let start = Instant::now();
        wait_bounded(collector, TAIL_FLUSH_GRACE, "shutdown-exit");
        assert!(
            start.elapsed() < TAIL_FLUSH_GRACE,
            "the collector must exit once its last sender drops, not wait out the grace"
        );
    }

    // -------------------------------------------------------- IPC dispatch

    fn test_request(method: &str, project: Option<PathBuf>, params: serde_json::Value) -> Request {
        Request {
            v: PROTO_MAJOR,
            id: "rid-1".into(),
            method: method.into(),
            project,
            params,
        }
    }

    fn error_code_of(resp: &Response) -> String {
        resp.error
            .as_ref()
            .expect("response must be an error")
            .code
            .clone()
    }

    #[test]
    fn dispatch_rejects_version_mismatch_unknown_methods_and_unwatched_projects() {
        let shared = test_shared();
        let mut shutting_down = false;

        let mut stale = test_request("ping", None, json!({}));
        stale.v = PROTO_MAJOR + 7;
        let (resp, body) = dispatch(&shared, &stale, &mut shutting_down);
        assert_eq!(error_code_of(&resp), "store.version_mismatch");
        assert!(matches!(body, IpcBody::Bytes(bytes) if bytes.is_empty()));

        let (resp, _) = dispatch(
            &shared,
            &test_request("ping", None, json!({})),
            &mut shutting_down,
        );
        assert!(resp.ok);
        let result = resp.result.unwrap();
        assert_eq!(result["proto"]["major"], PROTO_MAJOR);
        assert!(result["capabilities"].as_array().unwrap().len() > 10);

        let (resp, _) = dispatch(
            &shared,
            &test_request("no.such.method", None, json!({})),
            &mut shutting_down,
        );
        assert_eq!(error_code_of(&resp), "bad.method");

        let (resp, _) = dispatch(
            &shared,
            &test_request("timeline.log", None, json!({})),
            &mut shutting_down,
        );
        assert_eq!(error_code_of(&resp), "bad.params");

        let unwatched = tempfile::tempdir().unwrap();
        let (resp, _) = dispatch(
            &shared,
            &test_request(
                "timeline.log",
                Some(unwatched.path().to_path_buf()),
                json!({}),
            ),
            &mut shutting_down,
        );
        assert_eq!(error_code_of(&resp), "project.not_enrolled");

        // project.status for a project the daemon has never enrolled.
        let (resp, _) = dispatch(
            &shared,
            &test_request(
                "project.status",
                Some(unwatched.path().to_path_buf()),
                json!({}),
            ),
            &mut shutting_down,
        );
        assert_eq!(error_code_of(&resp), "project.not_enrolled");

        // shutdown flips the connection-level flag so the server hangs up.
        let mut flag = false;
        let (resp, _) = dispatch(
            &shared,
            &test_request("shutdown", None, json!({})),
            &mut flag,
        );
        assert!(resp.ok);
        assert!(flag, "shutdown must tell the connection to stop");
    }

    #[test]
    fn dispatch_param_validation_fails_closed_for_every_verb() {
        let shared = test_shared();
        let mut shutting_down = false;
        let project = Some(PathBuf::from("/tmp/sheafd-param-proj"));
        let cases: Vec<(&str, serde_json::Value, &str)> = vec![
            ("restore.plan", json!({}), "bad.params"),
            ("restore.apply", json!({}), "bad.params"),
            ("fragment.plan", json!({}), "bad.params"),
            ("fragment.plan", json!({"selections": []}), "bad.params"),
            (
                "fragment.plan",
                json!({"selections": [{"version": 1}]}),
                "bad.params",
            ),
            (
                "fragment.plan",
                json!({"selections": [{"version": 1, "source_frontier": "f",
                    "historical_path": "p", "extent": "match",
                    "range": {"start": 0, "end": 1}, "selected_text_sha256": "x",
                    "before_context_sha256": "x", "after_context_sha256": "x",
                    "query_fingerprint": "f"}], "mode": "explode"}),
                "bad.params",
            ),
            ("smart.plan", json!({}), "bad.params"),
            ("diff", json!({}), "bad.params"),
            ("timeline.info", json!({}), "bad.params"),
            ("checkpoint.create", json!({}), "bad.params"),
            (
                "timeline.grep",
                serde_json::to_value(point_grep("")).unwrap(),
                "bad.params",
            ),
            (
                "cache.backfill",
                json!({"limit": "not-a-number"}),
                "bad.params",
            ),
        ];
        for (method, params, expected_code) in cases {
            let (resp, _) = dispatch(
                &shared,
                &test_request(method, project.clone(), params),
                &mut shutting_down,
            );
            assert!(!resp.ok, "{method} with invalid params must not succeed");
            assert_eq!(error_code_of(&resp), expected_code, "{method} error code");
        }
    }

    #[test]
    fn project_control_gates_unwatched_warming_and_ready_projects() {
        let shared = test_shared();
        let root = PathBuf::from("/tmp/sheafd-control-root");

        // Unwatched: refused outright.
        let err = project_control(&shared, &root, "rid").unwrap_err();
        assert_eq!(error_code_of(&err), "project.not_enrolled");

        // Watched, eager boot window (not cold, not ready): the warming
        // error describes the initial capture.
        let entry_for = |cold: bool, ready: bool| {
            let (control_tx, _control_rx) = channel::<StoreCommand>();
            let (wake_tx, _wake_rx) = channel::<()>();
            let (events, _events_rx) = channel();

            WatchEntry {
                stop: watcher::new_stop_flag(),
                cold: Arc::new(AtomicBool::new(cold)),
                ready: Arc::new(AtomicBool::new(ready)),
                watch_handles: Vec::new(),
                collector: None,
                control: control_tx,
                events,

                wake: wake_tx,
            }
        };
        shared
            .table
            .lock()
            .unwrap()
            .insert(normalize(&root), entry_for(false, false));
        let err = project_control(&shared, &root, "rid").unwrap_err();
        assert_eq!(error_code_of(&err), "project.warming");
        assert!(err
            .error
            .unwrap()
            .message
            .contains("initial worktree capture"));

        // Ready: the control channel comes back.
        shared
            .table
            .lock()
            .unwrap()
            .insert(normalize(&root), entry_for(false, true));
        assert!(project_control(&shared, &root, "rid").is_ok());

        // A lazy project that never warms: the bounded budget elapses and
        // the warming error names the lazy open, instead of queueing the
        // mutation behind a caller that already gave up.
        shared
            .table
            .lock()
            .unwrap()
            .insert(normalize(&root), entry_for(true, false));
        let err = project_control(&shared, &root, "rid").unwrap_err();
        assert_eq!(error_code_of(&err), "project.warming");
        assert!(err.error.unwrap().message.contains("lazy project"));
    }

    #[test]
    fn enroll_notify_rejects_invalid_roots_and_watches_valid_stores() {
        let shared = test_shared();
        let mut shutting_down = false;
        let tmp = tempfile::tempdir().unwrap();

        let (resp, _) = dispatch(
            &shared,
            &test_request("enroll.notify", Some(tmp.path().join("missing")), json!({})),
            &mut shutting_down,
        );
        assert_eq!(error_code_of(&resp), "project.not_enrolled");

        let bare = tmp.path().join("bare");
        std::fs::create_dir_all(&bare).unwrap();
        let (resp, _) = dispatch(
            &shared,
            &test_request("enroll.notify", Some(bare), json!({})),
            &mut shutting_down,
        );
        assert_eq!(error_code_of(&resp), "project.not_enrolled");

        let valid = skeleton_project(tmp.path(), "notify-valid");
        let (resp, _) = dispatch(
            &shared,
            &test_request("enroll.notify", Some(valid.clone()), json!({})),
            &mut shutting_down,
        );
        assert!(resp.ok);
        assert_eq!(resp.result.unwrap()["watching"], true);
        assert!(shared.watching(&valid), "the notified project is watched");
    }

    #[test]
    fn timeline_log_before_cursor_paginates_and_validates_prefixes() {
        let tmp = tempfile::tempdir().unwrap();
        let live = skeleton_project(tmp.path(), "cursor-log");
        std::fs::write(live.join("tracked.txt"), "content").unwrap();
        let shared = test_shared();
        assert!(spawn_watch(&shared, &live));
        let shutting_down = std::cell::RefCell::new(false);

        let log_req = |before: Option<&str>| {
            let mut params = json!({"limit": 50});
            if let Some(b) = before {
                params["before"] = json!(b);
            }
            test_request("timeline.log", Some(live.clone()), params)
        };

        // Warm the project through the IPC path itself.
        let (resp, _) = dispatch(&shared, &log_req(None), &mut shutting_down.borrow_mut());
        assert!(
            resp.ok,
            "timeline.log over a warming project must wait for it"
        );
        let entries = resp.result.unwrap()["entries"].as_array().unwrap().clone();
        assert!(!entries.is_empty());

        // A second capture so the cursor has something to drain past.
        std::thread::sleep(Duration::from_millis(300));
        std::fs::write(live.join("tracked.txt"), "content v2").unwrap();
        assert!(
            wait_until(15_000, || {
                let (resp, _) = dispatch(&shared, &log_req(None), &mut shutting_down.borrow_mut());
                resp.result.unwrap()["entries"].as_array().unwrap().len() >= 2
            }),
            "the second capture never landed"
        );
        std::thread::sleep(Duration::from_millis(500));
        let (resp, _) = dispatch(&shared, &log_req(None), &mut shutting_down.borrow_mut());
        let entries = resp.result.unwrap()["entries"].as_array().unwrap().clone();
        let total = entries.len();

        // A valid full cursor drains everything up to and including it.
        let cursor = entries[0]["id"].as_str().unwrap().to_owned();
        let (resp, _) = dispatch(
            &shared,
            &log_req(Some(&cursor)),
            &mut shutting_down.borrow_mut(),
        );
        assert!(resp.ok);
        let after = resp.result.unwrap()["entries"].as_array().unwrap().clone();
        assert_eq!(after.len(), total - 1, "the cursor entry itself is drained");
        assert!(after.iter().all(|e| e["id"] != json!(cursor)));

        // Prefixes shorter than six hex characters are refused.
        let (resp, _) = dispatch(
            &shared,
            &log_req(Some("abc")),
            &mut shutting_down.borrow_mut(),
        );
        assert_eq!(error_code_of(&resp), "state.bad_reference");

        // A well-formed but unknown cursor is a real error, not an empty page.
        let (resp, _) = dispatch(
            &shared,
            &log_req(Some("ffffff")),
            &mut shutting_down.borrow_mut(),
        );
        assert_eq!(error_code_of(&resp), "state.bad_reference");
        assert!(resp.error.unwrap().message.contains("unknown cursor"));
    }

    #[test]
    fn timeline_log_omit_paths_strips_per_capture_path_lists() {
        // The squash span walk sets `omit_paths` so a full page of captures
        // stays under the envelope cap even when bulk-change captures carry
        // long path lists. The daemon must clear `paths` while leaving the
        // rest of each entry intact.
        let tmp = tempfile::tempdir().unwrap();
        let live = skeleton_project(tmp.path(), "omit-paths-log");
        std::fs::write(live.join("tracked.txt"), "content").unwrap();
        let shared = test_shared();
        assert!(spawn_watch(&shared, &live));
        let mut shutting_down = false;

        let log_req = |omit: bool| {
            test_request(
                "timeline.log",
                Some(live.clone()),
                json!({"limit": 50, "omit_paths": omit}),
            )
        };

        // Default: paths are present (the capture recorded the tracked file).
        let (resp, _) = dispatch(&shared, &log_req(false), &mut shutting_down);
        assert!(resp.ok, "timeline.log over a warming project must wait");
        let entries = resp.result.unwrap()["entries"].as_array().unwrap().clone();
        assert!(!entries.is_empty());
        assert!(
            entries
                .iter()
                .any(|e| !e["paths"].as_array().unwrap().is_empty()),
            "the baseline reply carries real path lists"
        );

        // omit_paths: identity/time survive; paths are emptied.
        let (resp, _) = dispatch(&shared, &log_req(true), &mut shutting_down);
        assert!(resp.ok);
        let slim = resp.result.unwrap()["entries"].as_array().unwrap().clone();
        assert_eq!(slim.len(), entries.len());
        for (full, thin) in entries.iter().zip(&slim) {
            assert_eq!(thin["id"], full["id"]);
            assert_eq!(thin["timestamp_ms"], full["timestamp_ms"]);
            assert!(
                thin["paths"].as_array().unwrap().is_empty(),
                "omit_paths must clear every path list"
            );
        }
    }

    #[test]
    fn timeline_log_streams_details_and_reads_a_named_branch() {
        let tmp = tempfile::tempdir().unwrap();
        let live = skeleton_project(tmp.path(), "detailed-branch-log");
        std::fs::write(live.join("tracked.txt"), "content\n").unwrap();
        let shared = test_shared();
        assert!(spawn_watch(&shared, &live));
        let mut shutting_down = false;

        let (created, _) = dispatch(
            &shared,
            &test_request(
                "branch.create",
                Some(live.clone()),
                json!({"name": "feature", "at": "@"}),
            ),
            &mut shutting_down,
        );
        assert!(created.ok, "branch creation failed: {:?}", created.error);

        let (response, IpcBody::Bytes(body)) = dispatch(
            &shared,
            &test_request(
                "timeline.log",
                Some(live.clone()),
                json!({
                    "branch": "feature",
                    "details": true,
                    "patch": true,
                    "limit": 10,
                }),
            ),
            &mut shutting_down,
        ) else {
            panic!("timeline.log details must use a byte body");
        };
        assert!(response.ok, "branch log failed: {:?}", response.error);
        let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let details = payload["details"].as_array().unwrap();
        let patches = payload["patches"].as_array().unwrap();
        assert_eq!(details.len(), 1);
        assert_eq!(patches.len(), 1);
        assert_eq!(
            details[0]["capture"]["id"],
            response.result.unwrap()["entries"][0]["id"]
        );
        assert!(patches[0]
            .as_str()
            .unwrap()
            .contains("diff --sheaf a/tracked.txt b/tracked.txt"));

        let (missing, _) = dispatch(
            &shared,
            &test_request("timeline.log", Some(live), json!({"branch": "missing"})),
            &mut shutting_down,
        );
        assert!(!missing.ok);
        assert!(missing.error.unwrap().message.contains("missing"));
    }

    // --------------------------------------------------- connection layer

    #[test]
    fn serve_connection_roundtrips_framed_requests_and_survives_garbage_and_eof() {
        let shared = test_shared();
        let (mut client, server) = UnixStream::pair().unwrap();
        let actor = std::thread::spawn(move || serve_connection(shared, server));

        let roundtrip = |client: &mut UnixStream, req: &Request| -> Response {
            ipc::write_frame(client, &serde_json::to_vec(req).unwrap(), MAX_ENVELOPE).unwrap();
            let frame = ipc::read_frame(client, MAX_ENVELOPE).unwrap();
            serde_json::from_slice(&frame).unwrap()
        };

        let resp = roundtrip(
            &mut client,
            &Request {
                v: PROTO_MAJOR,
                id: "a".into(),
                method: "ping".into(),
                project: None,
                params: json!({}),
            },
        );
        assert!(resp.ok && resp.id == "a");

        // An unparseable envelope is answered, not dropped.
        ipc::write_frame(&mut client, b"this is not json", MAX_ENVELOPE).unwrap();
        let frame = ipc::read_frame(&mut client, MAX_ENVELOPE).unwrap();
        let resp: Response = serde_json::from_slice(&frame).unwrap();
        assert_eq!(resp.id, "?");
        assert_eq!(resp.error.unwrap().code, "bad.request");

        let stale = Request {
            v: PROTO_MAJOR + 3,
            id: "v".into(),
            method: "ping".into(),
            project: None,
            params: json!({}),
        };
        let resp = roundtrip(&mut client, &stale);
        assert_eq!(error_code_of(&resp), "store.version_mismatch");

        let resp = roundtrip(
            &mut client,
            &test_request("no.such.method", None, json!({})),
        );
        assert_eq!(error_code_of(&resp), "bad.method");

        let resp = roundtrip(
            &mut client,
            &test_request(
                "timeline.log",
                Some(PathBuf::from("/tmp/sheafd-unwatched")),
                json!({}),
            ),
        );
        assert_eq!(error_code_of(&resp), "project.not_enrolled");

        // EOF ends the connection loop cleanly.
        drop(client);
        actor.join().unwrap().expect("serve_connection ends at EOF");
    }

    #[test]
    fn write_response_chunks_large_bodies_and_downgrades_oversized_envelopes() {
        // A socketpair's kernel buffer is smaller than the payloads below,
        // so the writer runs on its own thread and this side drains frames
        // concurrently — exactly how the real connection loop pairs up.
        let body = vec![b'x'; ipc::MAX_CHUNK + 1];

        // A body over one chunk is announced in the envelope and streamed.
        let write_body = body.clone();
        let (mut server, mut client) = UnixStream::pair().unwrap();
        let writer = std::thread::spawn(move || {
            write_response(
                &mut server,
                &Response::ok("rid", json!({"n": 1})),
                &write_body,
            )
            .unwrap()
        });
        let frame = ipc::read_frame(&mut client, MAX_ENVELOPE).unwrap();
        let env: Response = serde_json::from_slice(&frame).unwrap();
        assert_eq!(env.body.unwrap().chunks, 2);
        let mut received = ipc::read_frame(&mut client, ipc::MAX_CHUNK).unwrap();
        received.extend(ipc::read_frame(&mut client, ipc::MAX_CHUNK).unwrap());
        assert_eq!(received, body);
        writer.join().unwrap();

        // An envelope past the cap loses its body but keeps a stated error.
        let (mut server, mut client) = UnixStream::pair().unwrap();
        let writer = std::thread::spawn(move || {
            write_response(
                &mut server,
                &Response::err("big", IpcError::new("x", "y".repeat(MAX_ENVELOPE))),
                &[],
            )
            .unwrap()
        });
        let frame = ipc::read_frame(&mut client, MAX_ENVELOPE).unwrap();
        let env: Response = serde_json::from_slice(&frame).unwrap();
        assert_eq!(env.id, "big");
        assert_eq!(env.error.unwrap().code, "result.too_large");
        writer.join().unwrap();
    }

    #[test]
    fn write_streamed_response_flushes_records_then_summary_error_and_stall_terminators() {
        // Drive a real store-backed grep so the forwarded records are the
        // genuine article, not hand-built fixtures.
        let tmp = tempfile::tempdir().unwrap();
        let root = skeleton_project(tmp.path(), "streamed");
        std::fs::write(root.join("seed.txt"), "seed content\n").unwrap();
        let (mut store, ignore, _lock) = opened_store(&root);
        store.reconcile_worktree(&ignore).unwrap();

        let (collect_tx, collect_rx) = channel::<GrepStreamItem>();
        {
            let reply = collect_tx;
            let mut sink_record = |record: sheaf_core::store::GrepStreamRecord| {
                let _ = reply.send(GrepStreamItem::Record(record));
            };
            let result = store.grep_streaming(&point_grep("seed"), &mut Some(&mut sink_record));
            let _ = reply.send(GrepStreamItem::Done(result));
        }

        // Records, then the summary, then the empty terminator. The
        // streamed-body sentinel in the envelope is set by the `grep`
        // dispatch handler; mirror it here exactly as the handler would.
        let (mut server, mut client) = UnixStream::pair().unwrap();
        let mut envelope = Response::ok("rid", json!({"streamed": true}));
        envelope.body = Some(ipc::BodyInfo {
            chunks: ipc::STREAMED_BODY_SENTINEL,
        });
        let writer =
            std::thread::spawn(move || write_streamed_response(&mut server, &envelope, collect_rx));
        let frame = ipc::read_frame(&mut client, MAX_ENVELOPE).unwrap();
        let env: Response = serde_json::from_slice(&frame).unwrap();
        assert!(env.ok);
        assert_eq!(env.body.unwrap().chunks, ipc::STREAMED_BODY_SENTINEL);
        let mut saw_record = false;
        loop {
            let frame = ipc::read_frame(&mut client, ipc::MAX_CHUNK).unwrap();
            if frame.is_empty() {
                break;
            }
            let line: serde_json::Value = serde_json::from_slice(&frame).unwrap();
            match line["type"].as_str() {
                Some("hit") => saw_record = true,
                Some("summary") => assert!(line["report"].is_object()),
                other => panic!("unexpected streamed record type {other:?}"),
            }
        }
        assert!(saw_record, "the seeded grep must stream at least one hit");
        writer.join().unwrap().unwrap();

        // A failed walk still terminates with a typed error record.
        let (tx, rx) = channel::<GrepStreamItem>();
        tx.send(GrepStreamItem::Done(Err(sheaf_core::SheafError::Config(
            "boom".into(),
        ))))
        .unwrap();
        drop(tx);
        let (mut server, mut client) = UnixStream::pair().unwrap();
        write_streamed_response(&mut server, &Response::ok("rid", json!({})), rx).unwrap();
        let mut last = None;
        loop {
            let frame = ipc::read_frame(&mut client, ipc::MAX_CHUNK).unwrap();
            if frame.is_empty() {
                break;
            }
            last = Some(frame);
        }
        let line: serde_json::Value =
            serde_json::from_slice(&last.expect("a terminal error record")).unwrap();
        assert_eq!(line["type"], "error");

        // A stalled collector still ends the body so a client never hangs.
        // The sender is dropped without a terminal record, which is the
        // same `Err` arm a true past-deadline stall takes.
        let (tx, rx) = channel::<GrepStreamItem>();
        drop(tx);
        let (mut server, mut client) = UnixStream::pair().unwrap();
        write_streamed_response(&mut server, &Response::ok("rid", json!({})), rx).unwrap();
        let mut last = None;
        loop {
            let frame = ipc::read_frame(&mut client, ipc::MAX_CHUNK).unwrap();
            if frame.is_empty() {
                break;
            }
            last = Some(frame);
        }
        let line: serde_json::Value =
            serde_json::from_slice(&last.expect("a stall must still terminate")).unwrap();
        assert_eq!(line["code"], "internal");
    }
    #[test]
    fn ipc_handlers_report_writer_disconnects_instead_of_hanging() {
        let tmp = tempfile::tempdir().unwrap();
        let root = skeleton_project(tmp.path(), "stopped-writer");
        let shared = test_shared();
        let (control, receiver) = channel::<StoreCommand>();
        drop(receiver);
        let (wake, _wake_rx) = channel();
        let (events, _events_rx) = channel();

        shared.table.lock().unwrap().insert(
            normalize(&root),
            WatchEntry {
                stop: watcher::new_stop_flag(),
                cold: Arc::new(AtomicBool::new(false)),
                ready: Arc::new(AtomicBool::new(true)),
                watch_handles: Vec::new(),
                collector: None,
                control,
                events,

                wake,
            },
        );

        let selections = json!({"selections": [bogus_selection_handle()]});
        let requests = [
            ("timeline.log", json!({})),
            ("timeline.info", json!({"reference": "@"})),
            ("checkpoint.list", json!({})),
            ("checkpoint.create", json!({"name": "cp"})),
            ("restore.plan", json!({"at": "@"})),
            ("restore.apply", json!({"token": "token"})),
            ("restore.resume", json!({})),
            ("restore.abandon", json!({})),
            ("fragment.plan", selections.clone()),
            ("fragment.apply", json!({"token": "token"})),
            ("smart.plan", selections),
            ("store.doctor", json!({})),
            ("store.gc", json!({})),
            ("diff", json!({"from": "@"})),
            (
                "timeline.grep",
                serde_json::to_value(point_grep("x")).unwrap(),
            ),
            ("cache.backfill", json!({})),
        ];
        let mut shutting_down = false;
        for (method, params) in requests {
            let (response, _body) = dispatch(
                &shared,
                &test_request(method, Some(root.clone()), params),
                &mut shutting_down,
            );
            assert_eq!(error_code_of(&response), "internal", "{method}");
            assert!(response.error.unwrap().message.contains("writer stopped"));
        }
    }
    #[test]
    fn cold_collector_stops_when_event_or_wake_channels_end() {
        let tmp = tempfile::tempdir().unwrap();
        let root = skeleton_project(tmp.path(), "cold-stop");
        let stop = watcher::new_stop_flag();
        stop.store(true, Ordering::SeqCst);
        let (_event_tx, event_rx) = channel();
        let (_wake_tx, wake_rx) = channel();
        assert!(collect_cold(
            &root,
            &event_rx,
            &wake_rx,
            &stop,
            StoreLimits::default(),
            8 * 1024 * 1024,
        )
        .is_none());

        let stop = watcher::new_stop_flag();
        let (event_tx, event_rx) = channel();
        drop(event_tx);
        let (_wake_tx, wake_rx) = channel();
        assert!(collect_cold(
            &root,
            &event_rx,
            &wake_rx,
            &stop,
            StoreLimits::default(),
            8 * 1024 * 1024,
        )
        .is_none());
    }

    // ------------------------------------------------ watch policy edges

    #[test]
    fn global_git_ignore_candidates_resolve_xdg_and_home_and_drop_missing_files() {
        // The only test that touches these process-global vars; the drop
        // guard below restores them even on failure. The rule files this
        // test plants hold an inert pattern that can never match another
        // test's fixtures, so a racing spawn_watch is unaffected either way.
        struct EnvRestore {
            saved: Vec<(&'static str, Option<std::ffi::OsString>)>,
        }
        impl Drop for EnvRestore {
            fn drop(&mut self) {
                for (key, value) in &self.saved {
                    match value {
                        Some(v) => std::env::set_var(key, v),
                        None => std::env::remove_var(key),
                    }
                }
            }
        }
        let _restore = EnvRestore {
            saved: vec![
                ("XDG_CONFIG_HOME", std::env::var_os("XDG_CONFIG_HOME")),
                ("HOME", std::env::var_os("HOME")),
            ],
        };

        let tmp = tempfile::tempdir().unwrap();
        let xdg = tmp.path().join("xdg");
        std::fs::create_dir_all(xdg.join("git")).unwrap();
        std::fs::write(xdg.join("git/ignore"), "inert-global-name\n").unwrap();
        let home = tmp.path().join("home");
        std::fs::create_dir_all(&home).unwrap();
        std::env::set_var("XDG_CONFIG_HOME", &xdg);
        std::env::set_var("HOME", &home);

        // XDG wins when it points at an existing rules file.
        assert_eq!(
            global_git_ignore_candidates(),
            vec![xdg.join("git").join("ignore")]
        );

        // An empty XDG is treated as unset; HOME's default location is
        // consulted only when the file actually exists.
        std::env::set_var("XDG_CONFIG_HOME", "");
        assert!(global_git_ignore_candidates().is_empty());
        std::fs::create_dir_all(home.join(".config/git")).unwrap();
        std::fs::write(home.join(".config/git/ignore"), "inert-global-name\n").unwrap();
        assert_eq!(
            global_git_ignore_candidates(),
            vec![home.join(".config").join("git").join("ignore")]
        );
    }

    #[test]
    fn spawn_watch_falls_back_to_defaults_when_config_is_unparsable() {
        let tmp = tempfile::tempdir().unwrap();
        let root = skeleton_project(tmp.path(), "broken-config");
        // Valid TOML that still parses as a store marker (read_store_format
        // succeeds) but whose [watch] section cannot deserialize into
        // ProjectConfig: the watch must degrade to defaults, not refuse.
        std::fs::write(
            sheaf_dir(&root).join("config.toml"),
            "format_version = 2\n\n[watch]\ndebounce_ms = \"fast\"\n",
        )
        .unwrap();
        let shared = test_shared();
        assert!(
            spawn_watch(&shared, &root),
            "an unreadable config degrades to defaults, never a refusal"
        );
        assert!(shared.watching(&root));
    }

    #[test]
    fn spawn_watch_refuses_projects_with_uncompilable_ignore_patterns() {
        let tmp = tempfile::tempdir().unwrap();
        let root = skeleton_project(tmp.path(), "bad-patterns");
        // "[" is an unterminated glob class: config parses, the ignore
        // compiler does not, and a watch built on it would silently capture
        // everything the pattern meant to exclude.
        std::fs::write(
            sheaf_dir(&root).join("config.toml"),
            "format_version = 2\n\n[ignore]\npatterns = [\"[\"]\n",
        )
        .unwrap();
        let shared = test_shared();
        assert!(!spawn_watch(&shared, &root));
        assert!(!shared.watching(&root));
    }

    #[test]
    fn resume_enrollment_prune_failure_is_warned_and_enrollment_survives() {
        let tmp = tempfile::tempdir().unwrap();
        let scratch = tmp.path().join("scratch");
        std::fs::create_dir_all(&scratch).unwrap();
        let reg_dir = tmp.path().join("reg");
        let reg = Registry::at(reg_dir.join("enrollments.jsonl"));
        reg.upsert(&scratch).unwrap();
        std::fs::remove_dir_all(&scratch).unwrap();

        // A read-only registry directory: list() still works, but the
        // prune's rewrite cannot, so the failure is downgraded to a warning
        // and the stale enrollment is left for the operator.
        use std::os::unix::fs::PermissionsExt;
        let writable = std::fs::metadata(&reg_dir).unwrap().permissions().mode();
        let mut perms = std::fs::metadata(&reg_dir).unwrap().permissions();
        perms.set_mode(0o555);
        std::fs::set_permissions(&reg_dir, perms).unwrap();

        let shared = test_shared();
        let (resumed, pruned) = resume_enrollments(&shared, &reg);
        assert_eq!((resumed, pruned), (0, 0), "the failed prune is not a prune");
        assert_eq!(reg.list().unwrap().len(), 1, "the enrollment survives");

        let mut perms = std::fs::metadata(&reg_dir).unwrap().permissions();
        perms.set_mode(writable & 0o777);
        std::fs::set_permissions(&reg_dir, perms).unwrap();
    }

    // ------------------------------------------ IPC-level store error arms

    #[test]
    fn ipc_handlers_surface_collector_store_errors_verbatim() {
        let tmp = tempfile::tempdir().unwrap();
        let root = skeleton_project(tmp.path(), "canned-errors");
        let shared = test_shared();
        let (control, rx) = channel::<StoreCommand>();
        let (wake, _wake_rx) = channel::<()>();
        let (events, _events_rx) = channel();

        shared.table.lock().unwrap().insert(
            normalize(&root),
            WatchEntry {
                stop: watcher::new_stop_flag(),
                cold: Arc::new(AtomicBool::new(false)),
                ready: Arc::new(AtomicBool::new(true)),
                watch_handles: Vec::new(),
                collector: None,
                control,
                events,

                wake,
            },
        );
        // A degraded collector (say, a store that fails to resolve) answers
        // with a typed store error: the envelope must carry that exact code
        // and no body, never a generic internal failure.
        let replier = std::thread::spawn(move || {
            while let Ok(command) = rx.recv() {
                let failure =
                    || sheaf_core::SheafError::StoreCorrupt("canned collector failure".into());
                let command = match command {
                    StoreCommand::InWorktree { command, .. } => *command,
                    other => other,
                };
                match command {
                    StoreCommand::PlanRestore { reply, .. } => {
                        let _ = reply.send(Err(failure()));
                    }
                    StoreCommand::PlanFragment { reply, .. } => {
                        let _ = reply.send(Err(failure()));
                    }
                    StoreCommand::CacheBackfill { reply, .. } => {
                        let _ = reply.send(Err(failure()));
                    }
                    _ => {}
                }
            }
        });

        let mut shutting_down = false;
        let selections =
            json!({"selections": [serde_json::to_value(bogus_selection_handle()).unwrap()]});
        let cases = [
            ("restore.plan", json!({"at": "@"})),
            ("fragment.plan", selections),
            ("cache.backfill", json!({})),
        ];
        for (method, params) in cases {
            let (resp, body) = dispatch(
                &shared,
                &test_request(method, Some(root.clone()), params),
                &mut shutting_down,
            );
            assert!(!resp.ok, "{method} must fail");
            assert_eq!(error_code_of(&resp), "store.corrupt", "{method} code");
            assert!(
                matches!(body, IpcBody::Bytes(bytes) if bytes.is_empty()),
                "{method} must not stream a body"
            );
        }
        drop(shared);
        replier.join().unwrap();
    }

    #[test]
    fn stalled_writer_answers_within_the_bounded_reply_deadlines() {
        // A wedged collector (its reply receiver is held but never answered)
        // must cost the client a bounded wait, then a stated timeout — never
        // a hang. Verbs run on parallel threads; each measures its own wait
        // against its own deadline.
        let shared = test_shared();
        let run_stalled = |verb: &'static str, params: serde_json::Value, min: Duration| {
            let shared = shared.clone();
            std::thread::spawn(move || {
                let root = PathBuf::from(format!("/tmp/sheafd-stalled-{verb}"));
                let (control, stuck_rx) = channel::<StoreCommand>();
                let (wake, _wake_rx) = channel::<()>();
                let (events, _events_rx) = channel();

                shared.table.lock().unwrap().insert(
                    normalize(&root),
                    WatchEntry {
                        stop: watcher::new_stop_flag(),
                        cold: Arc::new(AtomicBool::new(false)),
                        ready: Arc::new(AtomicBool::new(true)),
                        watch_handles: Vec::new(),
                        collector: None,
                        control,
                        events,
                        wake,
                    },
                );
                let mut shutting_down = false;
                let started = Instant::now();
                let (resp, body) = dispatch(
                    &shared,
                    &test_request(verb, Some(root.clone()), params),
                    &mut shutting_down,
                );
                let elapsed = started.elapsed();
                assert!(!resp.ok, "{verb} must time out");
                assert_eq!(error_code_of(&resp), "internal", "{verb} code");
                assert!(
                    resp.error.unwrap().message.contains("timed out"),
                    "{verb} must report the deadline, not a generic failure"
                );
                assert!(
                    matches!(body, IpcBody::Bytes(bytes) if bytes.is_empty()),
                    "{verb} must not stream a body"
                );
                assert!(
                    elapsed >= min,
                    "{verb} gave up early after {elapsed:?} (deadline {min:?})"
                );
                drop(stuck_rx);
            })
        };
        // REQUEST_SOFT is 10s, DIFF_HARD 30s; the assertions keep a 1s slack.
        let soft = Duration::from_secs(9);
        let hard = Duration::from_secs(29);
        let handles = vec![
            run_stalled("timeline.info", json!({"reference": "@"}), soft),
            run_stalled("checkpoint.create", json!({"name": "cp"}), soft),
            run_stalled("restore.plan", json!({"at": "@"}), soft),
            run_stalled(
                "fragment.plan",
                json!({"selections": [serde_json::to_value(bogus_selection_handle()).unwrap()]}),
                soft,
            ),
            run_stalled("diff", json!({"from": "@"}), hard),
            run_stalled("cache.backfill", json!({}), hard),
        ];
        for handle in handles {
            handle.join().unwrap();
        }
    }

    // ---------------------------------------- store-backed verbs over IPC

    #[test]
    fn restore_cycle_over_ipc_resumes_a_real_intent_and_abandons_with_reconciliation() {
        let tmp = tempfile::tempdir().unwrap();
        let root = skeleton_project(tmp.path(), "ipc-restore-cycle");
        std::fs::write(root.join("seed.txt"), "v1\n").unwrap();
        let shared = test_shared();
        assert!(spawn_watch(&shared, &root));
        let shutting_down = std::cell::RefCell::new(false);
        let send = |method: &str, params: serde_json::Value| {
            dispatch(
                &shared,
                &test_request(method, Some(root.clone()), params),
                &mut shutting_down.borrow_mut(),
            )
        };

        // Warm the lazy project: the open-time reconcile captures v1.
        let (resp, _) = send("timeline.log", json!({"limit": 10}));
        assert!(resp.ok, "warm-up failed: {:?}", resp.error);
        std::thread::sleep(Duration::from_millis(300));
        std::fs::write(root.join("seed.txt"), "v2\n").unwrap();
        assert!(
            wait_until(15_000, || {
                let (resp, _) = send("timeline.log", json!({"limit": 10}));
                resp.result.unwrap()["entries"].as_array().unwrap().len() >= 2
            }),
            "the v2 capture never landed"
        );

        // Without a pending intent the operator verb refuses by name.
        let (resp, _) = send("restore.resume", json!({}));
        assert_eq!(error_code_of(&resp), "restore.plan_stale");

        // Park a genuine intent for @~1, byte-for-byte what a crashed
        // restore leaves behind, then let the operator resume it.
        let (resp, IpcBody::Bytes(body)) = send("restore.plan", json!({"at": "@~1"})) else {
            panic!("restore.plan must answer with a byte body");
        };
        assert!(resp.ok, "plan failed: {:?}", resp.error);
        let plan: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(plan["actions"].as_array().unwrap().len(), 1);
        std::fs::create_dir_all(root.join(".sheaf/state")).unwrap();
        let park_intent = |token: &serde_json::Value, target: &serde_json::Value| {
            std::fs::write(
                root.join(".sheaf/state/restore.intent"),
                json!({
                    "token": token,
                    "mode": "full",
                    "scope": [],
                    "target": target,
                    "started_ms": chrono::Utc::now().timestamp_millis(),
                })
                .to_string(),
            )
            .unwrap();
        };
        park_intent(&plan["token"], &plan["target"]);

        let (resp, _) = send("restore.resume", json!({}));
        assert!(resp.ok, "resume failed: {:?}", resp.error);
        let outcome = resp.result.unwrap()["outcome"].clone();
        assert_eq!(outcome["files_written"], 1);
        assert_eq!(outcome["resumed"], true);
        assert_eq!(
            std::fs::read(root.join("seed.txt")).unwrap(),
            b"v1\n",
            "the worktree actually moved back"
        );

        // Abandoning drops the intent and reconciles uncaptured worktree
        // state as ordinary history, so nothing on disk is uncaptured.
        std::fs::write(root.join("after.json"), "concurrent work\n").unwrap();
        park_intent(&plan["token"], &plan["target"]);
        let (resp, _) = send("restore.abandon", json!({}));
        assert!(resp.ok, "abandon failed: {:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["abandoned"], true);
        assert!(
            result["reconciled_as"].is_string(),
            "the uncaptured after.json must be reconciled: {result}"
        );
        assert!(!root.join(".sheaf/state/restore.intent").exists());
    }

    #[test]
    fn smart_plan_serves_both_phases_and_validates_params_over_ipc() {
        let tmp = tempfile::tempdir().unwrap();
        let root = skeleton_project(tmp.path(), "smart-plan-ipc");
        std::fs::write(root.join("seed.txt"), "seed content\n").unwrap();
        let shared = test_shared();
        assert!(spawn_watch(&shared, &root));
        let shutting_down = std::cell::RefCell::new(false);
        let send = |params: serde_json::Value| {
            dispatch(
                &shared,
                &test_request("smart.plan", Some(root.clone()), params),
                &mut shutting_down.borrow_mut(),
            )
        };

        // Malformed handles and malformed head_texts are refused before any
        // store is touched.
        let (resp, _) = send(json!({"selections": [{"version": 1}]}));
        assert_eq!(error_code_of(&resp), "bad.params");
        assert!(resp
            .error
            .unwrap()
            .message
            .contains("invalid selection handle"));
        let (resp, _) = send(json!({
            "selections": [serde_json::to_value(bogus_selection_handle()).unwrap()],
            "head_texts": 5,
        }));
        assert_eq!(error_code_of(&resp), "bad.params");
        assert!(resp.error.unwrap().message.contains("head_texts"));

        // Warm the project so the phase replies come from a live collector.
        let (resp, _) = dispatch(
            &shared,
            &test_request("timeline.log", Some(root.clone()), json!({})),
            &mut shutting_down.borrow_mut(),
        );
        assert!(resp.ok, "warm-up failed: {:?}", resp.error);
        // Phase one names the destination paths whose HEAD content the
        // caller must fetch. A handle whose frontier cannot be read
        // degrades to its own historical path.
        let handle = serde_json::to_value(bogus_selection_handle()).unwrap();
        let (resp, _) = send(json!({"selections": [handle.clone()]}));
        assert!(resp.ok, "phase one failed: {:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["phase"], "resolve");
        assert_eq!(
            result["paths"],
            json!(["never-tracked.txt"]),
            "an unreadable frontier degrades to the historical path"
        );
        // Phase two answers with the plan itself; an unresolvable handle
        // degrades into a conflict inside the plan, never a hard failure.
        let (resp, _) = send(json!({
            "selections": [handle],
            "head_texts": {},
        }));
        assert!(resp.ok, "phase two failed: {:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["plan_summary"]["phase"], "plan");
        assert_eq!(result["plan_summary"]["files"], 0);
        assert!(
            result["plan_summary"]["conflicts"].as_u64().unwrap() >= 1,
            "an unresolvable selection must conflict: {result}"
        );
        assert!(result["plan"].is_object());
    }

    #[test]
    fn read_verbs_report_store_errors_and_backfill_success_over_ipc() {
        let tmp = tempfile::tempdir().unwrap();
        let root = skeleton_project(tmp.path(), "read-verb-errors");
        std::fs::write(root.join("seed.txt"), "seed content\n").unwrap();
        let shared = test_shared();
        assert!(spawn_watch(&shared, &root));
        let shutting_down = std::cell::RefCell::new(false);
        let send = |method: &str, params: serde_json::Value| {
            dispatch(
                &shared,
                &test_request(method, Some(root.clone()), params),
                &mut shutting_down.borrow_mut(),
            )
        };

        // Warm the lazy project so store-backed verbs have a writer.
        let (resp, _) = send("timeline.log", json!({"limit": 10}));
        assert!(resp.ok, "warm-up failed: {:?}", resp.error);

        // Store-level reference errors surface as the store's own code.
        let (resp, _) = send("timeline.info", json!({"reference": "bogus-ref"}));
        assert_eq!(error_code_of(&resp), "state.bad_reference");
        let (resp, _) = send(
            "checkpoint.create",
            json!({"name": "cp-fine", "at": "bogus-ref"}),
        );
        assert_eq!(error_code_of(&resp), "state.bad_reference");
        let (resp, body) = send("diff", json!({"from": "bogus-ref"}));
        assert_eq!(error_code_of(&resp), "state.bad_reference");
        assert!(matches!(body, IpcBody::Bytes(bytes) if bytes.is_empty()));

        // Param-level validation fires before the store is touched.
        let (resp, _) = send("diff", json!({"from": "@", "paths": [1]}));
        assert_eq!(error_code_of(&resp), "bad.params");
        let (resp, _) = send("restore.plan", json!({"at": "@", "paths": [1]}));
        assert_eq!(error_code_of(&resp), "bad.params");
        let (resp, _) = send("timeline.grep", json!({"mode": "point"}));
        assert_eq!(error_code_of(&resp), "bad.params");
        assert!(resp.error.unwrap().message.contains("invalid grep request"));

        // cache.backfill answers with a bounded report over the live store.
        let (resp, _) = send("cache.backfill", json!({}));
        assert!(resp.ok, "backfill failed: {:?}", resp.error);
        let report = resp.result.unwrap()["backfill"].clone();
        assert_eq!(report["complete"], true);
        assert_eq!(report["root"], root.display().to_string());
    }

    #[test]
    fn worktree_and_merge_cycle_over_ipc() {
        let tmp = tempfile::tempdir().unwrap();
        // Build divergent history plus a linked worktree, then release the
        // writer lock so the daemon reopens the very same store.
        let (store, _ignore, lock, _primary, _linked, source) = branched_store(tmp.path());
        drop(store);
        drop(lock);
        let primary = tmp.path().join("primary");
        let linked = tmp.path().join("linked");

        let shared = test_shared();
        assert!(spawn_watch(&shared, &primary));
        let shutting_down = std::cell::RefCell::new(false);
        let send = |method: &str, params: serde_json::Value| {
            dispatch(
                &shared,
                &test_request(method, Some(primary.clone()), params),
                &mut shutting_down.borrow_mut(),
            )
        };

        // Warm the lazy store so the writer is live.
        let (resp, _) = send("timeline.log", json!({"limit": 10}));
        assert!(resp.ok, "warm-up failed: {:?}", resp.error);

        // worktree.list names the primary and the present linked worktree.
        let (resp, _) = send("worktree.list", json!({}));
        assert!(resp.ok, "worktree.list failed: {:?}", resp.error);
        assert_eq!(
            resp.result.unwrap()["worktrees"].as_array().unwrap().len(),
            2
        );

        // merge.plan requires a source; then answers with the source-only plan.
        let (resp, _) = send("merge.plan", json!({}));
        assert_eq!(error_code_of(&resp), "bad.params");
        let (resp, _) = send("merge.plan", json!({ "source": source }));
        assert!(resp.ok, "merge.plan failed: {:?}", resp.error);
        let plan = resp.result.unwrap()["plan"].clone();
        assert_eq!(plan["actions"].as_array().unwrap().len(), 1);
        let token = plan["token"].as_str().unwrap().to_owned();

        // merge.apply requires a token; a bogus one is stale; the real one
        // squashes the source change onto the primary worktree.
        let (resp, _) = send("merge.apply", json!({}));
        assert_eq!(error_code_of(&resp), "bad.params");
        let (resp, _) = send("merge.apply", json!({"token": "no-such-token"}));
        assert_eq!(error_code_of(&resp), "merge.plan_stale");
        let (resp, _) = send("merge.apply", json!({ "token": token }));
        assert!(resp.ok, "merge.apply failed: {:?}", resp.error);
        assert_eq!(resp.result.unwrap()["outcome"]["files_written"], 1);
        assert_eq!(
            std::fs::read_to_string(primary.join("source.txt")).unwrap(),
            "source\n"
        );

        // merge.resume with nothing pending refuses by name.
        let (resp, _) = send("merge.resume", json!({}));
        assert_eq!(error_code_of(&resp), "merge.plan_stale");

        // worktree.add validates its params, refuses an existing directory,
        // then materializes a fresh worktree and starts watching it.
        let (resp, _) = send("worktree.add", json!({"destination": "/tmp/x"}));
        assert_eq!(error_code_of(&resp), "bad.params", "missing reference");
        let (resp, _) = send("worktree.add", json!({"reference": "base"}));
        assert_eq!(error_code_of(&resp), "bad.params", "missing destination");
        let (resp, _) = send(
            "worktree.add",
            json!({"reference": "base", "destination": "relative/dir"}),
        );
        assert_eq!(
            error_code_of(&resp),
            "bad.params",
            "destination not absolute"
        );
        let (resp, _) = send(
            "worktree.add",
            json!({"reference": "base", "destination": linked.display().to_string()}),
        );
        assert!(
            resp.error.unwrap().message.contains("already exists"),
            "add onto an existing dir must be refused"
        );
        let fresh = tmp.path().join("linked-2");
        let (resp, _) = send(
            "worktree.add",
            json!({"reference": "base", "destination": fresh.display().to_string()}),
        );
        assert!(resp.ok, "worktree.add failed: {:?}", resp.error);
        let result = resp.result.unwrap();
        assert_eq!(result["watching"], true);
        assert_eq!(result["worktree"]["primary"], false);
        assert!(
            fresh.is_dir(),
            "the fresh worktree was materialized on disk"
        );
    }
}
