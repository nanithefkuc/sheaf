//! `sheaf` CLI: enrollment and status, the capture timeline and checkpoints,
//! non-destructive worktree restore, the diff surface, timeline text search,
//! retention/gc, and the squash collapse (preview by default, `--` to commit
//! + stamp).

use std::io::{IsTerminal as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use anyhow::{Context as _, Result};
use chrono::{DateTime, Local, Utc};
use clap::{Parser, Subcommand};

use sheaf_core::config;
use sheaf_core::init::{init_project, resolve_project_root, InitOptions};
use sheaf_core::ipc::{Client, PROTO_MAJOR, PROTO_MINOR};
use sheaf_core::registry::{normalize_existing, Registry};

#[derive(Parser)]
#[command(
    name = "sheaf",
    version,
    about = "flight recorder for your worktree — CRDT history beneath git",
    long_about = "sheaf watches enrolled projects and records every change as\nCRDT operations, giving you fine-grained rollback without touching git."
)]
struct Args {
    /// Color human-readable terminal output. `auto` colors terminals unless
    /// NO_COLOR is set; JSON output is always left machine-readable.
    #[arg(long, global = true, value_enum, default_value_t = ColorWhen::Auto)]
    color: ColorWhen,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Enroll a directory: create its store skeleton and tell the daemon.
    Init {
        /// Project directory (default: current directory).
        path: Option<PathBuf>,
    },
    /// Show local store + daemon health for a project.
    Status {
        /// Project directory (default: nearest ancestor with a store).
        path: Option<PathBuf>,
    },
    /// Browse capture history.
    ///
    /// The human view prints oldest → newest, so the latest change lands at
    /// the bottom of terminal scrollback where the eye already is. `--json`
    /// keeps the wire order (newest first) for scripts.
    Log {
        /// Project directory (default: nearest ancestor with a store).
        #[arg(long, short = 'C')]
        project: Option<PathBuf>,
        /// Only captures touching this root-relative path.
        #[arg(long)]
        path: Option<PathBuf>,
        /// Follow renames: include captures made under this path's former
        /// names (first-class rename history, not content guessing).
        #[arg(long, requires = "path")]
        follow: bool,
        /// Include every divergent branch, not only the current lineage.
        #[arg(long)]
        all: bool,
        /// Continue after this full or unique capture-ID prefix.
        #[arg(long)]
        before: Option<String>,
        #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u16).range(1..=1000))]
        limit: u16,
        /// Emit the IPC-compatible JSON result.
        #[arg(long)]
        json: bool,
    },
    /// Discover every literal occurrence at one point, or follow occurrence history.
    ///
    /// Point discovery at @ is the default. Add --history for lifecycle
    /// transitions over a range. Every restorable occurrence carries a stable
    /// selection handle plus line-oriented presentation coordinates.
    #[command(after_help = "\
EXAMPLES:
    sheaf grep \"fn parse\"                        discover every occurrence at @
    sheaf grep TODO --at @~20 --path src          discover at one historical point
    sheaf grep TODO --history --path src --follow follow history and renames
    sheaf grep needle --history --all             include divergent branches
    sheaf grep needle --history --at @~5 --path src/lib.rs --line 3
                                                   follow the occurrence at that anchor
    sheaf grep needle --history --episode ep1:abc123
                                                   re-follow one episode by ID
    sheaf grep needle --extent line --json        line extent, NDJSON output")]
    Grep {
        /// Literal text to discover at one point (default) or follow through history.
        query: String,
        /// Follow occurrence episodes through history instead of discovering at one point.
        #[arg(long)]
        history: bool,
        /// Discovery point or occurrence-anchor point. Defaults to @ in point mode.
        #[arg(long)]
        at: Option<String>,
        /// Only this root-relative path (file or subtree); with --line, the
        /// anchor occurrence's path.
        #[arg(long)]
        path: Option<PathBuf>,
        /// Anchor history to the occurrence at --at on this one-based line of
        /// --path. Requires --history, --at, and --path.
        #[arg(long, requires_all = ["history", "at", "path"])]
        line: Option<usize>,
        /// One-based Unicode-scalar column of the anchor match start; disambiguates
        /// several occurrences on the anchored line.
        #[arg(long, requires = "line")]
        column: Option<usize>,
        /// Anchor history to one branch-qualified episode ID (as printed by a
        /// prior history query).
        #[arg(long, requires = "history", conflicts_with_all = ["line", "selection"])]
        episode: Option<String>,
        /// Anchor history with a full selection handle: a JSON file holding a
        /// grep hit or bare handle ('-' reads stdin).
        #[arg(long, requires = "history", conflicts_with_all = ["line", "episode"])]
        selection: Option<String>,
        /// Follow renames: also search this path's former names.
        #[arg(long, requires = "path")]
        follow: bool,
        /// Include every divergent branch, not only the current lineage.
        #[arg(long)]
        all: bool,
        /// Emit one hit per capture instead of only lifecycle transitions.
        #[arg(long)]
        every_capture: bool,
        /// Restorable extent per hit.
        #[arg(long, value_enum, default_value_t = GrepExtentArg::Match)]
        extent: GrepExtentArg,
        /// Oldest history bound (exclusive): capture ID, checkpoint, @~N, time.
        #[arg(long)]
        from: Option<String>,
        /// Inclusive history upper bound; requires --history and defaults to @.
        #[arg(long)]
        to: Option<String>,
        /// Continue from CAPTURE, RESUME:INDEX, or AFTER:RESUME:INDEX.
        #[arg(long)]
        after: Option<String>,
        /// Maximum hits before returning a continuation cursor.
        #[arg(long, default_value_t = 1000, value_parser = clap::value_parser!(u32).range(1..=100000))]
        max_results: u32,
        #[arg(long, short = 'C')]
        project: Option<PathBuf>,
        /// Emit the IPC-compatible JSON result.
        #[arg(long)]
        json: bool,
    },
    /// Show the file-level changes captured in one timeline point.
    Info {
        /// Full or unique shortened capture ID (at least 6 hexadecimal characters).
        reference: String,
        /// Project directory (default: nearest ancestor with a store).
        #[arg(long, short = 'C')]
        project: Option<PathBuf>,
        /// Emit the IPC-compatible JSON result.
        #[arg(long)]
        json: bool,
    },
    /// Compare the worktree or two timeline points.
    ///
    /// With no point: the live worktree against `@` (uncaptured edits).
    /// With one point: the worktree against that point. With two points
    /// (or `A..B`): point against point, branches included.
    #[command(after_help = "\
EXAMPLES:
    sheaf diff                                   worktree vs the last capture
    sheaf diff checkpoint:before-refactor        worktree vs a checkpoint
    sheaf diff @~3..@~1                          two points on the lineage
    sheaf diff @~2 --path src/lib.rs --stat      one file, summary only")]
    Diff {
        /// Old point (capture ID, checkpoint:<name>, @, @~N, or a time);
        /// defaults to @. `A..B` compares two points.
        from: Option<String>,
        /// New point; omit to compare the old point against the worktree.
        to: Option<String>,
        /// Limit the comparison to these root-relative paths (repeatable).
        #[arg(long = "path")]
        paths: Vec<PathBuf>,
        /// Print a per-file summary instead of a patch.
        #[arg(long)]
        stat: bool,
        /// Emit the IPC-compatible JSON result with the patch inline.
        #[arg(long)]
        json: bool,
        /// Exit 1 when differences exist (git parity for scripting).
        #[arg(long)]
        exit_code: bool,
        #[arg(long, short = 'C')]
        project: Option<PathBuf>,
    },
    /// Create or list named timeline checkpoints.
    ///
    /// `sheaf checkpoint <name>` is shorthand for `checkpoint create <name>`,
    /// so a meaningful restore point is one word away.
    Checkpoint {
        #[command(subcommand)]
        command: Option<CheckpointCmd>,
        /// Shorthand: a bare name creates a checkpoint at `@` (no subcommand).
        name: Option<String>,
        /// With the bare-name form: pin an explicit point instead of `@`.
        #[arg(long, requires = "name")]
        at: Option<String>,
        #[arg(long, short = 'C', requires = "name")]
        project: Option<PathBuf>,
    },
    /// Put the worktree back to an earlier point, non-destructively.
    ///
    /// With no paths the whole worktree is repositioned and later edits
    /// branch from there. With paths, only those files/directories move back
    /// and the change is recorded as ordinary forward history.
    #[command(after_help = "\
EXAMPLES:
    sheaf restore before-refactor                a checkpoint, by bare name
    sheaf restore @~3                             three captures back
    sheaf restore @~2h                           two hours ago (relative)
    sheaf restore \"2 hours ago\" src/parser.rs      one file, by wall-clock time
    sheaf restore 10:30                          that time today
    sheaf restore 2026-08-27T10:30               a local date and time
    sheaf restore --at 7f3a2b1c9d0e --dry-run     show the plan, touch nothing
    sheaf restore --resume                        finish an interrupted restore
    sheaf restore --abandon                       drop a pending restore intent
    sheaf restore --selection sel.json --dry-run  preview a fragment splice
    sheaf restore --selection sel.json --insert   reinsert a deleted unit")]
    Restore {
        /// Timeline point, unless given with --at. Accepts a capture-ID
        /// prefix, a `checkpoint:<name>` (or a bare checkpoint name), `@`,
        /// `@~N` captures back, `@~<duration>` (e.g. `2h`), or a timestamp
        /// (`10:30`, `2026-08-27T10:30`, `"2 hours ago"`).
        #[arg(value_name = "POINT|PATH")]
        args: Vec<String>,
        /// The timeline point, when the positional slot holds paths only.
        #[arg(long)]
        at: Option<String>,
        /// Compute and print the plan without touching the worktree.
        #[arg(long)]
        dry_run: bool,
        /// Finish an interrupted restore whose intent is still pending,
        /// overriding the staleness bound that gates automatic replay.
        #[arg(long, conflicts_with_all = ["at", "dry_run"])]
        resume: bool,
        /// Discard a pending restore intent; the worktree stays exactly as
        /// it is and anything half-applied becomes ordinary history.
        #[arg(long, conflicts_with_all = ["at", "dry_run", "resume"])]
        abandon: bool,
        /// Restore only a selected historical fragment: a JSON
        /// file holding a selection handle from `sheaf grep --json`
        /// (`-` reads stdin). Accepts a bare handle, a grep hit object, or
        /// an array of either.
        #[arg(
            long,
            value_name = "HANDLE_JSON",
            conflicts_with_all = ["args", "at", "resume", "abandon"]
        )]
        selection: Option<String>,
        /// With --selection: reinsert the unit at its unique deletion scar
        /// (the unit must be absent; replace is the default mode).
        #[arg(long, requires = "selection", conflicts_with = "delete")]
        insert: bool,
        /// With --selection: delete the selected unit from its unique
        /// current position (the unit must be present).
        #[arg(long, requires = "selection", conflicts_with = "insert")]
        delete: bool,
        #[arg(long, short = 'C')]
        project: Option<PathBuf>,
        /// Emit the IPC-compatible JSON result.
        #[arg(long)]
        json: bool,
    },
    /// Check store integrity without touching anything.
    ///
    /// Verifies journal framing, the snapshot chain, head sanity, intent
    /// parseability, and that every blob history references is present.
    Doctor {
        /// Project directory (default: nearest ancestor with a store).
        #[arg(long, short = 'C')]
        project: Option<PathBuf>,
        /// Emit the JSON report.
        #[arg(long)]
        json: bool,
        /// Fix what can be fixed safely: truncate torn journal tails to
        /// their CRC-clean prefix, remove superseded snapshots, clear a
        /// quarantined restore intent and leftover staging. Ambiguous
        /// corruption is refused with guidance, never guessed at.
        #[arg(long)]
        fix: bool,
    },
    /// Retention: report (or with --apply, run) reachability-constrained
    /// garbage collection.
    ///
    /// The plan never removes anything a restore to ANY timeline point could
    /// still need: only journal segments a surviving snapshot contains,
    /// snapshots a newer one supersedes, and blobs no recorded event or
    /// entry can ever reach again (charter constraint 7).
    Gc {
        /// Actually remove the collected bytes (default: report only).
        /// `--collect` is an accepted alias. Applying also executes any
        /// retention trim (expiry and explicit collectable marks).
        #[arg(long, visible_alias = "collect")]
        apply: bool,
        #[arg(long, short = 'C')]
        project: Option<PathBuf>,
        /// Emit the JSON plan/report.
        #[arg(long)]
        json: bool,
        /// Set the automatic edit-expiry horizon (e.g. 30d, 72h, 45m, 90s)
        /// and exit. Expiry is reachability-bound: checkpointed points,
        /// branch tips, the head, and pending restores never expire.
        #[arg(long, value_name = "DURATION")]
        set_expiry: Option<String>,
        /// Mark an edit as explicitly collectable, bypassing reachability
        /// protection. Takes effect at the next `gc --apply`.
        /// The current head and pending restore targets cannot be marked.
        #[arg(value_name = "REF")]
        mark: Option<String>,
    },
    /// Collapse a span of captures into a commit-sized change.
    ///
    /// Read-only by default: resolves the span, shows the collapse
    /// candidate and a drafted commit message, and runs no git commands.
    /// Everything after an explicit `--` is forwarded to `git commit` — that
    /// passthrough is the sanction to stage the collapse, commit, and stamp
    /// the frame (`git-<sha>` checkpoint + `.sheaf/frames.jsonl`).
    #[command(after_help = "\
EXAMPLES:
    sheaf squash                             preview last frame → worktree
    sheaf squash @~12                        preview the last 12 captures
    sheaf squash checkpoint:before-rework    preview from a checkpoint
    sheaf squash @~3..@~1                    preview a point-to-point span
    sheaf squash @~12 -- -m \"message\"        collapse, commit, stamp the frame")]
    Squash {
        /// Anchor point where the span starts (capture ID, checkpoint:<name>,
        /// bare checkpoint name, `@`, `@~N`, `@~<duration>`, or `A..B`).
        /// Default: the last stamped commit frame; with `--`, the last git
        /// commit's time when no frame was ever stamped.
        range: Option<String>,
        /// Smart squash: commit only the selected unit(s) from a JSON file
        /// (or `-` for stdin) of grep selection handles, leaving unrelated
        /// worktree edits untouched. Preview by default; `-- git commit
        /// options` stages just the selected patch as a projected
        /// (partial) frame.
        #[arg(long, value_name = "HANDLE_JSON", conflicts_with = "range")]
        selection: Option<String>,
        /// Options forwarded verbatim to `git commit` — only after `--`.
        #[arg(last = true, value_name = "GIT_OPTS")]
        git_args: Vec<String>,
        #[arg(long, short = 'C')]
        project: Option<PathBuf>,
        /// Emit the IPC-compatible JSON result.
        #[arg(long)]
        json: bool,
    },
    /// Materialize and list live physical worktrees for timeline branches.
    Worktree {
        #[command(subcommand)]
        command: WorktreeCmd,
    },
    /// Squash a divergent timeline source onto the active worktree.
    ///
    /// Preview by default. `--apply` executes the exact plan token returned
    /// by the daemon; conflicting paths leave the worktree untouched.
    #[command(after_help = "\
EXAMPLES:
    sheaf merge checkpoint:experiment          preview source → this worktree
    sheaf merge checkpoint:experiment --apply  apply as one capture
    sheaf merge --resume                       finish an interrupted merge")]
    Merge {
        /// Source capture, checkpoint, or branch-tip reference.
        source: Option<String>,
        /// Apply the previewed plan.
        #[arg(long, conflicts_with = "resume")]
        apply: bool,
        /// Resume a crash-interrupted merge intent.
        #[arg(long, conflicts_with_all = ["source", "apply"])]
        resume: bool,
        #[arg(long, short = 'C')]
        project: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },

    /// Manage the per-user service unit (systemd user session).
    Service {
        #[command(subcommand)]
        command: ServiceCmd,
    },
    /// Manage the derived grep cache (disposable; timeline stays
    /// authoritative regardless of its state).
    Cache {
        #[command(subcommand)]
        command: CacheCmd,
    },
}

#[derive(Subcommand)]
enum WorktreeCmd {
    /// List the primary and every linked worktree.
    List {
        #[arg(long, short = 'C')]
        project: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Materialize one timeline point as a live linked worktree.
    Add {
        /// Capture, checkpoint, or branch-tip reference to materialize.
        reference: String,
        /// New directory. It must not already exist or overlap another worktree.
        destination: PathBuf,
        #[arg(long, short = 'C')]
        project: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
}


#[derive(Subcommand)]
enum CacheCmd {
    /// Index every retained capture's touched paths into the grep cache.
    ///
    /// Idempotent: captures that already have complete rows publish
    /// nothing. Old histories become fast as they are searched; this makes
    /// the whole current lineage fast up front. Also indexes divergent
    /// branches with --all.
    #[command(after_help = "\
EXAMPLES:
    sheaf cache backfill                 index the current lineage
    sheaf cache backfill --all           also index divergent branches
    sheaf cache backfill --limit 200     index at most 200 captures now")]
    Backfill {
        /// Also index captures exclusive to divergent branches.
        #[arg(long)]
        all: bool,
        /// Index at most this many not-yet-complete captures this run
        /// (already-complete captures do not count).
        #[arg(long, value_parser = clap::value_parser!(u32).range(1..=100_000))]
        limit: Option<u32>,
        #[arg(long, short = 'C')]
        project: Option<PathBuf>,
        /// Emit the IPC-compatible JSON result.
        #[arg(long)]
        json: bool,
    },
    /// Wipe the cache and backfill from scratch (bumps the generation).
    ///
    /// The repair verb for cache damage reported by `sheaf doctor`: torn
    /// mapping lines, orphaned or corrupt content, and stale watermarks
    /// are all gone afterwards because every row is republished from
    /// authoritative history.
    Rebuild {
        /// Also index captures exclusive to divergent branches.
        #[arg(long)]
        all: bool,
        #[arg(long, short = 'C')]
        project: Option<PathBuf>,
        /// Emit the IPC-compatible JSON result.
        #[arg(long)]
        json: bool,
    },
}

#[derive(Subcommand)]
enum ServiceCmd {
    /// Write the user unit file and enable the daemon at login.
    Install {
        /// Write the unit but do not start/enable it now.
        #[arg(long)]
        no_start: bool,
    },
    /// Show unit presence and daemon activity.
    Status,
    /// Stop, disable, and remove the unit file.
    Remove,
}

#[derive(Subcommand)]
enum CheckpointCmd {
    /// List checkpoints.
    List {
        #[arg(long, short = 'C')]
        project: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    /// Bind a new name to an exact timeline point.
    Create {
        name: String,
        /// Capture ID, checkpoint reference, @, or @~N (default: @).
        #[arg(long)]
        at: Option<String>,
        #[arg(long, short = 'C')]
        project: Option<PathBuf>,
    },
}

/// When to color human-facing output. `auto` means: a terminal, and no
/// `NO_COLOR` (the de-facto no-color convention, empty value allowed).
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum ColorWhen {
    Auto,
    Always,
    Never,
}

/// Restorable extent a grep hit resolves to: the matched span alone, or the
/// whole line containing it.
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum GrepExtentArg {
    Match,
    Line,
}

impl GrepExtentArg {
    fn to_extent(self) -> sheaf_core::store::SelectionExtent {
        match self {
            GrepExtentArg::Match => sheaf_core::store::SelectionExtent::Match,
            GrepExtentArg::Line => sheaf_core::store::SelectionExtent::Line,
        }
    }
}

impl Cmd {
    fn outputs_json(&self) -> bool {
        match self {
            Cmd::Log { json, .. }
            | Cmd::Info { json, .. }
            | Cmd::Diff { json, .. }
            | Cmd::Grep { json, .. }
            | Cmd::Doctor { json, .. }
            | Cmd::Gc { json, .. }
            | Cmd::Restore { json, .. }
            | Cmd::Squash { json, .. }
            | Cmd::Merge { json, .. } => *json,
            Cmd::Worktree {
                command: WorktreeCmd::List { json, .. } | WorktreeCmd::Add { json, .. },
            } => *json,
            Cmd::Checkpoint {
                command: Some(CheckpointCmd::List { json, .. }),
                ..
            } => *json,
            _ => false,
        }
    }
}

impl ColorWhen {
    fn enabled(self, is_tty: bool) -> bool {
        match self {
            ColorWhen::Always => true,
            ColorWhen::Never => false,
            ColorWhen::Auto => is_tty && std::env::var_os("NO_COLOR").is_none_or(|v| v.is_empty()),
        }
    }
}

/// Wrap `text` in an ANSI SGR `code` when coloring is on. Width math is done
/// on the plain strings by callers; only the final assembly paints.
fn paint(on: bool, code: &str, text: &str) -> String {
    if on {
        format!("\x1b[{code}m{text}\x1b[0m")
    } else {
        text.to_owned()
    }
}

/// Terminal width in columns: `$COLUMNS`, then the TIOCGWINSZ ioctl, then
/// the classic 80. Piped output has no geometry — 80 keeps lines copyable.
fn terminal_width() -> usize {
    if let Some(w) =
        std::env::var_os("COLUMNS").and_then(|v| v.to_str().and_then(|s| s.parse::<usize>().ok()))
    {
        if w > 0 {
            return w;
        }
    }
    #[cfg(unix)]
    unsafe {
        let mut ws: libc::winsize = std::mem::zeroed();
        if libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut ws) == 0 && ws.ws_col > 0 {
            return ws.ws_col as usize;
        }
    }
    80
}

/// Fit as many paths as `budget` cells allow, folding the overflow into the
/// existing `… +N more` marker. At least one path always shows (an
/// over-long single path is left for the terminal to wrap).
fn fit_paths(paths: &[String], budget: usize) -> String {
    if paths.is_empty() {
        return "(metadata)".to_owned();
    }
    if budget < 16 {
        // Degenerate width: keep the marker, don't fight the terminal.
        return format!("{}, … +{} more", paths[0], paths.len() - 1);
    }
    // Cell-accurate lengths (`…` is one cell, three bytes).
    let cells = |s: &str| s.chars().count();
    let suffix = |hidden: usize| cells(&format!(", … +{hidden} more"));
    let mut shown = 1usize;
    let mut acc = cells(&paths[0]);
    for k in 1..paths.len() {
        let next = acc + 2 + cells(&paths[k]);
        let hidden_after = paths.len() - (k + 1);
        let suffix_len = if hidden_after > 0 {
            suffix(hidden_after)
        } else {
            0
        };
        if next + suffix_len > budget {
            break;
        }
        acc = next;
        shown = k + 1;
    }
    let hidden = paths.len() - shown;
    if hidden == 0 {
        paths.join(", ")
    } else {
        format!("{}, … +{} more", paths[..shown].join(", "), hidden)
    }
}

fn main() -> ExitCode {
    // Piped into `head`, a paginated viewer, or an editor, stdout closes
    // early; Rust ignores SIGPIPE by default, which turns that into an
    // EPIPE panic deep inside println!. Restore the Unix default so `sheaf`
    // dies quietly with SIGPIPE, exactly like every other filter.
    #[cfg(unix)]
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .init();

    let Args { cmd, color } = Args::parse();
    // A command-wide base color makes every human-oriented command visibly
    // consistent. Individual timeline fields layer their semantic colors on
    // top; JSON is deliberately never decorated.
    let color_on = color.enabled(std::io::stdout().is_terminal()) && !cmd.outputs_json();
    if color_on {
        print!("\x1b[36m");
    }
    let result = match cmd {
        Cmd::Init { path } => cmd_init(path.as_deref()),
        Cmd::Status { path } => cmd_status(path.as_deref()),
        Cmd::Log {
            project,
            path,
            follow,
            all,
            before,
            limit,
            json,
        } => cmd_log(
            project.as_deref(),
            path.as_deref(),
            follow,
            all,
            before.as_deref(),
            limit as usize,
            json,
            color,
        ),
        Cmd::Info {
            reference,
            project,
            json,
        } => cmd_info(project.as_deref(), &reference, json, color),
        Cmd::Grep {
            query,
            history,
            at,
            path,
            line,
            column,
            episode,
            selection,
            follow,
            all,
            every_capture,
            extent,
            from,
            to,
            after,
            max_results,
            project,
            json,
        } => cmd_grep(GrepArgs {
            project: project.as_deref(),
            query: &query,
            history,
            at: at.as_deref(),
            path: path.as_deref(),
            line,
            column,
            episode: episode.as_deref(),
            selection: selection.as_deref(),
            follow,
            all,
            every_capture,
            extent: extent.to_extent(),
            from: from.as_deref(),
            to: to.as_deref(),
            after: after.as_deref(),
            max_results: max_results as usize,
            as_json: json,
            color,
        }),
        Cmd::Diff {
            from,
            to,
            paths,
            stat,
            json,
            exit_code,
            project,
        } => cmd_diff(
            project.as_deref(),
            from.as_deref(),
            to.as_deref(),
            &paths,
            stat,
            json,
            exit_code,
        ),
        Cmd::Checkpoint {
            command,
            name,
            at,
            project,
        } => match command {
            Some(CheckpointCmd::List { project, json }) => {
                cmd_checkpoint_list(project.as_deref(), json, color)
            }
            Some(CheckpointCmd::Create { name, at, project }) => {
                cmd_checkpoint_create(project.as_deref(), &name, at.as_deref())
            }
            // Bare-name shorthand: `sheaf checkpoint <name>` == `create <name>`.
            None => match name {
                Some(name) => cmd_checkpoint_create(project.as_deref(), &name, at.as_deref()),
                None => {
                    eprintln!("sheaf: checkpoint needs a name (e.g. `sheaf checkpoint \"before refactor\"`) or a subcommand (`create`/`list`)");
                    Err(ExitErr::SilentCode(2))
                }
            },
        },
        Cmd::Restore {
            args,
            at,
            dry_run,
            resume,
            abandon,
            selection,
            insert,
            delete,
            project,
            json,
        } => {
            if resume {
                cmd_restore_resume(project.as_deref(), json)
            } else if abandon {
                cmd_restore_abandon(project.as_deref(), json)
            } else if let Some(source) = &selection {
                let mode = if insert {
                    sheaf_core::store::FragmentMode::Insert
                } else if delete {
                    sheaf_core::store::FragmentMode::Delete
                } else {
                    sheaf_core::store::FragmentMode::Replace
                };
                cmd_fragment_restore(project.as_deref(), source, mode, dry_run, json)
            } else {
                cmd_restore(project.as_deref(), &args, at.as_deref(), dry_run, json)
            }
        }
        Cmd::Doctor { project, json, fix } => cmd_doctor(project.as_deref(), json, fix),
        Cmd::Gc {
            apply,
            project,
            json,
            set_expiry,
            mark,
        } => cmd_gc(project.as_deref(), apply, json, set_expiry, mark),
        Cmd::Squash {
            range,
            selection,
            git_args,
            project,
            json,
        } => match selection.as_deref() {
            Some(spec) => cmd_smart_squash(project.as_deref(), spec, &git_args, json),
            None => cmd_squash(project.as_deref(), range.as_deref(), &git_args, json),
        },
        Cmd::Worktree { command } => match command {
            WorktreeCmd::List { project, json } => {
                cmd_worktree_list(project.as_deref(), json)
            }
            WorktreeCmd::Add {
                reference,
                destination,
                project,
                json,
            } => cmd_worktree_add(
                project.as_deref(),
                &reference,
                &destination,
                json,
            ),
        },
        Cmd::Merge {
            source,
            apply,
            resume,
            project,
            json,
        } => cmd_merge(
            project.as_deref(),
            source.as_deref(),
            apply,
            resume,
            json,
        ),

        Cmd::Service { command } => match command {
            ServiceCmd::Install { no_start } => cmd_service_install(no_start),
            ServiceCmd::Status => cmd_service_status(),
            ServiceCmd::Remove => cmd_service_remove(),
        },
        Cmd::Cache { command } => match command {
            CacheCmd::Backfill {
                all,
                limit,
                project,
                json,
            } => cmd_cache_backfill(project.as_deref(), all, false, limit, json),
            CacheCmd::Rebuild { all, project, json } => {
                cmd_cache_backfill(project.as_deref(), all, true, None, json)
            }
        },
    };
    if color_on {
        print!("\x1b[0m");
    }
    result
        .map(|()| ExitCode::SUCCESS)
        .unwrap_or_else(|e| match e {
            ExitErr::SilentCode(n) => ExitCode::from(n),
            ExitErr::Fatal(err) => {
                eprintln!("sheaf: {err:#}");
                ExitCode::FAILURE
            }
        })
}

enum ExitErr {
    SilentCode(u8),
    Fatal(anyhow::Error),
}
impl From<anyhow::Error> for ExitErr {
    fn from(e: anyhow::Error) -> Self {
        ExitErr::Fatal(e)
    }
}
impl From<sheaf_core::SheafError> for ExitErr {
    fn from(e: sheaf_core::SheafError) -> Self {
        ExitErr::Fatal(e.into())
    }
}
type CliResult = Result<(), ExitErr>;

fn cmd_init(path: Option<&Path>) -> CliResult {
    let target = match path {
        Some(p) => p.to_path_buf(),
        None => std::env::current_dir().context("no current directory")?,
    };

    let report = init_project(&target, InitOptions::default())?;

    println!("root:          {}", report.root.display());
    if report.store_created {
        println!(
            "store:         created (.sheaf/ v{})",
            config::STORE_FORMAT_VERSION
        );
    } else if report.reused_ancestor {
        println!("store:         reused ancestor store");
    } else {
        println!("store:         already initialized");
    }
    if report.git_exclude_updated {
        println!("git:           added .sheaf/ to .git/info/exclude");
    }
    println!(
        "enrollment:    {}",
        if report.newly_enrolled {
            "registered"
        } else {
            "already registered"
        }
    );
    if report.daemon_notified {
        println!("daemon:        notified — watching live");
    } else {
        for n in &report.notes {
            println!("note:          {n}");
        }
    }
    Ok(())
}

fn cmd_status(path: Option<&Path>) -> CliResult {
    let start = match path {
        Some(p) => p.to_path_buf(),
        None => std::env::current_dir().context("no current directory")?,
    };

    // The README's Quick Start opens with `sheaf status` *before* any project
    // is enrolled, to answer "is the daemon up?". So when no project root is
    // found we do not error out — we report daemon health alone and point the
    // user at `sheaf init`. An explicit `-C <path>` that resolves nothing is
    // still that: no project, daemon-only status.
    let root = resolve_project_root(&start).map(|r| normalize_existing(&r));
    let Some(root) = root else {
        println!(
            "project:       (none — no .sheaf/config.toml above {})",
            start.display()
        );
        report_daemon_status(None);
        println!("hint:          run `sheaf init` in a project to start recording it");
        return Ok(());
    };
    let format = config::read_store_format(&root).ok();
    let cfg_ok = config::load(&root).ok();

    println!("project:       {}", root.display());
    println!(
        "store:         format {}",
        format.map(|f| f.to_string()).unwrap_or_else(|| "?".into())
    );
    if let Some(cfg) = &cfg_ok {
        println!(
            "watch config:  debounce={}ms ignore-patterns={} tracked-text-limit={}MiB snapshot-every={}",
            cfg.watch.debounce_ms,
            cfg.ignore.patterns.len(),
            cfg.watch.max_tracked_bytes / (1024 * 1024),
            match cfg.store.snapshot_edit_size {
                0 => "off".to_string(),
                n => format!("{n} edits ([store] in config.toml)"),
            }
        );
        match &cfg.retention.expiry {
            Some(spec) => println!(
                "retention:     edits expire after {spec} (reachability-bound; `sheaf gc --apply` reclaims)"
            ),
            None => println!(
                "retention:     no expiry — history is kept whole (`sheaf gc --set-expiry 30d` to bound it)"
            ),
        }
    }

    match Registry::global() {
        Ok(reg) => {
            let listed = reg
                .list()
                .is_ok_and(|l| l.iter().any(|e| normalize_existing(&e.root) == root));
            println!("enrolled:      {}", yn(listed));
        }
        Err(_) => println!("enrolled:      ? (registry unavailable)"),
    }

    report_daemon_status(Some(&root));
    Ok(())
}

/// Print daemon health, and — when a project `root` is given — its watch
/// state, warm-up, and any pending restore for that project. With `root` as
/// `None` (no enrolled project) only daemon presence is reported, so the
/// README's pre-`init` `sheaf status` still answers "is the daemon up?".
fn report_daemon_status(root: Option<&Path>) {
    let socket = sheaf_core::paths::control_socket_path();
    match Client::connect(&socket, Duration::from_millis(600)) {
        Ok(mut c) => match c.ping() {
            Ok((major, minor, ver)) => {
                println!("daemon:        running v{ver} (proto {major}.{minor})");
                let Some(root) = root else {
                    return;
                };
                let status = c
                    .call("project.status", Some(root), serde_json::json!({}), None)
                    .ok()
                    .filter(|reply| reply.response.ok)
                    .and_then(|reply| reply.response.result);
                let watching = status
                    .as_ref()
                    .and_then(|value| value.get("watching"))
                    .and_then(serde_json::Value::as_bool);
                let ready = status
                    .as_ref()
                    .and_then(|value| value.get("ready"))
                    // Older daemons have no warm-up state and are ready once
                    // they report watching.
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(watching == Some(true));
                let cold = status
                    .as_ref()
                    .and_then(|value| value.get("cold"))
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false);
                println!("watching:      {}", yn(watching == Some(true)));
                if watching == Some(true) && cold {
                    println!("idle:          yes (store opens on first activity)");
                } else if watching == Some(true) && !ready {
                    println!("initializing:  yes (background worktree capture in progress)");
                }
                if let Some(pending) = pending_restore_note(&mut c, root) {
                    println!("pending:       {pending}");
                    println!("hint:          a restore was interrupted; it finishes on the next daemon start");
                }
                if watching != Some(true) {
                    println!("hint:          not currently watched — restart the daemon or re-run `sheaf init`");
                }
            }
            Err(e) => {
                println!("daemon:        unreachable ({e})");
                degraded_note();
            }
        },
        Err(_) => {
            println!("daemon:        not running ({})", socket.display());
            degraded_note();
        }
    }
}

/// `project.status` reports an unfinished restore; a wedged one is otherwise
/// invisible — the worktree just looks inexplicably odd.
fn pending_restore_note(client: &mut Client, root: &Path) -> Option<String> {
    let reply = client
        .call("project.status", Some(root), serde_json::json!({}), None)
        .ok()?;
    let pending = reply
        .response
        .result
        .as_ref()?
        .get("pending_restore")?
        .clone();
    if pending.is_null() {
        return None;
    }
    let target = pending
        .get("target")
        .and_then(|t| t.get("capture_id"))
        .and_then(serde_json::Value::as_str)
        .map(short)
        .unwrap_or("(frontier)")
        .to_owned();
    Some(format!("restore to {target} did not finish"))
}

fn timeline_root(path: Option<&Path>) -> Result<PathBuf, ExitErr> {
    let start = match path {
        Some(p) => p.to_path_buf(),
        None => std::env::current_dir().context("no current directory")?,
    };
    match resolve_project_root(&start) {
        Some(root) => Ok(normalize_existing(&root)),
        None => {
            eprintln!("sheaf: no project root found above {}", start.display());
            Err(ExitErr::SilentCode(3))
        }
    }
}

// Flat view options mirror the flat CLI flags; eight is the honest shape.
#[allow(clippy::too_many_arguments)]
fn cmd_log(
    project: Option<&Path>,
    path: Option<&Path>,
    follow: bool,
    all: bool,
    before: Option<&str>,
    limit: usize,
    as_json: bool,
    color: ColorWhen,
) -> CliResult {
    let root = timeline_root(project)?;
    let params = serde_json::json!({
        "path": path.map(|p| p.to_string_lossy().to_string()),
        "follow": follow,
        "all": all,
        "before": before,
        "limit": limit,
    });
    let socket = sheaf_core::paths::control_socket_path();
    let (entries, tips, degraded) = match Client::connect(&socket, Duration::from_secs(2)) {
        Ok(mut client) => {
            let reply = client.call("timeline.log", Some(&root), params, None)?;
            if !reply.response.ok {
                return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into());
            }
            let value = reply.response.result.unwrap_or_default();
            let entries = serde_json::from_value(value.get("entries").cloned().unwrap_or_default())
                .context("daemon returned invalid timeline entries")?;
            let tips = value
                .get("tips")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(1) as usize;
            (entries, tips, false)
        }
        Err(_) => {
            let _guard = shared_read_guard(&root)?;
            let reader = sheaf_core::store::TimelineReader::open(&root)?;
            let mut entries = reader.captures(
                all,
                path,
                follow,
                if before.is_some() { usize::MAX } else { limit },
            )?;
            let tips = reader.branch_tips().map(|t| t.len()).unwrap_or(1);
            if let Some(cursor) = before {
                if cursor.len() < 6 || !cursor.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                    return Err(anyhow::anyhow!(
                        "timeline cursors require at least 6 hexadecimal capture-ID characters"
                    )
                    .into());
                }
                let resolved = reader.resolve(cursor)?;
                let id = resolved
                    .capture_id
                    .ok_or_else(|| anyhow::anyhow!("cursor `{cursor}` does not name a capture"))?;
                let pos = entries
                    .iter()
                    .position(|entry| entry.id == id)
                    .ok_or_else(|| {
                        anyhow::anyhow!("timeline cursor `{cursor}` is outside this view")
                    })?;
                entries.drain(..=pos);
                entries.truncate(limit);
            }
            (entries, tips, true)
        }
    };
    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(
                &serde_json::json!({"entries": entries, "tips": tips, "degraded": degraded})
            )
            .context("serialize timeline")?
        );
    } else {
        if degraded {
            eprintln!("note: daemon unavailable; showing a read-only store snapshot");
        }
        // Oldest → newest: the latest change lands at the bottom of the
        // terminal where scrollback already put the user's eyes.
        let is_tty = std::io::stdout().is_terminal();
        let color_on = color.enabled(is_tty);
        let width = terminal_width();
        // Fixed columns before the path list: marker, space, 12-char id,
        // two spaces, 19-char timestamp, two spaces.
        const LINE_OVERHEAD: usize = 37;
        for entry in entries.iter().rev() {
            let utc = DateTime::<Utc>::from_timestamp_millis(entry.timestamp_ms)
                .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
            let local: DateTime<Local> = utc.into();
            let time = local.format("%Y-%m-%d %H:%M:%S").to_string();
            let origin = origin_suffix(entry.origin.as_ref());
            // Width-fitting exists to protect a terminal; piped output has
            // no width to overflow, and greppers need whole path lists.
            let budget = if is_tty {
                width
                    .saturating_sub(LINE_OVERHEAD + origin.chars().count())
                    .max(16)
            } else {
                usize::MAX
            };
            let paths = fit_paths(&entry.paths, budget);
            // The marker column is the branch-aware part of the view: `*` is
            // the live lineage the worktree sits on, `+` a divergent branch
            // (an abandoned future, usually) that only --all surfaces.
            let marker = if all && !entry.on_current { '+' } else { '*' };
            println!(
                "{} {}  {}  {}{}",
                marker,
                paint(color_on, "36", entry.short_id()),
                paint(color_on, "2", &time),
                paths,
                origin
            );
            if !entry.checkpoints.is_empty() {
                println!(
                    "  {} {}",
                    paint(color_on, "35", "checkpoint:"),
                    entry.checkpoints.join(", ")
                );
            }
        }
        if !all && tips > 1 {
            eprintln!(
                "note: {tips} divergent branch tips exist; `sheaf log --all` lists every capture"
            );
        }
    }
    Ok(())
}

// -------------------------------------------------------------------- info

fn cmd_info(project: Option<&Path>, reference: &str, as_json: bool, color: ColorWhen) -> CliResult {
    let root = timeline_root(project)?;
    let socket = sheaf_core::paths::control_socket_path();
    let (info, degraded): (sheaf_core::store::CaptureInfo, bool) =
        match Client::connect(&socket, Duration::from_secs(2)) {
            Ok(mut client) => {
                let reply = client.call(
                    "timeline.info",
                    Some(&root),
                    serde_json::json!({"reference": reference}),
                    None,
                )?;
                if !reply.response.ok {
                    return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into());
                }
                let value = reply.response.result.unwrap_or_default();
                let info = serde_json::from_value(value.get("info").cloned().unwrap_or_default())
                    .context("daemon returned invalid capture info")?;
                (info, false)
            }
            Err(_) => {
                let _guard = shared_read_guard(&root)?;
                (
                    sheaf_core::store::TimelineReader::open(&root)?.capture_info(reference)?,
                    true,
                )
            }
        };
    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({"info": info, "degraded": degraded}))
                .context("serialize capture info")?
        );
        return Ok(());
    }
    if degraded {
        eprintln!("note: daemon unavailable; showing a read-only store snapshot");
    }
    let utc = DateTime::<Utc>::from_timestamp_millis(info.capture.timestamp_ms)
        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
    let local: DateTime<Local> = utc.into();
    let color_on = color.enabled(std::io::stdout().is_terminal());
    println!(
        "* {}  {}",
        paint(color_on, "36", info.capture.short_id()),
        paint(
            color_on,
            "2",
            &local.format("%Y-%m-%d %H:%M:%S").to_string()
        ),
    );
    for change in info.diff.entries {
        use sheaf_core::store::DiffKind;
        let (mark, name, ansi) = match change.kind {
            DiffKind::Added => ('+', change.path, "32"),
            DiffKind::Deleted => ('-', change.path, "31"),
            DiffKind::Renamed => (
                '~',
                format!("{} => {}", change.old_path.unwrap_or_default(), change.path),
                "33",
            ),
            DiffKind::Modified | DiffKind::TypeChanged => ('~', change.path, "33"),
        };
        println!("  {} {}", paint(color_on, ansi, &mark.to_string()), name);
    }
    Ok(())
}

// -------------------------------------------------------------------- diff

/// Exit code for `--exit-code` when differences exist (git parity).
const EXIT_DIFFERS: u8 = 1;

fn cmd_diff(
    project: Option<&Path>,
    from: Option<&str>,
    to: Option<&str>,
    raw_paths: &[PathBuf],
    stat: bool,
    as_json: bool,
    exit_code: bool,
) -> CliResult {
    let root = timeline_root(project)?;
    // `A..B` packs two points into one argument.
    let (from, to) = match (from, to) {
        (Some(range), None) if let Some((a, b)) = range.split_once("..") => {
            let (a, b) = (a.trim(), b.trim());
            if a.is_empty() || b.is_empty() {
                return Err(
                    anyhow::anyhow!("`{range}` needs a point on both sides of `..`").into(),
                );
            }
            (a.to_string(), Some(b.to_string()))
        }
        _ => (from.unwrap_or("@").to_string(), to.map(str::to_string)),
    };

    // Relative --path values mean what the user sees from the invocation
    // directory, exactly like restore scopes.
    let cwd = std::env::current_dir()
        .map(|dir| normalize_existing(&dir))
        .context("no current directory")?;
    let base = if cwd.starts_with(&root) {
        cwd
    } else {
        root.clone()
    };
    let mut scope = Vec::new();
    for raw in raw_paths {
        scope.push(sheaf_core::store::scope_key(
            &root,
            &base,
            &raw.to_string_lossy(),
        )?);
    }
    if scope.iter().any(String::is_empty) {
        scope.clear();
    }

    let socket = sheaf_core::paths::control_socket_path();
    let params = serde_json::json!({"from": from, "to": to, "paths": scope});
    let (outcome, patch) = match Client::connect(&socket, Duration::from_secs(2)) {
        Ok(mut client) => {
            // Diffing a large tree can legitimately compute past handshake
            // speed; keep the quick connect but give the work room (the
            // daemon allows itself 30s for the same reason).
            client.set_timeout(Duration::from_secs(35))?;
            let reply = client.call("diff", Some(&root), params, None)?;
            if !reply.response.ok {
                return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into());
            }
            let value = reply.response.result.unwrap_or_default();
            let outcome: sheaf_core::store::DiffOutcome =
                serde_json::from_value(value.get("diff").cloned().unwrap_or_default())
                    .context("daemon returned an invalid diff")?;
            let patch = String::from_utf8(reply.body)
                .map_err(|_| anyhow::anyhow!("daemon returned a non-UTF-8 patch"))?;
            (outcome, patch)
        }
        Err(_) => {
            let _guard = shared_read_guard(&root)?;
            let patterns = config::load(&root)
                .map(|cfg| cfg.ignore.patterns)
                .unwrap_or_else(|_| config::default_patterns());
            let ignore = sheaf_core::ignore::IgnoreSet::for_project(&root, &patterns)
                .map_err(|e| anyhow::anyhow!("bad ignore patterns: {e}"))?;
            let reader = sheaf_core::store::TimelineReader::open(&root)?;
            let outcome = reader.diff(&from, to.as_deref(), &scope, &ignore)?;
            let patch = String::from_utf8_lossy(&outcome.render_patch()).into_owned();
            (outcome, patch)
        }
    };
    if outcome.degraded {
        eprintln!("note: daemon unavailable; this diff reads a read-only store snapshot");
    }

    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "diff": outcome,
                "patch": patch,
            }))
            .context("serialize diff")?
        );
    } else if stat {
        print_diff_stat(&outcome);
    } else if !patch.is_empty() {
        std::io::stdout()
            .write_all(patch.as_bytes())
            .context("write patch")?;
    }

    if exit_code && !outcome.is_empty() {
        return Err(ExitErr::SilentCode(EXIT_DIFFERS));
    }
    Ok(())
}

struct GrepArgs<'a> {
    project: Option<&'a Path>,
    query: &'a str,
    history: bool,
    at: Option<&'a str>,
    path: Option<&'a Path>,
    line: Option<usize>,
    column: Option<usize>,
    episode: Option<&'a str>,
    selection: Option<&'a str>,
    follow: bool,
    all: bool,
    every_capture: bool,
    extent: sheaf_core::store::SelectionExtent,
    from: Option<&'a str>,
    to: Option<&'a str>,
    after: Option<&'a str>,
    max_results: usize,
    as_json: bool,
    color: ColorWhen,
}

/// Build the history anchor from the CLI flags. Exactly one form may apply;
/// clap already enforces the flag-level requirements and conflicts, so this
/// only rejects the mixed leftovers a hand-built invocation could produce.
fn grep_anchor(args: &GrepArgs<'_>) -> Result<Option<sheaf_core::store::GrepAnchor>, ExitErr> {
    let forms = [
        args.line.is_some(),
        args.selection.is_some(),
        args.episode.is_some(),
    ]
    .iter()
    .filter(|flag| **flag)
    .count();
    if forms > 1 {
        return Err(ExitErr::Fatal(anyhow::anyhow!(
            "--line, --selection, and --episode are mutually exclusive anchor forms"
        )));
    }
    if let Some(episode) = args.episode {
        return Ok(Some(sheaf_core::store::GrepAnchor::Episode {
            episode_id: episode.to_owned(),
        }));
    }
    if let Some(line) = args.line {
        // clap guarantees path presence for --line.
        let path = args
            .path
            .ok_or_else(|| anyhow::anyhow!("--line requires --path"))?;
        return Ok(Some(sheaf_core::store::GrepAnchor::Coordinate {
            path: path.to_string_lossy().to_string(),
            line,
            column: args.column,
        }));
    }
    if let Some(selection) = args.selection {
        let raw = if selection == "-" {
            let mut buffer = String::new();
            std::io::Read::read_to_string(&mut std::io::stdin(), &mut buffer)
                .context("reading the selection anchor from stdin")?;
            buffer
        } else {
            std::fs::read_to_string(selection)
                .with_context(|| format!("reading the selection anchor {selection}"))?
        };
        let value: serde_json::Value =
            serde_json::from_str(&raw).context("the selection anchor must be JSON")?;
        // Accept a bare handle, a full grep hit object, or a whole NDJSON
        // hit record; anything else is a user error, not a guess.
        let handle_value = value
            .get("hit")
            .and_then(|hit| hit.get("handle"))
            .or_else(|| value.get("handle"))
            .unwrap_or(&value);
        let handle: sheaf_core::store::SelectionHandle =
            serde_json::from_value(handle_value.clone())
                .context("the selection anchor must be a grep hit or a bare selection handle")?;
        return Ok(Some(sheaf_core::store::GrepAnchor::Selection {
            handle: Box::new(handle),
        }));
    }
    Ok(None)
}

fn parse_grep_after(after: &str) -> Result<(String, Option<String>, usize), ExitErr> {
    let parts: Vec<&str> = after.split(':').collect();
    let parsed = match parts.as_slice() {
        [capture] => ((*capture).to_owned(), None, 0usize),
        [resume, index] => (
            "@before-first".to_owned(),
            Some((*resume).to_owned()),
            index.parse::<usize>().map_err(|_| {
                ExitErr::Fatal(anyhow::anyhow!(
                    "grep --after record index must be an unsigned integer"
                ))
            })?,
        ),
        [processed, resume, index] => (
            (*processed).to_owned(),
            Some((*resume).to_owned()),
            index.parse::<usize>().map_err(|_| {
                ExitErr::Fatal(anyhow::anyhow!(
                    "grep --after record index must be an unsigned integer"
                ))
            })?,
        ),
        _ => {
            return Err(ExitErr::Fatal(anyhow::anyhow!(
                "grep --after expects CAPTURE, RESUME:INDEX, or AFTER:RESUME:INDEX"
            )));
        }
    };
    Ok(parsed)
}

fn grep_cursor_value(
    args: &GrepArgs<'_>,
    anchor: Option<&sheaf_core::store::GrepAnchor>,
    after: &str,
) -> Result<serde_json::Value, ExitErr> {
    let (after_capture_id, resume_capture_id, record_index) = parse_grep_after(after)?;
    Ok(serde_json::json!({
        "query_fingerprint": grep_fingerprint(args, anchor),
        "after_capture_id": after_capture_id,
        "resume_capture_id": resume_capture_id,
        "record_index": record_index,
        "path_index": 0,
        "match_index": 0,
    }))
}

fn cmd_grep(args: GrepArgs) -> CliResult {
    let root = timeline_root(args.project)?;
    // Parsed once: a `--selection -` anchor reads stdin, and re-parsing
    // for the degraded fallback or a cursor fingerprint would hit EOF.
    let anchor = grep_anchor(&args)?;
    let cursor = args
        .after
        .map(|after| grep_cursor_value(&args, anchor.as_ref(), after))
        .transpose()?;
    let params = serde_json::json!({
        "query": {"kind": "literal", "text": args.query},
        "mode": if args.history { "history" } else { "point" },
        "at": args.at,
        "anchor": anchor,
        "from": args.from,
        "to": args.to,
        "path": args.path.map(|p| p.to_string_lossy().to_string()),
        "follow": args.follow,
        "all": args.all,
        "every_capture": args.every_capture,
        "extent": extent_wire(args.extent),
        "budget": {
            "max_results": args.max_results,
            "max_materialized_bytes": 64u64 * 1024 * 1024,
            "max_elapsed_ms": 5000u64,
        },
        "cursor": cursor,
    });

    // Records print as the walk finalizes them (GNU-grep liveness); the
    // report tail prints when the stream ends. Both transports stream:
    // daemon-served via streamed body frames, degraded via the engine's
    // record callback.
    let mut printer = GrepStreamPrinter::new(args.as_json, args.color);
    let mut terminal: Option<serde_json::Value> = None;
    let mut record = |printer: &mut GrepStreamPrinter, value: &serde_json::Value| {
        match value.get("type").and_then(serde_json::Value::as_str) {
            Some("hit") => {
                if let Ok(hit) = serde_json::from_value::<sheaf_core::store::GrepHit>(
                    value.get("hit").cloned().unwrap_or_default(),
                ) {
                    printer
                        .record(&sheaf_core::store::GrepStreamRecord::Hit { hit: Box::new(hit) });
                }
            }
            Some("event") => {
                if let Ok(event) = serde_json::from_value::<sheaf_core::store::GrepEvent>(
                    value.get("event").cloned().unwrap_or_default(),
                ) {
                    printer.record(&sheaf_core::store::GrepStreamRecord::Event { event });
                }
            }
            // summary/error terminate the stream body; parsed below.
            Some("summary") | Some("error") => terminal = Some(value.clone()),
            _ => {}
        }
    };

    let socket = sheaf_core::paths::control_socket_path();
    let report: sheaf_core::store::GrepReport = match Client::connect(
        &socket,
        Duration::from_secs(2),
    ) {
        Ok(mut client) => {
            client.set_timeout(Duration::from_secs(35))?;
            let ping = client.call("ping", Some(&root), serde_json::json!({}), None)?;
            let capabilities = ping
                .response
                .result
                .as_ref()
                .and_then(|result| result.get("capabilities"))
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();
            let has = |name: &str| capabilities.iter().any(|cap| cap.as_str() == Some(name));
            if !has("timeline.grep.occurrences") {
                return Err(anyhow::anyhow!(
                        "running sheafd lacks occurrence-centered grep; rebuild/reinstall and restart sheafd"
                    )
                    .into());
            }
            // An anchored request silently downgrades on an older daemon
            // (serde drops the unknown field), so it needs an explicit
            // capability instead of best-effort forwarding.
            if anchor.is_some() && !has("timeline.grep.anchors") {
                return Err(anyhow::anyhow!(
                    "running sheafd lacks occurrence anchors; rebuild/reinstall and restart sheafd"
                )
                .into());
            }
            let reply =
                client.call_streaming("timeline.grep", Some(&root), params, &mut |chunk| {
                    // One record per frame on a streamed body (proto 1.5);
                    // split defensively so a batched body still renders.
                    for line in chunk.split(|byte| *byte == b'\n') {
                        if line.is_empty() {
                            continue;
                        }
                        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(line) {
                            record(&mut printer, &value);
                        }
                    }
                })?;
            if !reply.response.ok {
                return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into());
            }
            match terminal.take() {
                Some(value) if value.get("type").and_then(|t| t.as_str()) == Some("error") => {
                    return Err(anyhow::anyhow!(
                        "grep stream failed: {}",
                        value
                            .get("message")
                            .and_then(|m| m.as_str())
                            .unwrap_or("unknown")
                    )
                    .into());
                }
                Some(value) => serde_json::from_value::<sheaf_core::store::GrepReport>(
                    value.get("report").cloned().unwrap_or_default(),
                )
                .context("daemon returned an invalid grep summary")?,
                // No terminal record: an older daemon answered buffered
                // (records in the body, summary in the envelope).
                None => {
                    let summary = reply.response.result.unwrap_or_default();
                    grep_report_from_wire(&summary, &reply.body)?
                }
            }
        }
        Err(_) => {
            let _guard = shared_read_guard(&root)?;
            let reader = sheaf_core::store::TimelineReader::open(&root)?;
            let request = grep_request(&args, anchor.clone())?;
            let mut sink = |rec: sheaf_core::store::GrepStreamRecord| {
                printer.record(&rec);
            };
            reader.grep_streaming(&request, &mut Some(&mut sink))?
        }
    };

    if report.degraded {
        eprintln!("note: daemon unavailable; this grep reads a read-only store snapshot");
    }
    printer.finish(&report);
    Ok(())
}

fn extent_wire(extent: sheaf_core::store::SelectionExtent) -> &'static str {
    match extent {
        sheaf_core::store::SelectionExtent::Match => "match",
        sheaf_core::store::SelectionExtent::Line => "line",
        sheaf_core::store::SelectionExtent::Hunk => "hunk",
        sheaf_core::store::SelectionExtent::Symbol => "symbol",
    }
}

fn grep_request(
    args: &GrepArgs<'_>,
    anchor: Option<sheaf_core::store::GrepAnchor>,
) -> Result<sheaf_core::store::GrepRequest, ExitErr> {
    let mut request = sheaf_core::store::GrepRequest {
        query: sheaf_core::store::GrepQuery::literal(args.query),
        mode: if args.history {
            sheaf_core::store::GrepMode::History
        } else {
            sheaf_core::store::GrepMode::Point
        },
        at: args.at.map(str::to_owned),
        from: args.from.map(str::to_owned),
        to: args.to.map(str::to_owned),
        path: args.path.map(|p| p.to_string_lossy().to_string()),
        follow: args.follow,
        all: args.all,
        every_capture: args.every_capture,
        extent: args.extent,
        budget: sheaf_core::store::SearchBudget {
            max_results: args.max_results,
            max_materialized_bytes: 64 * 1024 * 1024,
            max_elapsed_ms: 5000,
        },
        cursor: None,
        anchor,
    };
    if let Some(after) = args.after {
        let (after_capture_id, resume_capture_id, record_index) = parse_grep_after(after)?;
        request.cursor = Some(sheaf_core::store::SearchCursor {
            query_fingerprint: request.fingerprint(),
            after_capture_id,
            resume_capture_id,
            record_index,
            path_index: 0,
            match_index: 0,
        });
    }
    Ok(request)
}

fn grep_fingerprint(args: &GrepArgs<'_>, anchor: Option<&sheaf_core::store::GrepAnchor>) -> String {
    grep_request(args, anchor.cloned())
        .map(|r| r.fingerprint())
        .unwrap_or_default()
}

/// Reassemble the full report from the envelope summary plus the NDJSON body.
fn grep_report_from_wire(
    summary: &serde_json::Value,
    body: &[u8],
) -> Result<sheaf_core::store::GrepReport, ExitErr> {
    let mut hits = Vec::new();
    let mut events = Vec::new();
    for line in body.split(|b| *b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let value: serde_json::Value =
            serde_json::from_slice(line).context("invalid grep NDJSON record")?;
        match value.get("type").and_then(serde_json::Value::as_str) {
            Some("hit") => hits.push(
                serde_json::from_value(value.get("hit").cloned().unwrap_or_default())
                    .context("invalid grep hit")?,
            ),
            Some("event") => events.push(
                serde_json::from_value(value.get("event").cloned().unwrap_or_default())
                    .context("invalid grep event")?,
            ),
            _ => {}
        }
    }
    Ok(sheaf_core::store::GrepReport {
        query_fingerprint: summary
            .get("query_fingerprint")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_owned(),
        complete: summary
            .get("complete")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true),
        stop_reason: summary
            .get("stop_reason")
            .and_then(|v| serde_json::from_value(v.clone()).ok()),
        cursor: summary
            .get("cursor")
            .and_then(|v| serde_json::from_value(v.clone()).ok()),
        hits,
        events,
        skipped_binary: summary
            .get("skipped_binary")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0) as usize,
        pruned_intervals: summary
            .get("pruned_intervals")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0) as usize,
        usage: summary
            .get("usage")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or(sheaf_core::store::SearchUsage {
                results: hits_len(summary),
                materialized_bytes: 0,
                elapsed_ms: 0,
                historical_forks: 0,
                historical_path_reads: 0,
                historical_cache_hits: 0,
                historical_disk_cache_hits: 0,
                content_dedup_hits: 0,
                cursor_replayed_captures: 0,
                trigram_skipped: 0,
            }),
        degraded: false,
    })
}

fn hits_len(summary: &serde_json::Value) -> usize {
    summary
        .get("hits")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0) as usize
}

/// Progressive grep rendering: each finalized record prints the moment
/// the walk emits it — GNU-grep liveness instead of a single flush after
/// the full scan — and the summary tail prints once the stream ends.
struct GrepStreamPrinter {
    json: bool,
    color_on: bool,
    rows: usize,
}

impl GrepStreamPrinter {
    fn new(json: bool, color: ColorWhen) -> Self {
        GrepStreamPrinter {
            json,
            color_on: color.enabled(std::io::IsTerminal::is_terminal(&std::io::stdout())),
            rows: 0,
        }
    }

    fn record(&mut self, record: &sheaf_core::store::GrepStreamRecord) {
        self.rows += 1;
        let mut out = std::io::stdout().lock();
        if self.json {
            let mut line = serde_json::to_vec(record).expect("grep record serializes");
            line.push(b'\n');
            let _ = out.write_all(&line);
        } else {
            match record {
                sheaf_core::store::GrepStreamRecord::Hit { hit } => {
                    let _ = writeln!(out, "{}", render_hit_row(hit, self.color_on));
                }
                sheaf_core::store::GrepStreamRecord::Event { event } => {
                    let _ = writeln!(out, "{}", render_event_row(event, self.color_on));
                }
            }
        }
        // A piped stdout is block-buffered; flush per record or the stream
        // the user asked for never actually streams.
        let _ = out.flush();
    }

    /// The tail after the stream: match count line, then the same notes
    /// the buffered view carried (truncation cursor, binary skips, gaps).
    fn finish(&self, report: &sheaf_core::store::GrepReport) {
        if self.json {
            // Uniform NDJSON contract: records first, summary last.
            let line = serde_json::to_vec(&serde_json::json!({
                "type": "summary",
                "report": report,
            }))
            .expect("grep report serializes");
            let mut out = std::io::stdout().lock();
            let _ = out.write_all(&line);
            let _ = out.write_all(b"\n");
            let _ = out.flush();
            return;
        }
        if self.rows == 0 && report.complete {
            // Only claim emptiness when the walk finished: a truncated
            // zero-row page has matches past the cursor.
            println!("no matches in timeline history");
        }
        if !report.complete {
            if let Some(cursor) = &report.cursor {
                let token = match cursor.resume_capture_id.as_deref() {
                    Some(resume) if cursor.after_capture_id == "@before-first" => {
                        format!("{}:{}", resume, cursor.record_index)
                    }
                    Some(resume) => format!(
                        "{}:{}:{}",
                        cursor.after_capture_id, resume, cursor.record_index
                    ),
                    None => cursor.after_capture_id.clone(),
                };
                eprintln!(
                    "note: results truncated ({}); resume with --after {}",
                    report
                        .stop_reason
                        .map(|r| format!("{r:?}"))
                        .unwrap_or_else(|| "budget".to_owned()),
                    token,
                );
            }
        }
        if report.skipped_binary > 0 {
            eprintln!("note: {} binary file(s) skipped", report.skipped_binary);
        }
        if report.pruned_intervals > 0 {
            eprintln!(
                "note: {} pruned interval(s) not searchable",
                report.pruned_intervals
            );
        }
    }
}

fn render_hit_row(hit: &sheaf_core::store::GrepHit, on: bool) -> String {
    use sheaf_core::store::LifecycleKind;
    let tag = match hit.kind {
        LifecycleKind::Present => paint(on, "32", "present"),
        LifecycleKind::Introduced => paint(on, "32", "introduced"),
        LifecycleKind::Reintroduced => paint(on, "32", "reintroduced"),
        LifecycleKind::Changed => paint(on, "33", "changed"),
        LifecycleKind::Relocated => paint(on, "36", "relocated"),
        LifecycleKind::Renamed => paint(on, "36", "renamed"),
        LifecycleKind::Moved => paint(on, "36", "moved"),
        LifecycleKind::Observed => "observed".to_owned(),
        _ => "present".to_owned(),
    };
    // History records name their episode so a follow-up --episode anchor is
    // one copy-paste away; point discovery has no episodes.
    let identity = match &hit.episode_id {
        Some(episode) => format!(
            "occurrence {}  selection {}  episode {}",
            &hit.occurrence_id[..16.min(hit.occurrence_id.len())],
            &hit.handle_id[..16.min(hit.handle_id.len())],
            episode,
        ),
        None => format!(
            "occurrence {}  selection {}",
            &hit.occurrence_id[..16.min(hit.occurrence_id.len())],
            &hit.handle_id[..16.min(hit.handle_id.len())],
        ),
    };
    format!(
        "{}  {:<12}  {}  {}:{}:{}\n    {}\n    {}",
        &hit.capture_id[..12.min(hit.capture_id.len())],
        tag,
        if hit.on_current {
            "·".to_owned()
        } else {
            paint(on, "35", "branch")
        },
        hit.path,
        hit.line,
        hit.column,
        hit.preview,
        identity,
    )
}

fn render_event_row(event: &sheaf_core::store::GrepEvent, on: bool) -> String {
    use sheaf_core::store::LifecycleKind;
    let tag = match event.kind {
        LifecycleKind::Removed => paint(on, "31", "removed"),
        LifecycleKind::Ambiguous => paint(on, "31", "ambiguous"),
        LifecycleKind::RetentionGap => paint(on, "90", "retention gap"),
        _ => "event".to_owned(),
    };
    // An ambiguity diagnostic names the terminated episode and its ordered
    // candidate handles so the follow-up is explicit, never guessed.
    let suffix = match (&event.episode_id, &event.candidates) {
        (Some(episode), Some(candidates)) if !candidates.is_empty() => {
            format!("  episode {}  candidates {}", episode, candidates.len())
        }
        (Some(episode), _) => format!("  episode {}", episode),
        _ => String::new(),
    };
    format!(
        "{}  {:<12}  {}  {}{}",
        &event.capture_id[..12.min(event.capture_id.len())],
        tag,
        if event.on_current {
            "·".to_owned()
        } else {
            paint(on, "35", "branch")
        },
        event.path.clone().unwrap_or_default(),
        suffix,
    )
}

fn print_diff_stat(outcome: &sheaf_core::store::DiffOutcome) {
    use sheaf_core::store::{DiffKind, SideContent};
    if outcome.entries.is_empty() {
        println!("no differences");
        return;
    }
    let mut added = 0usize;
    let mut removed = 0usize;
    for entry in &outcome.entries {
        added += entry.added_lines;
        removed += entry.removed_lines;
        let name = match &entry.old_path {
            Some(old) => format!("{old} => {}", entry.path),
            None => entry.path.clone(),
        };
        let kind_note = match entry.kind {
            DiffKind::Renamed => " (renamed)",
            DiffKind::TypeChanged => " (type change)",
            _ => "",
        };
        let binary_note = matches!(entry.old, SideContent::Binary { .. })
            || matches!(entry.new, SideContent::Binary { .. });
        if binary_note {
            println!("{name}{kind_note} | Bin");
        } else {
            let bar: String =
                "-".repeat(entry.removed_lines.min(50)) + &"+".repeat(entry.added_lines.min(50));
            println!(
                "{name}{kind_note} | {} {}",
                entry.added_lines + entry.removed_lines,
                bar
            );
        }
    }
    println!(
        "{} files changed, {added} insertions(+), {removed} deletions(-)",
        outcome.entries.len()
    );
    let from = side_label(&outcome.from);
    let to = side_label(&outcome.to);
    println!("({from} vs {to})");
}

fn side_label(side: &sheaf_core::store::SideDesc) -> String {
    if side.kind == "worktree" {
        return "worktree".to_owned();
    }
    match &side.capture_id {
        Some(id) => format!("capture:{}", &id[..12.min(id.len())]),
        None => "point".to_owned(),
    }
}

fn cmd_checkpoint_list(project: Option<&Path>, as_json: bool, color: ColorWhen) -> CliResult {
    let root = timeline_root(project)?;
    let socket = sheaf_core::paths::control_socket_path();
    let (checkpoints, degraded) = match Client::connect(&socket, Duration::from_secs(2)) {
        Ok(mut client) => {
            let reply = client.call("checkpoint.list", Some(&root), serde_json::json!({}), None)?;
            if !reply.response.ok {
                return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into());
            }
            let value = reply.response.result.unwrap_or_default();
            let checkpoints =
                serde_json::from_value(value.get("checkpoints").cloned().unwrap_or_default())
                    .context("daemon returned invalid checkpoints")?;
            (checkpoints, false)
        }
        Err(_) => {
            let _guard = shared_read_guard(&root)?;
            let reader = sheaf_core::store::TimelineReader::open(&root)?;
            (reader.checkpoints(), true)
        }
    };
    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(
                &serde_json::json!({"checkpoints": checkpoints, "degraded": degraded})
            )
            .context("serialize checkpoints")?
        );
    } else {
        if degraded {
            eprintln!("note: daemon unavailable; showing a read-only store snapshot");
        }
        let color_on = color.enabled(std::io::stdout().is_terminal());
        for cp in checkpoints {
            let id = cp
                .capture_id
                .as_deref()
                .map(|s| &s[..12.min(s.len())])
                .unwrap_or("------------");
            let when = cp
                .timestamp_ms
                .and_then(DateTime::<Utc>::from_timestamp_millis)
                .map(|utc| {
                    let local: DateTime<Local> = utc.into();
                    local.format("%Y-%m-%d %H:%M:%S").to_string()
                })
                .unwrap_or_else(|| "unknown time".to_owned());
            // A checkpoint pinned on a branch the worktree no longer holds
            // stays perfectly resolvable; the marker keeps that from being a
            // surprise when the timeline around it looks linear.
            let marker = if cp.on_current {
                String::new()
            } else {
                "  (off current lineage)".to_owned()
            };
            println!(
                "{:<24}  {}  {}{}",
                cp.name,
                paint(color_on, "36", id),
                paint(color_on, "2", &when),
                marker
            );
        }
    }
    Ok(())
}

fn cmd_checkpoint_create(project: Option<&Path>, name: &str, at: Option<&str>) -> CliResult {
    let root = timeline_root(project)?;
    let socket = sheaf_core::paths::control_socket_path();
    let mut client = Client::connect(&socket, Duration::from_secs(2)).map_err(|_| {
        anyhow::anyhow!("checkpoint creation requires the running daemon; no offline writer fallback is allowed")
    })?;
    let reply = client.call(
        "checkpoint.create",
        Some(&root),
        serde_json::json!({"name": name, "at": at}),
        None,
    )?;
    if !reply.response.ok {
        return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into());
    }
    let checkpoint: sheaf_core::store::Checkpoint = serde_json::from_value(
        reply
            .response
            .result
            .and_then(|v| v.get("checkpoint").cloned())
            .unwrap_or_default(),
    )
    .context("daemon returned invalid checkpoint")?;
    let id = checkpoint
        .capture_id
        .as_deref()
        .map(|s| &s[..12.min(s.len())])
        .unwrap_or("------------");
    println!("checkpoint {} -> {}", checkpoint.name, id);
    Ok(())
}
// --------------------------------------------------------------- worktrees

fn daemon_client(feature: &str) -> anyhow::Result<Client> {
    Client::connect(
        &sheaf_core::paths::control_socket_path(),
        Duration::from_secs(2),
    )
    .map_err(|_| anyhow::anyhow!("{feature} requires the running daemon"))
}

fn cmd_worktree_list(project: Option<&Path>, as_json: bool) -> CliResult {
    let root = timeline_root(project)?;
    let mut client = daemon_client("listing live worktrees")?;
    let reply = client.call(
        "worktree.list",
        Some(&root),
        serde_json::json!({}),
        None,
    )?;
    if !reply.response.ok {
        return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into());
    }
    let worktrees: Vec<sheaf_core::store::WorktreeInfo> = serde_json::from_value(
        reply
            .response
            .result
            .and_then(|value| value.get("worktrees").cloned())
            .unwrap_or_default(),
    )
    .context("daemon returned invalid worktree list")?;
    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&worktrees).map_err(anyhow::Error::from)?
        );

        return Ok(());
    }
    for worktree in worktrees {
        let marker = if worktree.primary { "*" } else { " " };
        let state = if worktree.present { "" } else { " (missing)" };
        let point = worktree
            .capture_id
            .as_deref()
            .map(|id| &id[..12.min(id.len())])
            .unwrap_or("------------");
        println!("{marker} {}  {point}{state}", worktree.path.display());
    }
    Ok(())
}

fn cmd_worktree_add(
    project: Option<&Path>,
    reference: &str,
    destination: &Path,
    as_json: bool,
) -> CliResult {
    let root = timeline_root(project)?;
    let destination = if destination.is_absolute() {
        destination.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(anyhow::Error::from)?
            .join(destination)

    };
    let mut client = daemon_client("creating a live worktree")?;
    client.set_timeout(Duration::from_secs(120))?;

    let reply = client.call(
        "worktree.add",
        Some(&root),
        serde_json::json!({
            "reference": reference,
            "destination": destination,
        }),
        None,

    )?;
    if !reply.response.ok {
        return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into());
    }
    let result = reply.response.result.unwrap_or_default();
    let worktree: sheaf_core::store::WorktreeInfo = serde_json::from_value(
        result.get("worktree").cloned().unwrap_or_default(),
    )
    .context("daemon returned invalid worktree")?;
    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&result).map_err(anyhow::Error::from)?
        );

    } else {
        let point = worktree
            .capture_id
            .as_deref()
            .map(|id| &id[..12.min(id.len())])
            .unwrap_or("------------");
        println!("worktree {} -> {point}", worktree.path.display());
        println!("watching: yes");
    }
    Ok(())
}

// ------------------------------------------------------------------- merge

fn print_merge_plan(plan: &sheaf_core::store::MergePlan) {
    let short = |point: &sheaf_core::store::ResolvedPoint| {
        point
            .capture_id
            .as_deref()
            .map(|id| id[..12.min(id.len())].to_owned())
            .unwrap_or_else(|| point.frontier[..12.min(point.frontier.len())].to_owned())
    };
    println!("merge base:   {}", short(&plan.base));
    println!("source:       {}", short(&plan.source));
    println!("target:       {}", short(&plan.target));
    println!("changes:      {}", plan.actions.len());
    println!("conflicts:    {}", plan.conflicts.len());
    for action in &plan.actions {
        println!("  {:?}  {}", action.kind, action.path);
    }
    for conflict in &plan.conflicts {
        println!("  conflict  {}: {}", conflict.path, conflict.reason);
    }
}

fn cmd_merge(
    project: Option<&Path>,
    source: Option<&str>,
    apply: bool,
    resume: bool,
    as_json: bool,
) -> CliResult {
    let root = timeline_root(project)?;
    let mut client = daemon_client("timeline merging")?;
    client.set_timeout(Duration::from_secs(120))?;

    if resume {
        let reply = client.call(
            "merge.resume",
            Some(&root),
            serde_json::json!({}),
            None,

        )?;
        if !reply.response.ok {
            return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into());
        }
        let outcome: sheaf_core::store::MergeOutcome = serde_json::from_value(
            reply
                .response
                .result
                .and_then(|value| value.get("outcome").cloned())
                .unwrap_or_default(),
        )
        .context("daemon returned invalid merge outcome")?;
        if as_json {
            println!(
                "{}",
                serde_json::to_string_pretty(&outcome).map_err(anyhow::Error::from)?
            );

        } else {
            println!(
                "merge resumed: {} written, {} deleted",
                outcome.files_written, outcome.files_deleted
            );
        }
        return Ok(());
    }
    let Some(source) = source else {
        return Err(anyhow::anyhow!("merge needs a source reference or `--resume`").into());
    };
    let reply = client.call(
        "merge.plan",
        Some(&root),
        serde_json::json!({"source": source}),
        None,

    )?;
    if !reply.response.ok {
        return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into());
    }
    let plan: sheaf_core::store::MergePlan = serde_json::from_value(
        reply
            .response
            .result
            .and_then(|value| value.get("plan").cloned())
            .unwrap_or_default(),
    )
    .context("daemon returned invalid merge plan")?;
    if !apply {
        if as_json {
            println!(
                "{}",
                serde_json::to_string_pretty(&plan).map_err(anyhow::Error::from)?
            );

        } else {
            print_merge_plan(&plan);
            if plan.conflicts.is_empty() {
                println!("apply:        sheaf merge {source} --apply");
            } else {
                println!("apply:        blocked until conflicts are resolved");
            }
        }
        return Ok(());
    }
    if !plan.conflicts.is_empty() {
        if as_json {
            println!(
                "{}",
                serde_json::to_string_pretty(&plan).map_err(anyhow::Error::from)?
            );

        } else {
            print_merge_plan(&plan);
            eprintln!("sheaf: merge blocked by {} conflict(s)", plan.conflicts.len());
        }
        return Err(ExitErr::SilentCode(EXIT_RESTORE_BLOCKED));
    }
    let reply = client.call(
        "merge.apply",
        Some(&root),
        serde_json::json!({"token": plan.token}),
        None,

    )?;
    if !reply.response.ok {
        return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into());
    }
    let outcome: sheaf_core::store::MergeOutcome = serde_json::from_value(
        reply
            .response
            .result
            .and_then(|value| value.get("outcome").cloned())
            .unwrap_or_default(),
    )
    .context("daemon returned invalid merge outcome")?;
    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&outcome).map_err(anyhow::Error::from)?
        );

    } else {
        println!(
            "merged {} change(s): {} written, {} deleted",
            plan.actions.len(),
            outcome.files_written,
            outcome.files_deleted
        );
        if let Some(id) = outcome.capture_id {
            println!("capture: {}", &id[..12.min(id.len())]);
        }
    }
    Ok(())
}


// ------------------------------------------------------------------ restore

/// Exit code for a restore that refuses to run: the worktree is untouched and
/// the reason is on stderr.
const EXIT_RESTORE_BLOCKED: u8 = 4;

fn cmd_restore(
    project: Option<&Path>,
    args: &[String],
    at: Option<&str>,
    dry_run: bool,
    as_json: bool,
) -> CliResult {
    let root = timeline_root(project)?;
    // Relative paths mean what the user sees: the invocation directory when
    // standing inside the project, the project root when addressing it from
    // outside with `-C`.
    let cwd = std::env::current_dir()
        .map(|dir| normalize_existing(&dir))
        .context("no current directory")?;
    let base = if cwd.starts_with(&root) {
        cwd
    } else {
        root.clone()
    };

    // `--at` frees the whole positional slot for paths; without it the first
    // positional is the point, which reads as `sheaf restore @~3 src/`.
    let (point, raw_paths) = match at {
        Some(reference) => (reference.to_owned(), args),
        None => match args.split_first() {
            Some((first, rest)) => (first.clone(), rest),
            None => {
                eprintln!(
                    "sheaf: restore needs a timeline point (a capture ID, checkpoint:<name>, @~N, or a time)"
                );
                return Err(ExitErr::SilentCode(2));
            }
        },
    };
    let mut scope = Vec::new();
    for raw in raw_paths {
        scope.push(sheaf_core::store::scope_key(&root, &base, raw)?);
    }
    // An explicit path that names the project root means the whole tree.
    if scope.iter().any(String::is_empty) {
        scope.clear();
    }

    let socket = sheaf_core::paths::control_socket_path();
    let mut client = match Client::connect(&socket, Duration::from_secs(2)) {
        Ok(client) => Some(client),
        Err(_) if dry_run => None,
        Err(_) => {
            return Err(anyhow::anyhow!(
                "restore requires the running daemon; only `--dry-run` works offline"
            )
            .into())
        }
    };

    let plan: sheaf_core::store::RestorePlan = match client.as_mut() {
        Some(client) => {
            let reply = client.call(
                "restore.plan",
                Some(&root),
                serde_json::json!({"at": point, "paths": scope}),
                None,
            )?;
            if !reply.response.ok {
                return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into());
            }
            // Plans stream through body chunks (envelope holds a summary);
            // fall back to the envelope for an older daemon.
            if !reply.body.is_empty() {
                serde_json::from_slice(&reply.body)
                    .context("daemon returned an invalid streamed restore plan")?
            } else {
                serde_json::from_value(
                    reply
                        .response
                        .result
                        .and_then(|v| v.get("plan").cloned())
                        .unwrap_or_default(),
                )
                .context("daemon returned an invalid restore plan")?
            }
        }
        None => {
            let _guard = shared_read_guard(&root)?;
            let patterns = config::load(&root)
                .map(|cfg| cfg.ignore.patterns)
                .unwrap_or_else(|_| config::default_patterns());
            let ignore = sheaf_core::ignore::IgnoreSet::for_project(&root, &patterns)
                .map_err(|e| anyhow::anyhow!("bad ignore patterns: {e}"))?;
            let reader = sheaf_core::store::TimelineReader::open(&root)?;
            reader.plan_restore(&point, &scope, &ignore)?
        }
    };

    // Every path that stops short of applying reports the plan itself, so a
    // blocked or already-satisfied restore is as inspectable as a dry run.
    let stops_here = dry_run || !plan.applicable() || plan.is_noop();
    for missing in &plan.scope_missing {
        eprintln!("sheaf: no history or live path ever held `{missing}` — check the path");
    }
    if as_json {
        if stops_here {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({"plan": plan}))
                    .context("serialize restore plan")?
            );
        }
    } else {
        print_plan(&plan);
    }
    if !plan.applicable() {
        eprintln!("sheaf: restore blocked; nothing was changed");
        return Err(ExitErr::SilentCode(EXIT_RESTORE_BLOCKED));
    }
    if stops_here {
        if dry_run && plan.degraded {
            eprintln!("note: daemon unavailable; this plan reads a read-only store snapshot");
        }
        return Ok(());
    }

    let client = client.as_mut().expect("apply requires a live daemon");
    let reply = client.call(
        "restore.apply",
        Some(&root),
        serde_json::json!({"token": plan.token}),
        None,
    )?;
    if !reply.response.ok {
        return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into());
    }
    let outcome: sheaf_core::store::RestoreOutcome = serde_json::from_value(
        reply
            .response
            .result
            .and_then(|v| v.get("outcome").cloned())
            .unwrap_or_default(),
    )
    .context("daemon returned an invalid restore outcome")?;

    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({"outcome": outcome}))
                .context("serialize restore outcome")?
        );
    } else {
        print_outcome(&outcome);
    }
    Ok(())
}

/// Restore a selected historical fragment (a selection handle from
/// `sheaf grep`) into the live worktree. Preview-first and token-gated like
/// the whole-file engine; `--insert` / `--delete` name the two modes that
/// rewrite bytes the destination does not already mirror.
fn cmd_fragment_restore(
    project: Option<&Path>,
    source: &str,
    mode: sheaf_core::store::FragmentMode,
    dry_run: bool,
    as_json: bool,
) -> CliResult {
    use sheaf_core::store::{FragmentPlan, TimelineReader};

    let root = timeline_root(project)?;
    let raw = if source == "-" {
        use std::io::Read;
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .context("reading selection JSON from stdin")?;
        buf
    } else {
        std::fs::read_to_string(source)
            .with_context(|| format!("reading selection JSON from `{source}`"))?
    };
    let selections = parse_selection_payload(&raw)?;
    if selections.is_empty() {
        return Err(anyhow::anyhow!("the selection payload holds no handles").into());
    }

    let socket = sheaf_core::paths::control_socket_path();
    let mut client = match Client::connect(&socket, Duration::from_secs(2)) {
        Ok(client) => Some(client),
        Err(_) if dry_run => None,
        Err(_) => {
            return Err(anyhow::anyhow!(
                "fragment restore requires the running daemon; only `--dry-run` works offline"
            )
            .into())
        }
    };

    let plan: FragmentPlan = match client.as_mut() {
        Some(client) => {
            let reply = client.call(
                "fragment.plan",
                Some(&root),
                serde_json::json!({
                    "selections": selections,
                    "mode": match mode {
                        sheaf_core::store::FragmentMode::Replace => "replace",
                        sheaf_core::store::FragmentMode::Insert => "insert",
                        sheaf_core::store::FragmentMode::Delete => "delete",
                    },
                }),
                None,
            )?;
            if !reply.response.ok {
                return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into());
            }
            if !reply.body.is_empty() {
                serde_json::from_slice(&reply.body)
                    .context("daemon returned an invalid streamed fragment plan")?
            } else {
                return Err(anyhow::anyhow!(
                    "daemon returned no fragment plan body; upgrade the daemon"
                )
                .into());
            }
        }
        None => {
            let _guard = shared_read_guard(&root)?;
            let reader = TimelineReader::open(&root)?;
            reader.plan_fragment_restore(&selections, mode)?
        }
    };

    let stops_here = dry_run || !plan.applicable() || plan.is_noop();
    if as_json {
        if stops_here {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({"plan": plan}))
                    .context("serialize fragment plan")?
            );
        }
    } else {
        print_fragment_plan(&plan);
    }
    if !plan.applicable() {
        eprintln!("sheaf: fragment restore blocked; nothing was changed");
        return Err(ExitErr::SilentCode(EXIT_RESTORE_BLOCKED));
    }
    if stops_here {
        if dry_run && plan.degraded {
            eprintln!("note: daemon unavailable; this plan reads a read-only store snapshot");
        }
        return Ok(());
    }

    let client = client.as_mut().expect("apply requires a live daemon");
    let reply = client.call(
        "fragment.apply",
        Some(&root),
        serde_json::json!({"token": plan.token}),
        None,
    )?;
    if !reply.response.ok {
        return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into());
    }
    let outcome: sheaf_core::store::RestoreOutcome = serde_json::from_value(
        reply
            .response
            .result
            .and_then(|v| v.get("outcome").cloned())
            .unwrap_or_default(),
    )
    .context("daemon returned an invalid fragment outcome")?;

    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({"outcome": outcome}))
                .context("serialize fragment outcome")?
        );
    } else {
        print_outcome(&outcome);
    }
    Ok(())
}

/// Accept a bare handle, a grep hit object (`{"handle": {...}}`), or an
/// array of either — whatever `sheaf grep --json | jq ...` naturally emits.
fn parse_selection_payload(raw: &str) -> Result<Vec<sheaf_core::store::SelectionHandle>> {
    use sheaf_core::store::SelectionHandle;
    let value: serde_json::Value =
        serde_json::from_str(raw).context("selection payload is not valid JSON")?;
    fn one(value: serde_json::Value) -> Result<SelectionHandle> {
        let handle = value.get("handle").cloned().unwrap_or(value);
        serde_json::from_value(handle.clone())
            .with_context(|| format!("not a selection handle: {handle}"))
    }
    match value {
        serde_json::Value::Array(items) => items.into_iter().map(one).collect(),
        other => Ok(vec![one(other)?]),
    }
}

fn print_fragment_plan(plan: &sheaf_core::store::FragmentPlan) {
    let mode = match plan.mode {
        sheaf_core::store::FragmentMode::Replace => "replace",
        sheaf_core::store::FragmentMode::Insert => "insert",
        sheaf_core::store::FragmentMode::Delete => "delete",
    };
    println!(
        "fragment restore:  {mode}  ({} selection{})",
        plan.selections.len(),
        if plan.selections.len() == 1 { "" } else { "s" }
    );
    for handle in &plan.selections {
        println!(
            "  selection {}  {}@{}",
            &handle.id()[..12.min(handle.id().len())],
            handle.historical_path,
            &handle.source_frontier[..8.min(handle.source_frontier.len())],
        );
    }
    if plan.files.is_empty() && plan.conflicts.is_empty() {
        println!("already there — nothing to do");
        return;
    }
    for file in &plan.files {
        println!("  {}", file.path);
        for action in &file.actions {
            let verb = match action.kind {
                sheaf_core::store::FragmentActionKind::Replace => "replace",
                sheaf_core::store::FragmentActionKind::Insert => "insert ",
                sheaf_core::store::FragmentActionKind::Delete => "delete ",
            };
            println!(
                "    {verb}  {}..{}   {} bytes → {} bytes   sel {}",
                action.range.start,
                action.range.end,
                action.old_bytes,
                action.new_bytes,
                &action.selection_id[..12.min(action.selection_id.len())],
            );
        }
    }
    for conflict in &plan.conflicts {
        println!(
            "  BLOCKED sel {}: {:?} — {}",
            &conflict.selection_id[..12.min(conflict.selection_id.len())],
            conflict.condition,
            conflict.detail
        );
        for candidate in &conflict.candidates {
            println!(
                "           candidate {} at {}..{}",
                candidate.path, candidate.range.start, candidate.range.end
            );
        }
    }
}

fn print_plan(plan: &sheaf_core::store::RestorePlan) {
    use sheaf_core::store::ActionKind;
    let target = plan
        .target
        .capture_id
        .as_deref()
        .map(short)
        .unwrap_or("(frontier)");
    let scope = if plan.scope.is_empty() {
        "whole worktree".to_owned()
    } else {
        plan.scope.join(", ")
    };
    println!("restore to:  {target}");
    println!("scope:       {scope}");
    for action in &plan.actions {
        let verb = match action.kind {
            ActionKind::Create => "create",
            ActionKind::Update => "update",
            ActionKind::Delete => "delete",
        };
        let size = if action.kind == ActionKind::Delete {
            String::new()
        } else {
            human_bytes(action.bytes)
        };
        println!("  {verb}  {:<44}{size:>10}", action.path);
    }
    for blocked in &plan.obstructions {
        println!("  BLOCKED {:<44}{}", blocked.path, obstacle_text(blocked));
    }
    if plan.is_noop() && plan.applicable() {
        println!("already there — nothing to do");
        return;
    }
    println!(
        "{} to write, {} to delete, {} already current",
        plan.writes(),
        plan.deletes(),
        plan.unchanged
    );
    if plan.locally_modified > 0 {
        println!(
            "note:        {} of these overwrite uncaptured edits — they are captured first and stay reachable",
            plan.locally_modified
        );
    }
}

fn print_outcome(outcome: &sheaf_core::store::RestoreOutcome) {
    use sheaf_core::store::RestoreMode;
    let target = outcome
        .target
        .capture_id
        .as_deref()
        .map(short)
        .unwrap_or("(frontier)");
    println!(
        "restored to {target}: {} written, {} deleted, {} unchanged",
        outcome.files_written, outcome.files_deleted, outcome.unchanged
    );
    if let Some(saved) = &outcome.pre_restore_capture {
        println!("saved:       uncaptured work captured as {}", short(saved));
    }
    if let Some(capture) = &outcome.restore_capture {
        println!("recorded:    {} (forward history)", short(capture));
    }
    if outcome.mode == RestoreMode::Full {
        println!("branching:   new edits diverge from here; the abandoned future stays reachable");
    }
    if let Some(undo) = &outcome.undo.capture_id {
        println!("undo:        sheaf restore {}", short(undo));
    }
}

/// A scoped restore stays on the current lineage, so the timeline is where it
/// has to declare itself.
fn origin_suffix(origin: Option<&sheaf_core::store::CaptureOrigin>) -> String {
    use sheaf_core::store::OriginKind;
    let Some(origin) = origin else {
        return String::new();
    };
    match origin.kind {
        OriginKind::Restore => match &origin.target {
            Some(target) => format!("   [restore \u{2190} {}]", short(target)),
            None => "   [restore]".to_owned(),
        },
        OriginKind::PreRestore => "   [pre-restore snapshot]".to_owned(),
        OriginKind::FragmentRestore => {
            // The suffix names the selection handles that were spliced in;
            // their IDs are already content-addressed provenance.
            match origin.selections.first() {
                Some(selection) => format!(
                    "   [fragment \u{2190} {}]",
                    &selection[..12.min(selection.len())]
                ),
                None => "   [fragment restore]".to_owned(),
            }
        }
        OriginKind::Merge => match &origin.target {
            Some(source) => format!("   [merge \u{2190} {}]", short(source)),
            None => "   [merge]".to_owned(),
        },
    }
}

fn obstacle_text(blocked: &sheaf_core::store::Obstruction) -> &'static str {
    use sheaf_core::store::Obstacle;
    match blocked.obstacle {
        Obstacle::DirectoryInTheWay => "a directory occupies this path",
        Obstacle::SymlinkInTheWay => "a symlink occupies this path",
        Obstacle::MissingBlob => "the stored binary payload is missing",
        Obstacle::EscapesRoot => "the stored path escapes the project root",
        Obstacle::Unreadable => "the live path cannot be read",
    }
}

fn short(id: &str) -> &str {
    &id[..12.min(id.len())]
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 4] = ["B", "KiB", "MiB", "GiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

fn shared_read_guard(root: &Path) -> Result<std::fs::File, ExitErr> {
    let path = config::sheaf_dir(root).join("lock");
    match sheaf_core::store::try_lock_shared(&path) {
        Ok(Some(file)) => Ok(file),
        Ok(None) => Err(anyhow::anyhow!("store lock unavailable").into()),
        Err(e) => Err(anyhow::anyhow!("daemon unavailable and store is busy: {e}").into()),
    }
}

// ------------------------------------------------- intent lifecycle verbs

fn cmd_restore_resume(project: Option<&Path>, _as_json: bool) -> CliResult {
    let root = timeline_root(project)?;
    let socket = sheaf_core::paths::control_socket_path();
    let mut client = Client::connect(&socket, Duration::from_secs(2)).map_err(|_| {
        anyhow::anyhow!(
            "resuming a restore requires the running daemon; check `sheaf status` for the pending intent"
        )
    })?;
    let reply = client.call("restore.resume", Some(&root), serde_json::json!({}), None)?;
    if !reply.response.ok {
        return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into());
    }
    println!("restore resumed and completed");
    Ok(())
}

fn cmd_restore_abandon(project: Option<&Path>, _as_json: bool) -> CliResult {
    let root = timeline_root(project)?;
    let socket = sheaf_core::paths::control_socket_path();
    let mut client = Client::connect(&socket, Duration::from_secs(2)).map_err(|_| {
        anyhow::anyhow!(
            "abandoning a restore requires the running daemon; check `sheaf status` for the pending intent"
        )
    })?;
    let reply = client.call("restore.abandon", Some(&root), serde_json::json!({}), None)?;
    if !reply.response.ok {
        return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into());
    }
    let reconciled = reply.response.result.and_then(|v| {
        v.get("reconciled_as")
            .and_then(|c| c.as_str())
            .map(str::to_owned)
    });
    match reconciled {
        Some(id) => println!(
            "restore intent abandoned; the half-applied state was captured as {}",
            &id[..12.min(id.len())]
        ),
        None => println!("restore intent abandoned; worktree was already consistent"),
    }
    Ok(())
}

// ------------------------------------------------------------------ doctor

fn cmd_doctor(project: Option<&Path>, as_json: bool, fix: bool) -> CliResult {
    let start = match project {
        Some(p) => p.to_path_buf(),
        None => std::env::current_dir().context("no current directory")?,
    };
    let root = match resolve_project_root(&start) {
        Some(r) => normalize_existing(&r),
        None => {
            return Err(anyhow::anyhow!(
                "no project root found above {} (missing .sheaf/config.toml)",
                start.display()
            )
            .into())
        }
    };
    // Daemon first: a hot project's writer thread holds the exclusive
    // flock until the daemon exits, so an external lock would block
    // forever. The daemon runs the sweep itself (and the repair, on its
    // collector thread where journal truncation cannot race the
    // appender); offline, a read sweep takes a shared lock and a repair
    // takes the exclusive one after proving no writer exists. (Cold
    // projects hold no flock — they open lazily on first activity — so
    // offline repair of a parked project works without stopping the daemon.)
    let socket = sheaf_core::paths::control_socket_path();
    let mut daemon_reachable = false;
    let mut repair: Option<sheaf_core::store::RepairOutcome> = None;
    let report = if let Ok(mut client) = Client::connect(&socket, Duration::from_secs(2)) {
        daemon_reachable = true;
        // The sweep replays the uncovered journal tail into a fresh
        // document, which can legitimately compute past handshake speed on
        // a large store; give the work the same room diff/gc allow
        // (the daemon allows itself 30s for the same reason).
        client.set_timeout(Duration::from_secs(35))?;
        let reply = client.call(
            "store.doctor",
            Some(&root),
            serde_json::json!({"fix": fix}),
            None,
        )?;
        if !reply.response.ok {
            return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into());
        }
        let parsed: sheaf_core::store::DoctorReply = serde_json::from_value(
            reply
                .response
                .result
                .and_then(|v| v.get("report").cloned())
                .unwrap_or_default(),
        )
        .context("daemon returned an invalid doctor report")?;
        match parsed {
            sheaf_core::store::DoctorReply::Repair(outcome) => {
                let after = outcome.after.clone();
                repair = Some(*outcome);
                after
            }
            sheaf_core::store::DoctorReply::Report(report) => *report,
        }
    } else if fix {
        let lock_path = config::sheaf_dir(&root).join("lock");
        let _exclusive = match sheaf_core::store::try_lock_exclusive(&lock_path) {
            Ok(Some(f)) => f,
            Ok(None) => {
                return Err(anyhow::anyhow!(
                    "store is busy (a daemon or another writer holds {}); stop the daemon first",
                    lock_path.display()
                )
                .into())
            }
            Err(e) => return Err(anyhow::anyhow!("flock {}: {e}", lock_path.display()).into()),
        };
        let outcome = sheaf_core::store::doctor_fix(&root)?;
        let after = outcome.after.clone();
        repair = Some(outcome);
        after
    } else {
        let _guard = shared_read_guard(&root)?;
        sheaf_core::store::doctor(&root)?
    };
    if as_json {
        if let Some(outcome) = &repair {
            println!(
                "{}",
                serde_json::to_string_pretty(outcome).context("serialize repair outcome")?
            );
        } else {
            println!(
                "{}",
                serde_json::to_string_pretty(&report).context("serialize doctor report")?
            );
        }
    } else {
        if let Some(outcome) = &repair {
            println!("fixes applied: {}", outcome.applied.len());
            for f in &outcome.applied {
                println!("  - [{:<19}] {}", f.action, f.detail);
            }
            if !outcome.refused.is_empty() {
                println!("refused (left as-is, with guidance):");
                for r in &outcome.refused {
                    println!("  ! {:<16} {}", r.check, r.reason);
                }
            }
            println!();
        }
        println!("project: {}", report.root);
        // The integrity checks below are store/config sweeps; report daemon
        // reachability plainly so the line and the prose agree — doctor is a
        // read-only sweep of the store, run against the live daemon when it is
        // up and a locked read-only pass when it is not.
        println!(
            "daemon:  {}",
            if daemon_reachable {
                "reachable (sweep ran against the live store)"
            } else {
                "not reachable (read-only offline sweep)"
            }
        );
        for check in &report.checks {
            let mark = if check.ok { "ok  " } else { "FAIL" };
            println!("  [{mark}] {:<16} {}", check.name, check.detail);
        }
        println!(
            "store:   journal {} B / {} segs, snapshot {} B, blobs {} ({} orphan, {} B)",
            report.journal_bytes,
            report.journal_segments,
            report.snapshot_bytes,
            report.blob_count,
            report.orphan_blobs,
            report.orphan_blob_bytes,
        );
        println!(
            "history: {} captures, {} branch tips{}",
            report.captures,
            report.branch_tips,
            if report.pending_restore.is_some() {
                ", PENDING RESTORE INTENT (see `sheaf status`)"
            } else {
                ""
            }
        );
        println!(
            "verdict: {}",
            if report.ok {
                "healthy"
            } else {
                "problems found (see FAIL lines)"
            }
        );
        if !report.ok {
            return Err(ExitErr::SilentCode(5));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------- gc

fn cmd_gc(
    project: Option<&Path>,
    apply: bool,
    as_json: bool,
    set_expiry: Option<String>,
    mark: Option<String>,
) -> CliResult {
    let start = match project {
        Some(p) => p.to_path_buf(),
        None => std::env::current_dir().context("no current directory")?,
    };
    let root = match resolve_project_root(&start) {
        Some(r) => normalize_existing(&r),
        None => {
            return Err(anyhow::anyhow!(
                "no project root found above {} (missing .sheaf/config.toml)",
                start.display()
            )
            .into())
        }
    };

    // Expiry is a config knob: persist and report, no store touch needed.
    if let Some(spec) = set_expiry {
        sheaf_core::config::set_retention_expiry(&root, &spec)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        if !as_json {
            println!("edit expiry set to {spec} (reachability-bound; `gc --apply` reclaims)");
        } else {
            println!("{{\"expiry\": \"{spec}\"}}");
        }
        if mark.is_none() {
            return Ok(());
        }
    }

    // Prefer the daemon: the writer thread owns the store, so applying
    // and marking there can never race a live flush.
    let socket = sheaf_core::paths::control_socket_path();
    if let Ok(mut client) = Client::connect(&socket, Duration::from_secs(2)) {
        if let Some(reference) = &mark {
            let reply = client.call(
                "store.gc",
                Some(&root),
                serde_json::json!({"mark": reference}),
                None,
            )?;
            if !reply.response.ok {
                return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into());
            }
            let marked: sheaf_core::store::MarkedCapture = serde_json::from_value(
                reply
                    .response
                    .result
                    .and_then(|v| v.get("mark").cloned())
                    .unwrap_or_default(),
            )
            .context("daemon returned an invalid mark result")?;
            print_marked(&marked, as_json);
            return Ok(());
        }
        let reply = client.call(
            "store.gc",
            Some(&root),
            serde_json::json!({"apply": apply}),
            None,
        )?;
        if !reply.response.ok {
            return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into());
        }
        let outcome: sheaf_core::store::GcOutcome = serde_json::from_value(
            reply
                .response
                .result
                .and_then(|v| v.get("gc").cloned())
                .unwrap_or_default(),
        )
        .context("daemon returned an invalid gc result")?;
        print_gc_outcome(&outcome, as_json);
        return Ok(());
    }

    // Offline: require exclusive access so no writer races the unlinks.
    let lock_path = config::sheaf_dir(&root).join("lock");
    let _exclusive = match sheaf_core::store::try_lock_exclusive(&lock_path) {
        Ok(Some(f)) => f,
        Ok(None) => {
            return Err(anyhow::anyhow!(
                "store is busy (a daemon or another writer holds {}); stop the daemon first",
                lock_path.display()
            )
            .into())
        }
        Err(e) => return Err(anyhow::anyhow!("flock {}: {e}", lock_path.display()).into()),
    };
    // Offline writer path: open the store under the exclusive flock we
    // hold, so marks and retention trims run on the writer document.
    // Cadence/rotation limits come from the project's config; a config
    // that fails to parse here fails again (with the real error) inside
    // the open, so the fallback default masks nothing.
    let limits = config::load(&root).map(|cfg| cfg.store).unwrap_or_default();
    let mut store =
        sheaf_core::store::ProjectStore::open(&root, limits).map_err(|e| anyhow::anyhow!("{e}"))?;
    if let Some(reference) = &mark {
        let marked = sheaf_core::store::retention_mark(&mut store, reference)
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        print_marked(&marked, as_json);
        return Ok(());
    }
    let outcome = sheaf_core::store::gc_run_store(&mut store, apply)?;
    print_gc_outcome(&outcome, as_json);
    Ok(())
}

/// `sheaf cache backfill|rebuild`: explicitly populate the derived grep
/// cache (disposable state; the timeline is authoritative regardless).
/// Daemon path pages through bounded `cache.backfill` calls so the
/// collector is never held long; offline path opens the writer under the
/// exclusive flock like `gc` and runs once.
fn cmd_cache_backfill(
    project: Option<&Path>,
    all: bool,
    rebuild: bool,
    limit: Option<u32>,
    as_json: bool,
) -> CliResult {
    let start = match project {
        Some(p) => p.to_path_buf(),
        None => std::env::current_dir().context("no current directory")?,
    };
    let root = match resolve_project_root(&start) {
        Some(r) => normalize_existing(&r),
        None => {
            return Err(anyhow::anyhow!(
                "no project root found above {} (missing .sheaf/config.toml)",
                start.display()
            )
            .into())
        }
    };

    let socket = sheaf_core::paths::control_socket_path();
    if let Ok(mut client) = Client::connect(&socket, Duration::from_secs(2)) {
        // A backfill page can legitimately compute for a while (store
        // open on a cold daemon, then materialization); the daemon allows
        // itself 30s per reply, so match that headroom here.
        client.set_timeout(Duration::from_secs(35))?;
        // A user-supplied limit means exactly one bounded call; without
        // one, page until the store reports complete coverage. Paged calls
        // re-walk the covered prefix each time (cheap: rows are checked
        // in-memory), so only the final call's examined/skipped figures
        // describe the whole store; indexed/row counters accumulate.
        let single_shot = limit.is_some();
        let mut rebuild_pending = rebuild;
        let mut aggregate: Option<sheaf_core::store::GrepBackfillReport> = None;
        loop {
            let mut params = serde_json::json!({"all": all});
            if rebuild_pending {
                params["rebuild"] = serde_json::json!(true);
            }
            if let Some(limit) = limit {
                params["limit"] = serde_json::json!(limit);
            }
            let reply = client.call("cache.backfill", Some(&root), params, None)?;
            if !reply.response.ok {
                return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into());
            }
            let report: sheaf_core::store::GrepBackfillReport = serde_json::from_value(
                reply
                    .response
                    .result
                    .and_then(|v| v.get("backfill").cloned())
                    .unwrap_or_default(),
            )
            .context("daemon returned an invalid backfill result")?;
            if !as_json {
                let covered = report
                    .watermark
                    .as_ref()
                    .map_or(0, |wm| wm.captures_indexed);
                println!(
                    "  +{}/{} capture(s), {} row(s); coverage at {}",
                    report.captures_indexed, report.captures_examined, report.rows_written, covered
                );
            }
            rebuild_pending = false;
            let complete = report.complete;
            // A page that indexed nothing and saw failures is stalled
            // (materialization keeps failing); looping would spin on the
            // same captures forever, so surface the incomplete report.
            let stalled = report.captures_indexed == 0 && report.captures_failed > 0;
            aggregate = Some(match aggregate.take() {
                None => report,
                Some(mut acc) => {
                    acc.captures_indexed += report.captures_indexed;
                    acc.rows_written += report.rows_written;
                    acc.content_blobs_written += report.content_blobs_written;
                    acc.elapsed_ms += report.elapsed_ms;
                    acc.captures_examined = report.captures_examined;
                    acc.captures_skipped = report.captures_skipped;
                    acc.captures_failed += report.captures_failed;
                    acc.complete = report.complete;
                    acc.watermark = report.watermark.clone();
                    // The trigram index is rebuilt at the end of each run, so
                    // the final page carries the authoritative size.
                    acc.trigram_index_bytes = report.trigram_index_bytes;
                    acc
                }
            });
            if complete || stalled || single_shot {
                break;
            }
        }
        let report = aggregate.expect("at least one daemon reply");
        print_backfill_report(&report, as_json);
        return Ok(());
    }

    // Offline writer path: no daemon holds the store, so take the
    // exclusive flock. A cold page is fork-bound, so chunk the run and
    // print progress rather than going silent for minutes — except when
    // the user's own --limit means exactly one bounded call.
    eprintln!("note: daemon unavailable; this backfill opens the store under the exclusive lock");
    let lock_path = config::sheaf_dir(&root).join("lock");
    let _exclusive = match sheaf_core::store::try_lock_exclusive(&lock_path) {
        Ok(Some(lock)) => lock,
        Ok(None) => {
            return Err(anyhow::anyhow!(
                "store is busy (a daemon or another writer holds {}); stop the daemon first",
                lock_path.display()
            )
            .into())
        }
        Err(e) => return Err(anyhow::anyhow!("flock {}: {e}", lock_path.display()).into()),
    };
    let limits = config::load(&root).map(|cfg| cfg.store).unwrap_or_default();
    let store =
        sheaf_core::store::ProjectStore::open(&root, limits).map_err(|e| anyhow::anyhow!("{e}"))?;
    let chunk = limit.or(Some(128));
    let mut rebuild_pending = rebuild;
    let mut aggregate: Option<sheaf_core::store::GrepBackfillReport> = None;
    loop {
        let report = store
            .grep_cache_backfill(sheaf_core::store::GrepBackfillOptions {
                all,
                rebuild: rebuild_pending,
                limit: chunk,
                max_elapsed_ms: None,
            })
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        if !as_json {
            let covered = report
                .watermark
                .as_ref()
                .map_or(0, |wm| wm.captures_indexed);
            println!(
                "  +{}/{} capture(s), {} row(s); coverage at {}",
                report.captures_indexed, report.captures_examined, report.rows_written, covered
            );
        }
        rebuild_pending = false;
        let complete = report.complete;
        // Same stall guard as the daemon path: no progress plus failures
        // means looping would retry the same captures forever.
        let stalled = report.captures_indexed == 0 && report.captures_failed > 0;
        aggregate = Some(match aggregate.take() {
            None => report,
            Some(mut acc) => {
                acc.captures_indexed += report.captures_indexed;
                acc.rows_written += report.rows_written;
                acc.content_blobs_written += report.content_blobs_written;
                acc.elapsed_ms += report.elapsed_ms;
                acc.captures_examined = report.captures_examined;
                acc.captures_skipped = report.captures_skipped;
                acc.captures_failed += report.captures_failed;
                acc.complete = report.complete;
                acc.watermark = report.watermark.clone();
                acc
            }
        });
        if complete || stalled || limit.is_some() {
            break;
        }
    }
    print_backfill_report(&aggregate.expect("at least one offline report"), as_json);
    Ok(())
}

fn print_backfill_report(report: &sheaf_core::store::GrepBackfillReport, as_json: bool) {
    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(report).expect("backfill report serializes")
        );
        return;
    }
    let verb = if report.rebuilt {
        "rebuilt"
    } else {
        "backfilled"
    };
    let coverage = match &report.watermark {
        Some(wm) => format!(
            "watermark gen {} covers {} capture(s) through {}",
            wm.generation,
            wm.captures_indexed,
            &wm.through_capture_id[..12.min(wm.through_capture_id.len())]
        ),
        None => "no coverage watermark (nothing indexed or a hole broke the chain)".to_owned(),
    };
    println!(
        "grep cache {verb}: {} examined, {} already indexed, {} published ({} rows, {} content blobs) in {} ms",
        report.captures_examined,
        report.captures_skipped,
        report.captures_indexed,
        report.rows_written,
        report.content_blobs_written,
        report.elapsed_ms
    );
    println!("  {coverage}");
    if report.trigram_index_bytes > 0 {
        println!(
            "  trigram index {} KiB (rare/absent literals skip non-matching versions)",
            report.trigram_index_bytes / 1024
        );
    }
    if report.captures_failed > 0 {
        println!(
            "  {} capture(s) could not be materialized; queries fall back to exact reads for them",
            report.captures_failed
        );
    }
    if !report.complete {
        println!("  incomplete: rerun to continue from the watermark");
    }
}

fn print_marked(marked: &sheaf_core::store::MarkedCapture, as_json: bool) {
    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(marked).expect("mark result serializes")
        );
        return;
    }
    let prefix = &marked.capture_id[..12.min(marked.capture_id.len())];
    if marked.already_marked {
        println!("capture {prefix} was already marked; gc --apply will reclaim it");
    } else {
        println!(
            "capture {prefix} marked collectable; gc --apply will reclaim it even though reachability would protect it"
        );
    }
}

fn print_gc_outcome(outcome: &sheaf_core::store::GcOutcome, as_json: bool) {
    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(outcome).expect("gc outcome serializes")
        );
        return;
    }
    match outcome {
        sheaf_core::store::GcOutcome::Planned(plan) => {
            println!(
                "gc plan (report only; rerun with --apply): {} bytes collectable",
                plan.bytes_recovered
            );
            println!("  journal segments: {}", plan.segments.len());
            println!("  superseded snapshots: {}", plan.snapshots.len());
            println!("  unreachable blobs: {}", plan.orphan_blobs.len());
            print_retention(&plan.retention, false);
        }
        sheaf_core::store::GcOutcome::Applied(report) => {
            println!(
                "gc applied: {} segments, {} snapshots, {} blobs removed ({} bytes)",
                report.segments_removed,
                report.snapshots_removed,
                report.blobs_removed,
                report.bytes_recovered
            );
            if report.trimmed > 0 {
                println!(
                    "retention trimmed {} capture(s); history now starts at boundary {}",
                    report.trimmed,
                    report
                        .boundary_after
                        .as_deref()
                        .map(|b| &b[..16.min(b.len())])
                        .unwrap_or("?")
                );
            }
            println!(
                "timeline intact: {} captures remain addressable",
                report.captures_after
            );
            print_retention(&report.plan.retention, true);
        }
    }
}

fn print_retention(retention: &sheaf_core::store::RetentionFacts, after: bool) {
    if retention.expiry.is_none()
        && retention.prunable.is_empty()
        && retention.deferred_marks.is_empty()
    {
        if !after {
            println!("  retention: no expiry set, no marks (history is kept whole)");
        }
        return;
    }
    if let Some(spec) = &retention.expiry {
        println!("  retention: expiry {spec} (reachability-bound)");
    }
    if !retention.prunable.is_empty() {
        let expired = retention
            .prunable
            .iter()
            .filter(|c| c.cause.as_str() == "expiry")
            .count();
        let marked = retention
            .prunable
            .iter()
            .filter(|c| c.cause.as_str() == "gc mark")
            .count();
        let swept = retention.prunable.len() - expired - marked;
        let mut detail = Vec::new();
        if expired > 0 {
            detail.push(format!("{expired} expired"));
        }
        if marked > 0 {
            detail.push(format!("{marked} marked"));
        }
        if swept > 0 {
            detail.push(format!("{swept} swept by boundary"));
        }
        println!(
            "  prunable: {} capture(s) [{}] — gc --apply reclaims their history",
            retention.prunable.len(),
            detail.join(", ")
        );
    }
    if !retention.deferred_marks.is_empty() {
        println!(
            "  marked but pinned: {} (behind protected points; reclaims later)",
            retention.deferred_marks.join(", ")
        );
    }
    if !after {
        println!(
            "  protected: {} point(s) (head, branch tips, checkpoints, pending restores)",
            retention.protected.len()
        );
    }
}

// ------------------------------------------------------------------ squash

/// One squash page from the lineage walk (mirrors `timeline.log` clamping).
const SQUASH_PAGE: usize = 1000;
/// How long `--` waits for the daemon to capture pending worktree edits
/// before committing anyway (default debounce is 300ms; this absorbs a
/// burst plus slow fsync).
const SQUASH_CATCHUP: Duration = Duration::from_secs(10);

/// The resolved start of a squash span.
struct SquashAnchor {
    /// Human-facing description (what `anchor:` shows).
    label: String,
    /// Reference in timeline syntax, resolvable by `diff`/`info` IPC.
    reference: String,
    /// The user's explicit anchor text, when the anchor was not implicit.
    user_ref: Option<String>,
    capture_id: Option<String>,
}

/// Everything a preview shows and a commit stamps.
struct SquashPlan {
    anchor: SquashAnchor,
    /// `Some(B)` for `A..B` previews; `None` means the worktree.
    to_ref: Option<String>,
    diff: sheaf_core::store::DiffOutcome,
    stats: sheaf_core::store::SpanStats,
    degraded: bool,
}

/// Transport bundle: an IPC client when the daemon is up, otherwise a
/// lazily-opened read-only reader under the shared store lock. Every helper
/// takes this so a degraded invocation opens the store exactly once.
struct SquashCtx<'a> {
    root: &'a Path,
    client: Option<Client>,
    degraded_reader: Option<sheaf_core::store::TimelineReader>,
    _guard: Option<std::fs::File>,
}

impl<'a> SquashCtx<'a> {
    fn new(root: &'a Path, client: Option<Client>) -> Result<Self> {
        Ok(SquashCtx {
            root,
            client,
            degraded_reader: None,
            _guard: None,
        })
    }

    /// Read-only store view, opened at most once per invocation.
    fn reader(&mut self) -> Result<&sheaf_core::store::TimelineReader> {
        if self.degraded_reader.is_none() && self.client.is_none() {
            self._guard = Some(shared_read_guard(self.root).map_err(|e| match e {
                ExitErr::Fatal(err) => err,
                ExitErr::SilentCode(code) => anyhow::anyhow!(
                    "store lock unavailable for the read-only fallback (code {code})"
                ),
            })?);
            self.degraded_reader = Some(sheaf_core::store::TimelineReader::open(self.root)?);
        }
        Ok(self.degraded_reader.as_ref().expect("just opened"))
    }

    fn call(&mut self, method: &str, params: serde_json::Value) -> Result<serde_json::Value> {
        let Some(client) = self.client.as_mut() else {
            anyhow::bail!("daemon unavailable");
        };
        if method == "diff" {
            client.set_timeout(Duration::from_secs(35))?;
        }
        let reply = client.call(method, Some(self.root), params, None)?;
        if !reply.response.ok {
            anyhow::bail!(ipc_error_text(&reply.response));
        }
        Ok(reply.response.result.unwrap_or_default())
    }

    fn checkpoints(&mut self) -> Result<Vec<sheaf_core::store::Checkpoint>> {
        if self.client.is_some() {
            let value = self.call("checkpoint.list", serde_json::json!({}))?;
            return serde_json::from_value(value.get("checkpoints").cloned().unwrap_or_default())
                .context("daemon returned invalid checkpoints");
        }
        Ok(self.reader()?.checkpoints())
    }

    /// Capture ID a reference resolves to, when it names a capture. Soft:
    /// exotic points (multi-head `@`, empty store) return None and the span
    /// stats degrade to partial instead of failing the whole squash.
    fn resolve_capture_id(&mut self, reference: &str) -> Option<String> {
        if self.client.is_some() {
            let value = self
                .call(
                    "timeline.info",
                    serde_json::json!({ "reference": reference }),
                )
                .ok()?;
            return value
                .get("info")
                .and_then(|i| i.get("capture"))
                .and_then(|c| c.get("id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
        }
        self.reader()
            .ok()?
            .resolve(reference)
            .ok()
            .and_then(|point| point.capture_id)
    }

    fn diff(
        &mut self,
        from: &str,
        to: Option<&str>,
    ) -> Result<(sheaf_core::store::DiffOutcome, bool)> {
        if self.client.is_some() {
            let value = self.call(
                "diff",
                serde_json::json!({ "from": from, "to": to, "paths": [] }),
            )?;
            let outcome = serde_json::from_value(value.get("diff").cloned().unwrap_or_default())
                .context("daemon returned an invalid diff")?;
            return Ok((outcome, false));
        }
        let root = self.root.to_path_buf();
        let reader = self.reader()?;
        let patterns = config::load(&root)
            .map(|cfg| cfg.ignore.patterns)
            .unwrap_or_else(|_| config::default_patterns());
        let ignore = sheaf_core::ignore::IgnoreSet::for_project(&root, &patterns)
            .map_err(|e| anyhow::anyhow!("bad ignore patterns: {e}"))?;
        let outcome = reader.diff(from, to, &[], &ignore)?;
        Ok((outcome, true))
    }

    /// Captures strictly older than `cursor` (tip when None), newest first,
    /// at most [`SQUASH_PAGE`] per call.
    fn lineage_page(&mut self, cursor: Option<&str>) -> Result<Vec<sheaf_core::store::Capture>> {
        if self.client.is_some() {
            let value = self.call(
                "timeline.log",
                serde_json::json!({
                    "path": null, "follow": false, "all": false,
                    "before": cursor, "limit": SQUASH_PAGE,
                }),
            )?;
            return serde_json::from_value(value.get("entries").cloned().unwrap_or_default())
                .context("daemon returned invalid timeline entries");
        }
        let mut entries = self.reader()?.captures(false, None, false, usize::MAX)?;
        if let Some(cursor) = cursor {
            let pos = entries
                .iter()
                .position(|entry| entry.id == cursor)
                .ok_or_else(|| anyhow::anyhow!("cursor {cursor:.12} is off the current lineage"))?;
            entries.drain(..=pos);
        }
        entries.truncate(SQUASH_PAGE);
        Ok(entries)
    }
}

/// Resolve the squash anchor. `allow_git` is set only on the `--` path,
/// where the user's passthrough already sanctions git subprocesses: the
/// frame search then verifies sha ancestry and falls back to HEAD's commit
/// time. Preview mode stays pure store reads and runs no git.
fn squash_anchor(
    ctx: &mut SquashCtx,
    explicit: Option<&str>,
    allow_git: bool,
) -> Result<SquashAnchor> {
    if let Some(reference) = explicit {
        let capture_id = ctx.resolve_capture_id(reference);
        return Ok(SquashAnchor {
            label: reference.to_owned(),
            reference: reference.to_owned(),
            user_ref: Some(reference.to_owned()),
            capture_id,
        });
    }

    let checkpoints = ctx.checkpoints()?;
    if allow_git {
        // Newest first; off-lineage stamps are considered — the ancestry
        // check against HEAD is what decides they are stale (amend/rebase
        // or branch switches orphan old shas).
        let mut candidates: Vec<_> = checkpoints
            .iter()
            .filter(|cp| sheaf_core::store::anchor_sha(&cp.name).is_some())
            .collect();
        candidates.sort_by_key(|cp| cp.timestamp_ms.unwrap_or(i64::MIN));
        for cp in candidates.iter().rev() {
            let sha = sheaf_core::store::anchor_sha(&cp.name).expect("filtered");
            if git(ctx.root, ["merge-base", "--is-ancestor", sha, "HEAD"])
                .map(|o| o.status.success())
                .unwrap_or(false)
            {
                return Ok(SquashAnchor {
                    label: format!("checkpoint:{} (frame anchor)", cp.name),
                    reference: format!("checkpoint:{}", cp.name),
                    user_ref: None,
                    capture_id: cp.capture_id.clone(),
                });
            }
        }
        // No complete frame stamped: the projected-frame pass-through
        // fallback — the newest valid partial frame whose commit is still in HEAD's
        // ancestry (amend/rebase orphans older shas) contributes its own
        // span anchor.
        let (frames, _torn) = sheaf_core::store::read_frames(ctx.root).unwrap_or_default();
        for frame in frames.iter().rev() {
            if frame.kind != sheaf_core::store::FrameKind::Partial
                || frame.validate_projection().is_err()
            {
                continue;
            }
            let ancestor = git(
                ctx.root,
                ["merge-base", "--is-ancestor", &frame.sha, "HEAD"],
            )
            .map(|o| o.status.success())
            .unwrap_or(false);
            if ancestor {
                if let Some(anchor_id) = frame.anchor_capture_id.clone() {
                    return Ok(SquashAnchor {
                        label: format!("frame {} (projected anchor)", frame.checkpoint_name()),
                        reference: anchor_id.clone(),
                        user_ref: None,
                        capture_id: Some(anchor_id),
                    });
                }
            }
        }

        // No usable frame: anchor at the last commit's own time. Captures
        // after that instant are exactly what the next commit collapses.
        match git(ctx.root, ["show", "-s", "--format=%cI", "HEAD"]) {
            Ok(out) if out.status.success() => {
                let when = String::from_utf8_lossy(&out.stdout).trim().to_owned();
                let reference = format!("time:{when}");
                let capture_id = ctx.resolve_capture_id(&reference);
                return Ok(SquashAnchor {
                    label: format!("last git commit ({when})"),
                    reference,
                    user_ref: None,
                    capture_id,
                });
            }
            Ok(_) | Err(_) => {
                anyhow::bail!(
                    "no git commits to anchor against yet — make the first git commit, then squash the next span onto it (or pass an explicit anchor: `sheaf squash @~N -- ...`)"
                );
            }
        }
    }

    // Preview default: the latest on-lineage frame stamp. Store-only.
    match sheaf_core::store::frame_anchor(&checkpoints) {
        Some(cp) => Ok(SquashAnchor {
            label: format!("checkpoint:{} (frame anchor)", cp.name),
            reference: format!("checkpoint:{}", cp.name),
            user_ref: None,
            capture_id: cp.capture_id.clone(),
        }),
        None => {
            // No complete frame, but a projected (partial) frame's
            // pass-through anchor is a valid store-only fallback for the
            // preview so a history of partial commits still previews.
            let (frames, _torn) = sheaf_core::store::read_frames(ctx.root).unwrap_or_default();
            if let Some(anchor_id) = sheaf_core::store::newest_partial_anchor(&frames) {
                return Ok(SquashAnchor {
                    label: "projected frame anchor".to_owned(),
                    reference: anchor_id.to_owned(),
                    user_ref: None,
                    capture_id: Some(anchor_id.to_owned()),
                });
            }
            let stamps = checkpoints
                .iter()
                .any(|cp| sheaf_core::store::anchor_sha(&cp.name).is_some());
            anyhow::bail!(
                "{}",
                if stamps {
                    "frame checkpoints exist but none sit on the current lineage (branch switched or history rewritten); pass an explicit anchor, e.g. `sheaf squash @~12`"
                } else {
                    "no commit frames stamped yet; pass an explicit anchor, e.g. `sheaf squash @~12` (or commit with `--` to stamp the first frame)"
                }
            )
        }
    }
}

/// Resolve the anchor, diff the span, walk the captures, draft the message.
fn squash_plan(
    ctx: &mut SquashCtx,
    anchor: SquashAnchor,
    to_ref: Option<String>,
) -> Result<SquashPlan> {
    // For `A..B` the span is (A, B]: bounded above by B, inclusive.
    let until = match &to_ref {
        Some(b) => ctx.resolve_capture_id(b),
        None => None,
    };
    let (diff, degraded) = ctx.diff(&anchor.reference, to_ref.as_deref())?;
    let stats = squash_stats(ctx, anchor.capture_id.as_deref(), until.as_deref());
    Ok(SquashPlan {
        anchor,
        to_ref,
        diff,
        stats,
        degraded,
    })
}

/// Span stats over the captures between the anchor (exclusive) and the
/// upper bound (the tip, or B inclusive for `A..B`). An anchor that names
/// no capture (or a bound that cannot be resolved) yields honest partial
/// stats.
fn squash_stats(
    ctx: &mut SquashCtx,
    anchor_capture_id: Option<&str>,
    until_inclusive: Option<&str>,
) -> sheaf_core::store::SpanStats {
    let Some(anchor_id) = anchor_capture_id else {
        return sheaf_core::store::SpanStats {
            partial: true,
            ..Default::default()
        };
    };
    let mut last_error = None;
    let result = sheaf_core::store::collect_span(Some(anchor_id), until_inclusive, |cursor| {
        ctx.lineage_page(cursor).map_err(|e| {
            last_error = Some(format!("{e:#}"));
        })
    });
    match result {
        Ok((span, reached_anchor)) => {
            let mut stats = sheaf_core::store::span_stats(&span, !reached_anchor);
            if !reached_anchor {
                eprintln!(
                    "note: capture walk incomplete for squash span (anchor not on the current lineage or page budget hit); counts are partial"
                );
                stats.partial = true;
            }
            stats
        }
        Err(()) => {
            let why = last_error.unwrap_or_else(|| "page budget exhausted".into());
            eprintln!("note: capture walk failed for squash span ({why}); counts are partial");
            sheaf_core::store::SpanStats {
                partial: true,
                ..Default::default()
            }
        }
    }
}

/// Run git captured (stdout/stderr collected) in the project root.
fn git(
    root: &Path,
    args: impl IntoIterator<Item = impl AsRef<std::ffi::OsStr>>,
) -> Result<std::process::Output> {
    let out = std::process::Command::new("git")
        .args(args)
        .current_dir(root)
        .output()
        .map_err(|e| anyhow::anyhow!("running git: {e} (is git installed?)"))?;
    Ok(out)
}

fn cmd_squash(
    project: Option<&Path>,
    range: Option<&str>,
    git_args: &[String],
    as_json: bool,
) -> CliResult {
    let root = timeline_root(project)?;
    let (from, to) = match range {
        Some(range) => match sheaf_core::store::split_range(range) {
            Ok(pair) => pair,
            Err(message) => return Err(anyhow::anyhow!("{message}").into()),
        },
        None => ("", None), // no range; the default anchor resolution takes over
    };
    // `A..B` names two immutable points; a commit collapses anchor → the
    // live worktree, so accepting `A..B --` would stamp a span git never made.
    if to.is_some() && !git_args.is_empty() {
        return Err(anyhow::anyhow!(
            "`--` collapses from one anchor to the worktree; pass a single point (e.g. `sheaf squash @~12 -- -m \"...\"`), not `A..B`"
        )
        .into());
    }

    let socket = sheaf_core::paths::control_socket_path();
    let client = Client::connect(&socket, Duration::from_secs(2)).ok();
    if !git_args.is_empty() && client.is_none() {
        return Err(anyhow::anyhow!(
            "squash `--` requires the running daemon: the frame stamp writes a checkpoint (start sheafd, or use `sheaf service install`)"
        )
        .into());
    }
    let mut ctx = SquashCtx::new(&root, client)?;

    let explicit = range.map(|_| from).filter(|r| !r.is_empty());
    let anchor = squash_anchor(&mut ctx, explicit, !git_args.is_empty())?;
    let plan = squash_plan(&mut ctx, anchor, to.map(str::to_owned))?;

    if !git_args.is_empty() {
        squash_commit(&mut ctx, plan, git_args, as_json)
    } else {
        squash_print_preview(&mut ctx, &plan, as_json);
        Ok(())
    }
}

/// How the preview labels histories containing projected frames: the
/// timeline diff stays attribution; the git-uncommitted section is what
/// the next `--` would actually commit.
struct PartialAwareness {
    partial_frames: usize,
    uncommitted_stat: Option<String>,
}

fn partial_awareness(ctx: &mut SquashCtx, _plan: &SquashPlan) -> PartialAwareness {
    let (frames, _torn) = sheaf_core::store::read_frames(ctx.root).unwrap_or_default();
    let partial_frames = sheaf_core::store::partial_frame_count(&frames);
    if partial_frames == 0 {
        return PartialAwareness {
            partial_frames,
            uncommitted_stat: None,
        };
    }
    // Read-only git query — the one git touch the preview allows, since it only
    // reads: with projected frames in history, anchor→tip timeline content
    // overcounts what git still holds uncommitted; show the real remainder
    // beside it.
    let uncommitted_stat = git(ctx.root, ["diff", "--stat", "HEAD"])
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .filter(|s| !s.is_empty());
    PartialAwareness {
        partial_frames,
        uncommitted_stat,
    }
}

/// Preview: everything a human needs to decide whether to pass `--`.
fn squash_print_preview(ctx: &mut SquashCtx, plan: &SquashPlan, as_json: bool) {
    let partial = partial_awareness(ctx, plan);
    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "anchor": {
                    "label": plan.anchor.label,
                    "reference": plan.anchor.reference,
                    "capture_id": plan.anchor.capture_id,
                    "explicit": plan.anchor.user_ref,
                },
                "to": plan.to_ref,
                "span": plan.stats,
                "diff": plan.diff,
                "draft_subject": sheaf_core::store::draft_subject(&plan.diff),
                "draft_message": sheaf_core::store::draft_message(&plan.stats, &plan.diff),
                "degraded": plan.degraded,
                "partial_frames": partial.partial_frames,
                "git_uncommitted": partial.uncommitted_stat,
            }))
            .expect("serialize squash preview")
        );
        return;
    }
    if plan.degraded {
        eprintln!("note: daemon unavailable; this preview reads a read-only store snapshot");
    }
    println!("squash preview — nothing runs, nothing is written");
    println!("anchor:      {}", plan.anchor.label);
    let target = plan
        .to_ref
        .as_deref()
        .map(|r| r.to_owned())
        .unwrap_or_else(|| "worktree".to_owned());
    match (plan.stats.count, plan.stats.first_ms, plan.stats.last_ms) {
        (0, _, _) if !plan.stats.partial => {
            println!("span:        no captures ({target} vs anchor)")
        }
        (count, Some(first), Some(last)) => {
            let fmt = |ms: i64| {
                DateTime::<Utc>::from_timestamp_millis(ms)
                    .map(|utc| {
                        let local: DateTime<Local> = utc.into();
                        local.format("%Y-%m-%d %H:%M").to_string()
                    })
                    .unwrap_or_else(|| "?".into())
            };
            let noun = if count == 1 { "capture" } else { "captures" };
            println!(
                "span:        {count} {noun}, {} → {}",
                fmt(first),
                fmt(last)
            );
        }
        _ => println!("span:        capture stats unavailable (partial)"),
    }
    if !plan.stats.checkpoints.is_empty() {
        println!("checkpoints crossed: {}", plan.stats.checkpoints.join(", "));
    }
    if plan.stats.restores > 0 {
        println!("restores crossed:    {}", plan.stats.restores);
    }
    println!();
    print_diff_stat(&plan.diff);
    println!();
    println!("draft commit message");
    println!("----------------------------------------");
    print!(
        "{}",
        sheaf_core::store::draft_message(&plan.stats, &plan.diff)
    );
    println!("----------------------------------------");
    if partial.partial_frames > 0 {
        println!();
        println!(
            "projected frames: {} in history — the diff above is timeline attribution, \
             not uncommitted content",
            partial.partial_frames
        );
        match &partial.uncommitted_stat {
            Some(stat) => {
                println!("still-uncommitted git change (HEAD → worktree):");
                for line in stat.lines() {
                    println!("  {line}");
                }
            }
            None => println!("still-uncommitted git change: none (worktree matches HEAD)"),
        }
    }
    let anchor_hint = plan
        .anchor
        .user_ref
        .as_deref()
        .map(|r| format!("{r} "))
        .unwrap_or_default();
    println!();
    println!("to commit this span: sheaf squash {anchor_hint}-- <git commit options>");
}

/// The `--` path: the user typed the sanction; stage, commit,
/// stamp. Nothing here runs before the timeline has caught up with the
/// worktree, so the frame invariant — commit content == anchor..@ — holds.
fn squash_commit(
    ctx: &mut SquashCtx,
    plan: SquashPlan,
    git_args: &[String],
    as_json: bool,
) -> CliResult {
    // 1. Let the daemon capture pending edits: `git commit` photographs the
    // worktree, and the stamp pins `@`, so the two must agree.
    let mut caught_up = false;
    let deadline = std::time::Instant::now() + SQUASH_CATCHUP;
    while let Some(client) = ctx.client.as_mut() {
        let reply = client.call(
            "diff",
            Some(ctx.root),
            serde_json::json!({ "from": "@", "to": null, "paths": [] }),
            None,
        );
        match reply {
            Ok(reply) if reply.response.ok => {
                let pending = reply
                    .response
                    .result
                    .as_ref()
                    .and_then(|v| v.get("diff"))
                    .and_then(|d| d.get("entries"))
                    .and_then(serde_json::Value::as_array)
                    .map(|a| a.len())
                    .unwrap_or(0);
                if pending == 0 {
                    caught_up = true;
                    break;
                }
                if std::time::Instant::now() >= deadline {
                    eprintln!(
                        "note: worktree still has {pending} uncaptured file(s); committing anyway (the next frame absorbs the tail)"
                    );
                    break;
                }
                std::thread::sleep(Duration::from_millis(150));
            }
            Ok(reply) => return Err(anyhow::anyhow!(ipc_error_text(&reply.response)).into()),
            Err(e) => return Err(anyhow::anyhow!("daemon lost mid-squash: {e}").into()),
        }
    }

    // 2. Stage the collapse. The store must stay out of the index: sheaf
    // maintains `.sheaf/` in `.git/info/exclude` at init, and a squashing
    // user may have removed that guard — refuse rather than stage the store.
    // (Probing a file inside: gitignore directory patterns match contents,
    // and `check-ignore .sheaf` itself answers "not ignored".)
    let ignored = git(ctx.root, ["check-ignore", "--quiet", ".sheaf/config.toml"])
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !ignored {
        return Err(anyhow::anyhow!(
            "git does not ignore `.sheaf` here — add it to .git/info/exclude (or .gitignore) before squashing, so the store never lands in a commit"
        )
        .into());
    }
    let staged = git(ctx.root, ["add", "--all"])?;
    if !staged.status.success() {
        return Err(anyhow::anyhow!(
            "git add failed: {}",
            String::from_utf8_lossy(&staged.stderr).trim()
        )
        .into());
    }

    // 3. Commit, forwarding the passthrough verbatim. Without a message
    // flag the draft seeds git's editor as the template.
    let mut commit_args: Vec<String> = vec!["commit".into()];
    commit_args.extend(git_args.iter().cloned());
    let mut template: Option<std::path::PathBuf> = None;
    if !sheaf_core::store::passthrough_has_message(git_args) {
        let path = std::env::temp_dir().join(format!("sheaf-squash-{}.msg", std::process::id()));
        std::fs::write(
            &path,
            sheaf_core::store::draft_message(&plan.stats, &plan.diff),
        )
        .context("write squash draft template")?;
        commit_args.push("-t".into());
        commit_args.push(path.to_string_lossy().into_owned());
        template = Some(path);
    }
    let mut_args: Vec<&str> = commit_args.iter().map(String::as_str).collect();
    let status = std::process::Command::new("git")
        .args(mut_args)
        .current_dir(ctx.root)
        .status()
        .map_err(|e| anyhow::anyhow!("running git commit: {e}"));
    let commit_result = match status {
        Ok(status) if status.success() => Ok(()),
        Ok(status) => Err(anyhow::anyhow!(
            "git commit exited with {} — nothing stamped",
            status.code().unwrap_or(-1)
        )),
        Err(e) => Err(e),
    };
    if let Some(path) = template {
        let _ = std::fs::remove_file(path);
    }
    commit_result?;

    // 4. Read back the commit identity.
    let head = git(ctx.root, ["rev-parse", "HEAD"])?;
    if !head.status.success() {
        return Err(anyhow::anyhow!(
            "commit succeeded but `git rev-parse HEAD` failed: {}",
            String::from_utf8_lossy(&head.stderr).trim()
        )
        .into());
    }
    let sha = String::from_utf8_lossy(&head.stdout).trim().to_owned();
    let short = git(ctx.root, ["rev-parse", "--short", "HEAD"])
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_else(|_| sha[..12.min(sha.len())].to_owned());
    let committed_at_ms = git(ctx.root, ["show", "-s", "--format=%cI", "HEAD"])
        .ok()
        .and_then(|o| {
            String::from_utf8_lossy(&o.stdout)
                .trim()
                .to_owned()
                .parse::<DateTime<Utc>>()
                .ok()
        })
        .map(|dt| dt.timestamp_millis())
        .unwrap_or_else(|| Utc::now().timestamp_millis());

    // 5. Stamp: checkpoint at `@`, then the append-only frame record.
    let name = format!("git-{short}");
    let stamp = ctx.call(
        "checkpoint.create",
        serde_json::json!({ "name": name, "at": null }),
    );
    let tip_capture_id = match stamp {
        Ok(value) => value
            .get("checkpoint")
            .and_then(|c| c.get("capture_id"))
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned),
        Err(e) => {
            eprintln!(
                "sheaf: commit {short} succeeded but the frame checkpoint failed: {e:#}\nhint: finish it by hand with `sheaf checkpoint create {name}`"
            );
            None
        }
    };
    let frame = sheaf_core::store::CommitFrame {
        v: 1,
        sha,
        short_sha: short.clone(),
        anchor_capture_id: plan.anchor.capture_id.clone(),
        anchor_ref: plan.anchor.user_ref.clone(),
        tip_capture_id,
        committed_at_ms,
        stamped_at_ms: Utc::now().timestamp_millis(),
        captures: plan.stats.count,
        files: plan.diff.entries.len(),
        added: plan.diff.entries.iter().map(|e| e.added_lines).sum(),
        removed: plan.diff.entries.iter().map(|e| e.removed_lines).sum(),
        restores_crossed: plan.stats.restores,
        subject: sheaf_core::store::draft_subject(&plan.diff),
        kind: sheaf_core::store::FrameKind::Complete,
        projection: None,
    };
    let frame_index = {
        let (existing, torn) = sheaf_core::store::read_frames(ctx.root).unwrap_or_default();
        if torn > 0 {
            eprintln!("note: dropped {torn} torn trailing frame record(s) before appending");
        }
        existing.len() + 1
    };
    sheaf_core::store::append_frame(ctx.root, &frame)
        .context("append commit frame to .sheaf/frames.jsonl")?;

    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "caught_up": caught_up,
                "commit": frame.sha,
                "short_sha": frame.short_sha,
                "checkpoint": frame.checkpoint_name(),
                "tip_capture_id": frame.tip_capture_id,
                "anchor_capture_id": frame.anchor_capture_id,
                "frame": frame,
                "frame_index": frame_index,
            }))
            .expect("serialize squash result")
        );
    } else {
        println!(
            "caught up:   {}",
            if caught_up {
                "worktree == @ (timeline current)"
            } else {
                "no — committed with uncaptured tail"
            }
        );
        println!("anchor:      {}", plan.anchor.label);
        println!("committed:   {short}  {}", frame.subject);
        println!(
            "stamped:     checkpoint {} at capture {}",
            frame.checkpoint_name(),
            frame
                .tip_capture_id
                .as_deref()
                .map(|id| &id[..12.min(id.len())])
                .unwrap_or("(frontier)")
        );
        println!(
            "frame:       #{frame_index} → .sheaf/frames.jsonl ({} captures, {} files, +{}/-{})",
            frame.captures, frame.files, frame.added, frame.removed
        );
    }
    Ok(())
}

// ------------------------------------------------------------ smart squash

/// `sheaf squash --selection <file|-> [-- git commit options]`. Preview by
/// default; the `--` path stages only the selected patches, commits, and
/// records a projected frame.
fn cmd_smart_squash(
    project: Option<&Path>,
    selection_spec: &str,
    git_args: &[String],
    as_json: bool,
) -> CliResult {
    let root = timeline_root(project)?;
    let raw = if selection_spec == "-" {
        let mut buffer = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut buffer)
            .context("read selection payload from stdin")?;
        buffer
    } else {
        std::fs::read_to_string(selection_spec)
            .with_context(|| format!("read selection payload `{selection_spec}`"))?
    };
    let selections = parse_selection_payload(&raw)?;
    if selections.is_empty() {
        return Err(anyhow::anyhow!("selection payload holds no handles").into());
    }

    let socket = sheaf_core::paths::control_socket_path();
    let client = Client::connect(&socket, Duration::from_secs(2)).ok();
    if !git_args.is_empty() && client.is_none() {
        return Err(anyhow::anyhow!(
            "smart squash `--` requires the running daemon: the projected frame needs \
             capture quiescence and possibly a complete-frame stamp (start sheafd, or \
             use `sheaf service install`)"
        )
        .into());
    }
    let mutating = !git_args.is_empty();

    // Git is a hard prerequisite on both paths: HEAD is one side of the
    // selected patch.
    if !git(&root, ["rev-parse", "--verify", "HEAD"])
        .map(|o| o.status.success())
        .unwrap_or(false)
    {
        return Err(anyhow::anyhow!(
            "smart squash needs at least one git commit to diff against (make the \
             first commit with ordinary `sheaf squash --`)"
        )
        .into());
    }

    let mut ctx = SquashCtx::new(&root, client)?;

    if mutating {
        // Gates before anything is planned or written: a clean
        // index makes staged-tree verification exact, and capture
        // quiescence makes the projected frame's audit tip truthful.
        if !git(&root, ["diff", "--cached", "--quiet"])
            .map(|o| o.status.success())
            .unwrap_or(false)
        {
            return Err(anyhow::anyhow!(
                "the git index is not clean — commit, unstage, or stash staged work \
                 before smart squash (the first mutating release refuses rather than \
                 silently include it)"
            )
            .into());
        }
        smart_wait_caught_up(&mut ctx)?;
    }

    let (plan, degraded) = smart_plan_via(&mut ctx, &root, &selections)?;
    if !plan.applicable() {
        print_smart_plan(
            &plan,
            &sheaf_core::store::SmartAttribution::default(),
            degraded,
            as_json,
        );
        return Err(ExitErr::SilentCode(EXIT_RESTORE_BLOCKED));
    }

    // Timeline attribution: captures between the frame anchor and the tip
    // that touched the selection paths. Separate truth from the git patch.
    let (anchor_capture_id, attribution) = smart_attribution_for(&mut ctx, &plan);
    if !mutating {
        print_smart_plan(&plan, &attribution, degraded, as_json);
        return Ok(());
    }
    smart_squash_commit(
        &mut ctx,
        &root,
        plan,
        anchor_capture_id,
        &attribution,
        git_args,
        as_json,
    )
}

/// HEAD-side content of one path; `None` when the path is not in HEAD (or
/// not readable as text — either way the planner refuses that file).
fn head_text_of(root: &Path, path: &str) -> Option<String> {
    let out = git(root, ["show", &format!("HEAD:{path}")]).ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8(out.stdout).ok()
}

/// Plan through the daemon (two-phase: candidate paths, then plan) or, in
/// degraded mode, straight from the read-only reader with a lazy git
/// resolver.
fn smart_plan_via(
    ctx: &mut SquashCtx,
    root: &Path,
    selections: &[sheaf_core::store::SelectionHandle],
) -> Result<(sheaf_core::store::SmartPlan, bool)> {
    if let Some(client) = ctx.client.as_mut() {
        let resolve = client.call(
            "smart.plan",
            Some(root),
            serde_json::json!({ "selections": selections }),
            None,
        )?;
        let reply = &resolve.response;
        if !reply.ok {
            return Err(anyhow::anyhow!(ipc_error_text(reply)));
        }
        let result = reply
            .result
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("smart.plan returned no result"))?;
        let paths: Vec<String> = serde_json::from_value(result["paths"].clone())
            .context("smart.plan resolve phase returned no paths")?;
        let mut head_texts = std::collections::BTreeMap::new();
        for path in paths {
            if let Some(text) = head_text_of(root, &path) {
                head_texts.insert(path, text);
            }
        }
        let plan_call = client.call(
            "smart.plan",
            Some(root),
            serde_json::json!({ "selections": selections, "head_texts": head_texts }),
            None,
        )?;
        let reply = &plan_call.response;
        if !reply.ok {
            return Err(anyhow::anyhow!(ipc_error_text(reply)));
        }
        let result = reply
            .result
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("smart.plan returned no result"))?;
        let plan: sheaf_core::store::SmartPlan = serde_json::from_value(result["plan"].clone())
            .context("smart.plan returned an unreadable plan")?;
        Ok((plan, false))
    } else {
        let reader = ctx.reader()?;
        let plan = reader.plan_smart_degraded(selections, &mut |path| head_text_of(root, path))?;
        Ok((plan, true))
    }
}

/// Require the timeline to be quiescent before a mutating smart squash:
/// the projected frame's audit tip must name a capture that actually saw
/// the worktree bytes being committed. Unlike ordinary squash this is a
/// gate, not best-effort.
fn smart_wait_caught_up(ctx: &mut SquashCtx) -> Result<()> {
    let deadline = std::time::Instant::now() + SQUASH_CATCHUP;
    loop {
        let reply = ctx.call(
            "diff",
            serde_json::json!({ "from": "@", "to": null, "paths": [] }),
        )?;
        let pending = reply
            .get("diff")
            .and_then(|d| d.get("entries"))
            .and_then(serde_json::Value::as_array)
            .map(|a| a.len())
            .unwrap_or(0);
        if pending == 0 {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            anyhow::bail!(
                "the timeline has {pending} uncaptured file(s); smart squash refuses to \
                 commit a patch its own audit trail never saw — let the daemon settle \
                 (or `sheaf status`) and retry"
            );
        }
        std::thread::sleep(Duration::from_millis(150));
    }
}

/// Captures between the frame anchor and the tip that touched any
/// selection path, plus the anchor capture the projected frame carries as
/// its pass-through span. Attribution only — never the commit
/// content.
fn smart_attribution_for(
    ctx: &mut SquashCtx,
    plan: &sheaf_core::store::SmartPlan,
) -> (Option<String>, sheaf_core::store::SmartAttribution) {
    let paths: std::collections::BTreeSet<String> =
        plan.selections.iter().map(|s| s.path.clone()).collect();
    let checkpoints = ctx.checkpoints().ok();
    let anchor_id = checkpoints
        .as_ref()
        .and_then(|cps| sheaf_core::store::frame_anchor(cps))
        .and_then(|cp| cp.capture_id.clone());
    if checkpoints.is_none() {
        return (None, sheaf_core::store::SmartAttribution::default());
    }
    // With a frame anchor the span is exact. Without one (no commits
    // stamped yet), attribution falls back to a bounded recent window so
    // the drafted message still carries capture context.
    let mut last_error = None;
    if let Some(anchor_id) = anchor_id {
        let result = sheaf_core::store::collect_span(Some(&anchor_id), None, |cursor| {
            ctx.lineage_page(cursor).map_err(|e| {
                last_error = Some(format!("{e:#}"));
            })
        });
        return match result {
            Ok((span, _)) => (
                Some(anchor_id),
                sheaf_core::store::smart_attribution(&span, &paths),
            ),
            Err(()) => (None, sheaf_core::store::SmartAttribution::default()),
        };
    }
    let mut window = Vec::new();
    let mut cursor: Option<String> = None;
    while window.len() < 200 {
        let Ok(page) = ctx.lineage_page(cursor.as_deref()) else {
            break;
        };
        if page.is_empty() {
            break;
        }
        cursor = page.last().map(|c| c.id.clone());
        window.extend(page);
    }
    (None, sheaf_core::store::smart_attribution(&window, &paths))
}

fn print_smart_plan(
    plan: &sheaf_core::store::SmartPlan,
    attribution: &sheaf_core::store::SmartAttribution,
    degraded: bool,
    as_json: bool,
) {
    if as_json {
        // A preview carries no SHA fields of any kind: every digest in the
        // plan is an internal content hash, none is a git identity, and a
        // reported "staged" hash has already been mistaken for the blob
        // the commit would create (it never matches — the blob exists
        // only after `git hash-object` at commit time). Real SHAs appear
        // in the commit output and the frame, after the commit exists.
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "smart_squash": true,
                "selections": plan.selections.iter().map(|s| serde_json::json!({
                    "selection_id": s.selection_id,
                    "kind": s.kind,
                    "path": s.path,
                    "head": s.head,
                    "worktree": s.worktree,
                    "staged_bytes": s.staged_bytes,
                    "retired_bytes": s.retired_bytes,
                })).collect::<Vec<_>>(),
                "files": plan.files.iter().map(|f| serde_json::json!({
                    "path": f.path,
                    "added_bytes": f.added_bytes,
                    "retired_bytes": f.retired_bytes,
                })).collect::<Vec<_>>(),
                "conflicts": plan.conflicts,
                "unchanged": plan.unchanged,
                "attribution": attribution,
                "applicable": plan.applicable(),
                "draft_subject": sheaf_core::store::draft_smart_subject(plan),
                "draft_message": sheaf_core::store::draft_smart_message(plan, attribution),
                "degraded": degraded,
            }))
            .expect("serialize smart plan")
        );
        return;
    }
    if degraded {
        eprintln!("note: daemon unavailable; this preview reads a read-only store snapshot");
    }
    println!("smart squash preview — nothing runs, nothing is written");
    println!(
        "selections:  {} staged, {} already current",
        plan.selections.len(),
        plan.unchanged
    );
    for selection in &plan.selections {
        let kind = match selection.kind {
            sheaf_core::store::SmartKind::Replace => "replace",
            sheaf_core::store::SmartKind::Insert => "insert",
            sheaf_core::store::SmartKind::Delete => "delete",
        };
        println!(
            "  {:?} {kind} {}  head {}..{}  worktree {}..{}  +{}/-{}",
            &selection.selection_id[..12.min(selection.selection_id.len())],
            selection.path,
            selection.head.start,
            selection.head.end,
            selection.worktree.start,
            selection.worktree.end,
            selection.staged_bytes,
            selection.retired_bytes,
        );
    }
    if !plan.conflicts.is_empty() {
        println!();
        for conflict in &plan.conflicts {
            let side = conflict
                .side
                .map(|s| match s {
                    sheaf_core::store::SmartSide::Head => " @HEAD",
                    sheaf_core::store::SmartSide::Worktree => " @worktree",
                })
                .unwrap_or("");
            println!(
                "refusal:     {:?}{}{} — {}",
                conflict.condition,
                side,
                conflict
                    .path
                    .as_deref()
                    .map(|p| format!(" `{p}`"))
                    .unwrap_or_default(),
                conflict.detail
            );
            for candidate in conflict.candidates.iter().take(4) {
                println!(
                    "  candidate   {} {}..{}",
                    candidate.path, candidate.range.start, candidate.range.end
                );
            }
        }
        println!("nothing will be committed while any selection refuses");
    }
    println!();
    println!("selected git patch (HEAD → staged)");
    for file in &plan.files {
        // No blob SHA here: the preview stages nothing, so no git blob
        // exists to name. Sizes only — identities come with the commit.
        println!(
            "  {}  +{}/-{} bytes",
            file.path, file.added_bytes, file.retired_bytes
        );
    }
    println!();
    println!("timeline attribution (captures touching the selection paths)");
    println!(
        "  {} capture(s){}{}",
        attribution.captures,
        if attribution.restores > 0 {
            format!(", {} restore-crossed", attribution.restores)
        } else {
            String::new()
        },
        if attribution.checkpoints.is_empty() {
            String::new()
        } else {
            format!(", checkpoints: {}", attribution.checkpoints.join(", "))
        },
    );
    println!();
    println!("draft commit message");
    println!("----------------------------------------");
    print!(
        "{}",
        sheaf_core::store::draft_smart_message(plan, attribution)
    );
    println!("----------------------------------------");
}

/// Run git with bytes on stdin; returns captured output.
fn git_stdin(
    root: &Path,
    args: impl IntoIterator<Item = impl AsRef<std::ffi::OsStr>>,
    input: &[u8],
) -> Result<std::process::Output> {
    use std::io::Write as _;
    use std::process::Stdio;
    let mut child = std::process::Command::new("git")
        .args(args)
        .current_dir(root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| anyhow::anyhow!("running git: {e} (is git installed?)"))?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(input)?;
    }
    child
        .wait_with_output()
        .map_err(|e| anyhow::anyhow!("waiting on git: {e}"))
}

fn smart_squash_commit(
    ctx: &mut SquashCtx,
    root: &Path,
    plan: sheaf_core::store::SmartPlan,
    anchor_capture_id: Option<String>,
    attribution: &sheaf_core::store::SmartAttribution,
    git_args: &[String],
    as_json: bool,
) -> CliResult {
    // The store must stay out of the index (same guard as ordinary squash).
    let ignored = git(root, ["check-ignore", "--quiet", ".sheaf/config.toml"])
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !ignored {
        return Err(anyhow::anyhow!(
            "git does not ignore `.sheaf` here — add it to .git/info/exclude (or .gitignore) \
             before smart squash, so the store never lands in a commit"
        )
        .into());
    }

    // Pre-commit identity of everything the projection records.
    let parent_sha = text_out(git(root, ["rev-parse", "HEAD"])?, "rev-parse HEAD")?;
    let git_tree_before = text_out(git(root, ["rev-parse", "HEAD^{tree}"])?, "tree of HEAD")?;

    // Stage only the selected patches: write each staged blob, then point
    // the index at it. The worktree is never touched, so unrelated dirty
    // edits stay dirty.
    let mut digest_entries = Vec::new();
    for file in &plan.files {
        let mode_line = text_out(
            git(root, ["ls-files", "-s", "--", &file.path])?,
            "git ls-files mode",
        )
        .unwrap_or_default();
        let mode = mode_line
            .split_whitespace()
            .next()
            .unwrap_or("100644")
            .to_owned();
        let old_blob = text_out(
            git(root, ["rev-parse", &format!("HEAD:{}", file.path)])?,
            "HEAD blob of selected path",
        )
        .unwrap_or_default();
        let hash_out = git_stdin(
            root,
            ["hash-object", "-w", "--stdin"],
            file.staged_text.as_bytes(),
        )?;
        if !hash_out.status.success() {
            return Err(anyhow::anyhow!(
                "git hash-object failed for `{}`: {} — index untouched, nothing committed",
                file.path,
                String::from_utf8_lossy(&hash_out.stderr).trim()
            )
            .into());
        }
        let blob = String::from_utf8_lossy(&hash_out.stdout).trim().to_owned();
        let staged = git(
            root,
            [
                "update-index",
                "--add",
                "--cacheinfo",
                &format!("{mode},{blob},{}", file.path),
            ],
        )?;
        if !staged.status.success() {
            let _ = git(root, ["reset"]);
            return Err(anyhow::anyhow!(
                "git update-index failed for `{}`: {} — index reset, nothing committed",
                file.path,
                String::from_utf8_lossy(&staged.stderr).trim()
            )
            .into());
        }
        digest_entries.push((file.path.clone(), old_blob, blob));
    }

    // Verify the staged tree contains exactly the selected patch: paths
    // and blob shas must match the plan one for one.
    let staged_tree = text_out(git(root, ["write-tree"])?, "git write-tree")?;
    let diff_tree = git(
        root,
        ["diff-tree", "-r", "--no-renames", "HEAD", &staged_tree],
    )?;
    if !diff_tree.status.success() {
        let _ = git(root, ["reset"]);
        return Err(anyhow::anyhow!(
            "git diff-tree failed: {} — index reset, nothing committed",
            String::from_utf8_lossy(&diff_tree.stderr).trim()
        )
        .into());
    }
    let mut seen = std::collections::BTreeMap::new();
    for line in String::from_utf8_lossy(&diff_tree.stdout).lines() {
        // :<oldmode> <newmode> <oldblob> <newblob> <status>\t<path>
        let Some((meta, path)) = line.split_once('\t') else {
            continue;
        };
        let fields: Vec<&str> = meta.split_whitespace().collect();
        if fields.len() >= 4 && meta.starts_with(':') {
            seen.insert(path.to_owned(), fields[3].to_owned());
        }
    }
    let expected: std::collections::BTreeMap<String, String> = digest_entries
        .iter()
        .map(|(p, _, b)| (p.clone(), b.clone()))
        .collect();
    if seen != expected {
        let _ = git(root, ["reset"]);
        return Err(anyhow::anyhow!(
            "the staged tree does not match the selected patch exactly \
             (staged {:?}, planned {:?}) — index reset, nothing committed",
            seen.keys().collect::<Vec<_>>(),
            expected.keys().collect::<Vec<_>>()
        )
        .into());
    }
    let git_patch_sha256 = sheaf_core::store::patch_digest(&digest_entries);

    // Commit, forwarding the passthrough verbatim; the smart draft seeds
    // the editor template when no message flag is present.
    let mut commit_args: Vec<String> = vec!["commit".into()];
    commit_args.extend(git_args.iter().cloned());
    let mut template: Option<std::path::PathBuf> = None;
    if !sheaf_core::store::passthrough_has_message(git_args) {
        let path = std::env::temp_dir().join(format!("sheaf-smart-{}.msg", std::process::id()));
        std::fs::write(
            &path,
            sheaf_core::store::draft_smart_message(&plan, attribution),
        )
        .context("write smart-squash draft template")?;
        commit_args.push("-t".into());
        commit_args.push(path.to_string_lossy().into_owned());
        template = Some(path);
    }
    let mut_args: Vec<&str> = commit_args.iter().map(String::as_str).collect();
    let has_message = sheaf_core::store::passthrough_has_message(git_args);
    // With a message, git needs no editor: capture its summary and route
    // it to stderr so stdout stays clean for `--json`. Without one, git
    // opens an editor and must inherit the terminal.
    let commit_result = if has_message {
        match std::process::Command::new("git")
            .args(&mut_args)
            .current_dir(root)
            .output()
        {
            Ok(out) if out.status.success() => {
                eprint!("{}", String::from_utf8_lossy(&out.stdout));
                eprint!("{}", String::from_utf8_lossy(&out.stderr));
                Ok(())
            }
            Ok(out) => Err(anyhow::anyhow!(
                "git commit exited with {} — the selection is still staged; inspect with \
                 `git diff --cached`, then `git commit` or `git reset`\n{}",
                out.status.code().unwrap_or(-1),
                String::from_utf8_lossy(&out.stderr).trim()
            )),
            Err(e) => Err(anyhow::anyhow!("running git commit: {e}")),
        }
    } else {
        match std::process::Command::new("git")
            .args(&mut_args)
            .current_dir(root)
            .status()
            .map_err(|e| anyhow::anyhow!("running git commit: {e}"))
        {
            Ok(status) if status.success() => Ok(()),
            Ok(status) => Err(anyhow::anyhow!(
                "git commit exited with {} — the selection is still staged; inspect with \
                 `git diff --cached`, then `git commit` or `git reset`",
                status.code().unwrap_or(-1)
            )),
            Err(e) => Err(e),
        }
    };
    if let Some(path) = template {
        let _ = std::fs::remove_file(path);
    }
    commit_result?;

    // Read back the commit identity and post-commit tree.
    let sha = text_out(git(root, ["rev-parse", "HEAD"])?, "rev-parse HEAD")?;
    let short = text_out(git(root, ["rev-parse", "--short", "HEAD"])?, "short sha")?;
    let git_tree_after = text_out(git(root, ["rev-parse", "HEAD^{tree}"])?, "tree of HEAD")?;
    let committed_at_ms = git(root, ["show", "-s", "--format=%cI", "HEAD"])
        .ok()
        .and_then(|o| {
            String::from_utf8_lossy(&o.stdout)
                .trim()
                .to_owned()
                .parse::<DateTime<Utc>>()
                .ok()
        })
        .map(|dt| dt.timestamp_millis())
        .unwrap_or_else(|| Utc::now().timestamp_millis());

    // Audit tip: the captured tip inspected at commit time.
    let audit_tip = ctx.resolve_capture_id("@");
    let selection_ids: Vec<String> = plan
        .selections
        .iter()
        .map(|s| s.selection_id.clone())
        .collect();

    // Complete-frame eligibility: only at a real three-way
    // equality point — commit tree, worktree tree, captured tip.
    let clean_porcelain = git(root, ["status", "--porcelain"])
        .map(|o| o.status.success() && String::from_utf8_lossy(&o.stdout).trim().is_empty())
        .unwrap_or(false);
    let captured_current = ctx
        .call(
            "diff",
            serde_json::json!({ "from": "@", "to": null, "paths": [] }),
        )
        .ok()
        .and_then(|reply| {
            reply
                .get("diff")
                .and_then(|d| d.get("entries"))
                .and_then(serde_json::Value::as_array)
                .map(|a| a.is_empty())
        })
        .unwrap_or(false);

    let mut frame = sheaf_core::store::CommitFrame {
        v: 1,
        sha: sha.clone(),
        short_sha: short.clone(),
        // The pass-through span anchor: the frame-anchor span
        // start when one exists, otherwise the captured state the partial
        // commit projected from — the point an ordinary squash resumes at.
        anchor_capture_id: anchor_capture_id.or_else(|| audit_tip.clone()),
        anchor_ref: Some("smart".into()),
        tip_capture_id: None,
        committed_at_ms,
        stamped_at_ms: Utc::now().timestamp_millis(),
        captures: attribution.captures,
        files: plan.files.len(),
        added: plan.files.iter().map(|f| f.added_bytes).sum(),
        removed: plan.files.iter().map(|f| f.retired_bytes).sum(),
        restores_crossed: attribution.restores,
        subject: sheaf_core::store::draft_smart_subject(&plan),
        kind: sheaf_core::store::FrameKind::Partial,
        projection: None,
    };
    let checkpoint;
    if clean_porcelain && captured_current {
        let name = format!("git-{short}");
        let stamp = ctx.call(
            "checkpoint.create",
            serde_json::json!({ "name": name, "at": null }),
        );
        let tip = stamp.ok().and_then(|v| {
            v.get("checkpoint")
                .and_then(|c| c.get("capture_id"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        });
        frame.kind = sheaf_core::store::FrameKind::Complete;
        frame.tip_capture_id = tip.clone();
        checkpoint = Some(name);
        if tip.is_none() {
            eprintln!(
                "sheaf: commit {short} converged, but the frame checkpoint failed; the \
                 frame stays complete-but-unstamped (not anchor-eligible) — finish it by \
                 hand with `sheaf checkpoint create git-{short}`"
            );
        }
    } else {
        frame = frame.into_partial(sheaf_core::store::Projection {
            parent_sha,
            git_tree_before,
            git_tree_after,
            selection_ids,
            patch_sha256: git_patch_sha256,
            tip_capture_id: audit_tip.clone().unwrap_or_default(),
        });
        checkpoint = None;
    }

    let frame_index = {
        let (existing, torn) = sheaf_core::store::read_frames(root).unwrap_or_default();
        if torn > 0 {
            eprintln!("note: dropped {torn} torn trailing frame record(s) before appending");
        }
        existing.len() + 1
    };
    sheaf_core::store::append_frame(root, &frame).context("append frame to .sheaf/frames.jsonl")?;

    if as_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "commit": frame.sha,
                "short_sha": frame.short_sha,
                "kind": if frame.kind == sheaf_core::store::FrameKind::Complete { "complete" } else { "partial" },
                "checkpoint": checkpoint,
                "tip_capture_id": frame.tip_capture_id,
                "audit_tip_capture_id": audit_tip,
                "patch_sha256": frame.projection.as_ref().map(|p| p.patch_sha256.clone()),
                "frame": frame,
                "frame_index": frame_index,
            }))
            .expect("serialize smart squash result")
        );
    } else {
        let kind_word = if frame.kind == sheaf_core::store::FrameKind::Complete {
            "complete"
        } else {
            "projected (partial)"
        };
        println!("committed:   {short}  {}", frame.subject);
        println!(
            "frame:       #{frame_index} {kind_word} → .sheaf/frames.jsonl ({} file(s), +{}/-{} bytes)",
            frame.files, frame.added, frame.removed
        );
        match (&checkpoint, &frame.tip_capture_id) {
            (Some(name), Some(tip)) => println!(
                "stamped:     checkpoint {name} at capture {}",
                &tip[..12.min(tip.len())]
            ),
            _ => println!(
                "audit tip:   capture {} (unrelated worktree edits stay uncommitted; no \
                 equality checkpoint was stamped)",
                audit_tip
                    .as_deref()
                    .map(|t| &t[..12.min(t.len())])
                    .unwrap_or("(frontier)")
            ),
        }
    }
    Ok(())
}

fn text_out(out: std::process::Output, what: &str) -> Result<String> {
    if !out.status.success() {
        anyhow::bail!(
            "{what} failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_owned())
}

// ----------------------------------------------------------------- service

/// systemd user unit for sheafd, used by the service lifecycle commands.
fn unit_file_text(daemon_path: &Path) -> String {
    format!(
        "\
[Unit]
Description=sheaf - flight recorder for your worktree
After=default.target

[Service]
ExecStart={daemon} run
Restart=on-failure
RestartSec=3s
# Clean shutdown flush: SIGTERM drains watcher tails and every project's
# debounce tail before exit; systemd's SIGKILL is the backstop only.
TimeoutStopSec=30s
KillSignal=SIGTERM

[Install]
WantedBy=default.target
",
        daemon = daemon_path.display()
    )
}

fn unit_file_path() -> Result<PathBuf> {
    let base = match std::env::var_os("XDG_CONFIG_HOME") {
        Some(x) if !x.is_empty() => PathBuf::from(x),
        _ => {
            let home = std::env::var_os("HOME")
                .map(PathBuf::from)
                .ok_or_else(|| anyhow::anyhow!("HOME not set and XDG_CONFIG_HOME unset"))?;
            home.join(".config")
        }
    };
    Ok(base.join("systemd/user/sheafd.service"))
}

/// The sheafd binary sitting next to the running `sheaf` CLI.
fn sibling_daemon_path() -> Result<PathBuf> {
    let exe = std::env::current_exe().context("resolve current executable")?;
    let dir = exe
        .parent()
        .ok_or_else(|| anyhow::anyhow!("executable has no parent directory"))?;
    let daemon = dir.join("sheafd");
    if !daemon.is_file() {
        anyhow::bail!(
            "sheafd not found next to `sheaf` (expected {}); install both binaries side by side",
            daemon.display()
        );
    }
    Ok(daemon)
}

fn run_systemctl(args: &[&str]) -> CliResult {
    let status = std::process::Command::new("systemctl")
        .arg("--user")
        .args(args)
        .status()
        .map_err(|e| anyhow::anyhow!("run systemctl --user {}: {e}", args.join(" ")))?;
    if !status.success() {
        return Err(
            anyhow::anyhow!("systemctl --user {} failed with {status}", args.join(" ")).into(),
        );
    }
    Ok(())
}

fn cmd_service_install(no_start: bool) -> CliResult {
    let daemon = sibling_daemon_path()?;
    let unit_path = unit_file_path()?;
    std::fs::create_dir_all(
        unit_path
            .parent()
            .ok_or_else(|| anyhow::anyhow!("unit path has no parent"))?,
    )
    .context("create systemd user unit directory")?;
    sheaf_core::store::atomic_write_public(&unit_path, unit_file_text(&daemon).as_bytes())
        .context("write systemd user unit file")?;
    println!("unit:    {}", unit_path.display());
    println!("daemon:  {}", daemon.display());
    println!("policy:  Restart=on-failure, RestartSec=3s, TimeoutStopSec=30s");
    if no_start {
        println!("next:    systemctl --user daemon-reload && systemctl --user enable --now sheafd.service");
        return Ok(());
    }
    run_systemctl(&["daemon-reload"])?;
    run_systemctl(&["enable", "--now", "sheafd.service"])?;
    println!("service: enabled and started");
    Ok(())
}

fn cmd_service_status() -> CliResult {
    let unit_path = unit_file_path()?;
    if unit_path.exists() {
        println!("unit:    {} (present)", unit_path.display());
    } else {
        println!("unit:    {} (not installed)", unit_path.display());
    }
    let _ = std::process::Command::new("systemctl")
        .arg("--user")
        .args(["is-active", "sheafd.service"])
        .status()
        .map_err(|e| anyhow::anyhow!("run systemctl --user is-active: {e}"))?;
    Ok(())
}

fn cmd_service_remove() -> CliResult {
    // Stop first so a live daemon does not resurrect via the unit.
    let _ = std::process::Command::new("systemctl")
        .arg("--user")
        .args(["disable", "--now", "sheafd.service"])
        .status();
    let unit_path = unit_file_path()?;
    match std::fs::remove_file(&unit_path) {
        Ok(()) => {
            let _ = std::process::Command::new("systemctl")
                .arg("--user")
                .arg("daemon-reload")
                .status();
            println!("unit removed: {}", unit_path.display());
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            println!("unit was not installed");
        }
        Err(e) => return Err(anyhow::anyhow!("remove {}: {e}", unit_path.display()).into()),
    }
    Ok(())
}

fn ipc_error_text(response: &sheaf_core::ipc::Response) -> String {
    response
        .error
        .as_ref()
        .map(|e| format!("{}: {}", e.code, e.message))
        .unwrap_or_else(|| "unknown daemon error".to_owned())
}

fn degraded_note() {
    println!(
        "note:          read-only fallback active: `log`, `checkpoint list`, and `restore --dry-run` still work"
    );
}

fn yn(b: bool) -> &'static str {
    if b {
        "yes"
    } else {
        "no"
    }
}

// Silence unused import when PROTO_* surface changes; keeps handshake text honest.
#[allow(dead_code)]
const _: (u32, u32) = (PROTO_MAJOR, PROTO_MINOR);

#[cfg(test)]
mod log_view_tests {
    use super::*;

    fn s(items: &[&str]) -> Vec<String> {
        items.iter().map(|x| x.to_string()).collect()
    }

    #[test]
    fn short_lists_are_joined_whole() {
        assert_eq!(fit_paths(&s(&["a.rs", "b.rs"]), 80), "a.rs, b.rs");
    }

    #[test]
    fn empty_paths_show_the_metadata_marker() {
        assert_eq!(fit_paths(&[], 80), "(metadata)");
    }

    #[test]
    fn overflow_folds_into_the_more_marker() {
        let paths = s(&["aaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbb", "cccccccccccccccc"]);
        // Wide enough for two paths plus the marker, not three.
        let out = fit_paths(&paths, 50);
        assert_eq!(out, "aaaaaaaaaaaaaaaa, bbbbbbbbbbbbbbbb, … +1 more");
        assert!(out.chars().count() <= 50);
    }

    #[test]
    fn a_single_oversized_path_is_left_for_the_terminal_to_wrap() {
        let long = "x".repeat(200);
        let paths = vec![long.clone(), "b.rs".to_string()];
        let out = fit_paths(&paths, 30);
        assert!(out.starts_with(&long), "first path must survive: {out}");
        assert!(out.contains("+1 more"));
    }

    #[test]
    fn tiny_budgets_keep_the_marker_readable() {
        let out = fit_paths(&s(&["a.rs", "b.rs", "c.rs"]), 8);
        assert_eq!(out, "a.rs, … +2 more");
    }

    #[test]
    fn never_and_always_do_not_consult_the_environment() {
        assert!(!ColorWhen::Never.enabled(true));
        assert!(ColorWhen::Always.enabled(false));
    }

    #[test]
    fn auto_color_requires_a_terminal() {
        // Piped output is never auto-colored (the NO_COLOR branch is env
        // state; left untested to stay parallel-safe).
        assert!(!ColorWhen::Auto.enabled(false));
    }
}

/// Unit fixtures for the grep CLI layer: the `--after` cursor grammar, the
/// IPC wire conversion, the streamed-report reassembly, and the human row
/// renderers. These run the exact functions `cmd_grep` drives, without a
/// store or daemon, so their contracts hold regardless of transport.
#[cfg(test)]
mod grep_cli_layer_tests {
    use super::*;

    pub(super) fn handle() -> sheaf_core::store::SelectionHandle {
        sheaf_core::store::SelectionHandle {
            version: 1,
            source_frontier: "frontier-abc".to_owned(),
            source_capture_id: Some("0123456789ab".to_owned()),
            historical_path: "src/a.rs".to_owned(),
            extent: sheaf_core::store::SelectionExtent::Match,
            range: sheaf_core::store::ByteRange { start: 0, end: 5 },
            selected_text_sha256: "s".repeat(64),
            before_context_sha256: "b".repeat(64),
            after_context_sha256: "a".repeat(64),
            query_fingerprint: "fp-1".to_owned(),
            semantic: None,
        }
    }

    pub(super) fn hit(kind: sheaf_core::store::LifecycleKind, on_current: bool) -> GrepHitAlias {
        sheaf_core::store::GrepHit {
            capture_id: "0123456789abcdef".to_owned(),
            frontier: "frontier-abc".to_owned(),
            timestamp_ms: 0,
            lineage_id: "lineage".to_owned(),
            on_current,
            path: "src/a.rs".to_owned(),
            kind,
            line: 3,
            column: 7,
            occurrence_id: "occ-0000000000001".to_owned(),
            episode_id: Some("ep1:abc123".to_owned()),
            preview: "let needle = 1;".to_owned(),
            handle: handle(),
            handle_id: "sel-000000000001".to_owned(),
        }
    }

    type GrepHitAlias = sheaf_core::store::GrepHit;

    /// Render an `ExitErr` for assertions without requiring `Debug`.
    pub(super) fn fatal(e: ExitErr) -> String {
        match e {
            ExitErr::Fatal(inner) => format!("{inner:#}"),
            ExitErr::SilentCode(c) => format!("silent exit {c}"),
        }
    }

    pub(super) fn event(kind: sheaf_core::store::LifecycleKind) -> sheaf_core::store::GrepEvent {
        sheaf_core::store::GrepEvent {
            capture_id: "0123456789abcdef".to_owned(),
            frontier: "frontier-abc".to_owned(),
            timestamp_ms: 0,
            lineage_id: "lineage".to_owned(),
            on_current: false,
            kind,
            path: Some("src/a.rs".to_owned()),
            last_present_handle_id: Some("sel-000000000001".to_owned()),
            episode_id: Some("ep1:abc123".to_owned()),
            candidates: Some(vec!["cand-1".to_owned(), "cand-2".to_owned()]),
        }
    }

    pub(super) fn base_args(query: &str) -> GrepArgs<'_> {
        GrepArgs {
            project: None,
            query,
            history: false,
            at: None,
            path: None,
            line: None,
            column: None,
            episode: None,
            selection: None,
            follow: false,
            all: false,
            every_capture: false,
            extent: sheaf_core::store::SelectionExtent::Match,
            from: None,
            to: None,
            after: None,
            max_results: 1000,
            as_json: false,
            color: ColorWhen::Never,
        }
    }

    #[test]
    fn grep_extent_args_map_onto_selection_extents() {
        use sheaf_core::store::SelectionExtent;
        assert!(matches!(
            GrepExtentArg::Match.to_extent(),
            SelectionExtent::Match
        ));
        assert!(matches!(
            GrepExtentArg::Line.to_extent(),
            SelectionExtent::Line
        ));
    }

    #[test]
    fn extent_wire_uses_the_snake_case_names() {
        use sheaf_core::store::SelectionExtent;
        assert_eq!(extent_wire(SelectionExtent::Match), "match");
        assert_eq!(extent_wire(SelectionExtent::Line), "line");
        assert_eq!(extent_wire(SelectionExtent::Hunk), "hunk");
        assert_eq!(extent_wire(SelectionExtent::Symbol), "symbol");
    }

    #[test]
    fn outputs_json_follows_the_json_flag_per_command() {
        let log = Cmd::Log {
            project: None,
            path: None,
            follow: false,
            all: false,
            before: None,
            limit: 50,
            json: true,
        };
        assert!(log.outputs_json());

        let grep = Cmd::Grep {
            query: "x".to_owned(),
            history: false,
            at: None,
            path: None,
            line: None,
            column: None,
            episode: None,
            selection: None,
            follow: false,
            all: false,
            every_capture: false,
            extent: GrepExtentArg::Match,
            from: None,
            to: None,
            after: None,
            max_results: 1000,
            project: None,
            json: false,
        };
        assert!(!grep.outputs_json());

        let diff = Cmd::Diff {
            from: None,
            to: None,
            paths: Vec::new(),
            stat: false,
            json: true,
            exit_code: false,
            project: None,
        };
        assert!(diff.outputs_json());

        let status = Cmd::Status { path: None };
        assert!(!status.outputs_json());

        let init = Cmd::Init { path: None };
        assert!(!init.outputs_json());

        let checkpoint = Cmd::Checkpoint {
            command: Some(CheckpointCmd::List {
                project: None,
                json: true,
            }),
            name: None,
            at: None,
            project: None,
        };
        assert!(checkpoint.outputs_json());
    }

    #[test]
    fn paint_wraps_only_when_enabled() {
        assert_eq!(paint(false, "32", "present"), "present");
        assert_eq!(paint(true, "32", "present"), "\x1b[32mpresent\x1b[0m");
    }

    #[test]
    fn terminal_width_prefers_a_positive_columns_env() {
        // One sequential test: COLUMNS is process-global, so both branches
        // live here to stay parallel-safe with every other test.
        std::env::remove_var("COLUMNS");
        let width = terminal_width();
        assert!(width >= 1, "a width always resolves, got {width}");

        std::env::set_var("COLUMNS", "113");
        assert_eq!(terminal_width(), 113);

        // A zero COLUMNS is ignored as "no geometry".
        std::env::set_var("COLUMNS", "0");
        let width = terminal_width();
        assert!(width >= 1, "a width always resolves, got {width}");

        // A non-numeric COLUMNS parses as absent.
        std::env::set_var("COLUMNS", "wide");
        let width = terminal_width();
        assert!(width >= 1, "a width always resolves, got {width}");

        std::env::remove_var("COLUMNS");
    }

    #[test]
    fn parse_grep_after_accepts_the_three_cursor_grammars() {
        let plain = match parse_grep_after("1a2b3c") {
            Ok((capture, resume, index)) => (capture, resume, index),
            Err(_) => panic!("bare capture must parse"),
        };
        assert_eq!(plain, ("1a2b3c".to_owned(), None, 0));

        let resumed = match parse_grep_after("1a2b3c:7") {
            Ok(v) => v,
            Err(_) => panic!("RESUME:INDEX must parse"),
        };
        assert_eq!(
            resumed,
            ("@before-first".to_owned(), Some("1a2b3c".to_owned()), 7)
        );

        let full = match parse_grep_after("after:resume:3") {
            Ok(v) => v,
            Err(_) => panic!("AFTER:RESUME:INDEX must parse"),
        };
        assert_eq!(full, ("after".to_owned(), Some("resume".to_owned()), 3));
    }

    #[test]
    fn parse_grep_after_rejects_malformed_cursors() {
        match parse_grep_after("a:b:c:d") {
            Err(ExitErr::Fatal(e)) => {
                assert!(format!("{e:#}").contains("grep --after expects"), "{e}");
            }
            _ => panic!("four components must be rejected"),
        }
        match parse_grep_after("resume:notanumber") {
            Err(ExitErr::Fatal(e)) => {
                assert!(format!("{e:#}").contains("unsigned integer"), "{e}");
            }
            _ => panic!("a non-numeric index must be rejected"),
        }
    }

    #[test]
    fn grep_cursor_value_assembles_the_resume_cursor() {
        let args = base_args("needle");
        let value = match grep_cursor_value(&args, None, "cap:res:9") {
            Ok(v) => v,
            Err(e) => panic!("cursor assembly failed: {}", fatal(e)),
        };
        assert_eq!(value["after_capture_id"], "cap");
        assert_eq!(value["resume_capture_id"], "res");
        assert_eq!(value["record_index"], 9);
        assert_eq!(value["path_index"], 0);
        assert_eq!(value["match_index"], 0);
        assert!(!value["query_fingerprint"].as_str().unwrap().is_empty());
    }

    #[test]
    fn grep_request_maps_point_and_history_modes() {
        use sheaf_core::store::{GrepMode, GrepQuery};

        let point = match grep_request(&base_args("todo"), None) {
            Ok(r) => r,
            Err(e) => panic!("point request failed: {}", fatal(e)),
        };
        assert!(matches!(point.mode, GrepMode::Point));
        assert!(matches!(point.query, GrepQuery::Literal { .. }));
        assert!(point.cursor.is_none());
        assert!(point.anchor.is_none());
        assert_eq!(point.budget.max_results, 1000);
        assert_eq!(point.budget.max_materialized_bytes, 64 * 1024 * 1024);
        assert_eq!(point.budget.max_elapsed_ms, 5000);

        let mut history = base_args("todo");
        history.history = true;
        history.at = Some("@~5");
        history.from = Some("1a2b3c");
        history.to = Some("@");
        history.path = Some(Path::new("src"));
        history.follow = true;
        history.all = true;
        history.every_capture = true;
        history.extent = sheaf_core::store::SelectionExtent::Line;
        history.max_results = 42;
        let request = match grep_request(&history, None) {
            Ok(r) => r,
            Err(e) => panic!("history request failed: {}", fatal(e)),
        };
        assert!(matches!(request.mode, GrepMode::History));
        assert_eq!(request.at.as_deref(), Some("@~5"));
        assert_eq!(request.from.as_deref(), Some("1a2b3c"));
        assert_eq!(request.to.as_deref(), Some("@"));
        assert_eq!(request.path.as_deref(), Some("src"));
        assert!(request.follow && request.all && request.every_capture);
        assert!(matches!(
            request.extent,
            sheaf_core::store::SelectionExtent::Line
        ));
        assert_eq!(request.budget.max_results, 42);
    }

    #[test]
    fn grep_request_cursors_carry_the_query_fingerprint() {
        let mut args = base_args("needle");
        args.after = Some("cap:res:4");
        let request = match grep_request(&args, None) {
            Ok(r) => r,
            Err(e) => panic!("request failed: {}", fatal(e)),
        };
        let cursor = request.cursor.expect("after builds a cursor");
        assert_eq!(cursor.after_capture_id, "cap");
        assert_eq!(cursor.resume_capture_id, Some("res".to_owned()));
        assert_eq!(cursor.record_index, 4);
        assert_eq!(cursor.path_index, 0);
        assert_eq!(cursor.match_index, 0);

        // The cursor must be bound to the query it resumes: a different
        // query yields a different fingerprint.
        let mut other = base_args("different");
        other.after = Some("cap:res:4");
        let other_request = match grep_request(&other, None) {
            Ok(r) => r,
            Err(e) => panic!("request failed: {}", fatal(e)),
        };
        assert_eq!(
            cursor.query_fingerprint,
            match grep_request(&args, None) {
                Ok(r) => r.fingerprint(),
                Err(_) => panic!("fingerprint source"),
            }
        );
        assert_ne!(cursor.query_fingerprint, other_request.fingerprint());
    }

    #[test]
    fn grep_fingerprint_defaults_to_empty_when_the_request_is_invalid() {
        let mut args = base_args("needle");
        args.after = Some("x:y:z:w");
        assert_eq!(grep_fingerprint(&args, None), String::new());
        let args = base_args("needle");
        assert!(!grep_fingerprint(&args, None).is_empty());
    }

    #[test]
    fn grep_report_from_wire_reassembles_summary_and_body() {
        use sheaf_core::store::GrepStreamRecord;

        let hit_record = GrepStreamRecord::Hit {
            hit: Box::new(hit(sheaf_core::store::LifecycleKind::Introduced, true)),
        };
        let event_record = GrepStreamRecord::Event {
            event: event(sheaf_core::store::LifecycleKind::Removed),
        };
        let mut body = serde_json::to_vec(&hit_record).unwrap();
        body.push(b'\n');
        body.extend_from_slice(&serde_json::to_vec(&event_record).unwrap());
        body.push(b'\n');

        let summary = serde_json::json!({
            "query_fingerprint": "fp-9",
            "complete": true,
            "skipped_binary": 2,
            "pruned_intervals": 3,
            "hits": 1,
        });
        let report = match grep_report_from_wire(&summary, &body) {
            Ok(r) => r,
            Err(e) => panic!("wire reassembly failed: {}", fatal(e)),
        };
        assert_eq!(report.query_fingerprint, "fp-9");
        assert!(report.complete);
        assert_eq!(report.hits.len(), 1);
        assert_eq!(report.events.len(), 1);
        assert_eq!(report.hits[0].occurrence_id, "occ-0000000000001");
        assert_eq!(
            report.events[0].kind,
            sheaf_core::store::LifecycleKind::Removed
        );
        assert_eq!(report.skipped_binary, 2);
        assert_eq!(report.pruned_intervals, 3);
        assert!(!report.degraded);
    }

    #[test]
    fn grep_report_from_wire_defaults_usage_from_the_summary_count() {
        let summary = serde_json::json!({ "hits": 4 });
        let report = match grep_report_from_wire(&summary, b"") {
            Ok(r) => r,
            Err(e) => panic!("wire reassembly failed: {}", fatal(e)),
        };
        // No usage block: the streamed usage falls back to the summary's
        // hit count so scripts never see a zero that means "unknown".
        assert_eq!(report.usage.results, 4);
        assert!(report.complete, "absent complete defaults to true");
    }

    #[test]
    fn grep_report_from_wire_rejects_malformed_body_lines() {
        let summary = serde_json::json!({});
        match grep_report_from_wire(&summary, b"definitely not json") {
            Err(ExitErr::Fatal(e)) => {
                assert!(
                    format!("{e:#}").contains("invalid grep NDJSON record"),
                    "{e}"
                );
            }
            _ => panic!("malformed body must be rejected"),
        }
    }

    #[test]
    fn render_hit_row_names_the_lifecycle_coordinates_and_episode() {
        let row = render_hit_row(
            &hit(sheaf_core::store::LifecycleKind::Introduced, true),
            false,
        );
        assert!(row.contains("introduced"), "{row}");
        assert!(row.contains("src/a.rs:3:7"), "{row}");
        assert!(row.contains("0123456789ab"), "{row}");
        assert!(row.contains("episode ep1:abc123"), "{row}");
        assert!(!row.contains('\x1b'), "color off paints nothing");

        let branch = render_hit_row(&hit(sheaf_core::store::LifecycleKind::Changed, false), true);
        assert!(branch.contains("changed"), "{branch}");
        assert!(branch.contains("\x1b["), "color on paints SGR codes");
    }

    #[test]
    fn render_event_row_reports_removal_and_ambiguity() {
        // A real removal terminates an episode and carries no candidates.
        let mut removal = event(sheaf_core::store::LifecycleKind::Removed);
        removal.candidates = None;
        let removed = render_event_row(&removal, false);
        assert!(removed.contains("removed"), "{removed}");
        assert!(removed.contains("episode ep1:abc123"), "{removed}");
        assert!(
            !removed.contains("candidates"),
            "removals name no candidates"
        );

        let mut ambiguous = event(sheaf_core::store::LifecycleKind::Ambiguous);
        ambiguous.path = None;
        let row = render_event_row(&ambiguous, false);
        assert!(row.contains("ambiguous"), "{row}");
        assert!(row.contains("candidates 2"), "{row}");

        let gap = render_event_row(
            &event(sheaf_core::store::LifecycleKind::RetentionGap),
            false,
        );
        assert!(gap.contains("retention gap"), "{gap}");
    }

    #[test]
    fn hits_len_reads_the_summary_count() {
        assert_eq!(hits_len(&serde_json::json!({ "hits": 7 })), 7);
        assert_eq!(hits_len(&serde_json::json!({})), 0);
    }
}

#[cfg(test)]
mod output_and_argument_tests {
    use super::*;
    use sheaf_core::store::{
        ActionKind, CaptureOrigin, DiffKind, DiffOutcome, FileDiff, Obstacle, Obstruction,
        OriginKind, ResolvedPoint, RestoreAction, RestoreMode, RestoreOutcome, RestorePlan,
        SideContent, SideDesc,
    };

    #[test]
    fn grep_anchor_builds_episode_coordinate_and_selection_forms() {
        let mut args = grep_cli_layer_tests::base_args("needle");
        args.history = true;
        args.episode = Some("ep-1");
        assert!(matches!(
            grep_anchor(&args).unwrap_or_else(|e| panic!("{}", grep_cli_layer_tests::fatal(e))),
            Some(sheaf_core::store::GrepAnchor::Episode { episode_id }) if episode_id == "ep-1"
        ));

        args.episode = None;
        args.line = Some(4);
        args.path = Some(Path::new("src/lib.rs"));
        args.column = Some(9);
        assert!(matches!(
            grep_anchor(&args).unwrap_or_else(|e| panic!("{}", grep_cli_layer_tests::fatal(e))),
            Some(sheaf_core::store::GrepAnchor::Coordinate { path, line, column })
                if path == "src/lib.rs" && line == 4 && column == Some(9)
        ));

        let handle = grep_cli_layer_tests::handle();
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("selection.json");
        std::fs::write(&file, serde_json::to_vec(&handle).unwrap()).unwrap();
        args.line = None;
        args.path = None;
        args.column = None;
        args.selection = file.to_str();
        assert!(matches!(
            grep_anchor(&args).unwrap_or_else(|e| panic!("{}", grep_cli_layer_tests::fatal(e))),
            Some(sheaf_core::store::GrepAnchor::Selection { handle: selected })
                if *selected == handle
        ));
    }

    #[test]
    fn grep_anchor_rejects_mixed_forms_and_missing_line_path() {
        let mut args = grep_cli_layer_tests::base_args("needle");
        args.line = Some(1);
        args.episode = Some("ep");
        let error = grep_anchor(&args).unwrap_err();
        assert!(grep_cli_layer_tests::fatal(error).contains("mutually exclusive"));

        args.episode = None;
        args.path = None;
        let error = grep_anchor(&args).unwrap_err();
        assert!(grep_cli_layer_tests::fatal(error).contains("--line requires --path"));
    }

    #[test]
    fn selection_payload_accepts_arrays_and_wrapped_hits() {
        let handle = grep_cli_layer_tests::handle();
        let raw = serde_json::json!([handle, {"handle": handle}]).to_string();
        let parsed = parse_selection_payload(&raw).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0], parsed[1]);
        assert!(parse_selection_payload("not json").is_err());
        assert!(parse_selection_payload("{}").is_err());
    }

    #[test]
    fn output_helpers_cover_sizes_origins_obstacles_and_side_labels() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1024), "1.0 KiB");
        assert_eq!(human_bytes(1024 * 1024), "1.0 MiB");
        assert_eq!(human_bytes(1024 * 1024 * 1024), "1.0 GiB");
        assert_eq!(origin_suffix(None), "");
        assert_eq!(
            origin_suffix(Some(&CaptureOrigin {
                kind: OriginKind::Restore,
                target: Some("1234567890abcdef".into()),
                scope: vec![],
                selections: vec![],
            })),
            "   [restore ← 1234567890ab]"
        );
        assert_eq!(
            origin_suffix(Some(&CaptureOrigin {
                kind: OriginKind::Restore,
                target: None,
                scope: vec![],
                selections: vec![],
            })),
            "   [restore]"
        );
        assert_eq!(
            origin_suffix(Some(&CaptureOrigin {
                kind: OriginKind::PreRestore,
                target: None,
                scope: vec![],
                selections: vec![],
            })),
            "   [pre-restore snapshot]"
        );
        assert_eq!(
            origin_suffix(Some(&CaptureOrigin {
                kind: OriginKind::FragmentRestore,
                target: None,
                scope: vec![],
                selections: vec![],
            })),
            "   [fragment restore]"
        );
        for (obstacle, text) in [
            (
                Obstacle::DirectoryInTheWay,
                "a directory occupies this path",
            ),
            (Obstacle::SymlinkInTheWay, "a symlink occupies this path"),
            (
                Obstacle::MissingBlob,
                "the stored binary payload is missing",
            ),
            (
                Obstacle::EscapesRoot,
                "the stored path escapes the project root",
            ),
            (Obstacle::Unreadable, "the live path cannot be read"),
        ] {
            assert_eq!(
                obstacle_text(&Obstruction {
                    path: "x".into(),
                    obstacle
                }),
                text
            );
        }
        assert_eq!(
            side_label(&SideDesc {
                kind: "worktree".into(),
                capture_id: None,
                frontier: None
            }),
            "worktree"
        );
        assert_eq!(
            side_label(&SideDesc {
                kind: "point".into(),
                capture_id: Some("abcdef123456789".into()),
                frontier: None,
            }),
            "capture:abcdef123456"
        );
        assert_eq!(
            side_label(&SideDesc {
                kind: "point".into(),
                capture_id: None,
                frontier: Some("frontier".into())
            }),
            "point"
        );
    }

    #[test]
    fn diff_and_restore_renderers_accept_all_action_kinds() {
        let diff = DiffOutcome {
            from: SideDesc {
                kind: "point".into(),
                capture_id: None,
                frontier: Some("a".into()),
            },
            to: SideDesc {
                kind: "worktree".into(),
                capture_id: None,
                frontier: None,
            },
            entries: vec![
                FileDiff {
                    path: "new".into(),
                    old_path: None,
                    kind: DiffKind::Added,
                    old: SideContent::Absent,
                    new: SideContent::Text { bytes: 2 },
                    added_lines: 2,
                    removed_lines: 0,
                    hunks: vec![],
                },
                FileDiff {
                    path: "old".into(),
                    old_path: None,
                    kind: DiffKind::Deleted,
                    old: SideContent::Text { bytes: 2 },
                    new: SideContent::Absent,
                    added_lines: 0,
                    removed_lines: 2,
                    hunks: vec![],
                },
                FileDiff {
                    path: "renamed".into(),
                    old_path: Some("before".into()),
                    kind: DiffKind::Renamed,
                    old: SideContent::Text { bytes: 1 },
                    new: SideContent::Text { bytes: 1 },
                    added_lines: 0,
                    removed_lines: 0,
                    hunks: vec![],
                },
                FileDiff {
                    path: "binary".into(),
                    old_path: None,
                    kind: DiffKind::TypeChanged,
                    old: SideContent::Binary {
                        hash: "h".into(),
                        bytes: 1,
                    },
                    new: SideContent::Text { bytes: 1 },
                    added_lines: 0,
                    removed_lines: 0,
                    hunks: vec![],
                },
            ],
            degraded: false,
        };
        assert!(!diff.is_empty());
        print_diff_stat(&diff);
        let point = ResolvedPoint {
            capture_id: Some("abcdef123456789".into()),
            frontier: "f".into(),
        };
        let plan = RestorePlan {
            token: "t".into(),
            mode: RestoreMode::Scoped,
            scope: vec!["src".into()],
            base: point.clone(),
            target: point.clone(),
            actions: vec![
                RestoreAction {
                    path: "a".into(),
                    kind: ActionKind::Create,
                    content: None,
                    bytes: 2048,
                    hash: None,
                    exec: false,
                    local_modified: true,
                },
                RestoreAction {
                    path: "b".into(),
                    kind: ActionKind::Update,
                    content: None,
                    bytes: 0,
                    hash: None,
                    exec: false,
                    local_modified: false,
                },
                RestoreAction {
                    path: "c".into(),
                    kind: ActionKind::Delete,
                    content: None,
                    bytes: 0,
                    hash: None,
                    exec: false,
                    local_modified: false,
                },
            ],
            obstructions: vec![],
            unchanged: 1,
            locally_modified: 1,
            scope_missing: vec![],
            created_at_ms: 0,
            degraded: true,
        };
        print_plan(&plan);
        let outcome = RestoreOutcome {
            token: "t".into(),
            mode: RestoreMode::Full,
            target: point.clone(),
            undo: point.clone(),
            result: point,
            pre_restore_capture: Some("pre123456789012".into()),
            restore_capture: Some("rest123456789012".into()),
            files_written: 1,
            files_deleted: 1,
            unchanged: 1,
            written_paths: vec![],
            deleted_paths: vec![],
            resumed: false,
            progress_log: vec![],
        };
        print_outcome(&outcome);
        use sheaf_core::store::LifecycleKind;
        for kind in [
            LifecycleKind::Present,
            LifecycleKind::Reintroduced,
            LifecycleKind::Relocated,
            LifecycleKind::Renamed,
            LifecycleKind::Moved,
            LifecycleKind::Observed,
        ] {
            assert!(!render_hit_row(&grep_cli_layer_tests::hit(kind, true), false).is_empty());
        }
        assert!(!render_hit_row(
            &grep_cli_layer_tests::hit(LifecycleKind::Removed, true),
            false
        )
        .is_empty());
        assert!(
            render_event_row(&grep_cli_layer_tests::event(LifecycleKind::Present), false)
                .contains("event")
        );
        assert!(origin_suffix(Some(&CaptureOrigin {
            kind: OriginKind::FragmentRestore,
            target: None,
            scope: vec![],
            selections: vec!["selection-123456789".into()],
        }))
        .contains("fragment ← selection-1"));
    }

    #[test]
    fn merge_plan_renderer_prints_points_actions_and_conflicts() {
        use sheaf_core::store::{MergeAction, MergeConflict, MergePlan};
        // `base` carries a capture id (short = its 12 hex prefix); `source`
        // carries none, so `short` must fall back to the frontier prefix.
        let base = ResolvedPoint {
            capture_id: Some("abcdef1234567890".into()),
            frontier: "basefrontier".into(),
        };
        let source = ResolvedPoint {
            capture_id: None,
            frontier: "0123456789abcdef".into(),
        };
        let target = ResolvedPoint {
            capture_id: Some("fedcba0987654321".into()),
            frontier: "targetfrontier".into(),
        };
        let action = |path: &str, kind: ActionKind| MergeAction {
            path: path.into(),
            kind,
            content: None,
            bytes: 0,
            hash: None,
            exec: false,
        };
        let plan = MergePlan {
            token: "token".into(),
            base,
            source,
            target,
            actions: vec![
                action("src/new.rs", ActionKind::Create),
                action("src/mod.rs", ActionKind::Update),
                action("src/old.rs", ActionKind::Delete),
            ],
            conflicts: vec![MergeConflict {
                path: "src/conf.rs".into(),
                reason: "both branches changed this path differently".into(),
            }],
            unchanged: 2,
            created_at_ms: 0,
        };
        // Renders base/source/target/changes/conflicts plus a line per action
        // and per conflict across every ActionKind and both point-resolution
        // branches without panicking.
        print_merge_plan(&plan);
    }
}

#[cfg(test)]
mod private_branch_tests {
    use super::*;
    use std::os::unix::process::ExitStatusExt;
    use std::process::{ExitStatus, Output};

    fn output(success: bool, stdout: &[u8], stderr: &[u8]) -> Output {
        Output {
            status: if success {
                ExitStatus::from_raw(0)
            } else {
                ExitStatus::from_raw(256)
            },
            stdout: stdout.to_vec(),
            stderr: stderr.to_vec(),
        }
    }

    #[test]
    fn command_json_flags_cover_each_json_capable_variant() {
        let commands = [
            Cmd::Log {
                project: None,
                path: None,
                follow: false,
                all: false,
                before: None,
                limit: 1,
                json: true,
            },
            Cmd::Info {
                project: None,
                reference: "@".into(),
                json: true,
            },
            Cmd::Diff {
                project: None,
                from: None,
                to: None,
                paths: vec![],
                stat: false,
                json: true,
                exit_code: false,
            },
            Cmd::Grep {
                query: "x".into(),
                history: false,
                at: None,
                path: None,
                line: None,
                column: None,
                episode: None,
                selection: None,
                follow: false,
                all: false,
                every_capture: false,
                extent: GrepExtentArg::Match,
                from: None,
                to: None,
                after: None,
                max_results: 1,
                project: None,
                json: true,
            },
            Cmd::Doctor {
                project: None,
                json: true,
                fix: false,
            },
            Cmd::Gc {
                project: None,
                apply: false,
                json: true,
                set_expiry: None,
                mark: None,
            },
            Cmd::Restore {
                project: None,
                args: vec![],
                at: None,
                dry_run: false,
                resume: false,
                abandon: false,
                selection: None,
                insert: false,
                delete: false,
                json: true,
            },
            Cmd::Squash {
                project: None,
                range: None,
                selection: None,
                git_args: vec![],
                json: true,
            },
        ];
        assert!(commands.iter().all(Cmd::outputs_json));
        assert!(!Cmd::Checkpoint {
            command: None,
            name: None,
            at: None,
            project: None
        }
        .outputs_json());
    }

    #[test]
    fn color_auto_honors_empty_and_nonempty_no_color() {
        std::env::set_var("NO_COLOR", "1");
        assert!(!ColorWhen::Auto.enabled(true));
        std::env::set_var("NO_COLOR", "");
        assert!(ColorWhen::Auto.enabled(true));
        std::env::remove_var("NO_COLOR");
    }

    #[test]
    fn output_and_service_helpers_cover_success_errors_and_fallbacks() {
        assert_eq!(
            text_out(output(true, b" value \n", b""), "x").unwrap(),
            "value"
        );
        let error = text_out(output(false, b"", b"bad\n"), "x").unwrap_err();
        assert!(format!("{error:#}").contains("x failed: bad"));
        assert_eq!(
            ipc_error_text(&sheaf_core::ipc::Response::ok("1", serde_json::json!({}))),
            "unknown daemon error"
        );
        assert_eq!(
            ipc_error_text(&sheaf_core::ipc::Response::err(
                "1",
                sheaf_core::ipc::IpcError::new("E", "nope")
            )),
            "E: nope"
        );
        let unit = unit_file_text(Path::new("/opt/sheaf/sheafd"));
        assert!(unit.contains("ExecStart=/opt/sheaf/sheafd run"));
        assert!(unit.contains("Restart=on-failure"));
        assert_eq!(yn(true), "yes");
        assert_eq!(yn(false), "no");
    }

    #[test]
    fn selection_anchor_accepts_bare_wrapped_and_ndjson_handles() {
        let handle = crate::grep_cli_layer_tests::handle();
        let mut args = crate::grep_cli_layer_tests::base_args("needle");
        let raw = serde_json::to_string(&handle).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("selection.json");
        std::fs::write(&path, &raw).unwrap();
        args.selection = path.to_str();
        match grep_anchor(&args) {
            Ok(Some(sheaf_core::store::GrepAnchor::Selection { .. })) => {}
            Ok(_) => panic!("selection anchor missing"),
            Err(_) => panic!("selection anchor failed"),
        }
        let wrapped = serde_json::json!({"handle": handle});
        std::fs::write(&path, wrapped.to_string()).unwrap();
        assert!(grep_anchor(&args).is_ok_and(|v| v.is_some()));
        let record = serde_json::json!({"hit": {"handle": crate::grep_cli_layer_tests::handle()}});
        std::fs::write(&path, record.to_string()).unwrap();
        assert!(grep_anchor(&args).is_ok_and(|v| v.is_some()));
    }

    #[test]
    fn fragment_plan_prints_conflict_candidates() {
        use sheaf_core::store::{
            ByteRange, FragmentCondition, FragmentConflict, FragmentPlan, FragmentRange,
            ResolvedPoint,
        };
        let plan = FragmentPlan {
            token: "token".into(),
            mode: sheaf_core::store::FragmentMode::Replace,
            selections: vec![crate::grep_cli_layer_tests::handle()],
            files: vec![],
            conflicts: vec![FragmentConflict {
                selection_id: "selection-123456789".into(),
                path: Some("src/a.rs".into()),
                condition: FragmentCondition::Ambiguous,
                candidates: vec![FragmentRange {
                    path: "src/a.rs".into(),
                    range: ByteRange { start: 2, end: 8 },
                }],
                detail: "two matching units".into(),
            }],
            unchanged: 0,
            base: ResolvedPoint {
                capture_id: None,
                frontier: "frontier".into(),
            },
            created_at_ms: 0,
            degraded: false,
        };
        print_fragment_plan(&plan);
    }

    #[test]
    fn parsers_cover_empty_and_invalid_wire_defaults() {
        let args = crate::grep_cli_layer_tests::base_args("x");
        assert!(grep_request(&args, None).is_ok());
        assert!(grep_report_from_wire(&serde_json::json!({"usage": {"results": 3}}), b"").is_ok());
        assert!(parse_selection_payload("[]").unwrap().is_empty());
        assert!(parse_selection_payload("{\"handle\": null}").is_err());
    }

    #[test]
    fn streamed_printer_finishes_empty_truncated_and_annotated_reports() {
        use sheaf_core::store::{GrepStreamRecord, SearchStopReason};
        let hit =
            crate::grep_cli_layer_tests::hit(sheaf_core::store::LifecycleKind::Introduced, true);
        let mut human = GrepStreamPrinter::new(false, ColorWhen::Never);
        human.record(&GrepStreamRecord::Hit {
            hit: Box::new(hit.clone()),
        });
        human.record(&GrepStreamRecord::Event {
            event: crate::grep_cli_layer_tests::event(sheaf_core::store::LifecycleKind::Removed),
        });
        let complete: sheaf_core::store::GrepReport = serde_json::from_value(serde_json::json!({
            "query_fingerprint":"x", "complete":true, "hits":[], "events":[],
            "skipped_binary":1, "pruned_intervals":2, "usage":{"results":0,"materialized_bytes":0,"elapsed_ms":0}, "degraded":false
        }))
        .unwrap();
        GrepStreamPrinter::new(false, ColorWhen::Never).finish(&complete);
        let cursor = serde_json::json!({
            "after_capture_id":"after", "resume_capture_id":"resume",
            "record_index":4, "path_index":0, "match_index":0, "query_fingerprint":"x"
        });
        let truncated: sheaf_core::store::GrepReport = serde_json::from_value(serde_json::json!({
            "query_fingerprint":"x", "complete":false, "stop_reason": SearchStopReason::ResultLimit,
            "cursor":cursor, "hits":[], "events":[], "skipped_binary":0,
            "pruned_intervals":1, "usage":{"results":0,"materialized_bytes":0,"elapsed_ms":0}, "degraded":false
        }))
        .unwrap();
        GrepStreamPrinter::new(false, ColorWhen::Never).finish(&truncated);
        let before: sheaf_core::store::GrepReport = serde_json::from_value(serde_json::json!({
            "query_fingerprint":"x", "complete":false, "cursor":{
              "after_capture_id":"@before-first", "resume_capture_id":"resume",
              "record_index":2, "path_index":0, "match_index":0, "query_fingerprint":"x"
            }, "hits":[], "events":[], "skipped_binary":0, "pruned_intervals":0,
            "usage":{"results":0,"materialized_bytes":0,"elapsed_ms":0}, "degraded":false
        }))
        .unwrap();
        GrepStreamPrinter::new(false, ColorWhen::Never).finish(&before);
        GrepStreamPrinter::new(true, ColorWhen::Never).finish(&complete);
    }

    #[test]
    fn fragment_plan_prints_each_mode_and_action() {
        use sheaf_core::store::{
            ByteRange, FragmentAction, FragmentActionKind, FragmentFilePlan, FragmentMode,
            FragmentPlan, ResolvedPoint,
        };
        let handle = crate::grep_cli_layer_tests::handle();
        for (mode, kind) in [
            (FragmentMode::Replace, FragmentActionKind::Replace),
            (FragmentMode::Insert, FragmentActionKind::Insert),
            (FragmentMode::Delete, FragmentActionKind::Delete),
        ] {
            let action = FragmentAction {
                selection_id: handle.id(),
                handle: handle.clone(),
                kind,
                range: ByteRange { start: 2, end: 5 },
                old_fragment_sha256: "old".into(),
                new_fragment_sha256: "new".into(),
                old_bytes: 3,
                new_bytes: 4,
                line_glue: false,
            };
            let plan = FragmentPlan {
                token: "token".into(),
                mode,
                selections: vec![handle.clone(), handle.clone()],
                files: vec![FragmentFilePlan {
                    path: "src/a.rs".into(),
                    file_sha256: "file".into(),
                    result_sha256: "result".into(),
                    actions: vec![action],
                }],
                conflicts: vec![],
                unchanged: 0,
                base: ResolvedPoint {
                    capture_id: None,
                    frontier: "f".into(),
                },
                created_at_ms: 0,
                degraded: false,
            };
            print_fragment_plan(&plan);
        }
    }

    #[test]
    fn smart_plan_prints_empty_json_and_conflict_human_forms() {
        use sheaf_core::store::{
            ByteRange, SmartCandidate, SmartCondition, SmartConflict, SmartPlan, SmartSide,
        };
        let empty = SmartPlan {
            selections: vec![],
            files: vec![],
            conflicts: vec![],
            unchanged: 2,
            patch_sha256: "patch".into(),
        };
        print_smart_plan(&empty, &Default::default(), true, false);
        let conflict = SmartPlan {
            selections: vec![],
            files: vec![],
            conflicts: vec![SmartConflict {
                selection_id: "selection-123456789".into(),
                path: Some("src/a.rs".into()),
                side: Some(SmartSide::Head),
                condition: SmartCondition::Ambiguous,
                candidates: vec![SmartCandidate {
                    path: "src/a.rs".into(),
                    range: ByteRange { start: 1, end: 4 },
                }],
                detail: "multiple candidates".into(),
            }],
            unchanged: 0,
            patch_sha256: "patch".into(),
        };
        print_smart_plan(&conflict, &Default::default(), true, false);
    }

    #[test]
    fn squash_helpers_cover_explicit_anchor_and_partial_stats() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join(".sheaf/store")).unwrap();
        sheaf_core::config::write_skeleton(dir.path()).unwrap();
        std::fs::write(dir.path().join(".sheaf/lock"), b"").unwrap();
        let mut ctx = SquashCtx::new(dir.path(), None).unwrap();
        let anchor = squash_anchor(&mut ctx, Some("capture:abc"), false).unwrap();
        assert_eq!(anchor.label, "capture:abc");
        assert_eq!(anchor.user_ref.as_deref(), Some("capture:abc"));
        let stats = squash_stats(&mut ctx, None, None);
        assert!(stats.partial);
    }

    #[test]
    fn backfill_and_gc_renderers_cover_json_and_sparse_reports() {
        use sheaf_core::store::{GcOutcome, GrepBackfillReport};
        print_backfill_report(&GrepBackfillReport::default(), true);
        print_backfill_report(
            &GrepBackfillReport {
                rebuilt: true,
                complete: false,
                captures_failed: 2,
                ..Default::default()
            },
            false,
        );
        let planned = GcOutcome::Planned(sheaf_core::store::GcPlan {
            root: "root".into(),
            segments: vec![],
            snapshots: vec![],
            orphan_blobs: vec![],
            bytes_recovered: 0,
            retention: Default::default(),
        });
        print_gc_outcome(&planned, true);
    }

    #[test]
    fn systemctl_missing_binary_reports_execution_error() {
        let old = std::env::var_os("PATH");
        std::env::set_var("PATH", tempfile::tempdir().unwrap().path());
        let result = run_systemctl(&["daemon-reload"]);
        if let Some(path) = old {
            std::env::set_var("PATH", path);
        } else {
            std::env::remove_var("PATH");
        }
        let error = result.unwrap_err();
        let message = match error {
            ExitErr::Fatal(error) => format!("{error:#}"),
            ExitErr::SilentCode(code) => format!("silent exit {code}"),
        };
        assert!(message.contains("systemctl"));
    }

    #[test]
    fn command_handlers_reject_missing_projects_before_store_access() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path();
        assert!(cmd_doctor(Some(path), false, false).is_err());
        assert!(cmd_gc(Some(path), false, false, None, None).is_err());
        assert!(cmd_cache_backfill(Some(path), false, false, None, false).is_err());
        assert!(cmd_restore(Some(path), &[], None, true, false).is_err());
        assert!(cmd_grep(crate::GrepArgs {
            project: Some(path),
            query: "needle",
            history: false,
            at: None,
            path: None,
            line: None,
            column: None,
            episode: None,
            selection: None,
            follow: false,
            all: false,
            every_capture: false,
            extent: sheaf_core::store::SelectionExtent::Match,
            from: None,
            to: None,
            after: None,
            max_results: 1,
            as_json: false,
            color: ColorWhen::Never,
        })
        .is_err());
    }
}
