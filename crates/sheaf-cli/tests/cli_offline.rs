//! Offline CLI contract: dispatch, rendering, and error paths for every
//! command that has a no-daemon story, plus the refusals for commands that
//! correctly require the daemon. Fixtures are deterministic stores built
//! through sheaf-core (no daemon, no git, no network); the real binary is
//! spawned with a dead socket, so every read runs through the same degraded
//! fallback an offline `sheaf` uses.

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

use chrono::{Duration, Utc};
use sheaf_core::config;
use sheaf_core::events::{Batch, EventKind, FsEvent};
use sheaf_core::store::{ProjectStore, StoreLimits, TimelineReader};

// ---------------------------------------------------------------- helpers

fn skeleton(root: &Path) {
    std::fs::create_dir_all(root.join(".sheaf/store")).unwrap();
    config::write_skeleton(root).unwrap();
    // Degraded reads flock what is already there; leave an uncontended
    // lock file behind, as a stopped daemon would.
    std::fs::write(root.join(".sheaf/lock"), b"").unwrap();
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

fn apply(store: &mut ProjectStore, root: &Path, event: EventKind, age_h: i64) {
    let at = Utc::now() - Duration::hours(age_h);
    store
        .apply_batch(&Batch {
            root: root.to_path_buf(),
            started_at: at,
            flushed_at: at,
            events: vec![FsEvent::now(event)],
        })
        .unwrap();
}

fn touched(root: &Path, rel: &str, text: &str) -> EventKind {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, text).unwrap();
    // The event carries the absolute path, as the watcher observes it; the
    // batch carries the project root the path is resolved against.
    EventKind::Touched {
        path: root.join(rel).into(),
    }
}

/// Capture one event, returning the new capture's full ID.
fn capture(root: &Path, event: EventKind, age_h: i64) -> String {
    let mut store = open(root);
    apply(&mut store, root, event, age_h);
    drop(store);
    newest_capture_id(root)
}

fn newest_capture_id(root: &Path) -> String {
    let reader = TimelineReader::open(root).unwrap();
    reader.captures(false, None, false, 1).unwrap().remove(0).id
}

/// Per-invocation isolation: a dead socket forces the degraded reader, and
/// private XDG dirs keep the enrollment registry away from the real one.
struct Iso(tempfile::TempDir);

impl Iso {
    fn new(tag: &str) -> Iso {
        Iso(tempfile::tempdir().unwrap_or_else(|e| panic!("tempdir for {tag}: {e}")))
    }

    fn root(&self) -> PathBuf {
        self.0.path().join("proj")
    }

    fn enrolled_project(&self) -> PathBuf {
        let root = self.root();
        skeleton(&root);
        root
    }

    fn run(&self, root: &Path, args: &[&str]) -> Output {
        self.spawn(root, args, None)
    }

    fn run_stdin(&self, root: &Path, args: &[&str], payload: &str) -> Output {
        self.spawn(root, args, Some(payload))
    }

    fn spawn(&self, root: &Path, args: &[&str], stdin: Option<&str>) -> Output {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_sheaf"));
        cmd.args(args).current_dir(root);
        cmd.env("SHEAF_SOCKET", self.0.path().join("none").join("off.sock"));
        cmd.env("XDG_DATA_HOME", self.0.path().join("xdg-data"));
        cmd.env("XDG_CONFIG_HOME", self.0.path().join("xdg-config"));
        cmd.env("XDG_RUNTIME_DIR", self.0.path().join("xdg-run"));
        if stdin.is_some() {
            cmd.stdin(Stdio::piped());
        }
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = cmd.spawn().expect("spawn sheaf");
        if let Some(payload) = stdin {
            use std::io::Write as _;
            child
                .stdin
                .take()
                .unwrap()
                .write_all(payload.as_bytes())
                .unwrap();
        }
        child.wait_with_output().expect("wait sheaf")
    }
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn ok(out: &Output) {
    assert!(
        out.status.success(),
        "expected success: {}\n--- stdout ---\n{}\n--- stderr ---\n{}",
        out.status,
        stdout(out),
        stderr(out)
    );
}

fn code(out: &Output) -> i32 {
    out.status.code().expect("unix exit code")
}

/// Assert an exit code and that the needle appears in either stream.
fn fail(out: &Output, want: i32, needle: &str) {
    let combined = format!("{}{}", stdout(out), stderr(out));
    assert_eq!(
        code(out),
        want,
        "exit {}\n--- stdout ---\n{}\n--- stderr ---\n{}",
        code(out),
        stdout(out),
        stderr(out)
    );
    assert!(
        combined.contains(needle),
        "missing {needle:?}\n--- stdout ---\n{}\n--- stderr ---\n{}",
        stdout(out),
        stderr(out)
    );
}

/// Two captured versions of a.txt plus one capture introducing b.txt.
/// Returns (oldest, middle, newest) capture IDs.
fn three_capture_store(root: &Path) -> (String, String, String) {
    skeleton(root);
    let oldest = capture(root, touched(root, "a.txt", "v1\n"), 3);
    let middle = capture(root, touched(root, "b.txt", "bee\n"), 2);
    let newest = capture(root, touched(root, "a.txt", "v2\n"), 1);
    (oldest, middle, newest)
}

// ------------------------------------------------------------------- init

#[test]
fn init_reports_creation_reuse_and_offline_notes() {
    let iso = Iso::new("init");
    let root = iso.root();
    std::fs::create_dir_all(&root).unwrap();

    let out = iso.run(&root, &["init"]);
    ok(&out);
    let text = stdout(&out);
    assert!(text.contains("root:"), "{text}");
    assert!(text.contains("store:         created"), "{text}");
    assert!(text.contains("enrollment:    registered"), "{text}");
    // The daemon is unreachable: the report says so instead of lying.
    assert!(text.contains("daemon not reachable at"), "{text}");
    assert!(!text.contains("daemon:        notified"), "{text}");

    let out = iso.run(&root, &["init"]);
    ok(&out);
    let text = stdout(&out);
    assert!(
        text.contains("store:         already initialized"),
        "{text}"
    );
    assert!(text.contains("enrollment:    already registered"), "{text}");

    // A nested directory reuses the ancestor's store.
    let sub = root.join("crates/inner");
    std::fs::create_dir_all(&sub).unwrap();
    let out = iso.run(&sub, &["init"]);
    ok(&out);
    assert!(
        stdout(&out).contains("store:         reused ancestor store"),
        "{}",
        stdout(&out)
    );
}

// ----------------------------------------------------------------- status

#[test]
fn status_without_a_project_still_reports_the_daemon() {
    let iso = Iso::new("status-none");
    let bare = iso.0.path().join("bare");
    std::fs::create_dir_all(&bare).unwrap();

    let out = iso.run(&bare, &["status"]);
    ok(&out);
    let text = stdout(&out);
    assert!(
        text.contains("(none — no .sheaf/config.toml above"),
        "{text}"
    );
    assert!(text.contains("daemon:        not running"), "{text}");
    assert!(
        text.contains("read-only fallback active"),
        "expected degraded note: {text}"
    );
    assert!(text.contains("hint:          run `sheaf init`"), "{text}");
}

#[test]
fn status_in_a_project_reports_store_config_and_watch_state() {
    let iso = Iso::new("status");
    let root = iso.enrolled_project();

    let out = iso.run(&root, &["status"]);
    ok(&out);
    let text = stdout(&out);
    assert!(
        text.contains(&format!("project:       {}", root.display())),
        "{text}"
    );
    assert!(text.contains("store:         format 2"), "{text}");
    assert!(text.contains("watch config:  debounce="), "{text}");
    assert!(text.contains("retention:     no expiry"), "{text}");
    // The isolated registry never saw this project.
    assert!(text.contains("enrolled:      no"), "{text}");
    assert!(text.contains("daemon:        not running"), "{text}");

    // Expiry configured later shows up here too.
    let out = iso.run(&root, &["gc", "--set-expiry", "45m"]);
    ok(&out);
    let out = iso.run(&root, &["status"]);
    ok(&out);
    assert!(
        stdout(&out).contains("retention:     edits expire after 45m (reachability-bound"),
        "{}",
        stdout(&out)
    );
}

// ------------------------------------------------------- no-project errors

#[test]
fn timeline_commands_refuse_to_run_outside_a_project() {
    let iso = Iso::new("no-root");
    let bare = iso.0.path().join("bare");
    std::fs::create_dir_all(&bare).unwrap();

    for args in [
        vec!["log"],
        vec!["diff"],
        vec!["info", "@"],
        vec!["checkpoint", "list"],
        vec!["restore", "--dry-run", "@"],
    ] {
        let out = iso.run(&bare, &args);
        fail(&out, 3, "no project root found above");
    }

    // doctor, gc, and cache report the missing root in their own words,
    // as failures.
    let out = iso.run(&bare, &["doctor"]);
    fail(&out, 1, "(missing .sheaf/config.toml)");
    let out = iso.run(&bare, &["cache", "backfill"]);
    fail(&out, 1, "(missing .sheaf/config.toml)");
    let out = iso.run(&bare, &["gc"]);
    fail(&out, 1, "(missing .sheaf/config.toml)");
}

// -------------------------------------------------------------------- log

#[test]
fn log_orders_oldest_to_newest_and_json_keeps_wire_order() {
    let iso = Iso::new("log");
    let root = iso.enrolled_project();
    let (oldest, middle, newest) = three_capture_store(&root);
    fn short(id: &str) -> &str {
        &id[..12]
    }

    let out = iso.run(&root, &["log"]);
    ok(&out);
    let text = stdout(&out);
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 3, "{text}");
    assert!(
        lines[0].contains(short(&oldest)) && lines[0].contains("a.txt"),
        "oldest first: {text}"
    );
    assert!(lines[1].contains(short(&middle)) && lines[1].contains("b.txt"));
    assert!(lines[2].contains(short(&newest)) && lines[2].contains("a.txt"));
    assert!(
        stderr(&out).contains("note: daemon unavailable; showing a read-only store snapshot"),
        "{}",
        stderr(&out)
    );

    let out = iso.run(&root, &["log", "--json"]);
    ok(&out);
    let value: serde_json::Value = serde_json::from_str(&stdout(&out)).unwrap();
    assert_eq!(value["degraded"], serde_json::json!(true));
    assert_eq!(value["tips"], serde_json::json!(1));
    let entries = value["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 3);
    // Wire order is newest first, unlike the human view.
    assert_eq!(entries[0]["id"], serde_json::json!(newest));
    assert_eq!(entries[2]["id"], serde_json::json!(oldest));

    // Path filter and limit narrow the view.
    let out = iso.run(&root, &["log", "--path", "b.txt"]);
    ok(&out);
    let text = stdout(&out);
    assert!(text.contains("b.txt") && !text.contains("a.txt"), "{text}");

    let out = iso.run(&root, &["log", "--limit", "1"]);
    ok(&out);
    let text = stdout(&out);
    assert!(
        text.contains(short(&newest)) && !text.contains(short(&oldest)),
        "{text}"
    );
}

#[test]
fn log_before_cursor_validates_and_paginates() {
    let iso = Iso::new("log-cursor");
    let root = iso.enrolled_project();
    let (oldest, middle, newest) = three_capture_store(&root);

    // Too short or non-hex cursors are refused before any resolution.
    let out = iso.run(&root, &["log", "--before", "xyz"]);
    fail(&out, 1, "at least 6 hexadecimal capture-ID characters");
    let out = iso.run(&root, &["log", "--before", "zzzzzz"]);
    fail(&out, 1, "at least 6 hexadecimal capture-ID characters");

    // Well-formed but unknown cursors fail with the resolver's own error.
    let out = iso.run(&root, &["log", "--before", "000000f"]);
    fail(&out, 1, "unknown capture `000000f`");

    // A real cursor yields strictly older captures, still oldest-first.
    let out = iso.run(&root, &["log", "--before", &newest]);
    ok(&out);
    let text = stdout(&out);
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 2, "{text}");
    assert!(
        lines[0].contains(&oldest[..12]) && lines[1].contains(&middle[..12]),
        "{text}"
    );
}

// ------------------------------------------------------------------- info

#[test]
fn info_renders_entries_json_and_unknown_references() {
    let iso = Iso::new("info");
    let root = iso.enrolled_project();
    let (_, middle, _) = three_capture_store(&root);

    let out = iso.run(&root, &["info", &middle]);
    ok(&out);
    let text = stdout(&out);
    assert!(text.contains(&format!("* {}", &middle[..12])), "{text}");
    assert!(text.contains("  + b.txt"), "{text}");
    assert!(
        stderr(&out).contains("note: daemon unavailable; showing a read-only store snapshot"),
        "{}",
        stderr(&out)
    );

    let out = iso.run(&root, &["info", "--json", &middle]);
    ok(&out);
    let value: serde_json::Value = serde_json::from_str(&stdout(&out)).unwrap();
    assert_eq!(value["degraded"], serde_json::json!(true));
    assert_eq!(value["info"]["capture"]["id"], serde_json::json!(middle));

    let out = iso.run(&root, &["info", "zzzzzz"]);
    fail(&out, 1, "sheaf:");
}

#[test]
fn info_marks_renames_with_old_and_new_paths() {
    let iso = Iso::new("info-rename");
    let root = iso.enrolled_project();
    skeleton(&root);
    let first = capture(&root, touched(&root, "old.txt", "moved on\n"), 2);
    std::fs::write(root.join("new.txt"), "moved on\n").unwrap();
    let rename = capture(
        &root,
        EventKind::Renamed {
            from: root.join("old.txt"),
            to: root.join("new.txt"),
        },
        1,
    );
    assert_ne!(first, rename);

    let out = iso.run(&root, &["info", &rename]);
    ok(&out);
    let text = stdout(&out);
    assert!(
        text.contains("  ~ old.txt => new.txt"),
        "rename rendering missing: {text}"
    );
}

// ------------------------------------------------------------------- diff

#[test]
fn diff_renders_patches_stats_ranges_and_exit_codes() {
    let iso = Iso::new("diff");
    let root = iso.enrolled_project();
    let (oldest, _, newest) = three_capture_store(&root);

    // Worktree matches @: no differences, and --exit-code stays silent.
    let out = iso.run(&root, &["diff", "--exit-code"]);
    ok(&out);
    let out = iso.run(&root, &["diff", "--stat"]);
    ok(&out);
    assert_eq!(stdout(&out).trim(), "no differences");

    // Point-to-point across the two captured versions of a.txt (b.txt is
    // added in between).
    let range = format!("{oldest}..{newest}");
    let out = iso.run(&root, &["diff", &range]);
    ok(&out);
    let patch = stdout(&out);
    assert!(
        patch.contains("a.txt") && patch.contains("v1") && patch.contains("v2"),
        "{patch}"
    );

    let out = iso.run(&root, &["diff", "--exit-code", &range]);
    assert_eq!(code(&out), 1, "differences must mean exit 1");
    assert!(
        stdout(&out).contains("a.txt"),
        "patch still prints: {}",
        stdout(&out)
    );

    let out = iso.run(&root, &["diff", "--stat", &range]);
    ok(&out);
    let text = stdout(&out);
    assert!(text.contains("a.txt | "), "{text}");
    assert!(text.contains("2 files changed,"), "{text}");
    assert!(text.contains("insertions(+)"), "{text}");
    assert!(text.contains("(capture:"), "{text}");

    let out = iso.run(&root, &["diff", "--json", &range]);
    ok(&out);
    let value: serde_json::Value = serde_json::from_str(&stdout(&out)).unwrap();
    assert!(value["patch"].as_str().unwrap().contains("a.txt"));

    // A malformed range is refused at the CLI layer.
    let out = iso.run(&root, &["diff", "@.."]);
    fail(&out, 1, "needs a point on both sides of `..`");

    // A scope that escapes the project is refused.
    let out = iso.run(&root, &["diff", "--path", "../escape", &range]);
    fail(&out, 1, "outside the project");

    // An uncaptured worktree edit shows against @.
    std::fs::write(root.join("a.txt"), "v3 dirty\n").unwrap();
    let out = iso.run(&root, &["diff", "--exit-code"]);
    assert_eq!(code(&out), 1, "uncaptured edits must differ");
    let out = iso.run(&root, &["diff", "--stat"]);
    ok(&out);
    assert!(stdout(&out).contains("a.txt | "), "{}", stdout(&out));
}

// ---------------------------------------------------------------- restore

#[test]
fn restore_refuses_to_apply_offline_but_previews_dry_runs() {
    let iso = Iso::new("restore");
    let root = iso.enrolled_project();
    let (oldest, _, _) = three_capture_store(&root);

    // No point at all.
    let out = iso.run(&root, &["restore"]);
    fail(&out, 2, "restore needs a timeline point");

    // Applying needs the daemon; planning does not.
    let out = iso.run(&root, &["restore", &oldest]);
    fail(
        &out,
        1,
        "restore requires the running daemon; only `--dry-run` works offline",
    );

    let out = iso.run(&root, &["restore", "--dry-run", &oldest]);
    ok(&out);
    let text = stdout(&out);
    assert!(
        text.contains(&format!("restore to:  {}", &oldest[..12])),
        "{text}"
    );
    assert!(text.contains("scope:       whole worktree"), "{text}");
    assert!(text.contains("update  a.txt"), "{text}");
    assert!(text.contains("delete  b.txt"), "{text}");
    assert!(
        text.contains("1 to write, 1 to delete, 0 already current"),
        "{text}"
    );
    assert!(
        stderr(&out)
            .contains("note: daemon unavailable; this plan reads a read-only store snapshot"),
        "{}",
        stderr(&out)
    );

    // Scoped plan names the scope.
    let out = iso.run(&root, &["restore", "--dry-run", &oldest, "b.txt"]);
    ok(&out);
    assert!(
        stdout(&out).contains("scope:       b.txt"),
        "{}",
        stdout(&out)
    );

    // The plan as JSON carries the degraded flag.
    let out = iso.run(&root, &["restore", "--dry-run", "--json", &oldest]);
    ok(&out);
    let value: serde_json::Value = serde_json::from_str(&stdout(&out)).unwrap();
    assert_eq!(value["plan"]["degraded"], serde_json::json!(true));

    // Already satisfied: a noop plan says so and exits successfully.
    let out = iso.run(&root, &["restore", "--dry-run", "@"]);
    ok(&out);
    assert!(
        stdout(&out).contains("already there — nothing to do"),
        "{}",
        stdout(&out)
    );

    // An unknown scope path is called out but does not block by itself.
    let out = iso.run(&root, &["restore", "--dry-run", &oldest, "ghost.txt"]);
    ok(&out);
    assert!(
        stderr(&out).contains("no history or live path ever held `ghost.txt`"),
        "{}",
        stderr(&out)
    );

    // Resume/abandon are daemon writers, like apply.
    let out = iso.run(&root, &["restore", "--resume"]);
    fail(&out, 1, "resuming a restore requires the running daemon");
    let out = iso.run(&root, &["restore", "--abandon"]);
    fail(&out, 1, "abandoning a restore requires the running daemon");
}

#[test]
fn restore_reports_obstructions_and_exit_code_4() {
    let iso = Iso::new("restore-blocked");
    let root = iso.enrolled_project();
    let (oldest, _, _) = three_capture_store(&root);

    // A directory squatting on a path the restore must delete blocks the
    // whole plan; nothing changes and the reason is named.
    std::fs::remove_file(root.join("b.txt")).unwrap();
    std::fs::create_dir(root.join("b.txt")).unwrap();

    let out = iso.run(&root, &["restore", "--dry-run", &oldest]);
    assert_eq!(code(&out), 4, "blocked restores exit 4");
    let text = format!("{}{}", stdout(&out), stderr(&out));
    assert!(text.contains("a directory occupies this path"), "{text}");
    assert!(
        text.contains("restore blocked; nothing was changed"),
        "{text}"
    );
    assert!(stdout(&out).contains("BLOCKED b.txt"), "{}", stdout(&out));

    // The JSON path reports the same plan without the human markers.
    let out = iso.run(&root, &["restore", "--dry-run", "--json", &oldest]);
    assert_eq!(code(&out), 4);
    let value: serde_json::Value = serde_json::from_str(&stdout(&out)).unwrap();
    assert_eq!(value["plan"]["obstructions"].as_array().unwrap().len(), 1);
}

// ------------------------------------------------------------- checkpoint

#[test]
fn checkpoint_commands_enforce_the_daemon_boundary() {
    let iso = Iso::new("checkpoint");
    let root = iso.enrolled_project();
    three_capture_store(&root);

    // Bare `checkpoint` with no name explains itself.
    let out = iso.run(&root, &["checkpoint"]);
    fail(&out, 2, "checkpoint needs a name");

    // Creation is a timeline write: refused offline, with the why.
    let out = iso.run(&root, &["checkpoint", "create", "before-x"]);
    fail(
        &out,
        1,
        "checkpoint creation requires the running daemon; no offline writer fallback is allowed",
    );

    // Listing degrades to a read-only snapshot.
    let out = iso.run(&root, &["checkpoint", "list"]);
    ok(&out);
    assert!(stdout(&out).is_empty(), "{}", stdout(&out));
    assert!(
        stderr(&out).contains("note: daemon unavailable; showing a read-only store snapshot"),
        "{}",
        stderr(&out)
    );

    let out = iso.run(&root, &["checkpoint", "list", "--json"]);
    ok(&out);
    let value: serde_json::Value = serde_json::from_str(&stdout(&out)).unwrap();
    assert_eq!(value["checkpoints"], serde_json::json!([]));
    assert_eq!(value["degraded"], serde_json::json!(true));
}

// -------------------------------------------------------- fragment restore

#[test]
fn fragment_restore_payloads_parse_fail_closed_and_preview_offline() {
    let iso = Iso::new("fragment");
    let root = iso.enrolled_project();
    skeleton(&root);
    capture(&root, touched(&root, "note.txt", "keep TODO here\n"), 2);

    // A real handle comes from grep's NDJSON stream.
    let out = iso.run(&root, &["grep", "--json", "TODO"]);
    ok(&out);
    let mut handle_payload = None;
    for line in stdout(&out).lines().filter(|l| !l.is_empty()) {
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        if value["type"] == "summary" {
            let hits = value["report"]["hits"].as_array().unwrap();
            assert!(!hits.is_empty(), "fixture must match: {}", stdout(&out));
            handle_payload = Some(serde_json::to_string(&hits[0]).unwrap());
        }
    }
    let handle_payload = handle_payload.expect("grep summary record");

    // Malformed payloads are refused before any store access.
    let out = iso.run_stdin(
        &root,
        &["restore", "--selection", "-", "--dry-run"],
        "not json",
    );
    fail(&out, 1, "selection payload is not valid JSON");
    let out = iso.run_stdin(&root, &["restore", "--selection", "-", "--dry-run"], "[]");
    fail(&out, 1, "the selection payload holds no handles");
    let out = iso.run_stdin(
        &root,
        &["restore", "--selection", "-", "--dry-run"],
        "{\"a\":1}",
    );
    fail(&out, 1, "not a selection handle");

    // A missing file source reads as an error too.
    let out = iso.run(&root, &["restore", "--selection", "nope.json", "--dry-run"]);
    fail(&out, 1, "reading selection JSON from `nope.json`");

    // The stdin path previews the splice, degraded but applicable. The
    // selected text is identical to the live content, so the plan is a
    // truthful noop.
    let out = iso.run_stdin(
        &root,
        &["restore", "--selection", "-", "--dry-run"],
        &handle_payload,
    );
    ok(&out);
    let text = stdout(&out);
    assert!(
        text.contains("fragment restore:  replace  (1 selection)"),
        "{text}"
    );
    assert!(text.contains("note.txt"), "{text}");
    assert!(text.contains("replace"), "{text}");
    assert!(text.contains("already there — nothing to do"), "{text}");
    assert!(
        stderr(&out)
            .contains("note: daemon unavailable; this plan reads a read-only store snapshot"),
        "{}",
        stderr(&out)
    );

    // Applying the same splice still requires the daemon.
    let out = iso.run_stdin(&root, &["restore", "--selection", "-"], &handle_payload);
    fail(
        &out,
        1,
        "fragment restore requires the running daemon; only `--dry-run` works offline",
    );
}

// ----------------------------------------------------------------- doctor

#[test]
fn doctor_reports_offline_health_json_and_failure_exit_code() {
    let iso = Iso::new("doctor");
    let root = iso.enrolled_project();
    three_capture_store(&root);

    let out = iso.run(&root, &["doctor"]);
    ok(&out);
    let text = stdout(&out);
    assert!(
        text.contains("daemon:  not reachable (read-only offline sweep)"),
        "{text}"
    );
    assert!(text.contains("[ok  ]"), "{text}");
    assert!(text.contains("verdict: healthy"), "{text}");
    assert!(text.contains("store:   journal "), "{text}");
    assert!(text.contains("history: 3 captures,"), "{text}");

    let out = iso.run(&root, &["doctor", "--json"]);
    ok(&out);
    let value: serde_json::Value = serde_json::from_str(&stdout(&out)).unwrap();
    assert_eq!(value["ok"], serde_json::json!(true));
    assert!(value["checks"].as_array().unwrap().len() >= 5);

    // A broken config fails the sweep with the dedicated exit code.
    std::fs::write(root.join(".sheaf/config.toml"), "not [valid toml").unwrap();
    let out = iso.run(&root, &["doctor"]);
    fail(&out, 5, "verdict: problems found (see FAIL lines)");
    assert!(stdout(&out).contains("[FAIL]"), "{}", stdout(&out));

    // JSON reports the failure without the exit code (contract: parseable
    // output first; humans get the exit status).
    let out = iso.run(&root, &["doctor", "--json"]);
    ok(&out);
    let value: serde_json::Value = serde_json::from_str(&stdout(&out)).unwrap();
    assert_eq!(value["ok"], serde_json::json!(false));
}

#[test]
fn doctor_fix_runs_an_offline_repair_pass() {
    let iso = Iso::new("doctor-fix");
    let root = iso.enrolled_project();
    three_capture_store(&root);

    let out = iso.run(&root, &["doctor", "--fix"]);
    ok(&out);
    let text = stdout(&out);
    assert!(
        text.contains("fixes applied: 0"),
        "healthy store, nothing to fix: {text}"
    );
    assert!(text.contains("verdict: healthy"), "{text}");
}

// --------------------------------------------------------------------- gc

#[test]
fn gc_offline_reports_plans_expiry_marks_and_applies() {
    let iso = Iso::new("gc");
    let root = iso.enrolled_project();
    let (oldest, middle, newest) = three_capture_store(&root);

    let out = iso.run(&root, &["gc"]);
    ok(&out);
    let text = stdout(&out);
    assert!(
        text.contains("gc plan (report only; rerun with --apply):"),
        "{text}"
    );
    assert!(
        text.contains("retention: no expiry set, no marks (history is kept whole)"),
        "{text}"
    );

    let out = iso.run(&root, &["gc", "--json"]);
    ok(&out);
    let value: serde_json::Value = serde_json::from_str(&stdout(&out)).unwrap();
    assert_eq!(value["stage"], serde_json::json!("planned"));

    // Expiry is a config write; the report reflects it.
    let out = iso.run(&root, &["gc", "--set-expiry", "45m"]);
    ok(&out);
    assert!(
        stdout(&out).contains("edit expiry set to 45m (reachability-bound"),
        "{}",
        stdout(&out)
    );
    let cfg = std::fs::read_to_string(root.join(".sheaf/config.toml")).unwrap();
    assert!(cfg.contains("45m"), "{cfg}");
    let out = iso.run(&root, &["gc", "--json", "--set-expiry", "72h"]);
    ok(&out);
    let value: serde_json::Value = serde_json::from_str(&stdout(&out)).unwrap();
    assert_eq!(value["expiry"], serde_json::json!("72h"));

    let out = iso.run(&root, &["gc", "--set-expiry", "bogus"]);
    fail(&out, 1, "invalid expiry `bogus`");

    let out = iso.run(&root, &["gc"]);
    ok(&out);
    // The last expiry set wins; the plan names its protected points.
    let text = stdout(&out);
    assert!(
        text.contains("retention: expiry 72h (reachability-bound)"),
        "{}",
        text
    );
    assert!(text.contains("protected:"), "{text}");

    // Marking an older capture makes it collectable despite reachability.
    let out = iso.run(&root, &["gc", &oldest]);
    ok(&out);
    assert!(
        stdout(&out).contains(&format!(
            "capture {} marked collectable; gc --apply will reclaim it",
            &oldest[..12]
        )),
        "{}",
        stdout(&out)
    );
    let out = iso.run(&root, &["gc", &oldest]);
    ok(&out);
    assert!(
        stdout(&out).contains("was already marked"),
        "{}",
        stdout(&out)
    );

    let out = iso.run(&root, &["gc", "--json", &oldest]);
    ok(&out);
    let value: serde_json::Value = serde_json::from_str(&stdout(&out)).unwrap();
    assert_eq!(value["capture_id"], serde_json::json!(oldest));
    assert_eq!(value["already_marked"], serde_json::json!(true));

    let out = iso.run(&root, &["gc"]);
    ok(&out);
    assert!(
        stdout(&out).contains("prunable: 1 capture(s) [1 marked]"),
        "{}",
        stdout(&out)
    );

    // Apply reclaims the marked history and keeps the rest addressable.
    let out = iso.run(&root, &["gc", "--apply"]);
    ok(&out);
    let text = stdout(&out);
    assert!(text.contains("gc applied:"), "{text}");
    assert!(text.contains("retention trimmed 1 capture(s)"), "{text}");
    assert!(text.contains("timeline intact:"), "{text}");

    let out = iso.run(&root, &["log"]);
    ok(&out);
    let text = stdout(&out);
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 2, "the marked capture is gone: {text}");
    assert!(
        lines[0].contains(&middle[..12]) && lines[1].contains(&newest[..12]),
        "{text}"
    );
}

// ------------------------------------------------------------------ cache

#[test]
fn cache_backfill_and_rebuild_work_offline_with_reports() {
    let iso = Iso::new("cache");
    let root = iso.enrolled_project();
    three_capture_store(&root);

    // A bounded first pass is a single page; whether the store writer has
    // pre-populated the cache decides +N/M, so only stable parts asserted.
    let out = iso.run(&root, &["cache", "backfill", "--limit", "1"]);
    ok(&out);
    let text = stdout(&out);
    assert!(text.contains("grep cache backfilled:"), "{text}");
    assert!(text.contains("coverage at 3"), "{text}");
    assert!(text.contains("watermark gen"), "{text}");

    // A rebuild re-indexes every capture and reports full coverage.
    let out = iso.run(&root, &["cache", "rebuild"]);
    ok(&out);
    let text = stdout(&out);
    assert!(text.contains("grep cache rebuilt:"), "{text}");
    assert!(text.contains("covers 3 capture(s)"), "{text}");

    let out = iso.run(&root, &["cache", "backfill", "--json"]);
    ok(&out);
    let value: serde_json::Value = serde_json::from_str(&stdout(&out)).unwrap();
    assert_eq!(value["complete"], serde_json::json!(true));
}

// ----------------------------------------------------------------- squash

#[test]
fn squash_previews_explicit_anchors_offline_and_refuses_sanctioned_writes() {
    let iso = Iso::new("squash");
    let root = iso.enrolled_project();
    let (_, _, _) = three_capture_store(&root);

    // No frame stamped yet: the default anchor explains what to do.
    let out = iso.run(&root, &["squash"]);
    fail(&out, 1, "no commit frames stamped yet");
    fail(&out, 1, "or commit with `--` to stamp the first frame");

    // An explicit anchor previews read-only, degraded.
    let out = iso.run(&root, &["squash", "@~1"]);
    ok(&out);
    let text = stdout(&out);
    assert!(
        text.contains("squash preview — nothing runs, nothing is written"),
        "{text}"
    );
    assert!(text.contains("anchor:      @~1"), "{text}");
    assert!(text.contains("span:        1 capture,"), "{text}");
    assert!(text.contains("draft commit message"), "{text}");
    assert!(
        text.contains("to commit this span: sheaf squash @~1 -- <git commit options>"),
        "{text}"
    );
    assert!(
        stderr(&out)
            .contains("note: daemon unavailable; this preview reads a read-only store snapshot"),
        "{}",
        stderr(&out)
    );

    // The preview JSON carries the same plan for scripts.
    let out = iso.run(&root, &["squash", "--json", "@~1"]);
    ok(&out);
    let value: serde_json::Value = serde_json::from_str(&stdout(&out)).unwrap();
    assert_eq!(value["anchor"]["label"], serde_json::json!("@~1"));
    assert_eq!(value["degraded"], serde_json::json!(true));
    assert!(value["draft_subject"].is_string());

    // `--` is the sanction to commit: refused without a daemon.
    let out = iso.run(&root, &["squash", "@~1", "--", "-m", "hi"]);
    fail(&out, 1, "squash `--` requires the running daemon");

    // A span with an upper bound can never be sanctioned.
    let out = iso.run(&root, &["squash", "@~2..@~1", "--", "-m", "hi"]);
    fail(&out, 1, "not `A..B`");

    // Malformed ranges are refused before anything else.
    let out = iso.run(&root, &["squash", "garbage.."]);
    fail(&out, 1, "needs a point on both sides of `..`");
    let out = iso.run(&root, &["squash", "@~3...@~1"]);
    fail(&out, 1, "three-dot ranges are not a squash range");
}

// ------------------------------------------------------------------ color

#[test]
fn color_flags_paint_human_output_but_never_json() {
    let iso = Iso::new("color");
    let root = iso.enrolled_project();
    three_capture_store(&root);

    let out = iso.run(&root, &["log", "--color", "always"]);
    ok(&out);
    assert!(
        stdout(&out).contains("\x1b[36m"),
        "painted: {}",
        stdout(&out)
    );

    let out = iso.run(&root, &["log", "--color", "never"]);
    ok(&out);
    assert!(!stdout(&out).contains('\x1b'), "plain: {}", stdout(&out));

    let out = iso.run(&root, &["log", "--json", "--color", "always"]);
    ok(&out);
    assert!(
        !stdout(&out).contains('\x1b'),
        "JSON is never decorated: {}",
        stdout(&out)
    );
}

// ---------------------------------------------------------------- service

#[test]
fn service_status_reports_the_unit_without_touching_it() {
    let iso = Iso::new("service");
    let root = iso.enrolled_project();

    // `status` is read-only (is-active never changes unit state). Install
    // and remove are deliberately not exercised here: they would mutate
    // the developer's real systemd user session.
    let out = iso.run(&root, &["service", "status"]);
    ok(&out);
    let text = stdout(&out);
    assert!(text.contains("unit:"), "{text}");
    assert!(
        text.contains("(present)") || text.contains("(not installed)"),
        "{text}"
    );
}

#[test]
fn service_install_no_start_writes_unit_and_remove_cleans_it() {
    let iso = Iso::new("service-install");
    let root = iso.enrolled_project();

    let out = iso.run(&root, &["service", "install", "--no-start"]);
    ok(&out);
    let text = stdout(&out);
    assert!(text.contains("unit:"), "{text}");
    assert!(
        text.contains("next:    systemctl --user daemon-reload"),
        "{text}"
    );
    let unit = iso.0.path().join("xdg-config");
    assert!(unit.join("systemd/user/sheafd.service").is_file());

    let out = iso.run(&root, &["service", "remove"]);
    ok(&out);
    assert!(stdout(&out).contains("unit removed:"), "{}", stdout(&out));
}

#[test]
fn argument_validation_rejects_conflicting_restore_and_grep_flags() {
    let iso = Iso::new("usage-errors");
    let root = iso.enrolled_project();

    let out = iso.run(
        &root,
        &[
            "restore",
            "--selection",
            "missing.json",
            "--insert",
            "--delete",
        ],
    );
    fail(&out, 2, "cannot be used");

    let out = iso.run(&root, &["grep", "--max-results", "0", "needle"]);
    fail(&out, 2, "invalid value");

    let out = iso.run(&root, &["restore", "--help"]);
    ok(&out);
    assert!(stdout(&out).contains("--dry-run"), "{}", stdout(&out));
    assert!(stdout(&out).contains("--selection"), "{}", stdout(&out));
}

#[test]
fn smart_squash_mutation_requires_daemon_even_with_valid_selection() {
    let iso = Iso::new("smart-offline-refusal");
    let root = iso.enrolled_project();
    capture(&root, touched(&root, "a.txt", "needle\n"), 1);

    let out = iso.run(&root, &["grep", "needle", "--json"]);
    ok(&out);
    let selection = stdout(&out)
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find_map(|value| {
            (value["type"] == "summary")
                .then(|| serde_json::to_string(&value["report"]["hits"][0]).unwrap())
        })
        .expect("grep selection");
    let payload = iso.0.path().join("selection.json");
    std::fs::write(&payload, selection).unwrap();

    let out = iso.run(
        &root,
        &[
            "squash",
            "--selection",
            payload.to_str().unwrap(),
            "--",
            "-m",
            "must not run offline",
        ],
    );
    fail(&out, 1, "requires the running daemon");
}

#[test]
fn cache_backfill_without_limit_aggregates_multiple_pages() {
    let iso = Iso::new("cache-pages");
    let root = iso.enrolled_project();
    for n in 0..130 {
        let rel = format!("captures/{n}.txt");
        capture(&root, touched(&root, &rel, &format!("capture {n}\n")), 1);
    }

    let out = iso.run(&root, &["cache", "backfill"]);
    ok(&out);
    let text = stdout(&out);
    assert!(text.contains("grep cache backfilled:"), "{text}");
    assert!(
        text.contains("covers 130 capture(s)") || text.contains("coverage at 130"),
        "{text}"
    );
}

#[test]
fn malformed_grep_inputs_fail_before_store_access() {
    let iso = Iso::new("grep-malformed");
    let root = iso.enrolled_project();
    let out = iso.run(
        &root,
        &["grep", "--history", "needle", "--after", "a:b:c:d"],
    );
    fail(
        &out,
        1,
        "grep --after expects CAPTURE, RESUME:INDEX, or AFTER:RESUME:INDEX",
    );
    let out = iso.run(
        &root,
        &[
            "grep",
            "--history",
            "needle",
            "--after",
            "done:resume:not-a-number",
        ],
    );
    fail(
        &out,
        1,
        "grep --after record index must be an unsigned integer",
    );
    let out = iso.run(
        &root,
        &[
            "grep",
            "--history",
            "needle",
            "--after",
            "resume:not-a-number",
        ],
    );
    fail(
        &out,
        1,
        "grep --after record index must be an unsigned integer",
    );
    let out = iso.run(
        &root,
        &["grep", "--history", "needle", "--selection", "missing.json"],
    );
    fail(&out, 1, "reading the selection anchor missing.json");
    let selection = iso.0.path().join("bad-selection.json");
    std::fs::write(&selection, "not json").unwrap();
    let out = iso.run(
        &root,
        &[
            "grep",
            "--history",
            "needle",
            "--selection",
            selection.to_str().unwrap(),
        ],
    );
    fail(&out, 1, "the selection anchor must be JSON");
    let out = iso.run_stdin(
        &root,
        &["grep", "--history", "needle", "--selection", "-"],
        "not json",
    );
    fail(&out, 1, "the selection anchor must be JSON");
}

#[test]
fn truncated_store_is_reported_by_doctor() {
    let iso = Iso::new("corrupt-journal");
    let root = iso.enrolled_project();
    capture(&root, touched(&root, "a.txt", "needle\n"), 1);
    let journal = std::fs::read_dir(root.join(".sheaf/store/journal"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.is_file())
        .expect("journal segment");
    std::fs::write(journal, b"broken frame").unwrap();
    let out = iso.run(&root, &["doctor"]);
    fail(&out, 5, "problems found");
}

#[test]
fn grep_truncation_emits_resume_cursor() {
    let iso = Iso::new("grep-truncated");
    let root = iso.enrolled_project();
    capture(&root, touched(&root, "a.txt", "needle one\n"), 2);
    capture(&root, touched(&root, "a.txt", "needle two\n"), 1);
    let out = iso.run(
        &root,
        &["grep", "--history", "--max-results", "1", "needle"],
    );
    ok(&out);
    assert!(
        stderr(&out).contains("results truncated"),
        "{}",
        stderr(&out)
    );
    assert!(stderr(&out).contains("--after"), "{}", stderr(&out));
}

#[test]
fn smart_squash_requires_git_history_after_valid_selection() {
    let iso = Iso::new("smart-no-git");
    let root = iso.enrolled_project();
    capture(&root, touched(&root, "a.txt", "needle\n"), 1);
    let grep = iso.run(&root, &["grep", "--json", "needle"]);
    ok(&grep);
    let hit = stdout(&grep)
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .find_map(|v| (v["type"] == "summary").then(|| v["report"]["hits"][0].clone()))
        .expect("grep hit");
    let selection = iso.0.path().join("selection.json");
    std::fs::write(&selection, serde_json::to_vec(&hit).unwrap()).unwrap();
    let out = iso.run(
        &root,
        &["squash", "--selection", selection.to_str().unwrap()],
    );
    fail(&out, 1, "at least one git commit");
}

#[test]
fn service_install_reports_systemctl_failure() {
    let iso = Iso::new("service-start-failure");
    let root = iso.enrolled_project();
    let out = iso.run(&root, &["service", "install"]);
    fail(&out, 1, "systemctl --user");
}
