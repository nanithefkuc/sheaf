//! Daemon-backed CLI dispatch: the write paths and happy paths that have no
//! offline story. A real `sheafd` watches a real git repo while the real
//! `sheaf` binary drives `checkpoint create`, `restore` apply, `status`,
//! `log`, `doctor`, and `cache`. The harness follows tests/smart_squash.rs:
//! process-global XDG dirs are isolated per test, so tests serialize on a
//! mutex — and like smart_squash, this file needs the workspace binaries
//! (`scripts/coverage.sh`, or `cargo build --workspace --bins` beforehand).

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use serde_json::Value;
use sheaf_core::ipc::Client;

static ENV_LOCK: Mutex<()> = Mutex::new(());

const SHEAF: &str = env!("CARGO_BIN_EXE_sheaf");
const V1: &str = "fn one() {}\n";
const V2: &str = "fn two() {}\n";
const V3: &str = "fn three() {}\n";

struct EnvGuard {
    _lock: MutexGuard<'static, ()>,
    saved: Vec<(&'static str, Option<std::ffi::OsString>)>,
    base: PathBuf,
}

impl EnvGuard {
    fn new(tag: &str) -> EnvGuard {
        let lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let base =
            std::env::temp_dir().join(format!("sheaf-cli-dispatch-{tag}-{}", std::process::id()));
        let vars = ["XDG_DATA_HOME", "XDG_RUNTIME_DIR", "SHEAF_SOCKET"];
        let saved: Vec<(&'static str, Option<std::ffi::OsString>)> =
            vars.iter().map(|k| (*k, std::env::var_os(k))).collect();
        std::env::set_var("XDG_DATA_HOME", base.join("data"));
        std::env::set_var("XDG_RUNTIME_DIR", base.join("run"));
        std::env::remove_var("SHEAF_SOCKET");
        std::fs::create_dir_all(base.join("data")).unwrap();
        std::fs::create_dir_all(base.join("run")).unwrap();
        EnvGuard {
            _lock: lock,
            saved,
            base,
        }
    }

    fn socket(&self) -> PathBuf {
        self.base.join("run").join("sheaf.sock")
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        for (k, v) in &self.saved {
            match v {
                Some(val) => std::env::set_var(k, val),
                None => std::env::remove_var(k),
            }
        }
        let _ = std::fs::remove_dir_all(&self.base);
    }
}

struct Daemon {
    child: Child,
}

impl Daemon {
    fn spawn(socket: &Path) -> Daemon {
        // sheafd lives beside the sheaf binary in the workspace target dir.
        let daemon_bin = Path::new(SHEAF)
            .parent()
            .expect("target dir")
            .join("sheafd");
        let child = Command::new(&daemon_bin)
            .args(["run", "--socket"])
            .arg(socket)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap_or_else(|e| panic!("spawn sheafd at {daemon_bin:?}: {e}"));
        let d = Daemon { child };
        let deadline = Instant::now() + Duration::from_secs(10);
        while !socket.exists() {
            assert!(Instant::now() < deadline, "daemon socket never appeared");
            std::thread::sleep(Duration::from_millis(50));
        }
        d
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Fixture {
    _env: EnvGuard,
    _daemon: Daemon,
    root: PathBuf,
    socket: PathBuf,
}

/// A git repo with one commit, enrolled via the CLI while the daemon
/// watches, and the worktree captured up to HEAD.
fn fixture(tag: &str, initial: &str) -> Fixture {
    let env = EnvGuard::new(tag);
    let root = env.base.join("proj");
    std::fs::create_dir_all(&root).unwrap();
    let socket = env.socket();
    let daemon = Daemon::spawn(&socket);
    git(&root, &["init", "-q"]);
    git(&root, &["config", "user.email", "t@example.com"]);
    git(&root, &["config", "user.name", "T"]);
    write_file(&root, "src/lib.rs", initial);
    git(&root, &["add", "--all"]);
    git(&root, &["commit", "-q", "-m", "initial"]);

    // Enroll through the real CLI dispatch (not core's init_project) so the
    // command's own report is under test too.
    let out = Command::new(SHEAF)
        .env("SHEAF_SOCKET", &socket)
        .arg("init")
        .current_dir(&root)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run sheaf init");
    assert!(
        out.status.success(),
        "init failed: {}{}",
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout)
    );
    let out = String::from_utf8_lossy(&out.stdout).into_owned();
    assert!(
        out.contains("daemon:        notified — watching live"),
        "{out}"
    );

    let fx = Fixture {
        _env: env,
        _daemon: daemon,
        root,
        socket,
    };
    wait_caught_up(&fx);
    fx
}

fn git(root: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(args)
        .current_dir(root)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .output()
        .expect("run git");
    assert!(
        out.status.success(),
        "git {:?} failed: {}{}",
        args,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).into_owned()
}

fn sheaf(fx: &Fixture, args: &[&str]) -> (bool, String, String) {
    sheaf_at(fx, &fx.root, args)
}

fn sheaf_at(fx: &Fixture, root: &Path, args: &[&str]) -> (bool, String, String) {
    let out = Command::new(SHEAF)
        .env("SHEAF_SOCKET", &fx.socket)
        .args(args)
        .current_dir(root)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("run sheaf");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn write_file(root: &Path, rel: &str, contents: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, contents).unwrap();
}

/// Block until the daemon has captured the current worktree state.
fn wait_caught_up(fx: &Fixture) {
    wait_caught_up_at(fx, &fx.root);
}

fn wait_caught_up_at(fx: &Fixture, root: &Path) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(mut client) = Client::connect(&fx.socket, Duration::from_secs(2)) {
            if let Ok(reply) = client.call(
                "diff",
                Some(root),
                serde_json::json!({ "from": "@", "to": null, "paths": [] }),
                None,
            ) {
                if reply.response.ok {
                    let pending = reply
                        .response
                        .result
                        .and_then(|v| v.get("diff").and_then(|d| d.get("entries")).cloned())
                        .and_then(|e| e.as_array().map(|a| a.len()))
                        .unwrap_or(usize::MAX);
                    if pending == 0 {
                        return;
                    }
                }
            }
        }
        assert!(Instant::now() < deadline, "timeline never caught up");
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn json_out(out: &str) -> Value {
    serde_json::from_str(out).unwrap_or_else(|e| panic!("json output: {e}: {out}"))
}

// --------------------------------------------------------- checkpoint write

#[test]
fn checkpoint_create_writes_names_and_the_log_annotates_them() {
    let fx = fixture("checkpoint", V1);

    // Explicit form, bare shorthand, and a pinned earlier point.
    let (ok, out, err) = sheaf(&fx, &["checkpoint", "create", "before-work"]);
    assert!(ok, "{err}");
    assert!(out.contains("checkpoint before-work -> "), "{out}");

    write_file(&fx.root, "src/lib.rs", V2);
    wait_caught_up(&fx);
    let (ok, out, err) = sheaf(&fx, &["checkpoint", "after-work"]);
    assert!(ok, "{err}");
    assert!(out.contains("checkpoint after-work -> "), "{out}");
    let (ok, out, err) = sheaf(&fx, &["checkpoint", "create", "earlier", "--at", "@~1"]);
    assert!(ok, "{err}");
    assert!(out.contains("checkpoint earlier -> "), "{out}");

    // The human list carries name, 12-char id, and a timestamp; JSON
    // carries all three names machine-readable.
    let (ok, out, err) = sheaf(&fx, &["checkpoint", "list"]);
    assert!(ok, "{err}");
    for name in ["before-work", "after-work", "earlier"] {
        assert!(out.contains(name), "missing {name}: {out}");
    }
    let (ok, out, err) = sheaf(&fx, &["checkpoint", "list", "--json"]);
    assert!(ok, "{err}");
    let value = json_out(&out);
    assert_eq!(value["degraded"], serde_json::json!(false));
    let names: Vec<&str> = value["checkpoints"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"before-work"));
    assert!(names.contains(&"earlier"));

    // A capture after the pin annotates the log view.
    write_file(&fx.root, "src/lib.rs", V3);
    wait_caught_up(&fx);
    let (ok, out, err) = sheaf(&fx, &["log"]);
    assert!(ok, "{err}");
    assert!(out.contains("[Checkpoints: before-work, earlier]"), "{out}");

    // The name resolves as a restore point through the daemon planner.
    let (ok, out, err) = sheaf(&fx, &["restore", "--dry-run", "checkpoint:before-work"]);
    assert!(ok, "{err}");
    assert!(out.contains("restore to:"), "{out}");
    assert!(out.contains("scope:       whole worktree"), "{out}");
}

// ------------------------------------------------------------- branch CRUD

#[test]
fn branch_commands_preserve_metadata_and_name_automatic_divergence() {
    let fx = fixture("branch", V1);
    let (ok, out, err) = sheaf(&fx, &["branch", "list", "--json"]);
    assert!(ok, "{err}");
    let initial = json_out(&out);
    let names = |value: &serde_json::Value| -> Vec<String> {
        value["graph"]["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|node| node["branches"].as_array().cloned().unwrap_or_default())
            .map(|branch| branch["name"].as_str().unwrap().to_owned())
            .collect()
    };
    assert_eq!(names(&initial), vec!["main".to_owned()]);

    let (ok, out, err) = sheaf(
        &fx,
        &[
            "branch",
            "create",
            "experiment",
            "--metadata",
            "description=parser work",
            "--metadata",
            "tests=pass",
        ],
    );
    assert!(ok, "{err}");
    assert!(out.contains("branch experiment -> "), "{out}");

    let (ok, out, err) = sheaf(&fx, &["branch", "rename", "experiment", "primary"]);
    assert!(ok, "{err}");
    assert!(
        out.contains("branch experiment renamed to primary"),
        "{out}"
    );

    write_file(&fx.root, "src/lib.rs", V2);
    wait_caught_up(&fx);
    let (ok, _, err) = sheaf(&fx, &["restore", "@~1"]);
    assert!(ok, "{err}");
    write_file(&fx.root, "src/lib.rs", V3);
    wait_caught_up(&fx);

    let (ok, out, err) = sheaf(&fx, &["branch", "list", "--json"]);
    assert!(ok, "{err}");
    let value = json_out(&out);
    assert_eq!(value["degraded"], serde_json::json!(false));
    let all_branches: Vec<serde_json::Value> = value["graph"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|node| node["branches"].as_array().cloned().unwrap_or_default())
        .collect();
    assert_eq!(all_branches.len(), 3, "{value:#}");
    assert!(all_branches.iter().any(|branch| branch["name"] == "main"));
    let primary = all_branches
        .iter()
        .find(|branch| branch["name"] == "primary")
        .expect("renamed branch");
    assert_eq!(primary["metadata"]["description"], "parser work");
    assert_eq!(primary["metadata"]["tests"], "pass");
    let automatic = all_branches
        .iter()
        .find(|branch| {
            branch["name"]
                .as_str()
                .is_some_and(|name| name.starts_with("branch-"))
        })
        .expect("automatic branch name");
    let automatic_name = automatic["name"].as_str().unwrap().to_owned();

    let (ok, out, err) = sheaf(&fx, &["branch", "delete", &automatic_name]);
    assert!(ok, "{err}");
    assert!(
        out.contains(&format!("deleted branch {automatic_name}")),
        "{out}"
    );
    let (ok, out, err) = sheaf(&fx, &["branch", "list", "--json"]);
    assert!(ok, "{err}");
    let value = json_out(&out);
    let remaining = names(&value);
    assert_eq!(remaining.len(), 2);
    assert!(remaining.iter().any(|name| name == "main"));
    assert!(remaining.iter().any(|name| name == "primary"));
}

// ------------------------------------------------------------- status/log

#[test]
fn status_and_log_report_a_live_daemon() {
    let fx = fixture("status", V1);
    write_file(&fx.root, "src/lib.rs", V2);
    wait_caught_up(&fx);

    let (ok, out, err) = sheaf(&fx, &["status"]);
    assert!(ok, "{err}");
    assert!(out.contains("daemon:        running v"), "{out}");
    assert!(out.contains("watching:      yes"), "{out}");
    assert!(out.contains("enrolled:      yes"), "{out}");
    assert!(out.contains("store:         format 2"), "{out}");

    // Through the daemon, reads are not degraded.
    let (ok, out, err) = sheaf(&fx, &["log", "--json"]);
    assert!(ok, "{err}");
    let value = json_out(&out);
    assert_eq!(value["degraded"], serde_json::json!(false));
    assert!(value["entries"].as_array().unwrap().len() >= 2);

    let (ok, out, err) = sheaf(&fx, &["log"]);
    assert!(ok, "{err}");
    assert!(
        !err.contains("daemon unavailable"),
        "unexpected degraded note: {err}"
    );
    let lines: Vec<&str> = out.lines().collect();
    assert!(lines.len() >= 2, "{out}");
    // Newest lands at the bottom of the human view.
    assert!(
        lines[lines.len() - 1].contains(&value["entries"][0]["id"].as_str().unwrap()[..12]),
        "{out}"
    );
}

// ----------------------------------------------------------- restore apply

#[test]
fn restore_applies_scoped_then_full_and_reports_both_modes() {
    let fx = fixture("restore", V1);
    write_file(&fx.root, "src/lib.rs", V2);
    wait_caught_up(&fx);

    // Scoped: preview first, then apply — the file goes back, history
    // records the restore, and no branching is announced.
    let (ok, out, err) = sheaf(&fx, &["restore", "--dry-run", "@~1", "src/lib.rs"]);
    assert!(ok, "{err}");
    assert!(out.contains("scope:       src/lib.rs"), "{out}");
    assert!(out.contains("update  src/lib.rs"), "{out}");

    let (ok, out, err) = sheaf(&fx, &["restore", "@~1", "src/lib.rs"]);
    assert!(ok, "{err}");
    assert!(out.contains("restored to "), "{out}");
    assert!(out.contains("recorded:"), "forward history missing: {out}");
    assert!(out.contains("undo:"), "{out}");
    assert!(
        !out.contains("branching:"),
        "scoped restore must not branch: {out}"
    );
    assert_eq!(
        std::fs::read_to_string(fx.root.join("src/lib.rs")).unwrap(),
        V1,
        "scoped restore put the old content back"
    );

    // The scoped restore is visible as ordinary forward history.
    let (ok, out, err) = sheaf(&fx, &["log"]);
    assert!(ok, "{err}");
    assert!(out.contains("[restore"), "origin suffix missing: {out}");

    // Full tree: the same apply announces divergence and an undo point.
    write_file(&fx.root, "src/lib.rs", V3);
    wait_caught_up(&fx);
    let (ok, out, err) = sheaf(&fx, &["restore", "@~1"]);
    assert!(ok, "{err}");
    assert!(out.contains("branching:"), "{out}");
    assert!(out.contains("undo:        sheaf restore "), "{out}");
    assert_eq!(
        std::fs::read_to_string(fx.root.join("src/lib.rs")).unwrap(),
        V1,
        "full restore put the old content back"
    );

    // The abandoned future (V3) remains named and can be inspected one
    // lineage at a time.
    let (ok, out, err) = sheaf(&fx, &["branch", "list", "--json"]);
    assert!(ok, "{err}");
    let graph = json_out(&out);
    let abandoned = graph["graph"]["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|node| node["branches"].as_array().unwrap())
        .find(|branch| !branch["on_current"].as_bool().unwrap())
        .and_then(|branch| branch["name"].as_str())
        .expect("full restore names the abandoned branch");
    let (ok, out, err) = sheaf(&fx, &["log", "--branch", abandoned, "--json"]);
    assert!(ok, "{err}");
    let branch_log = json_out(&out);
    assert!(!branch_log["entries"].as_array().unwrap().is_empty());
    assert!(branch_log["entries"]
        .as_array()
        .unwrap()
        .iter()
        .any(|entry| !entry["on_current"].as_bool().unwrap()));
}

// ------------------------------------------------- doctor/cache via daemon

#[test]
fn doctor_and_cache_run_against_the_live_daemon() {
    let fx = fixture("doctor", V1);
    write_file(&fx.root, "src/lib.rs", V2);
    wait_caught_up(&fx);

    let (ok, out, err) = sheaf(&fx, &["doctor"]);
    assert!(ok, "{err}{out}");
    assert!(
        out.contains("daemon:  reachable (sweep ran against the live store)"),
        "{out}"
    );
    assert!(out.contains("verdict: healthy"), "{out}");

    let (ok, out, err) = sheaf(&fx, &["cache", "backfill", "--limit", "2"]);
    assert!(ok, "{err}");
    assert!(out.contains("grep cache backfilled:"), "{out}");

    let (ok, out, err) = sheaf(&fx, &["gc"]);
    assert!(ok, "{err}");
    assert!(
        out.contains("gc plan (report only; rerun with --apply):"),
        "{out}"
    );
    assert!(out.contains("retention: no expiry set, no marks"), "{out}");
}

#[test]
fn info_diff_gc_mark_and_fragment_plan_use_daemon_protocol() {
    let fx = fixture("daemon-reads", V1);
    write_file(&fx.root, "src/lib.rs", V2);
    wait_caught_up(&fx);

    let (ok, log, err) = sheaf(&fx, &["log", "--json"]);
    assert!(ok, "{err}");
    let entries = json_out(&log)["entries"].as_array().unwrap().clone();
    let reference = entries.last().unwrap()["id"].as_str().unwrap();

    let (ok, out, err) = sheaf(&fx, &["info", "--json", reference]);
    assert!(ok, "{err}");
    let value = json_out(&out);
    assert_eq!(value["degraded"], serde_json::json!(false));
    assert!(value["info"]["diff"]["entries"].is_array(), "{out}");

    let (ok, out, err) = sheaf(&fx, &["diff", "--json", "@~1"]);
    assert!(ok, "{err}");
    let value = json_out(&out);
    assert!(value["diff"]["degraded"].as_bool() == Some(false), "{out}");
    assert!(value["patch"].as_str().unwrap().contains("lib.rs"), "{out}");

    let (ok, out, err) = sheaf(&fx, &["gc", "--json", reference]);
    assert!(ok, "{err}");
    assert!(json_out(&out)["capture_id"].is_string(), "{out}");

    let (ok, grep, err) = sheaf(&fx, &["grep", "one", "--at", "@~1", "--json"]);
    assert!(ok, "{err}");
    let selection = grep
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find_map(|value| {
            (value["type"] == "summary")
                .then(|| serde_json::to_string(&value["report"]["hits"][0]).unwrap())
        })
        .expect("grep summary selection");
    let selection_path = fx.root.join("selection.json");
    std::fs::write(&selection_path, selection).unwrap();
    let selection_path = selection_path.to_string_lossy().into_owned();
    let (ok, out, err) = sheaf(
        &fx,
        &[
            "restore",
            "--selection",
            &selection_path,
            "--insert",
            "--dry-run",
            "--json",
        ],
    );
    assert!(!ok, "conflicting fragment unexpectedly succeeded: {out}");
    assert!(err.contains("fragment restore blocked"), "{err}");
    assert!(json_out(&out)["plan"]["conflicts"].is_array(), "{out}");
}

#[test]
fn fragment_restore_applies_a_selection_and_reports_json_outcome() {
    let fx = fixture("fragment-apply", "before\nTODO\nafter\n");
    write_file(&fx.root, "src/lib.rs", "before\nafter\n");
    wait_caught_up(&fx);

    let (ok, grep, err) = sheaf(
        &fx,
        &["grep", "TODO", "--extent", "line", "--at", "@~1", "--json"],
    );
    assert!(ok, "grep failed: {err}");
    let payload = grep
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find_map(|value| {
            (value["type"] == "summary")
                .then(|| serde_json::to_string(&value["report"]["hits"][0]).unwrap())
        })
        .expect("grep summary selection");
    let selection_path = fx.root.join("selection.json");
    std::fs::write(&selection_path, payload).unwrap();
    let path = selection_path.to_string_lossy().into_owned();

    let (ok, out, err) = sheaf(
        &fx,
        &["restore", "--selection", &path, "--insert", "--dry-run"],
    );
    assert!(ok, "fragment preview failed: {err}\n{out}");
    assert!(out.contains("insert"), "{out}");
    assert!(out.contains("src/lib.rs"), "{out}");

    let (ok, out, err) = sheaf(
        &fx,
        &["restore", "--selection", &path, "--insert", "--json"],
    );
    assert!(ok, "fragment apply failed: {err}\n{out}");
    let value = json_out(&out);
    assert!(value["outcome"].is_object(), "{out}");
    assert_eq!(
        std::fs::read_to_string(fx.root.join("src/lib.rs")).unwrap(),
        "before\nTODO\nafter\n"
    );
}

#[test]
fn restore_resume_and_abandon_report_daemon_errors_without_pending_intent() {
    let fx = fixture("restore-intent-errors", V1);

    let (ok, _out, err) = sheaf(&fx, &["restore", "--resume"]);
    assert!(!ok, "resume without intent unexpectedly succeeded");
    assert!(err.contains("pending") || err.contains("restore"), "{err}");

    let (ok, out, err) = sheaf(&fx, &["restore", "--abandon"]);
    assert!(ok, "abandon should be idempotent: {err}");
    assert!(out.contains("restore intent abandoned"), "{out}");
}

#[test]
fn live_worktree_edits_form_a_branch_that_squash_merges() {
    let fx = fixture("worktree-merge", V1);
    let (ok, _, err) = sheaf(&fx, &["checkpoint", "create", "branch-base"]);
    assert!(ok, "{err}");

    write_file(&fx.root, "src/target.rs", "pub fn target() {}\n");
    wait_caught_up(&fx);

    let linked = fx._env.base.join("branch");
    let (ok, out, err) = sheaf(
        &fx,
        &[
            "worktree",
            "add",
            "checkpoint:branch-base",
            linked.to_str().unwrap(),
            "--json",
        ],
    );
    assert!(ok, "{err}");
    let added = json_out(&out);
    assert_eq!(added["watching"], serde_json::json!(true));
    assert!(linked.join(".sheaf").is_file());
    assert!(!linked.join("src/target.rs").exists());

    write_file(&linked, "src/source.rs", "pub fn source() {}\n");
    wait_caught_up_at(&fx, &linked);
    let (ok, out, err) = sheaf_at(&fx, &linked, &["log", "--json"]);
    assert!(ok, "{err}");
    let source = json_out(&out)["entries"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();

    let (ok, out, err) = sheaf(&fx, &["worktree", "list", "--json"]);
    assert!(ok, "{err}");
    let listed = json_out(&out);
    let branch = listed
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["path"] == linked.to_string_lossy().as_ref())
        .expect("linked worktree listed");
    assert_eq!(branch["capture_id"], source);

    let (ok, out, err) = sheaf(&fx, &["merge", &source, "--json"]);
    assert!(ok, "{err}");
    let plan = json_out(&out);
    assert!(plan["conflicts"].as_array().unwrap().is_empty());
    assert_eq!(plan["actions"].as_array().unwrap().len(), 1);
    assert_eq!(plan["actions"][0]["path"], "src/source.rs");

    let (ok, out, err) = sheaf(&fx, &["merge", &source, "--apply", "--json"]);
    assert!(ok, "{err}");
    let outcome = json_out(&out);
    assert_eq!(outcome["files_written"], 1);
    assert!(outcome["capture_id"].is_string());
    assert_eq!(
        std::fs::read_to_string(fx.root.join("src/source.rs")).unwrap(),
        "pub fn source() {}\n"
    );
    assert_eq!(
        std::fs::read_to_string(fx.root.join("src/target.rs")).unwrap(),
        "pub fn target() {}\n"
    );
}

// --------------------------------------------------- worktree list / add (human)

/// Capture id at the tip of `root`'s timeline (the daemon's `log` view).
fn latest_capture(fx: &Fixture, root: &Path) -> String {
    let (ok, out, err) = sheaf_at(fx, root, &["log", "--json"]);
    assert!(ok, "{err}");
    json_out(&out)["entries"][0]["id"]
        .as_str()
        .expect("a capture at the tip")
        .to_owned()
}

#[test]
fn worktree_list_human_marks_primary_and_shows_linked_tips() {
    let fx = fixture("wt-list-human", V1);
    let (ok, _, err) = sheaf(&fx, &["checkpoint", "create", "base"]);
    assert!(ok, "{err}");

    let linked = fx._env.base.join("branch");
    // The human `worktree add` report names the destination and confirms the
    // daemon is watching the new physical worktree.
    let (ok, add_out, err) = sheaf(
        &fx,
        &[
            "worktree",
            "add",
            "checkpoint:base",
            linked.to_str().unwrap(),
        ],
    );
    assert!(ok, "{err}");
    assert!(
        add_out.starts_with(&format!("worktree {}", linked.display())),
        "add report names the worktree: {add_out}"
    );
    assert!(add_out.contains("watching: yes"), "{add_out}");

    let (ok, out, err) = sheaf(&fx, &["worktree", "list"]);
    assert!(ok, "{err}");
    let primary = out
        .lines()
        .find(|l| l.contains(fx.root.to_string_lossy().as_ref()))
        .expect("primary listed");
    assert!(primary.starts_with("* "), "primary marked `*`: {out}");
    let branch = out
        .lines()
        .find(|l| l.contains(linked.to_string_lossy().as_ref()))
        .expect("linked listed");
    assert!(branch.starts_with("  "), "linked left unmarked: {out}");
    assert!(!branch.contains("(missing)"), "present worktree: {out}");

    // Each human line shows the 12-char tip of that worktree, and it matches
    // the JSON view's full capture id.
    let (ok, jout, err) = sheaf(&fx, &["worktree", "list", "--json"]);
    assert!(ok, "{err}");
    let json = json_out(&jout);
    for item in json.as_array().unwrap() {
        let path = item["path"].as_str().unwrap();
        let tip = &item["capture_id"].as_str().unwrap()[..12];
        let line = out.lines().find(|l| l.contains(path)).expect("listed");
        assert!(line.contains(tip), "tip {tip} shown for {path}: {out}");
    }
}

#[test]
fn worktree_add_to_existing_destination_fails_clearly() {
    let fx = fixture("wt-add-clash", V1);
    let (ok, _, err) = sheaf(&fx, &["checkpoint", "create", "base"]);
    assert!(ok, "{err}");

    // A destination that already exists must be refused, untouched.
    let occupied = fx._env.base.join("occupied");
    std::fs::create_dir_all(&occupied).unwrap();
    std::fs::write(occupied.join("keep.txt"), b"mine").unwrap();

    let (ok, _, err) = sheaf(
        &fx,
        &[
            "worktree",
            "add",
            "checkpoint:base",
            occupied.to_str().unwrap(),
        ],
    );
    assert!(!ok, "add over an existing dir must fail");
    assert!(err.contains("already exists"), "{err}");
    // The pre-existing content is untouched and no `.sheaf` link was planted.
    assert_eq!(std::fs::read(occupied.join("keep.txt")).unwrap(), b"mine");
    assert!(!occupied.join(".sheaf").exists(), "no link created: {err}");
}

// ------------------------------------------------------------ merge (human)

#[test]
fn merge_preview_prints_plan_then_apply_writes_files() {
    let fx = fixture("merge-human", V1);
    let (ok, _, err) = sheaf(&fx, &["checkpoint", "create", "base"]);
    assert!(ok, "{err}");

    let linked = fx._env.base.join("branch");
    let (ok, _, err) = sheaf(
        &fx,
        &[
            "worktree",
            "add",
            "checkpoint:base",
            linked.to_str().unwrap(),
        ],
    );
    assert!(ok, "{err}");
    write_file(&linked, "src/added.rs", "pub fn added() {}\n");
    wait_caught_up_at(&fx, &linked);
    let source = latest_capture(&fx, &linked);

    // Preview: the plan text, then the copy-paste apply hint (no conflicts).
    let (ok, out, err) = sheaf(&fx, &["merge", &source]);
    assert!(ok, "{err}");
    assert!(out.contains("changes:      1"), "{out}");
    assert!(out.contains("conflicts:    0"), "{out}");
    assert!(out.contains("Create  src/added.rs"), "{out}");
    assert!(
        out.contains(&format!("apply:        sheaf merge {source} --apply")),
        "apply hint present: {out}"
    );

    // Apply: reports the write and stamps a merge capture.
    let (ok, out, err) = sheaf(&fx, &["merge", &source, "--apply"]);
    assert!(ok, "{err}");
    assert!(
        out.contains("merged 1 change(s): 1 written, 0 deleted"),
        "{out}"
    );
    assert!(out.contains("capture: "), "capture line: {out}");
    assert_eq!(
        std::fs::read_to_string(fx.root.join("src/added.rs")).unwrap(),
        "pub fn added() {}\n"
    );
}

#[test]
fn conflicting_merge_lists_conflicts_and_blocks_apply() {
    let fx = fixture("merge-conflict", V1);
    let (ok, _, err) = sheaf(&fx, &["checkpoint", "create", "base"]);
    assert!(ok, "{err}");

    let linked = fx._env.base.join("branch");
    let (ok, _, err) = sheaf(
        &fx,
        &[
            "worktree",
            "add",
            "checkpoint:base",
            linked.to_str().unwrap(),
        ],
    );
    assert!(ok, "{err}");

    // Both branches change src/lib.rs differently relative to the shared base.
    write_file(&linked, "src/lib.rs", V2);
    wait_caught_up_at(&fx, &linked);
    let source = latest_capture(&fx, &linked);
    write_file(&fx.root, "src/lib.rs", V3);
    wait_caught_up(&fx);

    // Preview lists the conflict and withholds the apply hint.
    let (ok, out, err) = sheaf(&fx, &["merge", &source]);
    assert!(ok, "{err}");
    assert!(out.contains("conflicts:    1"), "{out}");
    assert!(out.contains("conflict  src/lib.rs:"), "{out}");
    assert!(
        out.contains("apply:        blocked until conflicts are resolved"),
        "{out}"
    );
    assert!(!out.contains("--apply"), "no apply hint on conflict: {out}");

    // Forcing `--apply` still refuses, reporting the conflict on stderr, and
    // the target keeps its own version.
    let (ok, _, err) = sheaf(&fx, &["merge", &source, "--apply"]);
    assert!(!ok, "conflicting apply must be blocked");
    assert!(err.contains("blocked by 1 conflict"), "{err}");
    assert_eq!(
        std::fs::read_to_string(fx.root.join("src/lib.rs")).unwrap(),
        V3
    );

    // The `--json` apply path emits the machine-readable plan (conflicts and
    // all) and still exits non-zero without touching the worktree.
    let (ok, out, _err) = sheaf(&fx, &["merge", &source, "--apply", "--json"]);
    assert!(!ok, "json apply on conflict must also refuse");
    assert_eq!(json_out(&out)["conflicts"].as_array().unwrap().len(), 1);
    assert_eq!(
        std::fs::read_to_string(fx.root.join("src/lib.rs")).unwrap(),
        V3
    );
}

#[test]
fn merge_resume_without_intent_errors_cleanly() {
    let fx = fixture("merge-resume-empty", V1);
    let (ok, _, err) = sheaf(&fx, &["merge", "--resume"]);
    assert!(!ok, "resume without a pending merge must fail");
    assert!(
        err.contains("no pending merge intent") || err.contains("merge"),
        "{err}"
    );
}

#[test]
fn worktree_add_resolves_relative_destination_against_cwd() {
    let fx = fixture("wt-add-rel", V1);
    let (ok, _, err) = sheaf(&fx, &["checkpoint", "create", "base"]);
    assert!(ok, "{err}");

    // Run from `base` with a relative destination and an explicit `-C`
    // project; the CLI resolves the destination against the current
    // directory, not the project root.
    let base = fx._env.base.clone();
    let (ok, out, err) = sheaf_at(
        &fx,
        &base,
        &[
            "worktree",
            "add",
            "-C",
            fx.root.to_str().unwrap(),
            "checkpoint:base",
            "reldir",
            "--json",
        ],
    );
    assert!(ok, "{err}");
    let created = base.join("reldir");
    assert_eq!(
        json_out(&out)["worktree"]["path"],
        created.to_string_lossy().as_ref()
    );
    assert!(
        created.join(".sheaf").is_file(),
        "link planted at cwd-relative dest"
    );
}

#[test]
fn merge_with_unknown_source_reports_daemon_error() {
    let fx = fixture("merge-bad-source", V1);
    let (ok, _, err) = sheaf(&fx, &["merge", "no-such-reference"]);
    assert!(!ok, "merge against an unknown source must fail");
    assert!(!err.is_empty(), "a diagnostic is printed: {err}");

    // No source and no `--resume` is a usage error, not a daemon round-trip.
    let (ok, _, err) = sheaf(&fx, &["merge"]);
    assert!(!ok, "bare merge must fail");
    assert!(
        err.contains("needs a source reference or `--resume`"),
        "{err}"
    );
}
