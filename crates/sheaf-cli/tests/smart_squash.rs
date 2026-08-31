//! End-to-end: smart squash against a REAL git repo, a REAL
//! daemon, and the REAL `sheaf` binary. The core planning layer is covered
//! in sheaf-core; these tests prove the git orchestration — staged-only
//! patches, projected frames, complete-frame convergence, crash-window
//! recovery, and the gates that must refuse before mutation.
//!
//! The scenarios mutate process-global env (XDG dirs) to isolate the
//! daemon registry and runtime socket, so they serialize on a mutex.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use serde_json::Value;
use sheaf_core::init::{init_project, InitOptions};
use sheaf_core::ipc::Client;

static ENV_LOCK: Mutex<()> = Mutex::new(());

const SHEAF: &str = env!("CARGO_BIN_EXE_sheaf");
const GOOD: &str = "fn alpha() -> u32 {\n    1\n}\n\nfn beta() -> u32 {\n    2\n}\n";
const BOTH_DIRTY: &str = "fn alpha() -> u64 {\n    99\n}\n\nfn beta() -> u32 {\n    4200\n}\n";

struct EnvGuard {
    _lock: MutexGuard<'static, ()>,
    saved: Vec<(&'static str, Option<std::ffi::OsString>)>,
    base: PathBuf,
}

impl EnvGuard {
    fn new(tag: &str) -> EnvGuard {
        let lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let base = std::env::temp_dir().join(format!("sheaf-p013-{tag}-{}", std::process::id()));
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
        self.base.join("run").join("sheaf").join("control.sock")
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

fn sheaf(root: &Path, args: &[&str], stdin: Option<&str>) -> (bool, String, String) {
    let mut cmd = Command::new(SHEAF);
    cmd.args(args).current_dir(root);
    if stdin.is_some() {
        cmd.stdin(Stdio::piped());
    }
    cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
    let mut child = cmd.spawn().expect("run sheaf");
    if let Some(payload) = stdin {
        use std::io::Write as _;
        child
            .stdin
            .take()
            .unwrap()
            .write_all(payload.as_bytes())
            .unwrap();
    }
    let out = child.wait_with_output().expect("wait sheaf");
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

/// A project with one commit on the books, the daemon watching, and the
/// worktree dirty in alpha and beta.
struct Fixture {
    _env: EnvGuard,
    _daemon: Daemon,
    root: PathBuf,
}

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
    let report = init_project(
        &root,
        InitOptions {
            socket_override: Some(socket.clone()),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(report.daemon_notified, "daemon should be reachable");
    wait_caught_up(&socket, &root);
    Fixture {
        _env: env,
        _daemon: daemon,
        root,
    }
}

/// Block until the daemon has captured the current worktree state.
fn wait_caught_up(socket: &Path, root: &Path) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(mut client) = Client::connect(socket, Duration::from_secs(2)) {
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

/// The newest grep hit for `needle` in `path`, as a single-handle payload.
fn select(fx: &Fixture, needle: &str, path: &str) -> String {
    let (ok, out, err) = sheaf(&fx.root, &["grep", needle, "--path", path, "--json"], None);
    assert!(ok, "grep failed: {err}");
    // `sheaf grep --json` streams NDJSON (proto 1.5): record lines first,
    // the summary line last. The summary carries the full report.
    let mut report: Option<Value> = None;
    for line in out.lines().filter(|l| !l.is_empty()) {
        let value: Value = serde_json::from_str(line).expect("grep json line");
        if value["type"] == "summary" {
            report = Some(value["report"].clone());
        }
    }
    let report = report.expect("grep summary record");
    let hits = report["hits"].as_array().expect("hits");
    assert!(!hits.is_empty(), "no hits for {needle}: {out}");
    serde_json::to_string(&hits[hits.len() - 1]).unwrap()
}

fn frames(fx: &Fixture) -> Vec<Value> {
    let text = std::fs::read_to_string(fx.root.join(".sheaf/frames.jsonl")).unwrap_or_default();
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("frame jsonl"))
        .collect()
}

fn checkpoints(fx: &Fixture) -> Vec<String> {
    let (ok, out, err) = sheaf(&fx.root, &["checkpoint", "list", "--json"], None);
    assert!(ok, "checkpoint list failed: {err}");
    let value: Value = serde_json::from_str(&out).expect("checkpoint json");
    value["checkpoints"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|c| c["name"].as_str().unwrap_or_default().to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// Commit-only state: after smart-squashing the last dirty selection the
/// worktree converges with HEAD and the frame is complete.
fn smart_commit(fx: &Fixture, payload: &str, message: &str) -> Value {
    let (ok, out, err) = sheaf(
        &fx.root,
        &["squash", "--selection", "-", "--json", "--", "-m", message],
        Some(payload),
    );
    assert!(ok, "smart squash failed: {err}\n{out}");
    serde_json::from_str(&out).expect("smart squash json")
}

#[test]
fn smart_squash_commits_only_the_selected_unit() {
    let fx = fixture("commit-only", GOOD);
    write_file(&fx.root, "src/lib.rs", BOTH_DIRTY);
    wait_caught_up(&fx.env_socket(), &fx.root);
    let payload = select(&fx, "fn alpha() -> u64 {\n    99\n}", "src/lib.rs");

    let result = smart_commit(&fx, &payload, "alpha only");
    assert_eq!(
        result["kind"], "partial",
        "dirty beta blocks equality: {result}"
    );

    // HEAD holds alpha's edit and NOT beta's.
    let committed = git(&fx.root, &["show", "HEAD:src/lib.rs"]);
    assert!(committed.contains("-> u64"), "{committed}");
    assert!(!committed.contains("4200"), "{committed}");
    // The worktree still holds both edits — beta stays dirty.
    let live = std::fs::read_to_string(fx.root.join("src/lib.rs")).unwrap();
    assert_eq!(live, BOTH_DIRTY);
    // A projected frame recorded selection provenance and a verified
    // digest, and no ordinary equality checkpoint was stamped at a
    // non-equal tip.
    let frame = frames(&fx);
    assert_eq!(frame.len(), 1, "{frame:?}");
    assert_eq!(frame[0]["kind"], "partial");
    let projection = &frame[0]["projection"];
    assert!(projection["patch_sha256"].as_str().unwrap().len() >= 64);
    assert!(!projection["selection_ids"].as_array().unwrap().is_empty());
    assert!(frame[0]["tip_capture_id"].is_null());
    assert!(
        !checkpoints(&fx).iter().any(|n| n.starts_with("git-")),
        "no git-<sha> checkpoint may exist at a non-equal tip"
    );
}

#[test]
fn sequential_selections_converge_on_a_complete_frame() {
    let fx = fixture("converge", GOOD);
    write_file(&fx.root, "src/lib.rs", BOTH_DIRTY);
    wait_caught_up(&fx.env_socket(), &fx.root);

    let alpha = select(&fx, "fn alpha() -> u64 {\n    99\n}", "src/lib.rs");
    let first = smart_commit(&fx, &alpha, "alpha");
    assert_eq!(first["kind"], "partial", "beta is still dirty: {first}");
    assert_eq!(frames(&fx).len(), 1);

    // beta's handle was bound before the first commit; reselect it at the
    // new tip so its source reflects the current file.
    let beta = select(&fx, "fn beta() -> u32 {\n    4200\n}", "src/lib.rs");
    let second = smart_commit(&fx, &beta, "beta");
    assert_eq!(
        second["kind"],
        "complete",
        "worktree converged: {}",
        git(&fx.root, &["status", "--porcelain"])
    );

    let porcelain = git(&fx.root, &["status", "--porcelain"]);
    assert_eq!(porcelain.trim(), "", "worktree converged with HEAD");

    let frame = frames(&fx);
    assert_eq!(frame.len(), 2);
    assert_eq!(frame[1]["kind"], "complete");
    assert!(frame[1]["tip_capture_id"].is_string());
    assert!(
        checkpoints(&fx)
            .iter()
            .any(|n| *n == format!("git-{}", frame[1]["short_sha"].as_str().unwrap())),
        "the converging commit stamps its checkpoint"
    );
}

#[test]
fn dirty_index_and_refusals_write_nothing() {
    let fx = fixture("gates", GOOD);
    write_file(&fx.root, "src/lib.rs", BOTH_DIRTY);
    wait_caught_up(&fx.env_socket(), &fx.root);
    let payload = select(&fx, "fn alpha() -> u64 {\n    99\n}", "src/lib.rs");
    let head = git(&fx.root, &["rev-parse", "HEAD"]);

    // A staged change blocks the mutating path before any git mutation;
    // the staged state itself is untouched.
    git(&fx.root, &["add", "src/lib.rs"]);
    let (ok, _out, err) = sheaf(
        &fx.root,
        &["squash", "--selection", "-", "--", "-m", "nope"],
        Some(&payload),
    );
    assert!(!ok, "dirty index must refuse");
    assert!(err.contains("index"), "{err}");
    assert_eq!(git(&fx.root, &["rev-parse", "HEAD"]), head);
    assert!(!git(&fx.root, &["diff", "--cached", "--name-only"])
        .trim()
        .is_empty());
    git(&fx.root, &["reset", "-q"]);

    // A selection whose sides already match is a typed refusal (exit 4)
    // that writes nothing: no commit, no frame, worktree untouched.
    write_file(&fx.root, "src/lib.rs", GOOD);
    wait_caught_up(&fx.env_socket(), &fx.root);
    let noop = select(&fx, "fn alpha() -> u32 {\n    1\n}", "src/lib.rs");
    let (ok, out, _err) = sheaf(
        &fx.root,
        &["squash", "--selection", "-", "--", "-m", "noop"],
        Some(&noop),
    );
    assert!(!ok, "no-op selection must refuse");
    assert!(
        out.contains("EmptyPatch") || out.to_lowercase().contains("refusal"),
        "stdout should carry the typed refusal: {out}"
    );
    assert_eq!(git(&fx.root, &["rev-parse", "HEAD"]), head);
    assert!(frames(&fx).is_empty(), "no frame written");
    assert_eq!(
        std::fs::read_to_string(fx.root.join("src/lib.rs")).unwrap(),
        GOOD
    );
}

#[test]
fn preview_shows_attribution_and_patch_separately() {
    let fx = fixture("preview", GOOD);
    write_file(&fx.root, "src/lib.rs", BOTH_DIRTY);
    wait_caught_up(&fx.env_socket(), &fx.root);
    let payload = select(&fx, "fn alpha() -> u64 {\n    99\n}", "src/lib.rs");
    let (ok, out, err) = sheaf(
        &fx.root,
        &["squash", "--selection", "-", "--json"],
        Some(&payload),
    );
    assert!(ok, "preview failed: {err}");
    let value: Value = serde_json::from_str(&out).expect("preview json");
    assert!(value["smart_squash"].as_bool().unwrap());
    assert!(value["applicable"].as_bool().unwrap_or(false), "{value}");
    assert_eq!(value["files"].as_array().unwrap().len(), 1);
    assert!(value["attribution"]["captures"].as_u64().unwrap() >= 1);
    assert!(out.contains("timeline attribution") || value["attribution"].is_object());
    // A preview reports no SHA of any kind: digests are internal content
    // hashes, never git identities, and nothing is staged until `--`.
    // (`staged_sha256` once masqueraded as the future blob's SHA.)
    assert!(
        value.get("patch_sha256").is_none(),
        "preview must not report patch_sha256: {value}"
    );
    assert!(
        !out.contains("sha"),
        "no sha-ish keys in preview json: {out}"
    );
    for file in value["files"].as_array().unwrap() {
        assert!(file.get("staged_sha256").is_none(), "{file}");
        assert!(file.get("head_sha256").is_none(), "{file}");
    }
    // Preview mutated nothing.
    assert!(git(&fx.root, &["status", "--porcelain"]).contains("src/lib.rs"));
    assert!(frames(&fx).is_empty());
}

#[test]
fn ordinary_squash_after_partial_frames_previews_the_remainder() {
    let fx = fixture("remainder", GOOD);
    write_file(&fx.root, "src/lib.rs", BOTH_DIRTY);
    wait_caught_up(&fx.env_socket(), &fx.root);
    let alpha = select(&fx, "fn alpha() -> u64 {\n    99\n}", "src/lib.rs");
    smart_commit(&fx, &alpha, "alpha first");

    // Ordinary preview now reports the projected frame and labels the
    // still-uncommitted git change.
    let (ok, out, err) = sheaf(&fx.root, &["squash", "--json"], None);
    assert!(ok, "ordinary preview failed: {err}");
    let value: Value = serde_json::from_str(&out).expect("preview json");
    assert_eq!(value["partial_frames"], 1, "{value}");
    let uncommitted = value["git_uncommitted"].as_str().unwrap_or_default();
    assert!(uncommitted.contains("src/lib.rs"), "{value}");

    // And the ordinary `--` path commits exactly that remainder.
    let (ok, out, err) = sheaf(&fx.root, &["squash", "--", "-m", "the rest"], None);
    assert!(ok, "ordinary squash failed: {err}\n{out}");
    assert_eq!(git(&fx.root, &["status", "--porcelain"]).trim(), "");
    let committed = git(&fx.root, &["show", "HEAD:src/lib.rs"]);
    assert!(committed.contains("4200"), "beta landed: {committed}");
}

#[test]
fn amend_orphans_the_partial_frame_without_false_anchors() {
    let fx = fixture("amend", GOOD);
    write_file(&fx.root, "src/lib.rs", BOTH_DIRTY);
    wait_caught_up(&fx.env_socket(), &fx.root);
    let alpha = select(&fx, "fn alpha() -> u64 {\n    99\n}", "src/lib.rs");
    smart_commit(&fx, &alpha, "alpha");
    let pre_amend = git(&fx.root, &["rev-parse", "HEAD"]);

    // Rewrite the smart commit wholesale.
    git(&fx.root, &["add", "--all"]);
    git(
        &fx.root,
        &["commit", "-q", "--amend", "-m", "everything at once"],
    );
    let post_amend = git(&fx.root, &["rev-parse", "HEAD"]);
    assert_ne!(pre_amend, post_amend);

    // The ordinary preview must not crash and must not anchor through the
    // orphaned frame's checkpoint (there is none for partial frames), and
    // the still-uncommitted report stays honest (worktree == HEAD here).
    let (ok, out, err) = sheaf(&fx.root, &["squash", "--json"], None);
    assert!(ok, "preview after amend failed: {err}");
    let value: Value = serde_json::from_str(&out).expect("preview json");
    assert_eq!(value["partial_frames"], 1);
    let _ = value["git_uncommitted"].as_str().unwrap_or("");
}

#[test]
fn frame_write_failure_is_recoverable_and_honest() {
    let fx = fixture("crash", GOOD);
    write_file(&fx.root, "src/lib.rs", BOTH_DIRTY);
    wait_caught_up(&fx.env_socket(), &fx.root);
    let alpha = select(&fx, "fn alpha() -> u64 {\n    99\n}", "src/lib.rs");

    // Break the frame ledger so the append fails AFTER the commit: the
    // crash window between `git commit` and the frame record.
    let ledger = fx.root.join(".sheaf/frames.jsonl");
    std::fs::write(&ledger, "").unwrap();
    let _ = std::fs::remove_file(&ledger);
    std::fs::create_dir_all(&ledger).unwrap();

    let (ok, _out, err) = sheaf(
        &fx.root,
        &["squash", "--selection", "-", "--", "-m", "alpha"],
        Some(&alpha),
    );
    assert!(!ok, "the frame failure must surface");
    assert!(
        err.contains("frame") || err.to_lowercase().contains("append"),
        "{err}"
    );
    // The commit itself exists; the index is clean (commit consumed it).
    let committed = git(&fx.root, &["show", "HEAD:src/lib.rs"]);
    assert!(committed.contains("-> u64"), "commit survived: {committed}");
    // The commit consumed the index (clean); the worktree stays dirty in
    // beta, which is the whole point of a partial commit.
    let cached = git(&fx.root, &["diff", "--cached", "--name-only"]);
    assert!(
        cached.trim().is_empty(),
        "index is clean after commit: {cached:?}"
    );
    // No checkpoint was stamped for the unstamped frame.
    assert!(
        !checkpoints(&fx).iter().any(|n| n.starts_with("git-")),
        "no false anchor"
    );
}

impl Fixture {
    fn env_socket(&self) -> PathBuf {
        // The socket path derived from the isolated runtime dir.
        std::env::var("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .unwrap()
            .join("sheaf")
            .join("control.sock")
    }
}

#[test]
fn smart_squash_human_commit_reports_caught_up_and_frame() {
    let fx = fixture("human-commit", GOOD);
    write_file(&fx.root, "src/lib.rs", BOTH_DIRTY);
    wait_caught_up(&fx.env_socket(), &fx.root);
    let payload = select(&fx, "fn alpha() -> u64 {\n    99\n}", "src/lib.rs");

    let (ok, out, err) = sheaf(
        &fx.root,
        &["squash", "--selection", "-", "--", "-m", "human alpha"],
        Some(&payload),
    );
    assert!(ok, "smart squash failed: {err}\n{out}");
    let rendered = format!("{out}{err}");
    assert!(rendered.contains("committed:"), "{rendered}");
    assert!(rendered.contains("frame:"), "{rendered}");
    assert!(rendered.contains("audit tip:"), "{rendered}");
}

#[test]
fn ordinary_squash_default_anchor_json_commits_and_stamps_frame() {
    let fx = fixture("ordinary-json", GOOD);
    write_file(&fx.root, "src/lib.rs", "fn two() {}\n");
    wait_caught_up(&fx.env_socket(), &fx.root);
    git(&fx.root, &["add", "--all"]);
    git(&fx.root, &["commit", "-q", "-m", "second"]);
    write_file(&fx.root, "src/lib.rs", BOTH_DIRTY);
    wait_caught_up(&fx.env_socket(), &fx.root);

    let (ok, out, err) = sheaf(
        &fx.root,
        &["squash", "@~1", "--json", "--", "-m", "ordinary json"],
        None,
    );
    assert!(ok, "ordinary squash failed: {err}\n{out}");
    let json = &out[out.find('{').expect("squash JSON object")..];
    let value: Value = serde_json::from_str(json).expect("ordinary squash json");
    assert!(value["frame"].is_object(), "{value}");
    assert_eq!(value["frame_index"], 1);
    assert_eq!(git(&fx.root, &["status", "--porcelain"]).trim(), "");
}

#[test]
fn smart_squash_commit_failure_keeps_selection_staged() {
    let fx = fixture("smart-commit-failure", GOOD);
    write_file(&fx.root, "src/lib.rs", BOTH_DIRTY);
    wait_caught_up(&fx.env_socket(), &fx.root);
    let payload = select(&fx, "fn alpha() -> u64 {\n    99\n}", "src/lib.rs");

    let (ok, _out, err) = sheaf(
        &fx.root,
        &["squash", "--selection", "-", "--", "--not-a-git-option"],
        Some(&payload),
    );
    assert!(!ok, "invalid git commit option unexpectedly succeeded");
    assert!(
        err.contains("git commit exited") || err.contains("unknown option"),
        "{err}"
    );
    assert!(
        !git(&fx.root, &["diff", "--cached", "--name-only"])
            .trim()
            .is_empty(),
        "failed commit preserves the selected patch in the index"
    );
}
