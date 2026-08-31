//! Crash durability, streamed plans, and idle budgets against a
//! REAL daemon process.
//!
//! The kill -9 matrix is the phase's core promise: whatever the daemon was
//! doing when SIGKILL lands, the store it left behind must load, keep every
//! capture that crossed the fsync line, and come back serving after a
//! restart. These tests spawn `sheafd` as a child process and kill it for
//! real — no crash simulation shortcuts.
//!
//! The scenarios mutate process-global env (XDG dirs) to isolate the
//! daemon's registry and runtime socket, so they serialize on a mutex.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::Value;
use sheaf_core::config;
use sheaf_core::init::{init_project, InitOptions};
use sheaf_core::ipc::Client;
use sheaf_core::store::{ProjectStore, StoreLimits};

static ENV_LOCK: Mutex<()> = Mutex::new(());

const DAEMON: &str = env!("CARGO_BIN_EXE_sheafd");

struct EnvGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
    saved: Vec<(&'static str, Option<std::ffi::OsString>)>,
    base: PathBuf,
}

impl EnvGuard {
    fn new(tag: &str) -> EnvGuard {
        let lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let base = std::env::temp_dir().join(format!("sheaf-p006-{tag}-{}", std::process::id()));
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

    fn project(&self) -> PathBuf {
        let p = self.base.join("proj");
        std::fs::create_dir_all(&p).unwrap();
        p
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
    socket: PathBuf,
}

impl Daemon {
    fn spawn(socket: PathBuf) -> Daemon {
        let child = Command::new(DAEMON)
            .args(["run", "--socket"])
            .arg(&socket)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn sheafd");
        let d = Daemon { child, socket };
        d.wait_ping(10_000);
        d
    }

    fn wait_ping(&self, timeout_ms: u64) {
        let deadline = Instant::now() + Duration::from_millis(timeout_ms);
        loop {
            if Instant::now() > deadline {
                panic!("daemon never answered ping within {timeout_ms} ms");
            }
            if let Ok(mut c) = Client::connect(&self.socket, Duration::from_millis(400)) {
                if c.ping().is_ok() {
                    return;
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// SIGKILL, not SIGTERM: the whole point is no cleanup, no flush, no
    /// socket removal — exactly what a machine crash leaves behind.
    fn kill9(&mut self) {
        unsafe {
            libc::kill(self.child.id() as i32, libc::SIGKILL);
        }
        self.child.wait().unwrap();
    }

    fn shutdown(mut self) {
        if let Ok(mut c) = Client::connect(&self.socket, Duration::from_secs(2)) {
            let _ = call(&mut c, "shutdown", None, serde_json::json!({}));
        }
        let deadline = Instant::now() + Duration::from_secs(15);
        while Instant::now() < deadline {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => std::thread::sleep(Duration::from_millis(100)),
                Err(_) => return,
            }
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn call(client: &mut Client, method: &str, project: Option<&Path>, params: Value) -> Value {
    let reply = client
        .call(method, project, params, None)
        .unwrap_or_else(|e| panic!("{method} transport failed: {e}"));
    assert!(
        reply.response.ok,
        "{method} failed: {:?}",
        reply.response.error
    );
    let mut result = reply.response.result.unwrap_or(Value::Null);
    if !reply.body.is_empty() {
        // `call` reassembles body chunks; surface the payload for plans.
        result["__body"] = serde_json::from_slice(&reply.body).unwrap_or(Value::Null);
    }
    result
}

fn enroll(socket: &Path, root: &Path) {
    // The daemon is live; init registers in OUR isolated registry (env is
    // already set) and notifies this socket.
    let report = init_project(
        root,
        InitOptions {
            socket_override: Some(socket.to_path_buf()),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(
        report.daemon_notified,
        "daemon should be reachable at {socket:?}"
    );
}

fn write_file(root: &Path, rel: &str, contents: String) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, contents).unwrap();
}

fn capture_count(socket: &Path, root: &Path) -> usize {
    let mut c = Client::connect(socket, Duration::from_secs(2)).unwrap();
    let reply = c
        .call(
            "timeline.log",
            Some(root),
            serde_json::json!({"limit": 1000}),
            None,
        )
        .unwrap();
    if !reply.response.ok
        && reply
            .response
            .error
            .as_ref()
            .map(|error| error.code.as_str())
            == Some("project.warming")
    {
        return 0;
    }
    assert!(
        reply.response.ok,
        "timeline.log failed: {:?}",
        reply.response.error
    );
    let result = reply.response.result.unwrap_or(Value::Null);
    result
        .get("entries")
        .and_then(Value::as_array)
        .map(|a| a.len())
        .unwrap_or(0)
}

fn wait_ready(socket: &Path, root: &Path, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let mut client = Client::connect(socket, Duration::from_secs(2)).unwrap();
        let status = call(
            &mut client,
            "project.status",
            Some(root),
            serde_json::json!({}),
        );
        if status.get("ready").and_then(Value::as_bool) == Some(true) {
            return;
        }
        assert!(Instant::now() <= deadline, "project never became ready");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn wait_for_captures(socket: &Path, root: &Path, want: usize, timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    loop {
        let n = capture_count(socket, root);
        if n >= want {
            return n;
        }
        if Instant::now() > deadline {
            panic!("only {n} captures after {:?}", timeout);
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

#[test]
fn enrollment_honors_the_configured_text_budget() {
    let guard = EnvGuard::new("text-budget");
    let socket = guard.socket();
    let root = guard.project();
    config::write_skeleton(&root).unwrap();
    let config_path = config::config_file_path(&root);
    let raw = std::fs::read_to_string(&config_path).unwrap();
    assert!(raw.contains("max_tracked_bytes = 33554432"));
    std::fs::write(
        &config_path,
        raw.replace("max_tracked_bytes = 33554432", "max_tracked_bytes = 8"),
    )
    .unwrap();
    std::fs::write(root.join("a.txt"), b"aaaaaaaa").unwrap();
    std::fs::write(root.join("b.txt"), b"bbbbbbbb").unwrap();
    std::fs::create_dir(root.join("bulk")).unwrap();
    for i in 0..2000 {
        std::fs::write(root.join(format!("bulk/{i:04}.txt")), b"").unwrap();
    }

    let daemon = Daemon::spawn(socket.clone());
    let started = Instant::now();
    enroll(&socket, &root);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "enrollment should acknowledge before background reconciliation completes"
    );

    // Commands issued during warm-up must fail immediately and must never be
    // queued to execute after the caller has timed out.
    let mut client = Client::connect(&socket, Duration::from_secs(2)).unwrap();
    let status = call(
        &mut client,
        "project.status",
        Some(&root),
        serde_json::json!({}),
    );
    assert_eq!(status["ready"], false);
    let t0 = Instant::now();
    let reply = client
        .call(
            "checkpoint.create",
            Some(&root),
            serde_json::json!({"name": "must-not-land"}),
            None,
        )
        .unwrap();
    assert!(!reply.response.ok);
    assert_eq!(
        reply
            .response
            .error
            .as_ref()
            .map(|error| error.code.as_str()),
        Some("project.warming")
    );
    assert!(t0.elapsed() < Duration::from_secs(1));
    drop(client);

    wait_ready(&socket, &root, Duration::from_secs(15));
    wait_for_captures(&socket, &root, 8, Duration::from_secs(15));
    let mut client = Client::connect(&socket, Duration::from_secs(2)).unwrap();
    let checkpoints = call(
        &mut client,
        "checkpoint.list",
        Some(&root),
        serde_json::json!({}),
    );
    assert_eq!(checkpoints["checkpoints"].as_array().unwrap().len(), 0);
    daemon.shutdown();

    let reopened = ProjectStore::open_with_text_budget(&root, StoreLimits::default(), 8).unwrap();
    assert_eq!(reopened.tracked_text_bytes(), 8);
}

#[test]
fn kill9_matrix_committed_captures_survive_and_the_daemon_returns() {
    let guard = EnvGuard::new("matrix");
    let socket = guard.socket();
    let root = guard.project();
    let mut daemon = Daemon::spawn(socket.clone());
    enroll(&socket, &root);

    // Phase A: idle kill — nothing in flight, store must simply exist.
    write_file(&root, "a.txt", "hello\n".into());
    wait_for_captures(&socket, &root, 1, Duration::from_secs(15));
    daemon.kill9();

    // Phase B: restart recovers, committed capture is intact.
    let mut daemon = Daemon::spawn(socket.clone());
    assert_eq!(
        capture_count(&socket, &root),
        1,
        "the capture that crossed the fsync line must survive SIGKILL"
    );

    // Phase C: burst kill — write a rapid series and kill mid-window; at
    // least the earlier committed captures survive, the store still loads,
    // and no partial capture ever appears.
    for i in 0..30 {
        write_file(&root, &format!("burst/f{i}.txt"), format!("payload {i}\n"));
        std::thread::sleep(Duration::from_millis(15));
    }
    // Let the debouncer release at least one batch, then kill during the
    // next one's flight.
    let before = wait_for_captures(&socket, &root, 2, Duration::from_secs(15));
    std::thread::sleep(Duration::from_millis(80));
    daemon.kill9();

    // Phase D: second restart — everything committed is exactly preserved.
    let daemon2 = Daemon::spawn(socket.clone());
    let after_settled = {
        // give the boot reconciliation a moment; it may ADD a capture for
        // the in-flight burst, but never remove or rewrite history
        std::thread::sleep(Duration::from_secs(1));
        capture_count(&socket, &root)
    };
    assert!(
        after_settled >= before,
        "restart may add the reconciled tail but must never lose captures ({before} -> {after_settled})"
    );

    daemon2.shutdown();

    // The store passes its own integrity check after all of that (offline:
    // the running daemon holds the writer flock for the project's lifetime).
    let _guard = sheaf_core::store::try_lock_shared(&root.join(".sheaf/lock"))
        .unwrap()
        .expect("no writer holds the lock once the daemon stopped");
    let report = sheaf_core::store::doctor(&root).unwrap();
    assert!(
        report
            .checks
            .iter()
            .filter(|c| c.name != "config")
            .all(|c| c.ok),
        "post-kill integrity: {:?}",
        report.checks.iter().filter(|c| !c.ok).collect::<Vec<_>>()
    );
}

#[test]
fn oversized_plan_streams_through_body_chunks_and_applies() {
    let guard = EnvGuard::new("stream");
    let socket = guard.socket();
    let root = guard.project();
    let daemon = Daemon::spawn(socket.clone());
    enroll(&socket, &root);

    // A base capture, then ~12k files: the plan against the base point
    // holds ~12k deletes — past the 1 MiB envelope, which is the point.
    write_file(&root, "keep.txt", "base\n".into());
    wait_for_captures(&socket, &root, 1, Duration::from_secs(15));
    let mut c = Client::connect(&socket, Duration::from_secs(5)).unwrap();
    let base = call(
        &mut c,
        "timeline.log",
        Some(&root),
        serde_json::json!({"limit": 1}),
    );
    let base_id = base["entries"][0]["id"].as_str().unwrap().to_owned();
    drop(c);

    for i in 0..12000u32 {
        write_file(
            &root,
            &format!("many/f{i:05}.txt"),
            format!("content {i}\n"),
        );
    }
    wait_for_captures(&socket, &root, 2, Duration::from_secs(60));

    let mut c = Client::connect(&socket, Duration::from_secs(10)).unwrap();
    c.set_timeout(Duration::from_secs(120)).unwrap();
    let result = call(
        &mut c,
        "restore.plan",
        Some(&root),
        serde_json::json!({"at": base_id}),
    );

    // The envelope carries only the summary; the plan itself rides body
    // chunks reassembled by `call` into result["__body"].
    let summary = result
        .get("plan_summary")
        .expect("envelope carries a summary");
    assert_eq!(
        summary["actions_total"].as_u64().unwrap(),
        12000,
        "12000 created files to delete; keep.txt is unchanged"
    );
    let body = result.get("__body").expect("plan streamed as body chunks");
    assert!(
        serde_json::to_vec(body).unwrap().len() > 1024 * 1024,
        "this plan must exceed the envelope to prove the point"
    );
    assert_eq!(body["actions"].as_array().unwrap().len(), 12000);

    // And the token from the summary still applies (plan identity survives
    // the streaming).
    let token = summary["token"].as_str().unwrap().to_owned();
    let applied = call(
        &mut c,
        "restore.apply",
        Some(&root),
        serde_json::json!({"token": token}),
    );
    assert_eq!(
        applied["outcome"]["files_deleted"].as_u64().unwrap(),
        12000,
        "the restore removes exactly the 12000 files"
    );
    daemon.shutdown();
}

#[test]
fn idle_daemon_stays_quiet_no_busy_loop() {
    let guard = EnvGuard::new("idle");
    let socket = guard.socket();
    let root = guard.project();
    let daemon = Daemon::spawn(socket.clone());
    enroll(&socket, &root);
    write_file(&root, "seed.txt", "seed\n".into());
    wait_for_captures(&socket, &root, 1, Duration::from_secs(15));

    // Give every thread a chance to reach its blocking wait.
    std::thread::sleep(Duration::from_millis(1500));

    let pid = daemon.child.id() as i32;
    let switches_at = |tag: &str| -> i64 {
        let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
        let vol = status
            .lines()
            .find_map(|l| {
                l.strip_prefix("voluntary_ctxt_switches:")
                    .map(|v| v.trim().parse::<i64>().unwrap())
            })
            .unwrap();
        let nonvol = status
            .lines()
            .find_map(|l| {
                l.strip_prefix("nonvoluntary_ctxt_switches:")
                    .map(|v| v.trim().parse::<i64>().unwrap())
            })
            .unwrap();
        tracing::debug!(tag, vol, nonvol, "ctx switches");
        vol + nonvol
    };
    let t0 = switches_at("t0");
    std::thread::sleep(Duration::from_millis(2500));
    let t1 = switches_at("t1");
    let grew = t1 - t0;
    assert!(
        grew < 30,
        "an idle daemon burned {grew} context switches in 2.5 s — that is a busy loop, \
         not a parked poll()"
    );
    daemon.shutdown();
}

/// `timeline.grep` is served over IPC, advertised as a capability,
/// and its NDJSON body count matches the envelope summary; an incomplete run
/// returns a cursor that resumes to the same complete result.
/// Fragment restore over the wire — capability advertisement, a
/// typed conflict for a deleted unit, insert-mode planning against the
/// deletion scar, token-gated apply, and selection provenance on the
/// forward capture.
#[test]
fn fragment_restore_plans_applies_and_reports_provenance_over_ipc() {
    let guard = EnvGuard::new("fragment");
    let socket = guard.socket();
    let root = guard.project();
    let daemon = Daemon::spawn(socket.clone());
    enroll(&socket, &root);

    // v1 holds the unit; v2 deletes exactly its bytes, leaving the scar.
    write_file(&root, "lib.rs", "fn probe() { 1 }\nfn other() {}\n".into());
    wait_for_captures(&socket, &root, 1, Duration::from_secs(15));
    let v1 = capture_count(&socket, &root);
    write_file(&root, "lib.rs", "\nfn other() {}\n".into());
    wait_for_captures(&socket, &root, v1 + 1, Duration::from_secs(15));

    let mut client = Client::connect(&socket, Duration::from_secs(2)).unwrap();
    let pong = call(&mut client, "ping", None, serde_json::json!({}));
    let caps = pong
        .get("capabilities")
        .and_then(Value::as_array)
        .expect("capabilities array");
    for cap in ["fragment.plan", "fragment.apply"] {
        assert!(
            caps.iter().any(|c| c.as_str() == Some(cap)),
            "capability `{cap}` advertised"
        );
    }

    // The handle comes from the real grep surface: parse a hit out of the
    // NDJSON body and feed it straight into fragment.plan.
    let grep = client
        .call(
            "timeline.grep",
            Some(&root),
            serde_json::json!({
                "query": {"kind": "literal", "text": "fn probe() { 1 }"},
                "path": "lib.rs",
            }),
            None,
        )
        .unwrap();
    assert!(grep.response.ok, "grep failed: {:?}", grep.response.error);
    let line = grep
        .body
        .split(|b| *b == b'\n')
        .find(|l| !l.is_empty())
        .expect("grep emitted at least one hit");
    let record: Value = serde_json::from_slice(line).unwrap();
    let handle = record["hit"]["handle"].clone();
    assert!(handle.is_object(), "hit carries its handle: {record}");

    // Replace mode against the deleted unit is a typed conflict, and a
    // non-applicable plan is data, not an IPC error.
    let replace = client
        .call(
            "fragment.plan",
            Some(&root),
            serde_json::json!({"selections": [handle.clone()], "mode": "replace"}),
            None,
        )
        .unwrap();
    assert!(
        replace.response.ok,
        "plan failed: {:?}",
        replace.response.error
    );
    let summary = replace.response.result.unwrap_or(Value::Null);
    assert_eq!(summary["plan_summary"]["applicable"], false);
    assert_eq!(summary["plan_summary"]["conflicts"], 1);
    let plan: Value = serde_json::from_slice(&replace.body).unwrap();
    assert_eq!(plan["conflicts"][0]["condition"], "missing");

    // Insert mode finds the unique scar and applies by token.
    let insert = client
        .call(
            "fragment.plan",
            Some(&root),
            serde_json::json!({"selections": [handle.clone()], "mode": "insert"}),
            None,
        )
        .unwrap();
    assert!(insert.response.ok);
    let summary = insert.response.result.unwrap_or(Value::Null);
    assert_eq!(summary["plan_summary"]["applicable"], true);
    let token = summary["plan_summary"]["token"]
        .as_str()
        .expect("token in summary")
        .to_owned();

    let applied = client
        .call(
            "fragment.apply",
            Some(&root),
            serde_json::json!({"token": token}),
            None,
        )
        .unwrap();
    assert!(
        applied.response.ok,
        "apply failed: {:?}",
        applied.response.error
    );
    let outcome = applied.response.result.unwrap_or(Value::Null)["outcome"].clone();
    assert_eq!(outcome["mode"], "fragment");
    assert!(outcome["restore_capture"].is_string());
    assert_eq!(
        std::fs::read(root.join("lib.rs")).unwrap(),
        b"fn probe() { 1 }\nfn other() {}\n".to_vec(),
        "the unit is back at its scar and nothing else moved"
    );

    // The forward capture names the selection that produced it.
    let capture_id = outcome["restore_capture"].as_str().unwrap();
    let info = call(
        &mut client,
        "timeline.info",
        Some(&root),
        serde_json::json!({
            "reference": capture_id,
        }),
    );
    let origin = &info["info"]["capture"]["origin"];
    assert_eq!(origin["kind"], "fragment_restore");
    let selections = origin["selections"].as_array().expect("selections array");
    assert_eq!(selections.len(), 1);

    // An unknown token fails stale, never ad-hoc.
    let bad = client
        .call(
            "fragment.apply",
            Some(&root),
            serde_json::json!({"token": "deadbeef"}),
            None,
        )
        .unwrap();
    assert!(!bad.response.ok);
    assert_eq!(
        bad.response.error.as_ref().map(|e| e.code.as_str()),
        Some("restore.plan_stale")
    );

    daemon.shutdown();
}

/// Buffered callers of the streamed `timeline.grep` body: the summary is
/// the last NDJSON record in the body (proto 1.5), not the envelope result.
fn grep_summary(reply: &sheaf_core::ipc::Reply) -> Value {
    let mut summary = None;
    for line in reply.body.split(|b| *b == b'\n').filter(|l| !l.is_empty()) {
        let value: Value = serde_json::from_slice(line).expect("grep body record");
        if value["type"] == "summary" {
            summary = Some(value["report"].clone());
        }
    }
    summary.expect("grep summary record")
}

#[test]
fn timeline_grep_streams_ndjson_and_paginates_over_ipc() {
    let guard = EnvGuard::new("grep");
    let socket = guard.socket();
    let root = guard.project();
    let daemon = Daemon::spawn(socket.clone());
    enroll(&socket, &root);

    // Build a small evolving history of one function.
    write_file(&root, "lib.rs", "fn probe() { 1 }\n".into());
    wait_for_captures(&socket, &root, 1, Duration::from_secs(15));
    write_file(&root, "lib.rs", "fn probe() { 2 }\n".into());
    wait_for_captures(&socket, &root, 2, Duration::from_secs(15));
    write_file(&root, "lib.rs", "fn probe() { 3 }\n".into());
    wait_for_captures(&socket, &root, 3, Duration::from_secs(15));

    // Capability is advertised on the current daemon.
    let mut client = Client::connect(&socket, Duration::from_secs(2)).unwrap();
    let pong = call(&mut client, "ping", None, serde_json::json!({}));
    let caps = pong
        .get("capabilities")
        .and_then(Value::as_array)
        .expect("capabilities array");
    assert!(caps.iter().any(|c| c.as_str() == Some("timeline.grep")));

    // A full grep: the NDJSON body carries exactly hits + events records.
    let reply = client
        .call(
            "timeline.grep",
            Some(&root),
            serde_json::json!({
                "query": {"kind": "literal", "text": "fn probe"},
                "path": "lib.rs",
            }),
            None,
        )
        .unwrap();
    assert!(reply.response.ok, "grep failed: {:?}", reply.response.error);
    let summary = grep_summary(&reply);
    let hits = summary["hits"].as_array().map(|a| a.len()).unwrap_or(0) as u64;
    let events = summary["events"].as_array().map(|a| a.len()).unwrap_or(0) as u64;
    assert!(hits >= 3, "expected introduce + 2 changes, got {hits}");
    let body_lines = reply
        .body
        .split(|b| *b == b'\n')
        .filter(|l| !l.is_empty())
        .count() as u64;
    assert_eq!(
        body_lines,
        hits + events + 1,
        "NDJSON body = hit/event records + the summary record"
    );

    // Budget exhaustion returns an incomplete report with a cursor; resuming
    // recovers the remaining hits and completes.
    let page1 = client
        .call(
            "timeline.grep",
            Some(&root),
            serde_json::json!({
                "query": {"kind": "literal", "text": "fn probe"},
                "path": "lib.rs",
                "budget": {"max_results": 1},
            }),
            None,
        )
        .unwrap();
    let s1 = grep_summary(&page1);
    assert_eq!(s1["complete"], false);
    let mut cursor = s1["cursor"].clone();
    assert!(cursor.is_object(), "an incomplete run must carry a cursor");

    let mut total = s1["hits"].as_array().map(|a| a.len()).unwrap_or(0) as u64;
    let mut guard_iter = 0;
    loop {
        guard_iter += 1;
        assert!(guard_iter < 20, "pagination did not terminate");
        // A robust client forwards the whole cursor, including the
        // partial-capture resume fields (a single capture may now hold more
        // records than the budget).
        let page = client
            .call(
                "timeline.grep",
                Some(&root),
                serde_json::json!({
                    "query": {"kind": "literal", "text": "fn probe"},
                    "path": "lib.rs",
                    "budget": {"max_results": 1},
                    "cursor": cursor,
                }),
                None,
            )
            .unwrap();
        let s = grep_summary(&page);
        total += s["hits"].as_array().map(|a| a.len()).unwrap_or(0) as u64;
        if s["complete"].as_bool().unwrap_or(false) {
            break;
        }
        cursor = s["cursor"].clone();
    }
    assert_eq!(total, hits, "paged hits must equal the unbounded total");

    // A cursor for a different query scope is rejected with state.bad_cursor.
    let bad = client
        .call(
            "timeline.grep",
            Some(&root),
            serde_json::json!({
                "query": {"kind": "literal", "text": "fn probe"},
                "path": "lib.rs",
                "cursor": {
                    "query_fingerprint": "literal:fn probe|extent=match|all=1|every=0|from=|to=|path=|follow=0",
                    "after_capture_id": "abcdef123456",
                    "path_index": 0,
                    "match_index": 0,
                },
            }),
            None,
        )
        .unwrap();
    assert!(!bad.response.ok);
    assert_eq!(
        bad.response.error.as_ref().map(|e| e.code.as_str()),
        Some("state.bad_cursor")
    );

    daemon.shutdown();
}

#[test]
fn a_second_daemon_on_the_shared_registry_is_refused_at_startup() {
    let _env = EnvGuard::new("singleton");
    let first = Daemon::spawn(_env.socket());

    // Same XDG_DATA_HOME (EnvGuard pins process env, and the child inherits
    // it), different --socket: exactly the shape that used to yield two
    // watchers over one enrollment registry. The singleton must refuse it
    // before it binds anything.
    let rogue_socket = _env.base.join("run").join("rogue.sock");
    let err_file = _env.base.join("rogue.stderr");
    let ferr = std::fs::File::create(&err_file).unwrap();
    let mut rogue = Command::new(DAEMON)
        .args(["run", "--socket"])
        .arg(&rogue_socket)
        .stdout(Stdio::null())
        .stderr(Stdio::from(ferr))
        .spawn()
        .expect("spawn rogue sheafd");
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        match rogue.try_wait().expect("poll rogue sheafd") {
            Some(status) => break status,
            None if Instant::now() > deadline => {
                rogue.kill().unwrap();
                panic!("rogue sheafd kept running past the singleton refusal");
            }
            None => std::thread::sleep(Duration::from_millis(50)),
        }
    };
    assert!(!status.success(), "the rogue daemon must fail");
    assert!(
        !rogue_socket.exists(),
        "the refused daemon must not leave a socket behind"
    );
    let stderr = std::fs::read_to_string(&err_file).unwrap();
    assert!(
        stderr.contains("another sheafd") && stderr.contains("pid="),
        "the refusal must name the incumbent daemon: {stderr}"
    );

    first.shutdown();
}

#[test]
fn gitignore_edits_and_info_exclude_apply_without_restart() {
    let _env = EnvGuard::new("ignore-refresh");
    let root = _env.project();
    // A real repository shape: tracked file + .gitignore + info/exclude
    // whose rules arrive AFTER the daemon is already watching.
    config::write_skeleton(&root).unwrap();
    std::fs::write(root.join(".gitignore"), "initial-ignored.log\n").unwrap();
    std::fs::create_dir_all(root.join(".git/info")).unwrap();
    std::fs::write(root.join(".git/info/exclude"), "excluded-dir/\n").unwrap();
    std::fs::write(root.join("tracked.txt"), "v1\n").unwrap();
    let daemon = Daemon::spawn(_env.socket());
    init_project(
        &root,
        InitOptions {
            socket_override: Some(_env.socket().to_path_buf()),
            ..Default::default()
        },
    )
    .unwrap();
    wait_for_captures(&_env.socket(), &root, 1, Duration::from_secs(10));

    // Rules that exist from the start apply immediately.
    std::fs::write(root.join("initial-ignored.log"), "noise\n").unwrap();
    std::fs::create_dir_all(root.join("excluded-dir")).unwrap();
    std::fs::write(root.join("excluded-dir/secret.txt"), "private\n").unwrap();
    std::fs::write(root.join("tracked.txt"), "v2\n").unwrap();
    let before = capture_count(&_env.socket(), &root);
    wait_for_captures(&_env.socket(), &root, before + 1, Duration::from_secs(10));
    std::thread::sleep(Duration::from_millis(500));
    assert!(
        root.join("initial-ignored.log").is_file(),
        "fixture self-check"
    );
    let log_json = timeline_log(&_env.socket(), &root);
    for capture in &log_json {
        for p in capture["paths"].as_array().unwrap() {
            let p = p.as_str().unwrap();
            assert!(
                !p.contains("initial-ignored.log") && !p.contains("excluded-dir"),
                "pre-existing rules must filter: {p}"
            );
        }
    }

    // A rule ADDED after the daemon started must take effect without a
    // restart: the .gitignore save itself is the refresh trigger.
    let captured_before = capture_count(&_env.socket(), &root);
    std::fs::write(
        root.join(".gitignore"),
        "initial-ignored.log\nlate-ignored/\n",
    )
    .unwrap();
    // Give the refresh a moment to land, then write into the new pattern.
    std::thread::sleep(Duration::from_millis(700));
    std::fs::create_dir_all(root.join("late-ignored")).unwrap();
    std::fs::write(root.join("late-ignored/blob.bin"), b"\x00\x01\x02").unwrap();
    std::fs::write(root.join("tracked.txt"), "v3\n").unwrap();
    wait_for_captures(
        &_env.socket(),
        &root,
        captured_before + 1,
        Duration::from_secs(10),
    );
    std::thread::sleep(Duration::from_millis(500));
    let log_json = timeline_log(&_env.socket(), &root);
    for capture in &log_json {
        for p in capture["paths"].as_array().unwrap() {
            let p = p.as_str().unwrap();
            assert!(
                !p.contains("late-ignored"),
                "refreshed rules must filter: {p}"
            );
        }
    }

    daemon.shutdown();
}

/// Minimal `timeline.log` reader for the ignore test: every capture's
/// recorded paths.
fn timeline_log(socket: &Path, root: &Path) -> Vec<Value> {
    let mut c = Client::connect(socket, Duration::from_secs(2)).unwrap();
    let reply = c
        .call(
            "timeline.log",
            Some(root),
            serde_json::json!({ "limit": 100 }),
            None,
        )
        .unwrap();
    assert!(reply.response.ok, "{:?}", reply.response.error);
    reply
        .response
        .result
        .and_then(|v| v.get("captures").cloned())
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default()
}

/// The daemon's concurrent-connection ceiling drops clients past the cap
/// instead of wedging the accept loop, and freed slots serve again.
#[test]
fn connection_cap_drops_excess_clients_then_recovers() {
    let guard = EnvGuard::new("conn-cap");
    let socket = guard.socket();
    let daemon = Daemon::spawn(socket.clone());

    // Fill every connection slot: each client first proves it is being
    // served (the ping answers), then goes silent while holding its slot.
    let mut idle = Vec::new();
    for _ in 0..32 {
        let mut client =
            Client::connect(&socket, Duration::from_secs(2)).expect("slot client connects");
        assert!(client.ping().is_ok(), "slot client is served");
        idle.push(client);
    }

    // Slot 33 is accepted and dropped before its request is answered.
    let mut overflow = Client::connect(&socket, Duration::from_secs(2)).unwrap();
    assert!(
        overflow
            .call("ping", None, serde_json::json!({}), None)
            .is_err(),
        "a client over the connection cap must not be served"
    );

    // Freed slots serve again: the cap drops, it does not poison.
    drop(idle);
    drop(overflow);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(mut client) = Client::connect(&socket, Duration::from_millis(400)) {
            if client.ping().is_ok() {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "daemon never recovered after the cap"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    daemon.shutdown();
}

/// `project.status` surfaces a pending restore intent with its staleness
/// verdict, so an unfinished restore is actionable instead of invisible.
#[test]
fn project_status_surfaces_pending_restore_intents() {
    let guard = EnvGuard::new("status-intent");
    let socket = guard.socket();
    let daemon = Daemon::spawn(socket.clone());
    let root = guard.project();
    config::write_skeleton(&root).unwrap();
    write_file(&root, "seed.txt", "seed\n".into());
    enroll(&socket, &root);
    wait_ready(&socket, &root, Duration::from_secs(15));

    let mut client = Client::connect(&socket, Duration::from_secs(2)).unwrap();
    let status = call(
        &mut client,
        "project.status",
        Some(&root),
        serde_json::json!({}),
    );
    assert_eq!(status["registered"], true);
    assert!(
        status["pending_restore"].is_null(),
        "no intent has been parked yet"
    );

    let state = root.join(".sheaf/state");
    std::fs::create_dir_all(&state).unwrap();
    let park_intent = |started_ms: i64| {
        std::fs::write(
            state.join("restore.intent"),
            serde_json::json!({
                "token": "status-intent-token",
                "mode": "full",
                "scope": [],
                "target": {"frontier": "unresolvable", "capture_id": null},
                "started_ms": started_ms,
            })
            .to_string(),
        )
        .unwrap();
    };

    // A fresh intent is pending and auto-resumable.
    park_intent(chrono::Utc::now().timestamp_millis());
    let status = call(
        &mut client,
        "project.status",
        Some(&root),
        serde_json::json!({}),
    );
    let pending = status["pending_restore"].clone();
    assert_eq!(pending["token"], "status-intent-token");
    assert_eq!(pending["stale"], false);
    assert_eq!(pending["auto_resume"], true);
    assert!(pending["age_ms"].is_i64(), "age_ms must be reported");

    // An intent past the staleness bound is surfaced, never auto-replayed.
    park_intent(0);
    let status = call(
        &mut client,
        "project.status",
        Some(&root),
        serde_json::json!({}),
    );
    let pending = status["pending_restore"].clone();
    assert_eq!(pending["stale"], true);
    assert_eq!(pending["auto_resume"], false);

    // Leave no intent behind for the daemon's next boot.
    std::fs::remove_file(state.join("restore.intent")).unwrap();
    daemon.shutdown();
}
