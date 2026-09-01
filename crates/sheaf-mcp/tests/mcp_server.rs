//! End-to-end tests for the sheaf-mcp server: the real binary is driven
//! over stdio exactly the way an MCP client would, against a deterministic
//! fake `sheaf` executable. The fake records every invocation (argv + real
//! working directory) as one JSON line on stdout, so each test asserts both
//! the response shape *and* the exact CLI contract the server produced.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use serde_json::{json, Value};
use tempfile::TempDir;

/// A deterministic stand-in for the `sheaf` CLI.
///
/// Every invocation first prints one JSON line `{"argv":[...],"cwd":...}`
/// recording what the server ran and where. Then, in order: optional extra
/// stdout (`FAKE_SHEAF_STDOUT`), optional stderr (`FAKE_SHEAF_STDERR`),
/// optional sleep (`FAKE_SHEAF_SLEEP`, seconds), and finally exits with
/// `FAKE_SHEAF_EXIT` (default 0). When `FAKE_SHEAF_LOG` is set, the raw
/// argv words are appended to that file, letting tests prove a tool was
/// (or was never) spawned.
const FAKE_SHEAF: &str = r##"#!/bin/sh
[ -n "${FAKE_SHEAF_LOG:-}" ] && printf '%s\n' "$@" >> "$FAKE_SHEAF_LOG"
if [ -z "${FAKE_SHEAF_SILENT:-}" ]; then
  printf '{"argv":['
  first=1
  for a in "$@"; do
    if [ "$first" -eq 1 ]; then first=0; else printf ','; fi
    esc=$(printf '%s' "$a" | sed 's/\\/\\\\/g; s/"/\\"/g')
    printf '"%s"' "$esc"
  done
  printf '],"cwd":"%s"}\n' "$(pwd)"
fi
[ -n "${FAKE_SHEAF_STDOUT:-}" ] && printf '%s\n' "$FAKE_SHEAF_STDOUT"
[ -n "${FAKE_SHEAF_STDERR:-}" ] && printf '%s\n' "$FAKE_SHEAF_STDERR" >&2
[ -n "${FAKE_SHEAF_SLEEP:-}" ] && sleep "$FAKE_SHEAF_SLEEP"
exit "${FAKE_SHEAF_EXIT:-0}"
"##;

struct Server {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl Server {
    /// Spawn the real server binary with a hermetic environment. Every
    /// environment knob this server reads is pinned explicitly, so the
    /// test runner's own environment can never leak into a scenario.
    fn spawn(fake: &Path, server_cwd: &Path, extra_env: &[(&str, &str)]) -> Server {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_sheaf-mcp"));
        cmd.current_dir(server_cwd)
            .env("SHEAF_BIN", fake)
            .env("SHEAF_PROJECT", "")
            .env("SHEAF_MCP_ALLOW_WRITE", "")
            .env("SHEAF_MCP_CALL_TIMEOUT_SECS", "")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        for (k, v) in extra_env {
            cmd.env(k, v);
        }
        let mut child = cmd.spawn().expect("spawn sheaf-mcp");
        let stdin = child.stdin.take().expect("server stdin");
        let stdout = BufReader::new(child.stdout.take().expect("server stdout"));
        Server {
            child,
            stdin,
            stdout,
            next_id: 1,
        }
    }

    fn send_line(&mut self, line: &str) {
        writeln!(self.stdin, "{line}").expect("write to server stdin");
        self.stdin.flush().expect("flush server stdin");
    }

    /// Read the next reply line. Panics with context if the server dies —
    /// an EOF here is always a bug in the server or the scenario.
    fn recv_reply(&mut self) -> Value {
        let mut line = String::new();
        let n = self.stdout.read_line(&mut line).expect("read server reply");
        assert!(n > 0, "server closed stdout before replying");
        serde_json::from_str(line.trim()).expect("reply is JSON-RPC")
    }

    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        self.send_line(
            &json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}).to_string(),
        );
        self.recv_reply()
    }

    fn call_tool(&mut self, name: &str, arguments: Value) -> Value {
        self.request("tools/call", json!({"name": name, "arguments": arguments}))
    }
}

/// Dropping `stdin` closes the server's pipe, which ends its read loop.
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The result payload helpers: tools/call replies wrap the CLI's text.
fn result_of(reply: &Value) -> &Value {
    reply.get("result").expect("tools/call carries a result")
}

fn text_of(reply: &Value) -> &str {
    result_of(reply)["content"][0]["text"]
        .as_str()
        .expect("text content")
}

fn is_error(reply: &Value) -> bool {
    result_of(reply)["isError"].as_bool().expect("isError flag")
}

/// The invocation record the fake prints for the most recent call.
fn recorded(text: &str) -> (Vec<String>, String) {
    let first = text.lines().next().expect("fake output present");
    let v: Value = serde_json::from_str(first).expect("fake prints one JSON record");
    let argv = v["argv"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a.as_str().unwrap().to_owned())
        .collect();
    (argv, v["cwd"].as_str().unwrap().to_owned())
}

// ------------------------------------------------------------- fixtures

struct Fixture {
    _dir: TempDir,
    fake: PathBuf,
    server_cwd: PathBuf,
}

fn fixture() -> Fixture {
    let dir = TempDir::new().expect("tempdir");
    let fake = dir.path().join("bin/sheaf");
    std::fs::create_dir_all(fake.parent().unwrap()).unwrap();
    std::fs::write(&fake, FAKE_SHEAF).unwrap();
    std::fs::set_permissions(&fake, std::fs::Permissions::from_mode(0o755)).unwrap();
    let server_cwd = dir.path().join("server-cwd");
    std::fs::create_dir_all(&server_cwd).unwrap();
    Fixture {
        _dir: dir,
        fake,
        server_cwd,
    }
}

fn project_dir(f: &Fixture) -> PathBuf {
    let p = f._dir.path().join("proj");
    std::fs::create_dir_all(&p).unwrap();
    p
}

// ------------------------------------------------------------ handshake

#[test]
fn initialize_completes_the_handshake() {
    let f = fixture();
    let mut s = Server::spawn(&f.fake, &f.server_cwd, &[]);
    let reply = s.request(
        "initialize",
        json!({"protocolVersion": "2025-06-18", "clientInfo": {"name": "integration"}}),
    );
    assert_eq!(reply["result"]["protocolVersion"], "2025-06-18");
    assert_eq!(reply["result"]["serverInfo"]["name"], "sheaf-mcp");
    assert!(reply["result"]["capabilities"]["tools"].is_object());
}

#[test]
fn initialize_without_a_version_gets_the_server_default() {
    let f = fixture();
    let mut s = Server::spawn(&f.fake, &f.server_cwd, &[]);
    let reply = s.request("initialize", json!({}));
    assert_eq!(reply["result"]["protocolVersion"], "2024-11-05");
}

#[test]
fn notifications_are_consumed_silently_and_ordering_holds() {
    let f = fixture();
    let mut s = Server::spawn(&f.fake, &f.server_cwd, &[]);
    s.send_line(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#);
    // The next line out must be the ping's reply, proving the notification
    // produced none. The ping is the first id handed out (1).
    let reply = s.request("ping", json!({}));
    assert_eq!(reply["id"], 1);
    assert_eq!(reply["result"], json!({}));
}

#[test]
fn tools_list_exposes_the_documented_tools() {

    let f = fixture();
    let mut s = Server::spawn(&f.fake, &f.server_cwd, &[]);
    let reply = s.request("tools/list", json!({}));
    let names: Vec<&str> = reply["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(names.len(), 14);

    assert!(names.contains(&"sheaf_status"));
    assert!(names.contains(&"sheaf_restore_apply"));
    assert!(names.contains(&"sheaf_worktree_add"));
    assert!(names.contains(&"sheaf_merge_apply"));

    assert!(names.contains(&"sheaf_gc"));
}

#[test]
fn malformed_lines_get_a_parse_error_with_null_id() {
    let f = fixture();
    let mut s = Server::spawn(&f.fake, &f.server_cwd, &[]);
    s.send_line("totally not json");
    let reply = s.recv_reply();
    assert!(reply["id"].is_null());
    assert_eq!(reply["error"]["code"], -32700);
}

#[test]
fn unknown_methods_get_32601() {
    let f = fixture();
    let mut s = Server::spawn(&f.fake, &f.server_cwd, &[]);
    let reply = s.request("prompts/list", json!({}));
    assert_eq!(reply["error"]["code"], -32601);
    assert_eq!(reply["error"]["message"], "method not found: prompts/list");
}

// ------------------------------------------------------ project routing

#[test]
fn status_routes_to_the_explicit_project() {
    let f = fixture();
    let root = project_dir(&f);
    let mut s = Server::spawn(&f.fake, &f.server_cwd, &[]);
    let reply = s.call_tool("sheaf_status", json!({"project": root.to_string_lossy()}));
    assert!(!is_error(&reply));
    let (argv, cwd) = recorded(text_of(&reply));
    assert_eq!(
        argv,
        ["status".to_owned(), root.to_string_lossy().into_owned()]
    );
    assert_eq!(Path::new(&cwd), root.as_path());
}

#[test]
fn sheaf_project_env_is_the_fallback_root() {
    let f = fixture();
    let root = project_dir(&f);
    let mut s = Server::spawn(
        &f.fake,
        &f.server_cwd,
        &[("SHEAF_PROJECT", &root.to_string_lossy())],
    );
    let reply = s.call_tool("sheaf_doctor", json!({}));
    assert!(!is_error(&reply));
    let (argv, cwd) = recorded(text_of(&reply));
    assert_eq!(
        argv,
        [
            "doctor".to_owned(),
            "--json".to_owned(),
            "-C".to_owned(),
            root.to_string_lossy().into_owned()
        ]
    );
    assert_eq!(Path::new(&cwd), root.as_path());
}

#[test]
fn without_any_project_the_server_cwd_is_used() {
    let f = fixture();
    let mut s = Server::spawn(&f.fake, &f.server_cwd, &[]);
    let reply = s.call_tool("sheaf_status", json!({}));
    assert!(!is_error(&reply));
    let (argv, cwd) = recorded(text_of(&reply));
    assert_eq!(argv, ["status".to_owned()]);
    assert_eq!(Path::new(&cwd), f.server_cwd.as_path());
}

// --------------------------------------------------------- argv contracts

#[test]
fn log_arguments_reach_the_cli_verbatim_and_capped() {
    let f = fixture();
    let root = project_dir(&f);
    let mut s = Server::spawn(&f.fake, &f.server_cwd, &[]);
    let reply = s.call_tool(
        "sheaf_log",
        json!({"project": root.to_string_lossy(), "path": "src/lib.rs",
               "follow": true, "all": true, "before": "abc123", "limit": 5000}),
    );
    let (argv, _) = recorded(text_of(&reply));
    assert_eq!(
        argv,
        [
            "log",
            "--json",
            "-C",
            root.to_str().unwrap(),
            "--path",
            "src/lib.rs",
            "--follow",
            "--all",
            "--before",
            "abc123",
            "--limit",
            "1000"
        ]
    );
}

#[test]
fn diff_points_collapse_into_a_single_span_argument() {
    let f = fixture();
    let root = project_dir(&f);
    let mut s = Server::spawn(&f.fake, &f.server_cwd, &[]);
    let reply = s.call_tool(
        "sheaf_diff",
        json!({"project": root.to_string_lossy(), "from": "@~5", "to": "@~2", "stat": true}),
    );
    let (argv, _) = recorded(text_of(&reply));
    assert_eq!(
        argv,
        ["diff", "@~5..@~2", "--stat", "-C", root.to_str().unwrap()]
    );
}

#[test]
fn checkpoint_create_reaches_the_cli_with_name_and_at() {
    let f = fixture();
    let root = project_dir(&f);
    let mut s = Server::spawn(&f.fake, &f.server_cwd, &[]);
    let reply = s.call_tool(
        "sheaf_checkpoint_create",
        json!({"project": root.to_string_lossy(), "name": "before-rework", "at": "@~3"}),
    );
    assert!(!is_error(&reply));
    let (argv, _) = recorded(text_of(&reply));
    assert_eq!(
        argv,
        [
            "checkpoint",
            "create",
            "before-rework",
            "--at",
            "@~3",
            "-C",
            root.to_str().unwrap()
        ]
    );
}

// ------------------------------------------------------------ write gate

#[test]
fn restore_apply_is_denied_by_default_and_never_spawns() {
    let f = fixture();
    let log = f._dir.path().join("invocations.log");
    let mut s = Server::spawn(
        &f.fake,
        &f.server_cwd,
        &[("FAKE_SHEAF_LOG", log.to_str().unwrap())],
    );
    let reply = s.call_tool("sheaf_restore_apply", json!({"point": "@~1"}));
    assert!(is_error(&reply));
    assert!(
        text_of(&reply).contains("SHEAF_MCP_ALLOW_WRITE"),
        "denial must name the knob: {}",
        text_of(&reply)
    );
    assert!(!log.exists(), "the gated tool must not reach the CLI");
}

#[test]
fn restore_apply_runs_only_with_the_gate_open() {
    let f = fixture();
    let root = project_dir(&f);
    let mut s = Server::spawn(&f.fake, &f.server_cwd, &[("SHEAF_MCP_ALLOW_WRITE", "1")]);
    let reply = s.call_tool(
        "sheaf_restore_apply",
        json!({"point": "@~1", "paths": ["src/a.rs"], "project": root.to_string_lossy()}),
    );
    assert!(!is_error(&reply));
    let (argv, _) = recorded(text_of(&reply));
    assert_eq!(
        argv,
        [
            "restore",
            "--at",
            "@~1",
            "src/a.rs",
            "-C",
            root.to_str().unwrap()
        ]
    );
}

#[test]
fn restore_plan_stays_available_without_the_gate() {
    let f = fixture();
    let mut s = Server::spawn(&f.fake, &f.server_cwd, &[]);
    let reply = s.call_tool("sheaf_restore_plan", json!({"point": "@~1"}));
    assert!(!is_error(&reply));
    let (argv, _) = recorded(text_of(&reply));
    assert_eq!(argv, ["restore", "--at", "@~1", "--dry-run"]);
}

#[test]
fn collecting_gc_is_gated_but_reporting_is_not() {
    let f = fixture();
    let mut s = Server::spawn(&f.fake, &f.server_cwd, &[]);
    let reply = s.call_tool("sheaf_gc", json!({"apply": true}));
    assert!(is_error(&reply));
    assert!(text_of(&reply).contains("SHEAF_MCP_ALLOW_WRITE"));

    // A string-typed "true" gates identically — clients mis-type args.
    let reply = s.call_tool("sheaf_gc", json!({"apply": "true"}));
    assert!(is_error(&reply));

    // The report is a read: allowed with the gate closed.
    let reply = s.call_tool("sheaf_gc", json!({}));
    assert!(!is_error(&reply));
    let (argv, _) = recorded(text_of(&reply));
    assert_eq!(argv, ["gc", "--json"]);
}

#[test]
fn init_is_gated_and_then_runs_with_the_gate_open() {
    let f = fixture();
    let target = f._dir.path().join("new-project");
    std::fs::create_dir_all(&target).unwrap();

    let mut s = Server::spawn(&f.fake, &f.server_cwd, &[]);
    let reply = s.call_tool("sheaf_init", json!({"path": target.to_string_lossy()}));
    assert!(is_error(&reply));
    assert!(text_of(&reply).contains("SHEAF_MCP_ALLOW_WRITE"));
    drop(s);

    let mut s = Server::spawn(&f.fake, &f.server_cwd, &[("SHEAF_MCP_ALLOW_WRITE", "yes")]);
    let reply = s.call_tool("sheaf_init", json!({"path": target.to_string_lossy()}));
    assert!(!is_error(&reply));
    let (argv, _) = recorded(text_of(&reply));
    assert_eq!(
        argv,
        ["init".to_owned(), target.to_string_lossy().into_owned()]
    );
}

// ------------------------------------------------------- error plumbing

#[test]
fn cli_failures_surface_as_tool_errors_with_diagnostics() {
    let f = fixture();
    let mut s = Server::spawn(
        &f.fake,
        &f.server_cwd,
        &[
            ("FAKE_SHEAF_EXIT", "3"),
            ("FAKE_SHEAF_STDERR", "store lock held by pid 4242"),
        ],
    );
    let reply = s.call_tool("sheaf_status", json!({}));
    assert!(is_error(&reply));
    let text = text_of(&reply);
    assert!(text.contains("sheaf exited with exit status: 3"), "{text}");
    assert!(text.contains("store lock held by pid 4242"), "{text}");
}

#[test]
fn silent_cli_success_gets_the_placeholder_text() {
    let f = fixture();
    // FAKE_SHEAF_SILENT suppresses the invocation record: the CLI then
    // exits 0 with no output on either stream, which is the
    // "(ok, no output)" branch.
    let mut s = Server::spawn(&f.fake, &f.server_cwd, &[("FAKE_SHEAF_SILENT", "1")]);
    let reply = s.call_tool("sheaf_checkpoint_list", json!({}));
    assert!(!is_error(&reply));
    assert_eq!(text_of(&reply), "(ok, no output)");
}

#[test]
fn hanging_cli_calls_are_abandoned_at_the_timeout() {
    let f = fixture();
    let mut s = Server::spawn(
        &f.fake,
        &f.server_cwd,
        &[
            ("SHEAF_MCP_CALL_TIMEOUT_SECS", "1"),
            ("FAKE_SHEAF_SLEEP", "4"),
        ],
    );
    let reply = s.call_tool("sheaf_doctor", json!({}));
    assert!(is_error(&reply));
    assert!(
        text_of(&reply).contains("did not finish within 1s"),
        "{}",
        text_of(&reply)
    );
}

#[test]
fn unknown_tools_and_missing_arguments_error_locally() {
    let f = fixture();
    let mut s = Server::spawn(&f.fake, &f.server_cwd, &[]);

    let reply = s.call_tool("sheaf_frobnicate", json!({}));
    assert!(is_error(&reply));
    assert_eq!(text_of(&reply), "unknown tool: sheaf_frobnicate");

    let reply = s.call_tool("sheaf_checkpoint_create", json!({}));
    assert!(is_error(&reply));
    assert!(text_of(&reply).contains("needs a name"));

    let reply = s.call_tool("sheaf_restore_plan", json!({}));
    assert!(is_error(&reply));
    assert!(text_of(&reply).contains("needs a timeline point"));
    drop(s);

    // Initialization is write-gated before argument validation. Open the gate
    // to exercise the missing-path validation branch without spawning the CLI.
    let mut s = Server::spawn(&f.fake, &f.server_cwd, &[("SHEAF_MCP_ALLOW_WRITE", "1")]);
    let reply = s.call_tool("sheaf_init", json!({}));
    assert!(is_error(&reply));
    assert!(text_of(&reply).contains("needs a path"));
}

#[test]
fn a_session_serves_sequential_requests_on_one_connection() {
    let f = fixture();
    let mut s = Server::spawn(&f.fake, &f.server_cwd, &[]);
    let init = s.request("initialize", json!({"protocolVersion": "2024-11-05"}));
    assert!(init.get("result").is_some());
    let ping = s.request("ping", json!({}));
    assert_eq!(ping["result"], json!({}));
    let list = s.request("tools/list", json!({}));
    assert_eq!(list["result"]["tools"].as_array().unwrap().len(), 14);
    let status = s.call_tool("sheaf_status", json!({}));
    assert!(!is_error(&status));
}
