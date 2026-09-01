//! `sheaf-mcp` — an MCP (Model Context Protocol) server exposing the sheaf
//! CLI to agent clients over stdio.
//!
//! Design: this server is a thin, faithful adapter in front of the `sheaf`
//! binary, not a second implementation of the surface. The CLI already owns
//! project-root resolution, degraded-mode fallback, exit codes,
//! and `--json` output; wrapping it keeps one source of truth and means the
//! MCP surface tracks the CLI automatically. Zero protocol dependencies:
//! MCP stdio is newline-delimited JSON-RPC 2.0, which serde_json handles.
//!
//! Safety posture: read-only and annotation tools are always
//! available. Anything that rewrites the worktree or the store —
//! `restore apply`, `gc --apply`, `sheaf init` — requires the server to be
//! started with `SHEAF_MCP_ALLOW_WRITE=1`. Default is deny.
//!
//! Environment knobs: `SHEAF_BIN` picks the wrapped CLI, `SHEAF_PROJECT` is
//! the default project root, and `SHEAF_MCP_CALL_TIMEOUT_SECS` overrides the
//! per-call ceiling (minimum 1s) for slow stores and for tests.

use std::io::BufRead as _;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use serde_json::{json, Map, Value};

/// Protocol version we announce; the stable core subset (initialize / ping /
/// tools) did not change across MCP revisions, so we accept the client's.
const PROTOCOL_VERSION: &str = "2024-11-05";
const SERVER_NAME: &str = "sheaf-mcp";
const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Per-call ceiling for the wrapped CLI; `diff` may legitimately use 30s.
const DEFAULT_CALL_TIMEOUT_SECS: u64 = 60;
/// Tool text is capped so one huge patch cannot flood an agent context.
const MAX_OUTPUT_CHARS: usize = 200_000;

/// Effective per-call ceiling: `SHEAF_MCP_CALL_TIMEOUT_SECS` overrides the
/// default (a missing, malformed, or zero value falls back to it).
fn call_timeout() -> Duration {
    let secs = std::env::var("SHEAF_MCP_CALL_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&s| s > 0)
        .unwrap_or(DEFAULT_CALL_TIMEOUT_SECS);
    Duration::from_secs(secs)
}

fn main() {
    let stdin = std::io::stdin();
    let mut out = std::io::stdout().lock();
    for line in stdin.lock().lines() {
        // Stdout goes to the MCP client's pipe; if that pipe is gone the
        // write error below is our SIGPIPE — exit quietly, no panic.
        let Ok(line) = line else { break };
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if let Some(reply) = handle_line(trimmed) {
            use std::io::Write as _;
            if writeln!(out, "{reply}").is_err() {
                break;
            }
            let _ = out.flush();
        }
    }
}

fn handle_line(line: &str) -> Option<String> {
    let msg: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => {
            return Some(
                json!({
                    "jsonrpc": "2.0", "id": null,
                    "error": {"code": -32700, "message": format!("parse error: {e}")}
                })
                .to_string(),
            )
        }
    };

    let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
    // Notifications carry no id and get no reply (JSON-RPC 2.0 §4.1) —
    // `notifications/initialized` lands here.
    let id = msg.get("id").filter(|v| !v.is_null()).cloned()?;

    match method {
        "initialize" => Some(reply(
            id,
            json!({
                "protocolVersion": pick_protocol(msg.get("params")),
                "capabilities": {"tools": {"listChanged": false}},
                "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
            }),
        )),
        "ping" => Some(reply(id, json!({}))),
        "tools/list" => Some(reply(id, json!({"tools": tool_table()}))),
        "tools/call" => Some(reply(id, call_tool(msg.get("params")))),
        _ => Some(
            json!({
                "jsonrpc": "2.0", "id": id,
                "error": {"code": -32601, "message": format!("method not found: {method}")}
            })
            .to_string(),
        ),
    }
}

fn pick_protocol(params: Option<&Value>) -> String {
    let requested = params
        .and_then(|p| p.get("protocolVersion"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .unwrap_or(PROTOCOL_VERSION);
    requested.to_owned()
}

fn reply(id: Value, result: Value) -> String {
    json!({"jsonrpc": "2.0", "id": id, "result": result}).to_string()
}

// ------------------------------------------------------------------ tools

/// Argument readers accept both native JSON types and their string forms;
/// MCP clients vary in how faithfully they type tool arguments.
fn bool_arg(args: &Value, key: &str) -> bool {
    match args.get(key) {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => matches!(s.as_str(), "true" | "1" | "yes"),
        _ => false,
    }
}

fn str_arg<'a>(args: &'a Value, key: &str) -> Option<&'a str> {
    args.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}

fn str_vec(args: &Value, key: &str) -> Vec<String> {
    args.get(key)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default()
}

/// Project root resolution with the environment injected, so callers (and
/// tests) stay free of process-global state: explicit argument first, then
/// the default root.
fn project_from(args: &Value, env_project: Option<String>) -> Option<String> {
    str_arg(args, "project")
        .map(str::to_owned)
        .or_else(|| env_project.filter(|s| !s.is_empty()))
}

/// Project root for a call: explicit argument, then SHEAF_PROJECT, then the
/// server's working directory.
fn project_arg(args: &Value) -> Option<String> {
    project_from(args, std::env::var("SHEAF_PROJECT").ok())
}

/// Write-gate decision with the environment injected; see `allow_write`.
fn allow_write_from(env_value: Option<String>) -> bool {
    env_value
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
}

fn allow_write() -> bool {
    allow_write_from(std::env::var("SHEAF_MCP_ALLOW_WRITE").ok())
}

/// Pure decision: does this call rewrite the worktree or the store? `gc`
/// only mutates with `apply=true`; the report is a read.
fn write_gated(name: &str, args: &Value) -> bool {
    match name {
        "sheaf_restore_apply"
        | "sheaf_worktree_add"
        | "sheaf_merge_apply"
        | "sheaf_init" => true,

        "sheaf_gc" => bool_arg(args, "apply"),
        _ => false,
    }
}

/// Small schema builder: a tool is a name, a description, its property
/// schemas, and the required list.
fn tool(name: &str, description: &str, properties: Value, required: &[&str]) -> Value {
    json!({
        "name": name,
        "description": description,
        "inputSchema": {
            "type": "object",
            "properties": properties,
            "required": required,
        }
    })
}

fn merge_props(parts: &[Value]) -> Value {
    let mut map = Map::new();
    for part in parts {
        if let Value::Object(obj) = part {
            for (k, v) in obj {
                map.insert(k.clone(), v.clone());
            }
        }
    }
    Value::Object(map)
}

fn project_prop() -> Value {
    json!({
        "project": {"type": "string",
            "description": "Absolute project root (a directory with .sheaf/). Defaults to SHEAF_PROJECT or the server working directory."}
    })
}

fn paths_prop() -> Value {
    json!({
        "paths": {"type": "array", "items": {"type": "string"},
            "description": "Limit to these root-relative paths."}
    })
}

fn tool_table() -> Vec<Value> {
    vec![
        tool(
            "sheaf_status",
            "Show sheaf store + daemon health for a project: enrolled, daemon version, watching state, pending restore intents.",
            project_prop(),
            &[],
        ),
        tool(
            "sheaf_log",
            "Browse capture history, newest first. A capture is a debounced batch of worktree changes recorded as CRDT operations (sheaf's flight recorder).",
            merge_props(&[project_prop(), json!({
                "path": {"type": "string", "description": "Only captures touching this root-relative path."},
                "follow": {"type": "boolean", "description": "Follow renames: include captures under this path's former names."},
                "all": {"type": "boolean", "description": "Include divergent branches, not only the current lineage."},
                "before": {"type": "string", "description": "Pagination cursor: a capture-ID prefix (>= 6 hex chars)."},
                "limit": {"type": "integer", "description": "Max entries (1-1000, default 50)."}
            })]),
            &[],
        ),
        tool(
            "sheaf_diff",
            "Compare two timeline points, or a point against the live worktree. Points: capture-ID prefix, checkpoint:<name>, @ (last capture), @~N (N captures back), or a timestamp like '2 hours ago'. With no arguments: uncaptured worktree edits vs the last capture.",
            merge_props(&[json!({
                "from": {"type": "string", "description": "Old point; defaults to @."},
                "to": {"type": "string", "description": "New point; omit to compare against the worktree."},
                "stat": {"type": "boolean", "description": "Per-file summary instead of a full patch."}
            }), paths_prop(), project_prop()]),
            &[],
        ),
        tool(
            "sheaf_checkpoint_list",
            "List named timeline checkpoints (immutable pins on the capture timeline).",
            project_prop(),
            &[],
        ),
        tool(
            "sheaf_checkpoint_create",
            "Pin a name to an exact timeline point. Annotations only — never rewrites history. Create one before risky edits so recovery is one command away.",
            merge_props(&[json!({
                "name": {"type": "string", "description": "Checkpoint name; name the intent, e.g. before-journal-rework."},
                "at": {"type": "string", "description": "Point to pin (default: @, the last capture)."}
            }), project_prop()]),
            &["name"],
        ),
        tool(
            "sheaf_restore_plan",
            "Preview a non-destructive restore: prints the per-path plan (create/update/delete) without touching the worktree. Always run this before sheaf_restore_apply.",
            merge_props(&[json!({
                "point": {"type": "string", "description": "Timeline point to restore to."}
            }), paths_prop(), project_prop()]),
            &["point"],
        ),
        tool(
            "sheaf_restore_apply",
            "Apply a restore: repositions the worktree to an earlier point; later edits diverge onto a new branch and nothing is erased (the abandoned future stays reachable). REQUIRES SHEAF_MCP_ALLOW_WRITE=1. Preview with sheaf_restore_plan first.",
            merge_props(&[json!({
                "point": {"type": "string", "description": "Timeline point to restore to."}
            }), paths_prop(), project_prop()]),
            &["point"],
        ),
        tool(
            "sheaf_worktree_list",
            "List the primary and every live linked worktree, including each worktree's current timeline tip.",
            project_prop(),
            &[],
        ),
        tool(
            "sheaf_worktree_add",
            "Materialize a timeline point as a live linked worktree sharing the project's Sheaf store. REQUIRES SHEAF_MCP_ALLOW_WRITE=1.",
            merge_props(&[json!({
                "point": {"type": "string", "description": "Capture, checkpoint, or branch-tip reference to materialize."},
                "destination": {"type": "string", "description": "New non-overlapping directory for the worktree."}
            }), project_prop()]),
            &["point", "destination"],
        ),
        tool(
            "sheaf_merge_plan",
            "Preview a squash merge from a divergent timeline source onto the active worktree; reports explicit path conflicts without writing.",
            merge_props(&[json!({
                "source": {"type": "string", "description": "Capture, checkpoint, or branch-tip reference to merge."}
            }), project_prop()]),
            &["source"],
        ),
        tool(
            "sheaf_merge_apply",
            "Apply a conflict-free squash merge onto the active worktree as one capture. Old branches remain reachable. REQUIRES SHEAF_MCP_ALLOW_WRITE=1.",
            merge_props(&[json!({
                "source": {"type": "string", "description": "Capture, checkpoint, or branch-tip reference to merge."}
            }), project_prop()]),
            &["source"],
        ),

        tool(
            "sheaf_doctor",
            "Read-only store integrity sweep: journal framing, snapshot chain, blob presence, pending restore intents.",
            project_prop(),
            &[],
        ),
        tool(
            "sheaf_gc",
            "Retention: report collectable bytes (orphan blobs, covered journal segments, superseded snapshots). GC is reachability-constrained: it never removes anything a restore to ANY timeline point could still need. apply=true actually collects and REQUIRES SHEAF_MCP_ALLOW_WRITE=1.",
            merge_props(&[json!({
                "apply": {"type": "boolean", "description": "Actually collect (default: report only). Gated by SHEAF_MCP_ALLOW_WRITE=1."}
            }), project_prop()]),
            &[],
        ),
        tool(
            "sheaf_init",
            "Enroll a directory: create its .sheaf/ store skeleton and register it with the daemon. REQUIRES SHEAF_MCP_ALLOW_WRITE=1 (enrollment is an explicit opt-in that writes a new store into the directory).",
            json!({
                "path": {"type": "string", "description": "Directory to enroll."}
            }),
            &["path"],
        ),
    ]
}

// ------------------------------------------------------------- dispatch

fn call_tool(params: Option<&Value>) -> Value {
    let Some(args) = params else {
        return error_result("missing params");
    };
    let name = str_arg(args, "name").unwrap_or("");
    let tool_args = args.get("arguments").cloned().unwrap_or_else(|| json!({}));
    match run_tool(name, &tool_args) {
        Ok(text) => text_result(text, false),
        Err(e) => text_result(format!("{e:#}"), true),
    }
}

fn text_result(text: String, is_error: bool) -> Value {
    let text = if text.chars().count() > MAX_OUTPUT_CHARS {
        let head: String = text.chars().take(MAX_OUTPUT_CHARS).collect();
        format!("{head}\n… [truncated by sheaf-mcp]")
    } else {
        text
    };
    json!({
        "content": [{"type": "text", "text": text}],
        "isError": is_error,
    })
}

fn error_result(message: &str) -> Value {
    json!({
        "content": [{"type": "text", "text": message}],
        "isError": true,
    })
}

fn run_tool(name: &str, args: &Value) -> anyhow::Result<String> {
    // The wrapped CLI: SHEAF_BIN overrides, else PATH.
    let sheaf = std::env::var("SHEAF_BIN").unwrap_or_else(|_| "sheaf".to_owned());
    let project = project_arg(args);

    // Write gate: default-deny for anything that rewrites the
    // worktree or the store.
    if write_gated(name, args) && !allow_write() {
        anyhow::bail!(
            "`{name}` mutates the worktree or store and is disabled unless sheaf-mcp is \
             started with SHEAF_MCP_ALLOW_WRITE=1"
        );
    }

    run_with_timeout(build_command(&sheaf, name, args, project.as_deref())?)
}

/// Build the exact CLI invocation for a tool call without running it, so the
/// argv contract for every tool is directly testable.
fn build_command(
    bin: &str,
    name: &str,
    args: &Value,
    project: Option<&str>,
) -> anyhow::Result<Command> {
    let mut cmd = Command::new(bin);
    if let Some(root) = project {
        // Belt and braces: `status` resolves from cwd, timeline verbs from
        // -C; set both so the project is unambiguous.
        cmd.current_dir(root);
    }

    match name {
        "sheaf_status" => {
            cmd.arg("status");
            if let Some(root) = project {
                cmd.arg(root);
            }
        }
        "sheaf_log" => {
            cmd.args(["log", "--json"]);
            if let Some(root) = project {
                cmd.args(["-C", root]);
            }
            if let Some(p) = str_arg(args, "path") {
                cmd.args(["--path", p]);
            }
            if bool_arg(args, "follow") {
                cmd.arg("--follow");
            }
            if bool_arg(args, "all") {
                cmd.arg("--all");
            }
            if let Some(b) = str_arg(args, "before") {
                cmd.args(["--before", b]);
            }
            if let Some(n) = args.get("limit").and_then(Value::as_u64) {
                cmd.args(["--limit", &n.min(1000).to_string()]);
            }
        }
        "sheaf_diff" => {
            cmd.arg("diff");
            let from = str_arg(args, "from");
            let to = str_arg(args, "to");
            match (from, to) {
                (Some(a), Some(b)) => cmd.arg(format!("{a}..{b}")),
                (Some(a), None) => cmd.arg(a),
                (None, Some(b)) => cmd.arg(format!("@..{b}")),
                (None, None) => &mut cmd,
            };
            if bool_arg(args, "stat") {
                cmd.arg("--stat");
            } else {
                cmd.arg("--json");
            }
            for p in str_vec(args, "paths") {
                cmd.args(["--path", &p]);
            }
            if let Some(root) = project {
                cmd.args(["-C", root]);
            }
        }
        "sheaf_checkpoint_list" => {
            cmd.args(["checkpoint", "list", "--json"]);
            if let Some(root) = project {
                cmd.args(["-C", root]);
            }
        }
        "sheaf_checkpoint_create" => {
            let Some(cname) = str_arg(args, "name") else {
                anyhow::bail!("sheaf_checkpoint_create needs a name");
            };
            cmd.arg("checkpoint").arg("create").arg(cname);
            if let Some(at) = str_arg(args, "at") {
                cmd.args(["--at", at]);
            }
            if let Some(root) = project {
                cmd.args(["-C", root]);
            }
        }
        "sheaf_restore_plan" | "sheaf_restore_apply" => {
            let Some(point) = str_arg(args, "point") else {
                anyhow::bail!(
                    "{name} needs a timeline point (checkpoint:<name>, @~N, a capture ID, or a timestamp)"
                );
            };
            cmd.arg("restore").arg("--at").arg(point);
            if name == "sheaf_restore_plan" {
                cmd.arg("--dry-run");
            }
            // Restore scopes are POSITIONAL (unlike diff's --path): with
            // --at given, every positional is a root-relative path.
            for p in str_vec(args, "paths") {
                cmd.arg(&p);
            }
            if let Some(root) = project {
                cmd.args(["-C", root]);
            }
        }
        "sheaf_worktree_list" => {
            cmd.args(["worktree", "list", "--json"]);
            if let Some(root) = project {
                cmd.args(["-C", root]);
            }
        }
        "sheaf_worktree_add" => {
            let Some(point) = str_arg(args, "point") else {
                anyhow::bail!("sheaf_worktree_add needs a timeline point");
            };
            let Some(destination) = str_arg(args, "destination") else {
                anyhow::bail!("sheaf_worktree_add needs a destination");
            };
            cmd.args(["worktree", "add", point, destination, "--json"]);
            if let Some(root) = project {
                cmd.args(["-C", root]);
            }
        }
        "sheaf_merge_plan" | "sheaf_merge_apply" => {
            let Some(source) = str_arg(args, "source") else {
                anyhow::bail!("{name} needs a source timeline point");
            };
            cmd.arg("merge").arg(source);
            if name == "sheaf_merge_apply" {
                cmd.arg("--apply");
            }
            cmd.arg("--json");
            if let Some(root) = project {
                cmd.args(["-C", root]);
            }
        }

        "sheaf_doctor" => {
            cmd.arg("doctor").arg("--json");
            if let Some(root) = project {
                cmd.args(["-C", root]);
            }
        }
        "sheaf_gc" => {
            cmd.arg("gc").arg("--json");
            if bool_arg(args, "apply") {
                cmd.arg("--apply");
            }
            if let Some(root) = project {
                cmd.args(["-C", root]);
            }
        }
        "sheaf_init" => {
            let Some(path) = str_arg(args, "path")
                .map(str::to_owned)
                .or_else(|| project.map(str::to_owned))
            else {
                anyhow::bail!("sheaf_init needs a path to enroll");
            };
            cmd.arg("init").arg(path);
        }
        other => anyhow::bail!("unknown tool: {other}"),
    }

    Ok(cmd)
}

/// Run the CLI to completion with a timeout. Stdout/stderr are drained on
/// threads (a child blocked on a full pipe would otherwise never exit and
/// every call would ride the timeout), and both streams are combined so the
/// CLI's stderr diagnostics reach the client.
fn run_with_timeout(mut cmd: Command) -> anyhow::Result<String> {
    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .with_context(|| format!("spawn {:?}", cmd.get_program()))?;

    fn drain<R: std::io::Read + Send + 'static>(
        pipe: Option<R>,
    ) -> std::thread::JoinHandle<String> {
        std::thread::spawn(move || {
            let mut buf = String::new();
            if let Some(mut p) = pipe {
                let _ = p.read_to_string(&mut buf);
            }
            buf
        })
    }
    let out_reader = drain(child.stdout.take());
    let err_reader = drain(child.stderr.take());

    let timeout = call_timeout();
    let started = Instant::now();
    let status = loop {
        match child.try_wait()? {
            Some(status) => break status,
            None if started.elapsed() > timeout => {
                let _ = child.kill();
                let _ = child.wait();
                anyhow::bail!(
                    "sheaf did not finish within {}s; call abandoned",
                    timeout.as_secs()
                );
            }
            None => std::thread::sleep(Duration::from_millis(20)),
        }
    };

    let stdout = out_reader.join().unwrap_or_default();
    let stderr = err_reader.join().unwrap_or_default();
    let mut text = stdout;
    if !stderr.trim().is_empty() {
        if !text.trim().is_empty() {
            text.push('\n');
        }
        text.push_str(stderr.trim_end());
    }

    if status.success() {
        Ok(if text.trim().is_empty() {
            "(ok, no output)".to_owned()
        } else {
            text
        })
    } else {
        anyhow::bail!("sheaf exited with {status}:\n{}", text.trim_end());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    // ---------------------------------------------------------- helpers

    fn argv(cmd: &Command) -> Vec<String> {
        std::iter::once(cmd.get_program())
            .chain(cmd.get_args())
            .map(|a| a.to_string_lossy().into_owned())
            .collect()
    }

    fn cwd(cmd: &Command) -> Option<PathBuf> {
        cmd.get_current_dir().map(PathBuf::from)
    }

    fn parse(line: &str) -> Value {
        serde_json::from_str(line).expect("reply must be valid JSON")
    }

    fn handle_request(method: &str, id: Value, params: Value) -> Option<Value> {
        handle_line(
            &json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}).to_string(),
        )
        .map(|l| parse(&l))
    }

    fn call(name: &str, arguments: Value) -> Value {
        handle_request(
            "tools/call",
            json!(7),
            json!({"name": name, "arguments": arguments}),
        )
        .expect("tools/call always replies")
    }

    fn result_text(resp: &Value) -> &str {
        resp["result"]["content"][0]["text"]
            .as_str()
            .expect("text content")
    }

    fn is_error(resp: &Value) -> bool {
        resp["result"]["isError"].as_bool().unwrap()
    }

    // ------------------------------------------------- protocol surface

    #[test]
    fn parse_error_replies_with_null_id_and_code_32700() {
        let resp = handle_line("this is not json").unwrap();
        let v = parse(&resp);
        assert_eq!(v["jsonrpc"], "2.0");
        assert!(v["id"].is_null());
        assert_eq!(v["error"]["code"], -32700);
        assert!(v["error"]["message"]
            .as_str()
            .unwrap()
            .starts_with("parse error:"));
    }

    #[test]
    fn notifications_get_no_reply() {
        // No id at all, and an explicit null id: both are notifications
        // under JSON-RPC 2.0 §4.1 and must produce no line.
        assert!(handle_line(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#).is_none());
        assert!(
            handle_line(r#"{"jsonrpc":"2.0","method":"notifications/initialized","id":null}"#)
                .is_none()
        );
        assert!(handle_line(r#"{"jsonrpc":"2.0","method":"ping"}"#).is_none());
    }

    #[test]
    fn initialize_echoes_client_protocol_version() {
        let resp = handle_request(
            "initialize",
            json!(1),
            json!({"protocolVersion": "2025-06-18", "clientInfo": {"name": "t"}}),
        )
        .unwrap();
        assert_eq!(resp["result"]["protocolVersion"], "2025-06-18");
        assert_eq!(resp["result"]["serverInfo"]["name"], SERVER_NAME);
        assert_eq!(resp["result"]["serverInfo"]["version"], SERVER_VERSION);
        assert_eq!(
            resp["result"]["capabilities"]["tools"]["listChanged"],
            false
        );
        assert_eq!(resp["id"], 1);
    }

    #[test]
    fn initialize_defaults_protocol_version_when_absent_or_empty() {
        for params in [json!({}), json!({"protocolVersion": ""}), Value::Null] {
            let params = if params.is_null() { json!({}) } else { params };
            let resp = handle_request("initialize", json!(2), params).unwrap();
            assert_eq!(resp["result"]["protocolVersion"], PROTOCOL_VERSION);
        }
    }

    #[test]
    fn ping_replies_with_empty_result() {
        let resp = handle_request("ping", json!("ping-id"), json!({})).unwrap();
        assert_eq!(resp["id"], "ping-id");
        let result = resp["result"].clone();
        assert_eq!(result, json!({}));
    }

    #[test]
    fn unknown_method_reports_32601() {
        let resp = handle_request("resources/read", json!(9), json!({})).unwrap();
        assert_eq!(resp["error"]["code"], -32601);
        assert_eq!(resp["error"]["message"], "method not found: resources/read");
        assert_eq!(resp["id"], 9);
    }

    #[test]
    fn tools_list_returns_the_full_table() {
        let resp = handle_request("tools/list", json!(3), json!({})).unwrap();
        let tools = resp["result"]["tools"].as_array().unwrap();
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        assert_eq!(
            names,
            [
                "sheaf_status",
                "sheaf_log",
                "sheaf_diff",
                "sheaf_checkpoint_list",
                "sheaf_checkpoint_create",
                "sheaf_restore_plan",
                "sheaf_restore_apply",
                "sheaf_worktree_list",
                "sheaf_worktree_add",
                "sheaf_merge_plan",
                "sheaf_merge_apply",

                "sheaf_doctor",
                "sheaf_gc",
                "sheaf_init",
            ]
        );
        for t in tools {
            assert_eq!(t["inputSchema"]["type"], "object");
            assert!(t["inputSchema"]["properties"].is_object());
            assert!(t["description"].as_str().unwrap().len() > 10);
        }
    }

    #[test]
    fn required_arguments_match_the_schema_contract() {
        let resp = handle_request("tools/list", json!(3), json!({})).unwrap();
        let tools = resp["result"]["tools"].as_array().unwrap();
        let required = |name: &str| -> Vec<String> {
            tools.iter().find(|t| t["name"] == name).unwrap()["inputSchema"]["required"]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_owned())
                .collect()
        };
        assert!(required("sheaf_status").is_empty());
        assert!(required("sheaf_diff").is_empty());
        assert_eq!(required("sheaf_checkpoint_create"), ["name"]);
        assert_eq!(required("sheaf_restore_plan"), ["point"]);
        assert_eq!(required("sheaf_restore_apply"), ["point"]);
        assert_eq!(required("sheaf_worktree_add"), ["point", "destination"]);
        assert_eq!(required("sheaf_merge_plan"), ["source"]);
        assert_eq!(required("sheaf_merge_apply"), ["source"]);

        assert_eq!(required("sheaf_init"), ["path"]);
    }

    #[test]
    fn write_gated_tools_document_the_gate_in_their_description() {
        for name in [
            "sheaf_restore_apply",
            "sheaf_worktree_add",
            "sheaf_merge_apply",
            "sheaf_gc",
            "sheaf_init",
        ] {

            let resp = handle_request("tools/list", json!(3), json!({})).unwrap();
            let desc = resp["result"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .find(|t| t["name"] == name)
                .unwrap()["description"]
                .as_str()
                .unwrap();
            assert!(
                desc.contains("SHEAF_MCP_ALLOW_WRITE"),
                "{name} undocumented gate"
            );
        }
    }

    // ---------------------------------------------------- arg readers

    #[test]
    fn bool_arg_accepts_native_and_string_forms() {
        let cases: Vec<(Value, bool)> = vec![
            (json!({"k": true}), true),
            (json!({"k": false}), false),
            (json!({"k": "true"}), true),
            (json!({"k": "1"}), true),
            (json!({"k": "yes"}), true),
            (json!({"k": "True"}), false),
            (json!({"k": "false"}), false),
            (json!({"k": "0"}), false),
            (json!({"k": ""}), false),
            (json!({"k": 1}), false),
            (json!({}), false),
        ];
        for (args, want) in cases {
            assert_eq!(bool_arg(&args, "k"), want, "case {args}");
        }
    }

    #[test]
    fn str_arg_filters_missing_empty_and_non_strings() {
        assert_eq!(str_arg(&json!({"k": "v"}), "k"), Some("v"));
        assert_eq!(str_arg(&json!({"k": ""}), "k"), None);
        assert_eq!(str_arg(&json!({}), "k"), None);
        assert_eq!(str_arg(&json!({"k": 3}), "k"), None);
    }

    #[test]
    fn str_vec_keeps_only_strings_and_defaults_to_empty() {
        assert_eq!(
            str_vec(&json!({"k": ["a", "b"]}), "k"),
            vec!["a".to_owned(), "b".to_owned()]
        );
        // Mixed arrays drop non-strings rather than failing the call.
        assert_eq!(
            str_vec(&json!({"k": ["a", 1, null]}), "k"),
            vec!["a".to_owned()]
        );
        assert_eq!(str_vec(&json!({"k": []}), "k"), Vec::<String>::new());
        assert_eq!(str_vec(&json!({"k": "a"}), "k"), Vec::<String>::new());
        assert_eq!(str_vec(&json!({}), "k"), Vec::<String>::new());
    }

    #[test]
    fn pick_protocol_only_accepts_non_empty_strings() {
        assert_eq!(pick_protocol(Some(&json!({"protocolVersion": "x"}))), "x");
        assert_eq!(
            pick_protocol(Some(&json!({"protocolVersion": ""}))),
            PROTOCOL_VERSION
        );
        assert_eq!(
            pick_protocol(Some(&json!({"protocolVersion": 5}))),
            PROTOCOL_VERSION
        );
        assert_eq!(pick_protocol(Some(&json!({}))), PROTOCOL_VERSION);
        assert_eq!(pick_protocol(None), PROTOCOL_VERSION);
    }

    // ------------------------------------ environment-injected policies

    #[test]
    fn project_resolution_prefers_argument_then_env_then_none() {
        let with_arg = json!({"project": "/a"});
        let empty_arg = json!({"project": ""});
        let no_arg = json!({});
        // Explicit argument wins over any environment value.
        assert_eq!(
            project_from(&with_arg, Some("/env".to_owned())),
            Some("/a".to_owned())
        );
        // An empty argument falls through to the environment.
        assert_eq!(
            project_from(&empty_arg, Some("/env".to_owned())),
            Some("/env".to_owned())
        );
        // Empty environment values are ignored, exactly like a missing one.
        assert_eq!(project_from(&no_arg, Some(String::new())), None);
        assert_eq!(project_from(&no_arg, None), None);
    }

    #[test]
    fn allow_write_accepts_only_affirmative_values() {
        for on in ["1", "true", "yes"] {
            assert!(allow_write_from(Some(on.to_owned())), "{on}");
        }
        for off in [
            None,
            Some(String::new()),
            Some("0".into()),
            Some("false".into()),
            Some("no".into()),
            Some("YES".into()),
            Some("on".into()),
        ] {
            assert!(!allow_write_from(off.clone()), "must stay denied: {off:?}");
        }
    }

    #[test]
    fn write_gate_covers_restore_apply_init_and_collecting_gc() {
        let plain = json!({});
        let gc_report = json!({"apply": false});
        let gc_collect = json!({"apply": true});
        let gc_collect_str = json!({"apply": "true"});

        assert!(!write_gated("sheaf_status", &plain));
        assert!(!write_gated("sheaf_log", &plain));
        assert!(!write_gated("sheaf_diff", &plain));
        assert!(!write_gated("sheaf_checkpoint_list", &plain));
        // Annotation-only tools are not gated: they never rewrite anything.
        assert!(!write_gated("sheaf_checkpoint_create", &plain));
        assert!(!write_gated("sheaf_restore_plan", &plain));
        assert!(!write_gated("sheaf_worktree_list", &plain));
        assert!(!write_gated("sheaf_merge_plan", &plain));

        assert!(!write_gated("sheaf_doctor", &plain));
        assert!(!write_gated("sheaf_gc", &gc_report));
        assert!(!write_gated("made-up tool", &plain));

        assert!(write_gated("sheaf_restore_apply", &plain));
        assert!(write_gated("sheaf_init", &plain));
        assert!(write_gated("sheaf_worktree_add", &plain));
        assert!(write_gated("sheaf_merge_apply", &plain));

        assert!(write_gated("sheaf_gc", &gc_collect));
        // String-typed booleans gate too — clients mis-type arguments.
        assert!(write_gated("sheaf_gc", &gc_collect_str));
    }

    // --------------------------------------------- result constructors

    #[test]
    fn text_result_passes_short_text_through() {
        let v = text_result("hello".to_owned(), false);
        assert_eq!(v["content"][0]["type"], "text");
        assert_eq!(v["content"][0]["text"], "hello");
        assert_eq!(v["isError"], false);
        let v = text_result("boom".to_owned(), true);
        assert_eq!(v["isError"], true);
    }

    #[test]
    fn text_result_truncates_by_characters_not_bytes() {
        // 200_001 chars of 'é' (2 bytes each): the head must keep exactly
        // MAX_OUTPUT_CHARS characters, and the marker goes on a new line.
        let giant = "é".repeat(MAX_OUTPUT_CHARS + 1);
        let v = text_result(giant, false);
        let text = v["content"][0]["text"].as_str().unwrap();
        let head = "é".repeat(MAX_OUTPUT_CHARS);
        assert!(text.starts_with(&head));
        assert!(text.ends_with("\n… [truncated by sheaf-mcp]"));
        // 2 header chars + 200_000 kept chars.
        assert_eq!(
            text.chars().count(),
            MAX_OUTPUT_CHARS + "\n… [truncated by sheaf-mcp]".chars().count()
        );
    }

    #[test]
    fn error_result_marks_is_error() {
        let v = error_result("missing params");
        assert_eq!(v["content"][0]["text"], "missing params");
        assert_eq!(v["isError"], true);
    }

    #[test]
    fn call_tool_without_params_reports_missing() {
        // params key absent entirely (not just null): tools/call with no
        // object to read a tool name from.
        let resp = handle_line(r#"{"jsonrpc":"2.0","id":5,"method":"tools/call"}"#).unwrap();
        let v = parse(&resp);
        assert_eq!(v["result"]["content"][0]["text"], "missing params");
        assert_eq!(v["result"]["isError"], true);
    }

    #[test]
    fn call_tool_with_arguments_object_missing_still_runs() {
        // No "arguments" key: the tool runs with an empty argument object.
        let resp = call("sheaf_nope", json!(null));
        assert!(is_error(&resp));
        assert_eq!(result_text(&resp), "unknown tool: sheaf_nope");
    }

    // ------------------------------------------------- argv construction

    const BIN: &str = "/fake/sheaf";

    fn build(name: &str, arguments: Value, project: Option<&str>) -> anyhow::Result<Command> {
        build_command(BIN, name, &arguments, project)
    }

    #[test]
    fn status_passes_root_positionally_and_as_cwd() {
        let cmd = build("sheaf_status", json!({"project": "/p"}), Some("/p")).unwrap();
        assert_eq!(argv(&cmd), [BIN, "status", "/p"]);
        assert_eq!(cwd(&cmd), Some(PathBuf::from("/p")));

        // Without a project: bare status, no working directory override.
        let cmd = build("sheaf_status", json!({}), None).unwrap();
        assert_eq!(argv(&cmd), [BIN, "status"]);
        assert_eq!(cwd(&cmd), None);
    }

    #[test]
    fn log_flags_map_one_to_one_with_limit_capped_at_1000() {
        let cmd = build(
            "sheaf_log",
            json!({"project": "/p", "path": "src/lib.rs", "follow": true, "all": true,
                   "before": "abc123", "limit": 5000}),
            Some("/p"),
        )
        .unwrap();
        assert_eq!(
            argv(&cmd),
            [
                BIN,
                "log",
                "--json",
                "-C",
                "/p",
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
        // A sane limit passes through untouched; absent limit adds nothing.
        let cmd = build("sheaf_log", json!({"limit": 50}), None).unwrap();
        assert_eq!(argv(&cmd), [BIN, "log", "--json", "--limit", "50"]);
        let cmd = build("sheaf_log", json!({}), None).unwrap();
        assert_eq!(argv(&cmd), [BIN, "log", "--json"]);
    }

    #[test]
    fn diff_builds_span_point_and_respects_stat_flag() {
        let cmd = build(
            "sheaf_diff",
            json!({"from": "@~5", "to": "@~1", "paths": ["a.rs", "b/c.rs"]}),
            Some("/p"),
        )
        .unwrap();
        assert_eq!(
            argv(&cmd),
            [BIN, "diff", "@~5..@~1", "--json", "--path", "a.rs", "--path", "b/c.rs", "-C", "/p"]
        );

        // from only: compare a point against the live worktree.
        let cmd = build("sheaf_diff", json!({"from": "a1b2c3"}), None).unwrap();
        assert_eq!(argv(&cmd), [BIN, "diff", "a1b2c3", "--json"]);
        // to only: implicitly from @.
        let cmd = build("sheaf_diff", json!({"to": "checkpoint:x"}), None).unwrap();
        assert_eq!(argv(&cmd), [BIN, "diff", "@..checkpoint:x", "--json"]);
        // Neither: uncaptured worktree edits vs the last capture.
        let cmd = build("sheaf_diff", json!({}), None).unwrap();
        assert_eq!(argv(&cmd), [BIN, "diff", "--json"]);
        // stat swaps the format flag.
        let cmd = build("sheaf_diff", json!({"stat": true}), None).unwrap();
        assert_eq!(argv(&cmd), [BIN, "diff", "--stat"]);
    }

    #[test]
    fn checkpoint_list_and_create_map_to_the_subcommands() {
        let cmd = build(
            "sheaf_checkpoint_list",
            json!({"project": "/p"}),
            Some("/p"),
        )
        .unwrap();
        assert_eq!(
            argv(&cmd),
            [BIN, "checkpoint", "list", "--json", "-C", "/p"]
        );

        let cmd = build(
            "sheaf_checkpoint_create",
            json!({"name": "before-rework", "at": "@~3", "project": "/p"}),
            Some("/p"),
        )
        .unwrap();
        assert_eq!(
            argv(&cmd),
            [
                BIN,
                "checkpoint",
                "create",
                "before-rework",
                "--at",
                "@~3",
                "-C",
                "/p"
            ]
        );

        // Without `at` the CLI default (@) is used.
        let cmd = build("sheaf_checkpoint_create", json!({"name": "n"}), None).unwrap();
        assert_eq!(argv(&cmd), [BIN, "checkpoint", "create", "n"]);
    }

    #[test]
    fn checkpoint_create_without_name_is_a_local_error() {
        let err = build("sheaf_checkpoint_create", json!({}), None).unwrap_err();
        assert!(err.to_string().contains("needs a name"));
    }

    #[test]
    fn restore_plan_adds_dry_run_and_scopes_are_positional() {
        let cmd = build(
            "sheaf_restore_plan",
            json!({"point": "checkpoint:before-x", "paths": ["src/a.rs"]}),
            Some("/p"),
        )
        .unwrap();
        assert_eq!(
            argv(&cmd),
            [
                BIN,
                "restore",
                "--at",
                "checkpoint:before-x",
                "--dry-run",
                "src/a.rs",
                "-C",
                "/p"
            ]
        );
        // Apply is identical minus --dry-run.
        let cmd = build(
            "sheaf_restore_apply",
            json!({"point": "@~2", "paths": ["src/a.rs", "docs"]}),
            None,
        )
        .unwrap();
        assert_eq!(
            argv(&cmd),
            [BIN, "restore", "--at", "@~2", "src/a.rs", "docs"]
        );
    }

    #[test]
    fn restore_without_point_is_a_local_error_naming_the_tool() {
        for name in ["sheaf_restore_plan", "sheaf_restore_apply"] {
            let err = build(name, json!({}), None).unwrap_err();
            assert!(err.to_string().contains(name), "{err}");
            assert!(err.to_string().contains("timeline point"));
        }
    }

    #[test]
    fn worktree_and_merge_tools_map_to_the_subcommands() {
        let cmd = build("sheaf_worktree_list", json!({"project": "/p"}), Some("/p")).unwrap();
        assert_eq!(argv(&cmd), [BIN, "worktree", "list", "--json", "-C", "/p"]);

        let cmd = build(
            "sheaf_worktree_add",
            json!({"point": "checkpoint:x", "destination": "/w", "project": "/p"}),
            Some("/p"),
        )
        .unwrap();
        assert_eq!(
            argv(&cmd),
            [BIN, "worktree", "add", "checkpoint:x", "/w", "--json", "-C", "/p"]
        );

        let cmd = build("sheaf_merge_plan", json!({"source": "checkpoint:x"}), None).unwrap();
        assert_eq!(argv(&cmd), [BIN, "merge", "checkpoint:x", "--json"]);

        let cmd = build(
            "sheaf_merge_apply",
            json!({"source": "@~1", "project": "/p"}),
            Some("/p"),
        )
        .unwrap();
        assert_eq!(argv(&cmd), [BIN, "merge", "@~1", "--apply", "--json", "-C", "/p"]);
    }

    #[test]
    fn worktree_and_merge_tools_require_their_references() {
        assert!(build("sheaf_worktree_add", json!({"point": "@"}), None)
            .unwrap_err()
            .to_string()
            .contains("destination"));
        assert!(build("sheaf_worktree_add", json!({"destination": "/w"}), None)
            .unwrap_err()
            .to_string()
            .contains("timeline point"));
        for name in ["sheaf_merge_plan", "sheaf_merge_apply"] {
            let err = build(name, json!({}), None).unwrap_err();
            assert!(err.to_string().contains(name), "{err}");
            assert!(err.to_string().contains("source"));
        }
    }

    #[test]
    fn doctor_and_gc_flag_shapes() {
        let cmd = build("sheaf_doctor", json!({"project": "/p"}), Some("/p")).unwrap();
        assert_eq!(argv(&cmd), [BIN, "doctor", "--json", "-C", "/p"]);

        let cmd = build("sheaf_gc", json!({"project": "/p"}), Some("/p")).unwrap();
        assert_eq!(argv(&cmd), [BIN, "gc", "--json", "-C", "/p"]);
        let cmd = build("sheaf_gc", json!({"apply": true}), None).unwrap();
        assert_eq!(argv(&cmd), [BIN, "gc", "--json", "--apply"]);
    }

    #[test]
    fn init_uses_explicit_path_then_falls_back_to_project() {
        let cmd = build("sheaf_init", json!({"path": "/x"}), Some("/p")).unwrap();
        assert_eq!(argv(&cmd), [BIN, "init", "/x"]);

        let cmd = build("sheaf_init", json!({}), Some("/p")).unwrap();
        assert_eq!(argv(&cmd), [BIN, "init", "/p"]);

        let err = build("sheaf_init", json!({}), None).unwrap_err();
        assert!(err.to_string().contains("needs a path"));
    }

    #[test]
    fn unknown_tool_is_rejected_before_any_spawn() {
        let err = build("sheaf_frobnicate", json!({}), None).unwrap_err();
        assert_eq!(err.to_string(), "unknown tool: sheaf_frobnicate");
    }

    // ---------------------------------------------------- subprocess run

    #[test]
    fn run_with_timeout_combines_stdout_and_stderr() {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "echo out-line; echo err-line >&2"]);
        let text = run_with_timeout(cmd).unwrap();
        // stdout keeps its trailing newline; the stderr separator then adds
        // one more before the joined diagnostics.
        assert_eq!(text, "out-line\n\nerr-line");
    }

    #[test]
    fn run_with_timeout_places_placeholder_for_silent_success() {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "exit 0"]);
        assert_eq!(run_with_timeout(cmd).unwrap(), "(ok, no output)");
    }

    #[test]
    fn run_with_timeout_surfaces_exit_status_and_diagnostics() {
        let mut cmd = Command::new("sh");
        cmd.args(["-c", "echo diagnostics >&2; exit 3"]);
        let err = run_with_timeout(cmd).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("sheaf exited with exit status: 3"), "{msg}");
        assert!(msg.contains("diagnostics"), "{msg}");
    }

    #[test]
    fn run_with_timeout_reports_spawn_failures() {
        let cmd = Command::new("/nonexistent/sheaf-binary-for-tests");
        let err = run_with_timeout(cmd).unwrap_err();
        assert!(format!("{err:#}").contains("spawn"), "{err}");
    }

    // ----------------------------------------------------- session shape

    #[test]
    fn a_session_moves_through_initialize_notification_and_list() {
        let init = handle_request(
            "initialize",
            json!(0),
            json!({"protocolVersion": "2024-11-05"}),
        )
        .unwrap();
        assert_eq!(init["result"]["protocolVersion"], "2024-11-05");
        // The client's initialized notification is silently consumed.
        assert!(handle_line(r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#).is_none());
        let list = handle_request("tools/list", json!(1), json!({})).unwrap();
        assert_eq!(list["result"]["tools"].as_array().unwrap().len(), 14);
    }
}
