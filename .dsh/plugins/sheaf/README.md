# `dsh-sheaf`

Native [sheaf](../../) integration for DeepSeek Harness (DSH). DSH has no MCP
support, so instead of `sheaf-mcp` this bundle wraps the `sheaf` CLI directly —
same adapter philosophy (CLI-wrapping, default-deny
writes) — and contributes three things:

1. **The Host service `sheaf`** — a pure-JSON, per-project API over the CLI.
   This is the stable contract future DSH UI reads from. Every method takes
   plain JSON and returns plain JSON (the same JSON shapes the CLI emits
   wherever the CLI emits them); nothing in a return value is live runtime
   data, so a browser half can be added later without touching the host
   surface.
2. **Model tools `sheaf_*`** — the sheaf-mcp tool list plus `sheaf_info`, as
   native DSH tools, so agents navigate the timeline without shelling out.
3. **Conditional context injection** — a `systemPrompt` section
   (`sheaf:guidance`) that renders **only for sessions whose workspace is
   sheaf-enrolled**: on session start (and lazily on first assembly) the
   plugin probes `test -d <session-cwd>/.sheaf` through the shell seam and
   caches the answer per agent. Non-enrolled sessions resolve to empty text,
   and the assembler omits empty sections entirely — zero noise. The text
   distills the sheaf skill's discipline (checkpoints before risky work,
   timeline point syntax, non-destructive restores, the write gate).

## Install (profile bundle)

```sh
dsh plugin --profile web add "link:$PWD/.dsh/plugins/sheaf"
# then restart the profile process so the bundle layer loads
```

`dsh plugin add` detects the `dsh.bundle` declaration in `package.json` and
applies `cordis.patch.yml`, which inserts one **host-plane** row. Uninstall
with `dsh plugin --profile web remove dsh-sheaf`, then restart.

Host plane is required, not a preference: the plugin publishes the `sheaf`
service, and a service with consumers outside the agent plane cannot move
into an agent preset. A loose preset row would publish it process-global and
be rejected at mount; an `isolate` realm would hide it from the host/UI
plane. There is deliberately no `agent.cordis.yml` alternative here.

## Config

| Key | Default | Meaning |
|---|---|---|
| `bin` | `sheaf` | CLI binary (mirrors `SHEAF_BIN`) |
| `allowWrite` | `false` | default-deny write gate |
| `injectPrompt` | `true` | conditional guidance section for enrolled sessions |
| `defaultProject` | — | Fallback root when a call names no project |
| `timeoutMs` | `60000` | Per-call timeout |
| `maxOutputChars` | `200000` | Output cap per result |

**Project resolution** (first match wins): the call's `project` argument →
the calling agent's session workspace (tools only, so the CLI's
nearest-enrolled-ancestor discovery applies) → `defaultProject` → error
`sheaf.no_project`.

**Write gate**: `sheaf_restore_apply`, `sheaf_init`, and `sheaf_gc` with
`apply: true` refuse with code `sheaf.gated` unless `allowWrite: true`.
Everything else — including restore *plans* and checkpoint creation — always
works, exactly as in sheaf-mcp.

## The `sheaf` service (future-UI contract)

`apiVersion: 1`. All methods async, JSON-in/JSON-out, `signal` accepted.

| Method | Returns |
|---|---|
| `overview({project?})` | one-call panel payload: status, 5 recent captures, checkpoints, uncaptured-diff summary; per-part failures land in `errors` |
| `status({project?})` | parsed daemon/store health (`{fields, enrolled, daemonRunning, watching, pendingRestore, raw}`) — parsed from human output until the CLI grows `status --json` |
| `log({project?, path?, follow?, all?, before?, limit?})` | `timeline.log` result (newest-first entries, tips) |
| `info({reference, project?})` | `timeline.info` result |
| `diff({project?, from?, to?, paths?, stat?})` | diff outcome + truncated patch |
| `checkpointList({project?})` | `checkpoint.list` result |
| `checkpointCreate({name, at?, project?})` | pin result |
| `restorePlan({at, paths?, project?})` | full dry-run plan (never touches the worktree) |
| `restoreApply({at, paths?, project?})` | restore outcome — **gated** |
| `doctor({project?})` | integrity report |
| `gc({apply?, project?})` | retention plan/report — apply **gated** |
| `init({path})` | enrollment result — **gated** |
| `run({args, project?, timeoutMs?})` | raw CLI escape hatch |

Failures reject with `SheafError` carrying a stable `code`
(`sheaf.gated`, `sheaf.no_project`, `sheaf.cli_error`, `sheaf.timeout`,
`sheaf.bad_json`, `sheaf.bad_params`) plus `exitCode`/`stderr`/`command`
when a CLI run failed.

### Adding UI later

The intended path: a client half (this bundle or a DSH-side package) that
renders from `sheaf.overview()` — it already aggregates everything a status
panel needs, degrades per-part, and never requires host changes. Transport
can then be Remote descriptors on this service, a `webServer` route, or a
package-private RPC — the API shape does not change, because it was designed
as plain JSON from day one.

## Tools

`sheaf_status`, `sheaf_log`, `sheaf_info`, `sheaf_diff`,
`sheaf_checkpoint_list`, `sheaf_checkpoint_create`, `sheaf_restore_plan`,
`sheaf_restore_apply` (gated), `sheaf_doctor`, `sheaf_gc` (apply gated),
`sheaf_init` (gated). Tool results are the JSON above; CLI failures surface
as tool errors with the combined stderr text.

## Development

```sh
cd .dsh/plugins/sheaf && npm test
```

`index.js` is dependency-free plain JavaScript: no imports, every `export` a
line prefix. That is what makes **dual-mount testing** possible — stripping
the `export ` prefixes and appending `return { name, inject, apply }` yields
a valid dynamic-plugin package body, so every change is verified live in a
running DSH session (via the `cordis_*` tools) before it ships in the
bundle. Keep that property: do not introduce imports, `require`, or
non-prefix exports.
