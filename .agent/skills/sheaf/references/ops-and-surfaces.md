# Sheaf operations and tool surfaces

Use this reference for daemon lifecycle, integrity, retention, cache/storage
maintenance, installation drift, and agent tool adapters.

## Daemon and integrity

```sh
sheaf status
sheaf doctor
sheaf service status
sheaf service install
sheaf service remove
```

The daemon must be running for captures and timeline writes, including
checkpoint creation. Keep it active during ordinary work. Read operations can
open the store in degraded mode while the daemon is down, but use that mode
only when explicitly testing or recovering offline behavior.

Run `sheaf doctor` at the start of an agent turn and before publication or
retention work. Stop on integrity failure. A pending restore intent is operator
state to resume or abandon, not corruption to delete.

## Retention

```sh
sheaf gc                         # read-only report
sheaf gc --apply                 # collect the approved plan
sheaf gc --set-expiry 30d
sheaf gc <timeline-reference>    # explicitly mark one point collectable
```

Automatic collection is reachability-constrained: it preserves data needed to
restore retained timeline points, checkpoints, branch tips, the current head,
and pending restore targets. Explicitly marking a point is the destructive
override; confirm that action with the user. Preview collection before applying
it.

## Branch worktrees

```sh
sheaf worktree list                       # primary + every linked worktree
sheaf worktree add <reference> <dir>      # materialize a point as a live worktree
```

A linked worktree shares the store and is watched like the primary; each head
diverges independently, and nothing is copied or overwritten.

## Ignore heavy and generated trees

Keep `.sheaf/`, `.git/`, dependency caches, build outputs, editor temporary
files, and vendored/generated trees ignored. A large dependency tree captured
as text can inflate store size and daemon memory. Project-specific patterns
belong in `.sheaf/config.toml`; `.gitignore` rules are also respected.

## Stale binary recovery

The CLI and daemon must be upgraded together. If a source-tree behavior or IPC
method differs from the installed tools:

1. Build `sheaf` and `sheafd` from the same revision.
2. Stop the daemon.
3. Replace both installed binaries.
4. Restart the daemon and rerun `sheaf status` and `sheaf doctor`.

Do this before debugging a protocol error as a storage defect.

## Native agent tools

When available, prefer structured native tools over shell commands:

- `sheaf_status`, `sheaf_doctor`
- `sheaf_log`, `sheaf_info`, `sheaf_diff`
- `sheaf_checkpoint_list`, `sheaf_checkpoint_create`
- `sheaf_restore_plan`, `sheaf_restore_apply`
- `sheaf_worktree_list`, `sheaf_worktree_add`

Timeline grep, cache commands, fragment selection, and smart squash may be
CLI-only on some adapters. Use the CLI when the native catalog lacks the
operation.

Restore apply, initialization, and garbage-collection apply are commonly
write-gated. Respect the default deny setting and request the operator's opt-in
rather than bypassing the adapter. Checkpoint creation is safe and should stay
available because it only names an existing point.

For MCP clients, `sheaf-mcp` exposes the structured surface over stdio. DSH can
load the repository's `dsh-sheaf` plugin. Configure project roots with generic,
portable paths or rely on nearest-enrolled-ancestor discovery; never publish a
developer's home-directory path.
