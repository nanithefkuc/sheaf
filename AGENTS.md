# AGENTS.md — working on `sheaf`

Sheaf records worktree changes on an append-only timeline beneath Git. Git is
the sharing and publication boundary; `.sheaf/` is local state and must never
be committed.

## Start every agent turn safely

Before inspecting or changing the repository:

1. Run `sheaf status` and verify that the project is enrolled, the daemon is
   running, and this worktree is being watched.
2. Run `sheaf doctor` and verify that every integrity check passes.
3. Stop and report the problem if integrity fails. If the daemon is inactive,
   restore it before continuing (`sheaf service status`, the installed user
   service, or `sheafd run`).

The daemon should remain active throughout normal work. The only exception is
work explicitly investigating degraded/offline daemon behavior; identify that
mode before proceeding and avoid unrelated tree changes while capture is
unavailable.

## Checkpoint before every modification

Create a named checkpoint **before any operation that can modify the
worktree**, not only before risky changes:

```sh
sheaf checkpoint create before-<intent>
```

This includes manual edits, formatters, code generators, dependency commands,
test commands that rewrite fixtures or snapshots, scripts, renames, and work
handed to another agent or tool. Name checkpoints after intent, such as
`before-parser-cleanup`, never after dates. If the work changes direction,
place another checkpoint before starting the new unit.

A checkpoint is an immutable name for an existing timeline point. It is cheap,
does not copy data, and cannot be added retroactively at the moment it would
have been most useful.

## Version-control boundaries

- Commit and publish with Git; use Sheaf to recover and understand work between
  commits.
- Never commit `.sheaf/`. Keep it ignored in `.gitignore` as well as local Git
  excludes.
- Do not use `git stash`, backup copies, or throwaway branches merely to protect
  edits. Create a Sheaf checkpoint instead.
- After a Sheaf restore, do not run `git checkout .` or `git restore .` to
  “clean up.” Either commit the restored state or explicitly restore forward.

## Timeline operations

```sh
sheaf log                              # captured history
sheaf log --path path/to/file --follow # rename-aware file history
sheaf diff                             # uncaptured worktree changes
sheaf diff checkpoint:<name> --stat    # review a work unit
sheaf info <capture>
sheaf checkpoint list
```

Common timeline points are a capture-ID prefix, `checkpoint:<name>`, `@` for
the latest capture, `@~N` for N captures back, and timestamps such as
`"2 hours ago"`.

Always preview a restore before applying it:

```sh
sheaf restore --dry-run checkpoint:<name>
sheaf restore checkpoint:<name>
```

Restores are non-destructive: Sheaf captures the pre-restore state and keeps
abandoned futures reachable through `sheaf log --all`.

## Building and verification

```sh
CARGO_HOME=$PWD/.cargo-home cargo build --release --workspace
cargo test --workspace
./scripts/coverage.sh --summary-only
```

Performance is a ratchet, not a snapshot. `python3 scripts/perf_gate.py`
runs a fixed workload against the release binaries and compares every
metric against the budgets in `scripts/perf-budgets.json`; CI runs it on
every push and a breach fails the build. After a change improves a
metric, rerun the gate with `--update` to tighten the window — it only
narrows. Widening a budget requires `--allow-widen` and must be justified
in the commit that does it.

Budgets bind on the slowest hardware that runs the gate — the CI runner,
not a dev laptop. Tighten from the measured table a CI run prints, via
`--set name=value` (measured × slack); a local `--update` can tighten
below what CI can hold and re-break it.

This project targets at least 95% line coverage. Add meaningful tests for new
behavior and do not lower the threshold to make a change pass.

Installed `sheaf` and `sheafd` binaries can become stale while developing. If
behavior differs from the source tree, rebuild both, replace the installed
binaries together, and restart the daemon before investigating further.
