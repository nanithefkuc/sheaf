---
name: sheaf
description: >
  Use in any Sheaf-enrolled project (a worktree with `.sheaf/`). Sheaf records
  edits beneath Git. Consult this skill before modifying the tree, restoring
  history, or committing; and whenever the user mentions Sheaf, checkpoints,
  timeline history, undo, “what changed,” recovering moved or deleted text,
  or committing only part of a dirty worktree. It covers health checks,
  checkpoints, history search, non-destructive restore, squash, and retention.
---

# Sheaf — the worktree flight recorder

Sheaf continuously records worktree changes on an append-only timeline. Git
remains the sharing boundary; `.sheaf/` is local state and must never be
committed.

## Begin safely

At the start of work in an enrolled project:

1. Run `sheaf status`. Verify that the daemon is running and watching the
   project.
2. Run `sheaf doctor`. Stop and report any failed integrity check.
3. Restore the daemon before ordinary work if it is unavailable. Work offline
   only when explicitly testing degraded daemon behavior.

Prefer native `sheaf_*` tools when available; otherwise use the CLI.

## Checkpoint before modifying the tree

Create a checkpoint before every modification work unit, including edits,
formatters, generators, dependency commands, scripts, renames, and handoffs to
another tool or agent:

```sh
sheaf checkpoint create before-parser-rework
```

Checkpoints are immutable names for existing timeline points. They are instant,
copy no data, and cannot be placed retroactively at the moment they become
useful. Name the intent (`before-parser-rework`), not the date (`aug28`). Place
a new checkpoint when the work changes direction.

Do not use `git stash`, `.bak` copies, or throwaway branches merely to protect
work in an enrolled project. Those recreate a safety layer Sheaf already
provides while adding Git state that must later be reconciled.

### Review or rewind the work unit

```sh
sheaf diff checkpoint:before-parser-rework --stat
sheaf restore --dry-run checkpoint:before-parser-rework
sheaf restore checkpoint:before-parser-rework
```

Always read a restore plan before applying it. A restore first preserves the
current state; abandoned futures remain reachable through `sheaf log --all`.
After restoring, either commit the restored state or restore forward. Never run
`git checkout .` or `git restore .` as cleanup.

## If the checkpoint reflex was missed

Nothing is lost, but the boundary must be found:

```sh
sheaf log --path src/parser.rs --follow
sheaf diff "2 hours ago" --stat
sheaf diff @~40 --stat
sheaf checkpoint create found-before-parser-work --at <capture-id>
```

Narrow by path and wall-clock time before guessing capture counts. Once the
boundary is identified, name it so the restore is repeatable.

## Timeline points

| Form | Meaning |
|---|---|
| `a1b2c3` | capture-ID prefix |
| `before-thing` or `checkpoint:before-thing` | named checkpoint |
| `@` | latest captured state |
| `@~N` | N captures back |
| `@~2h` | relative duration back |
| `10:30`, `2026-08-27T10:30`, `"2 hours ago"` | wall-clock time |
| `A..B` | span used by diff and squash |

## Choose the right operation

- To inspect captures, diffs, renames, or text history, read
  [references/history-and-grep.md](references/history-and-grep.md). Read it
  before using `sheaf grep`: point discovery is the default, while lifecycle
  history requires `--history`.
- To restore a tree, path, or fragment, or to create a whole/partial Git commit,
  read [references/restore-and-squash.md](references/restore-and-squash.md).
- For daemon lifecycle, integrity, retention, cache maintenance, native tool
  surfaces, and stale binaries, read
  [references/ops-and-surfaces.md](references/ops-and-surfaces.md).

## Non-negotiable boundaries

- Never commit `.sheaf/`.
- Keep the daemon active during ordinary work.
- Check integrity and create a checkpoint before modifying the tree.
- Preview every restore and squash before sanctioning a write.
- Prefer `sheaf squash -- -m "subject"` over a bare `git commit` when finishing
  a captured work unit, so Git commits and timeline frames stay paired.
- Fragment operations rebind exactly and fail closed. Never force an ambiguous
  selection.
- Explicit retention marks can make history collectable. Confirm destructive
  retention actions with the user.
