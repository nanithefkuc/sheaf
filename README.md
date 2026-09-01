# Sheaf

> Ctrl-Z anytime, anywhere

Sheaf is a CRDT-based, locally persistent change logger for your projects.

Think of it as a **fine-grained flight recorder for your code.**

Initialize Sheaf in a project, and it continuously records changes as you work.
Made a mistake? Restore a previous point in time. Changed your mind after
restoring? Keep editing.

Nothing is implicitly overwritten.

Your history branches automatically, allowing you to move between different
versions of your work without manually creating branches, commits, or snapshots.

Sheaf works alongside Git, but does not require it.

## Why Sheaf?

Most development workflows have a gap.

Your editor's undo history is:
- Local to the editor
- Usually lost when the editor closes
- Often limited in size
- Unavailable from another editor

Git history is:
- Explicit
- Commit-based
- Too coarse-grained for "I accidentally changed this 20 minutes ago"

Sheaf fills the space in between.

Sheaf continuously records what happens to your project so you can move backward
and forward through your work at any time.

## Features
- **Persistent undo** - restore previous states long after closing your editor.
- **Automatic timeline branching** - restoring an old state and making changes 
  never destroys future history
- **Editor-independent history** - edit in one editor and restore from another.
- **Works without editor integrations** - watches filesystem changes
  automatically.
- **Editor-level history** *(planned)* - integrations will record
  fine-grained editing operations before they are flushed to disk.
- **CRDT-based history** - designed around durable, mergeable, Loro-based
  change operations.
- **Git-compatible** - use Sheaf alongside Git without changing your workflow.
- **Squash** - collapse a range of edits into a commit-sized change; previewed
  by default, committed only through an explicit `--` passthrough.
- **Branch worktrees** - materialize any timeline point as a live linked
  worktree that shares the store, turning a divergent branch into a real
  directory you can build and edit in.
- **Timeline merge** - squash a divergent timeline onto your current worktree
  as one capture; conflicting paths are reported and block the merge until
  resolved.
- **Checkpoints** - create meaningful restore points for branching or 
  semantically indicating a new phase.
- **Respects ignore rules** - anything git ignores — `.gitignore` files,
  `.git/info/exclude`, and the global git ignore — is automatically ignored
  by Sheaf too, and rule edits apply without restarting the daemon.

## Requirements

Sheaf currently supports Linux and requires Rust 1.82 or newer. Filesystem
capture uses inotify; macOS and Windows support is planned.

## Installation

Clone [the repository](https://github.com/nanithefkuc/sheaf), then use
`install.sh`:
```sh
./install.sh
```

## Quick Start

Ensure the `sheafd` daemon is online and running:
```sh
sheaf status
```

> [!NOTE]
> If the status shows the daemon as offline/unreachable, you can start the
> daemon by hand with `sheafd run`.
>
> Optionally, you can run the daemon as a systemd user service by running
> `sheaf service install`

Initialize Sheaf inside a project:
```sh
cd my-project
sheaf init
```

## Agent integration (MCP)

`install.sh` installs `sheaf-mcp` alongside `sheaf` and `sheafd`. The MCP
protocol standardizes communication with a server, but not a configuration
file name or discovery location shared by every client. Clients also do not
discover local servers by scanning `$PATH`.

This repository therefore keeps the durable, client-neutral server definition
in the project-root [`mcp.json`](mcp.json):

```json
{
  "mcpServers": {
    "sheaf": {
      "command": "sheaf-mcp"
    }
  }
}
```

Use the root file directly when the client supports project `mcp.json`
discovery. Otherwise, import or copy the `sheaf` entry into that client's
documented **project-level** MCP configuration. Keep `command` as `sheaf-mcp`
when the installed binary is on the harness process's `$PATH`; use an absolute
path when it is not. No special installation directory is required. Reload or
reconnect MCP servers in the client after changing the definition.

The server uses stdio and resolves the project from its working directory by
default. It exposes status, log, diff, checkpoint, restore planning, worktree
materialization, timeline merge, doctor, and retention tools. Worktree/store
rewrites are denied by default; clients
that need them must explicitly add `SHEAF_MCP_ALLOW_WRITE=1` to the server's
environment.

## Checking Changes

Browse your edit history:
```sh
# Browse the edit history
sheaf log

# Browse with colors in your terminal
sheaf log --color=always # force color (auto detects a tty)
```

Browse edits to a specific file:
```sh
# Browse edits on this file/directory
sheaf log --path src/lib.rs

# Browse edits to this file including file renaming
sheaf log --path src/lib.rs --follow
```

Get more information on what was exactly edited:
```sh
# Find out what actually happened at this recorded time
sheaf info 1a2b3c4d
```

## Restore Anything

Use `restore` to move your project to a previous point in its history.
```sh
# Restore to a file-level change with the short SHA256
sheaf restore 1a2b3c4d

# Restore to a relative number of changes ago
# `@` is the current HEAD of the timeline branch
sheaf restore @~10               # 10 changes ago

# Restore to a specific timestamp
sheaf restore @~2h               # 2 hours ago
sheaf restore '10:30'            # Specific time today
sheaf restore '2026-08-27T10:30' # Specific date and time

# Restore to a checkpoint with the checkpoint label
sheaf restore "pre-change"
```

## Checkpointing

Sometimes you want a meaningful point in history to reference. Create a 
checkpoint:
```sh
# Create a checkpoint with a label
sheaf checkpoint "before refactoring"

# List your checkpoints
sheaf checkpoint list
```

Check points are useful when:
- Starting a large refactor
- Trying an experimental implementation
- Handing work off to another tool or agent
- Before making a risky change
- Marking a meaningful milestone

Unlike a traditional commit, a checkpoint does not imply that your work is
complete or ready to share. It is simply a point that you may want to return to.

## Squashing Changes into Commits

Sheaf records changes at a much finer granularity than a typical Git commit.
`squash` turns a range of those edits into a commit-sized change.

```sh
# Preview what a commit would collapse: the diff plus a drafted commit
# message. Read-only — no git commands run, nothing is written.
sheaf squash                            # last commit frame → worktree
sheaf squash @~12                       # from 12 captures back
sheaf squash "before refactoring"       # from a checkpoint
sheaf squash @~3..@~1                   # any span, point to point

# Commit the span: everything after `--` is forwarded to `git commit`.
sheaf squash @~12 -- -m "Refactor the parser"

# Without a -m, git's editor opens with the drafted message as a template.
sheaf squash --
```

Every squash commit is stamped: sheaf records a `git-<sha>` checkpoint and
appends a frame to `.sheaf/frames.jsonl` pairing the commit with the exact
span of captures it collapsed. The next squash anchors there automatically,
so the worktree, git history, and the sheaf timeline stay in agreement.

## Branch Worktrees

Timelines branch automatically, but sometimes you want a divergent branch as a
real directory you can build and edit in — without disturbing your main
worktree. `worktree add` materializes any timeline point as a live linked
worktree that shares the same Sheaf store.

```sh
# List the primary worktree and every linked one, with each timeline tip
sheaf worktree list

# Materialize a checkpoint (or capture / branch tip) as a new worktree
sheaf worktree add "before refactoring" ../my-project-experiment
```

The daemon watches every linked worktree the same way it watches the primary,
so edits in either are captured on their own head. Each worktree diverges
independently; nothing is copied and nothing is overwritten.

## Merging Timelines

When work on a divergent branch — a linked worktree, a checkpoint, or any
capture — is ready, `merge` squashes that source onto your current worktree as
a single capture.

```sh
# Preview the squash: the files it would write and any conflicts. Read-only.
sheaf merge "before refactoring"

# Apply it as one capture on the current worktree.
sheaf merge "before refactoring" --apply
```

The merge is previewed by default. Paths that both sides changed divergently
are reported as conflicts and block the apply until resolved — the worktree is
never left half-merged. If a merge is interrupted, finish it with:

```sh
sheaf merge --resume
```

Merging never rewrites history: the source branch stays reachable, and the
merge lands as a new capture stamped with a merge origin.

## Maintenance

Persisting so many changes around can bloat up your storage. Manage it with
the garbage collector.
```sh
sheaf gc           # reports collectable bytes (orphans, covered segments, etc.)
sheaf gc --collect # actually deletes marked bytes to free up space

# Set an automatic expiry for the edits in the project
sheaf gc --set-expiry 30d # Edits expire after 30 days.
```

Automatic expiry never removes anything a restore to **any** timeline point
could still need, including divergent branches. Only an explicit mark
bypasses that protection — if you need to manually mark some edits as
collectable:
```sh
sheaf gc 1a2b3c4d # other methods of referencing edits also work
```

Check the integrity of the daemon, helper and storage as a read-only sweep:
```sh
sheaf doctor       # read-only integrity sweep; exit 5 on failure
sheaf doctor --fix # fixes whatever errors it can
```

## Development

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
./scripts/coverage.sh --summary-only
```

The project maintains a minimum 95% line-coverage target.

## License

Sheaf is available under the [MIT License](LICENSE).

## Editor Integrations

Sheaf works without editor integrations.

In its basic mode, the Sheaf daemon watches the filesystem and records changes
when applications write them to disk. This already provides persistent, 
project-level history.

Editor integrations — recording editing operations directly, before they are
flushed to disk — are planned. Editor support guides and an integration
development guide will land alongside them.

## Roadmap

Sheaf is intended to become more than just a local undo system. Our roadmap
includes:
- **Editor integrations:** Record fine-grained editing operations directly
  in editors, before they are flushed to disk.
- **A more intuitive branch viewer:** The current timeline viewer makes it
  hard to see where the timeline branches and how parallel branches run
  alongside each other; a dedicated branching view is planned.
- **Agentic Integration:** Allow AI agents to use Sheaf; restore changes
  within an agent turn.
- **Platform Support:** Future support for MacOS and Windows.

<sub>
    *macOS and Windows support are future work. On Linux, ignore rules
    (`.gitignore`, `.git/info/exclude`, and git's global ignore, plus
    Sheaf's own patterns) apply to what gets recorded.
</sub>
