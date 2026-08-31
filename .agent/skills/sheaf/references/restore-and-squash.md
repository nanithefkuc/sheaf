# Restore and squash

Use this reference before changing the worktree from history or turning
captured work into Git commits.

## Restore whole trees or selected paths

Always preview first:

```sh
sheaf restore --at checkpoint:before-rework --dry-run
sheaf restore --at checkpoint:before-rework
sheaf restore "2 hours ago" src/parser.rs --dry-run
sheaf restore "2 hours ago" src/parser.rs
```

A full-tree restore repositions the worktree and later edits diverge from that
point. The pre-restore state and abandoned future remain reachable through
`sheaf log --all`. A scoped restore changes only named paths and records the
result as ordinary forward history.

Plans bind the target and relevant worktree state. If the tree changes after
preview, recompute the plan rather than bypassing a stale-plan refusal.

After a restore, the worktree can deliberately differ from Git HEAD. Commit the
restored state or restore forward. Never use `git checkout .` or
`git restore .` to erase that deliberate state.

### Interrupted restore

```sh
sheaf status
sheaf restore --resume
sheaf restore --abandon
```

Resume completes the pending intent. Abandon keeps the worktree exactly as it
stands and records any partially applied state as ordinary history. Decide
explicitly; do not silently discard the intent.

## Restore one selected fragment

Create or obtain a grep selection handle, then preview the splice:

```sh
sheaf restore --selection selection.json --dry-run
sheaf restore --selection selection.json
sheaf restore --selection selection.json --insert
sheaf restore --selection selection.json --delete
```

| Mode | Required current state | Result |
|---|---|---|
| default replace | selected unit binds uniquely | replace it with historical bytes |
| `--insert` | unit absent; one deletion scar | reinsert it |
| `--delete` | unit present and unique | remove it |

Fragment operations validate source bytes, current context, and uniqueness.
Missing or ambiguous candidates write nothing. When rebinding fails, use a
scoped file restore or a manual splice guided by a diff; never force a match.

## Squash a captured span into Git

Squash is read-only until an explicit `--` sanctions the Git commit:

```sh
sheaf squash
sheaf squash @~12
sheaf squash checkpoint:before-rework
sheaf squash @~3..@~1
sheaf squash @~12 -- -m "Refactor parser"
```

A successful whole-worktree squash stages the collapse, commits it, creates a
`git-<sha>` checkpoint, and records the paired timeline frame. Prefer this over
a bare `git commit` for a completed Sheaf work unit so the next squash has an
exact anchor.

## Smart squash: commit selected units only

Use smart squash when unrelated worktree changes must remain dirty:

```sh
sheaf grep validate_input --json > discovery.json
# Extract the intended hit/handle after inspecting the current JSON shape.
sheaf squash --selection selection.json
sheaf squash --selection selection.json -- -m "Extract input validation"
```

Preview and inspect both the Git patch and timeline attribution. Smart squash
requires a clean Git index; resolve staged changes before proceeding. It stages
only the selected patch through Git plumbing and leaves unrelated worktree
bytes untouched.

A partial commit records a projected frame rather than a complete
`git-<sha>` anchor. Only a later commit that makes Git HEAD, the worktree, and
the captured tip converge earns a complete frame. Renames, overlapping units,
missing candidates, ambiguous contexts, or unsupported extents must fail
closed rather than expanding the commit silently.
