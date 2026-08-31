# Reading and searching Sheaf history

Use this reference for `log`, `info`, `diff`, timeline references, literal
search, occurrence history, selection handles, and grep-cache maintenance.

## Browse captures and diffs

```sh
sheaf log
sheaf log --path src/lib.rs --follow
sheaf log --all
sheaf info <capture-id>
sheaf diff
sheaf diff checkpoint:before-rework --stat
sheaf diff @~30..@~10
```

Human `sheaf log` output is oldest to newest so the latest row lands at the
bottom of a terminal. JSON output is newest first. Use `--follow` for a path's
former names and `--all` only when divergent branches matter.

## Grep has two modes

### Point discovery (default)

Point mode finds every literal occurrence at one timeline point. It defaults
to `@` and returns line-oriented coordinates plus a stable selection handle.

```sh
sheaf grep "fn parse"                       # every occurrence at @
sheaf grep TODO --at @~20 --path src       # one historical point
sheaf grep needle --extent line --json     # line-sized selections
```

`--follow`, `--all`, `--every-capture`, `--from`, and `--to` describe a history
walk and therefore require `--history`.

### Occurrence history

History mode follows literal occurrences as episodes and reports lifecycle
transitions such as introduced, changed, moved, removed, and reintroduced.

```sh
sheaf grep TODO --history --path src --follow
sheaf grep needle --history --all
sheaf grep needle --history --from @~50 --to @~10
sheaf grep needle --history --every-capture
```

Anchor a history query to exactly one occurrence when a literal appears more
than once:

```sh
sheaf grep needle --history --at @~5 --path src/lib.rs --line 3
sheaf grep needle --history --at @~5 --path src/lib.rs --line 3 --column 12
sheaf grep needle --history --episode ep1:abc123
sheaf grep needle --history --selection selection.json
```

Line and column are one-based; column counts Unicode scalar values. An episode
ID comes from a prior history result. `--selection` accepts a JSON file holding
a full selection handle, grep hit, or supported selection payload.

## Selection handles

A handle identifies selected historical bytes (`match` or `line` extent) and
their context at one immutable capture. Restore and smart squash consume these
handles.

Rebinding is exact and fail-closed:

- one current candidate permits the operation;
- no candidate returns `selection.missing`;
- multiple candidates return `selection.ambiguous`.

Similarity never authorizes a mutation. If a fragment cannot rebind, fall back
to a scoped whole-file restore or make a manual edit guided by `sheaf diff`.
Do not force a guess.

JSON formats may be buffered JSON or NDJSON depending on the command and
extent. Inspect the command's current output before writing a `jq` extraction;
pass only handles/hits to selection consumers, not an unrelated wrapper.

## Pagination and scope

Use the tightest useful `--path` and cap results with `--max-results`. Continue
a truncated query with its `--after` cursor. Binary content is skipped rather
than decoded. A bounded/truncated result is expected behavior on large stores;
it is not permission to treat the partial result as complete.

## Derived grep cache

The cache is disposable; retained timeline data remains authoritative.

```sh
sheaf cache backfill
sheaf cache rebuild
```

Use `backfill` to index retained captures not yet represented. Use `rebuild`
when doctor reports cache damage or authoritative results disagree with a
cache-assisted query; rebuilding wipes only the derived cache and increments
its generation.
