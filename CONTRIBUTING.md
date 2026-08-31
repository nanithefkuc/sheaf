# Contributing to Sheaf

Thanks for your interest in Sheaf! This project is young, and feedback of
every kind is welcome.

## Feedback we explicitly want

Beyond normal bug reports, we are actively looking for feedback on:

- **Usability** — which commands are confusing, which outputs are hard to
  read, what forced you to re-read the README.
- **Features** — what you tried to do and couldn't, what you expected to
  exist, what felt one step short of useful.
- **Memory usage** — resident memory of `sheafd` under your workload
  (large repos, long sessions, big files). Numbers welcome: `ps -o rss= -p $(pgrep sheafd)`.
- **Crashes and bugs** — anything from a panic to a wrong answer. Include
  `sheaf doctor` output and, if you can, the smallest reproduction steps.
- **Instability** — lost or duplicated captures, daemon restarts, timeline
  surprises, anything that made you distrust the recorder.
- **Adoption feelings** — even if you decided *not* to use Sheaf, we want
  to know what tipped you against it.

Open a GitHub issue for any of the above. There are no wrong questions.

## Building and testing

```sh
cargo build --workspace
cargo test --workspace
./scripts/e2e_cli.sh
./scripts/e2e_restore.sh
```

The project targets at least **95% line coverage** (`./scripts/coverage.sh`,
requires `cargo-llvm-cov`). Add meaningful tests for new behavior; do not
lower the threshold.

Formatting and linting are enforced:

```sh
cargo fmt --all
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

## Project layout

- `crates/sheaf-core` — store, timeline, ignore rules, watcher, grep
- `crates/sheaf-daemon` — the capture daemon (`sheafd`)
- `crates/sheaf-cli` — the `sheaf` command-line interface
- `crates/sheaf-mcp` — MCP server exposing the same operations
- `.dsh/plugins/sheaf` — DeepSeek Harness (DSH) integration
- `scripts/` — e2e suites and coverage tooling

## Pull requests

- Keep changes focused; one logical change per PR.
- New behavior comes with tests.
- Avoid reformatting or rewriting code unrelated to your change.

## A note on the development setup

The main development worktree records its own history with Sheaf itself
(dogfooding). Your clone does not need this — plain `git` is enough — but
if you enroll your clone, keep `.sheaf/` out of commits (it is already in
`.gitignore`).
