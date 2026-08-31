#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

command -v cargo-llvm-cov >/dev/null 2>&1 || {
  echo "error: cargo-llvm-cov is required (cargo install cargo-llvm-cov)" >&2
  exit 1
}

# Build real instrumented binaries before the test harnesses. Several
# integration tests execute sheaf/sheafd as child processes, and Cargo exposes
# their expected paths without guaranteeing those binaries were built by
# `cargo test --tests` alone.
cargo llvm-cov clean --workspace
eval "$(cargo llvm-cov show-env --sh)"
cargo build --workspace --all-features --bins
# Rustdoc does not support stable source-based coverage and can resolve stale
# instrumented dependency paths after the explicit binary build. Cover every
# executable test target here; CI runs doctests separately without coverage.
cargo test --workspace --all-features --lib --bins --tests
# These suites exercise the real command/daemon surfaces that unit tests cannot
# reach without replacing their IPC and inotify boundaries.
./scripts/e2e_cli.sh
./scripts/e2e_restore.sh
cargo llvm-cov report \
  --ignore-filename-regex '(^|/)(\.cargo-home|target)/' \
  --fail-under-lines 95 \
  "$@"
