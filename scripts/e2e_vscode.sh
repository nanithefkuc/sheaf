#!/usr/bin/env bash
# End-to-end harness for the VS Code editor integration: an isolated daemon
# over a private runtime/socket, a temp enrolled project with pre-seeded text
# history, and the extension-host test suite driven through @vscode/test-cli.
set -euo pipefail
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."
export CARGO_HOME="${CARGO_HOME:-$PWD/.cargo-home}"

SHEAF="$PWD/target/debug/sheaf"
DAEMON="$PWD/target/debug/sheafd"
if [ ! -x "$SHEAF" ] || [ ! -x "$DAEMON" ]; then
  echo "== building workspace binaries =="
  cargo build --workspace --bins
fi

E=$(mktemp -d /tmp/sheaf-e2e-vscode-XXXXXX)
export XDG_DATA_HOME="$E/data"
export XDG_RUNTIME_DIR="$E/run"
export SHEAF_SOCKET="$E/control.sock"
mkdir -p "$XDG_DATA_HOME" "$XDG_RUNTIME_DIR" "$E/proj"

DAEMON_PID=""
cleanup() {
  if [ -n "$DAEMON_PID" ]; then
    kill "$DAEMON_PID" 2>/dev/null || true
    wait "$DAEMON_PID" 2>/dev/null || true
  fi
  rm -rf "$E"
}
trap cleanup EXIT

"$SHEAF" init "$E/proj" >/dev/null

RUST_LOG=info "$DAEMON" run --socket "$SHEAF_SOCKET" >"$E/daemon.log" 2>&1 &
DAEMON_PID=$!

echo "== waiting for the daemon to answer =="
for _ in $(seq 1 50); do
  if "$SHEAF" status "$E/proj" >/dev/null 2>&1; then
    break
  fi
  sleep 0.2
done

# Pre-seed a small text history so the store has captures before the host test
# opens the workspace.
printf 'one\n' > "$E/proj/seed.txt"
sleep 1
printf 'one\ntwo\n' > "$E/proj/seed.txt"
sleep 1

export SHEAF_VSCODE_TEST_WORKSPACE="$E/proj"

echo "== running extension-host tests =="
npm --prefix editors/vscode run test:integration
