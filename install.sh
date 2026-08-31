#!/bin/sh
# sheaf installer: builds the workspace in release mode and puts the
# binaries on your PATH. Linux only (inotify), Rust 1.82+.
#
#   ./install.sh                 # installs to ~/.local/bin
#   BIN_DIR=/usr/local/bin ./install.sh
#
# After installing, start the daemon (or make it a service):
#   sheafd run
#   sheaf service install
set -eu

BIN_DIR="${BIN_DIR:-$HOME/.local/bin}"
ROOT="$(cd "$(dirname "$0")" && pwd)"

command -v cargo >/dev/null 2>&1 || {
    echo "error: cargo not found — install Rust from https://rustup.rs first" >&2
    exit 1
}

echo "==> building (release)"
cd "$ROOT"
cargo build --release --workspace

mkdir -p "$BIN_DIR"
for bin in sheaf sheafd sheaf-mcp; do
    if [ -f "target/release/$bin" ]; then
        install -m 0755 "target/release/$bin" "$BIN_DIR/$bin"
        echo "==> installed $BIN_DIR/$bin"
    fi
done

case ":$PATH:" in
    *":$BIN_DIR:"*) ;;
    *) echo "note: $BIN_DIR is not on your PATH" ;;
esac

echo "==> done. Start the daemon with 'sheafd run', or make it a service:"
echo "    sheaf service install"
