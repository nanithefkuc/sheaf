#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cd "$SCRIPT_DIR/.."
export CARGO_HOME="$PWD/.cargo-home"
export RUST_LOG=info

E=/tmp/sheaf-e2e-$$
mkdir -p "$E/data" "$E/proj" "$E/out"
export XDG_DATA_HOME="$E/data"
SHEAF="$PWD/target/debug/sheaf"
DAEMON="$PWD/target/debug/sheafd"
DUMP="$PWD/target/debug/examples/dump_store"
INSPECT="$PWD/target/debug/examples/inspect_journal"

echo "== force-fresh build =="
cargo clean -p sheaf-daemon -p sheaf-core >/dev/null 2>&1
cargo build --workspace --bins --examples >/dev/null 2>&1 || { echo BUILD FAILED; exit 1; }
test "$(strings target/debug/sheafd | grep -c content_differs)" -ge 1 || { echo "STALE DAEMON BINARY"; exit 1; }

"$SHEAF" init "$E/proj" >/dev/null

start_daemon() { RUST_LOG=info "$DAEMON" run --socket "$E/control.sock" >"$E/daemon-$1.log" 2>&1 & echo $!; }
wait_persisted() {
  local want=$1 tries=0 cur=0
  while :; do
    cur=$(python3 -c "import json;print(json.load(open('$E/proj/.sheaf/state/worktree.head'))['seq'])" 2>/dev/null || echo 0)
    [ "${cur:-0}" -ge "$want" ] && return 0
    tries=$((tries+1)); if [ $tries -gt 80 ]; then echo "TIMEOUT seq>=$want have=$cur"; return 1; fi
    sleep 0.25
  done
}

D1=$(start_daemon first)
sleep 1.5
printf '# E2E\nhello 🌍\n' > "$E/proj/readme.md"
printf '\xff\xfe\x00bin\x93\x94' > "$E/proj/data.bin"
sleep 2.4
wait_persisted 1 || true

echo "== journal after burst1 =="
"$INSPECT" "$E/proj"

printf 'line-a\n' >> "$E/proj/readme.md"
printf 'born inside dying window 🌊\n' > "$E/proj/window.txt"
kill -9 "$D1"; echo "SIGKILL'd writer pid=$D1"
printf 'created while nothing watches\n' > "$E/proj/gap-born.txt"
rm -f "$E/proj/data.bin"

D2=$(start_daemon second)
sleep 6
( wait_persisted 3 ) || true

echo "== journal post-restart =="
"$INSPECT" "$E/proj"
echo "== recover =="
"$DUMP" "$E/proj" "$E/out/recovered"
diff <(cd "$E/out/recovered" && find . -type f -exec sha256sum {} \; | sort) \
     <(cd "$E/proj" && find . -path ./.sheaf -prune -o -type f -exec sha256sum {} \; | sort) \
  && echo "BYTE-EXACT: recovered tree == live tree"
pkill -TERM -f "target/debug/sheafd" 2>/dev/null || sleep 0.2
echo "== daemon logs =="
echo "-- all exports --"; grep -hE "mode=Updates|batch persisted|boot" "$E"/daemon-*.log | head -n 30
echo "-- selected --"
grep -hE "reconcil|torn|compacted|FAILED|pending|panicked" "$E"/daemon-*.log | tail -n 14
echo "== store forensics =="
find "$E/proj/.sheaf" -type f | sed "s#$E/proj/##"
echo "identity=$(cat $E/proj/.sheaf/state/identity)"
echo "kept: $E"
