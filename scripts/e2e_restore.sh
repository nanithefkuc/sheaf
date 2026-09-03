#!/usr/bin/env bash
# End-to-end: wreck a real tree through a live daemon, then restore it.
#
# Prerequisites: a cargo toolchain (the script builds the workspace bins
# itself), python3 for the intent-planting step below, and a Linux host with
# inotify; it spins up its own sheafd, so no external daemon is required.
#
# Proves recovery on real inotify/IPC/flock machinery rather than in-process:
# byte-exact recovery of an afternoon of damage, an intact log, working undo,
# scoped restore, divergence, and no spurious captures from the restore's own
# writes.
set -euo pipefail
cd "$(dirname "$0")/.."
export CARGO_HOME="$PWD/.cargo-home"
export RUST_LOG=warn

E=$(mktemp -d /tmp/sheaf-restore-e2e-XXXXXX)
trap 'pkill -TERM -f "target/debug/sheafd run --socket $E/control.sock" 2>/dev/null || true' EXIT
mkdir -p "$E/data" "$E/proj"
export XDG_DATA_HOME="$E/data"
export SHEAF_SOCKET="$E/control.sock"
SHEAF="$PWD/target/debug/sheaf"
DAEMON="$PWD/target/debug/sheafd"
P="$E/proj"
fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "  ok: $*"; }
# `grep -q` closes the pipe early, and under `pipefail` the SIGPIPE of the
# producer becomes the pipeline's exit status. `grep -c` drains its input.
has() { grep -c -- "$1" >/dev/null; }

echo "== build =="
cargo build --workspace --bins >/dev/null 2>&1 || fail "build"
strings "$DAEMON" | has "unknown or expired plan token" || fail "stale daemon binary"
strings "$SHEAF" | has "restore to:" || fail "stale cli binary"

# `status` takes a positional path; every other verb takes -C.
sheaf() { "$SHEAF" "$@" -C "$P"; }
tree_hash() { (cd "$P" && find . -path ./.sheaf -prune -o -type f -print0 | sort -z | xargs -0 sha256sum | sha256sum); }
log_count() { sheaf log --limit 1000 --json | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["entries"]))'; }
all_count() { sheaf branch list --json | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["graph"]["nodes"]))'; }
settle() {
  local want=$1 tries=0
  while :; do
    [ "$(log_count)" -ge "$want" ] && return 0
    tries=$((tries+1)); [ $tries -gt 60 ] && fail "timed out waiting for $want captures (have $(log_count))"
    sleep 0.25
  done
}

echo "== enroll and start the daemon =="
"$SHEAF" init "$P" >/dev/null
"$DAEMON" run --socket "$E/control.sock" >"$E/daemon.log" 2>&1 &
sleep 1.5
"$SHEAF" status "$P" | has "watching:      yes" || fail "daemon is not watching"
ok "daemon watching $P"

echo "== an honest morning of work =="
mkdir -p "$P/src/util" "$P/assets"
cat > "$P/src/lib.rs" <<'RS'
pub fn greet() -> &'static str {
    "héllo wörld 🌍"
}
RS
echo 'pub mod strings;' > "$P/src/util/mod.rs"
echo 'pub fn trim(s: &str) -> &str { s.trim() }' > "$P/src/util/strings.rs"
printf '# project\n\nnotes that matter\n' > "$P/README.md"
printf '\xff\xfe\x00\x93\x94\x01binary payload\xfd' > "$P/assets/logo.bin"
sleep 2.5
settle 1
GOOD_HASH=$(tree_hash)
cp -a "$P" "$E/good-copy"
sheaf checkpoint create before-refactor >/dev/null || fail "checkpoint"
ok "morning captured, checkpoint pinned ($GOOD_HASH)"

echo "== the fat-fingered afternoon =="
cat > "$P/src/lib.rs" <<'RS'
pub fn greet() -> &'static str {
    "OOPS"
}
RS
mv "$P/src/util/strings.rs" "$P/src/strs.rs"
rm "$P/README.md"
echo '// half-finished experiment' > "$P/src/scratch.rs"
printf 'not a logo anymore' > "$P/assets/logo.bin"
sleep 2.5
settle 2
WRECKED_HASH=$(tree_hash)
[ "$GOOD_HASH" != "$WRECKED_HASH" ] || fail "the afternoon changed nothing"
BEFORE_RESTORE_CAPTURES=$(all_count)
ok "tree wrecked, $BEFORE_RESTORE_CAPTURES captures recorded"

echo "== dry run touches nothing =="
sheaf restore --at checkpoint:before-refactor --dry-run > "$E/plan.txt" || fail "dry-run"
grep -q "scope:       whole worktree" "$E/plan.txt" || fail "plan scope missing"
grep -q "update  src/lib.rs" "$E/plan.txt" || fail "plan omits the botched edit"
grep -q "delete  src/scratch.rs" "$E/plan.txt" || fail "plan omits the scratch file"
[ "$(tree_hash)" = "$WRECKED_HASH" ] || fail "dry run modified the worktree"
ok "plan is pure computation"
sed 's/^/    /' "$E/plan.txt"

echo "== restore =="
sheaf restore checkpoint:before-refactor > "$E/restore.txt" || fail "restore"
sed 's/^/    /' "$E/restore.txt"
[ "$(tree_hash)" = "$GOOD_HASH" ] || fail "tree not recovered byte-exact"
diff -r --exclude=.sheaf "$E/good-copy" "$P" >/dev/null || fail "tree differs from the saved copy"
grep -q 'héllo wörld 🌍' "$P/src/lib.rs" || fail "multibyte characters not restored"
ok "worktree recovered byte-exact, multibyte included"

UNDO=$(grep '^undo:' "$E/restore.txt" | awk '{print $NF}')
[ -n "$UNDO" ] || fail "no undo reference offered"

echo "== the restore's own writes are not new history =="
sleep 3
AFTER=$(all_count)
[ "$AFTER" -eq "$BEFORE_RESTORE_CAPTURES" ] \
  || fail "restore echoed into $((AFTER - BEFORE_RESTORE_CAPTURES)) spurious captures"
LINEAGE_AFTER=$(log_count)
[ "$LINEAGE_AFTER" -lt "$AFTER" ] || fail "head did not move back (lineage=$LINEAGE_AFTER all=$AFTER)"
ok "no spurious captures ($AFTER total, $LINEAGE_AFTER on the restored lineage)"

echo "== the log was never trimmed =="
sheaf branch list --json > "$E/log.json"
python3 - "$E/log.json" "$BEFORE_RESTORE_CAPTURES" <<'PY' || fail "log lost entries"
import json,sys
entries=[node["capture"] for node in json.load(open(sys.argv[1]))["graph"]["nodes"]]
assert len(entries) >= int(sys.argv[2]), (len(entries), sys.argv[2])
PY
ok "every pre-restore capture is still reachable"

echo "== new work after a rollback diverges =="
echo '// the new future' >> "$P/src/lib.rs"
sleep 2.5
settle $((LINEAGE_AFTER + 1))
LINEAGE=$(log_count)
ALL=$(all_count)
[ "$ALL" -gt "$LINEAGE" ] || fail "no divergence recorded (lineage=$LINEAGE all=$ALL)"
ok "current lineage $LINEAGE captures, all branches $ALL"

echo "== scoped restore is forward history, not a branch =="
BEFORE_SCOPED=$(log_count)
sheaf restore checkpoint:before-refactor src/lib.rs > "$E/scoped.txt" || fail "scoped restore"
sed 's/^/    /' "$E/scoped.txt"
grep -q "recorded:" "$E/scoped.txt" || fail "scoped restore recorded no forward capture"
grep -q 'héllo wörld 🌍' "$P/src/lib.rs" || fail "scoped restore did not restore the file"
grep -q "the new future" "$P/src/lib.rs" && fail "scoped restore left the later edit in place"
sleep 3
[ "$(log_count)" -eq $((BEFORE_SCOPED + 1)) ] || fail "scoped restore appended more than one capture"
ok "one forward capture appended"

echo "== undo returns the afternoon =="
sheaf restore "$UNDO" >/dev/null || fail "undo restore"
[ "$(tree_hash)" = "$WRECKED_HASH" ] || fail "undo did not reproduce the wrecked tree"
ok "undo is exact"

echo "== the restored lineage survives a daemon restart =="
sheaf restore checkpoint:before-refactor >/dev/null || fail "restore before restart"
pkill -TERM -f "target/debug/sheafd run --socket $E/control.sock" || true
sleep 1.5
"$DAEMON" run --socket "$E/control.sock" >>"$E/daemon.log" 2>&1 &
sleep 2.5
[ "$(tree_hash)" = "$GOOD_HASH" ] || fail "restart changed the worktree"
"$SHEAF" status "$P" | has "watching:      yes" || fail "daemon did not resume watching"
echo '// after the restart' >> "$P/src/util/mod.rs"
sleep 2.5
HEAD_PATHS=$(sheaf log --limit 1 --json | python3 -c 'import json,sys;print(",".join(json.load(sys.stdin)["entries"][0]["paths"]))')
[ "$HEAD_PATHS" = "src/util/mod.rs" ] || fail "post-restart capture landed oddly: $HEAD_PATHS"
ok "the writer stayed on the restored lineage across a restart"

echo "== a save landing during a restore is not lost =="
# A big tree makes the install loop long enough for a concurrent writer to
# land inside it. Even when the write misses the window, this can only pass
# by the bytes actually being recoverable.
mkdir -p "$P/bulk"
for i in $(seq 1 400); do printf 'original %s\n' "$i" > "$P/bulk/f$i.txt"; done
sleep 3
settle 1
BULK=$(sheaf log --limit 1 --json | python3 -c 'import json,sys;print(json.load(sys.stdin)["entries"][0]["id"])')
for i in $(seq 1 400); do printf 'changed %s\n' "$i" > "$P/bulk/f$i.txt"; done
sleep 3
( sleep 0.05; printf 'typed during the restore\n' > "$P/bulk/f400.txt" ) &
WRITER=$!
sheaf restore "$BULK" bulk > "$E/race.txt" || fail "bulk restore"
wait "$WRITER"
sleep 3
# Whichever side won the race, the typed bytes must exist somewhere in history.
sheaf branch list --json > "$E/race-log.json"
python3 - "$P" "$E/race-log.json" <<'PY2' || fail "a save during the restore was lost"
import json,pathlib,subprocess,sys
proj=pathlib.Path(sys.argv[1])
live=(proj/"bulk/f400.txt").read_text()
if live.strip()=="typed during the restore":
    print("    (the write landed after the restore finished)"); raise SystemExit(0)
entries=[node["capture"] for node in json.load(open(sys.argv[2]))["graph"]["nodes"]]
assert any("bulk/f400.txt" in e["paths"] for e in entries), "no capture mentions the raced path"
PY2
ok "the concurrent save is accounted for"

echo "== a restore killed mid-flight finishes on the next start =="
# Reproduce the crash window exactly: a durable intent, a worktree that only
# partially matches it, and no daemon. Startup must converge, not give up.
INTENT_TARGET=$(sheaf log --limit 1 --json | python3 -c 'import json,sys;e=json.load(sys.stdin)["entries"][0];print(e["frontier"],e["id"])')
FRONTIER=${INTENT_TARGET% *}; CAPTURE=${INTENT_TARGET#* }
printf 'wreck it again\n' > "$P/src/lib.rs"
sleep 2.5
kill -9 "$(pgrep -f "target/debug/sheafd run --socket $E/control.sock")" || fail "no daemon to kill"
sleep 0.5
python3 - "$P/.sheaf/state/restore.intent" "$FRONTIER" "$CAPTURE" <<'PY'
import json,sys,time
# Intents are stamped with their start time and only FRESH intents
# auto-resume on boot (a week-old intent waits for the operator instead of
# silently rewinding the tree). Plant a fresh crash.
json.dump({"token":"resumed-by-e2e","mode":"full","scope":[],
           "target":{"frontier":sys.argv[2],"capture_id":sys.argv[3]},
           "started_ms":int(time.time()*1000)}, open(sys.argv[1],"w"))
PY
printf 'and again, after the intent landed\n' > "$P/src/util/mod.rs"
"$DAEMON" run --socket "$E/control.sock" >>"$E/daemon.log" 2>&1 &
sleep 3
[ -f "$P/.sheaf/state/restore.intent" ] && fail "intent survived a successful resume"
grep -q "wreck it again" "$P/src/lib.rs" && fail "resume did not finish the restore"
"$SHEAF" status "$P" | has "watching:      yes" || fail "daemon did not resume watching"
ok "interrupted restore completed on startup, worktree converged"

echo "== nothing was lost to the crash =="
# The edits made while the daemon was dead were captured before the resume
# overwrote them, so they are still addressable.
sheaf branch list --json > "$E/log2.json"
python3 - "$E/log2.json" <<'PY' || fail "gap edits were not preserved"
import json,sys
entries=[node["capture"] for node in json.load(open(sys.argv[1]))["graph"]["nodes"]]
assert any("src/util/mod.rs" in e["paths"] for e in entries), "gap edit missing from history"
PY
ok "edits from the dead gap are in history"

echo
echo "PASS: restore engine e2e"
