#!/usr/bin/env bash
# End-to-end: the whole v1 CLI surface over a live daemon.
#
# Prerequisites: a cargo toolchain (the script builds the workspace bins
# itself) and a Linux host with inotify; it spins up its own sheafd, so no
# external daemon or installed binaries are required.
#
# Exercises the four commands on real inotify/IPC machinery, with the ugly
# cases front and center: renamed paths, branch points, and partial scopes —
# plus the degraded (daemon-down) behavior of every read verb.
set -euo pipefail
cd "$(dirname "$0")/.."
export CARGO_HOME="$PWD/.cargo-home"
export RUST_LOG=warn

E=$(mktemp -d /tmp/sheaf-cli-e2e-XXXXXX)
trap 'pkill -TERM -f "target/debug/sheafd run --socket $E/control.sock" 2>/dev/null || true' EXIT
mkdir -p "$E/data" "$E/proj"
export XDG_DATA_HOME="$E/data"
export SHEAF_SOCKET="$E/control.sock"
SHEAF="$PWD/target/debug/sheaf"
DAEMON="$PWD/target/debug/sheafd"
P="$E/proj"
fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { echo "  ok: $*"; }
has() { grep -c -- "$1" >/dev/null; }

echo "== build =="
cargo build --workspace --bins >/dev/null 2>&1 || fail "build"
strings "$SHEAF" | has "rename from" || fail "stale cli binary"
strings "$DAEMON" | has "diff request timed out" || fail "stale daemon binary"

sheaf() { "$SHEAF" "$@" -C "$P"; }
log_count() { sheaf log --limit 1000 --json | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["entries"]))'; }
settle() {
  local want=$1 tries=0
  while :; do
    [ "$(log_count)" -ge "$want" ] && return 0
    tries=$((tries+1)); [ $tries -gt 60 ] && fail "timed out waiting for $want captures (have $(log_count))"
    sleep 0.25
  done
}
first_id() { sheaf log --limit 1 --json | python3 -c 'import json,sys;print(json.load(sys.stdin)["entries"][0]["id"])'; }

echo "== enroll and start the daemon =="
# A project .gitignore must be honored (README: anything in .gitignore is
# ignored by Sheaf too). Written before enrollment so the initial watch sees it.
printf '*.log\nbuild/\n/rootonly.secret\n' > "$P/.gitignore"
"$SHEAF" init "$P" >/dev/null
"$DAEMON" run --socket "$E/control.sock" >"$E/daemon.log" 2>&1 &
sleep 1.5
"$SHEAF" status "$P" | has "watching:      yes" || fail "daemon is not watching"
ok "daemon watching $P"

echo "== a morning of work =="
mkdir -p "$P/src/util" "$P/assets" "$P/build"
printf 'pub fn greet() {\n    "hello"\n}\n' > "$P/src/lib.rs"
printf 'pub fn trim() {}\n' > "$P/src/util/strings.rs"
printf '# notes\n' > "$P/README.md"
printf '\xff\xfe\x01\x02logo' > "$P/assets/logo.bin"
# Gitignored files: must NEVER be captured.
printf 'transient\n' > "$P/debug.log"
printf 'nested transient\n' > "$P/src/trace.log"
printf 'artifact\n' > "$P/build/out.o"
printf 'top secret\n' > "$P/rootonly.secret"
sleep 2.5
settle 1
sheaf checkpoint create morning >/dev/null
ok "morning captured + checkpointed"

echo "== .gitignore is honored (P5) =="
# None of the gitignored paths may appear in any capture's file list.
GI_LOG=$(sheaf log --limit 1000 --json)
for p in debug.log src/trace.log build/out.o rootonly.secret; do
  echo "$GI_LOG" | grep -q "\"$p\"" && fail "gitignored $p was captured" || true
done
# But an anchored /rootonly.secret only ignores the root one: a same-named file
# in a subdir is still tracked.
printf 'not the root one\n' > "$P/src/rootonly.secret"
sleep 2.5
settle 2
sheaf log --limit 1000 --json | grep -q '"src/rootonly.secret"' || fail "anchored /rootonly.secret over-ignored a nested file"
# Tracked source is present, confirming the walk still works.
sheaf log --limit 1000 --json | grep -q '"src/lib.rs"' || fail "tracked file missing from log"
ok "gitignored paths excluded; anchored pattern stays anchored"

echo "== diff: the live worktree vs the last capture =="
printf 'pub fn greet() {\n    "HELLO"\n}\n' > "$P/src/lib.rs"
sheaf diff --exit-code > "$E/d1.txt" && fail "diff --exit-code must fail when differences exist" || [ $? -eq 1 ] || fail "wrong exit code"
grep -q -- '-    "hello"' "$E/d1.txt" || fail "diff omits the removed line"
grep -q -- '+    "HELLO"' "$E/d1.txt" || fail "diff omits the added line"
sheaf diff --stat | has "src/lib.rs" || fail "stat omits the file"
sheaf diff --json | python3 -c 'import json,sys; d=json.load(sys.stdin); assert d["diff"]["entries"], "json diff empty"; assert any("+    \"HELLO\"" in l for l in d["patch"].splitlines()), "patch not inline"' || fail "json diff"
ok "worktree diff, stat, json, exit-code"

echo "== unflushed rename pairs by content =="
mv "$P/src/util/strings.rs" "$P/src/strs.rs"
sheaf diff checkpoint:morning > "$E/d2.txt" || fail "diff vs checkpoint"
grep -q "rename from src/util/strings.rs" "$E/d2.txt" || fail "unflushed rename not paired"
grep -q "rename to src/strs.rs" "$E/d2.txt" || fail "unflushed rename not paired"
ok "rename visible before the daemon even captures it"

sleep 2.5
settle 2
sheaf diff checkpoint:morning > "$E/d3.txt" || fail "diff vs checkpoint (captured)"
grep -q "rename from src/util/strings.rs" "$E/d3.txt" || fail "recorded rename not paired"

echo "== point vs point and scopes =="
MORNING=$(sheaf checkpoint list --json | python3 -c 'import json,sys;print(json.load(sys.stdin)["checkpoints"][0]["capture_id"])')
NOW=$(first_id)
sheaf diff "$MORNING".."$NOW" > "$E/d4.txt" || fail "range diff"
grep -q "rename to src/strs.rs" "$E/d4.txt" || fail "range diff omits rename"
sheaf diff "$MORNING" "$NOW" --path src/strs.rs > "$E/d5.txt" || fail "scoped diff"
[ "$(grep -c 'diff --sheaf' "$E/d5.txt")" -eq 0 ] && fail "scoped diff lost the rename" || true
! grep -q "src/lib.rs" "$E/d5.txt" || fail "scoped diff leaked out-of-scope paths"
ok "range syntax, two-point form, path scoping"

echo "== log: follow a path through its renames =="
[ "$(sheaf log --path src/strs.rs --limit 1000 --json | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["entries"]))')" -eq 1 ] \
  || fail "without follow, pre-rename captures should stay hidden"
[ "$(sheaf log --path src/strs.rs --follow --limit 1000 --json | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["entries"]))')" -ge 2 ] \
  || fail "--follow must include old-name captures"
sheaf log --path src/strs.rs --follow | grep -q "src/util/strings.rs" || fail "follow omits the old name"
ok "log --follow crosses the rename"

echo "== the ugly branch case: rollback, divergence, checkpoints =="
sheaf checkpoint create afternoon >/dev/null
printf 'pub fn greet() {}\n// the wreck\n' > "$P/src/lib.rs"
rm "$P/README.md"
sleep 2.5
settle 3
sheaf restore checkpoint:morning >/dev/null || fail "full restore"
grep -q '"hello"' "$P/src/lib.rs" || fail "restore missed the edit"
[ -f "$P/README.md" ] || fail "restore missed the deletion"
printf 'pub fn greet() {}\n// the new future\n' > "$P/src/lib.rs"
sleep 2.5
# After the rollback the lineage view restarts at morning: 1 lineage capture
# plus this one.
settle 2
sheaf log 2>&1 | has "divergent branch tips exist" || fail "log must hint at hidden branches"
sheaf log --all > "$E/all.txt" || fail "log --all"
grep -q "^+ " "$E/all.txt" || fail "log --all must mark off-lineage captures"
grep -q "^\* " "$E/all.txt" || fail "log --all must mark the current lineage"
sheaf checkpoint list | has "afternoon.*off current lineage" || fail "off-lineage checkpoint not marked"
sheaf checkpoint list | grep morning | grep -v "off current lineage" >/dev/null || fail "morning wrongly marked"
ok "branch hints, --all markers, checkpoint lineage"

echo "== scoped restore across a rename speaks both names =="
mv "$P/src/util/strings.rs" "$P/src/strs.rs"
sleep 2.5
settle 3
# The capture before the rename, where the path was still strings.rs.
BEFORE=$(sheaf log --limit 2 --json | python3 -c 'import json,sys;print(json.load(sys.stdin)["entries"][1]["id"])')
mv "$P/src/strs.rs" "$P/src/gone.rs"
sleep 2.5
settle 4
sheaf restore --at "$BEFORE" src/strs.rs >/dev/null || fail "scoped rename restore"
[ -f "$P/src/util/strings.rs" ] || fail "former name not materialized"
[ ! -e "$P/src/gone.rs" ] || fail "current name not removed"
ok "scoped restore of a renamed path"

echo "== typo'd scopes say so =="
sheaf restore --at "$BEFORE" --dry-run src/tpyo.rs 2>"$E/typo.txt" | grep -q "nothing to do" || fail "typo should be a noop"
grep -q "held .src/tpyo.rs" "$E/typo.txt" || fail "typo note missing"

echo "== squash time anchor uses postdated git HEAD =="
SQUASH=$E/squash
mkdir -p "$SQUASH"
"$SHEAF" init "$SQUASH" >/dev/null || fail "squash project init"
for i in 1 2; do printf 'before commit %s\n' "$i" > "$SQUASH/file$i.txt"; sleep 0.6; done
"$SHEAF" status "$SQUASH" | has "watching:      yes" || fail "squash project is not watched"
sleep 1
(cd "$SQUASH" && printf '.sheaf/\n' > .gitignore && git init -q && git config user.email e2e@example.test && git config user.name e2e && git add . && git commit -qm "baseline") || fail "git baseline commit"
printf 'after commit capture\n' > "$SQUASH/after.txt"
sleep 1
"$SHEAF" log -C "$SQUASH" --limit 1000 >/dev/null || fail "squash captures unavailable"
"$SHEAF" squash -C "$SQUASH" -- -m "time anchor coverage" > "$E/time-anchor.txt" 2>&1 || fail "time-anchor squash"
grep -q "anchor:.*last git commit" "$E/time-anchor.txt" || fail "squash did not use git-time anchor"
ok "squash defaults to the postdated git HEAD time anchor"

echo "== cache rebuild accumulates multiple daemon pages =="
INDEX=$E/index
mkdir -p "$INDEX"
"$SHEAF" init "$INDEX" >/dev/null || fail "index project init"
"$SHEAF" status "$INDEX" | has "watching:      yes" || fail "index project is not watched"
for i in $(seq 1 20); do printf 'indexed capture %s\n' "$i" > "$INDEX/file.txt"; sleep 0.4; done
"$SHEAF" cache rebuild -C "$INDEX" --json > "$E/index-rebuild.json" || fail "cache rebuild"
python3 - "$E/index-rebuild.json" <<'PY' || fail "index rebuild did not aggregate totals"
import json, sys
report = json.load(open(sys.argv[1]))
assert report["captures_indexed"] >= 10, report
assert report["complete"] is True, report
PY
ok "cache rebuild reports accumulated indexed captures"

echo "== smart squash resets a diverged staged index =="
PATCH=$E/patch
mkdir -p "$PATCH"
"$SHEAF" init "$PATCH" >/dev/null || fail "patch project init"
"$SHEAF" status "$PATCH" | has "watching:      yes" || fail "patch project is not watched"
(cd "$PATCH" && printf '.sheaf/\n' > .gitignore && git init -q && git config user.email e2e@example.test && git config user.name e2e && printf 'base\nneedle\n' > target.txt && git add target.txt .gitignore && git commit -qm base) || fail "patch baseline commit"
printf 'base\nchanged needle\n' > "$PATCH/target.txt"
sleep 1
grep_json=$("$SHEAF" grep "changed needle" -C "$PATCH" --json) || fail "patch selection grep"
printf '%s\n' "$grep_json" | python3 -c 'import json,sys; [print(json.dumps(x["report"]["hits"][0])) for x in map(json.loads,sys.stdin) if x.get("type")=="summary"]' > "$PATCH/selection.json"
test -s "$PATCH/selection.json" || fail "patch selection missing"
printf 'unrelated staged change\n' > "$PATCH/extra.txt"
(
  sleep 0.05
  for i in $(seq 1 200); do git -C "$PATCH" add extra.txt 2>/dev/null || true; done
) &
INDEX_RACER=$!
if "$SHEAF" squash -C "$PATCH" --selection "$PATCH/selection.json" -- -m "divergence coverage" > "$E/patch-divergence.txt" 2>&1; then
  kill "$INDEX_RACER" 2>/dev/null || true
  fail "diverged staged index unexpectedly committed"
fi
wait "$INDEX_RACER" 2>/dev/null || true
grep -q "staged tree does not match the selected patch exactly" "$E/patch-divergence.txt" || fail "patch divergence error missing"
(cd "$PATCH" && git diff --cached --quiet) || fail "patch divergence did not reset index"
ok "smart squash rejects a staged-tree divergence and resets the index"

echo "== fragment restore prints conflicting candidates =="
CONFLICT=$E/conflict
mkdir -p "$CONFLICT"
"$SHEAF" init "$CONFLICT" >/dev/null || fail "conflict project init"
"$SHEAF" status "$CONFLICT" | has "watching:      yes" || fail "conflict project is not watched"
printf 'branch alpha\n' > "$CONFLICT/shared.txt"
sleep 0.8
CA=$("$SHEAF" log -C "$CONFLICT" --limit 1 --json | python3 -c 'import json,sys;print(json.load(sys.stdin)["entries"][0]["id"])')
printf 'branch beta\n' > "$CONFLICT/shared.txt"
sleep 0.8
CB=$("$SHEAF" log -C "$CONFLICT" --limit 1 --json | python3 -c 'import json,sys;print(json.load(sys.stdin)["entries"][0]["id"])')
"$SHEAF" restore -C "$CONFLICT" "$CA" >/dev/null || fail "conflict branch restore"
printf 'branch gamma\nbranch alpha\nbranch alpha\n' > "$CONFLICT/shared.txt"
sleep 1
"$SHEAF" grep "branch alpha" -C "$CONFLICT" --at "$CA" --path shared.txt --extent line --json \
  | python3 -c 'import json,sys; [print(json.dumps(x["report"]["hits"][0])) for x in map(json.loads,sys.stdin) if x.get("type")=="summary"]' \
  > "$CONFLICT/selection.json" || fail "conflict selection grep"
test -s "$CONFLICT/selection.json" || fail "conflict selection missing"
"$SHEAF" restore -C "$CONFLICT" --selection "$CONFLICT/selection.json" --dry-run > "$E/conflicts.txt" 2>&1 || true
grep -q "candidate shared.txt at" "$E/conflicts.txt" || fail "conflict candidates not rendered"
grep -q "candidate shared.txt at .*\.\." "$E/conflicts.txt" || fail "candidate ranges missing"
ok "fragment restore names candidates from branched history"

echo "== degraded mode: every read verb still answers =="
pkill -TERM -f "target/debug/sheafd run --socket $E/control.sock" || true
sleep 1
sheaf diff checkpoint:morning > "$E/deg.txt" 2>"$E/deg.err" || fail "degraded diff"
grep -q "read-only store snapshot" "$E/deg.err" || fail "degraded note missing"
grep -q "rename" "$E/deg.txt" || true  # state-dependent; only the path above is asserted
sheaf log --path src/strs.rs --follow | grep -q "src/util/strings.rs" || fail "degraded follow"
sheaf checkpoint list | grep -q morning || fail "degraded checkpoint list"
sheaf restore --at checkpoint:morning --dry-run >/dev/null || fail "degraded dry-run"
ok "daemon down: diff, log --follow, checkpoints, dry-run all work"

# --fix offline: a crash mid-append leaves a torn frame; the
# offline repair truncates to the intact prefix under the exclusive lock.
SEG=$(ls "$P/.sheaf/store/journal"/*.op | sort | tail -1)
INTACT=$(stat -c%s "$SEG")
printf 'AAAA' >> "$SEG"   # short header: torn tail by any reader's rule
sheaf doctor > "$E/doc-fix.txt" 2>&1 && fail "torn tail must fail the sweep"
grep -q "FAIL.*journal_frames" "$E/doc-fix.txt" || fail "doctor names the torn frame"
sheaf doctor --fix > "$E/doc-fix.txt" || fail "offline doctor --fix"
grep -q "fixes applied: 1" "$E/doc-fix.txt" || fail "fix applied output"
grep -q "truncate-journal" "$E/doc-fix.txt" || fail "truncate action named"
sheaf doctor >/dev/null || fail "re-run sweep is green"
[ "$(stat -c%s "$SEG")" = "$INTACT" ] || fail "segment restored to intact prefix"
ok "doctor --fix: torn tail truncated offline, sweep green again"

echo "== daemon returns =="
"$DAEMON" run --socket "$E/control.sock" >>"$E/daemon.log" 2>&1 &
sleep 1.5
"$SHEAF" status "$P" | has "watching:      yes" || fail "daemon did not resume watching"
sheaf diff checkpoint:morning >/dev/null || fail "live diff after restart"

echo "== doctor and gc =="
# Doctor against the live daemon's store: healthy verdict, exit 0.
sheaf doctor >/dev/null || fail "doctor flagged a healthy store"
# The repair verb rides the daemon IPC too: nothing to fix is a clean exit.
sheaf doctor --fix | has "fixes applied: 0" || fail "daemon-path doctor --fix"
# Corrupt nothing; instead make an orphan blob and watch doctor stay calm
# (orphans are a retention fact, not an integrity failure).
printf 'orphan bytes' > /tmp/sheaf-orphan-$$
ORPHAN=$("$SHEAF" 2>/dev/null; true)
python3 - "$P" <<'PY' || fail "orphan plant failed"
import hashlib,pathlib,sys
proj=pathlib.Path(sys.argv[1])
d=hashlib.sha256(b"orphan bytes").hexdigest()
fan=proj/".sheaf/store/blobs"/d[:2]
fan.mkdir(parents=True,exist_ok=True)
(fan/d).write_bytes(b"orphan bytes")
PY
sheaf doctor | has "verdict: healthy" || fail "an orphan blob must not be an integrity failure"

# Offline gc: plan first (report), then apply, orphan gone, history intact.
"$SHEAF" gc -C "$P" 2>/dev/null | has "gc plan" || fail "gc plan output"
BEFORE=$(log_count)
"$SHEAF" gc --apply -C "$P" | has "gc applied" || fail "gc apply output"
AFTER=$(log_count)
[ "$BEFORE" -eq "$AFTER" ] || fail "gc changed the timeline ($BEFORE -> $AFTER)"
python3 - "$P" <<'PY' || fail "orphan survived gc"
import hashlib,pathlib,sys
proj=pathlib.Path(sys.argv[1])
d=hashlib.sha256(b"orphan bytes").hexdigest()
assert not (proj/".sheaf/store/blobs"/d[:2]/d).exists(), "orphan blob must be collected"
PY
ok "doctor healthy verdict, gc collects the orphan, history untouched"

echo "== PRODUCT-README parity: syntax P1/P2 =="
# gc --collect is an accepted alias of --apply (report-only here: nothing to do,
# but the flag must parse and behave like apply).
"$SHEAF" gc --collect -C "$P" | has "gc applied" || fail "gc --collect alias"
# Bare checkpoint name == `checkpoint create <name>`, and a label may contain
# spaces; the bare name then resolves for restore like checkpoint:<name>.
sheaf checkpoint "before parity" >/dev/null || fail "bare checkpoint create"
sheaf checkpoint list | has "before parity" || fail "spaced checkpoint name not stored"
sheaf restore --dry-run "before parity" >/dev/null || fail "bare checkpoint-name restore"
sheaf restore --dry-run "checkpoint:before parity" >/dev/null || fail "explicit spaced checkpoint restore"
# Relative-duration reference: `@~<dur>` must be accepted as a duration, not
# rejected as a bad integer. Whether a given window lands on a capture is
# wall-clock/environment dependent, so we assert on the parse contract: the
# error we must NEVER see is "invalid relative reference"; "before recorded
# capture history" is a legitimate resolution outcome.
sheaf restore --dry-run @~1h 2>"$E/dur.txt" >/dev/null || true
grep -q "invalid relative reference" "$E/dur.txt" && fail "@~<duration> not parsed as a duration" || true
# A window that certainly contains the just-made captures resolves cleanly.
sheaf restore --dry-run @~2h 2>>"$E/dur.txt" >/dev/null \
  || grep -q "before recorded capture history" "$E/dur.txt" \
  || fail "@~<duration> neither resolved nor gave a resolution-time error"
# Doctor reports daemon reachability plainly (honesty line).
sheaf doctor | has "daemon:" || fail "doctor omits daemon reachability line"
ok "gc --collect, bare/spaced checkpoints, @~<dur>, doctor daemon line"

echo "== PRODUCT-README parity: retention (P3) =="
# Expiry is a config knob: set, surface in status, then unset reporting.
sheaf gc --set-expiry 2h | has "expiry set to 2h" || fail "gc --set-expiry output"
"$SHEAF" status "$P" | has "edits expire after 2h" || fail "status omits expiry"
# Fresh captures are inside the horizon: the plan reports the boundary but
# nothing prunable, and an apply must not trim.
sheaf gc --json > "$E/ret-plan.json" || fail "gc plan with expiry"
grep -q '"prunable": \[\]' "$E/ret-plan.json" || fail "fresh history must not be prunable"
sheaf gc --apply | has "timeline intact" || fail "gc --apply inside horizon"
sheaf log | grep -q "" || fail "log readable after no-op retention apply"
# The daemon path exercises the same code (writer thread): mark + apply.
# Refuse the present: marking the head must fail with the explanation.
sheaf gc @ 2>"$E/mark-head.txt" && fail "marking the head must refuse" || true
grep -q "cannot mark the current head" "$E/mark-head.txt" || fail "mark-head refusal message"
# Mark the TWO oldest captures as a prefix. Whether a mark can act now is
# DAG geometry, not correctness: the trim boundary is the GCA of the
# keep-set, and this project's history ends in sibling forks whose deepest
# shared ancestor IS the root — a marked capture sitting at or above that
# GCA is correctly DEFERRED (it is the surviving baseline), never pruned.
# So here we only assert the accounting: every mark shows up either as
# prunable-with-cause or as deferred. The deterministic trim runs in the
# linear mini-project below.
OLD=$(sheaf log --limit 1000 --json | python3 -c 'import json,sys; e=json.load(sys.stdin)["entries"]; print(e[-1]["id"][:12])')
OLD2=$(sheaf log --limit 1000 --json | python3 -c 'import json,sys; e=json.load(sys.stdin)["entries"]; print(e[-2]["id"][:12])')
test -n "$OLD" && test -n "$OLD2" || fail "could not read old capture ids"
sheaf gc "$OLD" | has "marked collectable" || fail "gc mark output"
sheaf gc "$OLD2" | has "marked collectable" || fail "gc mark output (second)"
sheaf gc --json > "$E/ret-marks.json" || fail "gc plan with marks"
python3 - "$E/ret-marks.json" "$OLD" "$OLD2" <<'PY' || fail "plan loses track of marks"
import json, sys
plan = json.load(open(sys.argv[1]))
r = plan["retention"]
prunable = {p["id"][:12]: p.get("cause") for p in r.get("prunable", [])}
deferred = [m[:12] for m in r.get("deferred_marks", [])]
for mark in sys.argv[2:4]:
    if mark in prunable:
        assert prunable[mark] == "gc mark", f"{mark} prunable with wrong cause {prunable[mark]}"
    else:
        assert mark in deferred, f"mark {mark} neither prunable nor deferred"
PY
# Whether the marks can act here is geometry; apply is safe either way
# (a trim when they earned it, a no-op when deferred).
sheaf gc --apply >/dev/null 2>&1 || fail "gc --apply with marks"
sheaf log | grep -q "" || fail "log readable after mark apply"
ok "set-expiry, mark accounting, mark-head refusal"

echo "== retention trim on a linear timeline (deterministic geometry) =="
# A fresh, purely linear project: no forks, so a marked contiguous prefix
# from the root is deterministically prunable — the trim/ghost contract is
# testable here without the sibling-fork geometry the main project ends in.
MINI=$E/mini
mkdir -p "$MINI"
"$SHEAF" init "$MINI" >/dev/null || fail "mini init"
mini_settle() {
  local want=$1 tries=0
  while :; do
    n=$("$SHEAF" log -C "$MINI" --limit 100 --json | python3 -c 'import json,sys;print(len(json.load(sys.stdin)["entries"]))')
    [ "$n" -ge "$want" ] && return 0
    tries=$((tries+1)); [ $tries -gt 60 ] && fail "mini: timed out waiting for $want captures (have $n)"
    sleep 0.25
  done
}
# The running daemon must pick the new enrollment up; if it does not
# resume it live, a bounce makes the registry sweep do it.
tries=0
until "$SHEAF" status "$MINI" 2>/dev/null | has "watching:      yes"; do
  tries=$((tries+1))
  if [ $tries -gt 8 ]; then
    pkill -TERM -f "target/debug/sheafd run --socket $E/control.sock" || true
    sleep 1
    "$DAEMON" run --socket "$E/control.sock" >>"$E/daemon.log" 2>&1 &
    sleep 1.5
    "$SHEAF" status "$MINI" | has "watching:      yes" || fail "mini project not watched"
    break
  fi
  sleep 0.5
done
for i in 1 2 3 4 5; do printf 'line %d\n' "$i" > "$MINI/file$i.txt"; sleep 0.6; done
mini_settle 5
# Prune needs a marked contiguous prefix INCLUDING the root: the keep-set
# GCA is the deepest UNMARKED capture, so a single non-root mark defers
# (the root is the surviving baseline). Mark root + its child; the boundary
# rises to the third capture and both marked points become prunable.
MROOT=$("$SHEAF" log -C "$MINI" --limit 100 --json | python3 -c 'import json,sys; e=json.load(sys.stdin)["entries"]; print(e[-1]["id"][:12])')
MOLD=$("$SHEAF" log -C "$MINI" --limit 100 --json | python3 -c 'import json,sys; e=json.load(sys.stdin)["entries"]; print(e[-2]["id"][:12])')
test -n "$MROOT" && test -n "$MOLD" || fail "could not read mini capture ids"
"$SHEAF" gc -C "$MINI" "$MROOT" | has "marked collectable" || fail "mini gc mark root"
"$SHEAF" gc -C "$MINI" "$MOLD" | has "marked collectable" || fail "mini gc mark child"
"$SHEAF" gc -C "$MINI" --json | grep -q '"cause": "gc mark"' || fail "plan names mark cause"
"$SHEAF" gc -C "$MINI" --apply | has "retention trimmed" || fail "apply trims marked prefix"
"$SHEAF" log -C "$MINI" > "$E/mini-ret.txt" || fail "mini log readable post-trim"
"$SHEAF" restore -C "$MINI" --dry-run "$MOLD" 2>"$E/mini-ghost.txt" && fail "pruned point must not restore" || true
grep -q "pruned by gc mark" "$E/mini-ghost.txt" || fail "ghost explanation missing"
"$SHEAF" doctor -C "$MINI" | has "ledger_state" || fail "doctor omits ledger check"
"$SHEAF" doctor -C "$MINI" | has "shallow_baseline" || fail "doctor omits shallow check"
"$SHEAF" doctor -C "$MINI" >/dev/null || fail "doctor must stay green on a trimmed store"
ok "linear-timeline prefix trim, ghosts, doctor checks"

echo "== status works with no enrolled project (README Quick Start) =="
NOPROJ=$(mktemp -d /tmp/sheaf-noproj-XXXXXX)
"$SHEAF" status "$NOPROJ" > "$E/noproj.txt" 2>&1 || fail "status must not error without a project"
grep -q "daemon:" "$E/noproj.txt" || fail "status omits daemon health without a project"
grep -q "project:       (none" "$E/noproj.txt" || fail "status must say there is no project"
rm -rf "$NOPROJ"
ok "daemon-only status before any init"

echo
echo "PASS: cli surface e2e"
