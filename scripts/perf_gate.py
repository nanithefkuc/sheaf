#!/usr/bin/env python3
"""Performance regression gate for sheaf.

Runs a fixed workload against the release binaries and compares every
metric against the budgets in scripts/perf-budgets.json. CI runs this on
every push; a breach fails the build.

The budgets are a ratchet, not a snapshot: after an optimization lands,
rerun with `--update` and the file tightens to measured*slack — it never
widens. Widening a budget requires `--allow-widen` and should carry a
reason in the commit message. As the performance frontier moves forward,
so does the window.

Metrics (class decides the slack factor):
  boot_ready_ms          daemon spawn -> client can talk to it
  initial_capture_ms     enrolled tree fully captured (diff vs @ clean)
  edit_settle_max_ms     worst round: last edit -> diff clean (debounce+persist)
  read_log_ms            `sheaf log --json --limit 50`        (median of 3)
  read_diff_ms           `sheaf diff --json @~5..@`           (median of 3)
  read_grep_ms           `sheaf grep <needle> --json`         (median of 3)
  checkpoint_create_ms   `sheaf checkpoint create base` (flush + annotation)
  restore_plan_ms        `sheaf restore --at checkpoint:base --dry-run`
  restore_apply_ms       `sheaf restore --at checkpoint:base` (whole tree rewritten)
  store_bytes            bytes under .sheaf/store after the workload
  daemon_rss_kb          daemon VmRSS after the workload

Usage:
  python3 scripts/perf_gate.py                 # run + compare, exit 1 on breach
  python3 scripts/perf_gate.py --update        # tighten budgets to measured*slack
  python3 scripts/perf_gate.py --update --allow-widen
  python3 scripts/perf_gate.py --keep          # keep the sandbox for debugging
"""

import argparse
import json
import os
import pathlib
import random
import shutil
import signal
import statistics
import subprocess
import sys
import tempfile
import time

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parent
DEFAULT_BUDGETS = HERE / "perf-budgets.json"

N_TEXT_FILES = 1200
N_BIN_FILES = 40
EDIT_ROUNDS = 8
EDITS_PER_ROUND = 150
NEEDLE = "needleword"

# Slack applied when --update writes a budget: wall-clock metrics carry
# headroom because CI runners are slower and noisier than a dev laptop;
# byte sizes are near-deterministic for a fixed workload.
SLACK = {"time_ms": 2.5, "bytes": 1.15, "rss_kb": 2.0}

METRIC_CLASS = {
    "boot_ready_ms": "time_ms",
    "initial_capture_ms": "time_ms",
    "edit_settle_max_ms": "time_ms",
    "read_log_ms": "time_ms",
    "read_diff_ms": "time_ms",
    "read_grep_ms": "time_ms",
    "restore_plan_ms": "time_ms",
    "restore_apply_ms": "time_ms",
    "store_bytes": "bytes",
    "daemon_rss_kb": "rss_kb",
}


def metric_class(name: str) -> str:
    if name in METRIC_CLASS:
        return METRIC_CLASS[name]
    if name.endswith("_ms"):
        return "time_ms"
    if name.endswith("_bytes"):
        return "bytes"
    if name.endswith("_rss_kb"):
        return "rss_kb"
    raise SystemExit(f"unknown metric class for {name!r}")


def run(cmd, cwd, env, timeout=120, check=True):
    proc = subprocess.run(
        cmd, cwd=cwd, env=env, timeout=timeout,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    if check and proc.returncode != 0:
        sys.stderr.write(proc.stderr.decode(errors="replace"))
        raise SystemExit(f"command failed ({proc.returncode}): {' '.join(map(str, cmd))}")
    return proc


def timed_ms(cmd, cwd, env, timeout=120, repeat=1):
    """Wall-clock ms of `cmd`; median over `repeat` runs."""
    samples = []
    for _ in range(repeat):
        start = time.monotonic()
        run(cmd, cwd=cwd, env=env, timeout=timeout)
        samples.append((time.monotonic() - start) * 1000.0)
    return statistics.median(samples)
def diff_pending(proc):
    try:
        value = json.loads(proc.stdout)
        return len(value.get("diff", {}).get("entries", []))
    except json.JSONDecodeError:
        return None  # transient (streaming hiccup): treat as "not clean yet"



def settle_ms(sheaf, proj, env, started, timeout_s=90.0):
    """Ms from `started` until the worktree is fully captured."""
    deadline = started + timeout_s
    while time.monotonic() < deadline:
        proc = run([sheaf, "diff", "--json"], cwd=proj, env=env, check=False)
        if proc.returncode == 0 and diff_pending(proc) == 0:
            return (time.monotonic() - started) * 1000.0
        time.sleep(0.1)
    raise SystemExit("worktree never settled: diff vs @ stayed dirty past the deadline")


def daemon_pid_for_socket(sock: str):
    for entry in pathlib.Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            cmdline = (entry / "cmdline").read_bytes().split(b"\0")
        except OSError:
            continue
        args = [a.decode(errors="replace") for a in cmdline if a]
        if len(args) >= 3 and args[0].endswith("sheafd") and sock in args:
            return int(entry.name)
    return None


def daemon_rss_kb(sock: str):
    pid = daemon_pid_for_socket(sock)
    if pid is None:
        raise SystemExit("cannot find sheafd process for RSS sampling")
    for line in pathlib.Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1])
    raise SystemExit("VmRSS missing from daemon status")


def tree_bytes(root: pathlib.Path) -> int:
    total = 0
    for path, _dirs, files in os.walk(root):
        for name in files:
            try:
                total += (pathlib.Path(path) / name).stat().st_size
            except OSError:
                pass
    return total


def build_workload(proj: pathlib.Path):
    rng = random.Random(20260901)
    dirs = ["src", "src/util", "src/deep/nested", "tests", "docs", "lib"]
    text_files = []
    for d in dirs:
        (proj / d).mkdir(parents=True, exist_ok=True)
    for i in range(N_TEXT_FILES):
        d = dirs[i % len(dirs)]
        path = proj / d / f"file{i:04d}.rs"
        lines = [f"// module {i} variant {rng.randrange(1 << 20)}"]
        lines += [f"pub fn f{j}() -> u{8 * (1 + j % 4)} {{ {rng.randrange(1 << 30)} }}" for j in range(18)]
        if i % 97 == 0:
            lines.append(f"// {NEEDLE} occurrence {i}")
        path.write_text("\n".join(lines) + "\n")
        text_files.append(path)
    for i in range(N_BIN_FILES):
        (proj / "assets").mkdir(exist_ok=True)
        (proj / "assets" / f"blob{i:02d}.bin").write_bytes(
            bytes(rng.randrange(256) for _ in range(16 * 1024))
        )
    return text_files


def measure(bin_dir: pathlib.Path, keep: bool):
    sheaf = os.environ.get("PERF_SHEAF") or str(bin_dir / "sheaf")
    sheafd = os.environ.get("PERF_SHEAFD") or str(bin_dir / "sheafd")
    for binary in (sheaf, sheafd):
        if not pathlib.Path(binary).exists():
            raise SystemExit(f"binary not found: {binary} (build with `cargo build --release --workspace --bins`)")

    sandbox = pathlib.Path(tempfile.mkdtemp(prefix="sheaf-perf-"))
    proj = sandbox / "proj"
    proj.mkdir()
    env = dict(os.environ)
    env["XDG_DATA_HOME"] = str(sandbox / "data")
    env["SHEAF_SOCKET"] = str(sandbox / "control.sock")
    (sandbox / "data").mkdir()
    socket = env["SHEAF_SOCKET"]

    daemon_log = open(sandbox / "daemon.log", "wb")
    daemon = None
    results = {}
    try:
        text_files = build_workload(proj)

        run([sheaf, "init", str(proj)], cwd=proj, env=env)

        boot_start = time.monotonic()
        daemon = subprocess.Popen(
            [sheafd, "run", "--socket", socket],
            cwd=proj, env=env, stdout=daemon_log, stderr=daemon_log,
        )
        while not pathlib.Path(socket).exists():
            if time.monotonic() - boot_start > 30:
                raise SystemExit("daemon socket never appeared")
            time.sleep(0.05)
        while True:
            proc = run([sheaf, "status"], cwd=proj, env=env, check=False)
            if proc.returncode == 0:
                break
            if time.monotonic() - boot_start > 30:
                raise SystemExit("daemon never became reachable")
            time.sleep(0.05)
        results["boot_ready_ms"] = (time.monotonic() - boot_start) * 1000.0

        capture_start = time.monotonic()
        results["initial_capture_ms"] = settle_ms(sheaf, proj, env, capture_start)
        results["checkpoint_create_ms"] = timed_ms(
            [sheaf, "checkpoint", "create", "base"], cwd=proj, env=env)

        worst_round = 0.0
        total = len(text_files)
        for rnd in range(EDIT_ROUNDS):
            edited_at = time.monotonic()
            for k in range(EDITS_PER_ROUND):
                path = text_files[(rnd * EDITS_PER_ROUND + k) % total]
                with open(path, "a") as fh:
                    fh.write(f"// edit r{rnd} k{k} {rnd * EDITS_PER_ROUND + k}\n")
            worst_round = max(worst_round, settle_ms(sheaf, proj, env, edited_at))
        results["edit_settle_max_ms"] = worst_round

        results["read_log_ms"] = timed_ms(
            [sheaf, "log", "--json", "--limit", "50"], cwd=proj, env=env, repeat=3)
        results["read_diff_ms"] = timed_ms(
            [sheaf, "diff", "--json", "@~5..@"], cwd=proj, env=env, repeat=3)
        results["read_grep_ms"] = timed_ms(
            [sheaf, "grep", NEEDLE, "--json"], cwd=proj, env=env, repeat=3)

        results["restore_plan_ms"] = timed_ms(
            [sheaf, "restore", "--at", "checkpoint:base", "--dry-run"], cwd=proj, env=env)

        apply_start = time.monotonic()
        run([sheaf, "restore", "--at", "checkpoint:base"], cwd=proj, env=env)
        results["restore_apply_ms"] = (time.monotonic() - apply_start) * 1000.0
        # The restore's own capture must land too before size sampling.
        settle_ms(sheaf, proj, env, time.monotonic(), timeout_s=60.0)

        results["store_bytes"] = tree_bytes(proj / ".sheaf" / "store")
        results["daemon_rss_kb"] = daemon_rss_kb(socket)
    finally:
        if daemon is not None and daemon.poll() is None:
            daemon.send_signal(signal.SIGTERM)
            try:
                daemon.wait(timeout=15)
            except subprocess.TimeoutExpired:
                daemon.kill()
                daemon.wait()
        daemon_log.close()
        if keep:
            print(f"sandbox kept at {sandbox}", file=sys.stderr)
        else:
            shutil.rmtree(sandbox, ignore_errors=True)
    return results


def fmt(value, cls):
    if cls == "bytes":
        return f"{value/1024:.0f} KiB"
    return f"{value:.0f}"


def compare(results, budgets):
    have = budgets.get("budgets", {})
    failures = []
    rows = []
    for name, measured in results.items():
        cls = metric_class(name)
        if name not in have:
            rows.append((name, fmt(measured, cls), "(none)", "-", "NEW"))
            continue
        budget = float(have[name])
        ratio = measured / budget
        status = "PASS" if ratio <= 1.0 else "FAIL"
        if status == "FAIL":
            failures.append(name)
        rows.append((name, fmt(measured, cls), fmt(budget, cls), f"{ratio:.2f}", status))

    width = max(len(r[0]) for r in rows)
    print(f"{'metric'.ljust(width)}  {'measured':>12}  {'budget':>12}  {'ratio':>5}  status")
    for name, measured, budget, ratio, status in rows:
        print(f"{name.ljust(width)}  {measured:>12}  {budget:>12}  {ratio:>5}  {status}")
    return failures


def update(results, path: pathlib.Path, allow_widen: bool):
    doc = {}
    if path.exists():
        doc = json.loads(path.read_text())
    doc.setdefault(
        "comment",
        "Performance ratchet: tighten with `python3 scripts/perf_gate.py --update` "
        "after optimizations; widening requires --allow-widen and a stated reason "
        "in the commit that does it.",
    )
    doc.setdefault("slack", SLACK)
    budgets = doc.setdefault("budgets", {})
    for name, measured in results.items():
        cls = metric_class(name)
        proposed = round(measured * SLACK[cls])
        if name not in budgets:
            budgets[name] = proposed
            print(f"  {name}: new budget {proposed} ({cls})")
            continue
        current = float(budgets[name])
        # Tighten only on real improvements: a 5% guard keeps run-to-run
        # noise from slowly ratcheting budgets below what CI can hold.
        if proposed < current * 0.95:
            print(f"  {name}: tightened {budgets[name]} -> {proposed} ({cls})")
            budgets[name] = proposed
        elif proposed > current and allow_widen:
            print(f"  {name}: WIDENED {budgets[name]} -> {proposed} ({cls})")
            budgets[name] = proposed
        else:
            print(f"  {name}: kept {budgets[name]} (measured {proposed}; widening needs --allow-widen)")
    doc["budgets"] = budgets
    path.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
    print(f"budgets written to {path} — commit the file with your change")

def set_budgets(path: pathlib.Path, pairs):
    """Write budgets from explicit NAME=VALUE pairs — no workload run.

    The sanctioned path for CI-derived numbers: budgets are enforced on
    the slowest hardware that runs the gate (CI runners), so tightening
    from a fast dev laptop via --update would re-break CI. Take the
    measured values a CI run prints, apply the class slack yourself, and
    set them here.
    """
    doc = {}
    if path.exists():
        doc = json.loads(path.read_text())
    doc.setdefault(
        "comment",
        "Performance ratchet: tighten with `python3 scripts/perf_gate.py --update` "
        "after optimizations; widening requires --allow-widen and a stated reason "
        "in the commit that does it.",
    )
    doc.setdefault("slack", SLACK)
    budgets = doc.setdefault("budgets", {})
    for pair in pairs:
        name, sep, raw = pair.partition("=")
        if not sep:
            raise SystemExit(f"--set expects NAME=VALUE, got {pair!r}")
        try:
            value = float(raw)
        except ValueError:
            raise SystemExit(f"--set value must be numeric, got {raw!r}")
        if value <= 0:
            raise SystemExit(f"--set value must be positive, got {raw!r}")
        cls = metric_class(name)  # rejects unknown metrics
        old = budgets.get(name)
        budgets[name] = int(round(value))
        if old is None:
            print(f"  {name}: new budget {budgets[name]} ({cls})")
        elif budgets[name] < float(old):
            print(f"  {name}: tightened {old} -> {budgets[name]} ({cls})")
        else:
            print(f"  {name}: set {old} -> {budgets[name]} ({cls})")
    doc["budgets"] = budgets
    path.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
    print(f"budgets written to {path} — commit the file with your change")



def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--budgets", type=pathlib.Path, default=DEFAULT_BUDGETS)
    parser.add_argument("--bin", type=pathlib.Path, default=REPO / "target" / "release")
    parser.add_argument("--update", action="store_true",
                        help="rewrite budgets to measured*slack (tighten-only)")
    parser.add_argument("--allow-widen", action="store_true",
                        help="with --update: permit widening budgets")
    parser.add_argument("--keep", action="store_true",
                        help="keep the sandbox directory for debugging")
    parser.add_argument("--set", action="append", metavar="NAME=VALUE",
                        help="write budgets directly (e.g. from a CI run's "
                             "measured table); no workload is run")
    args = parser.parse_args()

    cpu = ""
    try:
        for line in pathlib.Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    print(f"# host: {cpu or 'unknown'} x{os.cpu_count()}")
    if args.set:
        set_budgets(args.budgets, args.set)
        return

    results = measure(args.bin, args.keep)
    budgets = {}
    if args.budgets.exists():
        budgets = json.loads(args.budgets.read_text())
    failures = compare(results, budgets)
    if args.update:
        update(results, args.budgets, args.allow_widen)
    if failures:
        print(f"FAIL: {len(failures)} metric(s) over budget: {', '.join(failures)}")
        sys.exit(1)
    print("PASS: all budgeted metrics within their window")


if __name__ == "__main__":
    main()
