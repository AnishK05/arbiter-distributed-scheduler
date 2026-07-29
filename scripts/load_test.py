#!/usr/bin/env python3
"""Submit a burst of tasks and report placement distribution (Phase 4 DoD).

Against a running Arbiter cluster (ideally the 5-worker Phase 4 overlay),
submits one job with N replicas, waits until every task has been assigned,
then prints a per-node histogram plus simple concentration stats.

Use this twice — once with `--policy bin_pack`, once with `--policy spread` —
to produce the comparison archived in docs/benchmarks/phase4-placement.md.

Usage (from repo root):

    # Bring up 5 workers:
    make phase4-up
    make build

    python3 scripts/load_test.py --replicas 100 --cpu-millicores 100 \\
        --policy bin_pack --command 60

    python3 scripts/load_test.py --replicas 100 --cpu-millicores 100 \\
        --policy spread --command 60

With 5×2000m nodes and 100×100m tasks held running (`--command 60` sleeps
60s), bin-pack should concentrate onto fewer nodes near-full while spread
should land ~20 tasks on each node.
"""

from __future__ import annotations

import argparse
import statistics
import subprocess
import sys
import time
from collections import Counter


def run(cmd: list[str], check: bool = True) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True, check=check)
    return result.stdout.strip()


def psql(query: str) -> str:
    return run(
        [
            "docker",
            "exec",
            "arbiter-postgres",
            "psql",
            "-U",
            "arbiter",
            "-d",
            "arbiter",
            "-tA",
            "-c",
            query,
        ]
    )


def submit_job(args: argparse.Namespace) -> str:
    cmd = [
        args.arbiterctl,
        "--scheduler-addr",
        args.scheduler_addr,
        "submit",
        args.name,
        "--replicas",
        str(args.replicas),
        "--cpu-millicores",
        str(args.cpu_millicores),
        "--memory-mb",
        str(args.memory_mb),
        "--scheduling-policy",
        args.policy,
        "--image",
        args.image,
    ]
    if args.command is not None:
        cmd.extend(["--command", args.command])
    out = run(cmd)
    print(out)
    # "submitted job name (uuid) replicas=N ..."
    for token in out.split():
        if token.startswith("(") and token.endswith(")"):
            return token.strip("()")
        # also accept bare uuid after name
    # Fallback: parse "submitted job X (UUID)"
    start = out.find("(")
    end = out.find(")", start + 1)
    if start < 0 or end < 0:
        raise RuntimeError(f"could not parse job id from: {out!r}")
    return out[start + 1 : end]


def wait_until_assigned(job_id: str, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        row = psql(
            f"SELECT COUNT(*) FILTER (WHERE assigned_node_id IS NULL), COUNT(*) "
            f"FROM tasks WHERE job_id = '{job_id}';"
        )
        unassigned, total = (int(x) for x in row.split("|"))
        if total > 0 and unassigned == 0:
            return
        time.sleep(0.2)
    raise TimeoutError(f"tasks for job {job_id} still unassigned after {timeout_s}s")


def placement_rows(job_id: str) -> list[tuple[str, str, str]]:
    raw = psql(
        f"""
        SELECT COALESCE(n.hostname, 'unassigned'), t.status, t.id
        FROM tasks t
        LEFT JOIN nodes n ON n.id = t.assigned_node_id
        WHERE t.job_id = '{job_id}'
        ORDER BY n.hostname NULLS FIRST, t.created_at;
        """
    )
    rows: list[tuple[str, str, str]] = []
    if not raw:
        return rows
    for line in raw.splitlines():
        host, status, task_id = line.split("|")
        rows.append((host, status, task_id))
    return rows


def print_report(policy: str, job_id: str, rows: list[tuple[str, str, str]]) -> None:
    counts = Counter(host for host, _, _ in rows)
    statuses = Counter(status for _, status, _ in rows)
    values = list(counts.values())
    nodes_used = sum(1 for v in values if v > 0)
    mean = statistics.mean(values) if values else 0.0
    stdev = statistics.pstdev(values) if len(values) > 1 else 0.0
    max_c = max(values) if values else 0
    min_c = min(values) if values else 0
    concentration = (max_c / mean) if mean else 0.0

    print()
    print(f"policy={policy} job={job_id} tasks={len(rows)}")
    print(f"status counts: {dict(statuses)}")
    print("placement by hostname:")
    for host in sorted(counts):
        bar = "#" * counts[host]
        print(f"  {host:12s} {counts[host]:4d}  {bar}")
    print(
        f"nodes_used={nodes_used} min={min_c} max={max_c} "
        f"mean={mean:.2f} pstdev={stdev:.2f} concentration(max/mean)={concentration:.3f}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arbiterctl", default="./bin/arbiterctl")
    parser.add_argument("--scheduler-addr", default="localhost:7000")
    parser.add_argument("--name", default="load-test")
    parser.add_argument("--replicas", type=int, default=100)
    parser.add_argument("--cpu-millicores", type=int, default=50)
    parser.add_argument("--memory-mb", type=int, default=32)
    parser.add_argument("--policy", choices=("bin_pack", "spread"), default="bin_pack")
    parser.add_argument("--image", default="arbiter-workload:latest")
    parser.add_argument(
        "--command",
        default="60",
        help="container CMD override (default 60 → sleep_n.py sleeps 60s so capacity stays reserved)",
    )
    parser.add_argument("--wait-timeout-s", type=float, default=60.0)
    args = parser.parse_args()

    if args.replicas < 1:
        print("--replicas must be >= 1", file=sys.stderr)
        return 2

    job_id = submit_job(args)
    wait_until_assigned(job_id, args.wait_timeout_s)
    rows = placement_rows(job_id)
    if len(rows) != args.replicas:
        print(
            f"warning: expected {args.replicas} tasks, got {len(rows)}",
            file=sys.stderr,
        )
    print_report(args.policy, job_id, rows)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(exc.stderr or str(exc))
        raise SystemExit(1) from exc
