#!/usr/bin/env python3
"""Submit a burst of tasks and report placement + concurrency (Phases 4 & 10).

Against a running Arbiter cluster, submits one job with N replicas, samples
`tasks.status='running'` while the job is in flight, and prints:

  - placement histogram / concentration (Phase 4)
  - wall-clock time, peak concurrently-running count, throughput (Phase 10)

Usage (from repo root):

    make demo-up && make build

    # Section 10 concurrency benchmark (10-node demo):
    python3 scripts/load_test.py --tasks 750 --cpu-millicores 40 --memory-mb 32 \\
        --command 45 --wait-complete --policy bin_pack

    # Phase 4 placement comparison (hold capacity with long sleep):
    python3 scripts/load_test.py --replicas 100 --cpu-millicores 50 \\
        --policy bin_pack --command 60
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
    start = out.find("(")
    end = out.find(")", start + 1)
    if start < 0 or end < 0:
        raise RuntimeError(f"could not parse job id from: {out!r}")
    return out[start + 1 : end]


def wait_until_idle(timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        inflight = int(
            psql(
                "SELECT COUNT(*) FROM tasks WHERE status IN ('scheduled', 'running');"
            )
            or "0"
        )
        if inflight == 0:
            return
        time.sleep(0.5)
    raise TimeoutError(f"cluster still has inflight tasks after {timeout_s}s")


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


def count_running(job_id: str | None = None) -> int:
    if job_id:
        q = f"SELECT COUNT(*) FROM tasks WHERE job_id = '{job_id}' AND status = 'running';"
    else:
        q = "SELECT COUNT(*) FROM tasks WHERE status = 'running';"
    return int(psql(q) or "0")


def wait_until_complete(
    job_id: str, timeout_s: float, sample_interval_s: float
) -> tuple[float, int, list[int]]:
    """Wait until no pending/scheduled/running tasks remain for the job.

    Returns (wall_clock_s, peak_running, samples).
    """
    t0 = time.monotonic()
    deadline = t0 + timeout_s
    peak = 0
    samples: list[int] = []
    while time.monotonic() < deadline:
        row = psql(
            f"""
            SELECT
              COUNT(*) FILTER (WHERE status IN ('pending','scheduled','running')),
              COUNT(*) FILTER (WHERE status = 'running'),
              COUNT(*) FILTER (WHERE status = 'succeeded'),
              COUNT(*) FILTER (WHERE status IN ('failed','cancelled')),
              COUNT(*)
            FROM tasks WHERE job_id = '{job_id}';
            """
        )
        inflight, running, succeeded, failed, total = (int(x) for x in row.split("|"))
        samples.append(running)
        if running > peak:
            peak = running
        if total > 0 and inflight == 0:
            wall = time.monotonic() - t0
            print(
                f"complete: total={total} succeeded={succeeded} failed={failed} "
                f"wall_s={wall:.2f} peak_running={peak}"
            )
            return wall, peak, samples
        time.sleep(sample_interval_s)
    raise TimeoutError(f"job {job_id} still inflight after {timeout_s}s (peak_running={peak})")


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


def print_report(
    policy: str,
    job_id: str,
    rows: list[tuple[str, str, str]],
    wall_s: float | None = None,
    peak_running: int | None = None,
) -> None:
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
        bar = "#" * min(counts[host], 80)
        print(f"  {host:12s} {counts[host]:4d}  {bar}")
    print(
        f"nodes_used={nodes_used} min={min_c} max={max_c} "
        f"mean={mean:.2f} pstdev={stdev:.2f} concentration(max/mean)={concentration:.3f}"
    )
    if wall_s is not None and peak_running is not None:
        throughput = (len(rows) / wall_s) if wall_s > 0 else 0.0
        print(
            f"wall_clock_s={wall_s:.2f} peak_concurrent_running={peak_running} "
            f"throughput_tasks_per_s={throughput:.2f}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arbiterctl", default="./bin/arbiterctl")
    parser.add_argument("--scheduler-addr", default="localhost:7000")
    parser.add_argument("--name", default="load-test")
    parser.add_argument("--replicas", type=int, default=None, help="number of task replicas")
    parser.add_argument(
        "--tasks",
        type=int,
        default=None,
        help="alias for --replicas (IMPLEMENTATION_PLAN.md Section 10 naming)",
    )
    parser.add_argument("--cpu-millicores", type=int, default=50)
    parser.add_argument("--memory-mb", type=int, default=32)
    parser.add_argument("--policy", choices=("bin_pack", "spread"), default="bin_pack")
    parser.add_argument("--image", default="arbiter-workload:latest")
    parser.add_argument(
        "--command",
        default="60",
        help="container CMD override (default 60 → sleep_n.py sleeps 60s)",
    )
    parser.add_argument("--wait-timeout-s", type=float, default=120.0)
    parser.add_argument(
        "--idle-timeout-s",
        type=float,
        default=180.0,
        help="max time to wait for prior scheduled/running tasks to finish before submitting",
    )
    parser.add_argument(
        "--wait-complete",
        action="store_true",
        help="wait until the job finishes and report peak concurrent running + throughput",
    )
    parser.add_argument(
        "--complete-timeout-s",
        type=float,
        default=600.0,
        help="max time to wait for job completion when --wait-complete is set",
    )
    parser.add_argument(
        "--sample-interval-s",
        type=float,
        default=0.5,
        help="how often to sample running-task count during --wait-complete",
    )
    args = parser.parse_args()

    if args.tasks is not None and args.replicas is not None and args.tasks != args.replicas:
        print("conflict: --tasks and --replicas disagree", file=sys.stderr)
        return 2
    if args.tasks is not None:
        args.replicas = args.tasks
    if args.replicas is None:
        args.replicas = 100

    if args.replicas < 1:
        print("--replicas/--tasks must be >= 1", file=sys.stderr)
        return 2

    print("waiting for cluster idle (no scheduled/running tasks)...")
    wait_until_idle(args.idle_timeout_s)

    job_id = submit_job(args)
    wait_until_assigned(job_id, args.wait_timeout_s)

    wall_s = None
    peak_running = None
    if args.wait_complete:
        wall_s, peak_running, _ = wait_until_complete(
            job_id, args.complete_timeout_s, args.sample_interval_s
        )

    rows = placement_rows(job_id)
    if len(rows) != args.replicas:
        print(
            f"warning: expected {args.replicas} tasks, got {len(rows)}",
            file=sys.stderr,
        )
    print_report(args.policy, job_id, rows, wall_s=wall_s, peak_running=peak_running)
    if args.wait_complete and peak_running is not None and peak_running < 500:
        print(
            f"note: peak_concurrent_running={peak_running} is below the 500 resume target; "
            "increase cluster capacity or lower --cpu-millicores/--memory-mb",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(exc.stderr or str(exc))
        raise SystemExit(1) from exc
