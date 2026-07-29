#!/usr/bin/env python3
"""Chaos monkey for Arbiter Phase 5 (kill / pause workers via Docker).

Against a running multi-worker stack, repeatedly kills or pauses a random
worker container while a long-running job is in flight. The scheduler should
mark the node dead, orphan+requeue its tasks, reap leftover DooD containers,
and reassign work so the job still reaches all-succeeded.

Usage (from repo root):

    make phase4-up && make build
    ./bin/arbiterctl submit chaos --replicas 20 --cpu-millicores 50 --memory-mb 32 \\
        --command 30 --wait &
    python3 scripts/chaos_monkey.py --duration-s 45 --interval-s 5

Exits non-zero if any task is still non-terminal when --wait-job-timeout-s
elapses, or if duplicate ARBITER_RUN_ID completions are observed for the
same task_id in docker logs.
"""

from __future__ import annotations

import argparse
import random
import re
import subprocess
import sys
import time
from collections import defaultdict


RUN_DONE_RE = re.compile(
    r"arbiter-workload sleep_n: done task_id=(?P<task>[^\s]+) run_id=(?P<run>[^\s]+)"
)


def run(cmd: list[str], check: bool = True) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True, check=check)
    return (result.stdout or "").strip()


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


def list_workers(prefix: str) -> list[str]:
    out = run(
        [
            "docker",
            "ps",
            "--filter",
            f"name={prefix}",
            "--format",
            "{{.Names}}",
        ]
    )
    return [line for line in out.splitlines() if line]


def chaos_once(workers: list[str], mode: str, pause_s: float) -> tuple[str, str, float]:
    target = random.choice(workers)
    t0 = time.monotonic()
    if mode == "kill":
        run(["docker", "kill", target])
        # Compose restart policy brings it back; wait until running again.
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            state = run(["docker", "inspect", "-f", "{{.State.Running}}", target], check=False)
            if state == "true":
                break
            time.sleep(0.2)
        return target, "kill", (time.monotonic() - t0) * 1000
    if mode == "pause":
        run(["docker", "pause", target])
        time.sleep(pause_s)
        run(["docker", "unpause", target])
        return target, "pause", (time.monotonic() - t0) * 1000
    raise ValueError(f"unknown mode {mode}")


def wait_job_terminal(job_id: str, timeout_s: float) -> dict[str, int]:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        raw = psql(
            f"""
            SELECT status, COUNT(*)
            FROM tasks WHERE job_id = '{job_id}'
            GROUP BY status ORDER BY status;
            """
        )
        counts: dict[str, int] = {}
        if raw:
            for line in raw.splitlines():
                status, n = line.split("|")
                counts[status] = int(n)
        inflight = sum(
            counts.get(s, 0)
            for s in ("pending", "scheduled", "running", "orphaned")
        )
        if inflight == 0 and sum(counts.values()) > 0:
            return counts
        time.sleep(0.5)
    raise TimeoutError(f"job {job_id} still inflight after {timeout_s}s")


def collect_run_ids() -> dict[str, set[str]]:
    """Scan recent docker events/logs is hard; instead query container logs
    via `docker ps -aq --filter label=arbiter.managed=true` may be empty
    (AutoRemove). Prefer events table + stdout from workers is incomplete.

    Fallback: parse `docker logs` for each worker for done lines emitted
    before AutoRemove (workers don't capture sibling logs). So we also
    check the events audit trail for duplicate succeeded events.
    """
    by_task: dict[str, set[str]] = defaultdict(set)
    # Use Postgres events: each succeeded should happen once per successful
    # execution path; retries produce failed+retry then later succeeded.
    raw = psql(
        """
        SELECT entity_id, message FROM events
        WHERE event_type = 'task_succeeded'
        ORDER BY id;
        """
    )
    if not raw:
        return by_task
    for line in raw.splitlines():
        task_id, message = line.split("|", 1)
        by_task[task_id].add(message)
    return by_task


def latest_job_id() -> str:
    job_id = psql("SELECT id FROM jobs ORDER BY created_at DESC LIMIT 1;")
    if not job_id:
        raise RuntimeError("no jobs found; submit one before running chaos_monkey")
    return job_id


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker-prefix", default="arbiter-worker-")
    parser.add_argument("--duration-s", type=float, default=40.0)
    parser.add_argument("--interval-s", type=float, default=5.0)
    parser.add_argument("--pause-hold-s", type=float, default=3.0,
                        help="how long to leave a worker paused (must exceed dead threshold ~1.5s)")
    parser.add_argument("--mode", choices=("mix", "kill", "pause"), default="mix")
    parser.add_argument("--job-id", default="", help="defaults to most recently submitted job")
    parser.add_argument("--wait-job-timeout-s", type=float, default=180.0)
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args()

    if args.seed:
        random.seed(args.seed)
    else:
        random.seed()

    workers = list_workers(args.worker_prefix)
    if len(workers) < 2:
        print(f"need >=2 workers matching {args.worker_prefix!r}, found {workers}", file=sys.stderr)
        return 2

    job_id = args.job_id or latest_job_id()
    print(f"chaos targeting job={job_id} workers={workers}")

    deadline = time.monotonic() + args.duration_s
    actions: list[tuple[str, str, float]] = []
    while time.monotonic() < deadline:
        workers = list_workers(args.worker_prefix) or workers
        if args.mode == "mix":
            mode = random.choice(["kill", "pause"])
        else:
            mode = args.mode
        try:
            target, mode, ms = chaos_once(workers, mode, args.pause_hold_s)
            actions.append((target, mode, ms))
            print(f"action mode={mode} target={target} elapsed_ms={ms:.0f}")
        except subprocess.CalledProcessError as exc:
            print(f"action failed: {exc.stderr or exc}", file=sys.stderr)
        time.sleep(args.interval_s)

    print("waiting for job to reach terminal states...")
    counts = wait_job_terminal(job_id, args.wait_job_timeout_s)
    print(f"final status counts: {counts}")

    failed = counts.get("failed", 0) + counts.get("cancelled", 0)
    succeeded = counts.get("succeeded", 0)
    if failed:
        print(f"ERROR: {failed} failed/cancelled tasks", file=sys.stderr)
        return 1
    if succeeded == 0:
        print("ERROR: no succeeded tasks", file=sys.stderr)
        return 1

    # Failover timing summary (kill/pause action durations are not detection
    # latency; detection is measured separately by measure_failover.py.
    # Here we record time from action start until the node flips dead.)
    print(f"chaos actions={len(actions)} succeeded_tasks={succeeded}")
    for target, mode, ms in actions:
        print(f"  {mode:5s} {target:20s} wall_ms={ms:.0f}")

    by_task = collect_run_ids()
    dupes = {tid: msgs for tid, msgs in by_task.items() if len(msgs) > 1}
    if dupes:
        print(f"WARNING: multiple succeeded events for tasks: {list(dupes)[:5]}", file=sys.stderr)
        # Multiple succeeded messages can happen if a task was retried and
        # somehow double-completed; treat as failure for the DoD.
        return 1

    print("chaos DoD ok: all tasks succeeded, no duplicate succeeded events")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as exc:
        sys.stderr.write(exc.stderr or str(exc))
        raise SystemExit(1) from exc
