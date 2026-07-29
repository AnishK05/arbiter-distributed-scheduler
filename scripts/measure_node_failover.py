#!/usr/bin/env python3
"""Measure worker kill → dead → reassignment failover (Section 10).

With a steady job holding running tasks across the demo cluster, kills a
worker that currently has running tasks, then measures:

  1) detection_ms  — T0 → nodes.status='dead'
  2) reassign_ms   — T0 → a previously assigned task reaches 'running' on
                     a different node (or 'succeeded' after reassignment)

Runs N trials and prints p50/p95/max. Used for the resume "sub-3s failover"
claim (detection p95); reassignment is reported separately.

Usage (demo cluster up via `make demo-up`):

    make build
    ./bin/arbiterctl submit steady --replicas 80 --cpu-millicores 40 \\
        --memory-mb 32 --command 120
    python3 scripts/measure_node_failover.py --trials 20 --threshold-ms 3000
"""

from __future__ import annotations

import argparse
import statistics
import subprocess
import sys
import time


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


def wait_for_container(container: str, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        state = run(["docker", "inspect", "-f", "{{.State.Running}}", container], check=False)
        if state == "true":
            # Also wait until node is ready again after restart.
            return
        # docker start if exists but stopped
        run(["docker", "start", container], check=False)
        time.sleep(0.5)
    raise RuntimeError(f"container {container} did not become running within {timeout_s}s")


def wait_node_ready(hostname: str, timeout_s: float) -> str:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        row = psql(
            f"SELECT id, status FROM nodes WHERE hostname='{hostname}' ORDER BY created_at DESC LIMIT 1;"
        )
        if row and "|" in row:
            node_id, status = row.split("|", 1)
            if status == "ready":
                return node_id
        time.sleep(0.3)
    raise RuntimeError(f"node {hostname} never became ready")


def pick_victim() -> tuple[str, str, list[str]]:
    """Return (hostname, container, task_ids) for a ready node with running tasks."""
    raw = psql(
        """
        SELECT n.hostname, n.id, t.id
        FROM tasks t
        JOIN nodes n ON n.id = t.assigned_node_id
        WHERE t.status = 'running' AND n.status = 'ready'
          AND n.hostname LIKE 'worker-%'
        ORDER BY n.hostname, t.created_at
        LIMIT 50;
        """
    )
    if not raw:
        raise RuntimeError("no running tasks on ready workers; submit a long-running job first")
    by_host: dict[str, list[str]] = {}
    for line in raw.splitlines():
        host, _node_id, task_id = line.split("|")
        by_host.setdefault(host, []).append(task_id)
    hostname = max(by_host, key=lambda h: len(by_host[h]))
    # Compose container names: arbiter-worker-1 … arbiter-worker-10
    container = f"arbiter-{hostname}"
    return hostname, container, by_host[hostname]


def wait_dead(node_id: str, t0: float, timeout_s: float) -> float:
    deadline = t0 + timeout_s
    while time.monotonic() < deadline:
        if psql(f"SELECT status FROM nodes WHERE id='{node_id}';") == "dead":
            return (time.monotonic() - t0) * 1000
        time.sleep(0.05)
    raise TimeoutError("node never marked dead")


def wait_reassigned(task_ids: list[str], old_node_id: str, t0: float, timeout_s: float) -> float:
    ids = ",".join(f"'{t}'" for t in task_ids[:20])
    deadline = t0 + timeout_s
    while time.monotonic() < deadline:
        row = psql(
            f"""
            SELECT COUNT(*) FROM tasks
            WHERE id IN ({ids})
              AND (
                (status = 'running' AND assigned_node_id IS DISTINCT FROM '{old_node_id}')
                OR status = 'succeeded'
              );
            """
        )
        if int(row or "0") > 0:
            return (time.monotonic() - t0) * 1000
        time.sleep(0.1)
    raise TimeoutError("no task reassigned/succeeded after kill")


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    k = (len(ordered) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(ordered) - 1)
    if f == c:
        return ordered[f]
    return ordered[f] + (ordered[c] - ordered[f]) * (k - f)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trials", type=int, default=20)
    parser.add_argument("--threshold-ms", type=float, default=3000.0,
                        help="assert detection p95 under this many ms")
    parser.add_argument("--poll-timeout-s", type=float, default=15.0)
    parser.add_argument("--restart-timeout-s", type=float, default=60.0)
    parser.add_argument("--settle-s", type=float, default=3.0,
                        help="pause after worker restart before next trial")
    args = parser.parse_args()

    detections: list[float] = []
    reassigns: list[float] = []

    for i in range(1, args.trials + 1):
        hostname, container, task_ids = pick_victim()
        node_id = psql(f"SELECT id FROM nodes WHERE hostname='{hostname}' AND status='ready' LIMIT 1;")
        if not node_id:
            raise RuntimeError(f"ready node id missing for {hostname}")

        print(f"trial {i}/{args.trials}: kill {container} ({len(task_ids)} running tasks)")
        t0 = time.monotonic()
        run(["docker", "kill", container])
        det = wait_dead(node_id, t0, args.poll_timeout_s)
        rea = wait_reassigned(task_ids, node_id, t0, args.poll_timeout_s)
        detections.append(det)
        reassigns.append(rea)
        print(f"  detection_ms={det:.1f} reassign_ms={rea:.1f}")

        wait_for_container(container, args.restart_timeout_s)
        wait_node_ready(hostname, args.restart_timeout_s)
        time.sleep(args.settle_s)

    print()
    print(
        f"detection_ms: n={len(detections)} "
        f"p50={percentile(detections, 50):.1f} "
        f"p95={percentile(detections, 95):.1f} "
        f"max={max(detections):.1f} "
        f"mean={statistics.mean(detections):.1f}"
    )
    print(
        f"reassign_ms:  n={len(reassigns)} "
        f"p50={percentile(reassigns, 50):.1f} "
        f"p95={percentile(reassigns, 95):.1f} "
        f"max={max(reassigns):.1f} "
        f"mean={statistics.mean(reassigns):.1f}"
    )
    p95 = percentile(detections, 95)
    if p95 > args.threshold_ms:
        print(f"FAIL: detection p95 {p95:.1f}ms > threshold {args.threshold_ms:.1f}ms", file=sys.stderr)
        return 1
    print(f"ok: detection p95 {p95:.1f}ms <= {args.threshold_ms:.1f}ms")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (subprocess.CalledProcessError, RuntimeError, TimeoutError) as exc:
        print(exc, file=sys.stderr)
        raise SystemExit(1) from exc
