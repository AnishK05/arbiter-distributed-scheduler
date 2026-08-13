#!/usr/bin/env python3
"""Measure Postgres-lease leader failover time (Phase 6 DoD).

Kills the current leader's container, polls `leader_lease` until another
replica holds a non-expired lease, and asserts every trial lands under one
lease TTL (default 5s). Optionally submits a job after failover to confirm
the new leader still schedules.

Usage (from repo root, with the Phase 6 stack up via `make phase6-up`):

    python3 scripts/measure_leader_failover.py
    python3 scripts/measure_leader_failover.py --trials 5 --threshold-ms 5000
    python3 scripts/measure_leader_failover.py --submit-after

Exits non-zero if any trial exceeds the threshold.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time

from arbiterctl_path import resolve_arbiterctl


CONTAINER_BY_ID = {
    "scheduler-1": "arbiter-scheduler-1",
    "scheduler-2": "arbiter-scheduler-2",
    "scheduler-3": "arbiter-scheduler-3",
}


def run(cmd: list[str]) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"command failed ({result.returncode}): {' '.join(cmd)}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
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


def host_reachable_leader_addr(advertise: str) -> str:
    """Map compose advertise addrs to host-side localhost ports."""
    return advertise.replace("host.docker.internal", "localhost")


def current_leader() -> tuple[str, str, int]:
    row = psql(
        "SELECT leader_id || '|' || leader_addr || '|' || epoch "
        "FROM leader_lease WHERE id = 1 AND expires_at > now();"
    )
    if not row:
        raise RuntimeError("no live leader lease; are the schedulers up?")
    leader_id, addr, epoch_s = row.split("|", 2)
    return leader_id, addr, int(epoch_s)


def wait_for_leader_change(
    old_id: str, old_epoch: int, poll_timeout_s: float, poll_interval_s: float
) -> tuple[float, str, str, int]:
    t0 = time.monotonic()
    deadline = t0 + poll_timeout_s
    while time.monotonic() < deadline:
        row = psql(
            "SELECT leader_id || '|' || leader_addr || '|' || epoch FROM leader_lease "
            "WHERE id = 1 AND expires_at > now();"
        )
        if row:
            leader_id, addr, epoch_s = row.split("|", 2)
            epoch = int(epoch_s)
            if leader_id != old_id and epoch > old_epoch:
                return (time.monotonic() - t0) * 1000, leader_id, addr, epoch
        time.sleep(poll_interval_s)
    raise RuntimeError(
        f"no new leader within {poll_timeout_s}s after killing {old_id!r}"
    )


def wait_for_container(container: str, timeout_s: float) -> None:
    # docker kill leaves restart:unless-stopped containers to come back; if
    # that stalls, start explicitly so the HA set remains three replicas.
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        state = run(["docker", "inspect", "-f", "{{.State.Running}}", container])
        if state == "true":
            time.sleep(1.0)
            return
        try:
            run(["docker", "start", container])
        except subprocess.CalledProcessError:
            pass
        time.sleep(0.2)
    raise RuntimeError(f"container {container!r} did not report running within {timeout_s}s")


def wait_for_ready_worker(timeout_s: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        status = psql("SELECT status FROM nodes WHERE hostname='worker-1' LIMIT 1;")
        if status == "ready":
            return
        time.sleep(0.2)
    raise RuntimeError("worker-1 did not return to ready after failover")


def submit_probe_job(scheduler_addr: str) -> None:
    wait_for_ready_worker()
    run(
        [
            resolve_arbiterctl(),
            "--scheduler-addr",
            scheduler_addr,
            "submit",
            f"phase6-failover-probe-{int(time.time())}",
            "--replicas",
            "1",
            "--cpu-millicores",
            "50",
            "--memory-mb",
            "32",
            "--command",
            "2",
            "--wait",
            "--wait-timeout",
            "60s",
        ]
    )


def run_trial(
    poll_timeout_s: float,
    poll_interval_s: float,
    restart_timeout_s: float,
    submit_after: bool,
) -> float:
    leader_id, _, epoch = current_leader()
    container = CONTAINER_BY_ID.get(leader_id)
    if not container:
        raise RuntimeError(f"unknown leader_id {leader_id!r}; expected one of {list(CONTAINER_BY_ID)}")

    print(f"  killing leader {leader_id} ({container}) epoch={epoch}")
    run(["docker", "kill", container])

    elapsed_ms, new_id, new_addr, new_epoch = wait_for_leader_change(
        leader_id, epoch, poll_timeout_s, poll_interval_s
    )
    print(f"  new leader {new_id} ({new_addr}) epoch={new_epoch} in {elapsed_ms:.1f}ms")

    # Bring the killed replica back before submit so the advertise set stays full.
    wait_for_container(container, restart_timeout_s)

    if submit_after:
        host_addr = host_reachable_leader_addr(new_addr)
        print(f"  submitting probe job via {host_addr}...")
        submit_probe_job(host_addr)
        print("  probe job succeeded")

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            current_leader()
            break
        except RuntimeError:
            time.sleep(0.2)
    return elapsed_ms


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trials", type=int, default=5)
    parser.add_argument(
        "--threshold-ms",
        type=float,
        default=6000,
        help="max acceptable election time (lease TTL + one renew interval)",
    )
    parser.add_argument("--poll-timeout-s", type=float, default=15)
    parser.add_argument("--poll-interval-s", type=float, default=0.05)
    parser.add_argument("--restart-timeout-s", type=float, default=60)
    parser.add_argument(
        "--submit-after",
        action="store_true",
        help="submit a 1-replica job after each failover to verify scheduling",
    )
    args = parser.parse_args()

    times: list[float] = []
    for i in range(1, args.trials + 1):
        print(f"trial {i}/{args.trials}")
        try:
            ms = run_trial(
                args.poll_timeout_s,
                args.poll_interval_s,
                args.restart_timeout_s,
                args.submit_after,
            )
        except Exception as exc:  # noqa: BLE001 - surface clearly for DoD runs
            print(f"FAIL: {exc}", file=sys.stderr)
            return 1
        times.append(ms)
        if ms > args.threshold_ms:
            print(
                f"FAIL: election took {ms:.1f}ms > threshold {args.threshold_ms}ms",
                file=sys.stderr,
            )
            return 1

    print(
        "ok: "
        + ", ".join(f"{t:.1f}ms" for t in times)
        + f" (max={max(times):.1f}ms, threshold={args.threshold_ms}ms)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
