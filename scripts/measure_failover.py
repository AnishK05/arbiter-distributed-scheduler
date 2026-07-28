#!/usr/bin/env python3
"""Measure heartbeat-based failure-detection time (Phase 2 DoD).

Kills a worker container N times, polls Postgres for the corresponding
`nodes.status` to flip to 'dead', and asserts every trial lands under a
configurable threshold. This is the automated form of the manual methodology
described in docs/benchmarks/phase2-failure-detection.md, and the same
approach is reused (with reassignment added) for the full resume-metric
failover benchmark in Phase 5+ (see IMPLEMENTATION_PLAN.md Section 10).

Usage (from repo root, with the dev stack already up via `make up`):

    python3 scripts/measure_failover.py
    python3 scripts/measure_failover.py --trials 10 --threshold-ms 3000
    python3 scripts/measure_failover.py --container arbiter-worker-1 --hostname worker-1

Exits non-zero if any trial exceeds the threshold, or if the node never
reaches 'dead' within --poll-timeout-s.
"""

import argparse
import subprocess
import sys
import time


def run(cmd: list[str]) -> str:
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
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


def get_node_id(hostname: str) -> str:
    node_id = psql(f"SELECT id FROM nodes WHERE hostname='{hostname}';")
    if not node_id:
        raise RuntimeError(
            f"no node found with hostname={hostname!r}; is the worker registered yet?"
        )
    return node_id


def get_status(node_id: str) -> str:
    return psql(f"SELECT status FROM nodes WHERE id='{node_id}';")


def wait_for_container(container: str, timeout_s: float) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        state = run(["docker", "inspect", "-f", "{{.State.Running}}", container])
        if state == "true":
            return
        time.sleep(0.1)
    raise RuntimeError(f"container {container!r} did not report running within {timeout_s}s")


def run_trial(container: str, node_id: str, poll_timeout_s: float, poll_interval_s: float) -> float:
    t0 = time.monotonic()
    run(["docker", "kill", container])

    deadline = t0 + poll_timeout_s
    while time.monotonic() < deadline:
        if get_status(node_id) == "dead":
            return (time.monotonic() - t0) * 1000
        time.sleep(poll_interval_s)
    raise TimeoutError(
        f"node {node_id} was not marked dead within {poll_timeout_s}s of killing {container!r}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--container", default="arbiter-worker-1", help="worker container name to kill each trial")
    parser.add_argument("--hostname", default="worker-1", help="node hostname (as registered with the scheduler)")
    parser.add_argument("--trials", type=int, default=5, help="number of kill/restart cycles to measure")
    parser.add_argument("--threshold-ms", type=float, default=3000, help="each trial must land under this many ms")
    parser.add_argument("--poll-interval-s", type=float, default=0.05, help="how often to poll node status")
    parser.add_argument("--poll-timeout-s", type=float, default=10, help="max time to wait for a trial's dead transition")
    parser.add_argument("--restart-wait-s", type=float, default=3, help="time to let the worker re-register between trials")
    args = parser.parse_args()

    print(f"Resolving node id for hostname={args.hostname!r}...")
    node_id = get_node_id(args.hostname)
    print(f"  node_id={node_id}")

    elapsed_ms: list[float] = []
    failed = False

    for trial in range(1, args.trials + 1):
        status = get_status(node_id)
        if status != "ready":
            print(f"[trial {trial}] waiting for node to be 'ready' before killing (currently {status!r})...")
            deadline = time.monotonic() + args.restart_wait_s
            while time.monotonic() < deadline and get_status(node_id) != "ready":
                time.sleep(0.1)

        try:
            elapsed = run_trial(args.container, node_id, args.poll_timeout_s, args.poll_interval_s)
        except TimeoutError as exc:
            print(f"[trial {trial}] FAIL: {exc}")
            failed = True
            break

        verdict = "OK" if elapsed <= args.threshold_ms else "FAIL (over threshold)"
        print(f"[trial {trial}] killed {args.container!r} -> marked dead in {elapsed:.0f}ms [{verdict}]")
        elapsed_ms.append(elapsed)
        if elapsed > args.threshold_ms:
            failed = True

        # Bring the worker back so the next trial has a live node to kill.
        run(["docker", "start", args.container])
        wait_for_container(args.container, args.poll_timeout_s)
        time.sleep(args.restart_wait_s)

    if elapsed_ms:
        print()
        print(f"Trials: {len(elapsed_ms)}/{args.trials}")
        print(f"  min={min(elapsed_ms):.0f}ms  max={max(elapsed_ms):.0f}ms  avg={sum(elapsed_ms)/len(elapsed_ms):.0f}ms")
        print(f"  threshold={args.threshold_ms:.0f}ms")

    if failed:
        print(f"\nFAILED: at least one trial exceeded the {args.threshold_ms:.0f}ms threshold.")
        return 1

    print(f"\nPASSED: all {len(elapsed_ms)} trials stayed under {args.threshold_ms:.0f}ms.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
