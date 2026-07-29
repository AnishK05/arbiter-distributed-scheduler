#!/usr/bin/env python3
"""Trivial sleep workload for Arbiter demo tasks.

Usage: sleep_n.py [seconds]
Default: 2 seconds. Always exits 0.

Prints ARBITER_TASK_ID / ARBITER_RUN_ID when set so Phase 5 chaos runs can
detect duplicate executions of the same task.
"""

import os
import sys
import time


def main() -> int:
    seconds = 2.0
    if len(sys.argv) > 1:
        seconds = float(sys.argv[1])
    task_id = os.environ.get("ARBITER_TASK_ID", "")
    run_id = os.environ.get("ARBITER_RUN_ID", "")
    print(
        f"arbiter-workload sleep_n: task_id={task_id} run_id={run_id} sleeping {seconds}s",
        flush=True,
    )
    time.sleep(seconds)
    print(
        f"arbiter-workload sleep_n: done task_id={task_id} run_id={run_id}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
