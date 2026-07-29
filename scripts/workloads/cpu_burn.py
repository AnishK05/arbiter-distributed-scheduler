#!/usr/bin/env python3
"""Trivial CPU-burn workload for Arbiter demo tasks.

Usage: cpu_burn.py [seconds]
Default: 2 seconds. Burns a single core in a tight loop, then exits 0.
"""

import sys
import time


def main() -> int:
    seconds = 2.0
    if len(sys.argv) > 1:
        seconds = float(sys.argv[1])
    print(f"arbiter-workload cpu_burn: burning for {seconds}s", flush=True)
    deadline = time.time() + seconds
    x = 0
    while time.time() < deadline:
        x = (x + 1) % 1_000_003
    print(f"arbiter-workload cpu_burn: done (x={x})", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
