#!/usr/bin/env python3
"""Trivial sleep workload for Arbiter demo tasks.

Usage: sleep_n.py [seconds]
Default: 2 seconds. Always exits 0.
"""

import sys
import time


def main() -> int:
    seconds = 2.0
    if len(sys.argv) > 1:
        seconds = float(sys.argv[1])
    print(f"arbiter-workload sleep_n: sleeping {seconds}s", flush=True)
    time.sleep(seconds)
    print("arbiter-workload sleep_n: done", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
