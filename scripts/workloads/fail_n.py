#!/usr/bin/env python3
"""Trivial exit-nonzero workload for testing failed-task handling.

Usage: fail_n.py [exit_code]
Default exit code: 1.
"""

import sys


def main() -> int:
    code = 1
    if len(sys.argv) > 1:
        code = int(sys.argv[1])
    print(f"arbiter-workload fail_n: exiting with code {code}", flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
