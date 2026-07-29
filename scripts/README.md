# Scripts

Python tooling (not core services — see `IMPLEMENTATION_PLAN.md` Section 3):

- `measure_failover.py` (Phase 2) — kills a worker container N times via `docker kill`/`docker
  start` and asserts the scheduler marks it `dead` in Postgres within a threshold; see
  `docs/benchmarks/phase2-failure-detection.md`. The reassignment-aware version of this same
  methodology becomes part of the full resume-metric benchmark in Phase 5+ (Section 10).
- `load_test.py` (Phase 4) — submits a configurable burst of tasks and prints per-node placement
  distribution + concentration stats; used for the bin_pack vs spread comparison in
  `docs/benchmarks/phase4-placement.md`.
- `chaos_monkey.py` (Phase 5) — kills/pauses worker containers via the Docker Engine API
  (`docker kill` / `docker pause` / `docker unpause`) while a job is in flight; asserts the job
  still reaches all-succeeded with no duplicate `task_succeeded` events.
- `workloads/` (Phase 3) — trivial example programs (`sleep_n.py`, `cpu_burn.py`, `fail_n.py`) baked
  into `arbiter-workload:latest` (`Dockerfile.workload`) so submitted tasks have something to
  actually execute. Default ENTRYPOINT sleeps 2s and exits 0. `sleep_n.py` prints
  `ARBITER_TASK_ID` / `ARBITER_RUN_ID` for Phase 5 duplicate-execution checks.
