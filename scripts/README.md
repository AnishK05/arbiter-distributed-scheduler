# Scripts

Python tooling (not core services — see `IMPLEMENTATION_PLAN.md` Section 3):

- `load_test.py` (Phase 4) — submits a configurable burst of tasks and reports throughput/peak
  concurrency; used to produce the resume-metric benchmarks in Section 10 of the plan.
- `chaos_monkey.py` (Phase 5) — kills/pauses worker containers via the Docker Engine API
  (`docker kill` / `docker pause` / `docker unpause`) to exercise failure detection, reassignment,
  and fencing.
- `workloads/` (Phase 3) — trivial example programs (sleep, CPU-burn) baked into the default task
  image so submitted tasks have something to actually execute.
