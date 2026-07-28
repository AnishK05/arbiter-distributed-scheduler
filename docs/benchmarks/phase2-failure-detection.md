# Phase 2 — Failure Detection Timing (preliminary)

This is a preliminary measurement taken while implementing Phase 2, to validate the failure
detector's configured thresholds actually behave as intended. It is **not** the final resume-metric
benchmark (that's produced per `IMPLEMENTATION_PLAN.md` Section 10, once reassignment exists in
Phase 5 and the full 10-node demo cluster exists in Phase 10) — just evidence that the mechanism
works and is well within the sub-3s target with room to spare for later phases' added latency
(task reassignment, etc.).

## Configuration

- `--heartbeat-interval-ms=500` (default)
- Derived failure-detector thresholds (`internal/failuredetector.DefaultConfig`):
  - Poll interval: 250ms
  - `not_ready` after: 1000ms of no heartbeat
  - `dead` after: 1500ms of no heartbeat

## Method

`scripts/measure_failover.py` automates this: against the running `docker-compose.yml` stack
(Postgres, Redis, 1 scheduler, 1 worker), for each trial it records `T0`, runs
`docker kill <worker container>`, polls `nodes.status` in Postgres until it reads `'dead'` (`T1`),
asserts `T1 - T0` is under `--threshold-ms` (default 3000), then `docker start`s the worker back up
(re-registration happens automatically) before the next trial. Exits non-zero if any trial exceeds
the threshold or never reaches `dead` within `--poll-timeout-s`.

```bash
python3 scripts/measure_failover.py --trials 5
```

## Results (5 trials)

| Trial | Elapsed (ms) |
|---|---|
| 1 | 1452 |
| 2 | 1505 |
| 3 | 1654 |
| 4 | 1482 |
| 5 | 1596 |

min=1452ms, max=1654ms, avg=1538ms — all within roughly the configured 1500ms `DeadAfter`
threshold plus polling/measurement granularity (as expected, since the worker was sending
heartbeats right up until the kill), and comfortably under the 3-second target. `measure_failover.py`
exits 0 (all trials passed).
