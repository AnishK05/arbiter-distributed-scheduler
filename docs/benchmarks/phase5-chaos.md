# Phase 5 — Chaos / Reassignment / Fencing

Evidence for the Phase 5 DoD (`IMPLEMENTATION_PLAN.md` Section 8): a job with
many running tasks survives kill/pause of worker containers, all tasks reach
`succeeded`, and fencing prevents duplicate successful completions.

## Cluster

| Component | Config |
|---|---|
| Stack | `make phase4-up` (5 workers × 2000m / 1024MB) |
| Job | 20 replicas × 50m / 32MB, `sleep_n.py 40` |
| Chaos | `scripts/chaos_monkey.py` (Docker `kill` / `pause`) |

## Method

```bash
make phase4-up && make build

./bin/arbiterctl submit chaos-dod --replicas 20 --cpu-millicores 50 --memory-mb 32 --command 40
JOB=$(docker exec arbiter-postgres psql -U arbiter -d arbiter -tA \
  -c "SELECT id FROM jobs WHERE name='chaos-dod' ORDER BY created_at DESC LIMIT 1;")

python3 scripts/chaos_monkey.py --job-id "$JOB" --duration-s 35 --interval-s 4 --pause-hold-s 3
```

Pause hold is ≥ the failure-detector dead threshold (~1.5s at the default
500ms heartbeat) so paused workers are marked dead, their tasks orphaned +
requeued, and leftover DooD containers reaped via `dockerutil.KillTaskContainers`.

A separate single-kill probe measured detection + completion wall time:

```text
kill_to_dead_ms ≈ 1437
kill_to_all_terminal_ms ≈ 45652   # includes remaining ~40s sleep on reassigned tasks
final: succeeded=20
```

## Results

### Pause chaos (`mode=mix`, all sampled pauses)

```
final status counts: {'succeeded': 20}
chaos DoD ok: all tasks succeeded, no duplicate succeeded events
```

Scheduler log excerpts:

```
node marked dead hostname=worker-1 orphaned_tasks=20 epoch=1
reaped orphan task containers count=20
node marked dead hostname=worker-2 orphaned_tasks=20 epoch=3
reaped orphan task containers count=20
```

### Kill chaos (`mode=kill`)

```
final status counts: {'succeeded': 20}
chaos DoD ok: all tasks succeeded, no duplicate succeeded events
kill_to_dead_ms=1437
```

## Takeaway

| Check | Result |
|---|---|
| All tasks `succeeded` after chaos | yes (20/20) |
| Duplicate `task_succeeded` events | none |
| Orphan container reap on dead | yes (`reaped orphan task containers`) |
| Kill → `dead` detection | ~1.4s (under 3s target) |

Automatic task reassignment + fencing held under both pause (hung node) and
kill (crashed node) fault injection driven purely through the Docker Engine API.
