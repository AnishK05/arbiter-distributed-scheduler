# Phase 10 — Resume Metrics (Section 10)

Evidence for the resume claim in `IMPLEMENTATION_PLAN.md` Section 10 / DoD:

> Built a cluster scheduler sustaining **500+ concurrent tasks across 10 nodes** via
> leader-elected coordination, **heartbeat-based failure detection with sub-3s failover**,
> bin-packing allocation, and demonstrated **leader failover** within one lease TTL.

Measured on the standalone demo stack (`make demo-up` /
`docker compose -f deploy/docker-compose.demo.yml up -d --build`), 2026-07-30.

## Stack

| Component | Detail |
|---|---|
| Schedulers | 3 HA replicas (Postgres lease TTL 5s) |
| Workers | 10 varied capacity (1000–4000m CPU, Σ **23500m**; Σ **12032 MiB** mem) |
| Image | `busybox:1.36` (small rootfs; safer under Docker **vfs** than larger workloads) |
| Autoscaler | Off (static 10-node claim) |

## 1. Concurrent tasks (500+)

```bash
make demo-up && make build
docker pull busybox:1.36
python3 scripts/load_test.py --tasks 750 --cpu-millicores 30 --memory-mb 16 \
  --image busybox:1.36 --command sleep --command 55 \
  --wait-complete --policy bin_pack --name phase10-concurrency
```

**Note:** At `--memory-mb 32` the cluster is memory-bound at
`⌊12032/32⌋ = 376` concurrent tasks (CPU would allow ~671 @ 35m). Lowering memory
requests to 16 MiB unlocks the CPU budget so peak concurrency can cross 500.

### Result

```text
complete: total=750 succeeded=750 failed=0 wall_s=118.33 peak_running=750
nodes_used=10 min=32 max=126 mean=75.00
wall_clock_s=118.33 peak_concurrent_running=750 throughput_tasks_per_s=6.34
```

| Metric | Value |
|---|---|
| Submitted | 750 |
| Succeeded | 750 |
| **Peak concurrent `running`** | **750** |
| Wall clock | 118.33 s |
| Throughput | 6.34 tasks/s |
| Nodes used | 10 |

Resume-safe claim: **500+ concurrent tasks across 10 nodes** (measured peak 750).

## 2. Sub-3s node failover (≥20 trials)

After rebuilding schedulers with **async orphan container reaping** (sync DooD
`KillTaskContainers` under vfs previously blocked the failure-detector tick and
inflated detection p95 under load):

```bash
./bin/arbiterctl submit steady --replicas 100 --cpu-millicores 40 --memory-mb 16 \
  --image busybox:1.36 --command sleep --command 900 --scheduling-policy spread
python3 scripts/measure_node_failover.py --trials 20 --threshold-ms 3000
```

Heartbeat defaults: `H=500ms`, dead after `3H=1500ms`, poll `H/2=250ms`.

### Result

```text
detection_ms: n=20 p50=1473.2 p95=1656.4 max=1703.3 mean=1447.9
reassign_ms:  n=20 p50=1774.6 p95=1958.8 max=1988.6 mean=1736.8
ok: detection p95 1656.4ms <= 3000.0ms
```

| Metric | p50 | p95 | max |
|---|---|---|---|
| Kill → `nodes.status=dead` | 1473 ms | **1656 ms** | 1703 ms |
| Kill → task running elsewhere / succeeded | 1775 ms | 1959 ms | 1989 ms |

Resume-safe claim: **sub-3s failover** (detection p95 **1.66 s**; reassignment p95 **1.96 s**).

## 3. Leader failover

```bash
python3 scripts/measure_leader_failover.py --trials 10 --threshold-ms 5000 --submit-after
```

Lease TTL = 5s (demo compose). Each trial kills the current leader, waits for a new
lease holder, then submits a probe job through the new leader.

### Result

```text
ok: 4367.2ms, 4209.3ms, 4459.3ms, 4617.9ms, 4288.0ms,
    4783.1ms, 4802.9ms, 4202.1ms, 4435.4ms, 4794.9ms
    (max=4802.9ms, threshold=5000.0ms)
```

| Metric | Value |
|---|---|
| Trials | 10 / 10 under one lease TTL |
| Max election | **4803 ms** (< 5000 ms TTL) |
| Probe jobs | all succeeded on the new leader |

## Demo URLs

| Service | URL |
|---|---|
| Scheduler API / health | http://localhost:8080/healthz |
| Dashboard | http://localhost:3100 |
| Grafana | http://localhost:3000 (admin/admin) |
| Prometheus | http://localhost:9090 |

## Environment notes

- Host Docker storage driver was **vfs** (full rootfs copy per container). Executor uses
  `AutoRemove: true` and unique `arbiter-task-<id>-<runID8>` names so burst runs stay
  disk-safe; prefer `busybox` (or similarly tiny images) for Section 10 concurrency.
- Failure-detector orphan reaping runs in a background goroutine so vfs force-removes
  cannot stall dead-detection for other nodes (`internal/failuredetector`).
