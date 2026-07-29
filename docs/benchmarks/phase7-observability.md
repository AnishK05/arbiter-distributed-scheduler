# Phase 7 — Observability

Evidence for the Phase 7 DoD (`IMPLEMENTATION_PLAN.md` Section 8): Grafana
shows live utilization while load runs, and a failover panel reflects leader /
node failure events.

## Stack

```bash
make phase7-up   # Phase 6 HA + Prometheus :9090 + Grafana :3000
make build
```

| Service | URL |
|---|---|
| Grafana | http://localhost:3000 (admin/admin, anonymous Viewer) |
| Prometheus | http://localhost:9090 |
| Dashboards | Arbiter / Cluster Overview, Task Throughput, Failover Events |

## Live utilization

Submitted `phase7-load` (15 replicas × 50m / 32MB, 25s sleep) while the
cluster was up:

```text
arbiter_tasks_running = 15
arbiter_node_cpu_allocated_millicores{hostname="worker-1"} = 750
arbiter_node_cpu_capacity_millicores{hostname="worker-1"} = 2000
CPU utilization (PromQL) = 0.375
arbiter_scheduling_latency_seconds_count = 15
arbiter_tasks_total{status="scheduled|running"} = 15
```

Cluster Overview panels bind to these series (`node_cpu_*`, `queue_depth`,
`tasks_running`, `nodes_total`).

## Failover panel

1. `docker kill arbiter-scheduler-2` (then-current leader) → new leader
   `scheduler-3` in ~4.5s.
2. Prometheus afterwards:

```text
arbiter_is_leader{scheduler-3:8080} = 1
sum(arbiter_leader_elections_total) = 1
```

3. `docker pause arbiter-worker-1` (~3s) → failure detector marks the node:

```text
sum(arbiter_failover_seconds_count) = 1
sum(arbiter_heartbeat_misses_total) = 2
```

Failover Events dashboard panels (`arbiter_is_leader`,
`arbiter_leader_elections_total`, `arbiter_failover_seconds`,
`arbiter_heartbeat_misses_total`, `nodes_total{status}`) reflect these series.

## Metrics endpoint smoke check

```bash
curl -s http://localhost:8080/metrics | grep '^arbiter_'
curl -s http://localhost:8081/metrics | grep arbiter_worker_tasks_running
```
