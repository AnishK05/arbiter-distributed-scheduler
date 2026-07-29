# Phase 9 — Simulated Autoscaling

Evidence for the Phase 9 DoD (`IMPLEMENTATION_PLAN.md` Section 8): a sustained
burst load triggers an extra worker to appear and take tasks; a subsequent idle
period causes it to be reclaimed; both are visible in `events` and Grafana.

## Stack

```bash
make phase9-up   # Phase 8 stack + leader autoscaler enabled
make build
```

| Signal | Where |
|---|---|
| Events | `GET /api/v1/events`, dashboard live feed, Postgres `events` |
| Metrics | `arbiter_scale_up_total`, `arbiter_scale_down_total`, `arbiter_autoscaled_workers` |
| Grafana | Cluster Overview (Autoscaler panel) + Arbiter / Autoscaling |

## Policy (defaults)

| Knob | Default |
|---|---|
| Pending threshold | 3 |
| Sustain window | 8s |
| Idle reclaim window | 20s |
| Max autoscaled workers | 3 |
| Worker image | `deploy-worker:latest` |
| Network | `deploy_default` |

Only containers/nodes labeled autoscaled are reclaimed; `arbiter-worker-1` is never removed.

## DoD run (2026-07-29)

Cluster: `make phase9-up` (leader on `:8087` / `scheduler-3`).

### Burst

```text
./bin/arbiterctl submit burst --replicas 40 --cpu-millicores 200 --memory-mb 64 --command 30
# → pending queue stayed ≥3 while worker-1 saturated at 10×200m running
```

### Scale-up

After the 8s sustain window:

```text
arbiter_scale_up_total = 1 → 2
arbiter-worker-auto-1 / worker-auto-2 registered ready
running tasks rose from 10 → 20 (autoscaled workers took load)
```

Events:

```text
node_scaled_up   autoscaler scaled up: launched worker-auto-1 …
node_registered  hostname=worker-auto-1 …
node_scaled_up   autoscaler scaled up: launched worker-auto-2 …
```

### Scale-down (idle reclaim)

After the burst drained and autoscaled nodes stayed at zero allocation ≥20s:

```text
arbiter_scale_down_total = 2
node_cordoned / node_scaled_down for worker-auto-1 and worker-auto-2
docker ps: only arbiter-worker-1 remains (static compose worker untouched)
```

### Grafana / metrics

```text
arbiter_scale_up_total 2
arbiter_scale_down_total 2
```

Panels: Cluster Overview → “Autoscaler Scale Events”; dashboard **Arbiter Autoscaling**.

## Checklist

- [x] Sustained burst triggers extra worker(s)
- [x] New workers take tasks (running count increases)
- [x] Idle period reclaims autoscaled workers
- [x] Visible in `events` and Prometheus/Grafana
- [x] Static `arbiter-worker-1` not removed
