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

## DoD run (fill after stack exercise)

```bash
# Burst that exceeds a single 2000m worker so pending stays elevated
./bin/arbiterctl submit burst --replicas 40 --cpu-millicores 200 --memory-mb 64 --command 30

# Watch scale-up
watch -n2 'curl -s http://localhost:8080/api/v1/nodes | python3 -c "import sys,json; ns=json.load(sys.stdin)[\"nodes\"];
print([(n[\"hostname\"], n[\"status\"]) for n in ns if n[\"status\"]!=\"dead\"])"'

curl -s 'http://localhost:8080/api/v1/events?limit=20' | python3 -c "import sys,json; 
[print(e['EventType'], e['Message'][:90]) for e in json.load(sys.stdin)['events'] if 'scale' in e['EventType'] or 'cordon' in e['EventType']]"

# After jobs finish, wait idle window → scale-down
curl -s http://localhost:8080/metrics | grep arbiter_scale_
```

_Results from the live DoD exercise will be appended below after `make phase9-up`._
