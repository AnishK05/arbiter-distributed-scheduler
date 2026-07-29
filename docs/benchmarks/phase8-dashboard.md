# Phase 8 — CLI & Web Dashboard

Evidence for the Phase 8 DoD (`IMPLEMENTATION_PLAN.md` Section 8): from the
dashboard, submit a job, watch tasks move pending → scheduled → running →
succeeded live, and see node utilization bars update in real time.

## Stack

```bash
make phase8-up   # Phase 6 HA + Prometheus/Grafana + Next.js dashboard :3100
make build
```

| Surface | URL / command |
|---|---|
| Dashboard | http://localhost:3100 |
| REST / SSE | http://localhost:8080/api/v1/... |
| Grafana | http://localhost:3000 (unchanged) |
| CLI | `./bin/arbiterctl describe task <id>` / `./bin/arbiterctl logs <id>` |

## Design summary

- Thin JSON HTTP API on the scheduler (`internal/httpapi`), not grpc-gateway.
- Leader-only `eventfanout` polls Postgres `events` → Redis `pubsub:cluster-events`
  → dashboard `EventSource` on `/api/v1/events/stream`.
- Task containers keep `AutoRemove: false` so `logs` can read Docker labels after exit.

## DoD run (fill after stack exercise)

```bash
# API smoke
curl -s http://localhost:8080/api/v1/nodes | head -c 200
curl -s -N http://localhost:8080/api/v1/events/stream | head -n 5

# Dashboard submit (or use the form at :3100)
curl -s -X POST http://localhost:8080/api/v1/jobs \
  -H 'Content-Type: application/json' \
  -d '{"name":"phase8-dod","image":"arbiter-workload:latest","command":["12"],"cpu_millicores":50,"memory_mb":32,"replicas":3,"scheduling_policy":"bin_pack"}'

# Watch task status transitions
watch -n1 'curl -s http://localhost:8080/api/v1/tasks | python3 -c "import sys,json; [print(t[\"Status\"], t[\"ID\"][:8]) for t in json.load(sys.stdin).get(\"tasks\",[])[-6:]]"'

# CLI
./bin/arbiterctl get tasks
./bin/arbiterctl describe task <task-id>
./bin/arbiterctl logs <task-id>
```

_Results from the live DoD exercise will be appended below after `make phase8-up`._
