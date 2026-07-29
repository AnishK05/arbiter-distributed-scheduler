# Phase 8 — CLI & Web Dashboard

Evidence for the Phase 8 DoD (`IMPLEMENTATION_PLAN.md` Section 8): from the
dashboard / REST API, submit a job, watch tasks move pending → scheduled →
running → succeeded live, and see node utilization update in real time.

## Stack

```bash
make phase8-up   # Phase 6 HA + Prometheus/Grafana + Next.js dashboard :3100
make build
```

| Surface | URL / command |
|---|---|
| Dashboard | http://localhost:3100 |
| REST / SSE | http://localhost:8080/api/v1/... (followers return `409 NOT_LEADER` on mutate) |
| Grafana | http://localhost:3000 (unchanged) |
| CLI | `./bin/arbiterctl describe task <id>` / `./bin/arbiterctl logs <id>` |

## Design summary

- Thin JSON HTTP API on the scheduler (`internal/httpapi`), not grpc-gateway.
- Leader-only `eventfanout` polls Postgres `events` → Redis `pubsub:cluster-events`
  → dashboard `EventSource` on `/api/v1/events/stream`.
- Task containers keep `AutoRemove: false` so `logs` can read Docker labels after exit.

## DoD run (2026-07-29)

Cluster: `make phase8-up` (3 schedulers + worker-1 + dashboard :3100).

### Submit (leader-gated)

`POST` to `:8080` / `:8086` returned `409 NOT_LEADER` with
`leader_addr=host.docker.internal:7002`. Submit on `:8087` succeeded:

```text
job phase8-dod  ID=04ed6c3e-071f-40ff-a3b0-fe1d334bd6d0
image=arbiter-workload:latest  replicas=3  cpu=100m  mem=64MB  command=["15"]
```

Dashboard submit uses the same follow-leader mapping (`7000→8080`, `7001→8086`,
`7002→8087`).

### Live status + utilization

Polled `/api/v1/tasks?job_id=…` and `/api/v1/nodes` every 1s:

```text
t=1:  statuses=[pending]   worker-1 allocated CPU=0
t=2:  statuses=[running]   worker-1 allocated CPU=300   ← util bar rises
…
t=17: statuses=[succeeded] worker-1 allocated CPU=0     ← util bar clears
```

`scheduled` was visible in the audit stream (scheduling completed between polls):

```text
job_submitted
task_scheduled  (×3 onto worker-1)
task_running    (×3)
task_succeeded  (×3, exit_code=0)
```

SSE `/api/v1/events/stream` returned `text/event-stream` snapshots (`id:` + `data:` JSON).
Dashboard HTTP `GET http://localhost:3100/` → **200**.

### CLI

```text
$ ./bin/arbiterctl describe task 46750c6f-…
ID:      46750c6f-5caa-419b-b86a-c6b6431842ae
JOB:     04ed6c3e-071f-40ff-a3b0-fe1d334bd6d0
STATUS:  succeeded
NODE:    3fd3ae97-5c07-40a6-9ee3-37e90ef85384
EXIT:    0

$ ./bin/arbiterctl logs 46750c6f-…
… arbiter-workload sleep_n: … sleeping 15.0s
… arbiter-workload sleep_n: done …
```

## Checklist

- [x] Submit job from dashboard/REST
- [x] Watch pending → scheduled → running → succeeded (poll + events)
- [x] Utilization updates live (0 → 300m → 0 on worker-1)
- [x] Live event feed (SSE)
- [x] `arbiterctl describe task` / `logs`
