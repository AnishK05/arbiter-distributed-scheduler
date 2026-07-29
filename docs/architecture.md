# Arbiter Architecture

This is a quick-reference architecture doc. For the full rationale behind every decision here, see
[`IMPLEMENTATION_PLAN.md`](../IMPLEMENTATION_PLAN.md) at the repo root — that document is the
source of truth; this page just collects the diagrams and a short summary for fast orientation.

## System Overview

Arbiter is a distributed scheduler: a control plane (3 leader-elected scheduler replicas) places
tasks onto worker nodes (10 simulated worker processes/containers in the demo cluster), monitors
their health via heartbeats, and automatically reassigns work when a node fails.

```mermaid
graph TB
    subgraph ControlPlane["Control Plane (HA — 3 replicas)"]
        S1["Scheduler Replica 1 (LEADER)"]
        S2["Scheduler Replica 2 (follower)"]
        S3["Scheduler Replica 3 (follower)"]
    end

    subgraph DataStores["Data Stores"]
        PG[("PostgreSQL — durable source of truth")]
        R[("Redis — heartbeats, pub/sub, hot cache")]
    end

    subgraph Workers["Worker Nodes (10 simulated worker processes/containers)"]
        W1["Worker Agent 1"]
        W2["Worker Agent 2"]
        WN["Worker Agent N (10 in demo)"]
    end

    CLI["arbiterctl (CLI)"]
    DASH["Next.js Dashboard"]
    PROM["Prometheus"]
    GRAF["Grafana"]

    S1 <--> PG
    S2 <--> PG
    S3 <--> PG
    S1 <--> R
    S2 <--> R
    S3 <--> R

    W1 <-- "gRPC: register / heartbeat / assign" --> S1
    W2 <-- "gRPC: register / heartbeat / assign" --> S1
    WN <-- "gRPC: register / heartbeat / assign" --> S1

    CLI -- "gRPC / REST" --> S1
    DASH -- "gRPC-gateway / REST / WS" --> S1
    PROM -- scrape --> S1
    PROM -- scrape --> S2
    PROM -- scrape --> S3
    PROM -- scrape --> W1
    PROM -- scrape --> W2
    PROM -- scrape --> WN
    GRAF -- query --> PROM
```

Only the elected leader makes placement decisions and talks to workers; followers stay hot and
ready to take over via a Postgres-lease-based leader election (Section 6.6 of the implementation
plan).

## Task Lifecycle

```mermaid
stateDiagram-v2
    [*] --> pending
    pending --> scheduled: placement decision made
    scheduled --> running: worker starts container
    running --> succeeded: exit code 0
    running --> failed: exit code != 0 / crash
    running --> orphaned: node heartbeat lost (epoch fenced)
    orphaned --> pending: re-enqueued for reassignment
    failed --> pending: retries_used < retry_limit
    failed --> [*]: retries exhausted
    succeeded --> [*]
    pending --> cancelled: user cancel
    scheduled --> cancelled: user cancel
    cancelled --> [*]
```

## Happy Path + Failover Sequence

```mermaid
sequenceDiagram
    participant Client
    participant Leader as Scheduler (Leader)
    participant PG as PostgreSQL
    participant WA as Worker A
    participant WB as Worker B

    Client->>Leader: SubmitJob(spec)
    Leader->>PG: INSERT tasks (status=pending)
    loop scheduling loop
        Leader->>PG: SELECT pending tasks FOR UPDATE SKIP LOCKED
        Leader->>Leader: Filter + Score nodes (bin-pack)
        Leader->>PG: UPDATE task SET status=scheduled, node=WA, assigned_epoch=WA.epoch
    end
    WA->>Leader: Heartbeat (epoch=E)
    Leader-->>WA: TaskAssignment
    WA->>WA: docker run (cpu/mem limited)
    WA->>Leader: ReportTaskStatus(running)
    Note over WA: worker container killed/paused (chaos test)
    Leader->>Leader: 3 missed heartbeats from WA
    Leader->>PG: node WA: status=dead, epoch=E+1
    Leader->>PG: tasks on WA: status=orphaned -> pending
    Leader->>Leader: reschedule orphaned tasks (bin-pack)
    Leader->>PG: UPDATE task SET status=scheduled, node=WB
    WB->>Leader: Heartbeat
    Leader-->>WB: TaskAssignment
    WB->>WB: docker run
    Note over WA: WA reconnects (unpaused), still epoch=E
    WA->>Leader: Heartbeat (epoch=E)
    Leader-->>WA: epoch_invalid=true
    WA->>WA: kill local containers, re-register
```

## Repository Map

| Path | Purpose |
|---|---|
| `cmd/scheduler` | Control-plane binary (gRPC + HTTP servers; placement, failure detection, election in later phases) |
| `cmd/worker` | Worker-agent binary (registration, heartbeats, task execution in later phases) |
| `cmd/arbiterctl` | CLI client |
| `internal/grpcapi` | gRPC service implementations for `ClusterControl` / `SchedulerAPI` |
| `internal/scheduler` | Placement engine (filter/score, queue, retry) — Phase 3+ |
| `internal/election` | Leader election (Postgres lease + fencing epochs) |
| `internal/failuredetector` | Heartbeat-timeout tracking — Phase 2 |
| `internal/store` | Postgres repositories — Phase 1+ |
| `internal/cache` | Redis client wrappers — Phase 2+ |
| `internal/executor` | Docker-based task execution on the worker — Phase 3 |
| `internal/metrics` | Prometheus collectors — Phase 7 |
| `gen/` | Generated protobuf/gRPC Go code (from `proto/`, via `make proto`) |
| `proto/` | `.proto` service/message definitions + buf config |
| `migrations/` | SQL schema migrations |
| `dashboard/` | Next.js + TypeScript web UI — Phase 8 |
| `scripts/` | Python tooling: load-test, chaos monkey, failover measurement |
| `deploy/` | Docker Compose stacks, Prometheus/Grafana config |
| `docs/` | This doc, design-decision notes, and archived benchmark output |

See [`IMPLEMENTATION_PLAN.md`](../IMPLEMENTATION_PLAN.md) for the full phase-by-phase build plan,
data model, gRPC API, and the decisions log.
