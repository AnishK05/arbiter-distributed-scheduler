# Arbiter — Distributed Scheduler & Compute Orchestration

Arbiter is a from-scratch distributed scheduler / compute orchestrator — a small, student-scoped
cousin of Kubernetes, Borg, Nomad, and Mesos. A cluster of worker nodes runs tasks; a leader-elected
scheduler control plane decides where those tasks run, monitors node health via heartbeats, and
automatically reassigns work when a node fails.

**Resume claim this repo measures (see [`docs/benchmarks/phase10-resume-metrics.md`](docs/benchmarks/phase10-resume-metrics.md)):**

> Built a cluster scheduler sustaining 500+ concurrent tasks across 10 nodes via leader-elected
> coordination, heartbeat-based failure detection with sub-3s failover, bin-packing allocation, and
> automatic task reassignment

The full, phase-by-phase implementation plan lives in
[`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md). Architecture diagrams are in
[`docs/architecture.md`](docs/architecture.md).

**Status:** Phase 10 complete — demo cluster + archived Section 10 benchmarks.

## Architecture

```mermaid
graph TB
    subgraph ControlPlane["Control Plane (HA — 3 replicas)"]
        S1["Scheduler Replica 1 (LEADER)"]
        S2["Scheduler Replica 2 (follower)"]
        S3["Scheduler Replica 3 (follower)"]
    end

    subgraph DataStores["Data Stores"]
        PG[("PostgreSQL")]
        R[("Redis")]
    end

    subgraph Workers["10 simulated worker containers"]
        W1["worker-1 … worker-10"]
    end

    CLI["arbiterctl"]
    DASH["Next.js Dashboard :3100"]
    PROM["Prometheus :9090"]
    GRAF["Grafana :3000"]

    S1 <--> PG
    S2 <--> PG
    S3 <--> PG
    S1 <--> R
    W1 <-- "gRPC register / heartbeat / assign" --> S1
    CLI -- "gRPC / REST" --> S1
    DASH -- "REST / SSE" --> S1
    PROM -- scrape --> S1
    PROM -- scrape --> W1
    GRAF -- query --> PROM
```

## Tech Stack

Go · gRPC/Protobuf · PostgreSQL · Redis · Docker · Prometheus · Grafana · Next.js/TypeScript ·
Python (tooling)

## Development Setup (Windows/WSL2)

Primary development happens on Windows. The recommended (and only supported) setup is:

1. Install [Docker Desktop for Windows](https://www.docker.com/products/docker-desktop/) with the
   **WSL2 backend** enabled (default in current versions).
2. Install a WSL2 distro if you don't have one: `wsl --install` (Ubuntu is a good default).
3. In Docker Desktop's Settings → Resources → WSL Integration, enable integration for your distro.
4. Clone this repo **inside** the WSL2 filesystem (e.g. `~/dev/arbiter-distributed-scheduler`), not
   under `/mnt/c/...` — this matters for Docker bind-mount/file-watch performance.
5. Do all development (editing, `make`, `go`, `docker compose`, etc.) from inside a WSL2 shell (or
   an editor with WSL support, e.g. VS Code's "Remote - WSL" extension / Cursor's WSL support).

This isn't a workaround — it's the standard way to do Linux-container-based backend development on
Windows. See [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md) Section 4 for the full rationale.

If you're on macOS/Linux natively, none of the above applies — just make sure Docker and Go are
installed.

## Prerequisites

- [Go](https://go.dev/dl/) 1.22+ (repo pin: 1.22.2 / `GOTOOLCHAIN=local`)
- [Docker](https://docs.docker.com/get-docker/) + Docker Compose v2
- `make`
- (Optional, only if you edit `.proto` files) run `make tools`

## Demo cluster (recommended)

One compose file brings up the full **10-node simulated cluster**:

```bash
git clone https://github.com/AnishK05/arbiter-distributed-scheduler.git
cd arbiter-distributed-scheduler

make demo-up          # or: docker compose -f deploy/docker-compose.demo.yml up -d --build
make build
```

| Surface | URL |
|---|---|
| **Dashboard** | http://localhost:3100 |
| **Grafana** | http://localhost:3000 (admin/admin, anonymous Viewer) |
| **Prometheus** | http://localhost:9090 |
| Scheduler API / gRPC | http://localhost:8080 · `localhost:7000` (HA: `:7001`/`:7002`, `:8086`/`:8087`) |

```bash
# Confirm 10 ready workers
curl -s http://localhost:8080/api/v1/nodes | python3 -c "import sys,json; print(sum(1 for n in json.load(sys.stdin)['nodes'] if n['status']=='ready'))"

# Submit a small job
./bin/arbiterctl submit demo --replicas 5 --wait

# Section 10 concurrency benchmark (archives → docs/benchmarks/phase10-resume-metrics.md)
python3 scripts/load_test.py --tasks 750 --cpu-millicores 40 --memory-mb 32 \
  --command 45 --wait-complete --policy bin_pack

make demo-down
```

Worker capacities are **varied** (1000m–4000m) and total **23500m** simulated CPU so a packed burst
can sustain 500+ concurrent running tasks. This is a simulated multi-node cluster on one machine
(see Section 12 Q3 in the plan) — honest and normal for a project of this scope.

## Incremental phase stacks

For day-to-day development you can still bring up smaller stacks:

```bash
make up              # 1 scheduler + 1 worker
make phase4-up       # + 5 equal workers (placement)
make phase6-up       # 3 scheduler replicas (HA)
make phase7-up       # + Prometheus/Grafana
make phase8-up       # + dashboard :3100
make phase9-up       # + simulated autoscaler
```

```bash
# Phase 3 DoD
./bin/arbiterctl submit demo --replicas 5 --wait

# Phase 4 placement comparison
python3 scripts/load_test.py --replicas 100 --cpu-millicores 50 --policy bin_pack
python3 scripts/load_test.py --replicas 100 --cpu-millicores 50 --policy spread

# Phase 5 chaos
./bin/arbiterctl submit chaos --replicas 20 --cpu-millicores 50 --memory-mb 32 --command 45 &
python3 scripts/chaos_monkey.py --duration-s 40 --interval-s 5

# Phase 6 leader failover
python3 scripts/measure_leader_failover.py --trials 5 --submit-after

# Phase 8 CLI
./bin/arbiterctl describe task <task-id>
./bin/arbiterctl logs <task-id>
```

For faster edit/rebuild cycles against Dockerized Postgres/Redis:

```bash
docker compose -f deploy/docker-compose.yml up -d postgres redis
make run-scheduler   # separate terminal
make run-worker      # separate terminal
```

Run `make help` for all targets.

## Repository Layout

See [`docs/architecture.md`](docs/architecture.md#repository-map).

## Testing

```bash
make vet    # go vet
make lint   # golangci-lint
make test   # go test ./... -race
```

CI (`.github/workflows/ci.yml`) runs all three on every push/PR.

`internal/store`, `internal/cache`, and `internal/failuredetector` integration tests skip unless
`ARBITER_TEST_POSTGRES_URL` / `ARBITER_TEST_REDIS_ADDR` are set. CI provides both. Locally:

```bash
docker compose -f deploy/docker-compose.yml up -d postgres redis
ARBITER_TEST_POSTGRES_URL="postgres://arbiter:arbiter@localhost:5432/arbiter?sslmode=disable" \
ARBITER_TEST_REDIS_ADDR="localhost:6379" \
make test
```

## Benchmarks

Archived evidence for every phase DoD (and the Section 10 resume metrics) lives under
[`docs/benchmarks/`](docs/benchmarks/).
