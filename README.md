# Arbiter — Distributed Scheduler & Compute Orchestration

Arbiter is a from-scratch distributed scheduler / compute orchestrator — a small, student-scoped
cousin of Kubernetes, Borg, Nomad, and Mesos. A cluster of worker nodes runs tasks; a leader-elected
scheduler control plane decides where those tasks run, monitors node health via heartbeats, and
automatically reassigns work when a node fails.

The full, phase-by-phase implementation plan — architecture, data model, scheduling algorithm,
leader election, fencing/fault-tolerance design, gRPC API, milestones, and benchmark methodology —
lives in [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md). A quick-reference architecture summary
with diagrams is in [`docs/architecture.md`](docs/architecture.md).

**Status:** Phase 4 (bin-packing & pluggable scheduling policies) — see
[`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md) Section 8 for the full milestone list.

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
Windows. See [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md) Section 4 for the full rationale and
the specific Windows pitfalls this design avoids (CRLF line endings, `SIGSTOP`-based fault
injection, etc.).

If you're on macOS/Linux natively, none of the above applies — just make sure Docker and Go are
installed.

## Prerequisites

- [Go](https://go.dev/dl/) 1.22+
- [Docker](https://docs.docker.com/get-docker/) + Docker Compose v2
- `make`
- (Optional, only needed if you edit `.proto` files) [`buf`](https://buf.build/docs/installation),
  `protoc-gen-go`, `protoc-gen-go-grpc` — run `make tools` to install all three via `go install`.

## Quickstart

```bash
# Bring up Postgres + Redis + 1 scheduler + 1 worker (containerized)
make up

# Scheduler health check
curl http://localhost:8080/healthz

# Confirm the worker registered
docker exec arbiter-postgres psql -U arbiter -d arbiter -c "SELECT hostname, address, status, epoch FROM nodes;"

# Phase 3 DoD: submit 5 replicas and wait for all to succeed
make build
./bin/arbiterctl submit demo --replicas 5 --wait

# Phase 4: 5-worker cluster + bin_pack vs spread placement comparison
make phase4-up
python3 scripts/load_test.py --replicas 100 --cpu-millicores 50 --policy bin_pack
python3 scripts/load_test.py --replicas 100 --cpu-millicores 50 --policy spread

# Tear down
make down
```

For faster edit/rebuild cycles, run the scheduler/worker binaries directly on the host (against the
same Dockerized Postgres/Redis) instead of rebuilding containers on every change:

```bash
docker compose -f deploy/docker-compose.yml up -d postgres redis
make run-scheduler   # separate terminal
make run-worker      # separate terminal
```

Run `make help` to see all available targets (build, test, lint, proto codegen, etc.).

## Repository Layout

See [`docs/architecture.md`](docs/architecture.md#repository-map) for a table of every top-level
directory and what it's for.

## Testing

```bash
make vet    # go vet
make lint   # golangci-lint
make test   # go test ./... -race
```

CI (`.github/workflows/ci.yml`) runs all three on every push/PR.

`internal/store`, `internal/cache`, and `internal/failuredetector`'s tests are backed by real
Postgres/Redis integration tests; they skip themselves unless `ARBITER_TEST_POSTGRES_URL` /
`ARBITER_TEST_REDIS_ADDR` are set. CI provides both service containers automatically. To run them
locally: `docker compose -f deploy/docker-compose.yml up -d postgres redis`, then

```bash
ARBITER_TEST_POSTGRES_URL="postgres://arbiter:arbiter@localhost:5432/arbiter?sslmode=disable" \
ARBITER_TEST_REDIS_ADDR="localhost:6379" \
make test
```
