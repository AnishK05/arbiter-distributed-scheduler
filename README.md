# Arbiter — Distributed Scheduler & Compute Orchestration

Arbiter is a from-scratch distributed scheduler / compute orchestrator — a small, student-scoped
cousin of Kubernetes, Borg, Nomad, and Mesos. A cluster of worker nodes runs tasks; a leader-elected
scheduler control plane decides where those tasks run, monitors node health via heartbeats, and
automatically reassigns work when a node fails.

The full, phase-by-phase implementation plan — architecture, data model, scheduling algorithm,
leader election, fencing/fault-tolerance design, gRPC API, milestones, and benchmark methodology —
lives in [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md). A quick-reference architecture summary
with diagrams is in [`docs/architecture.md`](docs/architecture.md).

**Status:** Phase 0 (scaffolding) — see [`IMPLEMENTATION_PLAN.md`](IMPLEMENTATION_PLAN.md) Section 8
for the full milestone list.

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
# Bring up Postgres + Redis
make up

# In separate terminals: build and run the scheduler and worker binaries
make run-scheduler
make run-worker

# Scheduler health check
curl http://localhost:8080/healthz

# Tear down
make down
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
