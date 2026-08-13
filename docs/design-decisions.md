# Design Decisions Log

This file records specific implementation-level decisions made while building Arbiter that are
narrower than the scope-level decisions already captured in
[`IMPLEMENTATION_PLAN.md`](../IMPLEMENTATION_PLAN.md) Section 12 ("Decisions Log"). Add an entry
here whenever a phase's plan says "pick one, document the choice" or similar.

## Phase 0

- **Generated protobuf code is committed to the repo** (under `gen/`) rather than gitignored and
  regenerated on every build. This keeps `go build`/`go test` working out of the box on a fresh
  clone (including on Windows/WSL2) without requiring `buf`/`protoc` plugins to be installed first.
  Run `make proto` to regenerate after editing `proto/arbiter/v1/arbiter.proto`.
- **Structured logging via `log/slog`** (standard library, JSON handler) rather than a third-party
  logging library (e.g. zerolog/zap). Sufficient for this project's needs and avoids an extra
  dependency; can be revisited if richer structured-logging features are needed later.

## Phase 1

- **Migrations are read from disk via `os.DirFS`, not `go:embed`.** `go:embed` patterns can't
  reference files outside the embedding package's own directory tree, and `internal/store` can't
  reach the top-level `/migrations` directory without either duplicating the SQL files or
  restructuring the repo layout away from what `IMPLEMENTATION_PLAN.md` specifies. Instead,
  `store.Migrate` takes an `fs.FS`, and `cmd/scheduler` passes `os.DirFS(*migrationsPath)`
  (`--migrations-path`, default `migrations`, resolved relative to the process's working
  directory). `Dockerfile.scheduler` sets `WORKDIR /app` and `COPY`s `migrations/` alongside the
  binary so the same default flag value resolves correctly in the container.
- **`internal/store.RegisterNode` upserts on `(hostname, address)`** rather than always inserting a
  new row. A worker that restarts and re-registers reuses its existing node row (refreshing
  capacity/labels, resetting status to `ready`, and incrementing `epoch`) instead of accumulating
  duplicate rows per restart. Incrementing `epoch` on *every* registration — not just when the
  Phase 2 failure detector declares a node dead — is deliberately conservative: it establishes the
  fencing invariant (Section 6.5) from day one, so enforcing it on the read side in Phase 5 doesn't
  require a data-model change.
- **Dependency versions for `github.com/jackc/pgx/v5` and `github.com/golang-migrate/migrate/v4`
  are pinned below their absolute latest releases** (`v5.6.0` and `v4.18.1` respectively) because
  newer releases bump their minimum Go version past what this project targets (`go 1.22.2`, chosen
  for broad compatibility including a fresh Windows/WSL2 Go install). Both pinned versions are
  still recent, actively-used releases — this isn't a "stuck on an old version" situation, just
  avoiding an unnecessary toolchain bump this early in the project. Revisit when there's a concrete
  reason to need a newer Go version.
- **`internal/store`'s node tests are Postgres-backed integration tests**, gated on the
  `ARBITER_TEST_POSTGRES_URL` env var (skipped if unset) rather than mocked. The store package is a
  thin wrapper around real SQL (upserts, JSONB, constraints) where a mock would mostly test the
  mock; CI provides a real `postgres:16-alpine` service container so these run on every push/PR.

## Phase 2

- **Node status state machine uses two thresholds derived from one flag.** `cmd/scheduler`'s single
  `--heartbeat-interval-ms` (default 500) flag drives everything: the interval advertised to
  workers, the failure detector's poll interval (half the heartbeat interval), the `not_ready`
  threshold (2 missed heartbeats), and the `dead` threshold (3 missed heartbeats — 1500ms at the
  default). Deriving all of these from one number means they can never drift out of sync with each
  other; see `internal/failuredetector.DefaultConfig`. Measured `kill -> dead` detection time is
  ~1450-1650ms across 5 trials, reproducible via `scripts/measure_failover.py`
  (`docs/benchmarks/phase2-failure-detection.md`), comfortably under the 3s target with room for
  later phases' added latency (e.g. Phase 5 reassignment).
- **A missing/expired Redis heartbeat key is treated as "maximally stale"**, not skipped. This
  matters for a node that registers but crashes before its first heartbeat, or if Redis itself is
  restarted/flushed — either way, the safe default is to let the node age out and be marked dead
  rather than being silently ignored by the detector forever. The Redis key's own TTL (5 minutes,
  `internal/cache.lastSeenTTL`) is set far above the detection thresholds specifically so it's never
  what triggers a dead verdict — the detector's age comparison always gets there first.
- **`not_ready -> ready` recovery happens immediately in the `Heartbeat` handler**, not on the next
  failure-detector poll. As soon as a heartbeat arrives from a node the detector had downgraded to
  `not_ready` (but not yet `dead`), it's restored to `ready` right away — there's no reason to wait
  up to a full poll interval when the proof of liveness just arrived. The `ready -> not_ready` and
  `-> dead` directions, by contrast, can only be *detected* by the poll loop noticing an absence, so
  they stay there.
- **Basic epoch checking in `Heartbeat` is implemented now, ahead of full fencing enforcement
  (Phase 5).** If a heartbeat's epoch doesn't match the node's current epoch (i.e. this node was
  marked dead — and its epoch bumped — since this process last registered, most likely because it
  was unreachable behind a partition), the handler returns `epoch_invalid=true` and the worker
  re-registers. This doesn't yet need to kill any locally-running task containers, since tasks don't
  exist until Phase 3 — that enforcement is added in Phase 5 once there's actually something to
  fence.
- **`github.com/redis/go-redis/v9` is pinned to `v9.6.1`**, for the same reason as the Phase 1
  Postgres-related pins: its latest release requires a newer Go version than this project targets.

## Phase 3

- **Task resource requests live on the parent `jobs` row, not on each `tasks` row.** Replicas of a
  job always share the same image/command/CPU/memory request; joining `tasks`↔`jobs` for scheduling
  and assignment avoids duplicating those columns N times and keeps `SubmitJob` as the single place
  that validates them. Phase 4's Filter/Scorer plugins consume the same joined `TaskWithJob` view.
- **Phase 3 placement is naive first-fit** (first `ready` node with enough residual CPU+memory),
  regardless of the job's stored `scheduling_policy`. The field is persisted and accepted by
  `SubmitJob` so Phase 4 can wire `bin_pack` / `spread` scorers without a schema or API change.
- **Docker task execution talks to the Engine HTTP API over the unix socket**, not the
  `github.com/docker/docker` Go SDK. The SDK's current releases pull OpenTelemetry exporters that
  require Go ≥ 1.25, which is past this project's pinned `go 1.22.2` toolchain. The executor only
  needs create/start/wait/kill/remove — a thin `net/http` + unix-dial client is enough and keeps
  `go.mod` free of that dependency tree.
- **Docker cgroup CPU/memory limits are not applied on task containers.** Scheduler-side accounting
  (sum of `scheduled`/`running` job requests vs node capacity) is the source of truth for placement.
  Passing `NanoCpus`/`Memory` through the Engine API fails on some nested/cgroup-v2 hosts with
  "cannot enter cgroupv2 … with domain controllers — it is in threaded mode"; omitting those fields
  keeps demos portable. Requests are still recorded as container labels for inspection.
- **Workers mount the host Docker socket (Docker-out-of-Docker)** so task containers are siblings of
  the worker container on the same daemon. The compose stack builds `arbiter-workload:latest` via a
  one-shot `workload` service (ENTRYPOINT overridden to `true`) before the worker starts, so the
  default demo image is always present for `arbiterctl submit`.

## Phase 4

- **Placement is Filter → Score (Kubernetes-style), not first-fit.** Hard filters
  (`ReadyFilter`, `ResourceFilter`, `LabelSelectorFilter`) eliminate ineligible nodes; a per-job
  scorer ranks the rest. `scheduling_policy=bin_pack` (default) prefers the node that would be
  fullest after placement; `spread` prefers the currently least-utilized node. Tie-breaks use
  hostname ascending for determinism.
- **Job constraints are a nodeSelector-style AND map** (`map[string]string` on the job, exposed as
  `arbiterctl submit --constraint key=value`). Workers accept `--labels key=value,...` at
  registration so label filtering is exercisable without a schema change.
- **In-tick allocation accounting is mandatory for correctness.** `GetNodeAllocations` alone is not
  enough inside one scheduling batch — each successful `Place` updates a local `pendingAlloc` map
  before the next task is considered, so a single tick cannot overcommit a node. Unit tests cover
  both the no-overcommit invariant and the bin_pack-fills-first / spread-even distributions.
- **Phase 4 DoD cluster is a compose overlay, not the full demo file.**
  `deploy/docker-compose.phase4.yml` adds workers 2–5 beside the base stack (`make phase4-up`).
  The 10-worker `docker-compose.demo.yml` remains a Phase 10 deliverable.
## Phase 5

- **`MarkNodeDead` orphans and requeues in the same transaction as the epoch bump.** Every
  `scheduled`/`running` task on the dead node is cleared back to `pending` (assignment wiped) with
  `task_orphaned` + `task_requeued` events. Including `scheduled` (not only `running`) avoids
  tasks stuck forever on a node that never started them.
- **Failed tasks retry with exponential backoff via `next_retry_at`.** If `retries_used <
  job.retry_limit`, a failure becomes `pending` again with backoff `500ms * 2^(n-1)` (capped at
  16s). `ClaimPendingTasksForScheduling` ignores rows whose `next_retry_at` is still in the future.
- **Status-report fencing uses `TaskStatusUpdate.node_id` + `epoch`.** Reports that do not match the
  task's current assignment return `FailedPrecondition` / `ErrStaleTaskReport`, so a zombie worker
  that reconnects after reassignment cannot complete (or fail) a task that now belongs elsewhere.
- **Scheduler mounts the Docker socket to reap orphan DooD containers.** Killing a worker container
  does not kill sibling task containers on the host daemon; the failure detector calls
  `dockerutil.KillTaskContainers` for orphaned task IDs so reassignment cannot double-execute.
- **Each container start gets a unique `ARBITER_RUN_ID`** (label + env). Workloads print it so chaos
  runs can assert no duplicate successful executions for the same task.

## Phase 6

- **Followers reject mutating RPCs with `NOT_LEADER` + advertise address**, rather than
  transparently proxying to the leader. Reads (`ListNodes` / `ListJobs` / `ListTasks`) stay
  available on any replica. Workers and `arbiterctl` parse the `FailedPrecondition` message and
  redial the advertised leader (`internal/leaderclient`). Proxying would hide failover from clients
  and couple every replica to outbound fan-out; redirect keeps the control plane simpler and matches
  how many gRPC control planes surface leadership.
- **Lease defaults: TTL 5s, renew every 1s** (`internal/election`). Only the lease holder runs the
  scheduling loop and failure detector; followers keep renewing/attempting the lease and otherwise
  idle. Takeover bumps `leader_lease.epoch` and emits a `leader_elected` event. Worst-case election
  delay is roughly TTL (if the lease was just renewed) plus one renew interval for the follower to
  notice expiry.
- **Phase 6 DoD cluster is a compose overlay** (`deploy/docker-compose.phase6.yml`, `make phase6-up`)
  with 3 scheduler replicas on host ports 7000/7001/7002. Advertise addresses use
  `host.docker.internal:<port>` plus `extra_hosts: host.docker.internal:host-gateway` so both
  in-cluster workers and host-side `arbiterctl` can follow redirects. The full 10-worker
  `docker-compose.demo.yml` remains a Phase 10 deliverable.
- **Workers rotate across a configured address list on `Unavailable`.** In addition to following
  `NOT_LEADER` redirects, `--scheduler-addrs` / `ARBITER_SCHEDULER_ADDRS` lists every replica so a
  killed leader does not permanently stick the worker to a dead seed. The Phase 6 compose overlay
  sets all three advertise addresses.
- **Failover measurement** is automated by `scripts/measure_leader_failover.py` (kill leader
  container → poll `leader_lease` for a new holder → submit a probe job to the new leader). Target:
  election within lease TTL + one renew interval (~6s). See
  `docs/benchmarks/phase6-leader-failover.md`.

## Phase 7

- **Prometheus client is `client_golang` v1.20.5** (pinned for Go 1.22.2; avoids newer client
  releases that raise the minimum Go version). Collectors live in `internal/metrics` on a private
  registry (not the global default) so tests can instantiate safely.
- **Cluster gauges refresh from Postgres on the leader only** (~1s): `queue_depth`, `tasks_running`,
  `nodes_total{status}`, and per-node CPU/memory capacity vs allocation. Followers still expose
  process metrics and `arbiter_is_leader=0`. Counters/histograms (`tasks_total`, scheduling latency,
  heartbeat misses, failover seconds, leader elections) are updated inline at the call sites.
- **`arbiter_failover_seconds` is node-death detection latency** (last-seen age when marked dead),
  not leader-election time — election is covered by `arbiter_leader_elections_total` +
  `arbiter_is_leader` flips. This matches the resume "sub-3s heartbeat failover" signal.
- **Phase 7 compose overlay** (`make phase7-up`) stacks Phase 6 HA + Prometheus (:9090) + Grafana
  (:3000, admin/admin, anonymous Viewer). Provisioned dashboards: Cluster Overview, Task Throughput,
  Failover Events. See `docs/benchmarks/phase7-observability.md`.

## Phase 8

- **Thin hand-written JSON HTTP API** (`internal/httpapi`) on the scheduler's existing HTTP port
  rather than grpc-gateway — stays inside the Go 1.22.2 pin and keeps SSE custom. Endpoints under
  `/api/v1/{nodes,jobs,tasks,events}` plus `POST /api/v1/jobs`, task logs, and SSE
  `/api/v1/events/stream`. Mutating calls on followers return `409 NOT_LEADER` with `leader_addr`
  (dashboard follows).
- **Redis pub/sub fan-out** (`pubsub:cluster-events`): a leader-only poller
  (`internal/eventfanout`) tails Postgres `events` and publishes; SSE subscribers get a recent
  snapshot then live Redis messages (with Postgres poll fallback).
- **`arbiterctl describe task` / `logs`**: describe uses new gRPC `GetTask`; logs hit scheduler
  HTTP `/api/v1/tasks/{id}/logs` (Docker label lookup on the shared host socket — workers and
  schedulers all mount `/var/run/docker.sock`). Task containers use `AutoRemove: true` so large
  bursts don't exhaust Docker VFS storage (full rootfs copy per container). Logs are available
  while a task is running; orphan cleanup still removes stale containers on node death.
- **Dashboard** is Next.js (App Router) on host port **3100** (`make phase8-up`) so it does not
  collide with Grafana on 3000. See `docs/benchmarks/phase8-dashboard.md`.

## Phase 9

- **Leader-only simulated autoscaler** (`internal/autoscaler`): when pending tasks stay above a
  threshold for a sustained window, the leader launches an extra worker container via the Docker
  Engine API (DooD). Containers are labeled `arbiter.autoscaled=true` and register with node label
  `autoscaled=true`; only those workers are ever reclaimed. Compose-static workers are untouched.
- **Scale-down**: an idle autoscaled node (zero allocation) is cordoned, its container removed, then
  marked dead. Events: `node_scaled_up` / `node_cordoned` / `node_scaled_down`.
- **Metrics**: `arbiter_scale_up_total`, `arbiter_scale_down_total`, `arbiter_autoscaled_workers`
  plus Grafana panels on Cluster Overview and a dedicated **Arbiter Autoscaling** dashboard.
- Enabled via `make phase9-up` (`ARBITER_AUTOSCALER=true` + thresholds). See
  `docs/benchmarks/phase9-autoscaling.md`.

## Phase 10

- **Standalone demo compose** (`deploy/docker-compose.demo.yml`, project name `arbiter-demo`): 3
  scheduler replicas, **10 varied-capacity workers** (1000m–4000m, Σ 23500m), Postgres, Redis,
  Prometheus (`prometheus.demo.yml` scrapes all 10 workers), Grafana, dashboard — one
  `make demo-up` / `docker compose -f deploy/docker-compose.demo.yml up -d --build`.
- **Autoscaler disabled** in the demo so the static 10-node claim stays clean (Phase 9 overlay
  remains for autoscaling demos).
- **`scripts/load_test.py`**: `--tasks` alias, `--wait-complete` samples peak concurrent `running`
  + wall-clock/throughput for Section 10; prunes exited task containers during the run.
- **`scripts/measure_node_failover.py`**: N-trial kill → dead → reassignment timing with p50/p95.
- **Executor**: container names include a run-id suffix to avoid DooD name collisions under burst;
  create 409 → force-remove + retry; `AutoRemove: true` for VFS-friendly demos.
- **Async orphan reaping**: after `MarkNodeDead`, DooD `KillTaskContainers` runs in a background
  goroutine with its own timeout. Synchronous reaping under the vfs storage driver blocked the
  failure-detector tick and pushed kill→dead p95 above 3s under load; async reaping keeps
  detection near the configured `DeadAfter` (~1.5s).
- Resume-metric evidence: `docs/benchmarks/phase10-resume-metrics.md` (peak 750 concurrent;
  detection p95 1.66s; leader election max 4.8s < 5s TTL).
- Local runbook for Windows PowerShell **and** WSL/Linux/macOS: `docs/local-setup.md`.
  Windows hosts use `scripts/arbiter.ps1` / `verify_demo.ps1` (Make alternative); Python tools
  resolve `bin/arbiterctl.exe` via `scripts/arbiterctl_path.py`.
- Also: `scripts/verify_demo.sh` / `make demo-verify` for Unix shells.
