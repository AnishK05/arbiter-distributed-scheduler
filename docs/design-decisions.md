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