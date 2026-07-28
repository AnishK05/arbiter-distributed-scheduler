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
