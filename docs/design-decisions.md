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
