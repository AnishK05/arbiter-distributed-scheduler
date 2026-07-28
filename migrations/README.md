# Migrations

SQL schema migrations for PostgreSQL (managed with `golang-migrate` or `goose` — chosen in
Phase 1). The first migration (Phase 1) creates the `nodes` table; `jobs`/`tasks` (Phase 3),
`leader_lease` (Phase 6), and `events` (Phase 2) follow in their respective phases.

See `IMPLEMENTATION_PLAN.md` Section 6.1 for the full schema design.
