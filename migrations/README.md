# Migrations

SQL schema migrations for PostgreSQL, managed with
[`golang-migrate`](https://github.com/golang-migrate/migrate). Files follow its
`{version}_{name}.{up,down}.sql` naming convention.

The scheduler binary runs pending migrations automatically on startup (see
`internal/store.Store.Migrate`, invoked from `cmd/scheduler/main.go`) against the `file://`
source rooted at this directory (`--migrations-path`, default `migrations`) — no separate manual
migration step is needed for local dev or the Docker Compose stack.

To run migrations manually (e.g. to roll back), install the CLI via `make tools` and run:

```bash
migrate -path migrations -database "$ARBITER_POSTGRES_URL" up
migrate -path migrations -database "$ARBITER_POSTGRES_URL" down 1
```

| Migration | Adds |
|---|---|
| `000001_create_nodes_table` | `nodes` table (Phase 1) |

`jobs`/`tasks` (Phase 3), `events` (Phase 2), and `leader_lease` (Phase 6) follow in their
respective phases. See `IMPLEMENTATION_PLAN.md` Section 6.1 for the full schema design.
