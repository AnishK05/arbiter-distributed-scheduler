// Package store is the PostgreSQL persistence layer for Arbiter. It is the
// durable source of truth for cluster state (nodes, and — starting Phase 3 —
// jobs/tasks, the leader lease, and the audit event log). See
// IMPLEMENTATION_PLAN.md Section 6.1 for the full schema design and
// rationale (e.g. why Redis is never the source of truth for this data).
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"

	"github.com/golang-migrate/migrate/v4"
	pgxmigrate "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"

	// Registers the "pgx" database/sql driver used only for running
	// migrations (see Migrate below). Runtime queries go through pgxpool,
	// which is the more efficient, pgx-native pooled interface.
	_ "github.com/jackc/pgx/v5/stdlib"
)

// Store wraps a PostgreSQL connection pool and exposes repository methods
// for each entity (nodes for now; jobs/tasks/leases/events in later phases).
type Store struct {
	pool *pgxpool.Pool
}

// Connect opens a connection pool to Postgres and verifies connectivity with
// a ping. Callers are responsible for calling Close when done.
func Connect(ctx context.Context, connString string) (*Store, error) {
	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("store: create connection pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: ping database: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases all pooled connections.
func (s *Store) Close() {
	s.pool.Close()
}

// Migrate applies all pending "up" migrations from the given filesystem
// migrations source (see cmd/scheduler's embedded migrationsFS). It is safe
// to call on every scheduler startup: golang-migrate is a no-op if the
// schema is already at the latest version, and takes a Postgres advisory
// lock internally so concurrent scheduler replicas racing to migrate at
// startup don't corrupt the schema_migrations table.
func Migrate(connString string, migrationsFS fs.FS) error {
	db, err := sql.Open("pgx", connString)
	if err != nil {
		return fmt.Errorf("store: open migration connection: %w", err)
	}
	defer func() { _ = db.Close() }()

	dbDriver, err := pgxmigrate.WithInstance(db, &pgxmigrate.Config{})
	if err != nil {
		return fmt.Errorf("store: init migration db driver: %w", err)
	}

	sourceDriver, err := iofs.New(migrationsFS, ".")
	if err != nil {
		return fmt.Errorf("store: load migrations source: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", sourceDriver, "pgx5", dbDriver)
	if err != nil {
		return fmt.Errorf("store: init migrator: %w", err)
	}
	defer func() { _, _ = m.Close() }()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("store: run migrations: %w", err)
	}
	return nil
}
