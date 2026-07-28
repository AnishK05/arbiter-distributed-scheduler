CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE nodes (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    hostname           TEXT NOT NULL,
    address            TEXT NOT NULL,
    cpu_capacity_mc    BIGINT NOT NULL,
    mem_capacity_mb    BIGINT NOT NULL,
    labels             JSONB NOT NULL DEFAULT '{}',
    status             TEXT NOT NULL DEFAULT 'unknown',
    epoch              BIGINT NOT NULL DEFAULT 0,
    last_heartbeat_at  TIMESTAMPTZ,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT nodes_status_check CHECK (status IN ('unknown', 'ready', 'not_ready', 'dead', 'cordoned')),
    -- A worker re-registering from the same hostname+address (e.g. after a restart) should
    -- update its existing row rather than accumulate duplicate node rows. See
    -- internal/store.RegisterNode and IMPLEMENTATION_PLAN.md Section 6.1/6.5.
    CONSTRAINT nodes_hostname_address_key UNIQUE (hostname, address)
);

CREATE INDEX idx_nodes_status ON nodes (status);
