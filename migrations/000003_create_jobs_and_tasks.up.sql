CREATE TABLE jobs (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name               TEXT NOT NULL,
    image              TEXT NOT NULL,
    command            TEXT[] NOT NULL DEFAULT '{}',
    cpu_request_mc     BIGINT NOT NULL,
    mem_request_mb     BIGINT NOT NULL,
    replicas           INT NOT NULL DEFAULT 1,
    retry_limit        INT NOT NULL DEFAULT 3,
    scheduling_policy  TEXT NOT NULL DEFAULT 'bin_pack',
    constraints        JSONB NOT NULL DEFAULT '{}',
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT jobs_replicas_check CHECK (replicas >= 1),
    CONSTRAINT jobs_cpu_request_check CHECK (cpu_request_mc > 0),
    CONSTRAINT jobs_mem_request_check CHECK (mem_request_mb > 0),
    CONSTRAINT jobs_scheduling_policy_check CHECK (scheduling_policy IN ('bin_pack', 'spread'))
);

CREATE TABLE tasks (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id             UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    status             TEXT NOT NULL DEFAULT 'pending',
    assigned_node_id   UUID REFERENCES nodes(id),
    assigned_epoch     BIGINT,
    retries_used       INT NOT NULL DEFAULT 0,
    exit_code          INT,
    last_error         TEXT,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    scheduled_at       TIMESTAMPTZ,
    started_at         TIMESTAMPTZ,
    finished_at        TIMESTAMPTZ,

    CONSTRAINT tasks_status_check CHECK (status IN (
        'pending', 'scheduled', 'running', 'succeeded', 'failed', 'orphaned', 'cancelled'
    ))
);

CREATE INDEX idx_tasks_status ON tasks (status);
CREATE INDEX idx_tasks_job_id ON tasks (job_id);
CREATE INDEX idx_tasks_assigned_node_status ON tasks (assigned_node_id, status);
