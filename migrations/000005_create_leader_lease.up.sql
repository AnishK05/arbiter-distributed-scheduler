CREATE TABLE leader_lease (
    id            INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    leader_id     TEXT NOT NULL DEFAULT '',
    leader_addr   TEXT NOT NULL DEFAULT '',
    epoch         BIGINT NOT NULL DEFAULT 0,
    acquired_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Expired sentinel so the first replica to start acquires immediately.
    expires_at    TIMESTAMPTZ NOT NULL DEFAULT TIMESTAMPTZ '1970-01-01 00:00:00+00'
);

INSERT INTO leader_lease (id, leader_id, leader_addr, epoch, acquired_at, expires_at)
VALUES (1, '', '', 0, now(), TIMESTAMPTZ '1970-01-01 00:00:00+00');
