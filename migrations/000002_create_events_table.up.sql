CREATE TABLE events (
    id           BIGSERIAL PRIMARY KEY,
    entity_type  TEXT NOT NULL,
    entity_id    TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    message      TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_events_entity ON events (entity_type, entity_id);
CREATE INDEX idx_events_created_at ON events (created_at);
