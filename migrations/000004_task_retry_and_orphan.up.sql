ALTER TABLE tasks
    ADD COLUMN next_retry_at TIMESTAMPTZ;

-- Speeds up ClaimPendingTasksForScheduling's "ready for pickup" filter.
CREATE INDEX idx_tasks_pending_retry
    ON tasks (status, next_retry_at)
    WHERE status = 'pending';
