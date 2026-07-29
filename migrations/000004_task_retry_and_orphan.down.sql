DROP INDEX IF EXISTS idx_tasks_pending_retry;
ALTER TABLE tasks DROP COLUMN IF EXISTS next_retry_at;
