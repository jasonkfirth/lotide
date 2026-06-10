CREATE INDEX IF NOT EXISTS task_completed_cleanup_idx ON task (completed_at)
WHERE state='completed' AND completed_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS task_failed_cleanup_idx ON task (attempted_at)
WHERE state='failed' AND attempted_at IS NOT NULL;
