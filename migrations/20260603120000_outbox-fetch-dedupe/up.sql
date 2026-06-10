CREATE INDEX IF NOT EXISTS task_active_outbox_community_idx
ON task ((params->>'community_id'))
WHERE kind='fetch_community_outbox' AND state IN ('pending', 'running');
