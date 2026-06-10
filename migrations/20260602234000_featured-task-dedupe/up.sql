CREATE INDEX IF NOT EXISTS task_active_featured_community_idx
ON task ((params->>'community_id'))
WHERE kind='fetch_community_featured' AND state IN ('pending', 'running');

CREATE INDEX IF NOT EXISTS community_follow_local_community_idx
ON community_follow (community)
WHERE local;
