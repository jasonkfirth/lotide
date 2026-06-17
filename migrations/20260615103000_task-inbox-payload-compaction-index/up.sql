CREATE INDEX IF NOT EXISTS task_completed_inbox_payload_compaction_idx
ON task (completed_at)
WHERE state='completed'
AND kind IN ('ingest_object_from_inbox', 'verify_and_ingest_object_from_inbox')
AND params->>'discarded' IS NULL;
