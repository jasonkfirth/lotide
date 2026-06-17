/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260616143000_task-retention-settings/up.sql

    Purpose:

        Add operator-tunable task retention and task payload compaction
        settings.

    Responsibilities:

        - keep successful task row retention explicit
        - keep failed task row retention explicit
        - separate failed inbox task row retention from large payload retention

    This file intentionally does NOT contain:

        - task cleanup implementation
        - admin page rendering
        - task queue scheduling policy
*/

BEGIN;
    ALTER TABLE site
        ADD COLUMN cleanup_completed_task_retention_days INTEGER NOT NULL DEFAULT 3,
        ADD COLUMN cleanup_failed_task_retention_days INTEGER NOT NULL DEFAULT 14,
        ADD COLUMN cleanup_failed_inbox_task_payload_compaction_hours INTEGER NOT NULL DEFAULT 1;

    ALTER TABLE site
        ADD CONSTRAINT cleanup_completed_task_retention_days_range
            CHECK (cleanup_completed_task_retention_days BETWEEN 1 AND 30),
        ADD CONSTRAINT cleanup_failed_task_retention_days_range
            CHECK (cleanup_failed_task_retention_days BETWEEN 1 AND 365),
        ADD CONSTRAINT cleanup_failed_inbox_task_payload_compaction_hours_range
            CHECK (cleanup_failed_inbox_task_payload_compaction_hours BETWEEN 1 AND 168);
COMMIT;

/* end of migrations/20260616143000_task-retention-settings/up.sql */
