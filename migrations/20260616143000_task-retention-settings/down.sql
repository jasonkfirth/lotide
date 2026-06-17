/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260616143000_task-retention-settings/down.sql

    Purpose:

        Remove the operator-tunable task retention settings.

    Responsibilities:

        - drop task retention constraints before dropping their columns
        - leave task rows untouched

    This file intentionally does NOT contain:

        - task row restoration
        - cleanup job changes
        - admin page changes
*/

BEGIN;
    ALTER TABLE site
        DROP CONSTRAINT cleanup_completed_task_retention_days_range,
        DROP CONSTRAINT cleanup_failed_task_retention_days_range,
        DROP CONSTRAINT cleanup_failed_inbox_task_payload_compaction_hours_range;

    ALTER TABLE site
        DROP COLUMN cleanup_completed_task_retention_days,
        DROP COLUMN cleanup_failed_task_retention_days,
        DROP COLUMN cleanup_failed_inbox_task_payload_compaction_hours;
COMMIT;

/* end of migrations/20260616143000_task-retention-settings/down.sql */
