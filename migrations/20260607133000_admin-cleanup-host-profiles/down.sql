/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260607133000_admin-cleanup-host-profiles/down.sql

    Purpose:

        Remove the admin cleanup settings and host profile indexes.

    Responsibilities:

        - drop indexes added for the host profile view
        - remove notification and inbox cleanup site settings

    This file intentionally does NOT contain:

        - restoration of deleted notification or task data
        - changes to existing federation health tables
*/

BEGIN;
    DROP INDEX IF EXISTS notification_cleanup_idx;
    DROP INDEX IF EXISTS federation_event_host_status_created_idx;
    DROP INDEX IF EXISTS actor_target_profile_host_updated_idx;

    ALTER TABLE site
        DROP CONSTRAINT cleanup_failed_inbox_task_payload_retention_days_range,
        DROP CONSTRAINT cleanup_notification_retention_days_range;

    ALTER TABLE site
        DROP COLUMN cleanup_failed_inbox_task_payload_retention_days,
        DROP COLUMN cleanup_failed_inbox_task_payloads_enabled,
        DROP COLUMN cleanup_notification_retention_days,
        DROP COLUMN cleanup_notifications_enabled;
COMMIT;

/* end of migrations/20260607133000_admin-cleanup-host-profiles/down.sql */
