/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260607133000_admin-cleanup-host-profiles/up.sql

    Purpose:

        Add operator controls for notification and inbox cleanup, and add
        indexes used by the consolidated host capability view.

    Responsibilities:

        - keep user notification cleanup opt-in
        - make failed inbox payload retention configurable
        - support host profile queries without scanning operational tables

    This file intentionally does NOT contain:

        - cleanup job implementation
        - host capability classification logic
        - user notification rendering
*/

BEGIN;
    ALTER TABLE site
        ADD COLUMN cleanup_notifications_enabled BOOLEAN NOT NULL DEFAULT (FALSE),
        ADD COLUMN cleanup_notification_retention_days INTEGER NOT NULL DEFAULT (365),
        ADD COLUMN cleanup_failed_inbox_task_payloads_enabled BOOLEAN NOT NULL DEFAULT (TRUE),
        ADD COLUMN cleanup_failed_inbox_task_payload_retention_days INTEGER NOT NULL DEFAULT (7);

    ALTER TABLE site
        ADD CONSTRAINT cleanup_notification_retention_days_range
            CHECK (cleanup_notification_retention_days BETWEEN 1 AND 3650),
        ADD CONSTRAINT cleanup_failed_inbox_task_payload_retention_days_range
            CHECK (cleanup_failed_inbox_task_payload_retention_days BETWEEN 1 AND 365);

    CREATE INDEX IF NOT EXISTS actor_target_profile_host_updated_idx
        ON actor_target_profile (
            lower(regexp_replace(substring(actor_ap_id from '^https?://([^/]+)'), '^www\.', '')),
            updated_at DESC
        );

    CREATE INDEX IF NOT EXISTS federation_event_host_status_created_idx
        ON federation_event (host, status, created_at DESC, id DESC)
        WHERE host IS NOT NULL;

    CREATE INDEX IF NOT EXISTS notification_cleanup_idx
        ON notification (created_at)
        WHERE created_at IS NOT NULL;
COMMIT;

/* end of migrations/20260607133000_admin-cleanup-host-profiles/up.sql */
