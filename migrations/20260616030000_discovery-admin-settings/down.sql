/*
    Project: Lotide database migrations
    -----------------------------------

    File: migrations/20260616030000_discovery-admin-settings/down.sql

    Purpose:

        Remove operator-tunable community discovery pacing.

    Responsibilities:

        - drop discovery pacing constraints
        - drop discovery pacing columns

    This file intentionally does NOT contain:

        - task cleanup
        - discovery result cleanup
        - admin page rendering changes
*/

BEGIN;
    ALTER TABLE site
        DROP CONSTRAINT discovery_enqueue_limit_range,
        DROP CONSTRAINT discovery_refresh_interval_hours_range;

    ALTER TABLE site
        DROP COLUMN discovery_enqueue_limit,
        DROP COLUMN discovery_refresh_interval_hours;
COMMIT;

/* end of migrations/20260616030000_discovery-admin-settings/down.sql */
