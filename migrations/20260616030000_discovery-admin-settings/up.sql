/*
    Project: Lotide database migrations
    -----------------------------------

    File: migrations/20260616030000_discovery-admin-settings/up.sql

    Purpose:

        Add operator-tunable community discovery pacing.

    Responsibilities:

        - store the number of discovery hosts queued per scheduler pass
        - store the base refresh interval for healthy discovery hosts
        - keep settings bounded so discovery cannot accidentally crowd out
          normal federation work

    This file intentionally does NOT contain:

        - discovery scheduling logic
        - federation target classification
        - admin page rendering changes
*/

BEGIN;
    ALTER TABLE site
        ADD COLUMN discovery_enqueue_limit INTEGER NOT NULL DEFAULT 100,
        ADD COLUMN discovery_refresh_interval_hours INTEGER NOT NULL DEFAULT 6;

    ALTER TABLE site
        ADD CONSTRAINT discovery_enqueue_limit_range
            CHECK (discovery_enqueue_limit BETWEEN 10 AND 500),
        ADD CONSTRAINT discovery_refresh_interval_hours_range
            CHECK (discovery_refresh_interval_hours BETWEEN 1 AND 168);
COMMIT;

/* end of migrations/20260616030000_discovery-admin-settings/up.sql */
