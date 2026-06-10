/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260605224500_host-interaction-probes/down.sql

    Purpose:

        Remove host-level interaction probe metadata.

    Responsibilities:

        - undo the schema changes from this migration

    This file intentionally does NOT contain:

        - removal of host suppressions learned from probes
        - task cleanup
        - community cleanup
*/

ALTER TABLE community_discovery_server
    DROP COLUMN interaction_probe_latest_error,
    DROP COLUMN interaction_probe_success_at,
    DROP COLUMN interaction_probe_checked_at;

/* end of migrations/20260605224500_host-interaction-probes/down.sql */
