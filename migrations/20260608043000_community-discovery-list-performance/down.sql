/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260608043000_community-discovery-list-performance/down.sql

    Purpose:

        Remove indexes added for the global community directory.

    Responsibilities:

        - undo only the index additions from the matching up migration

    This file intentionally does NOT contain:

        - data rewrites
        - community discovery state changes
        - platform compatibility changes
*/

BEGIN;
    DROP INDEX IF EXISTS community_discovery_active_community_idx;
    DROP INDEX IF EXISTS community_remote_ap_host_idx;
    DROP INDEX IF EXISTS community_not_deleted_name_idx;
COMMIT;

/* end of migrations/20260608043000_community-discovery-list-performance/down.sql */
