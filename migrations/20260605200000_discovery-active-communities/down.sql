/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260605200000_discovery-active-communities/down.sql

    Purpose:

        Remove the discovery activity-evidence column and index.

    Responsibilities:

        - reverse the schema additions made by the up migration

    This file intentionally does NOT contain:

        - recreation of deleted empty community shells
        - changes to local communities
        - changes to followed communities
*/

DROP INDEX IF EXISTS community_discovery_active_post_count_idx;

ALTER TABLE community_discovery
    DROP COLUMN remote_post_count;

/* end of migrations/20260605200000_discovery-active-communities/down.sql */
