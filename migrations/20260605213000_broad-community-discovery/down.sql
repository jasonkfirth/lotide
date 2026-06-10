/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260605213000_broad-community-discovery/down.sql

    Purpose:

        Revert the discovery index predicate used by the broad community
        discovery migration.

    Responsibilities:

        - restore the previous one-or-more-post discovery index predicate

    This file intentionally does NOT contain:

        - recreation of deleted empty community shells
        - restoration of historical community_discovery.active values
        - changes to federation blocking or suppression policy
*/

DROP INDEX IF EXISTS community_discovery_active_post_count_idx;

CREATE INDEX community_discovery_active_post_count_idx
ON community_discovery (host, last_seen DESC)
WHERE active AND remote_post_count > 0;

/* end of migrations/20260605213000_broad-community-discovery/down.sql */
