/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260608043000_community-discovery-list-performance/up.sql

    Purpose:

        Add indexes for the global community directory and discovery worker.

    Responsibilities:

        - support alphabetic community directory pages
        - support normalized remote-host checks without full table scans
        - keep active discovery lookups cheap as the directory grows

    This file intentionally does NOT contain:

        - community discovery scheduling logic
        - ActivityPub platform parsing
        - visibility policy decisions
*/

BEGIN;
    CREATE INDEX IF NOT EXISTS community_not_deleted_name_idx
        ON community (lower(name), id)
        WHERE NOT deleted;

    CREATE INDEX IF NOT EXISTS community_remote_ap_host_idx
        ON community (
            lower(regexp_replace(substring(ap_id from '^https?://([^/]+)'), '^www\.', '')),
            id
        )
        WHERE NOT local
        AND NOT deleted
        AND ap_id IS NOT NULL;

    CREATE INDEX IF NOT EXISTS community_discovery_active_community_idx
        ON community_discovery (community, remote_post_count)
        WHERE active;
COMMIT;

/* end of migrations/20260608043000_community-discovery-list-performance/up.sql */
