/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260605220000_prune-cross-host-community-discovery/up.sql

    Purpose:

        Remove bad rows created when broad community discovery consumed
        federated "all communities" API responses as if they were local
        server community lists.

    Responsibilities:

        - deactivate discovery rows whose actor host differs from the scanned
          server host
        - deactivate discovery rows with fewer than two reported remote posts
        - remove empty, unfollowed community shells created from those rows

    This file intentionally does NOT contain:

        - deletion of followed communities
        - deletion of communities with stored posts
        - suppression of slow or ambiguous hosts
*/

UPDATE community_discovery
SET active=FALSE
FROM community
WHERE community.id=community_discovery.community
AND (
    lower(regexp_replace(substring(community.ap_id from '^https?://([^/]+)'), '^www\.', ''))
        IS DISTINCT FROM lower(regexp_replace(community_discovery.host, '^www\.', ''))
    OR COALESCE(community_discovery.remote_post_count, 0) < 2
);

DELETE FROM community
USING community_discovery
WHERE community.id=community_discovery.community
AND NOT community.local
AND NOT community.deleted
AND NOT community_discovery.active
AND NOT EXISTS (
    SELECT 1 FROM community_follow
    WHERE community_follow.community=community.id
    AND community_follow.local
)
AND NOT EXISTS (
    SELECT 1 FROM post
    WHERE post.community=community.id
);

/* end of migrations/20260605220000_prune-cross-host-community-discovery/up.sql */
