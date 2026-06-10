/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260605213000_broad-community-discovery/up.sql

    Purpose:

        Keep broad community discovery useful after the scheduler begins
        harvesting candidate hosts from all known ActivityPub traffic.

    Responsibilities:

        - hide discovered communities with fewer than two known remote posts
        - remove empty, unfollowed shells that no longer qualify
        - align the discovery index with the stricter activity threshold

    This file intentionally does NOT contain:

        - deletion of followed communities
        - deletion of communities with stored posts
        - changes to federation blocking or suppression policy
*/

UPDATE community_discovery
SET active=FALSE
WHERE COALESCE(remote_post_count, 0) < 2;

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

DROP INDEX IF EXISTS community_discovery_active_post_count_idx;

CREATE INDEX community_discovery_active_post_count_idx
ON community_discovery (host, last_seen DESC)
WHERE active AND remote_post_count >= 2;

/* end of migrations/20260605213000_broad-community-discovery/up.sql */
