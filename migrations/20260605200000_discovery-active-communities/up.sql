/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260605200000_discovery-active-communities/up.sql

    Purpose:

        Keep the global community discovery list limited to communities
        whose remote server reports that they have had activity.

    Responsibilities:

        - store remote post-count evidence from discovery APIs
        - deactivate discovered communities with no post evidence
        - remove empty, unfollowed inactive remote community shells

    This file intentionally does NOT contain:

        - deletion of followed communities
        - deletion of communities that still have local post records
        - suppression of local communities
*/

ALTER TABLE community_discovery
    ADD COLUMN remote_post_count BIGINT;

UPDATE community_discovery
SET remote_post_count = post_counts.post_count
FROM (
    SELECT community.id AS community, COUNT(post.id)::BIGINT AS post_count
    FROM community
    LEFT JOIN post ON post.community=community.id
        AND post.approved
        AND NOT post.deleted
    GROUP BY community.id
) AS post_counts
WHERE community_discovery.community=post_counts.community
AND community_discovery.remote_post_count IS NULL;

UPDATE community_discovery
SET active=FALSE
WHERE COALESCE(remote_post_count, 0) <= 0;

UPDATE community_discovery
SET active=FALSE
FROM community_discovery_server
WHERE community_discovery.host=community_discovery_server.host
AND (
    community_discovery_server.suppressed_reason IS NOT NULL
    OR NOT community_discovery_server.active
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

CREATE INDEX community_discovery_active_post_count_idx
ON community_discovery (host, last_seen DESC)
WHERE active AND remote_post_count > 0;

/* end of migrations/20260605200000_discovery-active-communities/up.sql */
