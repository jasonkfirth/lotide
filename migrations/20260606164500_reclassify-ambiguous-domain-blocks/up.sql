/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260606164500_reclassify-ambiguous-domain-blocks/up.sql

    Purpose:

        Reclassify vague remote domain-block messages as diagnostics instead
        of proof that a server, community, or user is banned.

    Responsibilities:

        - clear suppressions created only from generic "Domain ... is blocked"
          text
        - clear older hard-coded suppressions that were based on the same weak
          evidence
        - allow those hosts and communities to be probed again

    This file intentionally does NOT contain:

        - removal of confirmed manual defederation records
        - deletion of communities, posts, comments, or follows
        - assumptions about the user's own ban status on a remote instance
*/

WITH ambiguous_host AS (
    SELECT host
    FROM community_discovery_server
    WHERE suppressed_reason IN (
        'Domain lotide.example is blocked',
        'Domain "lotide.example" is blocked',
        'Domain lotide.example is blocked by the remote instance',
        'InternalStr("Error in remote response: {\"error\":\"unknown\",\"message\":\"Domain \\\"lotide.example\\\" is blocked\"}")'
    )
    OR (
        host IN ('lemmy.blahaj.zone', 'lemmy.dbzer0.com')
        AND suppressed_reason LIKE 'Known domain block:%'
    )

    UNION

    SELECT lower(regexp_replace(substring(community.ap_id from '^https?://([^/]+)'), '^www\.', '')) AS host
    FROM community
    INNER JOIN community_server_visibility_suppression
        ON community_server_visibility_suppression.community=community.id
    WHERE community_server_visibility_suppression.reason IN (
        'Domain lotide.example is blocked',
        'Domain "lotide.example" is blocked',
        'Domain lotide.example is blocked by the remote instance',
        'InternalStr("Error in remote response: {\"error\":\"unknown\",\"message\":\"Domain \\\"lotide.example\\\" is blocked\"}")'
    )
    AND community.ap_id IS NOT NULL

    UNION

    SELECT lower(regexp_replace(substring(community.ap_id from '^https?://([^/]+)'), '^www\.', '')) AS host
    FROM community
    INNER JOIN community_user_visibility_suppression
        ON community_user_visibility_suppression.community=community.id
    WHERE community_user_visibility_suppression.reason IN (
        'Domain lotide.example is blocked',
        'Domain "lotide.example" is blocked',
        'Domain lotide.example is blocked by the remote instance',
        'InternalStr("Error in remote response: {\"error\":\"unknown\",\"message\":\"Domain \\\"lotide.example\\\" is blocked\"}")'
    )
    AND community.ap_id IS NOT NULL
), cleared_host AS (
    UPDATE community_discovery_server
    SET active=TRUE,
        failed_checks=0,
        latest_error=NULL,
        suppressed_reason=NULL,
        suppressed_at=NULL,
        interaction_probe_checked_at=NULL,
        interaction_probe_latest_error=NULL
    WHERE host IN (SELECT host FROM ambiguous_host WHERE host IS NOT NULL)
    RETURNING host
), cleared_server_suppression AS (
    DELETE FROM community_server_visibility_suppression
    USING community
    WHERE community.id=community_server_visibility_suppression.community
    AND community_server_visibility_suppression.reason IN (
        'Domain lotide.example is blocked',
        'Domain "lotide.example" is blocked',
        'Domain lotide.example is blocked by the remote instance',
        'InternalStr("Error in remote response: {\"error\":\"unknown\",\"message\":\"Domain \\\"lotide.example\\\" is blocked\"}")'
    )
    RETURNING community.id
), cleared_user_suppression AS (
    DELETE FROM community_user_visibility_suppression
    USING community
    WHERE community.id=community_user_visibility_suppression.community
    AND community_user_visibility_suppression.reason IN (
        'Domain lotide.example is blocked',
        'Domain "lotide.example" is blocked',
        'Domain lotide.example is blocked by the remote instance',
        'InternalStr("Error in remote response: {\"error\":\"unknown\",\"message\":\"Domain \\\"lotide.example\\\" is blocked\"}")'
    )
    RETURNING community.id
)
UPDATE community_discovery
SET active=TRUE
WHERE host IN (SELECT host FROM ambiguous_host WHERE host IS NOT NULL)
AND COALESCE(remote_post_count, 0) >= 2;

/* end of migrations/20260606164500_reclassify-ambiguous-domain-blocks/up.sql */
