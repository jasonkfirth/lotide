use futures::FutureExt;
use std::process::Stdio;
use std::sync::Arc;

use crate::types::{CollectionTargetLocalID, CommunityLocalID, UserLocalID};

const TASK_DEFAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);
const TASK_DISCOVERY_TIMEOUT: std::time::Duration = std::time::Duration::from_mins(1);
const TASK_ERROR_MAX_CHARS: usize = 4096;
const TASK_CLEANUP_BATCH_SIZE: i64 = 10000;
const TASK_CLEANUP_MAX_BATCHES: usize = 100;
const REMOTE_POST_CLEANUP_BATCH_SIZE: i64 = 100;
const REMOTE_POST_CLEANUP_MAX_BATCHES: usize = 50;
const REMOTE_COMMUNITY_CLEANUP_BATCH_SIZE: i64 = 500;
const REMOTE_COMMUNITY_CLEANUP_MAX_BATCHES: usize = 20;
const REMOTE_INTERACTION_CLEANUP_BATCH_SIZE: i64 = 10000;
const REMOTE_INTERACTION_CLEANUP_MAX_BATCHES: usize = 100;
const JANITOR_CRON: &str = "29 5 3 * * *";
const JANITOR_BATCH_SIZE: i64 = 1000;
const JANITOR_MAX_BATCHES: usize = 10;
const JANITOR_FOLLOW_REPAIR_LIMIT: i64 = 100;
const PG_REPACK_CRON: &str = "43 31 4 * * 0";
const PG_REPACK_TIMEOUT: std::time::Duration = std::time::Duration::from_mins(30);
const PG_REPACK_MIN_RELATION_BYTES: i64 = 64 * 1024 * 1024;
const PG_REPACK_MIN_DEAD_TUPLES: i64 = 10_000;
const PG_REPACK_MAX_TABLES: i64 = 2;
/*
    Broad community discovery is useful, but remote hosts can be slow or dead.
    Keep each scheduler pass bounded so discovery does not crowd out outgoing
    federation, previews, or reply fetches.
*/
const DEFAULT_COMMUNITY_DISCOVERY_ENQUEUE_LIMIT: i64 = 100;
const DEFAULT_COMMUNITY_DISCOVERY_REFRESH_INTERVAL_HOURS: i32 = 6;
const PREVIEW_CACHE_CLEANUP_CRON: &str = "37 19 * * * *";
const DISCOVERY_SETTINGS_SQL: &str = "\
SELECT discovery_enqueue_limit, discovery_refresh_interval_hours \
FROM site \
WHERE local";
const CLEANUP_SETTINGS_SQL: &str = "\
SELECT cleanup_remote_posts_enabled, cleanup_remote_post_retention_days, \
    cleanup_preview_posts_enabled, cleanup_preview_post_retention_hours, \
    cleanup_deleted_remote_communities_enabled, \
    cleanup_unfollowed_remote_communities_enabled, \
    cleanup_remote_interactions_enabled, \
    cleanup_notifications_enabled, cleanup_notification_retention_days, \
    cleanup_failed_inbox_task_payloads_enabled, \
    cleanup_failed_inbox_task_payload_retention_days, \
    cleanup_completed_task_retention_days, \
    cleanup_failed_task_retention_days, \
    cleanup_failed_inbox_task_payload_compaction_hours \
FROM site \
WHERE local";
const PG_REPACK_CANDIDATE_TABLES_SQL: &str = "\
SELECT schemaname, relname \
FROM pg_stat_user_tables \
WHERE schemaname='public' \
AND pg_total_relation_size(relid) >= $1 \
AND n_dead_tup >= $2 \
AND EXISTS (\
    SELECT 1 \
    FROM pg_index \
    WHERE pg_index.indrelid=pg_stat_user_tables.relid \
    AND pg_index.indisprimary\
) \
ORDER BY pg_total_relation_size(relid) DESC, relname \
LIMIT $3";
/*
    Remote community pages depend on periodic outbox reads. If those reads fall
    into the generic task bucket, a large inbox verification backlog can make a
    healthy followed community look stale or broken for hours.
*/
const TAKE_NEXT_TASK_SQL: &str = "\
UPDATE task SET state='running', attempted_at=current_timestamp, latest_error=NULL WHERE id=(\
    SELECT id FROM task \
        WHERE state='pending' \
        AND (attempted_at IS NULL OR attempted_at + (EXP(attempts) * INTERVAL '20 SECONDS') < current_timestamp) \
        ORDER BY \
            CASE \
                WHEN kind='deliver_to_audience' THEN 0 \
                WHEN kind='deliver_to_inbox' AND attempts=0 THEN 1 \
                WHEN kind IN (\
                    'seed_community_discovery_hosts', \
                    'seed_discourse_discovery_hosts'\
                ) AND attempts=0 THEN 2 \
                WHEN kind='fetch_community_outbox' AND params->>'preview'='true' THEN 3 \
                WHEN kind='fetch_collection_target_preview' THEN 3 \
                WHEN kind='fetch_remote_post_refresh' AND attempts=0 THEN 4 \
                WHEN kind='fetch_community_outbox' AND attempts=0 THEN 5 \
                WHEN kind='deliver_to_inbox' THEN 6 \
                WHEN kind='verify_and_ingest_object_from_inbox' AND attempts=0 THEN 7 \
                WHEN kind='fetch_post_replies' AND attempts=0 THEN 7 \
                WHEN kind='fetch_platform_post_thread' AND attempts=0 THEN 7 \
                WHEN kind='probe_community_host_interaction' AND attempts=0 THEN 8 \
                WHEN kind='discover_server_communities' AND attempts=0 \
                    AND EXISTS (\
                        SELECT 1 FROM community_discovery_server \
                        WHERE community_discovery_server.host=task.params->>'host' \
                        AND community_discovery_server.software IN (\
                            'fedigroups-directory', \
                            'mbin-compatible'\
                        )\
                    ) THEN 9 \
                WHEN kind='discover_server_communities' AND attempts=0 \
                    AND EXISTS (\
                        SELECT 1 FROM community_discovery_server \
                        WHERE community_discovery_server.host=task.params->>'host' \
                        AND community_discovery_server.software IN (\
                            'wordpress', \
                            'funkwhale', \
                            'owncast', \
                            'castopod', \
                            'writefreely', \
                            'postmarks', \
                            'bookwyrm', \
                            'pixelfed', \
                            'gotosocial', \
                            'misskey', \
                            'sharkey', \
                            'iceshrimp', \
                            'snac', \
                            'mitra', \
                            'wafrn', \
                            'bonfire', \
                            'gancio', \
                            'mobilizon'\
                        )\
                    ) THEN 10 \
                WHEN kind='discover_server_communities' AND attempts=0 THEN 11 \
                WHEN attempts>0 \
                    AND attempted_at < current_timestamp - INTERVAL '1 HOUR' \
                    AND kind IN (\
                        'fetch_community_featured', \
                        'fetch_community_outbox', \
                        'fetch_post_replies', \
                        'fetch_remote_post_refresh', \
                        'fetch_platform_post_thread'\
                    ) THEN 12 \
                WHEN attempts=0 THEN 13 \
                ELSE 14 \
            END, \
            id \
        FOR UPDATE SKIP LOCKED LIMIT 1\
    ) RETURNING id, kind, params";
/*
    Discovery has its own narrow runner.

    The normal task runner still has discovery in its priority order so a quiet
    server can drain everything from one queue. Busy servers can keep receiving
    inbox and thread-fetch work indefinitely, though, so discovery also gets a
    dedicated lane that only takes discovery and host-probe tasks.
*/
const TAKE_NEXT_DISCOVERY_TASK_SQL: &str = "\
UPDATE task SET state='running', attempted_at=current_timestamp, latest_error=NULL WHERE id=(\
    SELECT id FROM task \
        WHERE state='pending' \
        AND kind IN (\
            'seed_community_discovery_hosts', \
            'seed_discourse_discovery_hosts', \
            'discover_server_communities', \
            'probe_community_host_interaction'\
        ) \
        AND (attempted_at IS NULL OR attempted_at + (EXP(attempts) * INTERVAL '20 SECONDS') < current_timestamp) \
        ORDER BY \
            CASE \
                WHEN kind IN (\
                    'seed_community_discovery_hosts', \
                    'seed_discourse_discovery_hosts'\
                ) AND attempts=0 THEN 0 \
                WHEN kind='probe_community_host_interaction' AND attempts=0 THEN 1 \
                WHEN kind='discover_server_communities' AND attempts=0 \
                    AND EXISTS (\
                        SELECT 1 FROM community_discovery_server \
                        WHERE community_discovery_server.host=task.params->>'host' \
                        AND community_discovery_server.software IN (\
                            'fedigroups-directory', \
                            'mbin-compatible'\
                        )\
                    ) THEN 2 \
                WHEN kind='discover_server_communities' AND attempts=0 \
                    AND EXISTS (\
                        SELECT 1 FROM community_discovery_server \
                        WHERE community_discovery_server.host=task.params->>'host' \
                        AND community_discovery_server.software IN (\
                            'wordpress', \
                            'funkwhale', \
                            'owncast', \
                            'castopod', \
                            'writefreely', \
                            'postmarks', \
                            'bookwyrm', \
                            'pixelfed', \
                            'gotosocial', \
                            'misskey', \
                            'sharkey', \
                            'iceshrimp', \
                            'snac', \
                            'mitra', \
                            'wafrn', \
                            'bonfire', \
                            'gancio', \
                            'mobilizon'\
                        )\
                    ) THEN 3 \
                WHEN kind='discover_server_communities' AND attempts=0 THEN 4 \
                ELSE 5 \
            END, \
            id \
        FOR UPDATE SKIP LOCKED LIMIT 1\
    ) RETURNING id, kind, params";
/*
    Readback has its own narrow runner.

    Inbound inbox verification can arrive in large bursts. Reply fetches,
    platform thread refreshes, and post readback are what make local actions
    feel reliable to users, so they get a small dedicated lane instead of
    waiting behind every raw inbox request.
*/
const TAKE_NEXT_READBACK_TASK_SQL: &str = "\
UPDATE task SET state='running', attempted_at=current_timestamp, latest_error=NULL WHERE id=(\
    SELECT id FROM task \
        WHERE state='pending' \
        AND kind IN (\
            'fetch_community_featured', \
            'fetch_community_outbox', \
            'fetch_collection_target_preview', \
            'fetch_post_replies', \
            'fetch_remote_post_refresh', \
            'fetch_platform_post_thread'\
        ) \
        AND (attempted_at IS NULL OR attempted_at + (EXP(attempts) * INTERVAL '20 SECONDS') < current_timestamp) \
        ORDER BY \
            CASE \
                WHEN kind='fetch_remote_post_refresh' AND attempts=0 THEN 0 \
                WHEN kind='fetch_post_replies' AND attempts=0 THEN 1 \
                WHEN kind='fetch_platform_post_thread' AND attempts=0 THEN 2 \
                WHEN kind='fetch_community_outbox' AND params->>'preview'='true' THEN 3 \
                WHEN kind='fetch_collection_target_preview' THEN 3 \
                WHEN kind='fetch_community_outbox' AND attempts=0 THEN 4 \
                WHEN kind='fetch_community_featured' AND attempts=0 THEN 5 \
                WHEN attempts>0 \
                    AND attempted_at < current_timestamp - INTERVAL '1 HOUR' THEN 6 \
                ELSE 7 \
            END, \
            id \
        FOR UPDATE SKIP LOCKED LIMIT 1\
    ) RETURNING id, kind, params";
/*
    Inbox verification has its own lane too.

    Large Lemmy-family instances can send many Announce activities quickly.
    Keeping inbox work separate prevents the generic runner from becoming a
    catch-all bottleneck while still leaving outbound delivery first in the
    main queue.
*/
const TAKE_NEXT_INBOX_TASK_SQL: &str = "\
UPDATE task SET state='running', attempted_at=current_timestamp, latest_error=NULL WHERE id=(\
    SELECT id FROM task \
        WHERE state='pending' \
        AND kind IN (\
            'ingest_object_from_inbox', \
            'verify_and_ingest_object_from_inbox'\
        ) \
        AND (attempted_at IS NULL OR attempted_at + (EXP(attempts) * INTERVAL '20 SECONDS') < current_timestamp) \
        ORDER BY \
            CASE \
                WHEN kind='verify_and_ingest_object_from_inbox' AND attempts=0 THEN 0 \
                WHEN kind='ingest_object_from_inbox' AND attempts=0 THEN 1 \
                WHEN attempts>0 \
                    AND attempted_at < current_timestamp - INTERVAL '1 HOUR' THEN 2 \
                ELSE 3 \
            END, \
            id \
        FOR UPDATE SKIP LOCKED LIMIT 1\
    ) RETURNING id, kind, params";
const RESET_INTERRUPTED_TASKS_SQL: &str = "\
UPDATE task \
SET state=(CASE WHEN attempts + 1 < max_attempts THEN 'pending'::lt_task_state ELSE 'failed'::lt_task_state END), \
    attempts=attempts + 1, \
    latest_error='Worker stopped while task was running', \
    attempted_at=current_timestamp \
WHERE state='running'";
const CLEANUP_COMPLETED_TASKS_SQL: &str = "\
WITH old_task AS (\
    SELECT id FROM task \
    WHERE state='completed' \
    AND completed_at < current_timestamp - make_interval(days => $2::INTEGER) \
    ORDER BY completed_at \
    LIMIT $1 \
    FOR UPDATE SKIP LOCKED\
) DELETE FROM task USING old_task WHERE task.id=old_task.id";
const CLEANUP_FAILED_INBOX_TASKS_SQL: &str = "\
WITH old_task AS (\
    SELECT id FROM task \
    WHERE state='failed' \
    AND kind IN ('ingest_object_from_inbox', 'verify_and_ingest_object_from_inbox') \
    AND attempted_at < current_timestamp - make_interval(days => $2::INTEGER) \
    ORDER BY attempted_at \
    LIMIT $1 \
    FOR UPDATE SKIP LOCKED\
) DELETE FROM task USING old_task WHERE task.id=old_task.id";
const COMPACT_FAILED_INBOX_TASK_PAYLOADS_SQL: &str = "\
WITH compacted_task AS (\
    SELECT id \
    FROM task \
    WHERE state='failed' \
    AND kind IN ('ingest_object_from_inbox', 'verify_and_ingest_object_from_inbox') \
    AND attempted_at < current_timestamp - make_interval(hours => $2::INTEGER) \
    AND params->>'discarded' IS NULL \
    ORDER BY attempted_at \
    LIMIT $1 \
    FOR UPDATE SKIP LOCKED\
) UPDATE task \
SET params=json_build_object(\
    'discarded', TRUE, \
    'reason', 'inbox task params removed after permanent failure', \
    'original_bytes', octet_length(params::TEXT)\
) \
FROM compacted_task \
WHERE task.id=compacted_task.id";
const CLEANUP_FAILED_TASKS_SQL: &str = "\
WITH old_task AS (\
    SELECT id FROM task \
    WHERE state='failed' \
    AND attempted_at < current_timestamp - make_interval(days => $2::INTEGER) \
    ORDER BY attempted_at \
    LIMIT $1 \
    FOR UPDATE SKIP LOCKED\
) DELETE FROM task USING old_task WHERE task.id=old_task.id";
const FAIL_TASK_SQL: &str = "\
UPDATE task \
SET state=(CASE \
        WHEN $6 THEN 'failed'::lt_task_state \
        WHEN attempts + 1 < max_attempts THEN 'pending'::lt_task_state \
        ELSE 'failed'::lt_task_state \
    END), \
    attempts=(CASE WHEN $6 THEN max_attempts ELSE attempts + 1 END), \
    latest_error=$2, \
    attempted_at=current_timestamp, \
    params=(CASE \
        WHEN kind IN ($3, $4) \
            AND attempts + 1 >= max_attempts \
            AND $5 THEN json_build_object(\
            'discarded', TRUE, \
            'reason', 'inbox task params removed after permanent failure', \
            'original_bytes', octet_length(params::TEXT)\
        ) \
        ELSE params \
    END) \
WHERE id=$1";
/*
    Preview fetches are allowed for remote communities without an accepted
    local follow so users can inspect a community before subscribing. Those
    rows are a short-lived cache, not durable federation state, so unfollowed
    preview posts expire quickly unless a local user replies to or likes them.
*/
const CLEANUP_OLD_REMOTE_POSTS_SQL: &str = "\
WITH old_post AS (\
    SELECT post.id \
    FROM post \
    INNER JOIN community ON community.id=post.community \
    WHERE NOT post.local \
    AND NOT community.local \
    AND (\
        (\
            $2 \
            AND (\
                post.deleted \
                OR community.deleted \
                OR post.created < current_timestamp - make_interval(days => $3::INTEGER)\
            )\
        ) \
        OR (\
            $4 \
            AND post.updated_local < current_timestamp - make_interval(hours => $5::INTEGER) \
            AND NOT EXISTS (\
                SELECT 1 FROM community_follow \
                WHERE community_follow.community=community.id \
                AND community_follow.local \
                AND community_follow.accepted\
            )\
        )\
    ) \
    AND NOT EXISTS (SELECT 1 FROM reply WHERE reply.post=post.id AND reply.local) \
    AND NOT EXISTS (SELECT 1 FROM post_like WHERE post_like.post=post.id AND post_like.local) \
    AND NOT EXISTS (\
        SELECT 1 FROM reply_like \
        INNER JOIN reply ON reply.id=reply_like.reply \
        WHERE reply.post=post.id \
        AND reply_like.local\
    ) \
    ORDER BY post.created \
    LIMIT $1 \
    FOR UPDATE OF post SKIP LOCKED\
), old_reply AS (\
    SELECT reply.id \
    FROM reply \
    INNER JOIN old_post ON old_post.id=reply.post\
), deleted_notification AS (\
    DELETE FROM notification \
    USING old_post \
    WHERE notification.post=old_post.id \
    OR notification.parent_post=old_post.id \
    OR notification.reply IN (SELECT id FROM old_reply) \
    OR notification.parent_reply IN (SELECT id FROM old_reply)\
), deleted_post_like AS (\
    DELETE FROM post_like \
    USING old_post \
    WHERE post_like.post=old_post.id\
), deleted_reply_like AS (\
    DELETE FROM reply_like \
    USING old_reply \
    WHERE reply_like.reply=old_reply.id\
), deleted_post AS (\
    DELETE FROM post \
    USING old_post \
    WHERE post.id=old_post.id \
    RETURNING post.id\
) SELECT COUNT(*)::BIGINT FROM deleted_post";
const CLEANUP_OLD_NOTIFICATIONS_SQL: &str = "\
WITH old_notification AS (\
    SELECT notification.id \
    FROM notification \
    WHERE notification.created_at < current_timestamp - make_interval(days => $2::INTEGER) \
    AND NOT EXISTS (\
        SELECT 1 \
        FROM person \
        WHERE person.id=notification.to_user \
        AND notification.created_at > person.last_checked_notifications\
    ) \
    ORDER BY notification.created_at \
    LIMIT $1 \
    FOR UPDATE SKIP LOCKED\
) DELETE FROM notification \
USING old_notification \
WHERE notification.id=old_notification.id";
const CLEANUP_REMOTE_COMMUNITIES_SQL: &str = "\
WITH stale_community AS (\
    SELECT community.id \
    FROM community \
    WHERE NOT community.local \
    AND (\
        ($2 AND community.deleted) \
        OR ($3 AND NOT community.deleted)\
    ) \
    AND NOT EXISTS (\
        SELECT 1 FROM community_follow \
        WHERE community_follow.community=community.id \
        AND community_follow.local \
        AND community_follow.accepted\
    ) \
    AND NOT EXISTS (\
        SELECT 1 FROM community_discovery \
        WHERE community_discovery.community=community.id \
        AND community_discovery.active \
        AND community_discovery.remote_post_count >= 2\
    ) \
    AND NOT EXISTS (SELECT 1 FROM post WHERE post.community=community.id) \
    ORDER BY community.id \
    LIMIT $1 \
    FOR UPDATE SKIP LOCKED\
), deleted_follow AS (\
    DELETE FROM community_follow \
    USING stale_community \
    WHERE community_follow.community=stale_community.id\
), deleted_moderator AS (\
    DELETE FROM community_moderator \
    USING stale_community \
    WHERE community_moderator.community=stale_community.id\
), deleted_community AS (\
    DELETE FROM community \
    USING stale_community \
    WHERE community.id=stale_community.id \
    RETURNING community.id\
) SELECT COUNT(*)::BIGINT FROM deleted_community";
const CLEANUP_OLD_REMOTE_POST_LIKES_SQL: &str = "\
WITH old_like AS (\
    SELECT post_like.post, post_like.person \
    FROM post_like \
    INNER JOIN post ON post.id=post_like.post \
    WHERE NOT post_like.local \
    AND NOT post.local \
    AND (\
        post.deleted \
        OR post.created < current_timestamp - make_interval(days => $2::INTEGER)\
    ) \
    ORDER BY post_like.post, post_like.person \
    LIMIT $1 \
    FOR UPDATE OF post_like SKIP LOCKED\
), deleted_like AS (\
    DELETE FROM post_like \
    USING old_like \
    WHERE post_like.post=old_like.post \
    AND post_like.person=old_like.person \
    RETURNING post_like.post\
), affected_post AS (\
    SELECT DISTINCT post FROM deleted_like\
), updated_post AS (\
    UPDATE post \
    SET cached_likes_for_sort=(\
        SELECT COUNT(*) \
        FROM post_like \
        WHERE post_like.post=post.id \
        AND post_like.person != post.author\
    ) \
    WHERE post.id IN (SELECT post FROM affected_post) \
    RETURNING post.id\
) SELECT COUNT(*)::BIGINT FROM deleted_like";
const CLEANUP_OLD_REMOTE_REPLY_LIKES_SQL: &str = "\
WITH old_like AS (\
    SELECT reply_like.reply, reply_like.person \
    FROM reply_like \
    INNER JOIN reply ON reply.id=reply_like.reply \
    INNER JOIN post ON post.id=reply.post \
    WHERE NOT reply_like.local \
    AND NOT reply.local \
    AND NOT post.local \
    AND (\
        reply.deleted \
        OR post.deleted \
        OR post.created < current_timestamp - make_interval(days => $2::INTEGER)\
    ) \
    ORDER BY reply_like.reply, reply_like.person \
    LIMIT $1 \
    FOR UPDATE OF reply_like SKIP LOCKED\
), deleted_like AS (\
    DELETE FROM reply_like \
    USING old_like \
    WHERE reply_like.reply=old_like.reply \
    AND reply_like.person=old_like.person \
    RETURNING reply_like.reply\
) SELECT COUNT(*)::BIGINT FROM deleted_like";
const ENQUEUE_FOLLOWED_REMOTE_COMMUNITY_OUTBOX_FETCHES_SQL: &str = "\
INSERT INTO task (kind, params, max_attempts, created_at) \
SELECT $1, json_build_object(\
    'community_id', community.id, \
    'outbox_url', community.ap_outbox\
), $2, current_timestamp \
FROM community \
WHERE NOT community.local \
AND NOT community.deleted \
AND community.ap_outbox IS NOT NULL \
AND NOT EXISTS (\
    SELECT 1 FROM community_discovery_server \
    WHERE community_discovery_server.host=lower(regexp_replace(substring(community.ap_id from '^https?://([^/]+)'), '^www\\.', '')) \
    AND (\
        community_discovery_server.suppressed_reason IS NOT NULL \
        OR (\
            NOT community_discovery_server.active \
            AND community_discovery_server.last_checked > current_timestamp - INTERVAL '24 HOURS'\
        ) \
        OR (\
            community_discovery_server.failed_checks >= 3 \
            AND community_discovery_server.last_checked > current_timestamp - INTERVAL '12 HOURS'\
        )\
    )\
) \
AND EXISTS (\
    SELECT 1 FROM community_follow \
    WHERE community_follow.community=community.id \
    AND community_follow.local \
    AND community_follow.accepted\
) \
AND NOT EXISTS (\
    SELECT 1 FROM task \
    WHERE task.kind=$1 \
    AND task.state='failed' \
    AND task.params->>'community_id'=community.id::TEXT \
    AND task.attempted_at > current_timestamp - INTERVAL '6 HOURS'\
) \
AND NOT EXISTS (\
    SELECT 1 FROM task \
    WHERE task.kind=$1 \
    AND task.state IN ('pending', 'running') \
    AND task.params->>'community_id'=community.id::TEXT\
)";
const UPSERT_KNOWN_COMMUNITY_DISCOVERY_SERVERS_SQL: &str = "\
INSERT INTO community_discovery_server (host) \
SELECT DISTINCT host \
FROM (\
    SELECT lower(regexp_replace(substring(ap_url from '^https?://([^/]+)'), '^www\\.', '')) AS host \
    FROM (\
        SELECT ap_id AS ap_url FROM community WHERE NOT local AND NOT deleted \
        UNION ALL SELECT ap_inbox FROM community WHERE NOT local AND NOT deleted \
        UNION ALL SELECT ap_shared_inbox FROM community WHERE NOT local AND NOT deleted \
        UNION ALL SELECT ap_outbox FROM community WHERE NOT local AND NOT deleted \
        UNION ALL SELECT ap_followers FROM community WHERE NOT local AND NOT deleted \
        UNION ALL SELECT ap_id FROM post WHERE NOT local AND NOT deleted \
        UNION ALL SELECT ap_id FROM reply WHERE NOT local AND NOT deleted \
        UNION ALL SELECT ap_id FROM person WHERE NOT local \
        UNION ALL SELECT ap_inbox FROM person WHERE NOT local \
        UNION ALL SELECT ap_shared_inbox FROM person WHERE NOT local \
        UNION ALL SELECT ap_id FROM post_like WHERE NOT local \
        UNION ALL SELECT ap_id FROM reply_like WHERE NOT local\
    ) AS candidate_url \
    WHERE ap_url IS NOT NULL \
    AND ap_url ~ '^https?://'\
) AS candidate_host \
WHERE host IS NOT NULL \
ON CONFLICT (host) DO NOTHING";
const ENQUEUE_COMMUNITY_DISCOVERY_HOST_SEED_SQL: &str = "\
INSERT INTO task (kind, params, max_attempts, created_at) \
SELECT $1, '{}'::JSON, $2, current_timestamp \
WHERE NOT EXISTS (\
    SELECT 1 FROM task \
    WHERE task.kind=$1 \
    AND task.state IN ('pending', 'running')\
) \
AND NOT EXISTS (\
    SELECT 1 FROM task \
    WHERE task.kind=$1 \
    AND task.state='completed' \
    AND task.completed_at > current_timestamp - INTERVAL '24 HOURS'\
) \
AND NOT EXISTS (\
    SELECT 1 FROM task \
    WHERE task.kind=$1 \
    AND task.state='failed' \
    AND task.attempted_at > current_timestamp - INTERVAL '6 HOURS'\
)";
const ENQUEUE_DISCOURSE_DISCOVERY_HOST_SEED_SQL: &str = "\
INSERT INTO task (kind, params, max_attempts, created_at) \
SELECT $1, '{}'::JSON, $2, current_timestamp \
WHERE NOT EXISTS (\
    SELECT 1 FROM task \
    WHERE task.kind=$1 \
    AND task.state IN ('pending', 'running')\
) \
AND NOT EXISTS (\
    SELECT 1 FROM task \
    WHERE task.kind=$1 \
    AND task.state='completed' \
    AND task.completed_at > current_timestamp - INTERVAL '7 DAYS'\
) \
AND NOT EXISTS (\
    SELECT 1 FROM task \
    WHERE task.kind=$1 \
    AND task.state='failed' \
    AND task.attempted_at > current_timestamp - INTERVAL '6 HOURS'\
)";
const ENQUEUE_DUE_COMMUNITY_DISCOVERY_SQL: &str = "\
INSERT INTO task (kind, params, max_attempts, created_at) \
SELECT $1, \
    json_build_object(\
        'host', community_discovery_server.host, \
        'software', community_discovery_server.software\
    ), \
    $2, current_timestamp \
FROM community_discovery_server \
LEFT JOIN LATERAL (\
    SELECT COUNT(*) FILTER (\
            WHERE community_discovery.active \
            AND community_discovery.remote_post_count >= 2\
        ) AS useful_community_count, \
        max(community_discovery.last_seen) FILTER (\
            WHERE community_discovery.active \
            AND community_discovery.remote_post_count >= 2\
        ) AS newest_useful_community_seen \
    FROM community_discovery \
    WHERE community_discovery.host=community_discovery_server.host\
) AS discovery_state ON TRUE \
WHERE suppressed_reason IS NULL \
AND NOT EXISTS (\
    SELECT 1 FROM community_server_visibility_suppression \
    INNER JOIN community \
        ON community.id=community_server_visibility_suppression.community \
    WHERE lower(regexp_replace(substring(community.ap_id from '^https?://([^/]+)'), '^www\\.', ''))=community_discovery_server.host\
) \
AND (\
    (active AND failed_checks=0 \
        AND (last_checked IS NULL OR last_checked < current_timestamp - make_interval(hours => $4::INTEGER))) \
    OR (active AND failed_checks=1 \
        AND (last_checked IS NULL OR last_checked < current_timestamp - make_interval(hours => ($4::INTEGER * 2)))) \
    OR (active AND failed_checks>=2 \
        AND (last_checked IS NULL OR last_checked < current_timestamp - make_interval(hours => ($4::INTEGER * 4)))) \
    OR (NOT active AND (last_checked IS NULL OR last_checked < current_timestamp - make_interval(hours => ($4::INTEGER * 4))))\
) \
AND NOT EXISTS (\
    SELECT 1 FROM task \
    WHERE task.kind=$1 \
    AND task.state IN ('pending', 'running') \
    AND task.params->>'host'=community_discovery_server.host\
) \
ORDER BY \
    CASE \
        WHEN community_discovery_server.software IN (\
            'fedigroups-directory', \
            'mbin-compatible'\
        ) THEN 0 \
        WHEN community_discovery_server.software IN (\
            'wordpress', \
            'funkwhale', \
            'owncast', \
            'castopod', \
            'writefreely', \
            'postmarks', \
            'bookwyrm', \
            'pixelfed', \
            'gotosocial', \
            'misskey', \
            'sharkey', \
            'iceshrimp', \
            'snac', \
            'mitra', \
            'wafrn', \
            'bonfire', \
            'gancio', \
            'mobilizon'\
        ) THEN 1 \
        WHEN community_discovery_server.software IN ('discourse', 'hubzilla', 'friendica') THEN 2 \
        ELSE 3 \
    END, \
    CASE \
        WHEN discovery_state.useful_community_count > 0 \
            AND COALESCE(discovery_state.newest_useful_community_seen, 'epoch'::TIMESTAMPTZ) \
                < current_timestamp - INTERVAL '2 DAYS' THEN 0 \
        ELSE 1 \
    END, \
    COALESCE(discovery_state.newest_useful_community_seen, 'epoch'::TIMESTAMPTZ), \
    CASE \
        WHEN active AND failed_checks=0 THEN 0 \
        WHEN active THEN 1 \
        ELSE 2 \
    END, \
    COALESCE(last_checked, 'epoch'::TIMESTAMPTZ), \
    host \
LIMIT $3";
const ENQUEUE_DUE_COMMUNITY_INTERACTION_PROBES_SQL: &str = "\
INSERT INTO task (kind, params, max_attempts, created_at) \
SELECT $1, json_build_object('host', host), $2, current_timestamp \
FROM (\
    SELECT host \
    FROM community_discovery_server \
    WHERE (\
        interaction_probe_checked_at IS NULL \
        OR (\
            suppressed_reason IS NOT NULL \
            AND interaction_probe_checked_at < current_timestamp - INTERVAL '7 DAYS'\
        ) \
        OR (\
            suppressed_reason IS NULL \
            AND interaction_probe_checked_at < current_timestamp - INTERVAL '3 DAYS'\
        )\
    ) \
    AND EXISTS (\
        SELECT 1 \
        FROM community \
        INNER JOIN post ON post.community=community.id \
        WHERE NOT community.local \
        AND NOT community.deleted \
        AND community.ap_id IS NOT NULL \
        AND COALESCE(community.ap_inbox, community.ap_shared_inbox) IS NOT NULL \
        AND lower(regexp_replace(substring(community.ap_id from '^https?://([^/]+)'), '^www\\.', ''))=community_discovery_server.host \
        AND NOT post.local \
        AND NOT post.deleted \
        AND post.approved \
        AND post.ap_id IS NOT NULL\
    ) \
    AND NOT EXISTS (\
        SELECT 1 FROM task \
        WHERE task.kind=$1 \
        AND task.state IN ('pending', 'running') \
        AND task.params->>'host'=community_discovery_server.host\
    ) \
ORDER BY COALESCE(interaction_probe_checked_at, 'epoch'::TIMESTAMPTZ), host \
    LIMIT 50\
) AS due_host";
/*
    Janitor checks

    These are deterministic database repairs for drift we can recognize from
    the schema itself. They are kept small and local: no remote fetches, no
    broad deletes, and every write is limited so a large old instance does not
    monopolize the worker.
*/
const JANITOR_REPAIR_CONVENTIONAL_COMMUNITY_ENDPOINTS_SQL: &str = "\
WITH target_community AS (\
    SELECT id, regexp_replace(ap_id, '/+$', '') AS actor_url \
    FROM community \
    WHERE NOT local \
    AND ap_id IS NOT NULL \
    AND ap_id ~* '^https?://[^/]+/((apub/)?communities|video-channels|c|m|magazine|magazines)/[^/?#]+/?$' \
    AND (ap_inbox IS NULL OR ap_outbox IS NULL OR ap_followers IS NULL) \
    ORDER BY id \
    LIMIT $1 \
    FOR UPDATE SKIP LOCKED\
) UPDATE community \
SET ap_inbox=COALESCE(community.ap_inbox, target_community.actor_url || '/inbox'), \
    ap_outbox=COALESCE(community.ap_outbox, target_community.actor_url || '/outbox'), \
    ap_followers=COALESCE(community.ap_followers, target_community.actor_url || '/followers') \
FROM target_community \
WHERE community.id=target_community.id";
const JANITOR_PENDING_COMMUNITY_FOLLOWS_SQL: &str = "\
SELECT community_follow.community, community_follow.follower \
FROM community_follow \
INNER JOIN community ON community.id=community_follow.community \
WHERE community_follow.local \
AND NOT community.local \
AND NOT community.deleted \
AND NOT community_follow.accepted \
AND community_follow.federation_sent_at IS NULL \
AND community.ap_id IS NOT NULL \
AND COALESCE(community.ap_inbox, community.ap_shared_inbox) IS NOT NULL \
ORDER BY community_follow.community, community_follow.follower \
LIMIT $1";
const JANITOR_PENDING_COMMUNITY_FOLLOW_UNDOS_SQL: &str = "\
SELECT local_community_follow_undo.id, local_community_follow_undo.community, \
    local_community_follow_undo.follower \
FROM local_community_follow_undo \
INNER JOIN community ON community.id=local_community_follow_undo.community \
WHERE local_community_follow_undo.federation_sent_at IS NULL \
AND NOT community.local \
AND community.ap_id IS NOT NULL \
AND COALESCE(community.ap_inbox, community.ap_shared_inbox) IS NOT NULL \
AND NOT EXISTS (\
    SELECT 1 FROM task \
    WHERE task.kind='deliver_to_inbox' \
    AND task.state IN ('pending', 'running') \
    AND task.params::TEXT LIKE '%' || local_community_follow_undo.id::TEXT || '%'\
) \
ORDER BY local_community_follow_undo.created_at, local_community_follow_undo.id \
LIMIT $1";
const JANITOR_COMPLETE_LOCAL_COMMUNITY_FOLLOW_UNDOS_SQL: &str = "\
WITH local_undo AS (\
    SELECT local_community_follow_undo.id \
    FROM local_community_follow_undo \
    INNER JOIN community ON community.id=local_community_follow_undo.community \
    WHERE local_community_follow_undo.federation_received_at IS NULL \
    AND community.local \
    ORDER BY local_community_follow_undo.created_at, local_community_follow_undo.id \
    LIMIT $1 \
    FOR UPDATE OF local_community_follow_undo SKIP LOCKED\
) UPDATE local_community_follow_undo \
SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp), \
    federation_received_at=COALESCE(federation_received_at, current_timestamp) \
FROM local_undo \
WHERE local_community_follow_undo.id=local_undo.id";
const JANITOR_COMPLETE_LOCAL_USER_FOLLOW_UNDOS_SQL: &str = "\
WITH local_undo AS (\
    SELECT local_user_follow_undo.id \
    FROM local_user_follow_undo \
    INNER JOIN person ON person.id=local_user_follow_undo.target \
    WHERE local_user_follow_undo.federation_received_at IS NULL \
    AND person.local \
    ORDER BY local_user_follow_undo.created_at, local_user_follow_undo.id \
    LIMIT $1 \
    FOR UPDATE OF local_user_follow_undo SKIP LOCKED\
) UPDATE local_user_follow_undo \
SET federation_sent_at=COALESCE(federation_sent_at, current_timestamp), \
    federation_received_at=COALESCE(federation_received_at, current_timestamp) \
FROM local_undo \
WHERE local_user_follow_undo.id=local_undo.id";
const JANITOR_PENDING_COLLECTION_TARGET_FOLLOW_UNDOS_SQL: &str = "\
SELECT local_collection_target_follow_undo.id, \
    local_collection_target_follow_undo.collection_target, \
    local_collection_target_follow_undo.follower \
FROM local_collection_target_follow_undo \
INNER JOIN collection_target \
    ON collection_target.id=local_collection_target_follow_undo.collection_target \
WHERE local_collection_target_follow_undo.federation_sent_at IS NULL \
AND collection_target.ap_id IS NOT NULL \
AND collection_target.owner_ap_id IS NOT NULL \
AND COALESCE(collection_target.owner_shared_inbox, collection_target.owner_inbox) IS NOT NULL \
AND NOT EXISTS (\
    SELECT 1 FROM task \
    WHERE task.kind='deliver_to_inbox' \
    AND task.state IN ('pending', 'running') \
    AND task.params::TEXT LIKE '%' || local_collection_target_follow_undo.id::TEXT || '%'\
) \
ORDER BY local_collection_target_follow_undo.created_at, local_collection_target_follow_undo.id \
LIMIT $1";
const JANITOR_PENDING_USER_FOLLOW_UNDOS_SQL: &str = "\
SELECT local_user_follow_undo.id, local_user_follow_undo.target, \
    local_user_follow_undo.follower \
FROM local_user_follow_undo \
INNER JOIN person ON person.id=local_user_follow_undo.target \
WHERE local_user_follow_undo.federation_sent_at IS NULL \
AND NOT person.local \
AND person.ap_id IS NOT NULL \
AND person.ap_inbox IS NOT NULL \
AND NOT EXISTS (\
    SELECT 1 FROM task \
    WHERE task.kind='deliver_to_inbox' \
    AND task.state IN ('pending', 'running') \
    AND task.params::TEXT LIKE '%' || local_user_follow_undo.id::TEXT || '%'\
) \
ORDER BY local_user_follow_undo.created_at, local_user_follow_undo.id \
LIMIT $1";
const JANITOR_RECALCULATE_POST_LIKES_SQL: &str = "\
WITH candidate AS (\
    SELECT post.id, COUNT(post_like.person)::INTEGER AS like_count \
    FROM post \
    LEFT OUTER JOIN post_like \
        ON post_like.post=post.id \
        AND post_like.person != post.author \
    GROUP BY post.id \
    HAVING post.cached_likes_for_sort IS DISTINCT FROM COUNT(post_like.person)::INTEGER \
    ORDER BY post.id \
    LIMIT $1\
) UPDATE post \
SET cached_likes_for_sort=candidate.like_count \
FROM candidate \
WHERE post.id=candidate.id";
const JANITOR_DEACTIVATE_STALE_COMMUNITY_DISCOVERY_SQL: &str = "\
WITH stale_discovery AS (\
    SELECT community_discovery.community \
    FROM community_discovery \
    INNER JOIN community ON community.id=community_discovery.community \
    INNER JOIN community_discovery_server \
        ON community_discovery_server.host=community_discovery.host \
    WHERE community_discovery.active \
    AND (\
        community.deleted \
        OR COALESCE(community_discovery.remote_post_count, 0) < 2 \
        OR community_discovery_server.suppressed_reason IS NOT NULL\
    ) \
    ORDER BY community_discovery.community \
    LIMIT $1 \
    FOR UPDATE OF community_discovery SKIP LOCKED\
) UPDATE community_discovery \
SET active=FALSE \
FROM stale_discovery \
WHERE community_discovery.community=stale_discovery.community";
const JANITOR_REACTIVATE_CURRENT_COMMUNITY_DISCOVERY_SQL: &str = "\
WITH current_discovery AS (\
    SELECT community_discovery.community \
    FROM community_discovery \
    INNER JOIN community ON community.id=community_discovery.community \
    INNER JOIN community_discovery_server \
        ON community_discovery_server.host=community_discovery.host \
    WHERE NOT community_discovery.active \
    AND NOT community.deleted \
    AND community_discovery.remote_post_count >= 2 \
    AND community_discovery_server.active \
    AND community_discovery_server.failed_checks < 3 \
    AND community_discovery_server.suppressed_reason IS NULL \
    ORDER BY community_discovery.community \
    LIMIT $1 \
    FOR UPDATE OF community_discovery SKIP LOCKED\
) UPDATE community_discovery \
SET active=TRUE \
FROM current_discovery \
WHERE community_discovery.community=current_discovery.community";
const JANITOR_COMPACT_COMPLETED_INBOX_TASK_PAYLOADS_SQL: &str = "\
WITH compacted_task AS (\
    SELECT id \
    FROM task \
    WHERE state='completed' \
    AND kind IN ('ingest_object_from_inbox', 'verify_and_ingest_object_from_inbox') \
    AND completed_at < current_timestamp - INTERVAL '1 HOUR' \
    AND params->>'discarded' IS NULL \
    ORDER BY completed_at \
    LIMIT $1 \
    FOR UPDATE SKIP LOCKED\
) UPDATE task \
SET params=json_build_object(\
    'discarded', TRUE, \
    'reason', 'inbox task params removed after successful ingest', \
    'original_bytes', octet_length(params::TEXT)\
) \
FROM compacted_task \
WHERE task.id=compacted_task.id";
const JANITOR_FAIL_TERMINAL_INBOX_TASKS_SQL: &str = "\
WITH terminal_task AS (\
    SELECT id \
    FROM task \
    WHERE state='pending' \
    AND kind IN ('ingest_object_from_inbox', 'verify_and_ingest_object_from_inbox') \
    AND attempted_at IS NOT NULL \
    AND latest_error IS NOT NULL \
    AND (\
        lower(latest_error) LIKE '%just a moment%' \
        OR lower(latest_error) LIKE '%<!doctype html%' \
        OR lower(latest_error) LIKE '%<html%' \
        OR (lower(latest_error) LIKE '%error%' AND lower(latest_error) LIKE '%gone%') \
        OR lower(latest_error) LIKE '%couldnt_find_activity%' \
        OR lower(latest_error) LIKE '%not found%' \
        OR lower(latest_error) LIKE '%no such like%' \
        OR lower(latest_error) LIKE '%tombstone%' \
        OR lower(latest_error) LIKE '%status: 404%' \
        OR lower(latest_error) LIKE '%status: 410%' \
        OR lower(latest_error) LIKE '%unknown content type found for activity%' \
        OR lower(latest_error) LIKE '%invalid or unsupported data%' \
        OR lower(latest_error) LIKE '%signature check failed%' \
        OR lower(latest_error) LIKE '%request not signed%' \
        OR lower(latest_error) LIKE '%http body exceeded%' \
        OR lower(latest_error) LIKE '%entity too large%' \
        OR lower(latest_error) LIKE '%not a person%' \
        OR lower(latest_error) LIKE '%not a group%' \
        OR lower(latest_error) LIKE '%data did not match any variant of untagged enum knownobject%' \
        OR lower(latest_error) LIKE '%data did not match any variant of untagged enum either%' \
        OR lower(latest_error) LIKE '%notcontained%' \
        OR lower(latest_error) LIKE '%status: 400%' \
        OR lower(latest_error) LIKE '%status: 403%'\
    ) \
    ORDER BY attempted_at \
    LIMIT $1 \
    FOR UPDATE SKIP LOCKED\
) UPDATE task \
SET state='failed'::lt_task_state, \
    attempts=max_attempts, \
    attempted_at=current_timestamp \
FROM terminal_task \
WHERE task.id=terminal_task.id";
const JANITOR_COMPLETE_IRRELEVANT_INBOX_TASKS_SQL: &str = "\
WITH inbox_task AS (\
    SELECT task.id, (task.params->>'body')::JSONB AS body_json \
    FROM task \
    WHERE task.state='pending' \
    AND task.kind='verify_and_ingest_object_from_inbox'\
), untracked_announce AS (\
    SELECT inbox_task.id \
    FROM inbox_task \
    WHERE inbox_task.body_json->>'type'='Announce' \
    AND jsonb_typeof(inbox_task.body_json->'actor')='string' \
    AND NOT EXISTS (\
        SELECT 1 \
        FROM community \
        INNER JOIN community_follow \
            ON community_follow.community=community.id \
            AND community_follow.local \
            AND community_follow.accepted \
        WHERE NOT community.deleted \
        AND community.ap_id=inbox_task.body_json->>'actor'\
    )\
), delete_task AS (\
    SELECT inbox_task.id, \
        inbox_task.body_json->>'actor' AS actor, \
        CASE \
            WHEN jsonb_typeof(inbox_task.body_json->'object')='string' \
            THEN inbox_task.body_json->>'object' \
            WHEN jsonb_typeof(inbox_task.body_json->'object')='object' \
            THEN inbox_task.body_json->'object'->>'id' \
            ELSE NULL \
        END AS object_id \
    FROM inbox_task \
    WHERE inbox_task.body_json->>'type'='Delete' \
    AND jsonb_typeof(inbox_task.body_json->'actor')='string'\
), irrelevant_delete AS (\
    SELECT delete_task.id \
    FROM delete_task \
    WHERE delete_task.object_id IS NOT NULL \
    AND NOT EXISTS (SELECT 1 FROM person WHERE ap_id=delete_task.actor OR ap_id=delete_task.object_id) \
    AND NOT EXISTS (SELECT 1 FROM community WHERE ap_id=delete_task.actor OR ap_id=delete_task.object_id) \
    AND NOT EXISTS (SELECT 1 FROM post WHERE ap_id=delete_task.object_id) \
    AND NOT EXISTS (SELECT 1 FROM reply WHERE ap_id=delete_task.object_id) \
    AND NOT EXISTS (SELECT 1 FROM post_like WHERE ap_id=delete_task.object_id) \
    AND NOT EXISTS (SELECT 1 FROM reply_like WHERE ap_id=delete_task.object_id)\
), target_task AS (\
    SELECT id FROM untracked_announce \
    UNION ALL \
    SELECT id FROM irrelevant_delete \
    ORDER BY id \
    LIMIT $1\
) UPDATE task \
SET state='completed'::lt_task_state, \
    attempts=attempts + 1, \
    completed_at=current_timestamp, \
    latest_error=NULL \
FROM target_task \
WHERE task.id=target_task.id";
const JANITOR_REPAIR_BLANK_POST_TITLES_SQL: &str = "\
WITH candidate AS (\
    SELECT post.id, \
        COALESCE(NULLIF(LEFT(source.first_line, 80), ''), '[no title]') AS new_title \
    FROM post \
    LEFT JOIN LATERAL (\
        SELECT btrim(line) AS first_line \
        FROM regexp_split_to_table(\
            replace(\
                regexp_replace(\
                    COALESCE(post.content_text, post.content_markdown, post.content_html, ''), \
                    '<[^>]*>', \
                    ' ', \
                    'g'\
                ), \
                chr(13), \
                ''\
            ), \
            chr(10)\
        ) AS line \
        WHERE btrim(line) <> '' \
        LIMIT 1\
    ) AS source ON TRUE \
    WHERE post.title IS NULL \
    OR btrim(post.title)='' \
    ORDER BY post.id \
    LIMIT $1 \
    FOR UPDATE OF post SKIP LOCKED\
) UPDATE post \
SET title=candidate.new_title \
FROM candidate \
WHERE post.id=candidate.id";

#[derive(Clone, Copy, Debug)]
struct CleanupSettings {
    cleanup_remote_posts_enabled: bool,
    cleanup_remote_post_retention_days: i32,
    cleanup_preview_posts_enabled: bool,
    cleanup_preview_post_retention_hours: i32,
    cleanup_deleted_remote_communities_enabled: bool,
    cleanup_unfollowed_remote_communities_enabled: bool,
    cleanup_remote_interactions_enabled: bool,
    cleanup_notifications_enabled: bool,
    cleanup_notification_retention_days: i32,
    cleanup_failed_inbox_task_payloads_enabled: bool,
    cleanup_failed_inbox_task_payload_retention_days: i32,
    cleanup_completed_task_retention_days: i32,
    cleanup_failed_task_retention_days: i32,
    cleanup_failed_inbox_task_payload_compaction_hours: i32,
}

impl CleanupSettings {
    fn remote_post_cleanup_enabled(&self) -> bool {
        self.cleanup_remote_posts_enabled || self.cleanup_preview_posts_enabled
    }

    fn remote_community_cleanup_enabled(&self) -> bool {
        self.cleanup_deleted_remote_communities_enabled
            || self.cleanup_unfollowed_remote_communities_enabled
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct JanitorReport {
    repaired_community_endpoints: u64,
    requeued_community_follows: u64,
    requeued_community_follow_undos: u64,
    requeued_collection_target_follow_undos: u64,
    requeued_user_follow_undos: u64,
    completed_local_follow_undos: u64,
    recalculated_post_likes: u64,
    deactivated_discoveries: u64,
    reactivated_discoveries: u64,
    compacted_completed_inbox_tasks: u64,
    failed_terminal_inbox_tasks: u64,
    completed_irrelevant_inbox_tasks: u64,
    repaired_post_titles: u64,
}

impl JanitorReport {
    fn total_changes(&self) -> u64 {
        self.repaired_community_endpoints
            + self.requeued_community_follows
            + self.requeued_community_follow_undos
            + self.requeued_collection_target_follow_undos
            + self.requeued_user_follow_undos
            + self.completed_local_follow_undos
            + self.recalculated_post_likes
            + self.deactivated_discoveries
            + self.reactivated_discoveries
            + self.compacted_completed_inbox_tasks
            + self.failed_terminal_inbox_tasks
            + self.completed_irrelevant_inbox_tasks
            + self.repaired_post_titles
    }
}

fn task_timeout(kind: &str) -> std::time::Duration {
    /*
        Directory discovery has to talk to slow public servers and sometimes
        probe a page of actors before it can reject a host safely. Keep normal
        federation tasks on the shorter timeout, but give discovery a bounded
        window large enough to finish useful work.
    */
    match kind {
        "discover_server_communities"
        | "seed_community_discovery_hosts"
        | "seed_discourse_discovery_hosts" => TASK_DISCOVERY_TIMEOUT,
        _ => TASK_DEFAULT_TIMEOUT,
    }
}

fn task_error_is_terminal(kind: &str, err: &str) -> bool {
    let inbound_inbox = matches!(
        kind,
        "ingest_object_from_inbox" | "verify_and_ingest_object_from_inbox"
    );
    let outbound_delivery = kind == "deliver_to_inbox";

    if !inbound_inbox && !outbound_delivery {
        return false;
    }

    /*
        These failures describe a response shape that will not become valid by
        retrying the same stored inbox request. Timeouts and transport errors
        are intentionally excluded so temporarily unhealthy remote servers still
        get the normal retry budget.
    */
    let err = err.to_ascii_lowercase().replace("\\\"", "\"");

    if outbound_delivery {
        /*
            Outbound delivery has already received a response from the remote
            inbox at this point. HTML means we hit a browser page, login page,
            challenge page, or broken endpoint instead of an ActivityPub inbox.
            Retrying the same signed activity just creates queue churn.

            Some threadiverse servers also return a clear "Domain ... is
            blocked" JSON error from the inbox. That is an authorization
            decision, not a transient transport failure, so let the normal
            visibility-suppression path record it and stop retrying the same
            activity.
        */
        if err.contains("domain ") && err.contains(" is blocked") {
            return true;
        }

        return [
            "just a moment",
            "cloudflare challenge remained",
            "<!doctype html",
            "<html",
            "domain_blocked",
        ]
        .iter()
        .any(|needle| err.contains(needle));
    }

    [
        "just a moment",
        "<!doctype html",
        "<html",
        "\"error\":\"gone\"",
        "\"error\":\"couldnt_find_activity\"",
        "\"error\":\"not found\"",
        "no such like",
        "tombstone",
        "status: 404",
        "status: 410",
        "unknown content type found for activity",
        "invalid or unsupported data",
        "signature check failed",
        "request not signed",
        "http body exceeded",
        "entity too large",
        "not a person",
        "not a group",
        "data did not match any variant of untagged enum knownobject",
        "data did not match any variant of untagged enum either",
        "notcontained",
        "status: 400",
        "status: 403",
    ]
    .iter()
    .any(|needle| err.contains(needle))
}

pub async fn run_worker(
    ctx: Arc<crate::BaseContext>,
    recv: tokio::sync::mpsc::Receiver<()>,
) -> Result<(), crate::Error> {
    futures::try_join!(
        run_task_runner(ctx.clone(), recv),
        run_discovery_task_runner(ctx.clone()),
        run_readback_task_runner(ctx.clone()),
        run_inbox_task_runner(ctx.clone()),
        run_schedule(ctx),
    )?;

    Ok(())
}

async fn perform_next_queued_task(
    ctx: Arc<crate::BaseContext>,
    db: &tokio_postgres::Client,
    take_sql: &str,
) -> Result<bool, crate::Error> {
    let row = db.query_opt(take_sql, &[]).await?;

    let Some(row) = row else {
        return Ok(false);
    };

    let task_id: i64 = row.get(0);
    let kind: &str = row.get(1);
    let params: serde_json::Value = row.get(2);

    let result = tokio::time::timeout(task_timeout(kind), perform_task(ctx, kind, params)).await;
    let result = match result {
        Err(_) => Err(crate::Error::InternalStrStatic("Timeout")),
        Ok(res) => res,
    };

    if let Err(err) = result {
        let err = truncate_task_error(format!("{err:?}"));
        let terminal_error = task_error_is_terminal(kind, &err);
        let settings = load_cleanup_settings(db).await?;
        db.execute(
            FAIL_TASK_SQL,
            &[
                &task_id,
                &err,
                &<crate::tasks::IngestObjectFromInbox as crate::tasks::TaskDef>::KIND,
                &<crate::tasks::VerifyAndIngestObjectFromInbox as crate::tasks::TaskDef>::KIND,
                &settings.cleanup_failed_inbox_task_payloads_enabled,
                &terminal_error,
            ],
        )
        .await?;
    } else {
        db.execute("UPDATE task SET state='completed', completed_at=current_timestamp, attempts = attempts + 1 WHERE id=$1", &[&task_id]).await?;
    }

    Ok(true)
}

async fn run_task_runner(
    ctx: Arc<crate::BaseContext>,
    mut recv: tokio::sync::mpsc::Receiver<()>,
) -> Result<(), crate::Error> {
    let db = ctx.db_pool.get().await?;

    // TODO allow disabling this so multiple workers can run
    db.execute(RESET_INTERRUPTED_TASKS_SQL, &[]).await?;

    // TODO consider running tasks in parallel
    loop {
        if !perform_next_queued_task(ctx.clone(), &db, TAKE_NEXT_TASK_SQL).await? {
            match tokio::time::timeout(std::time::Duration::from_mins(1), recv.recv()).await {
                Err(tokio::time::error::Elapsed { .. }) => {}
                Ok(recv_res) => recv_res.ok_or(crate::Error::InternalStrStatic(
                    "Worker trigger senders lost",
                ))?,
            }
        }
    }
}

async fn run_discovery_task_runner(ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
    let db = ctx.db_pool.get().await?;

    loop {
        if !perform_next_queued_task(ctx.clone(), &db, TAKE_NEXT_DISCOVERY_TASK_SQL).await? {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;
        }
    }
}

async fn run_readback_task_runner(ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
    let db = ctx.db_pool.get().await?;

    loop {
        if !perform_next_queued_task(ctx.clone(), &db, TAKE_NEXT_READBACK_TASK_SQL).await? {
            tokio::time::sleep(std::time::Duration::from_secs(15)).await;
        }
    }
}

async fn run_inbox_task_runner(ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
    let db = ctx.db_pool.get().await?;

    loop {
        if !perform_next_queued_task(ctx.clone(), &db, TAKE_NEXT_INBOX_TASK_SQL).await? {
            tokio::time::sleep(std::time::Duration::from_secs(15)).await;
        }
    }
}

async fn run_schedule(ctx: Arc<crate::BaseContext>) -> Result<(), crate::Error> {
    let scheduler = tokio_cron_scheduler::JobScheduler::new().await?;

    let initial_outbox_fetches =
        enqueue_followed_remote_community_outbox_fetches(ctx.clone()).await?;
    if initial_outbox_fetches > 0 {
        log::debug!("Enqueued {initial_outbox_fetches} followed remote community outbox fetches");
    }

    let initial_discovery_host_seed = enqueue_community_discovery_host_seed(ctx.clone()).await?;
    if initial_discovery_host_seed > 0 {
        log::debug!("Enqueued {initial_discovery_host_seed} community discovery host seed task");
    }

    let initial_discourse_discovery_host_seed =
        enqueue_discourse_discovery_host_seed(ctx.clone()).await?;
    if initial_discourse_discovery_host_seed > 0 {
        log::debug!(
            "Enqueued {initial_discourse_discovery_host_seed} Discourse Discover host seed task"
        );
    }

    let initial_community_discoveries = enqueue_due_community_discovery(ctx.clone()).await?;
    if initial_community_discoveries > 0 {
        log::debug!("Enqueued {initial_community_discoveries} community discovery tasks");
    }

    let initial_community_interaction_probes =
        enqueue_due_community_interaction_probes(ctx.clone()).await?;
    if initial_community_interaction_probes > 0 {
        log::debug!(
            "Enqueued {initial_community_interaction_probes} community host interaction probe tasks"
        );
    }

    let initial_janitor_report = run_janitor(ctx.clone()).await?;
    if initial_janitor_report.total_changes() > 0 {
        log::debug!("Janitor repaired database drift: {initial_janitor_report:?}");
    }

    /*
        The hourly cleanup keeps preview imports and task rows short-lived, but
        a restarted instance may miss one or more cleanup windows. Run the same
        bounded cleanup once at scheduler start so normal restarts do not leave
        stale remote previews around until the next cron tick.
    */
    let db = ctx.db_pool.get().await?;
    let settings = load_cleanup_settings(&db).await?;
    let initial_remote_cleanup = cleanup_old_remote_posts(&db, &settings).await?;
    if initial_remote_cleanup > 0 {
        log::debug!("Cleaned up {initial_remote_cleanup} old or unfollowed remote posts");
    }
    let initial_task_cleanup = cleanup_old_tasks(&db, &settings).await?;
    if initial_task_cleanup > 0 {
        log::debug!("Cleaned up or compacted {initial_task_cleanup} old task rows");
    }
    std::mem::drop(db);

    scheduler
        .add(tokio_cron_scheduler::Job::new_async("17 7 * * * *", {
            let ctx = ctx.clone();
            move |_, _| {
                let ctx = ctx.clone();
                Box::pin(
                    async move {
                        let res =
                            enqueue_followed_remote_community_outbox_fetches(ctx.clone()).await?;

                        if res > 0 {
                            log::debug!(
                                "Enqueued {res} followed remote community outbox fetches"
                            );
                        }

                        Result::<_, crate::Error>::Ok(())
                    }
                    .map(|res| {
                        if let Err(err) = res {
                            log::error!(
                                "Failed to enqueue followed remote community outbox fetches: {err:?}"
                            );
                        }
                    }),
                )
            }
        })?)
        .await?;

    scheduler
        .add(tokio_cron_scheduler::Job::new_async("23 5 2 * * *", {
            let ctx = ctx.clone();
            move |_, _| {
                let ctx = ctx.clone();
                Box::pin(
                    async move {
                        let res = enqueue_community_discovery_host_seed(ctx.clone()).await?;

                        if res > 0 {
                            log::debug!("Enqueued {res} community discovery host seed task");
                        }

                        Result::<_, crate::Error>::Ok(())
                    }
                    .map(|res| {
                        if let Err(err) = res {
                            log::error!(
                                "Failed to enqueue community discovery host seed task: {err:?}"
                            );
                        }
                    }),
                )
            }
        })?)
        .await?;

    scheduler
        .add(tokio_cron_scheduler::Job::new_async("19 41 4 * * 1", {
            let ctx = ctx.clone();
            move |_, _| {
                let ctx = ctx.clone();
                Box::pin(
                    async move {
                        let res = enqueue_discourse_discovery_host_seed(ctx.clone()).await?;

                        if res > 0 {
                            log::debug!("Enqueued {res} Discourse Discover host seed task");
                        }

                        Result::<_, crate::Error>::Ok(())
                    }
                    .map(|res| {
                        if let Err(err) = res {
                            log::error!(
                                "Failed to enqueue Discourse Discover host seed task: {err:?}"
                            );
                        }
                    }),
                )
            }
        })?)
        .await?;

    scheduler
        .add(tokio_cron_scheduler::Job::new_async("41 11 */3 * * *", {
            let ctx = ctx.clone();
            move |_, _| {
                let ctx = ctx.clone();
                Box::pin(
                    async move {
                        let res = enqueue_due_community_discovery(ctx.clone()).await?;

                        if res > 0 {
                            log::debug!("Enqueued {res} community discovery tasks");
                        }

                        Result::<_, crate::Error>::Ok(())
                    }
                    .map(|res| {
                        if let Err(err) = res {
                            log::error!("Failed to enqueue community discovery tasks: {err:?}");
                        }
                    }),
                )
            }
        })?)
        .await?;

    scheduler
        .add(tokio_cron_scheduler::Job::new_async("13 17 */6 * * *", {
            let ctx = ctx.clone();
            move |_, _| {
                let ctx = ctx.clone();
                Box::pin(
                    async move {
                        let res = enqueue_due_community_interaction_probes(ctx.clone()).await?;

                        if res > 0 {
                            log::debug!("Enqueued {res} community host interaction probe tasks");
                        }

                        Result::<_, crate::Error>::Ok(())
                    }
                    .map(|res| {
                        if let Err(err) = res {
                            log::error!(
                                "Failed to enqueue community host interaction probe tasks: {err:?}"
                            );
                        }
                    }),
                )
            }
        })?)
        .await?;

    scheduler
        .add(tokio_cron_scheduler::Job::new_async(
            PREVIEW_CACHE_CLEANUP_CRON,
            {
                let ctx = ctx.clone();
                move |_, _| {
                    let ctx = ctx.clone();
                    Box::pin(
                        async move {
                            let db = ctx.db_pool.get().await?;

                            let settings = load_cleanup_settings(&db).await?;
                            let res = cleanup_old_remote_posts(&db, &settings).await?;

                            if res > 0 {
                                log::debug!("Cleaned up {res} old or unfollowed remote posts");
                            }

                            let res = cleanup_old_tasks(&db, &settings).await?;

                            if res > 0 {
                                log::debug!("Cleaned up or compacted {res} old task rows");
                            }

                            Result::<_, crate::Error>::Ok(())
                        }
                        .map(|res| {
                            if let Err(err) = res {
                                log::error!(
                                    "Failed to clean up short-lived remote preview cache: {err:?}"
                                );
                            }
                        }),
                    )
                }
            },
        )?)
        .await?;

    scheduler
        .add(tokio_cron_scheduler::Job::new_async("22 23 22 * * *", {
            let ctx = ctx.clone();
            move |_, _| {
                let ctx = ctx.clone();
                Box::pin(
                    async move {
                        let db = ctx.db_pool.get().await?;

                        let settings = load_cleanup_settings(&db).await?;

                        let res = cleanup_old_tasks(&db, &settings).await?;

                        if res > 0 {
                            log::debug!("Cleaned up or compacted {res} old task rows");
                        }

                        let res = cleanup_old_remote_posts(&db, &settings).await?;

                        if res > 0 {
                            log::debug!("Cleaned up {res} old remote posts");
                        }

                        let res = cleanup_remote_communities(&db, &settings).await?;

                        if res > 0 {
                            log::debug!("Cleaned up {res} deleted remote communities");
                        }

                        let res = cleanup_old_remote_interactions(&db, &settings).await?;

                        if res > 0 {
                            log::debug!("Cleaned up {res} old remote interaction rows");
                        }

                        let res = cleanup_old_notifications(&db, &settings).await?;

                        if res > 0 {
                            log::debug!("Cleaned up {res} old notification rows");
                        }

                        Result::<_, crate::Error>::Ok(())
                    }
                    .map(|res| {
                        if let Err(err) = res {
                            log::error!("Failed to clean up old tasks: {err:?}");
                        }
                    }),
                )
            }
        })?)
        .await?;

    scheduler
        .add(tokio_cron_scheduler::Job::new_async(JANITOR_CRON, {
            let ctx = ctx.clone();
            move |_, _| {
                let ctx = ctx.clone();
                Box::pin(
                    async move {
                        let report = run_janitor(ctx.clone()).await?;

                        if report.total_changes() > 0 {
                            log::debug!("Janitor repaired database drift: {report:?}");
                        }

                        Result::<_, crate::Error>::Ok(())
                    }
                    .map(|res| {
                        if let Err(err) = res {
                            log::error!("Failed to run database janitor: {err:?}");
                        }
                    }),
                )
            }
        })?)
        .await?;

    scheduler
        .add(tokio_cron_scheduler::Job::new_async(PG_REPACK_CRON, {
            let ctx = ctx.clone();
            move |_, _| {
                let ctx = ctx.clone();
                Box::pin(
                    async move {
                        let repacked = run_pg_repack_janitor(ctx.clone()).await?;

                        if repacked > 0 {
                            log::debug!("Repacked {repacked} bloated database tables");
                        }

                        Result::<_, crate::Error>::Ok(())
                    }
                    .map(|res| {
                        if let Err(err) = res {
                            log::error!("Failed to run pg_repack janitor: {err:?}");
                        }
                    }),
                )
            }
        })?)
        .await?;

    Ok(scheduler.start().await?)
}

async fn enqueue_followed_remote_community_outbox_fetches(
    ctx: Arc<crate::BaseContext>,
) -> Result<u64, crate::Error> {
    let db = ctx.db_pool.get().await?;
    let kind = <crate::tasks::FetchCommunityOutbox as crate::tasks::TaskDef>::KIND;
    let max_attempts = <crate::tasks::FetchCommunityOutbox as crate::tasks::TaskDef>::MAX_ATTEMPTS;
    let inserted = db
        .execute(
            ENQUEUE_FOLLOWED_REMOTE_COMMUNITY_OUTBOX_FETCHES_SQL,
            &[&kind, &max_attempts],
        )
        .await?;

    if inserted > 0 {
        ctx.notify_worker(&db).await?;
    }

    Ok(inserted)
}

async fn enqueue_due_community_discovery(
    ctx: Arc<crate::BaseContext>,
) -> Result<u64, crate::Error> {
    let db = ctx.db_pool.get().await?;
    let kind = <crate::tasks::DiscoverServerCommunities as crate::tasks::TaskDef>::KIND;
    let max_attempts =
        <crate::tasks::DiscoverServerCommunities as crate::tasks::TaskDef>::MAX_ATTEMPTS;

    db.execute(UPSERT_KNOWN_COMMUNITY_DISCOVERY_SERVERS_SQL, &[])
        .await?;

    let (enqueue_limit, refresh_interval_hours) =
        db.query_opt(DISCOVERY_SETTINGS_SQL, &[]).await?.map_or(
            (
                DEFAULT_COMMUNITY_DISCOVERY_ENQUEUE_LIMIT,
                DEFAULT_COMMUNITY_DISCOVERY_REFRESH_INTERVAL_HOURS,
            ),
            |row| (i64::from(row.get::<_, i32>(0)), row.get::<_, i32>(1)),
        );

    let inserted = db
        .execute(
            ENQUEUE_DUE_COMMUNITY_DISCOVERY_SQL,
            &[
                &kind,
                &max_attempts,
                &enqueue_limit,
                &refresh_interval_hours,
            ],
        )
        .await?;

    if inserted > 0 {
        ctx.notify_worker(&db).await?;
    }

    Ok(inserted)
}

async fn enqueue_community_discovery_host_seed(
    ctx: Arc<crate::BaseContext>,
) -> Result<u64, crate::Error> {
    let db = ctx.db_pool.get().await?;
    let kind = <crate::tasks::SeedCommunityDiscoveryHosts as crate::tasks::TaskDef>::KIND;
    let max_attempts =
        <crate::tasks::SeedCommunityDiscoveryHosts as crate::tasks::TaskDef>::MAX_ATTEMPTS;
    let inserted = db
        .execute(
            ENQUEUE_COMMUNITY_DISCOVERY_HOST_SEED_SQL,
            &[&kind, &max_attempts],
        )
        .await?;

    if inserted > 0 {
        ctx.notify_worker(&db).await?;
    }

    Ok(inserted)
}

async fn enqueue_discourse_discovery_host_seed(
    ctx: Arc<crate::BaseContext>,
) -> Result<u64, crate::Error> {
    let db = ctx.db_pool.get().await?;
    let kind = <crate::tasks::SeedDiscourseDiscoveryHosts as crate::tasks::TaskDef>::KIND;
    let max_attempts =
        <crate::tasks::SeedDiscourseDiscoveryHosts as crate::tasks::TaskDef>::MAX_ATTEMPTS;
    let inserted = db
        .execute(
            ENQUEUE_DISCOURSE_DISCOVERY_HOST_SEED_SQL,
            &[&kind, &max_attempts],
        )
        .await?;

    if inserted > 0 {
        ctx.notify_worker(&db).await?;
    }

    Ok(inserted)
}

async fn enqueue_due_community_interaction_probes(
    ctx: Arc<crate::BaseContext>,
) -> Result<u64, crate::Error> {
    let db = ctx.db_pool.get().await?;
    let kind = <crate::tasks::ProbeCommunityHostInteraction as crate::tasks::TaskDef>::KIND;
    let max_attempts =
        <crate::tasks::ProbeCommunityHostInteraction as crate::tasks::TaskDef>::MAX_ATTEMPTS;

    db.execute(UPSERT_KNOWN_COMMUNITY_DISCOVERY_SERVERS_SQL, &[])
        .await?;

    let inserted = db
        .execute(
            ENQUEUE_DUE_COMMUNITY_INTERACTION_PROBES_SQL,
            &[&kind, &max_attempts],
        )
        .await?;

    if inserted > 0 {
        ctx.notify_worker(&db).await?;
    }

    Ok(inserted)
}

async fn cleanup_old_tasks(
    db: &tokio_postgres::Client,
    settings: &CleanupSettings,
) -> Result<u64, crate::Error> {
    /*
        Task rows are useful while they explain recent work, but successful
        inbox verification tasks can carry the full received object. A busy
        federated instance can therefore grow the task table quickly unless
        routine cleanup also compacts those payloads.
    */
    let mut changed = 0;

    for _ in 0..TASK_CLEANUP_MAX_BATCHES {
        let completed = db
            .execute(
                CLEANUP_COMPLETED_TASKS_SQL,
                &[
                    &TASK_CLEANUP_BATCH_SIZE,
                    &settings.cleanup_completed_task_retention_days,
                ],
            )
            .await?;

        let failed_inbox = db
            .execute(
                CLEANUP_FAILED_INBOX_TASKS_SQL,
                &[
                    &TASK_CLEANUP_BATCH_SIZE,
                    &settings.cleanup_failed_inbox_task_payload_retention_days,
                ],
            )
            .await?;

        let failed = db
            .execute(
                CLEANUP_FAILED_TASKS_SQL,
                &[
                    &TASK_CLEANUP_BATCH_SIZE,
                    &settings.cleanup_failed_task_retention_days,
                ],
            )
            .await?;

        let compacted_failed_inbox = if settings.cleanup_failed_inbox_task_payloads_enabled {
            db.execute(
                COMPACT_FAILED_INBOX_TASK_PAYLOADS_SQL,
                &[
                    &TASK_CLEANUP_BATCH_SIZE,
                    &settings.cleanup_failed_inbox_task_payload_compaction_hours,
                ],
            )
            .await?
        } else {
            0
        };

        let compacted_completed_inbox = db
            .execute(
                JANITOR_COMPACT_COMPLETED_INBOX_TASK_PAYLOADS_SQL,
                &[&TASK_CLEANUP_BATCH_SIZE],
            )
            .await?;

        let completed_irrelevant_inbox = db
            .execute(
                JANITOR_COMPLETE_IRRELEVANT_INBOX_TASKS_SQL,
                &[&TASK_CLEANUP_BATCH_SIZE],
            )
            .await?;

        let batch_changed = completed
            + failed_inbox
            + failed
            + compacted_failed_inbox
            + compacted_completed_inbox
            + completed_irrelevant_inbox;

        if batch_changed == 0 {
            break;
        }

        changed += batch_changed;
    }

    Ok(changed)
}

async fn load_cleanup_settings(
    db: &tokio_postgres::Client,
) -> Result<CleanupSettings, crate::Error> {
    let row = db.query_one(CLEANUP_SETTINGS_SQL, &[]).await?;

    Ok(CleanupSettings {
        cleanup_remote_posts_enabled: row.get(0),
        cleanup_remote_post_retention_days: row.get(1),
        cleanup_preview_posts_enabled: row.get(2),
        cleanup_preview_post_retention_hours: row.get(3),
        cleanup_deleted_remote_communities_enabled: row.get(4),
        cleanup_unfollowed_remote_communities_enabled: row.get(5),
        cleanup_remote_interactions_enabled: row.get(6),
        cleanup_notifications_enabled: row.get(7),
        cleanup_notification_retention_days: row.get(8),
        cleanup_failed_inbox_task_payloads_enabled: row.get(9),
        cleanup_failed_inbox_task_payload_retention_days: row.get(10),
        cleanup_completed_task_retention_days: row.get(11),
        cleanup_failed_task_retention_days: row.get(12),
        cleanup_failed_inbox_task_payload_compaction_hours: row.get(13),
    })
}

async fn run_janitor(ctx: Arc<crate::BaseContext>) -> Result<JanitorReport, crate::Error> {
    let db = ctx.db_pool.get().await?;

    let repaired_community_endpoints =
        run_janitor_batches(&db, JANITOR_REPAIR_CONVENTIONAL_COMMUNITY_ENDPOINTS_SQL).await?;
    let recalculated_post_likes =
        run_janitor_batches(&db, JANITOR_RECALCULATE_POST_LIKES_SQL).await?;
    let deactivated_discoveries =
        run_janitor_batches(&db, JANITOR_DEACTIVATE_STALE_COMMUNITY_DISCOVERY_SQL).await?;
    let reactivated_discoveries =
        run_janitor_batches(&db, JANITOR_REACTIVATE_CURRENT_COMMUNITY_DISCOVERY_SQL).await?;
    let compacted_completed_inbox_tasks =
        run_janitor_batches(&db, JANITOR_COMPACT_COMPLETED_INBOX_TASK_PAYLOADS_SQL).await?;
    let failed_terminal_inbox_tasks =
        run_janitor_batches(&db, JANITOR_FAIL_TERMINAL_INBOX_TASKS_SQL).await?;
    let completed_irrelevant_inbox_tasks =
        run_janitor_batches(&db, JANITOR_COMPLETE_IRRELEVANT_INBOX_TASKS_SQL).await?;
    let repaired_post_titles =
        run_janitor_batches(&db, JANITOR_REPAIR_BLANK_POST_TITLES_SQL).await?;
    let completed_local_follow_undos =
        run_janitor_batches(&db, JANITOR_COMPLETE_LOCAL_COMMUNITY_FOLLOW_UNDOS_SQL).await?
            + run_janitor_batches(&db, JANITOR_COMPLETE_LOCAL_USER_FOLLOW_UNDOS_SQL).await?;
    let pending_follows = db
        .query(
            JANITOR_PENDING_COMMUNITY_FOLLOWS_SQL,
            &[&JANITOR_FOLLOW_REPAIR_LIMIT],
        )
        .await?;
    let pending_community_follow_undos = db
        .query(
            JANITOR_PENDING_COMMUNITY_FOLLOW_UNDOS_SQL,
            &[&JANITOR_FOLLOW_REPAIR_LIMIT],
        )
        .await?;
    let pending_collection_target_follow_undos = db
        .query(
            JANITOR_PENDING_COLLECTION_TARGET_FOLLOW_UNDOS_SQL,
            &[&JANITOR_FOLLOW_REPAIR_LIMIT],
        )
        .await?;
    let pending_user_follow_undos = db
        .query(
            JANITOR_PENDING_USER_FOLLOW_UNDOS_SQL,
            &[&JANITOR_FOLLOW_REPAIR_LIMIT],
        )
        .await?;

    std::mem::drop(db);

    for row in &pending_follows {
        crate::apub_util::spawn_enqueue_send_community_follow(
            CommunityLocalID(row.get(0)),
            UserLocalID(row.get(1)),
            ctx.clone(),
        );
    }

    for row in &pending_community_follow_undos {
        crate::apub_util::spawn_enqueue_send_community_follow_undo(
            row.get(0),
            CommunityLocalID(row.get(1)),
            UserLocalID(row.get(2)),
            ctx.clone(),
        );
    }

    for row in &pending_collection_target_follow_undos {
        crate::apub_util::spawn_enqueue_send_collection_target_follow_undo(
            row.get(0),
            CollectionTargetLocalID(row.get(1)),
            UserLocalID(row.get(2)),
            ctx.clone(),
        );
    }

    for row in &pending_user_follow_undos {
        crate::apub_util::spawn_enqueue_send_user_follow_undo(
            UserLocalID(row.get(1)),
            UserLocalID(row.get(2)),
            row.get(0),
            ctx.clone(),
        );
    }

    Ok(JanitorReport {
        repaired_community_endpoints,
        requeued_community_follows: pending_follows.len() as u64,
        requeued_community_follow_undos: pending_community_follow_undos.len() as u64,
        requeued_collection_target_follow_undos: pending_collection_target_follow_undos.len()
            as u64,
        requeued_user_follow_undos: pending_user_follow_undos.len() as u64,
        completed_local_follow_undos,
        recalculated_post_likes,
        deactivated_discoveries,
        reactivated_discoveries,
        compacted_completed_inbox_tasks,
        failed_terminal_inbox_tasks,
        completed_irrelevant_inbox_tasks,
        repaired_post_titles,
    })
}

async fn run_janitor_batches(db: &tokio_postgres::Client, sql: &str) -> Result<u64, crate::Error> {
    let mut changed = 0;

    for _ in 0..JANITOR_MAX_BATCHES {
        let batch_changed = db.execute(sql, &[&JANITOR_BATCH_SIZE]).await?;

        if batch_changed == 0 {
            break;
        }

        changed += batch_changed;
    }

    Ok(changed)
}

#[derive(Debug, PartialEq, Eq)]
struct PgRepackConnection {
    dbname: String,
    host: Option<String>,
    port: Option<u16>,
    username: Option<String>,
    password: Option<String>,
}

async fn run_pg_repack_janitor(ctx: Arc<crate::BaseContext>) -> Result<u64, crate::Error> {
    if !pg_repack_janitor_enabled() {
        return Ok(0);
    }

    let connection = pg_repack_connection_from_env()?;
    let db = ctx.db_pool.get().await?;
    let candidates = db
        .query(
            PG_REPACK_CANDIDATE_TABLES_SQL,
            &[
                &PG_REPACK_MIN_RELATION_BYTES,
                &PG_REPACK_MIN_DEAD_TUPLES,
                &PG_REPACK_MAX_TABLES,
            ],
        )
        .await?;

    std::mem::drop(db);

    let mut repacked = 0;

    for candidate in candidates {
        let schema: String = candidate.get(0);
        let table: String = candidate.get(1);

        if !pg_repack_identifier_is_simple(&schema) || !pg_repack_identifier_is_simple(&table) {
            log::warn!("Skipping pg_repack candidate with quoted identifier requirements");
            continue;
        }

        run_pg_repack_for_table(&connection, &schema, &table).await?;
        repacked += 1;
    }

    Ok(repacked)
}

fn pg_repack_janitor_enabled() -> bool {
    std::env::var("LOTIDE_PG_REPACK_JANITOR").is_ok_and(|value| env_flag_enabled(&value))
}

fn env_flag_enabled(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn pg_repack_connection_from_env() -> Result<PgRepackConnection, crate::Error> {
    let database_url = std::env::var("LOTIDE_PG_REPACK_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .map_err(|_| {
            crate::Error::InternalStrStatic(
                "LOTIDE_PG_REPACK_JANITOR requires DATABASE_URL or LOTIDE_PG_REPACK_DATABASE_URL",
            )
        })?;

    pg_repack_connection_from_database_url(&database_url)
}

fn pg_repack_connection_from_database_url(
    database_url: &str,
) -> Result<PgRepackConnection, crate::Error> {
    if !database_url.starts_with("postgres://") && !database_url.starts_with("postgresql://") {
        if database_url.trim().is_empty() {
            return Err(crate::Error::InternalStrStatic(
                "Empty pg_repack database name",
            ));
        }

        return Ok(PgRepackConnection {
            dbname: database_url.to_owned(),
            host: None,
            port: None,
            username: None,
            password: None,
        });
    }

    let parsed = url::Url::parse(database_url).map_err(crate::Error::from)?;
    let dbname = parsed.path().trim_start_matches('/').to_owned();

    if dbname.is_empty() {
        return Err(crate::Error::InternalStrStatic(
            "Missing pg_repack database name",
        ));
    }

    let username = if parsed.username().is_empty() {
        None
    } else {
        Some(parsed.username().to_owned())
    };

    Ok(PgRepackConnection {
        dbname,
        host: parsed.host_str().map(str::to_owned),
        port: parsed.port(),
        username,
        password: parsed.password().map(str::to_owned),
    })
}

fn pg_repack_identifier_is_simple(identifier: &str) -> bool {
    let mut chars = identifier.chars();

    let Some(first) = chars.next() else {
        return false;
    };

    if identifier.len() > 63 || (!first.is_ascii_alphabetic() && first != '_') {
        return false;
    }

    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn default_pg_repack_bin() -> String {
    /*
        Debian packages install versioned PostgreSQL tooling under
        /usr/lib/postgresql/<major>/bin. MSYS2, Cygwin, and most manual installs
        put helper programs on PATH instead. The janitor is opt-in, so the
        default should favor a useful command name on non-Debian platforms
        while keeping the known Debian path for existing servers.
    */
    if cfg!(target_os = "linux") {
        "/usr/lib/postgresql/18/bin/pg_repack".to_owned()
    } else if cfg!(target_os = "windows") {
        "pg_repack.exe".to_owned()
    } else {
        "pg_repack".to_owned()
    }
}

async fn run_pg_repack_for_table(
    connection: &PgRepackConnection,
    schema: &str,
    table: &str,
) -> Result<(), crate::Error> {
    /*
        pg_repack builds a replacement table and swaps it into place. That keeps
        normal reads and writes moving, but it still needs a primary key and
        temporary disk space. The caller already filters candidates; this layer
        keeps the external process bounded and noninteractive.
    */
    let bin = std::env::var("LOTIDE_PG_REPACK_BIN").unwrap_or_else(|_| default_pg_repack_bin());
    let table_arg = format!("{schema}.{table}");
    let mut command = tokio::process::Command::new(bin);

    command
        .arg("--dbname")
        .arg(&connection.dbname)
        .arg("--table")
        .arg(&table_arg)
        .arg("--no-superuser-check")
        .arg("--no-kill-backend")
        .arg("--wait-timeout")
        .arg("30")
        .arg("--jobs")
        .arg("1")
        .arg("--no-password")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if let Some(host) = &connection.host {
        command.arg("--host").arg(host);
    }

    if let Some(port) = connection.port {
        command.arg("--port").arg(port.to_string());
    }

    if let Some(username) = &connection.username {
        command.arg("--username").arg(username);
    }

    if let Some(password) = &connection.password {
        command.env("PGPASSWORD", password);
    }

    let output = tokio::time::timeout(PG_REPACK_TIMEOUT, command.output())
        .await
        .map_err(|_| crate::Error::InternalStrStatic("pg_repack timed out"))?
        .map_err(crate::Error::from)?;

    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    Err(crate::Error::InternalStr(format!(
        "pg_repack failed for {}: {}",
        table_arg,
        truncate_task_error(stderr)
    )))
}

fn truncate_task_error(mut err: String) -> String {
    if let Some((cutoff, _)) = err.char_indices().nth(TASK_ERROR_MAX_CHARS) {
        err.truncate(cutoff);
        err.push_str("\n[truncated]");
    }

    err
}

async fn cleanup_old_remote_posts(
    db: &tokio_postgres::Client,
    settings: &CleanupSettings,
) -> Result<u64, crate::Error> {
    if !settings.remote_post_cleanup_enabled() {
        return Ok(0);
    }

    let mut removed = 0;

    for _ in 0..REMOTE_POST_CLEANUP_MAX_BATCHES {
        let batch_removed = db
            .query_one(
                CLEANUP_OLD_REMOTE_POSTS_SQL,
                &[
                    &REMOTE_POST_CLEANUP_BATCH_SIZE,
                    &settings.cleanup_remote_posts_enabled,
                    &settings.cleanup_remote_post_retention_days,
                    &settings.cleanup_preview_posts_enabled,
                    &settings.cleanup_preview_post_retention_hours,
                ],
            )
            .await?
            .get::<_, i64>(0);

        if batch_removed == 0 {
            break;
        }

        removed += crate::i64_to_u64_saturating(batch_removed);
    }

    Ok(removed)
}

async fn cleanup_remote_communities(
    db: &tokio_postgres::Client,
    settings: &CleanupSettings,
) -> Result<u64, crate::Error> {
    if !settings.remote_community_cleanup_enabled() {
        return Ok(0);
    }

    let mut removed = 0;

    for _ in 0..REMOTE_COMMUNITY_CLEANUP_MAX_BATCHES {
        let batch_removed = db
            .query_one(
                CLEANUP_REMOTE_COMMUNITIES_SQL,
                &[
                    &REMOTE_COMMUNITY_CLEANUP_BATCH_SIZE,
                    &settings.cleanup_deleted_remote_communities_enabled,
                    &settings.cleanup_unfollowed_remote_communities_enabled,
                ],
            )
            .await?
            .get::<_, i64>(0);

        if batch_removed == 0 {
            break;
        }

        removed += crate::i64_to_u64_saturating(batch_removed);
    }

    Ok(removed)
}

async fn cleanup_old_remote_interactions(
    db: &tokio_postgres::Client,
    settings: &CleanupSettings,
) -> Result<u64, crate::Error> {
    if !settings.cleanup_remote_interactions_enabled {
        return Ok(0);
    }

    let mut removed = 0;

    for _ in 0..REMOTE_INTERACTION_CLEANUP_MAX_BATCHES {
        let post_likes = db
            .query_one(
                CLEANUP_OLD_REMOTE_POST_LIKES_SQL,
                &[
                    &REMOTE_INTERACTION_CLEANUP_BATCH_SIZE,
                    &settings.cleanup_remote_post_retention_days,
                ],
            )
            .await?
            .get::<_, i64>(0);

        let reply_likes = db
            .query_one(
                CLEANUP_OLD_REMOTE_REPLY_LIKES_SQL,
                &[
                    &REMOTE_INTERACTION_CLEANUP_BATCH_SIZE,
                    &settings.cleanup_remote_post_retention_days,
                ],
            )
            .await?
            .get::<_, i64>(0);

        let batch_removed =
            crate::i64_to_u64_saturating(post_likes) + crate::i64_to_u64_saturating(reply_likes);

        if batch_removed == 0 {
            break;
        }

        removed += batch_removed;
    }

    Ok(removed)
}

async fn cleanup_old_notifications(
    db: &tokio_postgres::Client,
    settings: &CleanupSettings,
) -> Result<u64, crate::Error> {
    if !settings.cleanup_notifications_enabled {
        return Ok(0);
    }

    let mut removed = 0;

    for _ in 0..TASK_CLEANUP_MAX_BATCHES {
        let batch_removed = db
            .execute(
                CLEANUP_OLD_NOTIFICATIONS_SQL,
                &[
                    &TASK_CLEANUP_BATCH_SIZE,
                    &settings.cleanup_notification_retention_days,
                ],
            )
            .await?;

        if batch_removed == 0 {
            break;
        }

        removed += batch_removed;
    }

    Ok(removed)
}

async fn perform_task(
    ctx: Arc<crate::BaseContext>,
    kind: &str,
    params: serde_json::Value,
) -> Result<(), crate::Error> {
    use crate::tasks::TaskDef;

    match kind {
        crate::tasks::DeliverToInbox::KIND => {
            let def: crate::tasks::DeliverToInbox = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        #[allow(deprecated)]
        crate::tasks::DeliverToFollowers::KIND => {
            let def: crate::tasks::DeliverToFollowers = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::DeliverToAudience::KIND => {
            let def: crate::tasks::DeliverToAudience = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::FetchActor::KIND => {
            let def: crate::tasks::FetchActor = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::FetchCommunityFeatured::KIND => {
            let def: crate::tasks::FetchCommunityFeatured = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::FetchCollectionTargetPreview::KIND => {
            let def: crate::tasks::FetchCollectionTargetPreview = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::FetchCommunityOutbox::KIND => {
            let def: crate::tasks::FetchCommunityOutbox = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::FetchPostReplies::KIND => {
            let def: crate::tasks::FetchPostReplies = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::FetchRemotePostRefresh::KIND => {
            let def: crate::tasks::FetchRemotePostRefresh = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::FetchPlatformPostThread::KIND => {
            let def: crate::tasks::FetchPlatformPostThread = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::SeedCommunityDiscoveryHosts::KIND => {
            let def: crate::tasks::SeedCommunityDiscoveryHosts = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::SeedDiscourseDiscoveryHosts::KIND => {
            let def: crate::tasks::SeedDiscourseDiscoveryHosts = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::DiscoverServerCommunities::KIND => {
            let def: crate::tasks::DiscoverServerCommunities = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::ProbeCommunityHostInteraction::KIND => {
            let def: crate::tasks::ProbeCommunityHostInteraction = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::SendNotification::KIND => {
            let def: crate::tasks::SendNotification = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::SendNotificationForSubscription::KIND => {
            let def: crate::tasks::SendNotificationForSubscription =
                serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::IngestObjectFromInbox::KIND => {
            let def: crate::tasks::IngestObjectFromInbox = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        crate::tasks::VerifyAndIngestObjectFromInbox::KIND => {
            let def: crate::tasks::VerifyAndIngestObjectFromInbox = serde_json::from_value(params)?;
            def.perform(ctx).await?;
        }
        _ => {
            return Err(crate::Error::InternalStr(format!(
                "Unrecognized task type: {kind}"
            )));
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn task_order_prioritizes_delivery_before_fetch_backlog() {
        let sql = super::TAKE_NEXT_TASK_SQL;

        let audience_delivery = sql.find("kind='deliver_to_audience'").unwrap();
        let fresh_inbox_delivery = sql.find("kind='deliver_to_inbox' AND attempts=0").unwrap();
        let inbox_retry = sql.find("WHEN kind='deliver_to_inbox' THEN 6").unwrap();
        let preview_fetch = sql
            .find("kind='fetch_community_outbox' AND params->>'preview'='true'")
            .unwrap();
        let followed_outbox_fetch = sql
            .find("kind='fetch_community_outbox' AND attempts=0")
            .unwrap();
        let fresh_inbox_verify = sql
            .find("kind='verify_and_ingest_object_from_inbox' AND attempts=0")
            .unwrap();
        let post_replies_fetch = sql
            .find("kind='fetch_post_replies' AND attempts=0")
            .unwrap();
        let remote_post_refresh = sql
            .find("kind='fetch_remote_post_refresh' AND attempts=0")
            .unwrap();
        let platform_thread_fetch = sql
            .find("kind='fetch_platform_post_thread' AND attempts=0")
            .unwrap();
        let discovery_seed = sql.find("'seed_community_discovery_hosts'").unwrap();
        let priority_discovery = sql.find("'fedigroups-directory'").unwrap();
        let discovery = sql
            .find("WHEN kind='discover_server_communities' AND attempts=0 THEN 11")
            .unwrap();
        let interaction_probe = sql
            .find("kind='probe_community_host_interaction' AND attempts=0")
            .unwrap();
        let aged_fetch_retry = sql.find("attempted_at < current_timestamp").unwrap();
        let fresh_task = sql.find("WHEN attempts=0 THEN 13").unwrap();

        assert!(audience_delivery < fresh_inbox_delivery);
        assert!(fresh_inbox_delivery < discovery_seed);
        assert!(discovery_seed < preview_fetch);
        assert!(preview_fetch < followed_outbox_fetch);
        assert!(followed_outbox_fetch < fresh_inbox_verify);
        assert!(followed_outbox_fetch < post_replies_fetch);
        assert!(preview_fetch < remote_post_refresh);
        assert!(preview_fetch < platform_thread_fetch);
        assert!(remote_post_refresh < followed_outbox_fetch);
        assert!(followed_outbox_fetch < inbox_retry);
        assert!(inbox_retry < fresh_inbox_verify);
        assert!(inbox_retry < post_replies_fetch);
        assert!(inbox_retry < platform_thread_fetch);
        assert!(remote_post_refresh < priority_discovery);
        assert!(followed_outbox_fetch < priority_discovery);
        assert!(post_replies_fetch < interaction_probe);
        assert!(remote_post_refresh < interaction_probe);
        assert!(platform_thread_fetch < interaction_probe);
        assert!(interaction_probe < discovery);
        assert!(discovery < aged_fetch_retry);
        assert!(aged_fetch_retry < fresh_task);
    }

    #[test]
    fn worker_gives_discovery_tasks_a_longer_timeout() {
        assert_eq!(
            super::task_timeout("discover_server_communities"),
            super::TASK_DISCOVERY_TIMEOUT
        );
        assert_eq!(
            super::task_timeout("seed_discourse_discovery_hosts"),
            super::TASK_DISCOVERY_TIMEOUT
        );
        assert_eq!(
            super::task_timeout("deliver_to_inbox"),
            super::TASK_DEFAULT_TIMEOUT
        );
    }

    #[test]
    fn terminal_inbox_errors_do_not_keep_retrying() {
        assert!(super::task_error_is_terminal(
            "verify_and_ingest_object_from_inbox",
            "InternalStr(\"Error in remote response: <!DOCTYPE html><title>Just a moment...</title>\")"
        ));
        assert!(super::task_error_is_terminal(
            "verify_and_ingest_object_from_inbox",
            "InternalStr(\"Error in remote response: {\\\"error\\\":\\\"Gone\\\"}\")"
        ));
        assert!(super::task_error_is_terminal(
            "verify_and_ingest_object_from_inbox",
            "Internal(Error(\"data did not match any variant of untagged enum KnownObject\"))"
        ));
        assert!(super::task_error_is_terminal(
            "verify_and_ingest_object_from_inbox",
            "Internal(Error(\"data did not match any variant of untagged enum Either\", line: 1, column: 66))"
        ));
        assert!(super::task_error_is_terminal(
            "verify_and_ingest_object_from_inbox",
            "Internal(NotContained)"
        ));
        assert!(super::task_error_is_terminal(
            "verify_and_ingest_object_from_inbox",
            "InternalStrStatic(\"Not a Person\")"
        ));
        assert!(super::task_error_is_terminal(
            "verify_and_ingest_object_from_inbox",
            "UserError(Response { status: 410 })"
        ));
        assert!(super::task_error_is_terminal(
            "verify_and_ingest_object_from_inbox",
            "InternalStrStatic(\"HTTP body exceeded upload limit\")"
        ));
        assert!(super::task_error_is_terminal(
            "ingest_object_from_inbox",
            "UserError(Response { status: 403, body: Body(Full(b\"Signature check failed\")) })"
        ));
        assert!(!super::task_error_is_terminal(
            "verify_and_ingest_object_from_inbox",
            "InternalStrStatic(\"Remote request timed out\")"
        ));
        assert!(super::task_error_is_terminal(
            "deliver_to_inbox",
            "InternalStr(\"Error in remote response: <!DOCTYPE html><title>ActivityPub inbox error</title>\")"
        ));
        assert!(super::task_error_is_terminal(
            "deliver_to_inbox",
            "InternalStr(\"Error in remote response: {\\\"error\\\":\\\"unknown\\\",\\\"message\\\":\\\"Domain \\\\\\\"lotide.example\\\\\\\" is blocked\\\"}\")"
        ));
        assert!(super::task_error_is_terminal(
            "deliver_to_inbox",
            "InternalStr(\"Error in remote response: {\\\"error\\\":\\\"domain_blocked\\\"}\")"
        ));
        assert!(!super::task_error_is_terminal(
            "deliver_to_inbox",
            "InternalStr(\"Error in remote response: error code: 520\")"
        ));
        assert!(!super::task_error_is_terminal(
            "deliver_to_inbox",
            "InternalStr(\"Error in remote response: {\\\"error\\\":\\\"unknown\\\",\\\"message\\\":\\\"\\\"}\")"
        ));
        assert!(!super::task_error_is_terminal(
            "fetch_platform_post_thread",
            "InternalStr(\"Error in remote response: <!DOCTYPE html>\")"
        ));
    }

    #[test]
    fn fail_task_sql_can_terminal_fail_without_payload_compaction() {
        let sql = super::FAIL_TASK_SQL;

        assert!(sql.contains("WHEN $6 THEN 'failed'::lt_task_state"));
        assert!(sql.contains("attempts=(CASE WHEN $6 THEN max_attempts"));
        assert!(sql.contains("attempts + 1 >= max_attempts"));
        assert!(!sql.contains("AND (attempts + 1 >= max_attempts OR $6)"));
    }

    #[test]
    fn janitor_fails_terminal_inbox_tasks_in_batches() {
        let sql = super::JANITOR_FAIL_TERMINAL_INBOX_TASKS_SQL;

        assert!(sql.contains("state='pending'"));
        assert!(sql.contains("'verify_and_ingest_object_from_inbox'"));
        assert!(sql.contains("latest_error IS NOT NULL"));
        assert!(sql.contains("just a moment"));
        assert!(sql.contains("status: 410"));
        assert!(sql.contains("entity too large"));
        assert!(sql.contains("unknown content type found for activity"));
        assert!(sql.contains("not a person"));
        assert!(sql.contains("not a group"));
        assert!(sql.contains("untagged enum either"));
        assert!(sql.contains("notcontained"));
        assert!(sql.contains("status: 403"));
        assert!(sql.contains("SET state='failed'::lt_task_state"));
        assert!(sql.contains("attempts=max_attempts"));
        assert!(sql.contains("LIMIT $1"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
    }

    #[test]
    fn worker_has_a_dedicated_discovery_lane() {
        let sql = super::TAKE_NEXT_DISCOVERY_TASK_SQL;

        assert!(sql.contains("'seed_community_discovery_hosts'"));
        assert!(sql.contains("'seed_discourse_discovery_hosts'"));
        assert!(sql.contains("'discover_server_communities'"));
        assert!(sql.contains("'probe_community_host_interaction'"));
        assert!(!sql.contains("'verify_and_ingest_object_from_inbox'"));
        assert!(!sql.contains("'fetch_platform_post_thread'"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED LIMIT 1"));

        let seeds = sql
            .find("seed_community_discovery_hosts")
            .expect("discovery seed priority");
        let probe = sql
            .find("kind='probe_community_host_interaction' AND attempts=0 THEN 1")
            .expect("host probe priority");
        let priority_discovery = sql
            .find("'fedigroups-directory'")
            .expect("priority platform discovery");
        let general_discovery = sql
            .find("kind='discover_server_communities' AND attempts=0 THEN 4")
            .expect("general discovery");

        assert!(seeds < probe);
        assert!(probe < priority_discovery);
        assert!(priority_discovery < general_discovery);
    }

    #[test]
    fn worker_has_a_dedicated_readback_lane() {
        let sql = super::TAKE_NEXT_READBACK_TASK_SQL;

        assert!(sql.contains("'fetch_remote_post_refresh'"));
        assert!(sql.contains("'fetch_post_replies'"));
        assert!(sql.contains("'fetch_platform_post_thread'"));
        assert!(sql.contains("'fetch_community_outbox'"));
        assert!(sql.contains("'fetch_collection_target_preview'"));
        assert!(sql.contains("'fetch_community_featured'"));
        assert!(!sql.contains("'verify_and_ingest_object_from_inbox'"));
        assert!(!sql.contains("'discover_server_communities'"));
        assert!(!sql.contains("'deliver_to_inbox'"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED LIMIT 1"));

        let post_readback = sql
            .find("kind='fetch_remote_post_refresh' AND attempts=0 THEN 0")
            .expect("local post readback priority");
        let reply_fetch = sql
            .find("kind='fetch_post_replies' AND attempts=0 THEN 1")
            .expect("reply fetch priority");
        let platform_thread = sql
            .find("kind='fetch_platform_post_thread' AND attempts=0 THEN 2")
            .expect("platform thread priority");
        let outbox_preview = sql
            .find("kind='fetch_community_outbox' AND params->>'preview'='true' THEN 3")
            .expect("preview priority");
        let followed_outbox = sql
            .find("kind='fetch_community_outbox' AND attempts=0 THEN 4")
            .expect("followed outbox priority");

        assert!(post_readback < reply_fetch);
        assert!(reply_fetch < platform_thread);
        assert!(platform_thread < outbox_preview);
        assert!(outbox_preview < followed_outbox);
    }

    #[test]
    fn worker_has_a_dedicated_inbox_lane() {
        let sql = super::TAKE_NEXT_INBOX_TASK_SQL;

        assert!(sql.contains("'verify_and_ingest_object_from_inbox'"));
        assert!(sql.contains("'ingest_object_from_inbox'"));
        assert!(!sql.contains("'fetch_platform_post_thread'"));
        assert!(!sql.contains("'discover_server_communities'"));
        assert!(!sql.contains("'deliver_to_inbox'"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED LIMIT 1"));

        let verify = sql
            .find("kind='verify_and_ingest_object_from_inbox' AND attempts=0 THEN 0")
            .expect("verified inbox priority");
        let ingest = sql
            .find("kind='ingest_object_from_inbox' AND attempts=0 THEN 1")
            .expect("plain inbox priority");
        let retry = sql
            .find("attempted_at < current_timestamp - INTERVAL '1 HOUR' THEN 2")
            .expect("aged inbox retry priority");

        assert!(verify < ingest);
        assert!(ingest < retry);
    }

    #[test]
    fn worker_ages_fetch_retries_ahead_of_generic_backlog() {
        let sql = super::TAKE_NEXT_TASK_SQL;

        assert!(sql.contains("attempts>0"));
        assert!(sql.contains("INTERVAL '1 HOUR'"));
        assert!(sql.contains("'fetch_community_featured'"));
        assert!(sql.contains("'fetch_community_outbox'"));
        assert!(sql.contains("'fetch_post_replies'"));
        assert!(sql.contains("'fetch_remote_post_refresh'"));
        assert!(sql.contains("'fetch_platform_post_thread'"));
        assert!(sql.contains("WHEN attempts=0 THEN 13"));
        assert!(sql.contains("ELSE 14"));
    }

    #[test]
    fn worker_takes_pending_tasks_in_deterministic_order() {
        let sql = super::TAKE_NEXT_TASK_SQL;

        assert!(sql.contains("WHERE state='pending'"));
        assert!(sql.contains("attempted_at=current_timestamp"));
        assert!(sql.contains("latest_error=NULL"));
        assert!(sql.contains("ORDER BY"));
        assert!(sql.contains("id"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED LIMIT 1"));
    }

    #[test]
    fn worker_prioritizes_outbox_preview_tasks() {
        let sql = super::TAKE_NEXT_TASK_SQL;

        assert!(sql.contains("fetch_community_outbox"));
        assert!(sql.contains("params->>'preview'='true'"));
        assert!(sql.contains("WHEN kind='fetch_community_outbox'"));
        assert!(sql.contains("WHEN kind='fetch_collection_target_preview'"));
        assert!(sql.contains("params->>'preview'='true' THEN 3"));
        assert!(sql.contains("WHEN kind='fetch_collection_target_preview' THEN 3"));
    }

    #[test]
    fn worker_prioritizes_followed_outbox_fetches_before_inbox_verify_backlog() {
        let sql = super::TAKE_NEXT_TASK_SQL;

        let followed_outbox_fetch = sql
            .find("kind='fetch_community_outbox' AND attempts=0")
            .unwrap();
        let inbox_verify = sql
            .find("kind='verify_and_ingest_object_from_inbox' AND attempts=0")
            .unwrap();
        let post_replies_fetch = sql
            .find("kind='fetch_post_replies' AND attempts=0")
            .unwrap();
        let platform_thread_fetch = sql
            .find("kind='fetch_platform_post_thread' AND attempts=0")
            .unwrap();
        let generic_task = sql.find("WHEN attempts=0 THEN 13").unwrap();

        assert!(followed_outbox_fetch < inbox_verify);
        assert!(followed_outbox_fetch < post_replies_fetch);
        assert!(followed_outbox_fetch < platform_thread_fetch);
        assert!(followed_outbox_fetch < generic_task);
    }

    #[test]
    fn worker_prioritizes_remote_post_readback_before_bulk_refreshes() {
        let sql = super::TAKE_NEXT_TASK_SQL;

        let post_readback = sql
            .find("kind='fetch_remote_post_refresh' AND attempts=0")
            .unwrap();
        let followed_outbox_fetch = sql
            .find("kind='fetch_community_outbox' AND attempts=0")
            .unwrap();
        let platform_thread_fetch = sql
            .find("kind='fetch_platform_post_thread' AND attempts=0")
            .unwrap();

        assert!(post_readback < followed_outbox_fetch);
        assert!(post_readback < platform_thread_fetch);
    }

    #[test]
    fn worker_reset_does_not_requeue_exhausted_running_tasks() {
        let sql = super::RESET_INTERRUPTED_TASKS_SQL;

        assert!(sql.contains("state='running'"));
        assert!(sql.contains("attempts + 1 < max_attempts"));
        assert!(sql.contains("THEN 'pending'::lt_task_state"));
        assert!(sql.contains("ELSE 'failed'::lt_task_state"));
        assert!(sql.contains("Worker stopped while task was running"));
    }

    #[test]
    fn task_cleanup_deletes_completed_tasks_in_bounded_batches() {
        let sql = super::CLEANUP_COMPLETED_TASKS_SQL;

        assert!(sql.contains("state='completed'"));
        assert!(
            sql.contains("completed_at < current_timestamp - make_interval(days => $2::INTEGER)")
        );
        assert!(sql.contains("ORDER BY completed_at"));
        assert!(sql.contains("LIMIT $1"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
    }

    #[test]
    fn task_cleanup_deletes_failed_inbox_tasks_quickly() {
        let sql = super::CLEANUP_FAILED_INBOX_TASKS_SQL;

        assert!(sql.contains("state='failed'"));
        assert!(sql.contains("kind IN"));
        assert!(sql.contains("ingest_object_from_inbox"));
        assert!(sql.contains("verify_and_ingest_object_from_inbox"));
        assert!(
            sql.contains("attempted_at < current_timestamp - make_interval(days => $2::INTEGER)")
        );
        assert!(sql.contains("ORDER BY attempted_at"));
        assert!(sql.contains("LIMIT $1"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
    }

    #[test]
    fn task_cleanup_deletes_failed_tasks_in_bounded_batches() {
        let sql = super::CLEANUP_FAILED_TASKS_SQL;

        assert!(sql.contains("state='failed'"));
        assert!(
            sql.contains("attempted_at < current_timestamp - make_interval(days => $2::INTEGER)")
        );
        assert!(sql.contains("ORDER BY attempted_at"));
        assert!(sql.contains("LIMIT $1"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
    }

    #[test]
    fn task_cleanup_compacts_failed_inbox_payloads_after_debug_window() {
        let sql = super::COMPACT_FAILED_INBOX_TASK_PAYLOADS_SQL;

        assert!(sql.contains("state='failed'"));
        assert!(sql.contains("ingest_object_from_inbox"));
        assert!(sql.contains("verify_and_ingest_object_from_inbox"));
        assert!(
            sql.contains("attempted_at < current_timestamp - make_interval(hours => $2::INTEGER)")
        );
        assert!(sql.contains("params->>'discarded' IS NULL"));
        assert!(sql.contains("json_build_object"));
        assert!(sql.contains("original_bytes"));
        assert!(sql.contains("LIMIT $1"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
    }

    #[test]
    fn failed_inbox_tasks_drop_large_params_on_permanent_failure() {
        let sql = super::FAIL_TASK_SQL;

        assert!(sql.contains("kind IN ($3, $4)"));
        assert!(sql.contains("attempts + 1 >= max_attempts"));
        assert!(sql.contains("AND $5"));
        assert!(sql.contains("json_build_object"));
        assert!(sql.contains("original_bytes"));
    }

    #[test]
    fn task_errors_are_capped_before_storage() {
        let err = "x".repeat(super::TASK_ERROR_MAX_CHARS + 20);
        let err = super::truncate_task_error(err);

        assert!(err.len() < super::TASK_ERROR_MAX_CHARS + 20);
        assert!(err.ends_with("[truncated]"));
    }

    #[test]
    fn pg_repack_janitor_is_weekly_and_opt_in() {
        assert_eq!(super::PG_REPACK_CRON, "43 31 4 * * 0");
        assert_eq!(super::PG_REPACK_MAX_TABLES, 2);
        assert!(super::PG_REPACK_TIMEOUT.as_secs() <= 30 * 60);

        assert!(super::env_flag_enabled("true"));
        assert!(super::env_flag_enabled("1"));
        assert!(super::env_flag_enabled("YES"));
        assert!(!super::env_flag_enabled("false"));
        assert!(!super::env_flag_enabled(""));
    }

    #[test]
    fn pg_repack_candidates_are_large_primary_key_tables() {
        let sql = super::PG_REPACK_CANDIDATE_TABLES_SQL;

        assert!(sql.contains("FROM pg_stat_user_tables"));
        assert!(sql.contains("schemaname='public'"));
        assert!(sql.contains("pg_total_relation_size(relid) >= $1"));
        assert!(sql.contains("n_dead_tup >= $2"));
        assert!(sql.contains("pg_index.indisprimary"));
        assert!(sql.contains("LIMIT $3"));
    }

    #[test]
    fn pg_repack_connection_parses_database_url_without_leaking_password() {
        let connection = super::pg_repack_connection_from_database_url(
            "postgresql://lotide:secret@localhost:5432/lotide",
        )
        .unwrap();

        assert_eq!(
            connection,
            super::PgRepackConnection {
                dbname: "lotide".to_owned(),
                host: Some("localhost".to_owned()),
                port: Some(5432),
                username: Some("lotide".to_owned()),
                password: Some("secret".to_owned()),
            }
        );

        let connection = super::pg_repack_connection_from_database_url("lotide").unwrap();
        assert_eq!(connection.dbname, "lotide");
        assert_eq!(connection.password, None);
    }

    #[test]
    fn pg_repack_default_binary_matches_platform_conventions() {
        let bin = super::default_pg_repack_bin();

        if cfg!(target_os = "linux") {
            assert_eq!(bin, "/usr/lib/postgresql/18/bin/pg_repack");
        } else if cfg!(target_os = "windows") {
            assert_eq!(bin, "pg_repack.exe");
        } else {
            assert_eq!(bin, "pg_repack");
        }
    }

    #[test]
    fn pg_repack_identifier_validation_is_conservative() {
        assert!(super::pg_repack_identifier_is_simple("public"));
        assert!(super::pg_repack_identifier_is_simple("_task"));
        assert!(super::pg_repack_identifier_is_simple("task_2026"));
        assert!(!super::pg_repack_identifier_is_simple(""));
        assert!(!super::pg_repack_identifier_is_simple("2026_task"));
        assert!(!super::pg_repack_identifier_is_simple("needs-quote"));
        assert!(!super::pg_repack_identifier_is_simple("has.dot"));
        assert!(!super::pg_repack_identifier_is_simple(&"a".repeat(64)));
    }

    #[test]
    fn scheduler_enqueues_outbox_fetches_for_followed_remote_communities() {
        let sql = super::ENQUEUE_FOLLOWED_REMOTE_COMMUNITY_OUTBOX_FETCHES_SQL;

        assert!(sql.contains("kind, params, max_attempts, created_at"));
        assert!(sql.contains("'community_id', community.id"));
        assert!(sql.contains("'outbox_url', community.ap_outbox"));
        assert!(sql.contains("WHERE NOT community.local"));
        assert!(sql.contains("AND NOT community.deleted"));
        assert!(sql.contains("community.ap_outbox IS NOT NULL"));
        assert!(sql.contains("community_discovery_server"));
        assert!(sql.contains("community_discovery_server.suppressed_reason IS NOT NULL"));
        assert!(sql.contains("NOT community_discovery_server.active"));
        assert!(sql.contains("community_discovery_server.failed_checks >= 3"));
        assert!(sql.contains("INTERVAL '12 HOURS'"));
        assert!(sql.contains("INTERVAL '6 HOURS'"));
        assert!(sql.contains("community_follow.local"));
        assert!(sql.contains("community_follow.accepted"));
        assert!(sql.contains("task.state='failed'"));
        assert!(sql.contains("task.attempted_at > current_timestamp - INTERVAL '6 HOURS'"));
        assert!(sql.contains("task.state IN ('pending', 'running')"));
        assert!(sql.contains("task.params->>'community_id'=community.id::TEXT"));
    }

    #[test]
    fn scheduler_seeds_and_enqueues_due_community_discovery() {
        let seed_sql = super::UPSERT_KNOWN_COMMUNITY_DISCOVERY_SERVERS_SQL;
        let enqueue_sql = super::ENQUEUE_DUE_COMMUNITY_DISCOVERY_SQL;

        assert!(seed_sql.contains("regexp_replace"));
        assert!(seed_sql.contains("^www\\."));
        assert!(seed_sql.contains("FROM community WHERE NOT local AND NOT deleted"));
        assert!(seed_sql.contains("FROM post WHERE NOT local AND NOT deleted"));
        assert!(seed_sql.contains("FROM reply WHERE NOT local AND NOT deleted"));
        assert!(seed_sql.contains("FROM person WHERE NOT local"));
        assert!(seed_sql.contains("FROM post_like WHERE NOT local"));
        assert!(seed_sql.contains("FROM reply_like WHERE NOT local"));
        assert!(seed_sql.contains("WHERE host IS NOT NULL"));
        assert!(enqueue_sql.contains("INSERT INTO task"));
        assert!(enqueue_sql.contains("suppressed_reason IS NULL"));
        assert!(enqueue_sql.contains("community_server_visibility_suppression"));
        assert!(enqueue_sql.contains("regexp_replace"));
        assert!(enqueue_sql.contains("community_discovery_server.host"));
        assert!(enqueue_sql.contains("active AND failed_checks=0"));
        assert!(enqueue_sql.contains("active AND failed_checks=1"));
        assert!(enqueue_sql.contains("active AND failed_checks>=2"));
        assert!(enqueue_sql.contains("NOT active"));
        assert!(enqueue_sql.contains("make_interval(hours => $4::INTEGER)"));
        assert!(enqueue_sql.contains("make_interval(hours => ($4::INTEGER * 2))"));
        assert!(enqueue_sql.contains("make_interval(hours => ($4::INTEGER * 4))"));
        assert!(enqueue_sql.contains("'fedigroups-directory'"));
        assert!(enqueue_sql.contains("'mbin-compatible'"));
        assert!(enqueue_sql.contains("'wordpress'"));
        assert!(enqueue_sql.contains(
            "community_discovery_server.software IN ('discourse', 'hubzilla', 'friendica')"
        ));
        assert!(enqueue_sql.contains("LEFT JOIN LATERAL"));
        assert!(enqueue_sql.contains("useful_community_count"));
        assert!(enqueue_sql.contains("newest_useful_community_seen"));
        assert!(enqueue_sql.contains("remote_post_count >= 2"));
        assert!(enqueue_sql.contains("INTERVAL '2 DAYS'"));
        assert!(enqueue_sql.contains("'host', community_discovery_server.host"));
        assert!(enqueue_sql.contains("'software', community_discovery_server.software"));
        assert!(enqueue_sql.contains("ORDER BY"));
        assert!(enqueue_sql.contains("LIMIT $3"));
        assert!(enqueue_sql.contains("task.state IN ('pending', 'running')"));
        assert!(enqueue_sql.contains("task.params->>'host'=community_discovery_server.host"));
        assert!(super::DISCOVERY_SETTINGS_SQL.contains("discovery_enqueue_limit"));
        assert!(super::DISCOVERY_SETTINGS_SQL.contains("discovery_refresh_interval_hours"));
    }

    #[test]
    fn scheduler_enqueues_external_discovery_host_seed_sparingly() {
        let sql = super::ENQUEUE_COMMUNITY_DISCOVERY_HOST_SEED_SQL;

        assert!(sql.contains("INSERT INTO task"));
        assert!(sql.contains("'{}'::JSON"));
        assert!(sql.contains("task.kind=$1"));
        assert!(sql.contains("task.state IN ('pending', 'running')"));
        assert!(sql.contains("task.state='completed'"));
        assert!(sql.contains("INTERVAL '24 HOURS'"));
        assert!(sql.contains("task.state='failed'"));
        assert!(sql.contains("INTERVAL '6 HOURS'"));
    }

    #[test]
    fn scheduler_enqueues_discourse_discover_seed_weekly() {
        let sql = super::ENQUEUE_DISCOURSE_DISCOVERY_HOST_SEED_SQL;

        assert!(sql.contains("INSERT INTO task"));
        assert!(sql.contains("'{}'::JSON"));
        assert!(sql.contains("task.kind=$1"));
        assert!(sql.contains("task.state IN ('pending', 'running')"));
        assert!(sql.contains("task.state='completed'"));
        assert!(sql.contains("INTERVAL '7 DAYS'"));
        assert!(sql.contains("task.state='failed'"));
        assert!(sql.contains("INTERVAL '6 HOURS'"));
    }

    #[test]
    fn scheduler_enqueues_due_community_interaction_probes_per_host() {
        let enqueue_sql = super::ENQUEUE_DUE_COMMUNITY_INTERACTION_PROBES_SQL;

        assert!(enqueue_sql.contains("interaction_probe_checked_at IS NULL"));
        assert!(enqueue_sql.contains("suppressed_reason IS NOT NULL"));
        assert!(enqueue_sql.contains("INTERVAL '7 DAYS'"));
        assert!(enqueue_sql.contains("INTERVAL '3 DAYS'"));
        assert!(enqueue_sql.contains("regexp_replace"));
        assert!(enqueue_sql.contains("^www\\."));
        assert!(enqueue_sql.contains("COALESCE(community.ap_inbox, community.ap_shared_inbox)"));
        assert!(!enqueue_sql.contains("COALESCE(community.ap_shared_inbox, community.ap_inbox)"));
        assert!(enqueue_sql.contains("NOT post.local"));
        assert!(enqueue_sql.contains("post.approved"));
        assert!(enqueue_sql.contains("task.state IN ('pending', 'running')"));
        assert!(enqueue_sql.contains("task.params->>'host'=community_discovery_server.host"));
        assert!(enqueue_sql.contains("LIMIT 50"));
    }

    #[test]
    fn janitor_repairs_conventional_remote_community_endpoints() {
        let sql = super::JANITOR_REPAIR_CONVENTIONAL_COMMUNITY_ENDPOINTS_SQL;

        assert!(sql.contains("WHERE NOT local"));
        assert!(sql.contains("ap_id ~*"));
        assert!(sql.contains("(apub/)?communities"));
        assert!(sql.contains("video-channels"));
        assert!(sql.contains("magazine"));
        assert!(sql.contains("target_community.actor_url || '/inbox'"));
        assert!(sql.contains("target_community.actor_url || '/outbox'"));
        assert!(sql.contains("target_community.actor_url || '/followers'"));
        assert!(sql.contains("LIMIT $1"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
    }

    #[test]
    fn janitor_requeues_only_unsent_local_remote_follows() {
        let sql = super::JANITOR_PENDING_COMMUNITY_FOLLOWS_SQL;

        assert!(sql.contains("community_follow.local"));
        assert!(sql.contains("NOT community.local"));
        assert!(sql.contains("NOT community.deleted"));
        assert!(sql.contains("NOT community_follow.accepted"));
        assert!(sql.contains("community_follow.federation_sent_at IS NULL"));
        assert!(sql.contains("community.ap_id IS NOT NULL"));
        assert!(sql.contains("COALESCE(community.ap_inbox, community.ap_shared_inbox)"));
        assert!(!sql.contains("COALESCE(community.ap_shared_inbox, community.ap_inbox)"));
        assert!(sql.contains("LIMIT $1"));
    }

    #[test]
    fn janitor_requeues_only_actionable_follow_undos() {
        let community_sql = super::JANITOR_PENDING_COMMUNITY_FOLLOW_UNDOS_SQL;
        let collection_sql = super::JANITOR_PENDING_COLLECTION_TARGET_FOLLOW_UNDOS_SQL;
        let user_sql = super::JANITOR_PENDING_USER_FOLLOW_UNDOS_SQL;

        assert!(community_sql.contains("federation_sent_at IS NULL"));
        assert!(community_sql.contains("NOT community.local"));
        assert!(!community_sql.contains("NOT community.deleted"));
        assert!(community_sql.contains("COALESCE(community.ap_inbox, community.ap_shared_inbox)"));
        assert!(community_sql.contains("task.kind='deliver_to_inbox'"));
        assert!(community_sql.contains("task.state IN ('pending', 'running')"));
        assert!(community_sql.contains("task.params::TEXT LIKE '%'"));
        assert!(community_sql.contains("LIMIT $1"));

        assert!(collection_sql.contains("federation_sent_at IS NULL"));
        assert!(collection_sql.contains("collection_target.owner_ap_id IS NOT NULL"));
        assert!(collection_sql.contains(
            "COALESCE(collection_target.owner_shared_inbox, collection_target.owner_inbox)"
        ));
        assert!(collection_sql.contains("task.kind='deliver_to_inbox'"));
        assert!(collection_sql.contains("LIMIT $1"));

        assert!(user_sql.contains("federation_sent_at IS NULL"));
        assert!(user_sql.contains("NOT person.local"));
        assert!(user_sql.contains("person.ap_id IS NOT NULL"));
        assert!(user_sql.contains("person.ap_inbox IS NOT NULL"));
        assert!(user_sql.contains("task.kind='deliver_to_inbox'"));
        assert!(user_sql.contains("LIMIT $1"));
    }

    #[test]
    fn janitor_completes_local_follow_undos_without_delivery() {
        let community_sql = super::JANITOR_COMPLETE_LOCAL_COMMUNITY_FOLLOW_UNDOS_SQL;
        let user_sql = super::JANITOR_COMPLETE_LOCAL_USER_FOLLOW_UNDOS_SQL;

        assert!(community_sql.contains("INNER JOIN community"));
        assert!(community_sql.contains("federation_received_at IS NULL"));
        assert!(community_sql.contains("community.local"));
        assert!(community_sql.contains("federation_sent_at=COALESCE"));
        assert!(community_sql.contains("federation_received_at=COALESCE"));
        assert!(community_sql.contains("LIMIT $1"));
        assert!(community_sql.contains("FOR UPDATE OF local_community_follow_undo SKIP LOCKED"));

        assert!(user_sql.contains("INNER JOIN person"));
        assert!(user_sql.contains("federation_received_at IS NULL"));
        assert!(user_sql.contains("person.local"));
        assert!(user_sql.contains("federation_sent_at=COALESCE"));
        assert!(user_sql.contains("federation_received_at=COALESCE"));
        assert!(user_sql.contains("LIMIT $1"));
        assert!(user_sql.contains("FOR UPDATE OF local_user_follow_undo SKIP LOCKED"));
    }

    #[test]
    fn janitor_recalculates_cached_post_likes_from_like_rows() {
        let sql = super::JANITOR_RECALCULATE_POST_LIKES_SQL;

        assert!(sql.contains("cached_likes_for_sort"));
        assert!(sql.contains("post_like.person != post.author"));
        assert!(sql.contains("COUNT(post_like.person)::INTEGER"));
        assert!(sql.contains("IS DISTINCT FROM"));
        assert!(sql.contains("LIMIT $1"));
    }

    #[test]
    fn janitor_deactivates_stale_discovered_communities() {
        let sql = super::JANITOR_DEACTIVATE_STALE_COMMUNITY_DISCOVERY_SQL;

        assert!(sql.contains("community_discovery.active"));
        assert!(sql.contains("community.deleted"));
        assert!(sql.contains("COALESCE(community_discovery.remote_post_count, 0) < 2"));
        assert!(sql.contains("community_discovery_server.suppressed_reason IS NOT NULL"));
        assert!(!sql.contains("NOT community_discovery_server.active"));
        assert!(!sql.contains("community_discovery_server.failed_checks >= 3"));
        assert!(sql.contains("SET active=FALSE"));
        assert!(sql.contains("LIMIT $1"));
        assert!(sql.contains("FOR UPDATE OF community_discovery SKIP LOCKED"));
    }

    #[test]
    fn janitor_reactivates_current_discovered_communities() {
        let sql = super::JANITOR_REACTIVATE_CURRENT_COMMUNITY_DISCOVERY_SQL;

        assert!(sql.contains("NOT community_discovery.active"));
        assert!(sql.contains("NOT community.deleted"));
        assert!(sql.contains("community_discovery.remote_post_count >= 2"));
        assert!(sql.contains("community_discovery_server.active"));
        assert!(sql.contains("community_discovery_server.failed_checks < 3"));
        assert!(sql.contains("community_discovery_server.suppressed_reason IS NULL"));
        assert!(sql.contains("SET active=TRUE"));
        assert!(sql.contains("LIMIT $1"));
        assert!(sql.contains("FOR UPDATE OF community_discovery SKIP LOCKED"));
    }

    #[test]
    fn janitor_compacts_completed_inbox_task_payloads() {
        let sql = super::JANITOR_COMPACT_COMPLETED_INBOX_TASK_PAYLOADS_SQL;

        assert!(sql.contains("state='completed'"));
        assert!(sql.contains("ingest_object_from_inbox"));
        assert!(sql.contains("verify_and_ingest_object_from_inbox"));
        assert!(sql.contains("completed_at < current_timestamp - INTERVAL '1 HOUR'"));
        assert!(sql.contains("params->>'discarded' IS NULL"));
        assert!(sql.contains("json_build_object"));
        assert!(sql.contains("original_bytes"));
        assert!(sql.contains("LIMIT $1"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
    }

    #[test]
    fn janitor_completes_irrelevant_inbox_tasks_in_batches() {
        let sql = super::JANITOR_COMPLETE_IRRELEVANT_INBOX_TASKS_SQL;

        assert!(sql.contains("state='pending'"));
        assert!(sql.contains("verify_and_ingest_object_from_inbox"));
        assert!(sql.contains("body_json->>'type'='Announce'"));
        assert!(sql.contains("community_follow.accepted"));
        assert!(sql.contains("body_json->>'type'='Delete'"));
        assert!(sql.contains("jsonb_typeof(inbox_task.body_json->'actor')='string'"));
        assert!(sql.contains("FROM person WHERE ap_id=delete_task.actor"));
        assert!(sql.contains("FROM community WHERE ap_id=delete_task.actor"));
        assert!(sql.contains("FROM post WHERE ap_id=delete_task.object_id"));
        assert!(sql.contains("FROM reply WHERE ap_id=delete_task.object_id"));
        assert!(sql.contains("FROM post_like WHERE ap_id=delete_task.object_id"));
        assert!(sql.contains("FROM reply_like WHERE ap_id=delete_task.object_id"));
        assert!(sql.contains("SET state='completed'::lt_task_state"));
        assert!(sql.contains("LIMIT $1"));
    }

    #[test]
    fn janitor_repairs_blank_post_titles_with_bounded_fallback() {
        let sql = super::JANITOR_REPAIR_BLANK_POST_TITLES_SQL;

        assert!(sql.contains("post.title IS NULL"));
        assert!(sql.contains("btrim(post.title)=''"));
        assert!(sql.contains("regexp_replace"));
        assert!(sql.contains("<[^>]*>"));
        assert!(sql.contains("chr(13)"));
        assert!(sql.contains("chr(10)"));
        assert!(sql.contains("LEFT(source.first_line, 80)"));
        assert!(sql.contains("[no title]"));
        assert!(sql.contains("LIMIT $1"));
        assert!(sql.contains("FOR UPDATE OF post SKIP LOCKED"));
    }

    #[test]
    fn janitor_runs_as_bounded_daily_maintenance() {
        assert_eq!(super::JANITOR_CRON, "29 5 3 * * *");

        const {
            assert!(super::JANITOR_BATCH_SIZE <= 1000);
            assert!(super::JANITOR_MAX_BATCHES <= 10);
            assert!(super::JANITOR_FOLLOW_REPAIR_LIMIT <= 100);
        }
    }

    #[test]
    fn janitor_report_counts_all_changes() {
        let report = super::JanitorReport {
            repaired_community_endpoints: 1,
            requeued_community_follows: 2,
            requeued_community_follow_undos: 3,
            requeued_collection_target_follow_undos: 4,
            requeued_user_follow_undos: 5,
            completed_local_follow_undos: 6,
            recalculated_post_likes: 7,
            deactivated_discoveries: 8,
            reactivated_discoveries: 9,
            compacted_completed_inbox_tasks: 10,
            failed_terminal_inbox_tasks: 11,
            completed_irrelevant_inbox_tasks: 12,
            repaired_post_titles: 13,
        };

        assert_eq!(report.total_changes(), 91);
    }

    #[test]
    fn cleanup_settings_are_read_from_site_table() {
        let sql = super::CLEANUP_SETTINGS_SQL;

        assert!(sql.contains("cleanup_remote_posts_enabled"));
        assert!(sql.contains("cleanup_remote_post_retention_days"));
        assert!(sql.contains("cleanup_preview_posts_enabled"));
        assert!(sql.contains("cleanup_preview_post_retention_hours"));
        assert!(sql.contains("cleanup_deleted_remote_communities_enabled"));
        assert!(sql.contains("cleanup_unfollowed_remote_communities_enabled"));
        assert!(sql.contains("cleanup_remote_interactions_enabled"));
        assert!(sql.contains("cleanup_notifications_enabled"));
        assert!(sql.contains("cleanup_notification_retention_days"));
        assert!(sql.contains("cleanup_failed_inbox_task_payloads_enabled"));
        assert!(sql.contains("cleanup_failed_inbox_task_payload_retention_days"));
        assert!(sql.contains("cleanup_completed_task_retention_days"));
        assert!(sql.contains("cleanup_failed_task_retention_days"));
        assert!(sql.contains("cleanup_failed_inbox_task_payload_compaction_hours"));
        assert!(sql.contains("FROM site"));
        assert!(sql.contains("WHERE local"));
    }

    #[test]
    fn notification_cleanup_keeps_unread_notifications() {
        let sql = super::CLEANUP_OLD_NOTIFICATIONS_SQL;

        assert!(sql.contains("FROM notification"));
        assert!(
            sql.contains("created_at < current_timestamp - make_interval(days => $2::INTEGER)")
        );
        assert!(sql.contains("person.last_checked_notifications"));
        assert!(sql.contains("LIMIT $1"));
        assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
    }

    #[test]
    fn remote_post_cleanup_keeps_local_activity() {
        let sql = super::CLEANUP_OLD_REMOTE_POSTS_SQL;

        assert!(sql.contains("NOT post.local"));
        assert!(sql.contains("NOT community.local"));
        assert!(sql.contains("$2"));
        assert!(sql.contains("post.deleted"));
        assert!(sql.contains("community.deleted"));
        assert!(sql.contains("community_follow.accepted"));
        assert!(
            sql.contains("post.created < current_timestamp - make_interval(days => $3::INTEGER)")
        );
        assert!(sql.contains("$4"));
        assert!(sql.contains(
            "post.updated_local < current_timestamp - make_interval(hours => $5::INTEGER)"
        ));
        assert!(sql.contains("reply.local"));
        assert!(sql.contains("post_like.local"));
        assert!(sql.contains("reply_like.local"));
        assert!(sql.contains("DELETE FROM notification"));
        assert!(sql.contains("DELETE FROM post_like"));
        assert!(sql.contains("DELETE FROM reply_like"));
        assert!(sql.contains("SELECT COUNT(*)::BIGINT FROM deleted_post"));
        assert!(sql.contains("LIMIT $1"));
        assert!(sql.contains("FOR UPDATE OF post SKIP LOCKED"));
    }

    #[test]
    fn preview_cache_cleanup_runs_hourly() {
        assert_eq!(super::PREVIEW_CACHE_CLEANUP_CRON, "37 19 * * * *");
    }

    #[test]
    fn deleted_remote_community_cleanup_keeps_followed_or_active_rows() {
        let sql = super::CLEANUP_REMOTE_COMMUNITIES_SQL;

        assert!(sql.contains("NOT community.local"));
        assert!(sql.contains("$2 AND community.deleted"));
        assert!(sql.contains("$3 AND NOT community.deleted"));
        assert!(sql.contains("community_follow.accepted"));
        assert!(sql.contains("community_discovery.active"));
        assert!(sql.contains("community_discovery.remote_post_count >= 2"));
        assert!(sql.contains("NOT EXISTS (SELECT 1 FROM post WHERE post.community=community.id)"));
        assert!(sql.contains("DELETE FROM community_follow"));
        assert!(sql.contains("DELETE FROM community_moderator"));
        assert!(sql.contains("DELETE FROM community"));
        assert!(sql.contains("SELECT COUNT(*)::BIGINT FROM deleted_community"));
    }

    #[test]
    fn remote_post_like_cleanup_keeps_local_likes_and_refreshes_cache() {
        let sql = super::CLEANUP_OLD_REMOTE_POST_LIKES_SQL;

        assert!(sql.contains("NOT post_like.local"));
        assert!(sql.contains("NOT post.local"));
        assert!(sql.contains("post.deleted"));
        assert!(
            sql.contains("post.created < current_timestamp - make_interval(days => $2::INTEGER)")
        );
        assert!(sql.contains("FOR UPDATE OF post_like SKIP LOCKED"));
        assert!(sql.contains("DELETE FROM post_like"));
        assert!(sql.contains("SET cached_likes_for_sort="));
        assert!(sql.contains("COUNT(*)::BIGINT"));
    }

    #[test]
    fn remote_reply_like_cleanup_keeps_local_reply_activity() {
        let sql = super::CLEANUP_OLD_REMOTE_REPLY_LIKES_SQL;

        assert!(sql.contains("NOT reply_like.local"));
        assert!(sql.contains("NOT reply.local"));
        assert!(sql.contains("NOT post.local"));
        assert!(sql.contains("reply.deleted"));
        assert!(sql.contains("post.deleted"));
        assert!(
            sql.contains("post.created < current_timestamp - make_interval(days => $2::INTEGER)")
        );
        assert!(sql.contains("FOR UPDATE OF reply_like SKIP LOCKED"));
        assert!(sql.contains("DELETE FROM reply_like"));
        assert!(sql.contains("COUNT(*)::BIGINT"));
    }
}
