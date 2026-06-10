CREATE INDEX IF NOT EXISTS post_community_created_not_deleted_idx
ON post (community, created DESC, id DESC)
WHERE approved AND NOT deleted;

CREATE INDEX IF NOT EXISTS post_remote_cleanup_idx
ON post (created, id)
WHERE NOT local AND NOT deleted;

CREATE INDEX IF NOT EXISTS reply_local_post_idx
ON reply (post)
WHERE local;

CREATE INDEX IF NOT EXISTS community_follow_active_remote_idx
ON community_follow (follower, community)
WHERE local AND accepted;
