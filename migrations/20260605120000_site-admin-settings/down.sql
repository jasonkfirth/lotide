BEGIN;
	ALTER TABLE site
		DROP CONSTRAINT cleanup_preview_post_retention_hours_range,
		DROP CONSTRAINT cleanup_remote_post_retention_days_range,
		DROP CONSTRAINT site_name_not_blank;

	ALTER TABLE site
		DROP COLUMN cleanup_remote_interactions_enabled,
		DROP COLUMN cleanup_unfollowed_remote_communities_enabled,
		DROP COLUMN cleanup_deleted_remote_communities_enabled,
		DROP COLUMN cleanup_preview_post_retention_hours,
		DROP COLUMN cleanup_preview_posts_enabled,
		DROP COLUMN cleanup_remote_post_retention_days,
		DROP COLUMN cleanup_remote_posts_enabled,
		DROP COLUMN site_name;
COMMIT;
