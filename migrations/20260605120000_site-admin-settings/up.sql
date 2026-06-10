BEGIN;
	ALTER TABLE site
		ADD COLUMN site_name TEXT NOT NULL DEFAULT ('lotide'),
		ADD COLUMN cleanup_remote_posts_enabled BOOLEAN NOT NULL DEFAULT (FALSE),
		ADD COLUMN cleanup_remote_post_retention_days INTEGER NOT NULL DEFAULT (90),
		ADD COLUMN cleanup_preview_posts_enabled BOOLEAN NOT NULL DEFAULT (FALSE),
		ADD COLUMN cleanup_preview_post_retention_hours INTEGER NOT NULL DEFAULT (2),
		ADD COLUMN cleanup_deleted_remote_communities_enabled BOOLEAN NOT NULL DEFAULT (FALSE),
		ADD COLUMN cleanup_unfollowed_remote_communities_enabled BOOLEAN NOT NULL DEFAULT (FALSE),
		ADD COLUMN cleanup_remote_interactions_enabled BOOLEAN NOT NULL DEFAULT (FALSE);

	ALTER TABLE site
		ADD CONSTRAINT site_name_not_blank CHECK (length(btrim(site_name)) > 0),
		ADD CONSTRAINT cleanup_remote_post_retention_days_range CHECK (cleanup_remote_post_retention_days BETWEEN 1 AND 3650),
		ADD CONSTRAINT cleanup_preview_post_retention_hours_range CHECK (cleanup_preview_post_retention_hours BETWEEN 1 AND 720);
COMMIT;
