BEGIN;
	CREATE TABLE collection_target (
		id BIGSERIAL PRIMARY KEY,
		name TEXT NOT NULL,
		target_kind TEXT NOT NULL,
		software TEXT,
		ap_id TEXT NOT NULL UNIQUE,
		owner_actor BIGINT REFERENCES person ON DELETE SET NULL,
		owner_ap_id TEXT,
		owner_inbox TEXT,
		owner_shared_inbox TEXT,
		followers TEXT,
		first_page TEXT,
		last_page TEXT,
		summary_html TEXT,
		total_items BIGINT,
		created_local TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
		updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
		CHECK (target_kind <> '')
	);

	CREATE INDEX collection_target_owner_actor_idx
		ON collection_target(owner_actor);
	CREATE INDEX collection_target_target_kind_idx
		ON collection_target(target_kind);

	CREATE TABLE collection_target_follow (
		collection_target BIGINT NOT NULL REFERENCES collection_target ON DELETE CASCADE,
		follower BIGINT NOT NULL REFERENCES person ON DELETE CASCADE,
		local BOOLEAN NOT NULL,
		ap_id TEXT UNIQUE,
		accepted BOOLEAN NOT NULL,
		PRIMARY KEY(collection_target, follower)
	);

	CREATE INDEX collection_target_follow_follower_idx
		ON collection_target_follow(follower);
	CREATE INDEX collection_target_follow_local_accepted_idx
		ON collection_target_follow(collection_target, local, accepted);

	CREATE TABLE local_collection_target_follow_undo (
		id UUID PRIMARY KEY,
		collection_target BIGINT NOT NULL REFERENCES collection_target ON DELETE CASCADE,
		follower BIGINT NOT NULL REFERENCES person ON DELETE CASCADE,
		follow_ap_id TEXT
	);

	CREATE INDEX local_collection_target_follow_undo_target_idx
		ON local_collection_target_follow_undo(collection_target, follower);
COMMIT;
