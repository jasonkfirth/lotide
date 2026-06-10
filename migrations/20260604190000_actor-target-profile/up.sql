CREATE TABLE actor_target_profile (
	actor_ap_id TEXT PRIMARY KEY,
	target TEXT NOT NULL,
	family TEXT NOT NULL,
	actor_kind TEXT NOT NULL,
	source TEXT NOT NULL,
	confidence SMALLINT NOT NULL CHECK (confidence >= 0 AND confidence <= 100),
	has_inbox BOOLEAN NOT NULL DEFAULT FALSE,
	has_outbox BOOLEAN NOT NULL DEFAULT FALSE,
	has_followers BOOLEAN NOT NULL DEFAULT FALSE,
	has_featured BOOLEAN NOT NULL DEFAULT FALSE,
	evidence JSONB NOT NULL DEFAULT '{}'::JSONB,
	observed_object_types TEXT[] NOT NULL DEFAULT '{}',
	observed_activity_types TEXT[] NOT NULL DEFAULT '{}',
	created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE INDEX actor_target_profile_target_idx
ON actor_target_profile (target, family);

CREATE INDEX actor_target_profile_updated_idx
ON actor_target_profile (updated_at DESC);
