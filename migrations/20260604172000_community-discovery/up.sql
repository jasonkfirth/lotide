CREATE TABLE community_discovery_server (
	host TEXT PRIMARY KEY,
	software TEXT,
	active BOOLEAN NOT NULL DEFAULT TRUE,
	last_checked TIMESTAMPTZ,
	last_success TIMESTAMPTZ,
	failed_checks INTEGER NOT NULL DEFAULT 0,
	latest_error TEXT,
	suppressed_reason TEXT,
	suppressed_at TIMESTAMPTZ
);

CREATE TABLE community_discovery (
	community BIGINT PRIMARY KEY REFERENCES community ON DELETE CASCADE,
	host TEXT NOT NULL REFERENCES community_discovery_server ON DELETE CASCADE,
	discovered_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
	last_seen TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
	active BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX community_discovery_host_active_idx
ON community_discovery (host, last_seen DESC)
WHERE active;

CREATE TABLE community_server_visibility_suppression (
	community BIGINT PRIMARY KEY REFERENCES community ON DELETE CASCADE,
	reason TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);

CREATE TABLE community_user_visibility_suppression (
	community BIGINT NOT NULL REFERENCES community ON DELETE CASCADE,
	person BIGINT NOT NULL REFERENCES person ON DELETE CASCADE,
	reason TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
	updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,

	PRIMARY KEY (community, person)
);
