BEGIN;
	CREATE TABLE collection_target_item (
		id BIGSERIAL PRIMARY KEY,
		collection_target BIGINT NOT NULL REFERENCES collection_target ON DELETE CASCADE,
		ap_id TEXT NOT NULL UNIQUE,
		object_type TEXT,
		name TEXT NOT NULL,
		url TEXT,
		attributed_to TEXT,
		content_html TEXT,
		summary_html TEXT,
		image_url TEXT,
		published TIMESTAMP WITH TIME ZONE,
		created_local TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
		updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp
	);

	CREATE INDEX collection_target_item_target_published_idx
		ON collection_target_item(collection_target, published DESC NULLS LAST, id DESC);
COMMIT;
