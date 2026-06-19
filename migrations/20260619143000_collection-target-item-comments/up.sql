--
-- Project: Lotide database migrations
-- -----------------------------------
--
-- File: up.sql
--
-- Purpose:
--
--     Store local replies sent to source preview items.
--
-- Responsibilities:
--
--     - keep source-item replies separate from forum comments
--     - preserve the ActivityPub object id used by remote servers
--     - record delivery checkpoints for the user interface
--
-- This file intentionally does NOT contain:
--
--     - ActivityPub delivery logic
--     - remote reply crawling
--     - user interface rendering
--

BEGIN;
    CREATE TABLE collection_target_item_comment (
        id BIGSERIAL PRIMARY KEY,
        item BIGINT NOT NULL REFERENCES collection_target_item(id) ON DELETE CASCADE,
        author BIGINT REFERENCES person(id) ON DELETE CASCADE,
        local BOOLEAN NOT NULL DEFAULT TRUE,
        ap_id TEXT UNIQUE,
        content_text TEXT,
        content_markdown TEXT,
        content_html TEXT,
        created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
        deleted BOOLEAN NOT NULL DEFAULT FALSE,
        sensitive BOOLEAN NOT NULL DEFAULT FALSE,
        federation_sent_at TIMESTAMP WITH TIME ZONE,
        federation_received_at TIMESTAMP WITH TIME ZONE,
        federation_posted_at TIMESTAMP WITH TIME ZONE,
        federation_posted_ap_id TEXT
    );

    CREATE INDEX collection_target_item_comment_item_created_idx
        ON collection_target_item_comment(item, created ASC, id ASC);

    CREATE INDEX collection_target_item_comment_author_idx
        ON collection_target_item_comment(author);
COMMIT;

/* end of up.sql */
