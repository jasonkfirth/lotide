--
-- Project: Lotide database migrations
-- -----------------------------------
--
-- File: up.sql
--
-- Purpose:
--
--     Track local Like activities sent for source preview items.
--
-- Responsibilities:
--
--     - store one local like per source item and user
--     - preserve the ActivityPub Like id needed for Undo
--     - record delivery checkpoints for the user interface
--
-- This file intentionally does NOT contain:
--
--     - ActivityPub delivery logic
--     - source preview crawling
--     - user interface rendering
--

BEGIN;
    CREATE TABLE collection_target_item_like (
        item BIGINT NOT NULL REFERENCES collection_target_item(id) ON DELETE CASCADE,
        person BIGINT NOT NULL REFERENCES person(id) ON DELETE CASCADE,
        local BOOLEAN NOT NULL DEFAULT TRUE,
        ap_id TEXT UNIQUE,
        federation_sent_at TIMESTAMP WITH TIME ZONE,
        federation_received_at TIMESTAMP WITH TIME ZONE,
        federation_posted_at TIMESTAMP WITH TIME ZONE,
        created_local TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
        PRIMARY KEY (item, person)
    );

    CREATE INDEX collection_target_item_like_person_idx
        ON collection_target_item_like(person);

    CREATE TABLE local_collection_target_item_like_undo (
        id UUID PRIMARY KEY,
        item BIGINT NOT NULL REFERENCES collection_target_item(id) ON DELETE CASCADE,
        person BIGINT NOT NULL REFERENCES person(id) ON DELETE CASCADE,
        like_ap_id TEXT,
        federation_sent_at TIMESTAMP WITH TIME ZONE,
        federation_received_at TIMESTAMP WITH TIME ZONE,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp
    );

    CREATE INDEX local_collection_target_item_like_undo_item_person_idx
        ON local_collection_target_item_like_undo(item, person);
COMMIT;

/* end of up.sql */
