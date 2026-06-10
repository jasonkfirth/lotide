--
-- Project: Lotide database migrations
-- -----------------------------------
--
-- File: down.sql
--
-- Purpose:
--
--     Remove persisted federation status checkpoints for local Follow and Undo
--     Follow activities.
--
-- Responsibilities:
--
--     - undo the schema changes from this migration
--
-- This file intentionally does NOT contain:
--
--     - task table cleanup
--     - ActivityPub behavior changes
--     - user or community deletion
--

ALTER TABLE local_collection_target_follow_undo
    DROP COLUMN federation_received_at,
    DROP COLUMN federation_sent_at,
    DROP COLUMN created_at;

ALTER TABLE local_community_follow_undo
    DROP COLUMN federation_received_at,
    DROP COLUMN federation_sent_at,
    DROP COLUMN created_at;

ALTER TABLE collection_target_follow
    DROP COLUMN federation_received_at,
    DROP COLUMN federation_sent_at;

ALTER TABLE community_follow
    DROP COLUMN federation_received_at,
    DROP COLUMN federation_sent_at;

/* end of down.sql */
