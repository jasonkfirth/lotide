--
-- Project: Lotide database migrations
-- -----------------------------------
--
-- File: down.sql
--
-- Purpose:
--
--     Remove persisted federation delivery checkpoints for user Follow and
--     user follow Undo activities.
--
-- Responsibilities:
--
--     - undo the schema changes from this migration
--
-- This file intentionally does NOT contain:
--
--     - task cleanup
--     - ActivityPub behavior changes
--     - user or follow deletion
--

ALTER TABLE local_user_follow_undo
    DROP COLUMN federation_received_at,
    DROP COLUMN federation_sent_at,
    DROP COLUMN created_at;

ALTER TABLE person_follow
    DROP COLUMN federation_received_at,
    DROP COLUMN federation_sent_at;

/* end of down.sql */
