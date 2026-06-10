--
-- Project: Lotide database migrations
-- -----------------------------------
--
-- File: down.sql
--
-- Purpose:
--
--     Remove persisted federation status checkpoints for Like activities.
--
-- Responsibilities:
--
--     - undo the schema changes from this migration
--
-- This file intentionally does NOT contain:
--
--     - task table cleanup
--     - ActivityPub behavior changes
--     - user or content deletion
--

ALTER TABLE reply_like
    DROP COLUMN federation_posted_at,
    DROP COLUMN federation_received_at,
    DROP COLUMN federation_sent_at;

ALTER TABLE post_like
    DROP COLUMN federation_posted_at,
    DROP COLUMN federation_received_at,
    DROP COLUMN federation_sent_at;

/* end of down.sql */
