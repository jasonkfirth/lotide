--
-- Project: Lotide database migrations
-- -----------------------------------
--
-- File: down.sql
--
-- Purpose:
--
--     Remove persisted federation status checkpoints.
--
-- Responsibilities:
--
--     - undo the schema changes from this migration
--
-- This file intentionally does NOT contain:
--
--     - data cleanup outside the new status columns
--     - task table changes
--     - ActivityPub behavior changes
--

ALTER TABLE reply
    DROP COLUMN federation_posted_ap_id,
    DROP COLUMN federation_posted_at,
    DROP COLUMN federation_received_at,
    DROP COLUMN federation_sent_at;

ALTER TABLE post
    DROP COLUMN federation_received_at,
    DROP COLUMN federation_sent_at;

/* end of down.sql */
