--
-- Project: Lotide database migrations
-- -----------------------------------
--
-- File: down.sql
--
-- Purpose:
--
--     Remove persisted Like activity ids from local Undo rows.
--
-- Responsibilities:
--
--     - undo the schema additions made by the matching up migration
--
-- This file intentionally does NOT contain:
--
--     - data repair
--     - task delivery behavior
--

ALTER TABLE local_reply_like_undo
    DROP COLUMN like_ap_id;

ALTER TABLE local_post_like_undo
    DROP COLUMN like_ap_id;

/* end of down.sql */
