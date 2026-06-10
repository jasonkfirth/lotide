--
-- Project: Lotide database migrations
-- -----------------------------------
--
-- File: up.sql
--
-- Purpose:
--
--     Preserve the exact Like activity id that an Undo is revoking.
--
-- Responsibilities:
--
--     - store the Like id copied from the deleted local like row
--     - allow repeated like/unlike cycles to use fresh Like ids
--     - keep old Undo rows readable through the existing deterministic fallback
--
-- This file intentionally does NOT contain:
--
--     - ActivityPub packet construction
--     - task delivery behavior
--     - frontend status rendering
--

ALTER TABLE local_post_like_undo
    ADD COLUMN like_ap_id TEXT;

ALTER TABLE local_reply_like_undo
    ADD COLUMN like_ap_id TEXT;

/* end of up.sql */
