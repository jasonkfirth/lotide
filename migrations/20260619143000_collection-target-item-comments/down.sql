--
-- Project: Lotide database migrations
-- -----------------------------------
--
-- File: down.sql
--
-- Purpose:
--
--     Remove local source-item reply storage.
--
-- Responsibilities:
--
--     - drop indexes owned by the source-item reply table
--     - drop the source-item reply table
--
-- This file intentionally does NOT contain:
--
--     - cleanup for unrelated source preview data
--     - changes to forum comments
--     - changes to source-item likes
--

BEGIN;
    DROP INDEX collection_target_item_comment_author_idx;
    DROP INDEX collection_target_item_comment_item_created_idx;
    DROP TABLE collection_target_item_comment;
COMMIT;

/* end of down.sql */
