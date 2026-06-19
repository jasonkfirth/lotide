--
-- Project: Lotide database migrations
-- -----------------------------------
--
-- File: down.sql
--
-- Purpose:
--
--     Remove source preview item Like tracking.
--
-- Responsibilities:
--
--     - drop source item Like undo tracking
--     - drop source item Like rows
--
-- This file intentionally does NOT contain:
--
--     - remote Undo delivery
--     - source preview deletion
--     - post/comment Like rollback
--

BEGIN;
    DROP INDEX local_collection_target_item_like_undo_item_person_idx;
    DROP TABLE local_collection_target_item_like_undo;

    DROP INDEX collection_target_item_like_person_idx;
    DROP TABLE collection_target_item_like;
COMMIT;

/* end of down.sql */
