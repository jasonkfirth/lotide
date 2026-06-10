--
-- Project: Lotide database migrations
-- -----------------------------------
--
-- File: up.sql
--
-- Purpose:
--
--     Add persisted federation status checkpoints for local Follow and Undo
--     Follow activities sent to remote actors.
--
-- Responsibilities:
--
--     - record when a Follow or follow Undo has been queued for delivery
--     - record when a remote inbox accepted that delivery
--     - keep enough state for Hitide to display pending follow/unfollow status
--
-- This file intentionally does NOT contain:
--
--     - ActivityPub packet construction
--     - task execution logic
--     - UI rendering behavior
--

ALTER TABLE community_follow
    ADD COLUMN federation_sent_at timestamp with time zone,
    ADD COLUMN federation_received_at timestamp with time zone;

ALTER TABLE collection_target_follow
    ADD COLUMN federation_sent_at timestamp with time zone,
    ADD COLUMN federation_received_at timestamp with time zone;

ALTER TABLE local_community_follow_undo
    ADD COLUMN created_at timestamp with time zone NOT NULL DEFAULT current_timestamp,
    ADD COLUMN federation_sent_at timestamp with time zone,
    ADD COLUMN federation_received_at timestamp with time zone;

ALTER TABLE local_collection_target_follow_undo
    ADD COLUMN created_at timestamp with time zone NOT NULL DEFAULT current_timestamp,
    ADD COLUMN federation_sent_at timestamp with time zone,
    ADD COLUMN federation_received_at timestamp with time zone;

/* end of up.sql */
