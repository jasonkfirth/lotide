--
-- Project: Lotide database migrations
-- -----------------------------------
--
-- File: up.sql
--
-- Purpose:
--
--     Add persisted federation status checkpoints for local Like
--     activities sent to remote communities.
--
-- Responsibilities:
--
--     - record when a Like activity has been queued for delivery
--     - record when a remote inbox accepted the Like activity
--     - record when the same Like activity is later observed from the remote side
--
-- This file intentionally does NOT contain:
--
--     - task execution logic
--     - ActivityPub parsing rules
--     - UI rendering behavior
--

ALTER TABLE post_like
    ADD COLUMN federation_sent_at timestamp with time zone,
    ADD COLUMN federation_received_at timestamp with time zone,
    ADD COLUMN federation_posted_at timestamp with time zone;

ALTER TABLE reply_like
    ADD COLUMN federation_sent_at timestamp with time zone,
    ADD COLUMN federation_received_at timestamp with time zone,
    ADD COLUMN federation_posted_at timestamp with time zone;

/* end of up.sql */
