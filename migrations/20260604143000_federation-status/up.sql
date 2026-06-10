--
-- Project: Lotide database migrations
-- -----------------------------------
--
-- File: up.sql
--
-- Purpose:
--
--     Add persisted federation status checkpoints for local content
--     submitted to remote communities.
--
-- Responsibilities:
--
--     - record when a Create activity has been attempted
--     - record when a remote inbox accepted the Create activity
--     - record when a local comment is later observed from the remote side
--
-- This file intentionally does NOT contain:
--
--     - task execution logic
--     - ActivityPub parsing rules
--     - UI rendering behavior
--

ALTER TABLE post
    ADD COLUMN federation_sent_at timestamp with time zone,
    ADD COLUMN federation_received_at timestamp with time zone;

ALTER TABLE reply
    ADD COLUMN federation_sent_at timestamp with time zone,
    ADD COLUMN federation_received_at timestamp with time zone,
    ADD COLUMN federation_posted_at timestamp with time zone,
    ADD COLUMN federation_posted_ap_id text;

/* end of up.sql */
