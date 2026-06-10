--
-- Project: Lotide database migrations
-- -----------------------------------
--
-- File: up.sql
--
-- Purpose:
--
--     Add persisted federation delivery checkpoints for user Follow and
--     user follow Undo activities.
--
-- Responsibilities:
--
--     - record when local user Follow activities are sent
--     - record when remote inboxes accept those Follow activities
--     - record when local Accept activities for remote user follows are sent
--     - record when user follow Undo activities are sent and accepted
--
-- This file intentionally does NOT contain:
--
--     - ActivityPub packet construction
--     - inbox verification behavior
--     - UI rendering behavior
--

ALTER TABLE person_follow
    ADD COLUMN federation_sent_at timestamp with time zone,
    ADD COLUMN federation_received_at timestamp with time zone;

ALTER TABLE local_user_follow_undo
    ADD COLUMN created_at timestamp with time zone NOT NULL DEFAULT current_timestamp,
    ADD COLUMN federation_sent_at timestamp with time zone,
    ADD COLUMN federation_received_at timestamp with time zone;

/* end of up.sql */
