/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: 20260618103000_private-messages/up.sql

    Purpose:

        Store one-to-one ActivityPub direct messages separately from public
        posts and comments.

    Responsibilities:

        - persist local and remote private messages
        - track delivery status for locally-sent messages
        - let private messages appear in the existing notification stream

    This migration intentionally does NOT contain:

        - public post or comment storage
        - mailbox retention policy
        - ActivityPub delivery code
*/

BEGIN;

CREATE TABLE private_message (
    id BIGSERIAL PRIMARY KEY,
    ap_id TEXT NOT NULL UNIQUE,
    author BIGINT NOT NULL REFERENCES person ON DELETE CASCADE,
    recipient BIGINT NOT NULL REFERENCES person ON DELETE CASCADE,
    local BOOLEAN NOT NULL,
    content_text TEXT,
    content_markdown TEXT,
    content_html TEXT,
    sensitive BOOLEAN NOT NULL DEFAULT FALSE,
    deleted BOOLEAN NOT NULL DEFAULT FALSE,
    in_reply_to BIGINT REFERENCES private_message ON DELETE SET NULL,
    created TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    federation_sent_at TIMESTAMPTZ,
    federation_received_at TIMESTAMPTZ,
    CHECK (author <> recipient),
    CHECK (content_text IS NOT NULL OR content_markdown IS NOT NULL OR content_html IS NOT NULL)
);

CREATE INDEX private_message_recipient_author_created_idx
    ON private_message (recipient, author, created DESC, id DESC);

CREATE INDEX private_message_author_recipient_created_idx
    ON private_message (author, recipient, created DESC, id DESC);

CREATE INDEX private_message_recipient_unread_idx
    ON private_message (recipient, created DESC)
    WHERE NOT local AND NOT deleted;

ALTER TABLE notification
    ADD COLUMN private_message BIGINT REFERENCES private_message ON DELETE CASCADE;

CREATE INDEX notification_private_message_idx
    ON notification (private_message)
    WHERE private_message IS NOT NULL;

COMMIT;

/* end of 20260618103000_private-messages/up.sql */
