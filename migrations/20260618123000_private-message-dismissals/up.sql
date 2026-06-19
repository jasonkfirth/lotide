/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: 20260618123000_private-message-dismissals/up.sql

    Purpose:

        Let a local user hide a private-message conversation from the mailbox
        list without deleting the messages themselves.

    Responsibilities:

        - store one dismissal timestamp per local user and conversation partner
        - let newer messages revive a dismissed conversation automatically

    This migration intentionally does NOT contain:

        - deletion of private messages
        - notification retention policy
        - ActivityPub delivery changes
*/

BEGIN;

CREATE TABLE IF NOT EXISTS private_message_conversation_dismissal (
    owner BIGINT NOT NULL REFERENCES person ON DELETE CASCADE,
    partner BIGINT NOT NULL REFERENCES person ON DELETE CASCADE,
    dismissed_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    PRIMARY KEY (owner, partner),
    CHECK (owner <> partner)
);

CREATE INDEX IF NOT EXISTS private_message_conversation_dismissal_partner_idx
    ON private_message_conversation_dismissal (partner);

COMMIT;

/* end of 20260618123000_private-message-dismissals/up.sql */
