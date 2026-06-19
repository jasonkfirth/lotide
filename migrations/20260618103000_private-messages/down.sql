/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: 20260618103000_private-messages/down.sql

    Purpose:

        Remove the direct-message storage added by the matching up migration.

    Responsibilities:

        - remove notification linkage for private messages
        - drop private-message indexes and table

    This migration intentionally does NOT contain:

        - data export
        - reversible message preservation
        - ActivityPub cleanup delivery
*/

BEGIN;

DROP INDEX IF EXISTS notification_private_message_idx;

ALTER TABLE notification
    DROP COLUMN IF EXISTS private_message;

DROP INDEX IF EXISTS private_message_recipient_unread_idx;
DROP INDEX IF EXISTS private_message_author_recipient_created_idx;
DROP INDEX IF EXISTS private_message_recipient_author_created_idx;

DROP TABLE IF EXISTS private_message;

COMMIT;

/* end of 20260618103000_private-messages/down.sql */
