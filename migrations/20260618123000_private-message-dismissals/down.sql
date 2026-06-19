/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: 20260618123000_private-message-dismissals/down.sql

    Purpose:

        Remove private-message conversation dismissal state.

    Responsibilities:

        - drop dismissal indexes
        - drop dismissal rows

    This migration intentionally does NOT contain:

        - private-message deletion
        - notification cleanup
        - restoration of dismissed mailbox state
*/

BEGIN;

DROP INDEX IF EXISTS private_message_conversation_dismissal_partner_idx;

DROP TABLE IF EXISTS private_message_conversation_dismissal;

COMMIT;

/* end of 20260618123000_private-message-dismissals/down.sql */
