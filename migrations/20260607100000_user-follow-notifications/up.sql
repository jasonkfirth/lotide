/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: 20260607100000_user-follow-notifications/up.sql

    Purpose:

        Let local users see a normal notification when a remote actor follows
        their Lotide user account.

    Responsibilities:

        - store the actor that caused a notification
        - keep notification rows from blocking later remote-person cleanup

    This migration intentionally does NOT contain:

        - notification delivery logic
        - ActivityPub follow handling
        - frontend rendering
*/

ALTER TABLE notification
    ADD COLUMN from_user BIGINT REFERENCES person ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS notification_from_user_idx
    ON notification (from_user)
    WHERE from_user IS NOT NULL;

/* end of 20260607100000_user-follow-notifications/up.sql */
