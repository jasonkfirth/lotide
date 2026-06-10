/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: 20260607100000_user-follow-notifications/down.sql

    Purpose:

        Remove the optional notification actor link added for user follow
        notifications.

    Responsibilities:

        - drop the supporting index
        - drop the `notification.from_user` column

    This migration intentionally does NOT contain:

        - changes to follow rows
        - changes to person rows
*/

DROP INDEX IF EXISTS notification_from_user_idx;

ALTER TABLE notification
    DROP COLUMN from_user;

/* end of 20260607100000_user-follow-notifications/down.sql */
