/*
    Project: Lotide private message federation
    -------------------------------------------

    File: 20260618112000_private-message-object-type/down.sql

    Purpose:

        Remove the private-message object family metadata.

    Responsibilities:

        - drop the ap_object_type column from private_message

    This file intentionally does NOT contain:

        - message data deletion
        - notification cleanup
*/

ALTER TABLE private_message
    DROP COLUMN IF EXISTS ap_object_type;

/* end of 20260618112000_private-message-object-type/down.sql */
