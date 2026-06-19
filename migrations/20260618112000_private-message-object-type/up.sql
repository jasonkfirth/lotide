/*
    Project: Lotide private message federation
    -------------------------------------------

    File: 20260618112000_private-message-object-type/up.sql

    Purpose:

        Remember the ActivityPub object family used for a private message.

    Responsibilities:

        - record whether a DM is a Note or ChatMessage
        - let local replies mirror the remote conversation shape

    This file intentionally does NOT contain:

        - message routing logic
        - ActivityPub serialization code
*/

ALTER TABLE private_message
    ADD COLUMN ap_object_type TEXT NOT NULL DEFAULT 'Note';

UPDATE private_message
SET ap_object_type='Note'
WHERE ap_object_type IS NULL OR ap_object_type='';

/* end of 20260618112000_private-message-object-type/up.sql */
