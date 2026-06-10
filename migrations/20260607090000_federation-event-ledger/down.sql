/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260607090000_federation-event-ledger/down.sql

    Purpose:

        Remove the compact federation decision ledger.

    Responsibilities:

        - drop indexes owned by federation_event
        - drop the federation_event table

    This file intentionally does NOT contain:

        - changes to task rows
        - changes to content federation status columns
        - changes to host discovery or suppression state
*/

BEGIN;
    DROP TABLE federation_event;
COMMIT;

/* end of migrations/20260607090000_federation-event-ledger/down.sql */
