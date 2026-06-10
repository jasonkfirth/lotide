/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260605183000_safe-hot-rank/down.sql

    Purpose:

        Restore the historical hot ranking function.

    Responsibilities:

        - provide a reversible migration for local development and testing

    This file intentionally does NOT contain:

        - any post or comment data changes
        - application-side ranking logic
*/

CREATE OR REPLACE FUNCTION hot_rank(score BIGINT, created TIMESTAMPTZ) RETURNS FLOAT AS $$
    BEGIN
        RETURN (1000000 * (score + 1) / ((EXTRACT(EPOCH FROM current_timestamp) - EXTRACT(EPOCH FROM created)) ^ 1.8));
    END;
$$ LANGUAGE plpgsql;

/* end of migrations/20260605183000_safe-hot-rank/down.sql */
