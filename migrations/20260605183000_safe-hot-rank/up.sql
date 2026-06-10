/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260605183000_safe-hot-rank/up.sql

    Purpose:

        Replace the hot ranking function with a defensive version that cannot
        fail on malformed remote timestamps.

    Responsibilities:

        - keep hot sorting compatible with the existing post and comment feeds
        - clamp invalid score and age inputs before fractional exponent math

    This file intentionally does NOT contain:

        - feed query changes
        - data cleanup for future-dated remote posts
        - application-side ranking logic
*/

CREATE OR REPLACE FUNCTION hot_rank(score BIGINT, created TIMESTAMPTZ) RETURNS FLOAT AS $$
    DECLARE
        age_seconds DOUBLE PRECISION;
        rank_score DOUBLE PRECISION;
    BEGIN
        /*
            Remote servers can send bad timestamps.  The original formula used
            the raw age in a fractional exponent, so a future timestamp caused
            PostgreSQL to attempt a complex-number result and abort the whole
            feed query.  Lotide should treat bad ranking inputs as low-trust
            data, not as a reason to return HTTP 500.
        */
        age_seconds := GREATEST(ABS(EXTRACT(EPOCH FROM current_timestamp) - EXTRACT(EPOCH FROM created)), 1.0);
        rank_score := GREATEST((score + 1)::DOUBLE PRECISION, 0.0);

        RETURN 1000000.0 * rank_score / (age_seconds ^ 1.8);
    END;
$$ LANGUAGE plpgsql;

/* end of migrations/20260605183000_safe-hot-rank/up.sql */
