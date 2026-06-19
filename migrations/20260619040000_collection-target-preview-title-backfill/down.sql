--
-- Project: Lotide database migrations
-- -----------------------------------
--
-- File: down.sql
--
-- Purpose:
--
--     Document that the source preview title backfill is intentionally not
--     reversible.
--
-- Responsibilities:
--
--     - preserve manually improved or freshly fetched preview titles
--
-- This file intentionally does NOT contain:
--
--     - logic to restore old [no title] rows
--     - source preview deletion
--

BEGIN;
COMMIT;

/* end of down.sql */
