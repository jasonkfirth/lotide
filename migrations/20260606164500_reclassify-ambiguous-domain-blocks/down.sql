/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260606164500_reclassify-ambiguous-domain-blocks/down.sql

    Purpose:

        Document that ambiguous domain-block reclassification is not
        reversible.

    Responsibilities:

        - provide a no-op rollback for a diagnostic cleanup migration

    This file intentionally does NOT contain:

        - recreation of stale community or user suppressions
        - recreation of hard-coded host suppressions
        - deactivation of community discovery rows
*/

/* This data cleanup is intentionally not reversible. */

/* end of migrations/20260606164500_reclassify-ambiguous-domain-blocks/down.sql */
