/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260605220000_prune-cross-host-community-discovery/down.sql

    Purpose:

        Document that cross-host discovery pruning is not reversible.

    Responsibilities:

        - provide a no-op rollback for a data cleanup migration

    This file intentionally does NOT contain:

        - recreation of deleted empty community shells
        - reactivation of cross-host discovery rows
        - reactivation of below-threshold discovery rows
*/

/* This data cleanup is intentionally not reversible. */

/* end of migrations/20260605220000_prune-cross-host-community-discovery/down.sql */
