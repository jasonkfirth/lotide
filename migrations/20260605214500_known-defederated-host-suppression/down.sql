/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260605214500_known-defederated-host-suppression/down.sql

    Purpose:

        Remove host suppressions inserted by the known defederated host
        migration.

    Responsibilities:

        - clear only the exact known-domain-block suppressions from this
          migration

    This file intentionally does NOT contain:

        - reactivation of communities from those hosts
        - deletion of community discovery server rows
        - changes to suppressions recorded from later live failures
*/

UPDATE community_discovery_server
SET suppressed_reason=NULL,
    suppressed_at=NULL,
    latest_error=NULL
WHERE host IN ('programming.dev', 'lemmy.blahaj.zone', 'lemmy.dbzer0.com')
AND suppressed_reason LIKE 'Known domain block:%';

/* end of migrations/20260605214500_known-defederated-host-suppression/down.sql */
