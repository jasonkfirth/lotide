/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260605224500_host-interaction-probes/up.sql

    Purpose:

        Track empirical host-level interaction probes for discovered
        ActivityPub group servers.

    Responsibilities:

        - remember when Lotide last tested a host with a signed Like/Undo
        - remember when that host last accepted the full probe
        - store the latest probe error for diagnosis and suppression decisions

    This file intentionally does NOT contain:

        - hard-coded host block lists
        - post or community deletion
        - task scheduling policy
*/

ALTER TABLE community_discovery_server
    ADD COLUMN interaction_probe_checked_at timestamp with time zone,
    ADD COLUMN interaction_probe_success_at timestamp with time zone,
    ADD COLUMN interaction_probe_latest_error text;

/* end of migrations/20260605224500_host-interaction-probes/up.sql */
