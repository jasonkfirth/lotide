/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260605214500_known-defederated-host-suppression/up.sql

    Purpose:

        Preserve known host-level federation blocks discovered during live
        interoperability testing.

    Responsibilities:

        - mark tested domain-blocked hosts as suppressed for discovery
        - deactivate already discovered communities from those hosts

    This file intentionally does NOT contain:

        - suppression of slow or ambiguous hosts
        - suppression of lemmy.linuxuserspace.show
        - deletion of followed communities or posts
*/

INSERT INTO community_discovery_server
    (host, active, last_checked, latest_error, suppressed_reason, suppressed_at)
VALUES
    (
        'programming.dev',
        TRUE,
        current_timestamp,
        'Known domain block: programming.dev rejected community inbox delivery from the local Lotide host during live compatibility testing.',
        'Known domain block: programming.dev rejected community inbox delivery from the local Lotide host during live compatibility testing.',
        current_timestamp
    ),
    (
        'lemmy.blahaj.zone',
        TRUE,
        current_timestamp,
        'Known domain block: lemmy.blahaj.zone rejected Like delivery from the local Lotide host during live compatibility testing.',
        'Known domain block: lemmy.blahaj.zone rejected Like delivery from the local Lotide host during live compatibility testing.',
        current_timestamp
    ),
    (
        'lemmy.dbzer0.com',
        TRUE,
        current_timestamp,
        'Known domain block: lemmy.dbzer0.com rejected Like delivery from the local Lotide host during live compatibility testing.',
        'Known domain block: lemmy.dbzer0.com rejected Like delivery from the local Lotide host during live compatibility testing.',
        current_timestamp
    )
ON CONFLICT (host) DO UPDATE SET
    active=TRUE,
    last_checked=current_timestamp,
    latest_error=EXCLUDED.latest_error,
    suppressed_reason=EXCLUDED.suppressed_reason,
    suppressed_at=current_timestamp;

UPDATE community_discovery
SET active=FALSE
WHERE host IN ('programming.dev', 'lemmy.blahaj.zone', 'lemmy.dbzer0.com');

/* end of migrations/20260605214500_known-defederated-host-suppression/up.sql */
