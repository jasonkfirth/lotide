/*
    Project: Lotide Database Migrations
    -----------------------------------

    File: migrations/20260607090000_federation-event-ledger/up.sql

    Purpose:

        Add a compact federation decision ledger.

    Responsibilities:

        - record important inbound and outbound ActivityPub decisions
        - keep enough metadata to debug delivery and ingest status
        - support admin health views without storing full remote payloads

    This file intentionally does NOT contain:

        - raw ActivityPub payload archival
        - retry or queue scheduling policy
        - user-visible moderation decisions
*/

BEGIN;
    CREATE TABLE federation_event (
        id BIGSERIAL PRIMARY KEY,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT current_timestamp,
        direction TEXT NOT NULL,
        action TEXT NOT NULL,
        status TEXT NOT NULL,
        host TEXT,
        software TEXT,
        actor_ap_id TEXT,
        object_ap_id TEXT,
        target_ap_id TEXT,
        activity_type TEXT,
        task_id BIGINT REFERENCES task ON DELETE SET NULL,
        task_kind TEXT,
        error_class TEXT,
        error_text TEXT,

        CHECK (direction IN ('inbound', 'outbound', 'internal')),
        CHECK (status IN (
            'queued',
            'sent',
            'received',
            'accepted',
            'verified',
            'ingested',
            'rejected',
            'failed',
            'skipped'
        ))
    );

    CREATE INDEX federation_event_created_idx
        ON federation_event(created_at DESC, id DESC);

    CREATE INDEX federation_event_host_created_idx
        ON federation_event(host, created_at DESC, id DESC)
        WHERE host IS NOT NULL;

    CREATE INDEX federation_event_action_status_created_idx
        ON federation_event(action, status, created_at DESC, id DESC);

    CREATE INDEX federation_event_task_idx
        ON federation_event(task_kind, task_id)
        WHERE task_kind IS NOT NULL OR task_id IS NOT NULL;
COMMIT;

/* end of migrations/20260607090000_federation-event-ledger/up.sql */
