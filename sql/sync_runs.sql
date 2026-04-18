-- sync_runs: structured telemetry for every sync_account() invocation.
--
-- One row per run. Written from src/runtime/sync_runs.py, called in the
-- finally block of src/sync/scheduler.py::sync_account. Failure to write
-- a row is logged and swallowed — observability must never break sync.
--
-- Status values are the STATUS_* constants in src/sync/scheduler.py:
--   ok | skipped_no_connector | invalid_credentials | no_new_signals | error
-- Treated as free-form TEXT rather than ENUM so scheduler.py remains the
-- source of truth for vocabulary.
--
-- Applied manually via Supabase service-role key or Studio. This file is
-- the canonical schema for recreate/audit.

CREATE TABLE IF NOT EXISTS sync_runs (
    run_id                 UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    integration_account_id TEXT         NOT NULL,
    instance_id            TEXT         NOT NULL,
    integration_id         TEXT         NOT NULL,
    started_at             TIMESTAMPTZ  NOT NULL DEFAULT now(),
    finished_at            TIMESTAMPTZ,
    status                 TEXT         NOT NULL,
    signals_written        INTEGER      NOT NULL DEFAULT 0,
    duration_ms            INTEGER,
    error                  TEXT,
    backfill               BOOLEAN      NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS sync_runs_account_idx  ON sync_runs (integration_account_id, started_at DESC);
CREATE INDEX IF NOT EXISTS sync_runs_instance_idx ON sync_runs (instance_id,            started_at DESC);
CREATE INDEX IF NOT EXISTS sync_runs_status_idx   ON sync_runs (status,                 started_at DESC);
