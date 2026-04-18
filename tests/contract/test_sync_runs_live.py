"""
Live contract test — proves sync_runs writes land in Supabase and come back
with the shape we wrote. Skipped unless CI_CONTRACT=true AND SUPABASE creds set.

Requires: the `sync_runs` table created in Supabase (see sql/sync_runs.sql).
If the table doesn't exist, this test fails loudly with a 404 — which is the
correct behavior (forces the operator to apply the migration).

Run locally:
  SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=... CI_CONTRACT=true \\
    python -m pytest tests/contract/test_sync_runs_live.py --contract -v
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))


SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "")


pytestmark = pytest.mark.skipif(
    not (SUPABASE_URL and SUPABASE_KEY),
    reason="SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY required for live sync_runs contract tests",
)


def _headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }


def _delete_row(run_id):
    req = urllib.request.Request(
        f"{SUPABASE_URL}/rest/v1/sync_runs?run_id=eq.{run_id}",
        headers=_headers(),
        method="DELETE",
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass  # cleanup failure is non-fatal for the test


class TestSyncRunsLive:
    def test_round_trip_insert_and_select(self):
        """Insert a run, SELECT it back, verify shape matches, then delete."""
        from src.runtime.sync_runs import write_sync_run

        test_account = f"contract-test-{uuid.uuid4().hex[:8]}"
        payload = {
            "integration_account_id": test_account,
            "instance_id": "contract-test-instance",
            "integration_id": "amazon-connect",
            "status": "ok",
            "signals_written": 42,
            "duration_ms": 1337,
            "error": None,
            "backfill": False,
        }

        # Write
        write_sync_run(payload)

        # Supabase is strongly consistent on same-connection writes, but give it a tick
        time.sleep(0.5)

        # SELECT back
        req = urllib.request.Request(
            f"{SUPABASE_URL}/rest/v1/sync_runs?integration_account_id=eq.{test_account}&select=*",
            headers=_headers(),
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                rows = json.loads(r.read())
        except urllib.error.HTTPError as e:
            pytest.fail(
                f"Could not SELECT from sync_runs ({e.code}). "
                f"Has sql/sync_runs.sql been applied to Supabase? Body: {e.read()[:200]!r}"
            )

        assert len(rows) == 1, f"Expected exactly 1 row, got {len(rows)}: {rows}"
        row = rows[0]
        assert row["integration_account_id"] == test_account
        assert row["instance_id"] == "contract-test-instance"
        assert row["integration_id"] == "amazon-connect"
        assert row["status"] == "ok"
        assert row["signals_written"] == 42
        assert row["duration_ms"] == 1337
        assert row["error"] is None
        assert row["backfill"] is False
        assert row["run_id"]  # Supabase-generated UUID
        assert row["started_at"]  # DB default now()

        # Cleanup
        _delete_row(row["run_id"])

    def test_error_status_round_trip(self):
        """Error payload with status=error and error message persists correctly."""
        from src.runtime.sync_runs import write_sync_run

        test_account = f"contract-test-err-{uuid.uuid4().hex[:8]}"
        write_sync_run({
            "integration_account_id": test_account,
            "instance_id": "contract-test-instance",
            "integration_id": "salesforce",
            "status": "error",
            "signals_written": 0,
            "duration_ms": 200,
            "error": "HTTP 401 Unauthorized",
            "backfill": False,
        })
        time.sleep(0.5)

        req = urllib.request.Request(
            f"{SUPABASE_URL}/rest/v1/sync_runs?integration_account_id=eq.{test_account}&select=*",
            headers=_headers(),
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            rows = json.loads(r.read())

        assert len(rows) == 1
        assert rows[0]["status"] == "error"
        assert rows[0]["error"] == "HTTP 401 Unauthorized"

        _delete_row(rows[0]["run_id"])
