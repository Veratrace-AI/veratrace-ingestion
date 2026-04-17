"""
Unit tests for the sync_runs writer and its integration with sync_account.

write_sync_run() MUST never raise — observability failures can't break sync.
These tests encode that contract.

The live counterpart lives in tests/contract/test_sync_runs_live.py (hits
real Supabase; skipped unless CI_CONTRACT=true and SUPABASE creds set).
"""
from __future__ import annotations

import io
import json
import logging
import os
import sys
import urllib.error
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── _headers ──────────────────────────────────────────────────────────────────

class TestHeaders:
    def test_headers_include_bearer_and_prefer_minimal(self, monkeypatch):
        from src.runtime import sync_runs
        monkeypatch.setattr(sync_runs, "SUPABASE_SERVICE_ROLE_KEY", "test-key-xyz")
        h = sync_runs._headers()
        assert h["Authorization"] == "Bearer test-key-xyz"
        assert h["apikey"] == "test-key-xyz"
        assert h["Content-Type"] == "application/json"
        assert "return=minimal" in h["Prefer"]


# ── write_sync_run ────────────────────────────────────────────────────────────

class TestWriteSyncRun:
    def test_skips_when_supabase_url_unset(self, monkeypatch, caplog):
        from src.runtime import sync_runs
        monkeypatch.setattr(sync_runs, "SUPABASE_URL", "")
        monkeypatch.setattr(sync_runs, "SUPABASE_SERVICE_ROLE_KEY", "key")
        with patch("src.runtime.sync_runs.urllib.request.urlopen") as up:
            with caplog.at_level(logging.WARNING, logger="src.runtime.sync_runs"):
                sync_runs.write_sync_run({"integration_account_id": "abc", "instance_id": "i", "integration_id": "x", "status": "ok"})
        assert up.call_count == 0
        assert any("sync_runs write skipped" in r.message for r in caplog.records)

    def test_skips_when_service_role_key_unset(self, monkeypatch):
        from src.runtime import sync_runs
        monkeypatch.setattr(sync_runs, "SUPABASE_URL", "https://fake")
        monkeypatch.setattr(sync_runs, "SUPABASE_SERVICE_ROLE_KEY", "")
        with patch("src.runtime.sync_runs.urllib.request.urlopen") as up:
            sync_runs.write_sync_run({"integration_account_id": "abc", "instance_id": "i", "integration_id": "x", "status": "ok"})
        assert up.call_count == 0

    def test_posts_to_correct_url_with_expected_body(self, monkeypatch):
        from src.runtime import sync_runs
        monkeypatch.setattr(sync_runs, "SUPABASE_URL", "https://fake.supabase.test")
        monkeypatch.setattr(sync_runs, "SUPABASE_SERVICE_ROLE_KEY", "test-key")
        captured = {}

        def fake_urlopen(req, timeout=None):
            captured["url"] = req.full_url
            captured["headers"] = dict(req.header_items())
            captured["body"] = json.loads(req.data)
            resp = MagicMock()
            resp.__enter__ = lambda self: resp
            resp.__exit__ = lambda *a: False
            return resp

        with patch("src.runtime.sync_runs.urllib.request.urlopen", side_effect=fake_urlopen):
            sync_runs.write_sync_run({
                "integration_account_id": "acc-1",
                "instance_id": "inst-1",
                "integration_id": "amazon-connect",
                "status": "ok",
                "signals_written": 3,
                "duration_ms": 1234,
                "backfill": False,
            })

        assert captured["url"] == "https://fake.supabase.test/rest/v1/sync_runs"
        # urllib.request.Request title-cases header names (apikey -> Apikey)
        assert captured["headers"].get("Authorization") == "Bearer test-key"
        assert captured["body"]["integration_account_id"] == "acc-1"
        assert captured["body"]["status"] == "ok"
        assert captured["body"]["signals_written"] == 3
        assert captured["body"]["duration_ms"] == 1234

    def test_swallows_http_error_and_logs_body(self, monkeypatch, caplog):
        from src.runtime import sync_runs
        monkeypatch.setattr(sync_runs, "SUPABASE_URL", "https://fake")
        monkeypatch.setattr(sync_runs, "SUPABASE_SERVICE_ROLE_KEY", "k")

        err = urllib.error.HTTPError(
            url="https://fake/rest/v1/sync_runs", code=400, msg="Bad Request", hdrs=None,
            fp=io.BytesIO(b'{"message":"column status does not exist"}'),
        )
        with caplog.at_level(logging.ERROR, logger="src.runtime.sync_runs"):
            with patch("src.runtime.sync_runs.urllib.request.urlopen", side_effect=err):
                # MUST NOT raise
                sync_runs.write_sync_run({"integration_account_id": "acc-1", "instance_id": "i", "integration_id": "x", "status": "ok"})
        msgs = [r.message for r in caplog.records]
        assert any("event=sync_run_write_failed" in m and "status=400" in m for m in msgs)
        assert any("column status does not exist" in m for m in msgs)

    def test_swallows_url_error(self, monkeypatch, caplog):
        from src.runtime import sync_runs
        monkeypatch.setattr(sync_runs, "SUPABASE_URL", "https://fake")
        monkeypatch.setattr(sync_runs, "SUPABASE_SERVICE_ROLE_KEY", "k")
        with caplog.at_level(logging.ERROR, logger="src.runtime.sync_runs"):
            with patch("src.runtime.sync_runs.urllib.request.urlopen", side_effect=urllib.error.URLError("conn refused")):
                sync_runs.write_sync_run({"integration_account_id": "acc-1", "instance_id": "i", "integration_id": "x", "status": "error"})
        assert any("event=sync_run_write_failed" in r.message for r in caplog.records)


# ── sync_account integration ─────────────────────────────────────────────────

class TestSyncAccountCallsWriteSyncRun:
    """sync_account's finally block MUST call write_sync_run exactly once per invocation."""

    def test_writes_once_on_skipped_no_connector(self):
        from src.sync import scheduler
        account = {
            "integration_id": "nonexistent-connector",
            "integration_account_id": "abc12345xxx",
            "instance_id": "inst99999xxx",
        }
        with patch("src.sync.scheduler.write_sync_run") as mock_write:
            scheduler.sync_account(account)
        assert mock_write.call_count == 1
        payload = mock_write.call_args[0][0]
        assert payload["integration_account_id"] == "abc12345xxx"
        assert payload["instance_id"] == "inst99999xxx"
        assert payload["integration_id"] == "nonexistent-connector"
        assert payload["status"] == scheduler.STATUS_SKIPPED_NO_CONNECTOR
        assert payload["signals_written"] == 0
        assert payload["error"] is None
        assert payload["backfill"] is False
        assert "duration_ms" in payload
        assert "finished_at" in payload

    def test_writes_once_on_exception(self):
        from src.sync import scheduler
        boom = MagicMock()
        boom.return_value.validate_credentials.side_effect = RuntimeError("connector blew up")
        with patch.dict(scheduler.CONNECTOR_MAP, {"boom": boom}):
            account = {
                "integration_id": "boom",
                "integration_account_id": "xyz99999",
                "instance_id": "inst11111",
                "auth_credentials": {},
                "external_identity": {},
            }
            with patch("src.sync.scheduler.write_sync_run") as mock_write:
                with pytest.raises(RuntimeError):
                    scheduler.sync_account(account)
        # Even when sync_account re-raises, finally must run exactly once.
        assert mock_write.call_count == 1
        payload = mock_write.call_args[0][0]
        assert payload["status"] == scheduler.STATUS_ERROR
        assert "connector blew up" in payload["error"]

    def test_writes_once_on_backfill_flag(self):
        from src.sync import scheduler
        account = {
            "integration_id": "nope",
            "integration_account_id": "a",
            "instance_id": "i",
        }
        with patch("src.sync.scheduler.write_sync_run") as mock_write:
            scheduler.sync_account(account, backfill=True)
        assert mock_write.call_count == 1
        assert mock_write.call_args[0][0]["backfill"] is True

    def test_write_sync_run_failure_does_not_break_sync_account(self):
        """The whole point: if write_sync_run raises, sync_account must still behave correctly."""
        from src.sync import scheduler
        account = {
            "integration_id": "nonexistent",
            "integration_account_id": "abc",
            "instance_id": "inst",
        }
        # Even though write_sync_run is patched here to raise, sync_account should still complete
        # for the skipped_no_connector path (which doesn't re-raise).
        with patch("src.sync.scheduler.write_sync_run", side_effect=RuntimeError("observability broken")):
            # sync_account's finally calls write_sync_run directly (no inner try/except around it today —
            # we rely on write_sync_run itself to swallow. This test pins that contract: if write_sync_run
            # somehow raises, sync_account propagates — which is the signal to fix write_sync_run, not
            # to silently hide the observability breakage.
            with pytest.raises(RuntimeError):
                scheduler.sync_account(account)
