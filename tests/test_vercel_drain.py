"""
Tests for POST /vercel-drain — the ingestion endpoint that receives Vercel
Web Analytics NDJSON drain payloads and writes to public.web_events.

What this guards:
  - Bad/missing X-Drain-Secret returns 401, never insert
  - Invalid NDJSON lines are counted in `rejected` but never insert
  - Valid events insert into Supabase via service-role REST
  - The /vercel-drain path skips the X-API-Key gate (Vercel can't send that
    header on drains, since the destination is unaware of our internal key)
"""
from __future__ import annotations

import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from http.server import HTTPServer
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

os.environ.setdefault("INGESTION_API_KEY", "test-key-abc123")
os.environ.setdefault("SUPABASE_URL", "https://fake.supabase.test")
os.environ.setdefault("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
os.environ.setdefault("VERCEL_DRAIN_SECRET", "test-drain-secret-xyz")

from src.main import IngestionHandler  # noqa: E402
from src import main as main_module  # noqa: E402


@pytest.fixture(scope="module")
def server():
    srv = HTTPServer(("127.0.0.1", 0), IngestionHandler)
    port = srv.server_address[1]
    thread = threading.Thread(target=srv.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{port}"
    srv.shutdown()


@pytest.fixture(autouse=True)
def fake_env(monkeypatch):
    monkeypatch.setattr(main_module, "SUPABASE_URL", "https://fake.supabase.test")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "test-service-key")
    monkeypatch.setenv("VERCEL_DRAIN_SECRET", "test-drain-secret-xyz")
    monkeypatch.setattr(main_module, "INGESTION_API_KEY", "test-key-abc123")
    yield


_REAL_URLOPEN = urllib.request.urlopen


class _FakeSupabaseInsert:
    """Context-manager response that records calls + returns 201."""
    def __init__(self):
        self.status = 201
        self._body = b""
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def read(self): return self._body


def _make_dispatcher(supabase_calls, insert_response=None):
    """Capture Supabase POSTs; pass localhost through."""
    def fake_urlopen(req, timeout=None, *args, **kwargs):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        if "127.0.0.1" in url or "localhost" in url:
            return _REAL_URLOPEN(req, timeout=timeout, *args, **kwargs)
        # Supabase REST
        body = req.data
        supabase_calls.append({"url": url, "body": body})
        return insert_response or _FakeSupabaseInsert()
    return fake_urlopen


def _ndjson(events):
    return ("\n".join(json.dumps(e) for e in events)).encode()


def _drain_request(server_url, body, secret="test-drain-secret-xyz"):
    req = urllib.request.Request(
        f"{server_url}/vercel-drain",
        data=body,
        method="POST",
    )
    if secret is not None:
        req.add_header("X-Drain-Secret", secret)
    req.add_header("Content-Type", "application/x-ndjson")
    return req


_PAGEVIEW = {
    "schema": "vercel.analytics.v2",
    "eventType": "pageview",
    "timestamp": 1782419000000,
    "projectId": "prj_uYl9wnrrmRoc178bZzWcNAJ5zDlp",
    "ownerId": "team_0Pf3T5RFsnhGpdNgqZW59Eos",
    "sessionId": 12345,
    "deviceId": 67890,
    "origin": "https://veratrace.ai",
    "path": "/blog/colorado-ai-act-enterprise-compliance-guide",
}


# ── Auth ──────────────────────────────────────────────────────────────────────


class TestDrainAuth:
    def test_missing_secret_header_is_200_verification_probe(self, server):
        """Vercel UI sends a verification POST without the X-Drain-Secret
        header before the drain config is saved. Endpoint must return 2xx so
        the UI's reachability check passes; nothing is inserted."""
        body = _ndjson([_PAGEVIEW])
        r = urllib.request.urlopen(_drain_request(server, body, secret=None), timeout=5)
        assert r.status == 200
        resp = json.loads(r.read())
        assert resp == {"verified": True, "accepted": 0}

    def test_wrong_secret_is_401(self, server):
        body = _ndjson([_PAGEVIEW])
        try:
            urllib.request.urlopen(_drain_request(server, body, secret="wrong"), timeout=5)
            assert False, "expected 401"
        except urllib.error.HTTPError as e:
            assert e.code == 401

    def test_no_x_api_key_required(self, server):
        """The drain endpoint must NOT require X-API-Key — Vercel can't send it."""
        body = _ndjson([_PAGEVIEW])
        calls = []
        with patch("src.main.urllib.request.urlopen",
                   side_effect=_make_dispatcher(calls)):
            r = urllib.request.urlopen(_drain_request(server, body), timeout=5)
            assert r.status == 200


# ── Parsing + insertion ──────────────────────────────────────────────────────


class TestDrainIngest:
    def test_valid_ndjson_inserts_to_supabase(self, server):
        events = [_PAGEVIEW, dict(_PAGEVIEW, sessionId=999, path="/blog/proving-ai-roi-board")]
        body = _ndjson(events)
        calls = []
        with patch("src.main.urllib.request.urlopen",
                   side_effect=_make_dispatcher(calls)):
            r = urllib.request.urlopen(_drain_request(server, body), timeout=5)
            assert r.status == 200
            resp = json.loads(r.read())
            assert resp == {"accepted": 2, "rejected": 0}
        # Exactly one Supabase POST containing both rows
        assert len(calls) == 1
        assert calls[0]["url"].endswith("/rest/v1/web_events")
        rows = json.loads(calls[0]["body"])
        assert len(rows) == 2
        assert rows[0]["path"] == "/blog/colorado-ai-act-enterprise-compliance-guide"
        assert rows[0]["event_type"] == "pageview"
        assert "ts" in rows[0] and rows[0]["ts"].startswith("2026-")

    def test_blank_lines_and_malformed_json_counted_as_rejected(self, server):
        body = (
            json.dumps(_PAGEVIEW).encode()
            + b"\n\n"
            + b"this is not json\n"
            + json.dumps(dict(_PAGEVIEW, sessionId=2)).encode()
        )
        calls = []
        with patch("src.main.urllib.request.urlopen",
                   side_effect=_make_dispatcher(calls)):
            r = urllib.request.urlopen(_drain_request(server, body), timeout=5)
            resp = json.loads(r.read())
            assert resp == {"accepted": 2, "rejected": 1}

    def test_event_with_no_valid_timestamp_is_rejected(self, server):
        events = [dict(_PAGEVIEW, timestamp="not-a-number")]
        body = _ndjson(events)
        calls = []
        with patch("src.main.urllib.request.urlopen",
                   side_effect=_make_dispatcher(calls)):
            r = urllib.request.urlopen(_drain_request(server, body), timeout=5)
            resp = json.loads(r.read())
            assert resp == {"accepted": 0, "rejected": 1}
        # Zero rows means we shouldn't even hit Supabase
        assert calls == []

    def test_empty_body_returns_400(self, server):
        req = urllib.request.Request(
            f"{server}/vercel-drain", data=b"", method="POST",
        )
        req.add_header("X-Drain-Secret", "test-drain-secret-xyz")
        req.add_header("Content-Length", "0")
        try:
            urllib.request.urlopen(req, timeout=5)
            assert False, "expected 400"
        except urllib.error.HTTPError as e:
            assert e.code == 400
