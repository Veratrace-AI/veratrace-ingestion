"""
Tests for Amazon Connect connector — signal mapping, schema validation, region routing.
"""
import json
import pathlib
import pytest

# Adjust import path for test environment
import sys
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent.parent))

from src.connectors.amazon_connect.signal_mapper import ctr_to_signals
from src.connectors.amazon_connect.schema import EXPECTED_CTR_FIELDS, REQUIRED_FIELDS
from src.runtime.region_router import detect_region_from_arn
from src.runtime.schema_validator import compute_schema_hash, detect_drift


FIXTURE_DIR = pathlib.Path(__file__).parent.parent.parent / "src" / "connectors" / "amazon_connect" / "test_fixtures"


def load_fixture(name: str) -> dict:
    with open(FIXTURE_DIR / name) as f:
        return json.load(f)


class TestSignalMapper:
    """Test CTR → TwuSignal transformation."""

    def test_produces_three_signals_for_agent_handled_call(self):
        ctr = load_fixture("sample_ctr.json")
        signals = ctr_to_signals(ctr, instance_id="test-inst", integration_account_id="test-acct")
        assert len(signals) == 3
        assert signals[0].name == "contact_initiated"
        assert signals[1].name == "agent_connected"
        assert signals[2].name == "contact_completed"

    def test_produces_two_signals_for_ivr_only_call(self):
        ctr = load_fixture("sample_ctr.json")
        del ctr["Agent"]  # No agent involved
        ctr["ConnectedToSystemTimestamp"] = ""
        signals = ctr_to_signals(ctr, instance_id="test-inst", integration_account_id="test-acct")
        assert len(signals) == 2
        assert signals[0].name == "contact_initiated"
        assert signals[1].name == "contact_completed"

    def test_signals_have_correct_instance_id(self):
        ctr = load_fixture("sample_ctr.json")
        signals = ctr_to_signals(ctr, instance_id="my-instance", integration_account_id="my-acct")
        for s in signals:
            assert s.instance_id == "my-instance"
            assert s.source_integration_account_id == "my-acct"
            assert s.source_integration == "amazon-connect"

    def test_agent_signal_has_human_actor_type(self):
        ctr = load_fixture("sample_ctr.json")
        signals = ctr_to_signals(ctr, instance_id="test", integration_account_id="test")
        agent_signal = [s for s in signals if s.name == "agent_connected"][0]
        assert agent_signal.actor_type == "HUMAN"
        assert "agent-001" in agent_signal.actor_agent_id

    def test_completed_signal_has_duration(self):
        ctr = load_fixture("sample_ctr.json")
        signals = ctr_to_signals(ctr, instance_id="test", integration_account_id="test")
        completed = [s for s in signals if s.name == "contact_completed"][0]
        assert completed.payload["duration_seconds"] == 450  # 7m30s

    def test_raw_ctr_preserved_in_last_signal(self):
        ctr = load_fixture("sample_ctr.json")
        signals = ctr_to_signals(ctr, instance_id="test", integration_account_id="test")
        last = signals[-1]
        assert "_raw_ctr" in last.payload
        assert last.payload["_raw_ctr"]["ContactId"] == ctr["ContactId"]

    def test_pii_fields_flagged(self):
        ctr = load_fixture("sample_ctr.json")
        signals = ctr_to_signals(ctr, instance_id="test", integration_account_id="test")
        initiated = signals[0]
        assert "customer_endpoint" in initiated.pii_encrypted_fields

    def test_missing_required_fields_produces_degraded_signals(self):
        ctr = load_fixture("sample_ctr.json")
        del ctr["Channel"]  # Required field
        signals = ctr_to_signals(ctr, instance_id="test", integration_account_id="test")
        assert all(s.degraded for s in signals)
        assert "Channel" in signals[0].degraded_reason

    def test_missing_optional_fields_not_degraded(self):
        ctr = load_fixture("sample_ctr.json")
        del ctr["Recording"]  # Optional field
        del ctr["SentimentAnalysis"]
        signals = ctr_to_signals(ctr, instance_id="test", integration_account_id="test")
        assert not any(s.degraded for s in signals)


class TestRegionRouter:
    """Test region detection from Connect ARNs."""

    def test_us_east_1_from_arn(self):
        arn = "arn:aws:connect:us-east-1:123456789012:instance/abc123"
        assert detect_region_from_arn(arn) == "us-east-1"

    def test_eu_west_2_from_arn(self):
        arn = "arn:aws:connect:eu-west-2:123456789012:instance/abc123"
        assert detect_region_from_arn(arn) == "eu-west-2"

    def test_ap_southeast_from_arn(self):
        arn = "arn:aws:connect:ap-southeast-1:123456789012:instance/abc123"
        assert detect_region_from_arn(arn) == "ap-southeast-1"

    def test_invalid_arn_returns_none(self):
        assert detect_region_from_arn("not-an-arn") is None
        assert detect_region_from_arn("") is None


class TestSchemaValidation:
    """Test CTR schema drift detection."""

    def test_no_drift_on_expected_schema(self):
        ctr = load_fixture("sample_ctr.json")
        hash1 = compute_schema_hash(ctr)
        _, drifts = detect_drift(ctr, hash1, set(ctr.keys()))
        assert len(drifts) == 0

    def test_detects_removed_field(self):
        ctr = load_fixture("sample_ctr.json")
        expected_fields = set(ctr.keys())
        original_hash = compute_schema_hash(ctr)
        del ctr["Recording"]
        _, drifts = detect_drift(ctr, original_hash, expected_fields)
        removed = [d for d in drifts if d.severity == "removed"]
        assert len(removed) >= 1
        assert any(d.field == "Recording" for d in removed)

    def test_detects_added_field(self):
        ctr = load_fixture("sample_ctr.json")
        expected_fields = set(ctr.keys())
        original_hash = compute_schema_hash(ctr)
        ctr["NewField"] = "some value"
        _, drifts = detect_drift(ctr, original_hash, expected_fields)
        added = [d for d in drifts if d.severity == "added"]
        assert len(added) >= 1

    def test_expected_ctr_fields_covers_sample(self):
        ctr = load_fixture("sample_ctr.json")
        ctr_fields = set(ctr.keys())
        # All sample fields should be in our expected set
        missing = ctr_fields - EXPECTED_CTR_FIELDS
        assert not missing, f"Sample CTR has fields not in EXPECTED_CTR_FIELDS: {missing}"
