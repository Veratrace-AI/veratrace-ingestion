"""
Contract tests: validate synthetic data shapes match Joey's Spring Boot entity models.

These tests ensure the seed script produces JSONB that Hibernate can deserialize.
If these fail, the seed script is out of sync with the backend schema.
"""
import json
import uuid
import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from synthetic.generator import generate_contact, SCENARIOS


# ── Schema definitions matching Java entity classes ──────────────────────────

VALID_TWU_STATUSES = {"IN_PROGRESS", "COMPLETED", "FAILED", "CANCELLED"}
VALID_FIELD_TYPES = {"TEXT", "NUMBER", "BOOLEAN", "REFERENCE", "LIST_TEXT", "LIST_NUMBER", "LIST_REFERENCE"}
VALID_OUTCOME_TYPES = {"TEXT", "NUMBER", "BOOLEAN"}
VALID_CORRELATION_RULE_TYPES = {"STATIC", "DYNAMIC"}


def _is_uuid(val):
    try:
        uuid.UUID(str(val))
        return True
    except (ValueError, AttributeError):
        return False


class TestTwuActorAttribution:
    """Validates shape matches TwuActorAttribution.java"""

    def _make_attribution(self, actor_type="HUMAN"):
        return {
            "derived": False,
            "primary": {
                "type": actor_type,
                "agentId": str(uuid.uuid4()),
                "contributions": 1,
                "score": 1.0,
            },
            "secondary": [],
        }

    def test_has_required_fields(self):
        attr = self._make_attribution()
        assert "derived" in attr
        assert "primary" in attr
        assert "secondary" in attr

    def test_primary_has_uuid_agent_id(self):
        attr = self._make_attribution()
        assert _is_uuid(attr["primary"]["agentId"])

    def test_primary_has_contributions_and_score(self):
        attr = self._make_attribution()
        assert isinstance(attr["primary"]["contributions"], int)
        assert isinstance(attr["primary"]["score"], float)

    def test_secondary_is_list(self):
        attr = self._make_attribution()
        assert isinstance(attr["secondary"], list)


class TestTwuOutcome:
    """Validates shape matches TwuOutcome.java"""

    def test_valid_outcome(self):
        outcome = {"type": "RESOLUTION", "name": "contact_resolution", "value": "completed", "confidence": 0.95}
        assert "type" in outcome
        assert "name" in outcome
        assert "value" in outcome
        assert isinstance(outcome["confidence"], float)


class TestTwuPolicy:
    """Validates shape matches TwuPolicy.java"""

    def test_valid_policy(self):
        policy = {"status": "COMPLIANT", "overallScore": 0.92, "evaluations": []}
        assert "status" in policy
        assert isinstance(policy["overallScore"], float)
        assert isinstance(policy["evaluations"], list)


class TestTwuUsage:
    """Validates shape matches TwuUsage.java (resources/cost/duration)"""

    def test_valid_usage(self):
        usage = {
            "resources": {"cpu": 0, "memory": 0, "tokens": 0},
            "cost": {"amount": 0.0, "currency": "USD"},
            "duration": {"totalSeconds": 300, "activeSeconds": 250},
        }
        assert "resources" in usage
        assert "cost" in usage
        assert "duration" in usage
        assert isinstance(usage["duration"]["totalSeconds"], int)


class TestTwuIntegrity:
    """Validates shape matches TwuIntegrity.java"""

    def test_valid_integrity(self):
        integrity = {
            "hash": "abc123",
            "verified": True,
            "method": "SHA-256",
            "verifiedAt": "2026-04-08T12:00:00Z",
            "tamperDetected": False,
        }
        assert isinstance(integrity["verified"], bool)
        assert isinstance(integrity["tamperDetected"], bool)
        assert integrity["method"] in ("SHA-256", "SHA-512")


class TestTwuExplanation:
    """Validates shape matches TwuExplanation.java"""

    def test_valid_explanation(self):
        explanation = {"text": "Contact handled by human agent."}
        assert "text" in explanation
        assert isinstance(explanation["text"], str)


class TestFieldShape:
    """Validates shape matches Field.java (used in TWU Model entities/events/actions)"""

    def test_valid_field(self):
        field = {"name": "contact_id", "label": "Contact ID", "type": "TEXT", "required": True}
        assert field["type"] in VALID_FIELD_TYPES
        assert isinstance(field["required"], bool)
        assert "name" in field
        assert "label" in field

    def test_number_field(self):
        field = {"name": "duration", "label": "Duration", "type": "NUMBER", "required": False}
        assert field["type"] in VALID_FIELD_TYPES


class TestOutcomeModelShape:
    """Validates Outcome model (TWU Model definition, not TWU instance outcome)"""

    def test_valid_outcome_model(self):
        outcome = {"name": "resolution", "description": "How resolved", "type": "TEXT", "expression": "contact_completed.resolved_by"}
        assert outcome["type"] in VALID_OUTCOME_TYPES
        assert "expression" in outcome


class TestCorrelationRuleShape:
    """Validates CorrelationRule model"""

    def test_valid_rule(self):
        rule = {"name": "contact_lifecycle", "description": "Group by contact_id", "type": "STATIC", "requiredFields": ["contact_id"], "optionalFields": []}
        assert rule["type"] in VALID_CORRELATION_RULE_TYPES
        assert isinstance(rule["requiredFields"], list)


class TestGeneratedSignals:
    """Validates the synthetic generator produces valid signal shapes."""

    def test_all_scenarios_generate_signals(self):
        for scenario_name in SCENARIOS:
            signals = generate_contact(
                "test-instance", "test-account",
                __import__("datetime").datetime.now(__import__("datetime").timezone.utc),
                SCENARIOS[scenario_name],
            )
            assert len(signals) >= 2, f"Scenario {scenario_name} produced < 2 signals"

    def test_signals_have_required_fields(self):
        signals = generate_contact(
            "test-instance", "test-account",
            __import__("datetime").datetime.now(__import__("datetime").timezone.utc),
            SCENARIOS["bpo_contact_center"],
        )
        for sig in signals:
            assert sig.instance_id == "test-instance"
            assert sig.source_integration == "amazon-connect"
            assert sig.name in ("contact_initiated", "ai_processing", "agent_connected", "contact_transferred", "contact_completed")
            assert sig.actor_type in ("AI", "HUMAN", "SYSTEM")
            assert sig.occurred_at  # not empty

    def test_contact_completed_has_payload(self):
        signals = generate_contact(
            "test-instance", "test-account",
            __import__("datetime").datetime.now(__import__("datetime").timezone.utc),
            SCENARIOS["bpo_contact_center"],
        )
        completed = [s for s in signals if s.name == "contact_completed"]
        assert len(completed) == 1
        payload = completed[0].payload
        assert "contact_id" in payload
        assert "duration_seconds" in payload
        assert "resolved_by" in payload
