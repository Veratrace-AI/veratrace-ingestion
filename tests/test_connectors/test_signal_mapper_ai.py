"""
Tests for AI attribution extraction from Amazon Connect CTRs.

Covers:
- Lex bot resolved contact → ai_interaction signal
- Lex bot transferred to human → ai_interaction + agent_connected
- Contact Lens analytics → sentiment, categories on contact_completed
- Structured attribute parsing
- No Lex data → no ai_interaction signal (backward compatible)
"""
import json
import os
import pytest
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.connectors.amazon_connect.signal_mapper import ctr_to_signals

FIXTURES_DIR = os.path.join(
    os.path.dirname(__file__), "../../src/connectors/amazon_connect/test_fixtures"
)


def _load_fixture(name):
    with open(os.path.join(FIXTURES_DIR, name)) as f:
        return json.load(f)


class TestLexBotResolved:
    """CTR where Lex bot fully resolved the contact — no human agent."""

    @pytest.fixture
    def ctr(self):
        return _load_fixture("sample_ctr_lex_resolved.json")

    def test_produces_ai_interaction_signal(self, ctr):
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        ai_signals = [s for s in signals if s.name == "ai_interaction"]
        assert len(ai_signals) == 1

    def test_ai_signal_has_bot_name(self, ctr):
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        ai = [s for s in signals if s.name == "ai_interaction"][0]
        assert ai.payload["bot_name"] == "VeratraceSandboxBot"

    def test_ai_signal_has_intent_and_confidence(self, ctr):
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        ai = [s for s in signals if s.name == "ai_interaction"][0]
        assert ai.payload["intent"] == "ResetPassword"
        assert ai.payload["confidence"] == 0.94

    def test_resolved_by_bot_is_true(self, ctr):
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        ai = [s for s in signals if s.name == "ai_interaction"][0]
        assert ai.payload["resolved_by_bot"] is True

    def test_not_transferred_to_human(self, ctr):
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        ai = [s for s in signals if s.name == "ai_interaction"][0]
        assert ai.payload["transferred_to_human"] is False

    def test_no_agent_connected_signal(self, ctr):
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        agent_signals = [s for s in signals if s.name == "agent_connected"]
        assert len(agent_signals) == 0

    def test_contact_completed_resolved_by_ai(self, ctr):
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        completed = [s for s in signals if s.name == "contact_completed"][0]
        assert completed.payload["resolved_by"] == "AI"

    def test_ai_signal_type_is_ai(self, ctr):
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        ai = [s for s in signals if s.name == "ai_interaction"][0]
        assert ai.type == "AI"
        assert ai.actor_type == "AI"


class TestLexBotTransferred:
    """CTR where Lex bot tried but transferred to human agent."""

    @pytest.fixture
    def ctr(self):
        return _load_fixture("sample_ctr_lex_transferred.json")

    def test_produces_both_ai_and_agent_signals(self, ctr):
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        names = [s.name for s in signals]
        assert "ai_interaction" in names
        assert "agent_connected" in names

    def test_ai_signal_shows_low_confidence(self, ctr):
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        ai = [s for s in signals if s.name == "ai_interaction"][0]
        assert ai.payload["confidence"] == 0.38
        assert ai.payload["intent"] == "BillingInquiry"

    def test_resolved_by_bot_is_false(self, ctr):
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        ai = [s for s in signals if s.name == "ai_interaction"][0]
        assert ai.payload["resolved_by_bot"] is False

    def test_transferred_to_human_is_true(self, ctr):
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        ai = [s for s in signals if s.name == "ai_interaction"][0]
        assert ai.payload["transferred_to_human"] is True

    def test_contact_completed_resolved_by_human(self, ctr):
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        completed = [s for s in signals if s.name == "contact_completed"][0]
        assert completed.payload["resolved_by"] == "HUMAN"

    def test_slot_to_elicit_is_present(self, ctr):
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        ai = [s for s in signals if s.name == "ai_interaction"][0]
        assert ai.payload["slot_to_elicit"] == "dispute_amount"


class TestContactLens:
    """CTR with Contact Lens analytics (sentiment, categories)."""

    @pytest.fixture
    def ctr(self):
        return _load_fixture("sample_ctr_contact_lens.json")

    def test_contact_completed_has_contact_lens(self, ctr):
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        completed = [s for s in signals if s.name == "contact_completed"][0]
        assert "contact_lens" in completed.payload

    def test_sentiment_extracted(self, ctr):
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        lens = [s for s in signals if s.name == "contact_completed"][0].payload["contact_lens"]
        assert lens["overall_sentiment"] == "MIXED"
        assert lens["customer_sentiment"]["start"] == "NEGATIVE"
        assert lens["customer_sentiment"]["end"] == "POSITIVE"

    def test_categories_extracted(self, ctr):
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        lens = [s for s in signals if s.name == "contact_completed"][0].payload["contact_lens"]
        assert "Technical Issue" in lens["categories"]
        assert "Service Disruption" in lens["categories"]

    def test_transcript_turns_counted(self, ctr):
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        lens = [s for s in signals if s.name == "contact_completed"][0].payload["contact_lens"]
        assert lens["transcript_turns"] == 2

    def test_no_ai_interaction_without_lex(self, ctr):
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        ai_signals = [s for s in signals if s.name == "ai_interaction"]
        assert len(ai_signals) == 0


class TestStructuredAttributes:
    """Contact attributes are parsed into structured fields."""

    def test_known_keys_extracted(self):
        ctr = {
            "ContactId": "attr-test-001", "Channel": "CHAT",
            "InitiationTimestamp": "2026-04-09T10:00:00Z",
            "DisconnectTimestamp": "2026-04-09T10:05:00Z",
            "Attributes": {"customerSegment": "enterprise", "priority": "high", "randomKey": "randomValue"},
        }
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        initiated = [s for s in signals if s.name == "contact_initiated"][0]
        assert initiated.payload["customerSegment"] == "enterprise"
        assert initiated.payload["priority"] == "high"

    def test_unknown_keys_in_custom_attributes(self):
        ctr = {
            "ContactId": "attr-test-002", "Channel": "VOICE",
            "InitiationTimestamp": "2026-04-09T10:00:00Z",
            "DisconnectTimestamp": "2026-04-09T10:05:00Z",
            "Attributes": {"unknownKey": "unknownValue"},
        }
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        initiated = [s for s in signals if s.name == "contact_initiated"][0]
        assert initiated.payload["custom_attributes"]["unknownKey"] == "unknownValue"


class TestBackwardCompatibility:
    """CTR without Lex or Contact Lens still produces the original 3 signals."""

    def test_basic_ctr_produces_3_signals(self):
        ctr = {
            "ContactId": "basic-001", "Channel": "VOICE",
            "InitiationMethod": "INBOUND",
            "InitiationTimestamp": "2026-04-09T10:00:00Z",
            "ConnectedToSystemTimestamp": "2026-04-09T10:00:05Z",
            "DisconnectTimestamp": "2026-04-09T10:07:30Z",
            "DisconnectReason": "CUSTOMER_DISCONNECT",
            "Agent": {"ARN": "arn:aws:connect:us-east-1:123:instance/abc/agent/agent-001", "Username": "jsmith"},
            "Queue": {"Name": "GeneralSupport", "Duration": 4},
            "Attributes": {},
        }
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        names = [s.name for s in signals]
        assert names == ["contact_initiated", "agent_connected", "contact_completed"]

    def test_no_contact_lens_when_not_completed(self):
        ctr = {
            "ContactId": "no-lens-001", "Channel": "VOICE",
            "InitiationTimestamp": "2026-04-09T10:00:00Z",
            "DisconnectTimestamp": "2026-04-09T10:05:00Z",
            "AnalysisStatus": "IN_PROGRESS",
        }
        signals = ctr_to_signals(ctr, "inst-1", "acc-1")
        completed = [s for s in signals if s.name == "contact_completed"][0]
        assert "contact_lens" not in completed.payload
