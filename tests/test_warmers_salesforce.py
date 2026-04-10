"""
Tests for Salesforce warmer: scenarios, distribution, create/verify.
"""
from __future__ import annotations

import os
import sys
import pytest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from synthetic.warmers.salesforce import SalesforceWarmer, CASE_SCENARIOS, _SCENARIO_WEIGHTS


class TestSalesforceWarmerScenarios:

    def test_scenarios_not_empty(self):
        assert len(CASE_SCENARIOS) >= 10

    def test_weights_sum_to_100(self):
        assert sum(_SCENARIO_WEIGHTS) == 100

    def test_all_scenarios_have_required_fields(self):
        for s in CASE_SCENARIOS:
            assert "weight" in s
            assert "subject" in s
            assert "origin" in s
            assert "priority" in s
            assert "status" in s
            assert "description" in s

    def test_distribution_matches_enterprise_pattern(self):
        ai_resolved = sum(s["weight"] for s in CASE_SCENARIOS if s.get("ai_handled") == "true" and s.get("ai_confidence", 0) > 0.8)
        ai_triage = sum(s["weight"] for s in CASE_SCENARIOS if s.get("ai_handled") == "true" and s.get("ai_confidence", 0) <= 0.8)
        human_only = sum(s["weight"] for s in CASE_SCENARIOS if s.get("ai_handled") != "true")

        assert ai_resolved >= 30, f"AI auto-resolved {ai_resolved}% too low"
        assert human_only >= 15, f"Human-only {human_only}% too low"

    def test_scenarios_cover_vendor_reconciliation(self):
        vendor = [s for s in CASE_SCENARIOS if "vendor" in s["subject"].lower() or "bpo" in s["subject"].lower()]
        assert len(vendor) >= 2, "Need at least 2 vendor reconciliation scenarios"


class TestSalesforceWarmerMethods:

    def _make_warmer(self):
        return SalesforceWarmer(
            credentials={"access_token": "test-token", "instance_url": "https://test.salesforce.com"},
            external_identity={"tenantId": "orgid"},
        )

    @patch.object(SalesforceWarmer, "_api_get")
    def test_validate_access_succeeds(self, mock_get):
        mock_get.return_value = {"totalSize": 26}
        warmer = self._make_warmer()
        assert warmer.validate_access() is True

    @patch.object(SalesforceWarmer, "_api_get")
    def test_validate_access_fails_on_error(self, mock_get):
        mock_get.side_effect = Exception("401 Unauthorized")
        warmer = self._make_warmer()
        assert warmer.validate_access() is False

    @patch.object(SalesforceWarmer, "_api_post")
    def test_create_activity_returns_case_id(self, mock_post):
        mock_post.return_value = {"id": "5003V000005ABC", "success": True}
        warmer = self._make_warmer()
        result = warmer.create_activity({})
        assert result["id"] == "5003V000005ABC"
        assert result["type"] == "Case"

    @patch.object(SalesforceWarmer, "_api_get")
    def test_verify_activity_true_when_found(self, mock_get):
        mock_get.return_value = {"Id": "5003V000005ABC", "Subject": "Test"}
        warmer = self._make_warmer()
        assert warmer.verify_activity("5003V000005ABC") is True

    @patch.object(SalesforceWarmer, "_api_get")
    def test_verify_activity_false_when_not_found(self, mock_get):
        mock_get.side_effect = Exception("404")
        warmer = self._make_warmer()
        assert warmer.verify_activity("nonexistent") is False


class TestSalesforceWarmerAutoDiscovery:

    def test_warmer_auto_discovered(self):
        from synthetic.warmers import WARMERS
        assert "salesforce" in WARMERS
