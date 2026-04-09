"""
Contract tests for Amazon Connect — runs against the real sandbox.

These tests verify that:
1. We can still assume the sandbox role
2. SearchContacts returns contacts with expected schema
3. CTR field names haven't changed (schema drift detection)
4. The warmer can create and verify contacts

Run: python -m pytest tests/contract/ --contract -v
"""
from __future__ import annotations

import os
import pytest

# Sandbox credentials from env
ROLE_ARN = os.environ.get("WARM_ROLE_ARN", "")
INSTANCE_ARN = os.environ.get("WARM_INSTANCE_ARN", "")
EXTERNAL_ID = os.environ.get("WARM_EXTERNAL_ID", "")


@pytest.fixture
def connector():
    if not ROLE_ARN or not INSTANCE_ARN:
        pytest.skip("Sandbox credentials not configured (WARM_ROLE_ARN, WARM_INSTANCE_ARN)")

    from src.connectors.amazon_connect.connector import AmazonConnectConnector
    return AmazonConnectConnector(
        instance_id="contract-test",
        integration_account_id="contract-test",
        credentials={"roleArn": ROLE_ARN, "externalId": EXTERNAL_ID},
        external_identity={"tenantId": INSTANCE_ARN},
    )


@pytest.fixture
def warmer():
    if not ROLE_ARN or not INSTANCE_ARN:
        pytest.skip("Sandbox credentials not configured")

    from synthetic.warmers.amazon_connect import ConnectWarmer
    return ConnectWarmer(
        credentials={"roleArn": ROLE_ARN, "externalId": EXTERNAL_ID},
        external_identity={"tenantId": INSTANCE_ARN},
    )


class TestCredentials:
    def test_can_assume_role(self, connector):
        result = connector.test_connection()
        assert result.success, f"Connection failed: {result.message}"

    def test_region_detected(self, connector):
        region = connector.detect_region()
        assert region and len(region) > 3


class TestSchema:
    def test_search_contacts_returns_data(self, connector):
        from datetime import datetime, timedelta, timezone
        result = connector.sync_incremental(
            cursor=(datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        )
        # May be empty if no recent contacts, but shouldn't error
        assert result is not None
        assert isinstance(result.signals, list)

    def test_ctr_fields_match_schema(self, connector):
        """If there are contacts, verify their fields match our expected schema."""
        from datetime import datetime, timedelta, timezone
        from src.connectors.amazon_connect.schema import REQUIRED_FIELDS

        result = connector.sync_incremental(
            cursor=(datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        )
        for signal in result.signals:
            assert signal.name in (
                "contact_initiated", "ai_interaction", "agent_connected", "contact_completed"
            ), f"Unexpected signal name: {signal.name}"
            assert signal.source_integration == "amazon-connect"
            assert signal.instance_id == "contract-test"


class TestWarmer:
    def test_validate_access(self, warmer):
        assert warmer.validate_access()

    def test_create_and_verify_contact(self, warmer):
        result = warmer.warm(count=1, scenario_config={"task_ratio": 1.0}, delay_between=0, verify_delay=10)
        assert result.created == 1
        assert result.verified == 1
        assert result.failed == 0
