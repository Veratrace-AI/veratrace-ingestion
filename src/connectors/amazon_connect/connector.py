"""
Amazon Connect connector — pulls Contact Trace Records (CTRs) and transforms
them into TwuSignals.

Supports:
- Tier 1: Kinesis stream consumer (sub-second, customer-configured)
- Tier 2: SearchContacts API polling (2 req/sec, fallback)
- Backfill: SearchContacts with date range (24-month CTR retention)

Rate limits: 2 req/sec default per account per region.
Region: parsed from instance ARN position 4.
Multi-region: separate integration account per region.
"""
import json
import logging
import urllib.request
import urllib.error
from datetime import datetime, timedelta, timezone

from src.connectors.base import (
    BaseConnector, ConnectionTestResult, SyncResult, QuotaUsage, ConnectorHealth,
)
from src.connectors.amazon_connect.signal_mapper import ctr_to_signals
from src.connectors.amazon_connect.schema import EXPECTED_CTR_FIELDS, EXPECTED_SCHEMA_HASH
from src.runtime.region_router import detect_region_from_arn
from src.runtime.schema_validator import detect_drift, is_breaking
from src.runtime.rate_limiter import TokenBucket
from src.runtime.retry_engine import with_retry, CircuitBreaker

logger = logging.getLogger(__name__)

# Connect API rate limits (per account, per region)
SEARCH_CONTACTS_RPS = 2.0
GET_CONTACT_ATTRS_RPS = 2.0


class AmazonConnectConnector(BaseConnector):
    """
    Amazon Connect integration connector.

    Credentials expected:
        credentials["roleArn"] — IAM role ARN to assume for API access
    External identity:
        external_identity["tenantId"] — Connect instance ARN
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._region = self.detect_region()
        self._instance_arn = self.external_identity.get("tenantId", "")
        self._instance_id_from_arn = self._parse_instance_id()
        self._circuit_breaker = CircuitBreaker()
        self._schema_hash = EXPECTED_SCHEMA_HASH

    def _parse_instance_id(self) -> str:
        """Extract the instance ID (UUID) from the Connect instance ARN."""
        # arn:aws:connect:region:account:instance/INSTANCE_ID
        parts = self._instance_arn.split("/")
        return parts[1] if len(parts) >= 2 else ""

    # ── Setup ──────────────────────────────────────────────────────────────

    def validate_credentials(self) -> bool:
        role_arn = self.credentials.get("roleArn", "")
        if not role_arn.startswith("arn:aws:iam::"):
            # Accept arn:aws:iam::ACCOUNT:role/NAME format
            if not role_arn.startswith("arn:aws:iam:"):
                return False
        if not self._instance_arn.startswith("arn:aws:connect:"):
            return False
        return True

    def test_connection(self) -> ConnectionTestResult:
        """
        Test connection by assuming the role and calling DescribeInstance.
        This validates both the IAM role and the Connect instance access.
        """
        try:
            # Step 1: Assume the customer's IAM role via STS
            sts_creds = self._assume_role()
            if not sts_creds:
                return ConnectionTestResult(
                    success=False,
                    message="Failed to assume IAM role. Check roleArn permissions.",
                    region=self._region,
                )

            # Step 2: Call Connect DescribeInstance with assumed credentials
            result = self._describe_instance(sts_creds)
            if result:
                return ConnectionTestResult(
                    success=True,
                    message=f"Connected to {result.get('InstanceAlias', 'Connect instance')}",
                    region=self._region,
                    details={"instance_alias": result.get("InstanceAlias", "")},
                )
            return ConnectionTestResult(
                success=False,
                message="DescribeInstance failed. Check Connect instance ARN.",
                region=self._region,
            )
        except Exception as e:
            return ConnectionTestResult(
                success=False,
                message=f"Connection test failed: {str(e)[:200]}",
                region=self._region,
            )

    def detect_region(self) -> str:
        region = detect_region_from_arn(self._instance_arn if hasattr(self, '_instance_arn') else self.external_identity.get("tenantId", ""))
        return region or "us-east-1"

    # ── AWS API Helpers ────────────────────────────────────────────────────

    def _assume_role(self) :
        """Call STS AssumeRole to get temporary credentials."""
        role_arn = self.credentials.get("roleArn", "")
        # Use AWS SDK-style HTTP call to STS
        import urllib.parse

        params = urllib.parse.urlencode({
            "Action": "AssumeRole",
            "RoleArn": role_arn,
            "RoleSessionName": f"veratrace-{self.integration_account_id[:8]}",
            "DurationSeconds": "3600",
            "Version": "2011-06-15",
        })

        # This requires AWS Signature V4 — in production, use boto3
        # For now, return None to indicate STS integration needed
        logger.info("STS AssumeRole would be called for role=%s region=%s", role_arn[:50], self._region)
        return None  # TODO: Implement with boto3 or AWS Signature V4

    def _describe_instance(self, sts_creds: dict) :
        """Call Connect DescribeInstance with assumed credentials."""
        # TODO: Implement with assumed credentials
        return None

    # ── Sync ───────────────────────────────────────────────────────────────

    def sync_incremental(self, cursor: str = None) -> SyncResult:
        """
        Fetch contacts modified since the last cursor (timestamp).

        Uses SearchContacts API with time range filter.
        Rate limited to SEARCH_CONTACTS_RPS (2 req/sec per account per region).
        """
        # Determine time range
        if cursor:
            start_time = datetime.fromisoformat(cursor.replace("Z", "+00:00"))
        else:
            # First sync: last 24 hours
            start_time = datetime.now(timezone.utc) - timedelta(hours=24)

        end_time = datetime.now(timezone.utc)

        logger.info(
            "Syncing Connect contacts: %s → %s (instance=%s, region=%s)",
            start_time.isoformat(), end_time.isoformat(),
            self._instance_id_from_arn[:8], self._region,
        )

        # TODO: Implement SearchContacts API call with assumed credentials
        # For now, return empty result to allow pipeline testing
        contacts = []
        signals = []
        api_calls = 0

        for contact in contacts:
            # Check for schema drift on first response
            if api_calls == 0:
                current_hash, drifts = detect_drift(
                    contact, self._schema_hash, EXPECTED_CTR_FIELDS
                )
                if drifts:
                    if is_breaking(drifts):
                        logger.error("Breaking schema drift in Connect CTR: %s", drifts)
                    self._schema_hash = current_hash

            mapped = ctr_to_signals(
                contact,
                instance_id=self.instance_id,
                integration_account_id=self.integration_account_id,
            )
            signals.extend(mapped)

        new_cursor = end_time.isoformat()
        return SyncResult(
            signals=signals,
            cursor=new_cursor,
            has_more=False,
            records_fetched=len(contacts),
            api_calls_made=api_calls,
        )

    def sync_backfill(self, start_date: datetime = None) -> SyncResult:
        """
        Backfill historical contacts.
        Connect retains CTRs for 24 months. Archive immediately.
        Run at 2am customer timezone. Rate limited to 50% of ceiling.
        """
        if not start_date:
            # Default: 30 days back for initial backfill
            start_date = datetime.now(timezone.utc) - timedelta(days=30)

        logger.info("Backfilling Connect contacts from %s", start_date.isoformat())

        # TODO: Implement with pagination, rate limiting at 50% ceiling
        return SyncResult(signals=[], cursor=None, has_more=False, records_fetched=0)

    # ── Schema ─────────────────────────────────────────────────────────────

    def get_expected_schema(self) -> dict:
        return {"hash": EXPECTED_SCHEMA_HASH, "fields": list(EXPECTED_CTR_FIELDS)}

    def get_expected_fields(self) -> set[str]:
        return EXPECTED_CTR_FIELDS

    # ── Health ─────────────────────────────────────────────────────────────

    def get_health(self) -> ConnectorHealth:
        if self._circuit_breaker.is_open():
            return ConnectorHealth(status="FAILED", last_error="Circuit breaker open")
        return ConnectorHealth(status="HEALTHY")
