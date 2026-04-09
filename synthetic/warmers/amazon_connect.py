"""
Amazon Connect warmer — creates real chat and task contacts
in a Connect sandbox instance so the ingestion pipeline has
actual CTRs to pull via SearchContacts.

Requires:
- IAM role with connect:StartChatContact, connect:StartTaskContact
- At least one published Contact Flow in the Connect instance
- Use the sandbox CloudFormation template (amazon-connect-sandbox.yaml)
"""
from __future__ import annotations

import logging
import random
import time
import uuid

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError

from synthetic.warmers.base import BaseWarmer

logger = logging.getLogger(__name__)

# Realistic customer names for varied contact data
CUSTOMER_NAMES = [
    "Alex Rivera", "Jordan Chen", "Sam Patel", "Morgan Kim",
    "Taylor Brooks", "Casey Wong", "Jamie Foster", "Drew Martinez",
    "Riley Nguyen", "Quinn O'Brien", "Avery Shah", "Blake Thompson",
    "Reese Nakamura", "Skyler Davis", "Finley Clark", "Parker Lee",
]

CUSTOMER_SEGMENTS = ["enterprise", "mid-market", "smb", "consumer"]
CONTACT_REASONS = [
    "billing_inquiry", "technical_support", "account_setup",
    "product_question", "service_cancellation", "upgrade_request",
    "outage_report", "feature_request", "compliance_question",
]
PRIORITIES = ["high", "medium", "medium", "low", "low"]


class ConnectWarmer(BaseWarmer):
    """Creates real contacts in an Amazon Connect instance."""

    def __init__(self, credentials: dict, external_identity: dict):
        super().__init__(credentials, external_identity)
        self._instance_arn = external_identity.get("tenantId", "")
        self._instance_id = self._instance_arn.split("/")[-1] if "/" in self._instance_arn else ""
        self._region = self._parse_region()
        self._contact_flow_id = None
        self._assumed_creds = None
        self._assumed_creds_expiry = 0

    def _parse_region(self):
        parts = self._instance_arn.split(":")
        return parts[3] if len(parts) > 3 else "us-east-1"

    def _assume_role(self):
        now = time.time()
        if self._assumed_creds and self._assumed_creds_expiry > now + 300:
            return self._assumed_creds

        role_arn = self.credentials.get("roleArn", "")
        external_id = self.credentials.get("externalId", "")

        sts = boto3.client("sts", region_name=self._region)
        params = {
            "RoleArn": role_arn,
            "RoleSessionName": f"veratrace-warmer-{uuid.uuid4().hex[:8]}",
            "DurationSeconds": 3600,
        }
        if external_id:
            params["ExternalId"] = external_id

        resp = sts.assume_role(**params)
        creds = resp["Credentials"]
        self._assumed_creds = {
            "aws_access_key_id": creds["AccessKeyId"],
            "aws_secret_access_key": creds["SecretAccessKey"],
            "aws_session_token": creds["SessionToken"],
        }
        self._assumed_creds_expiry = creds["Expiration"].timestamp()
        return self._assumed_creds

    def _get_client(self):
        creds = self._assume_role()
        return boto3.client(
            "connect",
            region_name=self._region,
            aws_access_key_id=creds["aws_access_key_id"],
            aws_secret_access_key=creds["aws_secret_access_key"],
            aws_session_token=creds["aws_session_token"],
            config=BotoConfig(
                retries={"max_attempts": 2, "mode": "adaptive"},
                connect_timeout=10,
                read_timeout=30,
            ),
        )

    def _discover_contact_flow(self, client):
        """Find a usable contact flow in the instance."""
        if self._contact_flow_id:
            return self._contact_flow_id

        try:
            resp = client.list_contact_flows(
                InstanceId=self._instance_id,
                ContactFlowTypes=["CONTACT_FLOW"],
            )
            flows = resp.get("ContactFlowSummaryList", [])
            # Prefer flows with "default" or "sample" in the name
            for flow in flows:
                name_lower = flow.get("Name", "").lower()
                if any(kw in name_lower for kw in ["default", "sample", "inbound", "basic"]):
                    self._contact_flow_id = flow["Id"]
                    logger.info("Using contact flow: %s (%s)", flow["Name"], flow["Id"][:12])
                    return self._contact_flow_id

            # Fall back to first available flow
            if flows:
                self._contact_flow_id = flows[0]["Id"]
                logger.info("Using first contact flow: %s (%s)", flows[0]["Name"], flows[0]["Id"][:12])
                return self._contact_flow_id

        except ClientError as e:
            logger.error("Failed to list contact flows: %s", e)

        return None

    def validate_access(self) -> bool:
        """Verify write permissions by listing contact flows."""
        try:
            client = self._get_client()
            flow_id = self._discover_contact_flow(client)
            if not flow_id:
                logger.error("No contact flows found — create one in the Connect console first")
                return False
            logger.info("Access validated: instance=%s, flow=%s", self._instance_id[:12], flow_id[:12])
            return True
        except ClientError as e:
            logger.error("Access validation failed: %s", e)
            return False

    def create_activity(self, scenario_config: dict) -> dict:
        """Create a chat or task contact in Connect."""
        client = self._get_client()
        flow_id = self._discover_contact_flow(client)
        if not flow_id:
            raise RuntimeError("No contact flow available")

        # Decide channel: 70% chat, 30% task
        use_task = random.random() < scenario_config.get("task_ratio", 0.3)
        customer_name = random.choice(CUSTOMER_NAMES)
        segment = random.choice(CUSTOMER_SEGMENTS)
        reason = random.choice(CONTACT_REASONS)
        priority = random.choice(PRIORITIES)

        attributes = {
            "customerSegment": segment,
            "contactReason": reason,
            "priority": priority,
            "source": "veratrace-warmer",
            "caseId": f"WARM-{uuid.uuid4().hex[:8].upper()}",
        }

        if use_task:
            resp = client.start_task_contact(
                InstanceId=self._instance_id,
                ContactFlowId=flow_id,
                Name=f"Test task: {reason.replace('_', ' ')} ({customer_name})",
                Description=f"Automated warming task for {segment} customer",
                Attributes=attributes,
            )
            contact_id = resp["ContactId"]
            return {"id": contact_id, "type": "TASK", "customer": customer_name}
        else:
            resp = client.start_chat_contact(
                InstanceId=self._instance_id,
                ContactFlowId=flow_id,
                ParticipantDetails={"DisplayName": customer_name},
                Attributes=attributes,
            )
            contact_id = resp["ContactId"]
            return {"id": contact_id, "type": "CHAT", "customer": customer_name}

    def verify_activity(self, activity_id: str) -> bool:
        """Check if the contact produced a CTR visible in SearchContacts."""
        client = self._get_client()
        try:
            resp = client.describe_contact(
                InstanceId=self._instance_id,
                ContactId=activity_id,
            )
            contact = resp.get("Contact", {})
            has_initiation = bool(contact.get("InitiationTimestamp"))
            return has_initiation
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                return False
            raise
