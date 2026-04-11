"""
Intercom warmer — creates real conversations in an Intercom workspace
so the ingestion pipeline has actual data to pull.

Creates contacts, opens conversations, and adds message parts
simulating Fin AI and human agent interactions.
"""
from __future__ import annotations

import json
import logging
import random
import time
import uuid
import urllib.request
import urllib.error

from synthetic.warmers.base import BaseWarmer

logger = logging.getLogger(__name__)

INTERCOM_API_BASE = "https://api.intercom.io"

CUSTOMER_NAMES = [
    "Alex Rivera", "Jordan Chen", "Sam Patel", "Morgan Kim",
    "Taylor Brooks", "Casey Wong", "Jamie Foster", "Drew Martinez",
    "Riley Nguyen", "Quinn O'Brien", "Avery Shah", "Blake Thompson",
]

CONVERSATION_SCENARIOS = [
    # ── Fin auto-resolved (35%) ─────────────────────────────────────────
    {
        "weight": 20,
        "subject": "How do I reset my password?",
        "fin_resolved": True,
        "fin_response": "You can reset your password by going to Settings → Security → Reset Password. Would you like me to send a reset link?",
        "description": "Password reset — Fin auto-resolved",
    },
    {
        "weight": 15,
        "subject": "What are your pricing plans?",
        "fin_resolved": True,
        "fin_response": "We offer three plans: Starter ($49/mo), Growth ($99/mo), and Enterprise (custom). You can compare features at veratrace.ai/pricing.",
        "description": "Pricing inquiry — Fin auto-resolved from KB",
    },

    # ── Fin escalated → human (25%) ─────────────────────────────────────
    {
        "weight": 15,
        "subject": "I want to dispute my last invoice",
        "fin_resolved": False,
        "fin_response": "I understand you'd like to dispute an invoice. Let me connect you with our billing team who can help.",
        "human_response": "I've reviewed your account and applied a $50 credit. The updated invoice will be sent shortly.",
        "description": "Billing dispute — Fin escalated to human",
    },
    {
        "weight": 10,
        "subject": "Our integration stopped syncing data",
        "fin_resolved": False,
        "fin_response": "I'm sorry to hear about the sync issue. Let me escalate this to our technical team.",
        "human_response": "I found the issue — your API token expired. I've refreshed it and the sync should resume within 15 minutes.",
        "description": "Technical issue — Fin escalated",
    },

    # ── Human only (15%) ────────────────────────────────────────────────
    {
        "weight": 8,
        "subject": "We need a custom SLA for our enterprise deployment",
        "fin_resolved": False,
        "human_response": "I'd be happy to discuss custom SLA terms. Let me set up a call with our enterprise team.",
        "description": "Enterprise SLA — human only, no Fin",
    },
    {
        "weight": 7,
        "subject": "Requesting compliance documentation for SOC 2 audit",
        "fin_resolved": False,
        "human_response": "I'll prepare the compliance documentation package. Our security team will have it ready within 24 hours.",
        "description": "Compliance request — human only",
    },

    # ── Multi-message Fin (10%) ─────────────────────────────────────────
    {
        "weight": 10,
        "subject": "How do I set up SSO with Okta?",
        "fin_resolved": True,
        "fin_response": "Great question! Here's how to set up SSO with Okta:\n\n1. Go to Settings → Security → SSO\n2. Select Okta as your provider\n3. Enter your Okta domain\n\nWould you like more details on any step?",
        "description": "SSO setup — multi-step Fin resolution",
    },

    # ── Fin resolved, low CSAT (10%) ────────────────────────────────────
    {
        "weight": 10,
        "subject": "Why was I charged twice this month?",
        "fin_resolved": True,
        "fin_response": "I can see the duplicate charge was due to a payment processing delay. The second charge will be automatically refunded within 3-5 business days.",
        "low_csat": True,
        "description": "Duplicate charge — Fin resolved but customer unhappy",
    },

    # ── Vendor reconciliation (5%) ──────────────────────────────────────
    {
        "weight": 5,
        "subject": "Checking on outsourced support ticket resolution",
        "fin_resolved": True,
        "fin_response": "I've resolved your ticket by providing the requested documentation.",
        "vendor_claim": True,
        "description": "BPO vendor — Fin claims resolved, needs verification",
    },
]

_SCENARIO_WEIGHTS = [s["weight"] for s in CONVERSATION_SCENARIOS]


class IntercomWarmer(BaseWarmer):
    """Creates real conversations in an Intercom workspace."""

    def __init__(self, credentials, external_identity):
        super().__init__(credentials, external_identity)
        self._access_token = credentials.get("accessToken", credentials.get("access_token", ""))
        self._admin_id = None
        self._contact_ids = []

    def _api_get(self, path):
        url = f"{INTERCOM_API_BASE}{path}"
        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Bearer {self._access_token}")
        req.add_header("Accept", "application/json")
        req.add_header("Intercom-Version", "2.11")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())

    def _api_post(self, path, body):
        url = f"{INTERCOM_API_BASE}{path}"
        data = json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Authorization", f"Bearer {self._access_token}")
        req.add_header("Content-Type", "application/json")
        req.add_header("Accept", "application/json")
        req.add_header("Intercom-Version", "2.11")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())

    def validate_access(self):
        try:
            result = self._api_get("/me")
            self._admin_id = str(result.get("id", ""))
            logger.info("Intercom access validated — admin=%s", result.get("name", "?"))

            # Get or create a contact for conversations
            contacts = self._api_get("/contacts?per_page=5")
            contact_list = contacts.get("data", [])
            if contact_list:
                self._contact_ids = [c["id"] for c in contact_list[:5]]
            else:
                # Create a test contact
                contact = self._api_post("/contacts", {
                    "role": "user",
                    "email": f"test-{uuid.uuid4().hex[:6]}@veratrace-warming.com",
                    "name": random.choice(CUSTOMER_NAMES),
                })
                self._contact_ids = [contact["id"]]

            return True
        except Exception as e:
            logger.error("Intercom access failed: %s", e)
            return False

    def create_activity(self, scenario_config):
        scenario = random.choices(CONVERSATION_SCENARIOS, weights=_SCENARIO_WEIGHTS, k=1)[0]
        customer_name = random.choice(CUSTOMER_NAMES)
        contact_id = random.choice(self._contact_ids) if self._contact_ids else None

        if not contact_id:
            raise RuntimeError("No contacts available")

        # Create conversation (customer sends initial message)
        try:
            conv = self._api_post("/conversations", {
                "from": {"type": "user", "id": contact_id},
                "body": scenario["subject"],
            })
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            raise RuntimeError(f"Create conversation failed: {body[:200]}")

        conv_id = conv.get("conversation_id", conv.get("id", ""))

        # Add Fin response if applicable
        if scenario.get("fin_response") and self._admin_id:
            try:
                time.sleep(0.5)
                self._api_post(f"/conversations/{conv_id}/reply", {
                    "message_type": "comment",
                    "type": "admin",
                    "admin_id": self._admin_id,
                    "body": scenario["fin_response"],
                })
            except Exception as e:
                logger.debug("Fin reply failed (non-fatal): %s", str(e)[:80])

        # Add human response if escalated
        if scenario.get("human_response") and self._admin_id:
            try:
                time.sleep(0.5)
                self._api_post(f"/conversations/{conv_id}/reply", {
                    "message_type": "comment",
                    "type": "admin",
                    "admin_id": self._admin_id,
                    "body": scenario["human_response"],
                })
            except Exception as e:
                logger.debug("Human reply failed (non-fatal): %s", str(e)[:80])

        # Close the conversation
        if scenario.get("fin_resolved") or scenario.get("human_response"):
            try:
                time.sleep(0.5)
                self._api_post(f"/conversations/{conv_id}/parts", {
                    "message_type": "close",
                    "type": "admin",
                    "admin_id": self._admin_id,
                    "body": "Resolved.",
                })
            except Exception as e:
                logger.debug("Close failed (non-fatal): %s", str(e)[:80])

        return {
            "id": conv_id,
            "type": "conversation",
            "customer": customer_name,
            "scenario": scenario["description"],
        }

    def verify_activity(self, activity_id):
        try:
            result = self._api_get(f"/conversations/{activity_id}")
            return bool(result.get("id"))
        except Exception:
            return False


WARMER_ID = "intercom"
WARMER_CLASS = IntercomWarmer
