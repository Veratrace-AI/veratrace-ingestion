"""
Salesforce warmer — creates real Cases and Opportunities in a
Salesforce sandbox so the ingestion pipeline has actual data to pull.

Uses the Salesforce REST API to create records with AI/human
hybrid attribution metadata matching Veratrace use cases.

Requires: SF CLI auth (`sf org login web --alias sf-sandbox`)
or access_token + instance_url in credentials.
"""
from __future__ import annotations

import json
import logging
import random
import urllib.request
import urllib.error

from synthetic.warmers.base import BaseWarmer

logger = logging.getLogger(__name__)

SF_API_VERSION = "v60.0"

CUSTOMER_NAMES = [
    "Alex Rivera", "Jordan Chen", "Sam Patel", "Morgan Kim",
    "Taylor Brooks", "Casey Wong", "Jamie Foster", "Drew Martinez",
    "Riley Nguyen", "Quinn O'Brien", "Avery Shah", "Blake Thompson",
]

CASE_SCENARIOS = [
    # ── AI fully resolves (35%) ─────────────────────────────────────────
    {
        "weight": 20,
        "subject": "Password reset request",
        "origin": "Web",
        "priority": "Medium",
        "status": "Closed",
        "ai_handled": "true",
        "ai_agent": "CaseBot-v3",
        "ai_confidence": 0.95,
        "description": "Customer locked out — AI auto-resolved via identity verification.",
    },
    {
        "weight": 15,
        "subject": "Account balance inquiry",
        "origin": "Chat",
        "priority": "Low",
        "status": "Closed",
        "ai_handled": "true",
        "ai_agent": "InfoBot-v2",
        "ai_confidence": 0.92,
        "description": "Customer asked about current balance — AI retrieved and responded.",
    },

    # ── AI triage → human (25%) ─────────────────────────────────────────
    {
        "weight": 15,
        "subject": "Billing dispute — charge not recognized",
        "origin": "Phone",
        "priority": "High",
        "status": "Closed",
        "ai_handled": "true",
        "ai_agent": "TriageBot-v1",
        "ai_confidence": 0.38,
        "description": "AI categorized as billing dispute, routed to billing specialist.",
    },
    {
        "weight": 10,
        "subject": "Contract renewal negotiation",
        "origin": "Email",
        "priority": "High",
        "status": "Closed",
        "ai_handled": "true",
        "ai_agent": "TriageBot-v1",
        "ai_confidence": 0.22,
        "description": "AI detected contract intent, escalated to account manager.",
    },

    # ── Human only (15%) ────────────────────────────────────────────────
    {
        "weight": 8,
        "subject": "Compliance audit documentation request",
        "origin": "Email",
        "priority": "Critical",
        "status": "Closed",
        "ai_handled": "false",
        "description": "Legal compliance request — requires human judgment.",
    },
    {
        "weight": 7,
        "subject": "Complex technical integration issue",
        "origin": "Web",
        "priority": "High",
        "status": "Closed",
        "ai_handled": "false",
        "description": "Multi-system integration failure — AI escalated immediately.",
    },

    # ── SLA critical (10%) ──────────────────────────────────────────────
    {
        "weight": 6,
        "subject": "Service outage — production environment down",
        "origin": "Phone",
        "priority": "Critical",
        "status": "Closed",
        "ai_handled": "true",
        "ai_agent": "AlertBot-v1",
        "ai_confidence": 0.88,
        "description": "AI detected outage pattern, paged on-call and opened P1.",
    },
    {
        "weight": 4,
        "subject": "Urgent callback — executive escalation",
        "origin": "Phone",
        "priority": "Critical",
        "status": "Closed",
        "ai_handled": "false",
        "description": "VIP customer escalation — direct to senior agent.",
    },

    # ── Reassignment (10%) ──────────────────────────────────────────────
    {
        "weight": 6,
        "subject": "Technical support — wrong department routed",
        "origin": "Chat",
        "priority": "Medium",
        "status": "Closed",
        "ai_handled": "true",
        "ai_agent": "TriageBot-v1",
        "ai_confidence": 0.55,
        "description": "AI misrouted initially, transferred to correct team.",
    },
    {
        "weight": 4,
        "subject": "Language support — Spanish speaker",
        "origin": "Phone",
        "priority": "Medium",
        "status": "Closed",
        "ai_handled": "false",
        "description": "Transferred to bilingual agent.",
    },

    # ── Vendor reconciliation (5%) ──────────────────────────────────────
    {
        "weight": 3,
        "subject": "BPO vendor case — automated resolution claimed",
        "origin": "Web",
        "priority": "Low",
        "status": "Closed",
        "ai_handled": "true",
        "ai_agent": "VendorBot-External",
        "ai_confidence": 0.71,
        "description": "Vendor claims AI-resolved. Veratrace verifying attribution.",
    },
    {
        "weight": 2,
        "subject": "BPO vendor case — resolution overclaimed",
        "origin": "Web",
        "priority": "Medium",
        "status": "Closed",
        "ai_handled": "false",
        "description": "Vendor claimed AI but human actually resolved. Flagged for reconciliation.",
    },
]

_SCENARIO_WEIGHTS = [s["weight"] for s in CASE_SCENARIOS]


class SalesforceWarmer(BaseWarmer):
    """Creates real Cases in a Salesforce org."""

    def __init__(self, credentials, external_identity):
        super().__init__(credentials, external_identity)
        self._instance_url = credentials.get("instance_url", "").rstrip("/")
        self._access_token = credentials.get("access_token", "")

    def validate_access(self):
        try:
            result = self._api_get(
                f"/services/data/{SF_API_VERSION}/query?q=SELECT+count()+FROM+Case+LIMIT+1"
            )
            count = result.get("totalSize", 0)
            logger.info("Salesforce access validated — %d cases accessible", count)
            return True
        except Exception as e:
            logger.error("Salesforce access failed: %s", e)
            return False

    def create_activity(self, scenario_config):
        scenario = random.choices(CASE_SCENARIOS, weights=_SCENARIO_WEIGHTS, k=1)[0]
        customer = random.choice(CUSTOMER_NAMES)

        case_data = {
            "Subject": f"{scenario['subject']} — {customer}",
            "Description": scenario["description"],
            "Origin": scenario["origin"],
            "Priority": scenario["priority"],
            "Status": scenario["status"],
        }

        # Add AI fields if the scenario involves AI
        if scenario.get("ai_handled") == "true":
            case_data["AI_Handled__c"] = "true"
            case_data["AI_Agent_Name__c"] = scenario.get("ai_agent", "")
            case_data["AI_Confidence__c"] = scenario.get("ai_confidence", 0.0)

        try:
            result = self._api_post(f"/services/data/{SF_API_VERSION}/sobjects/Case", case_data)
            case_id = result.get("id", "")
            return {"id": case_id, "type": "Case", "customer": customer, "scenario": scenario["subject"]}
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            # Custom fields might not exist — retry without AI fields
            if "AI_Handled__c" in body or "INVALID_FIELD" in body:
                logger.warning("AI custom fields not found — creating without AI attribution")
                for key in ["AI_Handled__c", "AI_Agent_Name__c", "AI_Confidence__c"]:
                    case_data.pop(key, None)
                result = self._api_post(f"/services/data/{SF_API_VERSION}/sobjects/Case", case_data)
                return {"id": result.get("id", ""), "type": "Case", "customer": customer, "scenario": scenario["subject"]}
            raise

    def verify_activity(self, activity_id):
        try:
            result = self._api_get(f"/services/data/{SF_API_VERSION}/sobjects/Case/{activity_id}")
            return bool(result.get("Id"))
        except Exception:
            return False

    def _api_get(self, path):
        url = f"{self._instance_url}{path}"
        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Bearer {self._access_token}")
        req.add_header("Accept", "application/json")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())

    def _api_post(self, path, body):
        url = f"{self._instance_url}{path}"
        data = json.dumps(body).encode()
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Authorization", f"Bearer {self._access_token}")
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())


WARMER_ID = "salesforce"
WARMER_CLASS = SalesforceWarmer
