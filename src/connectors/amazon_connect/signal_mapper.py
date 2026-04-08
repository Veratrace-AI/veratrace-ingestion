"""
Amazon Connect CTR → TwuSignal mapper.

Transforms a Contact Trace Record into one or more TwuSignals representing
the lifecycle of a contact interaction. A single CTR may produce multiple
signals (contact started, agent connected, contact ended, etc.).

Graceful degradation: if non-critical fields are missing, the signal is
created with degraded=True and the raw payload is preserved.
"""
import logging
from datetime import datetime

from src.runtime.signal_writer import TwuSignal
from src.connectors.amazon_connect.schema import REQUIRED_FIELDS, PII_FIELDS

logger = logging.getLogger(__name__)


def ctr_to_signals(
    ctr: dict,
    instance_id: str,
    integration_account_id: str,
) -> list[TwuSignal]:
    """
    Transform an Amazon Connect CTR into TwuSignals.

    Each CTR produces 2-3 signals:
    1. contact_initiated — when the customer first connected
    2. agent_connected — when routed to an agent (if applicable)
    3. contact_completed — when the interaction ended

    Returns signals with degraded=True if required fields are missing.
    Raw CTR payload is always preserved in signal.payload for forensics.
    """
    signals = []
    contact_id = ctr.get("ContactId", "")
    channel = ctr.get("Channel", "UNKNOWN")

    # Check for required fields — degrade if missing
    missing = REQUIRED_FIELDS - set(ctr.keys())
    is_degraded = bool(missing)
    degraded_reason = f"Missing fields: {', '.join(missing)}" if missing else ""

    if is_degraded:
        logger.warning(
            "Degraded CTR (missing %s): ContactId=%s",
            missing, contact_id[:12],
        )

    # Determine actor type and ID
    agent = ctr.get("Agent", {})
    agent_arn = agent.get("ARN", "") if isinstance(agent, dict) else ""
    # If no agent, the contact was handled by IVR/bot (SYSTEM)
    actor_type = "HUMAN" if agent_arn else "SYSTEM"
    actor_id = agent_arn.split("/")[-1] if agent_arn else "system"

    # Signal 1: Contact initiated
    initiation_ts = ctr.get("InitiationTimestamp", "")
    if initiation_ts:
        signals.append(TwuSignal(
            instance_id=instance_id,
            type="INTEGRATION_EVENT",
            name="contact_initiated",
            occurred_at=_normalize_timestamp(initiation_ts),
            source_integration_account_id=integration_account_id,
            source_integration="amazon-connect",
            actor_type="SYSTEM",
            actor_agent_id="connect-routing",
            payload={
                "event": "contact_initiated",
                "contact_id": contact_id,
                "channel": channel,
                "initiation_method": ctr.get("InitiationMethod", ""),
                "queue": _safe_get_nested(ctr, "Queue", "Name"),
                "customer_endpoint_type": _safe_get_nested(ctr, "CustomerEndpoint", "Type"),
            },
            degraded=is_degraded,
            degraded_reason=degraded_reason,
            pii_encrypted_fields=_get_pii_fields(ctr),
        ))

    # Signal 2: Agent connected (only if an agent was involved)
    connected_ts = ctr.get("ConnectedToSystemTimestamp", "")
    if connected_ts and agent_arn:
        signals.append(TwuSignal(
            instance_id=instance_id,
            type="INTEGRATION_EVENT",
            name="agent_connected",
            occurred_at=_normalize_timestamp(connected_ts),
            source_integration_account_id=integration_account_id,
            source_integration="amazon-connect",
            actor_type=actor_type,
            actor_agent_id=actor_id,
            payload={
                "event": "agent_connected",
                "contact_id": contact_id,
                "channel": channel,
                "agent_arn": agent_arn,
                "agent_username": agent.get("Username", "") if isinstance(agent, dict) else "",
                "queue": _safe_get_nested(ctr, "Queue", "Name"),
                "queue_duration": _safe_get_nested(ctr, "Queue", "Duration"),
            },
            degraded=is_degraded,
            degraded_reason=degraded_reason,
        ))

    # Signal 3: Contact completed
    disconnect_ts = ctr.get("DisconnectTimestamp", "")
    if disconnect_ts:
        # Calculate duration
        duration_seconds = None
        if initiation_ts and disconnect_ts:
            try:
                start = datetime.fromisoformat(initiation_ts.replace("Z", "+00:00"))
                end = datetime.fromisoformat(disconnect_ts.replace("Z", "+00:00"))
                duration_seconds = int((end - start).total_seconds())
            except (ValueError, TypeError):
                pass

        signals.append(TwuSignal(
            instance_id=instance_id,
            type="INTEGRATION_EVENT",
            name="contact_completed",
            occurred_at=_normalize_timestamp(disconnect_ts),
            source_integration_account_id=integration_account_id,
            source_integration="amazon-connect",
            actor_type=actor_type,
            actor_agent_id=actor_id,
            payload={
                "event": "contact_completed",
                "contact_id": contact_id,
                "channel": channel,
                "disconnect_reason": ctr.get("DisconnectReason", ""),
                "duration_seconds": duration_seconds,
                "agent_interaction_duration": _safe_get_nested(ctr, "Agent", "AgentInteractionDuration"),
                "after_contact_work_duration": _safe_get_nested(ctr, "Agent", "AfterContactWorkDuration"),
                "hold_duration": _safe_get_nested(ctr, "Agent", "HoldDuration"),
                "attributes": ctr.get("Attributes", {}),
                "recording_status": _safe_get_nested(ctr, "Recording", "Status"),
                "analysis_status": ctr.get("AnalysisStatus", ""),
            },
            degraded=is_degraded,
            degraded_reason=degraded_reason,
        ))

    # Preserve full raw CTR in the last signal's payload for forensics
    if signals:
        signals[-1].payload["_raw_ctr"] = ctr

    return signals


def _normalize_timestamp(ts: str) -> str:
    """Ensure timestamp is ISO 8601 with UTC timezone."""
    if not ts:
        return ""
    if not ts.endswith("Z") and "+" not in ts:
        ts += "Z"
    return ts


def _safe_get_nested(data: dict, *keys) :
    """Safely navigate nested dicts without KeyError."""
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return None
    return current


def _get_pii_fields(ctr: dict) -> list[str]:
    """Identify which fields in this CTR contain PII."""
    pii = []
    if ctr.get("CustomerEndpoint"):
        pii.append("customer_endpoint")
    if ctr.get("Attributes"):
        pii.append("attributes")
    return pii
