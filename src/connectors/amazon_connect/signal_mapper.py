"""
Amazon Connect CTR → TwuSignal mapper.

Transforms a Contact Trace Record into one or more TwuSignals representing
the lifecycle of a contact interaction. A single CTR may produce multiple
signals (contact started, AI interaction, agent connected, contact ended).

Signal types:
  - contact_initiated  (SYSTEM)  — customer entered the system
  - ai_interaction     (AI)      — Lex bot processed the contact
  - agent_connected    (HUMAN)   — human agent took over
  - contact_completed  (varies)  — interaction ended, includes Contact Lens data

Graceful degradation: if non-critical fields are missing, the signal is
created with degraded=True and the raw payload is preserved.
"""
import logging
from datetime import datetime

from src.runtime.signal_writer import TwuSignal
from src.connectors.amazon_connect.schema import REQUIRED_FIELDS, PII_FIELDS

logger = logging.getLogger(__name__)

# Known contact attribute keys that carry structured data
STRUCTURED_ATTRIBUTE_KEYS = {
    "customerSegment", "priority", "contactReason", "source",
    "caseId", "botResolved", "transferReason", "slaTargetSeconds",
    "aiHandled", "aiAgent", "aiConfidence", "humanNeeded",
    "vendorClaimedAI", "resolution",
}


def ctr_to_signals(
    ctr: dict,
    instance_id: str,
    integration_account_id: str,
) -> list:
    """
    Transform an Amazon Connect CTR into TwuSignals.

    Each CTR produces 2-4 signals depending on what happened:
    1. contact_initiated — when the customer first connected
    2. ai_interaction — when a Lex bot processed the contact (if applicable)
    3. agent_connected — when routed to a human agent (if applicable)
    4. contact_completed — when the interaction ended

    Returns signals with degraded=True if required fields are missing.
    Raw CTR payload is always preserved in the last signal for forensics.
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
    actor_type = "HUMAN" if agent_arn else "SYSTEM"
    actor_id = agent_arn.split("/")[-1] if agent_arn else "system"

    # Parse structured attributes
    raw_attributes = ctr.get("Attributes", {})
    structured_attrs = _parse_structured_attributes(raw_attributes)

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
                **structured_attrs,
            },
            degraded=is_degraded,
            degraded_reason=degraded_reason,
            pii_encrypted_fields=_get_pii_fields(ctr),
        ))

    # Signal 2: AI interaction (Lex bot) — if a bot touched this contact
    ai_signal = _extract_ai_interaction(
        ctr, contact_id, channel, instance_id, integration_account_id,
        is_degraded, degraded_reason,
    )
    if ai_signal:
        signals.append(ai_signal)

    # Signal 3: Agent connected (only if a human agent was involved)
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

    # Signal 4: Contact completed — includes Contact Lens enrichment
    disconnect_ts = ctr.get("DisconnectTimestamp", "")
    if disconnect_ts:
        duration_seconds = None
        if initiation_ts and disconnect_ts:
            try:
                start = datetime.fromisoformat(initiation_ts.replace("Z", "+00:00"))
                end = datetime.fromisoformat(disconnect_ts.replace("Z", "+00:00"))
                duration_seconds = int((end - start).total_seconds())
            except (ValueError, TypeError):
                pass

        # Determine who ultimately resolved this contact
        lex = ctr.get("LexBotInteraction", {})
        resolved_by = "HUMAN" if agent_arn else ("AI" if lex else "SYSTEM")

        completed_payload = {
            "event": "contact_completed",
            "contact_id": contact_id,
            "channel": channel,
            "disconnect_reason": ctr.get("DisconnectReason", ""),
            "duration_seconds": duration_seconds,
            "resolved_by": resolved_by,
            "agent_interaction_duration": _safe_get_nested(ctr, "Agent", "AgentInteractionDuration"),
            "after_contact_work_duration": _safe_get_nested(ctr, "Agent", "AfterContactWorkDuration"),
            "hold_duration": _safe_get_nested(ctr, "Agent", "HoldDuration"),
            "recording_status": _safe_get_nested(ctr, "Recording", "Status"),
            "analysis_status": ctr.get("AnalysisStatus", ""),
            **structured_attrs,
        }

        # Enrich with Contact Lens data if available
        contact_lens = _extract_contact_lens(ctr)
        if contact_lens:
            completed_payload["contact_lens"] = contact_lens

        signals.append(TwuSignal(
            instance_id=instance_id,
            type="INTEGRATION_EVENT",
            name="contact_completed",
            occurred_at=_normalize_timestamp(disconnect_ts),
            source_integration_account_id=integration_account_id,
            source_integration="amazon-connect",
            actor_type=actor_type if agent_arn else ("AI" if lex else "SYSTEM"),
            actor_agent_id=actor_id if agent_arn else (lex.get("BotName", "system") if lex else "system"),
            payload=completed_payload,
            degraded=is_degraded,
            degraded_reason=degraded_reason,
        ))

    # Preserve full raw CTR in the last signal's payload for forensics
    if signals:
        signals[-1].payload["_raw_ctr"] = ctr

    return signals


def _extract_ai_interaction(ctr, contact_id, channel, instance_id,
                            integration_account_id, is_degraded, degraded_reason):
    """
    Extract Lex bot interaction data from the CTR.
    Returns an ai_interaction TwuSignal if a bot was involved, None otherwise.
    """
    lex = ctr.get("LexBotInteraction", {})
    if not lex or not lex.get("BotName"):
        return None

    bot_name = lex.get("BotName", "unknown-bot")
    intent = lex.get("IntentName", "")
    confidence = lex.get("ConfidenceScore", 0.0)
    slot_to_elicit = lex.get("SlotToElicit")
    session_attrs = lex.get("SessionAttributes", {})

    # Bot resolved if all slots filled (SlotToElicit is null) and confidence is high
    resolved_by_bot = slot_to_elicit is None and confidence > 0.5

    # Determine if contact was later transferred to a human
    agent = ctr.get("Agent", {})
    agent_arn = agent.get("ARN", "") if isinstance(agent, dict) else ""
    transferred_to_human = bool(agent_arn)

    # Use the initiation timestamp + a small offset for the AI interaction
    initiation_ts = ctr.get("InitiationTimestamp", "")
    ai_ts = ctr.get("ConnectedToSystemTimestamp", "") or initiation_ts

    return TwuSignal(
        instance_id=instance_id,
        type="AI",
        name="ai_interaction",
        occurred_at=_normalize_timestamp(ai_ts),
        source_integration_account_id=integration_account_id,
        source_integration="amazon-connect",
        actor_type="AI",
        actor_agent_id=bot_name,
        payload={
            "event": "ai_interaction",
            "contact_id": contact_id,
            "channel": channel,
            "bot_name": bot_name,
            "bot_alias": lex.get("BotAlias", ""),
            "intent": intent,
            "confidence": confidence,
            "slot_to_elicit": slot_to_elicit,
            "resolved_by_bot": resolved_by_bot,
            "transferred_to_human": transferred_to_human,
            "session_id": lex.get("SessionId", ""),
            "session_attributes": session_attrs,
        },
        degraded=is_degraded,
        degraded_reason=degraded_reason,
    )


def _extract_contact_lens(ctr):
    """
    Extract Contact Lens analytics from the CTR.
    Returns a dict with sentiment, categories, and issues — or None if not available.
    """
    if ctr.get("AnalysisStatus") != "COMPLETED":
        return None

    contact_lens = ctr.get("ContactLens", {})
    if not contact_lens:
        return None

    result = {}

    # Sentiment
    sentiment = contact_lens.get("SentimentAnalysis", {})
    if sentiment:
        result["overall_sentiment"] = sentiment.get("OverallSentiment")
        customer = sentiment.get("CustomerSentiment", {})
        if isinstance(customer, dict):
            result["customer_sentiment"] = {
                "start": customer.get("BeginningMomentSentiment"),
                "middle": customer.get("MiddleMomentSentiment"),
                "end": customer.get("EndMomentSentiment"),
            }
        agent = sentiment.get("AgentSentiment", {})
        if isinstance(agent, dict):
            result["agent_sentiment"] = {
                "start": agent.get("BeginningMomentSentiment"),
                "middle": agent.get("MiddleMomentSentiment"),
                "end": agent.get("EndMomentSentiment"),
            }

    # Categories
    categories = contact_lens.get("Categories", {})
    if categories:
        result["categories"] = categories.get("MatchedCategories", [])

    # Issues
    issues = contact_lens.get("IssuesDetected", [])
    if issues:
        result["issues"] = [i.get("Name", "") for i in issues if i.get("Name")]

    # Transcript summary (just count, don't store full text — PII risk)
    transcript = contact_lens.get("Transcript", [])
    if transcript:
        result["transcript_turns"] = len(transcript)

    return result if result else None


def _parse_structured_attributes(attributes):
    """
    Extract known attribute keys into a flat dict for structured signal fields.
    Unknown keys are preserved in 'custom_attributes'.
    """
    if not attributes or not isinstance(attributes, dict):
        return {}

    structured = {}
    custom = {}

    for key, value in attributes.items():
        if key in STRUCTURED_ATTRIBUTE_KEYS:
            structured[key] = value
        else:
            custom[key] = value

    if custom:
        structured["custom_attributes"] = custom

    return structured


def _normalize_timestamp(ts):
    """Ensure timestamp is ISO 8601 with UTC timezone."""
    if not ts:
        return ""
    if not ts.endswith("Z") and "+" not in ts:
        ts += "Z"
    return ts


def _safe_get_nested(data, *keys):
    """Safely navigate nested dicts without KeyError."""
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
        else:
            return None
    return current


def _get_pii_fields(ctr):
    """Identify which fields in this CTR contain PII."""
    pii = []
    if ctr.get("CustomerEndpoint"):
        pii.append("customer_endpoint")
    if ctr.get("Attributes"):
        pii.append("attributes")
    return pii
