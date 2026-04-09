"""
Expected schema for Amazon Connect Contact Trace Records (CTRs).

Based on: https://docs.aws.amazon.com/connect/latest/adminguide/ctr-data-model.html

Used for schema drift detection. If Connect changes their CTR format,
the schema validator will flag it before signals are corrupted.
"""

# Top-level fields in a Contact Trace Record
EXPECTED_CTR_FIELDS = {
    "ContactId",
    "InitialContactId",
    "PreviousContactId",
    "Channel",                    # VOICE, CHAT, TASK, EMAIL
    "InitiationMethod",           # INBOUND, OUTBOUND, TRANSFER, CALLBACK, QUEUE_TRANSFER, API
    "InitiationTimestamp",
    "ConnectedToSystemTimestamp",
    "DisconnectTimestamp",
    "DisconnectReason",
    "Queue",                      # { Name, ARN, EnqueueTimestamp, DequeueTimestamp, Duration }
    "Agent",                      # { ARN, Username, AfterContactWorkDuration, ... }
    "AgentConnectionAttempts",
    "Attributes",                 # Custom attributes set in contact flows (dict)
    "Recording",                  # { Location, Type, Status, DeletionReason }
    "CustomerEndpoint",           # { Type, Address }
    "SystemEndpoint",             # { Type, Address }
    "TransferCompletedTimestamp",
    "TransferredToEndpoint",
    "InstanceARN",
    "ScheduledTimestamp",
    "CampaignId",
    "AnsweringMachineDetectionStatus",
    # Lex bot interaction (when bot is in the contact flow)
    "LexBotInteraction",
    # Contact Lens fields (when enabled on instance)
    "AnalysisStatus",
    "ContactLens",
    "Transcript",
    "Categories",
    "SentimentAnalysis",
}

# Lex bot interaction sub-fields
LEX_FIELDS = {
    "BotName",
    "BotAlias",
    "IntentName",
    "ConfidenceScore",
    "SlotToElicit",
    "SessionId",
    "SessionAttributes",
}

# Contact Lens sub-fields
CONTACT_LENS_FIELDS = {
    "Transcript",          # Array of {ParticipantRole, Content, AbsoluteTime}
    "Categories",          # {MatchedCategories, CategoryResults}
    "SentimentAnalysis",   # {OverallSentiment, CustomerSentiment, AgentSentiment}
    "IssuesDetected",      # Array of {Name, Timestamp}
}

# Schema hash computed from sorted field names — update when Connect changes their CTR format
import hashlib
EXPECTED_SCHEMA_HASH = hashlib.sha256(
    "|".join(sorted(EXPECTED_CTR_FIELDS)).encode()
).hexdigest()[:16]

# Fields that contain PII — must be encrypted at write time
PII_FIELDS = {
    "CustomerEndpoint",           # phone number / email
    "Attributes",                 # may contain customer data
}

# Fields required for a valid signal (missing any = degraded signal)
REQUIRED_FIELDS = {
    "ContactId",
    "Channel",
    "InitiationTimestamp",
    "DisconnectTimestamp",
}
