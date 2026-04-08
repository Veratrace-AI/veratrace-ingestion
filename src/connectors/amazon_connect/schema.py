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
    # Contact Lens fields (when enabled)
    "AnalysisStatus",
    "Transcript",
    "Categories",
    "SentimentAnalysis",
}

# Schema hash computed from these fields — update when Connect changes their CTR format
# Recompute with: schema_validator.compute_schema_hash(sample_ctr)
EXPECTED_SCHEMA_HASH = "initial_placeholder"

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
