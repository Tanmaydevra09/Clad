"""
kafka/topics.py  —  Kafka topic name constants
===============================================
All topic names in one place.
Import this wherever you produce or consume, never hardcode strings.
"""

# ── Operational Topics ─────────────────────────────────────────
DISRUPTION_DETECTED  = "disruption.detected"
CLAIM_CREATED        = "claim.created"
CLAIM_APPROVED       = "claim.approved"
CLAIM_REJECTED       = "claim.rejected"
PAYOUT_REQUESTED     = "payout.requested"
PAYOUT_COMPLETED     = "payout.completed"

# ── Dead Letter Queues ─────────────────────────────────────────
CLAIM_PROCESSING_DLQ    = "claim.processing.dlq"
PAYOUT_PROCESSING_DLQ   = "payout.processing.dlq"
ANALYTICS_PROCESSING_DLQ = "analytics.processing.dlq"

# ── All topics (for admin / topic creation) ────────────────────
ALL_TOPICS = [
    DISRUPTION_DETECTED,
    CLAIM_CREATED,
    CLAIM_APPROVED,
    CLAIM_REJECTED,
    PAYOUT_REQUESTED,
    PAYOUT_COMPLETED,
    CLAIM_PROCESSING_DLQ,
    PAYOUT_PROCESSING_DLQ,
    ANALYTICS_PROCESSING_DLQ,
]

# ── Partition strategy ─────────────────────────────────────────
# claim_id → string key → Kafka partitions by key hash
# pincode  → disruption.detected partitioned by geographic zone
# No key   → DLQ topics (ordering not required)
