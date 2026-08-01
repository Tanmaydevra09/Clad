"""
db/indexes.py  —  All MongoDB index definitions for Clad
=========================================================
Run at startup via create_all_indexes().
All indexes are created with background=False for correctness
(Motor always creates in background by default on Atlas).

Index strategy is driven by actual query patterns:
  - Most queries use worker name or worker_id
  - Claims are queried by worker + recency, by status
  - Payouts have hard uniqueness on claim_id (idempotency)
  - processed_events uniqueness = idempotency enforcement
  - outbox_events polled by (status, created_at) for the publisher
"""

import logging
from pymongo import ASCENDING, DESCENDING, IndexModel

logger = logging.getLogger("clad.db.indexes")


async def create_all_indexes(db) -> None:
    """Create all required indexes. Safe to call multiple times (idempotent)."""

    # ── workers ────────────────────────────────────────────────
    await db.workers.create_indexes([
        IndexModel([("name", ASCENDING)],  unique=True, name="idx_workers_name_unique"),
        IndexModel([("pincode", ASCENDING)], name="idx_workers_pincode"),
        IndexModel([("plan", ASCENDING)],    name="idx_workers_plan"),
        IndexModel(
            [("risk_profile.clad_score", DESCENDING)],
            name="idx_workers_clad_score"
        ),
    ])
    logger.debug("workers indexes OK")

    # ── policies ───────────────────────────────────────────────
    await db.policies.create_indexes([
        IndexModel([("worker_name", ASCENDING)], unique=True, name="idx_policies_worker_name_unique"),
        IndexModel([("status", ASCENDING)],      name="idx_policies_status"),
    ])
    logger.debug("policies indexes OK")

    # ── claims ─────────────────────────────────────────────────
    await db.claims.create_indexes([
        IndexModel([("claim_id", ASCENDING)], unique=True, name="idx_claims_claim_id_unique"),
        IndexModel(
            [("worker_name", ASCENDING), ("created_at", DESCENDING)],
            name="idx_claims_worker_recency"
        ),
        IndexModel([("status", ASCENDING)], name="idx_claims_status"),
        IndexModel(
            [("status", ASCENDING), ("created_at", DESCENDING)],
            name="idx_claims_status_recency"
        ),
        IndexModel(
            [("pincode", ASCENDING), ("created_at", DESCENDING)],
            name="idx_claims_pincode_recency"
        ),
        IndexModel([("trigger_type", ASCENDING)], name="idx_claims_trigger_type"),
        IndexModel(
            [("payout_processed", ASCENDING), ("status", ASCENDING)],
            name="idx_claims_payout_queue"
        ),
        IndexModel([("created_at", DESCENDING)], name="idx_claims_created_at"),
    ])
    logger.debug("claims indexes OK")

    # ── payouts ────────────────────────────────────────────────
    # UNIQUE on claim_id = core idempotency enforcement
    # A second insert with same claim_id raises DuplicateKeyError
    await db.payouts.create_indexes([
        IndexModel([("claim_id", ASCENDING)],        unique=True, name="idx_payouts_claim_id_unique"),
        IndexModel([("idempotency_key", ASCENDING)], unique=True, name="idx_payouts_idempotency_key_unique"),
        IndexModel([("status", ASCENDING)],          name="idx_payouts_status"),
        IndexModel([("worker_name", ASCENDING)],     name="idx_payouts_worker"),
    ])
    logger.debug("payouts indexes OK")

    # ── trigger_events ─────────────────────────────────────────
    await db.trigger_events.create_indexes([
        IndexModel([("trigger_id", ASCENDING)], unique=True, name="idx_trigger_id_unique"),
        IndexModel(
            [("pincode", ASCENDING), ("triggered_at", DESCENDING)],
            name="idx_trigger_pincode_time"
        ),
        IndexModel([("trigger_type", ASCENDING), ("triggered_at", DESCENDING)],
                   name="idx_trigger_type_time"),
        IndexModel([("triggered_at", DESCENDING)], name="idx_trigger_time"),   # ETL watermark
    ])
    logger.debug("trigger_events indexes OK")

    # ── outbox_events ──────────────────────────────────────────
    # Polled by publisher: status=pending, sorted by created_at ASC
    await db.outbox_events.create_indexes([
        IndexModel([("event_id", ASCENDING)], unique=True, name="idx_outbox_event_id_unique"),
        IndexModel(
            [("status", ASCENDING), ("created_at", ASCENDING)],
            name="idx_outbox_status_time",
            partialFilterExpression={"status": "pending"}   # only index pending events
        ),
        IndexModel([("aggregate_id", ASCENDING)], name="idx_outbox_aggregate"),
    ])
    logger.debug("outbox_events indexes OK")

    # ── processed_events ───────────────────────────────────────
    # THE idempotency index — unique per (event_id, consumer)
    # A second insert raises DuplicateKeyError → skip duplicate
    await db.processed_events.create_indexes([
        IndexModel(
            [("event_id", ASCENDING), ("consumer_name", ASCENDING)],
            unique=True,
            name="idx_processed_event_consumer_unique"
        ),
        IndexModel([("processed_at", DESCENDING)], name="idx_processed_time"),
    ])
    logger.debug("processed_events indexes OK")

    # ── etl_checkpoints ────────────────────────────────────────
    await db.etl_checkpoints.create_indexes([
        IndexModel([("job_name", ASCENDING)], unique=True, name="idx_etl_job_unique"),
    ])
    logger.debug("etl_checkpoints indexes OK")

    logger.info("All MongoDB indexes created/verified")
