"""
consumers/fraud_consumer.py  —  Fraud processing consumer
==========================================================
Consumes: claim.created
Produces: claim.approved OR claim.rejected

The existing 5-layer fraud engine (services/fraud_engine.py) is
called UNCHANGED — we just moved it off the HTTP thread.

Pipeline per event:
  1. Idempotency check (BaseConsumer)
  2. Load worker from MongoDB
  3. Run 5-layer fraud engine (unchanged)
  4. Update claim status in MongoDB
  5. Publish claim.approved or claim.rejected
  6. Mark processed

If this consumer fails, the outbox event will be redelivered
(at-least-once). The idempotency check prevents double processing.
"""

import asyncio
import logging
import time
from datetime import datetime

from consumers.base import BaseConsumer, RetryableError, NonRetryableError
from kafka.topics import CLAIM_CREATED, CLAIM_APPROVED, CLAIM_REJECTED, CLAIM_PROCESSING_DLQ
from kafka.producer import produce_event

logger = logging.getLogger("clad.consumer.fraud")


class FraudConsumer(BaseConsumer):

    def __init__(self):
        super().__init__(
            topics        = [CLAIM_CREATED],
            group_id      = "fraud-processors",
            dlq_topic     = CLAIM_PROCESSING_DLQ,
            consumer_name = "fraud-processor",
        )

    async def handle(self, event: dict, payload: dict) -> None:
        """
        Process a claim.created event through the fraud engine.
        """
        from observability.metrics import (
            fraud_processing_duration,
            claims_approved_total,
            claims_rejected_total,
            fraud_score_histogram,
        )
        from db.operations import (
            get_worker_by_name,
            update_claim_status,
            increment_fraudulent_flags,
        )

        claim_id    = payload.get("claim_id", "unknown")
        worker_name = payload.get("worker_name", "")
        correlation = event.get("correlation_id", "")

        logger.info(
            f"FraudConsumer: processing claim_id={claim_id} "
            f"worker={worker_name} correlation={correlation}"
        )

        # ── Load worker from MongoDB ───────────────────────────
        worker = await get_worker_by_name(worker_name)
        if worker is None:
            raise NonRetryableError(f"Worker '{worker_name}' not found")

        # ── Build claim dict for fraud engine ──────────────────
        claim_data = {
            "amount":     payload.get("amount", 0),
            "reason":     payload.get("reason", "manual"),
            "trigger":    payload.get("trigger_type", "manual"),
            "created_at": payload.get("created_at", datetime.utcnow().isoformat()),
        }

        # ── Run 5-layer fraud engine ──────────────────────────
        # This is the EXISTING, UNCHANGED fraud engine from services/fraud_engine.py
        try:
            from services.fraud_engine import check_fraud
            t0 = time.monotonic()
            fraud = check_fraud(
                worker           = worker,
                claim            = claim_data,
                photo_submitted  = payload.get("photo_submitted", False),
                photo_metadata   = payload.get("photo_metadata"),
                gps_trace        = payload.get("gps_trace"),
                device_id        = payload.get("device_id"),
            )
            duration = time.monotonic() - t0
            fraud_processing_duration.observe(duration)

        except Exception as e:
            # IsolationForest / NetworkX failure — retryable
            raise RetryableError(f"Fraud engine error: {e}")

        fraud_score = int(fraud.get("score", 50))
        try:
            fraud_score_histogram.observe(fraud_score)
        except Exception:
            pass

        # ── Build fraud result for MongoDB ─────────────────────
        fraud_result = {
            "score":            fraud_score,
            "risk_level":       fraud.get("risk_level", "UNKNOWN"),
            "approved":         bool(fraud.get("approved", False)),
            "layers_triggered": fraud.get("layers_triggered", []),
            "layer_scores":     fraud.get("layer_scores", {}),
            "action":           fraud.get("action", ""),
            "checked_at":       datetime.utcnow().isoformat() + "Z",
        }

        # ── Determine outcome ──────────────────────────────────
        if fraud["approved"]:
            # Auto-approval routing by CladScore
            clad_score = float(worker.get("clad_score") or 50)
            if clad_score >= 85:
                status, payout_speed, review_note = "approved", "Instant",    "Auto-approved — CladScore A+"
            elif clad_score >= 75:
                status, payout_speed, review_note = "approved", "2hr auto",   "Auto-approved — CladScore A"
            elif clad_score >= 50:
                status, payout_speed, review_note = "approved", "6hr hold",   "Auto-approved — CladScore B"
            else:
                status, payout_speed, review_note = "pending_review", "24hr review", "Manual review — CladScore C/D"

            extra = {
                "fraud_result":  fraud_result,
                "payout_speed":  payout_speed,
                "review_note":   review_note,
            }
            await update_claim_status(claim_id, status, extra=extra)

            logger.info(
                f"FraudConsumer: APPROVED claim_id={claim_id} "
                f"score={fraud_score} payout_speed={payout_speed}"
            )
            claims_approved_total.inc()

            # Publish claim.approved
            await produce_event(
                topic          = CLAIM_APPROVED,
                key            = claim_id,
                payload        = {
                    **payload,
                    "fraud_score":  fraud_score,
                    "payout_speed": payout_speed,
                    "status":       status,
                },
                correlation_id = correlation,
                event_type     = CLAIM_APPROVED,
            )

        else:
            # Rejected by fraud engine
            extra = {
                "fraud_result": fraud_result,
                "review_note":  f"Rejected — {fraud.get('risk_level')} risk",
            }
            await update_claim_status(claim_id, "rejected_fraud", extra=extra)
            await increment_fraudulent_flags(worker_name)

            logger.warning(
                f"FraudConsumer: REJECTED claim_id={claim_id} "
                f"score={fraud_score} risk={fraud.get('risk_level')}"
            )
            claims_rejected_total.labels(reason=fraud.get("risk_level", "FRAUD")).inc()

            # Publish claim.rejected
            await produce_event(
                topic          = CLAIM_REJECTED,
                key            = claim_id,
                payload        = {
                    **payload,
                    "fraud_score": fraud_score,
                    "risk_level":  fraud.get("risk_level"),
                    "action":      fraud.get("action", ""),
                    "status":      "rejected_fraud",
                },
                correlation_id = correlation,
                event_type     = CLAIM_REJECTED,
            )


async def run_fraud_consumer():
    """Entry point for running the fraud consumer as a standalone process."""
    from db.mongo import init_db
    from kafka.producer import init_producer
    await init_db()
    init_producer()

    consumer = FraudConsumer()
    await consumer.run()


if __name__ == "__main__":
    import asyncio
    asyncio.run(run_fraud_consumer())
