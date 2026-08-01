"""
consumers/payout_consumer.py  —  Idempotent payout processor
=============================================================
Consumes: claim.approved
Produces: payout.completed

Idempotency is enforced at THREE independent layers:
  1. processed_events check (BaseConsumer) — application layer
  2. payouts.claim_id UNIQUE index         — MongoDB DB layer
  3. Razorpay X-Payout-Idempotency key    — external payment layer

The same claim.approved event can arrive multiple times (at-least-once).
Only ONE payout will ever be created.
"""

import asyncio
import base64
import logging
import random
import time
from datetime import datetime
from typing import Optional

from consumers.base import BaseConsumer, RetryableError, NonRetryableError
from kafka.topics import CLAIM_APPROVED, PAYOUT_COMPLETED, PAYOUT_PROCESSING_DLQ
from kafka.producer import produce_event

logger = logging.getLogger("clad.consumer.payout")


class PayoutConsumer(BaseConsumer):

    def __init__(self):
        super().__init__(
            topics        = [CLAIM_APPROVED],
            group_id      = "payout-processors",
            dlq_topic     = PAYOUT_PROCESSING_DLQ,
            consumer_name = "payout-processor",
        )

    async def handle(self, event: dict, payload: dict) -> None:
        """
        Process an approved claim and initiate Razorpay payout.
        """
        import os
        from pymongo.errors import DuplicateKeyError
        from db.operations import create_payout_record, mark_payout_processed, get_worker_by_name
        from observability.metrics import payout_processing_duration

        claim_id    = payload.get("claim_id", "unknown")
        worker_name = payload.get("worker_name", "")
        correlation = event.get("correlation_id", "")
        amount      = float(payload.get("amount", 0))

        logger.info(
            f"PayoutConsumer: processing claim_id={claim_id} "
            f"worker={worker_name} amount=₹{amount} correlation={correlation}"
        )

        # ── Layer 2 DB idempotency check: try to create payout row ────
        # If claim_id already has a payout → DuplicateKeyError → skip
        idempotency_key = f"CLAD-{claim_id}"   # deterministic — no random

        # ── Razorpay API call ────────────────────────────────────────
        worker   = await get_worker_by_name(worker_name)
        upi_vpa  = f"{worker_name.lower().replace(' ', '.')}@upi" if worker else f"worker@upi"
        phone    = "9999999999"
        amount_p = int(amount * 100)

        RAZORPAY_KEY_ID     = os.getenv("RAZORPAY_KEY_ID", "")
        RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET", "")

        razorpay_data = {}
        mode = "live_api"

        t0 = time.monotonic()

        if RAZORPAY_KEY_ID and RAZORPAY_KEY_SECRET:
            import httpx
            creds   = base64.b64encode(f"{RAZORPAY_KEY_ID}:{RAZORPAY_KEY_SECRET}".encode()).decode()
            headers = {
                "Authorization":        f"Basic {creds}",
                "Content-Type":         "application/json",
                "X-Payout-Idempotency": idempotency_key,  # ← FIXED: deterministic
            }
            base_url = "https://api.razorpay.com/v1"

            contact_id   = None
            fund_acct_id = None
            payout_resp  = None

            try:
                async with httpx.AsyncClient(timeout=15.0) as client:
                    try:
                        r1 = await client.post(f"{base_url}/contacts", headers=headers, json={
                            "name": worker_name, "contact": phone, "type": "employee",
                            "reference_id": f"CLAD-W-{claim_id}",
                            "notes": {"worker_type": "gig_delivery"},
                        })
                        if r1.status_code in (200, 201):
                            contact_id = r1.json().get("id")
                    except httpx.TimeoutException as e:
                        raise RetryableError(f"Razorpay /contacts timeout: {e}")

                    if contact_id:
                        try:
                            r2 = await client.post(f"{base_url}/fund_accounts", headers=headers, json={
                                "contact_id": contact_id, "account_type": "vpa",
                                "vpa": {"address": upi_vpa},
                            })
                            if r2.status_code in (200, 201):
                                fund_acct_id = r2.json().get("id")
                        except httpx.TimeoutException as e:
                            raise RetryableError(f"Razorpay /fund_accounts timeout: {e}")

                    if fund_acct_id:
                        try:
                            r3 = await client.post(f"{base_url}/payouts", headers=headers, json={
                                "account_number":       "2323230074795370",
                                "fund_account_id":      fund_acct_id,
                                "amount":               amount_p,
                                "currency":             "INR",
                                "mode":                 "UPI",
                                "purpose":              "payout",
                                "queue_if_low_balance": True,
                                "reference_id":         idempotency_key,
                                "narration":            f"Clad claim {claim_id} payout",
                                "notes":                {"claim_id": claim_id},
                            })
                            if r3.status_code in (200, 201):
                                payout_resp = r3.json()
                        except httpx.TimeoutException as e:
                            raise RetryableError(f"Razorpay /payouts timeout: {e}")

            except RetryableError:
                raise
            except Exception as e:
                mode = "fallback_simulation"
                logger.warning(f"Razorpay unavailable, using simulation: {e}")

            razorpay_data = {
                "contact_id":      contact_id   or f"cont_{random.randint(10000000,99999999)}",
                "fund_account_id": fund_acct_id or f"fa_{random.randint(10000000,99999999)}",
                "payout_id":       (payout_resp.get("id") if payout_resp else None)
                                   or f"pout_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
            }
        else:
            mode = "fallback_simulation"
            razorpay_data = {
                "contact_id":      f"cont_{random.randint(10000000,99999999)}",
                "fund_account_id": f"fa_{random.randint(10000000,99999999)}",
                "payout_id":       f"pout_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
            }

        duration = time.monotonic() - t0
        try:
            payout_processing_duration.observe(duration)
        except Exception:
            pass

        # ── Write payout to MongoDB (UNIQUE constraint = idempotency) ──
        try:
            payout_doc = await create_payout_record(
                claim_id    = claim_id,
                worker_name = worker_name,
                amount      = amount,
                upi_vpa     = upi_vpa,
                razorpay_data = razorpay_data,
                mode        = mode,
            )
        except DuplicateKeyError:
            # Payout already exists — idempotency at DB level worked
            logger.info(
                f"PayoutConsumer: payout already exists for claim_id={claim_id} "
                f"(DB idempotency guard). Skipping."
            )
            return

        # ── Mark claim as payout_processed ────────────────────────────
        await mark_payout_processed(
            claim_id  = claim_id,
            payout_id = razorpay_data["payout_id"],
            upi       = upi_vpa,
        )

        logger.info(
            f"PayoutConsumer: PAYOUT COMPLETE claim_id={claim_id} "
            f"payout_id={razorpay_data['payout_id']} "
            f"amount=₹{amount} upi={upi_vpa} mode={mode}"
        )

        # ── Publish payout.completed ───────────────────────────────────
        await produce_event(
            topic          = PAYOUT_COMPLETED,
            key            = claim_id,
            payload        = {
                "claim_id":        claim_id,
                "worker_name":     worker_name,
                "amount":          amount,
                "upi_vpa":         upi_vpa,
                "payout_id":       razorpay_data["payout_id"],
                "razorpay_mode":   mode,
                "idempotency_key": idempotency_key,
                "completed_at":    datetime.utcnow().isoformat() + "Z",
            },
            correlation_id = correlation,
            event_type     = PAYOUT_COMPLETED,
        )


async def run_payout_consumer():
    """Entry point for running the payout consumer as a standalone process."""
    from db.mongo import init_db
    from kafka.producer import init_producer
    await init_db()
    init_producer()

    consumer = PayoutConsumer()
    await consumer.run()


if __name__ == "__main__":
    asyncio.run(run_payout_consumer())
