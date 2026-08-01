"""
outbox/publisher.py  —  Transactional Outbox Publisher
=======================================================
Polls MongoDB outbox_events for pending events and publishes to Kafka.

This is the core mechanism of the Transactional Outbox Pattern:

1. When a claim is created, BOTH the claim document AND an outbox_event
   document are written in a single MongoDB transaction.

2. This publisher runs as a background task and polls for pending events.

3. It publishes each event to Kafka, then marks it as 'published'.

Why this matters:
  - If Kafka is down: events accumulate in outbox_events (safe)
  - When Kafka recovers: this publisher sends them all
  - No event is lost between MongoDB write and Kafka publish
  - Consumers still need idempotency (at-least-once delivery still possible)

Delivery guarantee: AT LEAST ONCE (not exactly-once).
This is why consumers check processed_events before doing work.
"""

import asyncio
import logging
import time

logger = logging.getLogger("clad.outbox.publisher")

POLL_INTERVAL_SECONDS = 0.1   # poll every 100ms
BATCH_SIZE            = 20    # process up to 20 events per poll cycle
MAX_OUTBOX_RETRIES    = 10    # after this many kafka failures, mark event as failed


async def run_outbox_publisher() -> None:
    """
    Main outbox publisher loop.
    Runs indefinitely as a background asyncio task.
    """
    from db.operations import (
        get_pending_outbox_events,
        mark_outbox_published,
        increment_outbox_retry,
    )
    from kafka.producer import produce_event, is_available as kafka_ok
    from observability.metrics import (
        outbox_pending_events,
        outbox_published_total,
        outbox_retries_total,
    )

    logger.info("Outbox publisher started")

    while True:
        try:
            pending = await get_pending_outbox_events(limit=BATCH_SIZE)

            if pending:
                try:
                    outbox_pending_events.set(len(pending))
                except Exception:
                    pass

            for outbox_event in pending:
                event_id  = outbox_event.get("event_id")
                topic     = outbox_event.get("topic")
                payload   = outbox_event.get("payload", {})
                key       = outbox_event.get("aggregate_id", event_id)
                retries   = int(outbox_event.get("retry_count", 0))

                if retries >= MAX_OUTBOX_RETRIES:
                    # Too many Kafka failures — mark as permanently failed
                    # (leaving in 'pending' would loop forever)
                    logger.error(
                        f"Outbox: max retries ({MAX_OUTBOX_RETRIES}) "
                        f"exceeded for event_id={event_id}, marking failed"
                    )
                    from db.mongo import outbox_events_col
                    col = outbox_events_col()
                    if col:
                        await col.update_one(
                            {"event_id": event_id},
                            {"$set": {"status": "failed"}}
                        )
                    continue

                if not kafka_ok():
                    # Kafka not available — leave pending, retry next cycle
                    logger.debug("Outbox: Kafka not available, will retry later")
                    break   # break inner loop but not outer while loop

                # Publish to Kafka
                try:
                    from kafka.producer import _producer
                    import json

                    if _producer:
                        value_bytes = json.dumps(payload, default=str).encode("utf-8")
                        key_bytes   = str(key).encode("utf-8")

                        _producer.produce(
                            topic=topic,
                            key=key_bytes,
                            value=value_bytes,
                        )
                        _producer.poll(0)   # trigger callbacks non-blocking

                    await mark_outbox_published(event_id)

                    try:
                        outbox_published_total.inc()
                    except Exception:
                        pass

                    logger.debug(
                        f"Outbox published: event_id={event_id} topic={topic}"
                    )

                except Exception as e:
                    await increment_outbox_retry(event_id, str(e))
                    try:
                        outbox_retries_total.inc()
                    except Exception:
                        pass
                    logger.warning(
                        f"Outbox: Kafka publish failed for event_id={event_id}: {e}"
                    )

        except Exception as e:
            logger.exception(f"Outbox publisher error: {e}")

        await asyncio.sleep(POLL_INTERVAL_SECONDS)


async def start_outbox_publisher_background() -> asyncio.Task:
    """
    Start the outbox publisher as a background asyncio task.
    Call this from FastAPI lifespan startup.
    Returns the task handle so it can be cancelled on shutdown.
    """
    task = asyncio.create_task(run_outbox_publisher())
    logger.info("Outbox publisher background task created")
    return task
