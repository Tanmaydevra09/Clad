"""
consumers/base.py  —  Base Kafka consumer with retry + DLQ + idempotency
=========================================================================
All Clad consumers inherit from BaseConsumer which provides:

1. Idempotency check:  check processed_events before doing work
2. Retry logic:        exponential backoff for transient errors
3. DLQ publishing:     after MAX_RETRIES, events go to DLQ
4. Prometheus metrics: events_consumed, errors, DLQ count
5. Correlation ID:     logged on every message

Usage:
    class FraudConsumer(BaseConsumer):
        async def handle(self, event: dict, payload: dict) -> None:
            # your business logic here
            ...

    consumer = FraudConsumer(
        topics=["claim.created"],
        group_id="fraud-processors",
        dlq_topic="claim.processing.dlq",
        consumer_name="fraud-processor",
    )
    await consumer.run()
"""

import os
import json
import asyncio
import logging
from abc import abstractmethod
from typing import Optional
from datetime import datetime

logger = logging.getLogger("clad.consumer.base")

# ── Retry config ───────────────────────────────────────────────
MAX_RETRIES      = 4
BACKOFF_SECONDS  = [1, 2, 4, 8]   # exponential: 1s, 2s, 4s, 8s


class RetryableError(Exception):
    """Raise this for transient failures that should be retried."""
    pass


class NonRetryableError(Exception):
    """Raise this for permanent failures that should go to DLQ immediately."""
    pass


class BaseConsumer:
    def __init__(
        self,
        topics: list[str],
        group_id: str,
        dlq_topic: str,
        consumer_name: str,
    ):
        self.topics        = topics
        self.group_id      = group_id
        self.dlq_topic     = dlq_topic
        self.consumer_name = consumer_name
        self._consumer     = None
        self._running      = False

    def _build_consumer_config(self) -> dict:
        brokers = os.getenv("KAFKA_BROKERS", "localhost:9092")
        config  = {
            "bootstrap.servers":  brokers,
            "group.id":           self.group_id,
            "auto.offset.reset":  "earliest",
            "enable.auto.commit": False,   # manual commit after processing
            "session.timeout.ms": 30000,
            "max.poll.interval.ms": 300000,
        }
        if os.getenv("KAFKA_SASL_USERNAME"):
            config.update({
                "security.protocol": "SASL_SSL",
                "sasl.mechanism":    "SCRAM-SHA-256",
                "sasl.username":     os.getenv("KAFKA_SASL_USERNAME"),
                "sasl.password":     os.getenv("KAFKA_SASL_PASSWORD"),
            })
        return config

    async def start(self) -> bool:
        """Initialize Kafka consumer. Returns True if started, False if Kafka unavailable."""
        try:
            from confluent_kafka import Consumer
            config = self._build_consumer_config()
            self._consumer = Consumer(config)
            self._consumer.subscribe(self.topics)
            self._running = True
            logger.info(
                f"Consumer started: name={self.consumer_name} "
                f"group={self.group_id} topics={self.topics}"
            )
            return True
        except ImportError:
            logger.warning("confluent-kafka not installed — consumer disabled")
            return False
        except Exception as e:
            logger.error(f"Consumer init failed: {e}")
            return False

    async def stop(self) -> None:
        self._running = False
        if self._consumer:
            self._consumer.close()
            self._consumer = None

    @abstractmethod
    async def handle(self, event: dict, payload: dict) -> None:
        """
        Process a single event. Must raise:
          RetryableError    — for transient failures
          NonRetryableError — for permanent failures
        Normal completion = success.
        """
        raise NotImplementedError

    async def _check_idempotency(self, event_id: str) -> bool:
        """Returns True if already processed (should skip)."""
        from db.operations import is_event_processed
        return await is_event_processed(event_id, self.consumer_name)

    async def _mark_processed(self, event_id: str) -> None:
        from db.operations import mark_event_processed
        await mark_event_processed(event_id, self.consumer_name)

    async def _process_with_retry(self, event: dict) -> None:
        """
        Wrap handle() with:
        1. Idempotency check
        2. Exponential backoff retry
        3. DLQ on max retries
        """
        from observability import metrics
        from kafka.producer import produce_dlq_event

        event_id       = event.get("event_id", "unknown")
        correlation_id = event.get("correlation_id", "unknown")
        event_type     = event.get("event_type", "unknown")
        payload        = event.get("payload", {})

        # ── Idempotency check ──────────────────────────────────
        if await self._check_idempotency(event_id):
            logger.info(
                f"Skipping duplicate event: event_id={event_id} "
                f"consumer={self.consumer_name}"
            )
            return

        # ── Retry loop ─────────────────────────────────────────
        last_error = None
        for attempt in range(MAX_RETRIES + 1):
            try:
                logger.info(
                    f"Processing: event_id={event_id} type={event_type} "
                    f"attempt={attempt+1} consumer={self.consumer_name} "
                    f"correlation_id={correlation_id}"
                )
                await self.handle(event, payload)

                # Success — mark processed
                await self._mark_processed(event_id)

                try:
                    metrics.kafka_events_consumed_total.labels(
                        topic=self.topics[0] if self.topics else "unknown",
                        consumer_group=self.group_id
                    ).inc()
                except Exception:
                    pass

                logger.info(
                    f"Success: event_id={event_id} consumer={self.consumer_name}"
                )
                return

            except NonRetryableError as e:
                logger.error(
                    f"NonRetryable failure: event_id={event_id} "
                    f"consumer={self.consumer_name} error={e}"
                )
                last_error = str(e)
                break   # go straight to DLQ

            except RetryableError as e:
                last_error = str(e)
                if attempt < MAX_RETRIES:
                    wait = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS)-1)]
                    logger.warning(
                        f"Retryable failure: event_id={event_id} "
                        f"attempt={attempt+1}/{MAX_RETRIES} "
                        f"waiting={wait}s error={e}"
                    )
                    await asyncio.sleep(wait)
                else:
                    logger.error(
                        f"Max retries exhausted: event_id={event_id} "
                        f"consumer={self.consumer_name}"
                    )

            except Exception as e:
                # Unexpected error — treat as retryable
                last_error = str(e)
                if attempt < MAX_RETRIES:
                    wait = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS)-1)]
                    logger.exception(
                        f"Unexpected error (will retry): event_id={event_id} "
                        f"attempt={attempt+1} wait={wait}s"
                    )
                    await asyncio.sleep(wait)

        # ── Max retries → DLQ ──────────────────────────────────
        await produce_dlq_event(
            dlq_topic     = self.dlq_topic,
            original_event = event,
            consumer_name = self.consumer_name,
            failure_reason = last_error or "unknown",
            retry_count   = MAX_RETRIES,
        )
        try:
            metrics.kafka_consumer_errors_total.labels(
                topic=self.topics[0] if self.topics else "unknown",
                error_type="max_retries_exceeded"
            ).inc()
        except Exception:
            pass

    async def run(self) -> None:
        """
        Main consumer loop. Polls Kafka, processes messages,
        commits offset only after successful processing.
        """
        if not self._consumer:
            ok = await self.start()
            if not ok:
                logger.warning(f"Consumer {self.consumer_name} could not start — exiting")
                return

        logger.info(f"Consumer loop starting: {self.consumer_name}")

        while self._running:
            try:
                msg = self._consumer.poll(timeout=1.0)

                if msg is None:
                    continue   # no message, keep polling

                if msg.error():
                    from confluent_kafka import KafkaError
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error(f"Kafka consumer error: {msg.error()}")
                    continue

                # Deserialize
                try:
                    event = json.loads(msg.value().decode("utf-8"))
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to deserialize message: {e}")
                    self._consumer.commit(message=msg)
                    continue

                # Process (with retry + idempotency)
                await self._process_with_retry(event)

                # Manual commit AFTER successful processing
                self._consumer.commit(message=msg)

            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.exception(f"Consumer loop error: {e}")
                await asyncio.sleep(1)

        await self.stop()
        logger.info(f"Consumer stopped: {self.consumer_name}")
