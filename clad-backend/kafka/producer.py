"""
kafka/producer.py  —  Kafka producer for Clad
==============================================
Wraps confluent-kafka Producer with:
  - JSON serialization
  - Delivery confirmation callbacks
  - Graceful fallback when Kafka unavailable
  - Prometheus metric instrumentation

Usage:
    from kafka.producer import produce_event
    await produce_event(topic=CLAIM_CREATED, key=claim_id, payload={...})
"""

import os
import json
import logging
import asyncio
from typing import Optional
from datetime import datetime
from uuid import uuid4

logger = logging.getLogger("clad.kafka.producer")

_producer = None
_kafka_available = False


def _get_kafka_config() -> dict:
    """Build confluent-kafka producer config from environment."""
    brokers = os.getenv("KAFKA_BROKERS", "localhost:9092")
    config  = {
        "bootstrap.servers": brokers,
        "client.id":         "clad-api-producer",
        "acks":              "all",        # wait for all ISR replicas
        "retries":           3,
        "retry.backoff.ms":  500,
        "compression.type":  "gzip",
        "linger.ms":         5,            # small batching window
    }
    # Upstash / cloud Kafka: add SASL/SSL config
    if os.getenv("KAFKA_SASL_USERNAME"):
        config.update({
            "security.protocol":  "SASL_SSL",
            "sasl.mechanism":     "SCRAM-SHA-256",
            "sasl.username":      os.getenv("KAFKA_SASL_USERNAME"),
            "sasl.password":      os.getenv("KAFKA_SASL_PASSWORD"),
        })
    return config


def init_producer() -> None:
    """Initialize the Kafka producer. Call once at startup."""
    global _producer, _kafka_available
    try:
        from confluent_kafka import Producer
        config = _get_kafka_config()
        _producer = Producer(config)
        _kafka_available = True
        logger.info(f"Kafka producer initialized: {config['bootstrap.servers']}")
    except ImportError:
        logger.warning("confluent-kafka not installed — Kafka disabled")
        _kafka_available = False
    except Exception as e:
        logger.warning(f"Kafka producer init failed: {e} — continuing without Kafka")
        _kafka_available = False


def close_producer() -> None:
    global _producer
    if _producer:
        _producer.flush(timeout=5)
        _producer = None


def is_available() -> bool:
    return _kafka_available and _producer is not None


def _on_delivery(err, msg):
    """Delivery callback — called by confluent-kafka on produce completion."""
    from observability.metrics import kafka_events_produced_total
    if err:
        logger.error(f"Kafka delivery failed: topic={msg.topic()} err={err}")
    else:
        topic = msg.topic()
        logger.debug(f"Kafka delivered: topic={topic} partition={msg.partition()} offset={msg.offset()}")
        try:
            kafka_events_produced_total.labels(topic=topic).inc()
        except Exception:
            pass


def build_event_envelope(
    event_type: str,
    payload: dict,
    correlation_id: Optional[str] = None,
    producer_name: str = "clad-api",
) -> dict:
    """
    Build the standard event envelope used by all Clad producers.

    {
      "event_id":      str  — unique per event
      "event_type":    str  — e.g. "claim.created"
      "event_version": int  — schema version for forward compatibility
      "timestamp":     str  — ISO8601 UTC
      "correlation_id": str — trace across the pipeline
      "producer":      str  — service that produced this event
      "payload":       dict — business data
    }
    """
    return {
        "event_id":       str(uuid4()),
        "event_type":     event_type,
        "event_version":  1,
        "timestamp":      datetime.utcnow().isoformat() + "Z",
        "correlation_id": correlation_id or str(uuid4()),
        "producer":       producer_name,
        "payload":        payload,
    }


async def produce_event(
    topic: str,
    key: str,
    payload: dict,
    correlation_id: Optional[str] = None,
    event_type: Optional[str] = None,
) -> Optional[str]:
    """
    Produce an event to a Kafka topic.

    Args:
        topic:          Kafka topic name (use kafka.topics constants)
        key:            Partition key (claim_id, pincode, etc.)
        payload:        Business event payload dict
        correlation_id: Propagated trace ID
        event_type:     If None, uses topic name as event_type

    Returns:
        event_id if produced successfully, None if Kafka unavailable.

    Note: This function does NOT crash if Kafka is down.
    The outbox pattern ensures events are re-published when Kafka recovers.
    """
    if not is_available():
        logger.debug(f"Kafka unavailable — event will be published via outbox: {topic}")
        return None

    envelope = build_event_envelope(
        event_type    = event_type or topic,
        payload       = payload,
        correlation_id = correlation_id,
    )
    value_bytes = json.dumps(envelope, default=str).encode("utf-8")
    key_bytes   = str(key).encode("utf-8") if key else None

    try:
        # confluent-kafka produce is non-blocking; callback fires later
        _producer.produce(
            topic    = topic,
            key      = key_bytes,
            value    = value_bytes,
            callback = _on_delivery,
        )
        # Poll to trigger delivery callbacks (non-blocking)
        _producer.poll(0)
        logger.debug(f"Kafka produce queued: topic={topic} key={key}")
        return envelope["event_id"]

    except Exception as e:
        logger.error(f"Kafka produce failed: topic={topic} error={e}")
        return None


async def produce_dlq_event(
    dlq_topic: str,
    original_event: dict,
    consumer_name: str,
    failure_reason: str,
    retry_count: int,
) -> None:
    """Publish a failed event to its Dead Letter Queue topic."""
    dlq_payload = {
        "original_event":  original_event,
        "dlq_metadata": {
            "consumer_name":  consumer_name,
            "failure_reason": failure_reason[:500],
            "retry_count":    retry_count,
            "dlq_timestamp":  datetime.utcnow().isoformat() + "Z",
            "correlation_id": original_event.get("correlation_id"),
        }
    }
    await produce_event(
        topic        = dlq_topic,
        key          = original_event.get("event_id", "unknown"),
        payload      = dlq_payload,
        correlation_id = original_event.get("correlation_id"),
        event_type   = f"{dlq_topic}.entry",
    )
    from observability.metrics import kafka_dlq_events_total
    try:
        kafka_dlq_events_total.labels(topic=dlq_topic).inc()
    except Exception:
        pass
    logger.warning(
        f"DLQ: topic={dlq_topic} event_id={original_event.get('event_id')} "
        f"reason={failure_reason[:100]} retries={retry_count}"
    )


async def readiness_check() -> dict:
    """Check if Kafka is reachable — for /readiness endpoint."""
    if not _kafka_available or not _producer:
        return {"status": "disconnected"}
    try:
        # Try listing metadata — this verifies broker connectivity
        metadata = _producer.list_topics(timeout=3)
        return {
            "status":       "ok",
            "broker_count": len(metadata.brokers),
            "topic_count":  len(metadata.topics),
        }
    except Exception as e:
        return {"status": "error", "error": str(e)[:100]}
