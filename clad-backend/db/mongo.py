"""
db/mongo.py  —  Async MongoDB client for Clad Insurance API
============================================================
Uses Motor (async PyMongo driver) so all DB calls are non-blocking
inside FastAPI's async event loop.

Collections:
  workers          — gig worker profiles + risk scores
  policies         — insurance policies (1:1 with worker)
  claims           — insurance claims + embedded fraud results
  payouts          — payout records (UNIQUE on claim_id)
  trigger_events   — environmental disruption events
  outbox_events    — transactional outbox for Kafka publishing
  processed_events — idempotency records per (event_id, consumer)
  etl_checkpoints  — watermarks for MongoDB → Snowflake ETL

Connection is lazily established on first use.
Call init_db() at application startup.
"""

import os
import logging
from typing import Optional

try:
    from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
    MOTOR_AVAILABLE = True
except ImportError:
    MOTOR_AVAILABLE = False

logger = logging.getLogger("clad.db")

# ── Singleton client ───────────────────────────────────────────
_client: Optional[object] = None
_db:     Optional[object] = None

# Collection name constants
WORKERS          = "workers"
POLICIES         = "policies"
CLAIMS           = "claims"
PAYOUTS          = "payouts"
TRIGGER_EVENTS   = "trigger_events"
OUTBOX_EVENTS    = "outbox_events"
PROCESSED_EVENTS = "processed_events"
ETL_CHECKPOINTS  = "etl_checkpoints"


def get_mongo_uri() -> str:
    uri = os.getenv("MONGO_URI", "")
    if not uri:
        # Local dev default (Docker Compose)
        uri = "mongodb://localhost:27017"
    return uri


def get_db_name() -> str:
    return os.getenv("MONGO_DB_NAME", "clad_insurance")


async def init_db() -> None:
    """
    Initialize the Motor client and create indexes.
    Call once at FastAPI startup (lifespan event).
    """
    global _client, _db

    if not MOTOR_AVAILABLE:
        logger.warning(
            "Motor not installed — running in legacy JSON mode. "
            "Install with: pip install motor"
        )
        return

    uri     = get_mongo_uri()
    db_name = get_db_name()

    logger.info(f"Connecting to MongoDB: {uri[:40]}... db={db_name}")
    _client = AsyncIOMotorClient(uri, serverSelectionTimeoutMS=5000)
    _db     = _client[db_name]

    # Verify connection
    try:
        await _client.admin.command("ping")
        logger.info("MongoDB connection established")
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        # Don't crash startup — fall back to JSON mode
        _client = None
        _db     = None
        return

    # Create all indexes
    from db.indexes import create_all_indexes
    await create_all_indexes(_db)
    logger.info("MongoDB indexes verified")


async def close_db() -> None:
    """Close the Motor client. Call at FastAPI shutdown."""
    global _client, _db
    if _client:
        _client.close()
        _client = None
        _db     = None
        logger.info("MongoDB connection closed")


def get_db():
    """Return the database instance (sync accessor for sync contexts)."""
    return _db


def is_connected() -> bool:
    """Check if MongoDB is connected and available."""
    return _db is not None


# ── Collection accessors ───────────────────────────────────────
# Returns None if not connected (callers must handle gracefully)

def workers_col():
    return _db[WORKERS] if _db else None

def policies_col():
    return _db[POLICIES] if _db else None

def claims_col():
    return _db[CLAIMS] if _db else None

def payouts_col():
    return _db[PAYOUTS] if _db else None

def trigger_events_col():
    return _db[TRIGGER_EVENTS] if _db else None

def outbox_events_col():
    return _db[OUTBOX_EVENTS] if _db else None

def processed_events_col():
    return _db[PROCESSED_EVENTS] if _db else None

def etl_checkpoints_col():
    return _db[ETL_CHECKPOINTS] if _db else None


async def readiness_check() -> dict:
    """
    Check if MongoDB is reachable — used by /readiness endpoint.
    Returns dict with status and latency.
    """
    import time
    if not _client:
        return {"status": "disconnected", "latency_ms": None}
    try:
        t0 = time.monotonic()
        await _client.admin.command("ping")
        latency_ms = round((time.monotonic() - t0) * 1000, 1)
        return {"status": "ok", "latency_ms": latency_ms}
    except Exception as e:
        return {"status": "error", "error": str(e)[:100], "latency_ms": None}
