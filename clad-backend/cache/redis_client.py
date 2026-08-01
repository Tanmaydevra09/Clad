"""
cache/redis_client.py  —  Redis async client for Clad
======================================================
Used for:
  - Short-lived environmental caching (weather, AQI, trigger results)
  - Redis-backed rate limiting (via slowapi)
  - Future: distributed locks

Keys and TTLs:
  weather:{pincode}   → 300s (5 min)
  aqi:{pincode}       → 300s (5 min)
  trigger:{pincode}   → 120s (2 min)
  premium:{hash}      → 600s (10 min)
  ratelimit:{ip}:{ep} → 60s  (sliding window)

Redis is NOT used for:
  - claims, policies, payouts, workers (MongoDB only)
  - financial state

If Redis is unavailable, all functions return None / False gracefully.
The application should continue operating — cache misses fall through to APIs.
"""

import os
import json
import logging
import asyncio
from typing import Optional, Any

logger = logging.getLogger("clad.cache")

_redis_client = None

# TTLs
TTL_WEATHER = 300   # 5 min
TTL_AQI     = 300   # 5 min
TTL_TRIGGER = 120   # 2 min
TTL_PREMIUM = 600   # 10 min


async def init_redis() -> None:
    """Initialize the async Redis client. Call at FastAPI startup."""
    global _redis_client
    try:
        import redis.asyncio as aioredis
        url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        _redis_client = aioredis.from_url(
            url,
            encoding="utf-8",
            decode_responses=True,
            socket_connect_timeout=3,
            socket_timeout=3,
        )
        # Verify connection
        await _redis_client.ping()
        logger.info(f"Redis connected: {url[:30]}...")
    except ImportError:
        logger.warning("redis package not installed — caching disabled")
        _redis_client = None
    except Exception as e:
        logger.warning(f"Redis unavailable — caching disabled: {e}")
        _redis_client = None


async def close_redis() -> None:
    global _redis_client
    if _redis_client:
        await _redis_client.aclose()
        _redis_client = None


def is_connected() -> bool:
    return _redis_client is not None


# ── Cache helpers ──────────────────────────────────────────────

async def cache_get(key: str) -> Optional[Any]:
    """Get a JSON-serialized value. Returns None on miss or error."""
    if not _redis_client:
        return None
    try:
        val = await _redis_client.get(key)
        if val is None:
            return None
        return json.loads(val)
    except Exception as e:
        logger.debug(f"Redis GET {key} error: {e}")
        return None


async def cache_set(key: str, value: Any, ttl: int) -> bool:
    """Set a JSON-serialized value with TTL seconds. Returns success."""
    if not _redis_client:
        return False
    try:
        await _redis_client.setex(key, ttl, json.dumps(value, default=str))
        return True
    except Exception as e:
        logger.debug(f"Redis SET {key} error: {e}")
        return False


async def cache_delete(key: str) -> None:
    if not _redis_client:
        return
    try:
        await _redis_client.delete(key)
    except Exception:
        pass


# ── Domain-specific cache functions ────────────────────────────

async def get_weather(pincode: str) -> Optional[dict]:
    return await cache_get(f"weather:{pincode}")

async def set_weather(pincode: str, data: dict) -> None:
    await cache_set(f"weather:{pincode}", data, TTL_WEATHER)


async def get_aqi(pincode: str) -> Optional[dict]:
    return await cache_get(f"aqi:{pincode}")

async def set_aqi(pincode: str, data: dict) -> None:
    await cache_set(f"aqi:{pincode}", data, TTL_AQI)


async def get_trigger(pincode: str) -> Optional[dict]:
    return await cache_get(f"trigger:{pincode}")

async def set_trigger(pincode: str, data: dict) -> None:
    await cache_set(f"trigger:{pincode}", data, TTL_TRIGGER)


async def get_premium_cache(cache_key: str) -> Optional[dict]:
    return await cache_get(f"premium:{cache_key}")

async def set_premium_cache(cache_key: str, data: dict) -> None:
    await cache_set(f"premium:{cache_key}", data, TTL_PREMIUM)


# ── Cache statistics ───────────────────────────────────────────

_stats = {"hits": 0, "misses": 0}


async def cache_get_tracked(key: str) -> Optional[Any]:
    """Cache get with hit/miss tracking for metrics."""
    val = await cache_get(key)
    if val is not None:
        _stats["hits"] += 1
    else:
        _stats["misses"] += 1
    return val


def get_cache_stats() -> dict:
    total = _stats["hits"] + _stats["misses"]
    hit_rate = round(_stats["hits"] / total * 100, 1) if total > 0 else 0
    return {
        "hits":     _stats["hits"],
        "misses":   _stats["misses"],
        "total":    total,
        "hit_rate": hit_rate,
    }


async def readiness_check() -> dict:
    """Check Redis is reachable — for /readiness endpoint."""
    import time
    if not _redis_client:
        return {"status": "disconnected", "latency_ms": None}
    try:
        t0 = time.monotonic()
        await _redis_client.ping()
        latency_ms = round((time.monotonic() - t0) * 1000, 1)
        return {"status": "ok", "latency_ms": latency_ms}
    except Exception as e:
        return {"status": "error", "error": str(e)[:100], "latency_ms": None}
