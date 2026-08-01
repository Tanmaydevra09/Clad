"""
db/operations.py  —  All MongoDB read/write operations for Clad
================================================================
Centralises every DB call so route handlers and services
import from here rather than calling Motor directly.

Design rules:
  - Every function is async
  - Returns plain dicts (not Motor documents) for JSON serializability
  - ObjectId is excluded from returned dicts
  - Falls back to core.db (JSON) if MongoDB is not connected
"""

import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from uuid import uuid4

logger = logging.getLogger("clad.db.ops")

# ── Projection that strips _id ─────────────────────────────────
_NO_ID = {"_id": 0}


def _strip_id(doc: dict) -> dict:
    """Remove MongoDB _id from a document dict."""
    if doc is None:
        return None
    doc.pop("_id", None)
    return doc


def _flatten_worker(doc: dict) -> dict:
    """
    Convert nested MongoDB worker document → flat dict
    that matches the original JSON schema for API compatibility.
    The frontend and existing routes expect a flat dict.
    """
    if doc is None:
        return None
    flat = {
        "name":             doc.get("name"),
        "pincode":          doc.get("pincode"),
        "plan":             doc.get("plan", "plus"),
        "pan_number":       doc.get("pan_number"),
        "pan_verified":     doc.get("pan_verified", False),
        "platform_links":   doc.get("platform_links", []),
        "policy_paused":    doc.get("policy_paused", False),
        "registered_at":    _fmt_dt(doc.get("registered_at")),
        "updated_at":       _fmt_dt(doc.get("updated_at")),
    }
    # Flatten nested sub-documents
    dp = doc.get("delivery_profile", {})
    flat.update({
        "total_deliveries":     dp.get("total_deliveries", 0),
        "has_delivery_history": dp.get("has_delivery_history", False),
        "account_age_days":     dp.get("account_age_days", 90),
        "delivery_consistency": dp.get("delivery_consistency", 0.80),
        "avg_daily_earning":    dp.get("avg_daily_earning", 600.0),
    })
    rp = doc.get("risk_profile", {})
    flat.update({
        "claim_free_weeks":     rp.get("claim_free_weeks", 0),
        "past_claims_count":    rp.get("past_claims_count", 0),
        "location_honesty":     rp.get("location_honesty", 0.85),
        "claim_history_score":  rp.get("claim_history_score", 1.0),
        "fraudulent_flags":     rp.get("fraudulent_flags", 0),
        "clad_score":           rp.get("clad_score"),
        "integrity_score":      rp.get("integrity_score"),
        "integrity_flags":      rp.get("integrity_flags", []),
        "integrity_passes_gate": rp.get("integrity_passes_gate", True),
    })
    return flat


def _fmt_dt(dt) -> Optional[str]:
    if dt is None:
        return None
    if isinstance(dt, str):
        return dt
    if isinstance(dt, datetime):
        return dt.isoformat() + "Z"
    return str(dt)


def _flatten_claim(doc: dict) -> dict:
    """Flatten claim MongoDB doc → API-compatible dict."""
    if doc is None:
        return None
    flat = {k: v for k, v in doc.items() if k not in ("_id",)}
    flat["created_at"] = _fmt_dt(flat.get("created_at"))
    flat["updated_at"] = _fmt_dt(flat.get("updated_at"))
    flat["payout_at"]  = _fmt_dt(flat.get("payout_at"))
    # Expose fraud_score at top level for backward compat
    fr = flat.get("fraud_result") or {}
    if fr and "fraud_score" not in flat:
        flat["fraud_score"] = fr.get("score")
    return flat


# ══════════════════════════════════════════════════════════════
# WORKERS
# ══════════════════════════════════════════════════════════════

async def get_worker_by_name(name: str) -> Optional[dict]:
    from db.mongo import workers_col
    col = workers_col()
    if col is None:
        return _json_get_worker(name)
    doc = await col.find_one({"name": name}, _NO_ID)
    return _flatten_worker(doc)


async def get_all_workers() -> List[dict]:
    from db.mongo import workers_col
    col = workers_col()
    if col is None:
        return _json_get_all_workers()
    cursor = col.find({}, _NO_ID)
    docs = await cursor.to_list(length=500)
    return [_flatten_worker(d) for d in docs]


async def create_worker(worker_data: dict) -> dict:
    """Insert new worker. Raises DuplicateKeyError if name exists."""
    from db.mongo import workers_col
    col = workers_col()
    if col is None:
        return _json_create_worker(worker_data)

    dp = worker_data.get("delivery_profile", {})
    rp = worker_data.get("risk_profile", {})

    doc = {
        "name":          worker_data["name"],
        "pincode":       str(worker_data.get("pincode", "560034")),
        "plan":          worker_data.get("plan", "plus"),
        "pan_number":    worker_data.get("pan_number"),
        "pan_verified":  bool(worker_data.get("pan_verified", False)),
        "platform_links": worker_data.get("platform_links", []),
        "policy_paused": False,
        "delivery_profile": {
            "total_deliveries":    int(worker_data.get("total_deliveries", 0)),
            "has_delivery_history": bool(worker_data.get("has_delivery_history", False)),
            "account_age_days":    int(worker_data.get("account_age_days", 90)),
            "delivery_consistency": float(worker_data.get("delivery_consistency", 0.80)),
            "avg_daily_earning":   float(worker_data.get("avg_daily_earning", 600.0)),
        },
        "risk_profile": {
            "claim_free_weeks":    int(worker_data.get("claim_free_weeks", 0)),
            "past_claims_count":   int(worker_data.get("past_claims_count", 0)),
            "location_honesty":    float(worker_data.get("location_honesty", 0.85)),
            "claim_history_score": float(worker_data.get("claim_history_score", 1.0)),
            "fraudulent_flags":    int(worker_data.get("fraudulent_flags", 0)),
            "clad_score":          worker_data.get("clad_score"),
            "integrity_score":     worker_data.get("integrity_score"),
            "integrity_flags":     worker_data.get("integrity_flags", []),
            "integrity_passes_gate": bool(worker_data.get("integrity_passes_gate", True)),
        },
        "registered_at": datetime.utcnow(),
        "updated_at":    datetime.utcnow(),
    }
    await col.insert_one(doc)
    return _flatten_worker(doc)


async def update_worker_fields(name: str, fields: dict) -> bool:
    """Update arbitrary flat fields on a worker doc."""
    from db.mongo import workers_col
    col = workers_col()
    if col is None:
        return _json_update_worker(name, fields)

    # Separate top-level vs nested updates
    set_dict = {"updated_at": datetime.utcnow()}
    nested_mapping = {
        "total_deliveries":    "delivery_profile.total_deliveries",
        "has_delivery_history":"delivery_profile.has_delivery_history",
        "account_age_days":    "delivery_profile.account_age_days",
        "delivery_consistency":"delivery_profile.delivery_consistency",
        "avg_daily_earning":   "delivery_profile.avg_daily_earning",
        "claim_free_weeks":    "risk_profile.claim_free_weeks",
        "past_claims_count":   "risk_profile.past_claims_count",
        "location_honesty":    "risk_profile.location_honesty",
        "claim_history_score": "risk_profile.claim_history_score",
        "fraudulent_flags":    "risk_profile.fraudulent_flags",
        "clad_score":          "risk_profile.clad_score",
        "integrity_score":     "risk_profile.integrity_score",
        "integrity_flags":     "risk_profile.integrity_flags",
        "integrity_passes_gate":"risk_profile.integrity_passes_gate",
    }
    for k, v in fields.items():
        mongo_key = nested_mapping.get(k, k)
        set_dict[mongo_key] = v

    result = await col.update_one({"name": name}, {"$set": set_dict})
    return result.modified_count > 0


async def update_worker_plan(name: str, plan: str) -> bool:
    from db.mongo import workers_col
    col = workers_col()
    if col is None:
        return _json_update_worker(name, {"plan": plan})
    result = await col.update_one(
        {"name": name},
        {"$set": {"plan": plan, "updated_at": datetime.utcnow()}}
    )
    return result.modified_count > 0


async def update_worker_pan(name: str, pan_number: str) -> bool:
    from db.mongo import workers_col
    col = workers_col()
    if col is None:
        return _json_update_worker(name, {"pan_number": pan_number, "pan_verified": True})
    result = await col.update_one(
        {"name": name},
        {"$set": {"pan_number": pan_number, "pan_verified": True, "updated_at": datetime.utcnow()}}
    )
    return result.modified_count > 0


async def toggle_policy_pause(name: str) -> Optional[bool]:
    """Toggle policy_paused. Returns new paused state."""
    from db.mongo import workers_col
    col = workers_col()
    if col is None:
        return _json_toggle_pause(name)
    doc = await col.find_one({"name": name}, {"policy_paused": 1, "_id": 0})
    if doc is None:
        return None
    new_state = not bool(doc.get("policy_paused", False))
    await col.update_one(
        {"name": name},
        {"$set": {"policy_paused": new_state, "updated_at": datetime.utcnow()}}
    )
    return new_state


async def increment_fraudulent_flags(name: str) -> None:
    from db.mongo import workers_col
    col = workers_col()
    if col is None:
        _json_inc_fraud_flags(name)
        return
    await col.update_one(
        {"name": name},
        {"$inc": {"risk_profile.fraudulent_flags": 1},
         "$set": {"updated_at": datetime.utcnow()}}
    )


# ══════════════════════════════════════════════════════════════
# POLICIES
# ══════════════════════════════════════════════════════════════

PLAN_META = {
    "basic": {"weekly_premium": 29, "weekly_cap": 800},
    "plus":  {"weekly_premium": 49, "weekly_cap": 1500},
    "pro":   {"weekly_premium": 79, "weekly_cap": 2500},
}


async def get_policy_by_worker(name: str) -> Optional[dict]:
    from db.mongo import policies_col
    col = policies_col()
    if col is None:
        return _json_get_policy(name)
    doc = await col.find_one({"worker_name": name}, _NO_ID)
    return _strip_id(doc)


async def get_all_policies() -> List[dict]:
    from db.mongo import policies_col
    col = policies_col()
    if col is None:
        return _json_get_all_policies()
    cursor = col.find({}, _NO_ID)
    return await cursor.to_list(length=500)


async def upsert_policy(worker_name: str, plan: str) -> dict:
    from db.mongo import policies_col
    col = policies_col()
    meta = PLAN_META.get(plan, PLAN_META["plus"])
    now  = datetime.utcnow()

    if col is None:
        return _json_upsert_policy(worker_name, plan)

    existing = await col.find_one({"worker_name": worker_name})
    if existing:
        await col.update_one(
            {"worker_name": worker_name},
            {"$set": {"plan": plan, "weekly_premium": meta["weekly_premium"],
                      "weekly_cap": meta["weekly_cap"], "updated_at": now,
                      "status": "active"}}
        )
        doc = await col.find_one({"worker_name": worker_name}, _NO_ID)
        return _strip_id(doc)

    doc = {
        "worker_name":    worker_name,
        "plan":           plan,
        "status":         "active",
        "weekly_premium": meta["weekly_premium"],
        "weekly_cap":     meta["weekly_cap"],
        "created_at":     now,
        "updated_at":     now,
    }
    await col.insert_one(doc)
    doc.pop("_id", None)
    return doc


async def update_policy_status(worker_name: str, status: str) -> None:
    from db.mongo import policies_col
    col = policies_col()
    if col is None:
        _json_set_policy_status(worker_name, status)
        return
    await col.update_one(
        {"worker_name": worker_name},
        {"$set": {"status": status, "updated_at": datetime.utcnow()}}
    )


# ══════════════════════════════════════════════════════════════
# CLAIMS
# ══════════════════════════════════════════════════════════════

def _next_claim_id(db_date: Optional[datetime] = None) -> str:
    """Generate stable business claim ID."""
    dt  = db_date or datetime.utcnow()
    uid = str(uuid4())[:4].upper()
    return f"CLM-{dt.strftime('%Y%m%d')}-{uid}"


async def count_claims() -> int:
    from db.mongo import claims_col
    col = claims_col()
    if col is None:
        from core.db import claims as _c; return len(_c)
    return await col.count_documents({})


async def create_claim_doc(
    worker_name: str,
    pincode: str,
    amount: float,
    reason: str,
    trigger_type: str,
    status: str,
    fraud_result: Optional[dict],
    vision_result: Optional[dict],
    payout_speed: str,
    review_note: str,
    photo_verified: bool = False,
) -> dict:
    """
    Insert a new claim into MongoDB.
    Returns the created document (without _id).
    """
    from db.mongo import claims_col
    col = claims_col()
    now     = datetime.utcnow()
    claim_id = _next_claim_id(now)

    doc = {
        "claim_id":       claim_id,
        "worker_name":    worker_name,
        "pincode":        str(pincode),
        "amount":         round(float(amount), 2),
        "reason":         reason,
        "trigger_type":   trigger_type,
        "status":         status,
        "fraud_result":   fraud_result,
        "vision_result":  vision_result,
        "payout_speed":   payout_speed,
        "review_note":    review_note,
        "photo_verified": photo_verified,
        "payout_processed": False,
        "payout_id":      None,
        "payout_upi":     None,
        "payout_at":      None,
        "created_at":     now,
        "updated_at":     now,
    }

    if col is None:
        return _json_create_claim(doc)

    await col.insert_one(doc)
    doc.pop("_id", None)
    return doc


async def get_claim_by_id(claim_id: str) -> Optional[dict]:
    """Find by business claim_id string (e.g. CLM-20260801-ABCD)."""
    from db.mongo import claims_col
    col = claims_col()
    if col is None:
        return _json_get_claim(claim_id)
    doc = await col.find_one({"claim_id": claim_id}, _NO_ID)
    return _flatten_claim(doc)


async def get_claims_by_worker(worker_name: str) -> List[dict]:
    from db.mongo import claims_col
    col = claims_col()
    if col is None:
        return _json_get_claims_by_worker(worker_name)
    cursor = col.find(
        {"worker_name": worker_name},
        _NO_ID,
        sort=[("created_at", -1)]
    )
    docs = await cursor.to_list(length=200)
    return [_flatten_claim(d) for d in docs]


async def get_all_claims() -> List[dict]:
    from db.mongo import claims_col
    col = claims_col()
    if col is None:
        from core.db import claims as _c; return list(_c)
    cursor = col.find({}, _NO_ID, sort=[("created_at", -1)])
    docs = await cursor.to_list(length=1000)
    return [_flatten_claim(d) for d in docs]


async def update_claim_status(claim_id: str, status: str, extra: Optional[dict] = None) -> bool:
    from db.mongo import claims_col
    col = claims_col()
    if col is None:
        return _json_update_claim(claim_id, {"status": status, **(extra or {})})
    update = {"status": status, "updated_at": datetime.utcnow()}
    if extra:
        update.update(extra)
    result = await col.update_one({"claim_id": claim_id}, {"$set": update})
    return result.modified_count > 0


async def mark_payout_processed(claim_id: str, payout_id: str, upi: str) -> bool:
    from db.mongo import claims_col
    col = claims_col()
    if col is None:
        return _json_update_claim(claim_id, {
            "payout_processed": True, "payout_id": payout_id,
            "payout_upi": upi, "payout_at": datetime.utcnow().isoformat() + "Z"
        })
    result = await col.update_one(
        {"claim_id": claim_id},
        {"$set": {
            "payout_processed": True,
            "payout_id":        payout_id,
            "payout_upi":       upi,
            "payout_at":        datetime.utcnow(),
            "updated_at":       datetime.utcnow(),
        }}
    )
    return result.modified_count > 0


async def get_approved_unpaid_claims() -> List[dict]:
    from db.mongo import claims_col
    col = claims_col()
    if col is None:
        from core.db import claims as _c
        return [c for c in _c if c.get("status") == "approved" and not c.get("payout_processed")]
    cursor = col.find(
        {"status": "approved", "payout_processed": False},
        _NO_ID
    )
    docs = await cursor.to_list(length=500)
    return [_flatten_claim(d) for d in docs]


# ══════════════════════════════════════════════════════════════
# PAYOUTS
# ══════════════════════════════════════════════════════════════

async def create_payout_record(
    claim_id: str,
    worker_name: str,
    amount: float,
    upi_vpa: str,
    razorpay_data: dict,
    mode: str,
) -> dict:
    """
    Insert a payout record.
    Raises pymongo.errors.DuplicateKeyError if claim_id already has a payout
    — this is the DB-level idempotency guard.
    """
    from db.mongo import payouts_col
    col = payouts_col()

    idempotency_key = f"CLAD-{claim_id}"   # deterministic — no random
    now = datetime.utcnow()

    doc = {
        "claim_id":           claim_id,
        "worker_name":        worker_name,
        "amount":             round(float(amount), 2),
        "upi_vpa":            upi_vpa,
        "idempotency_key":    idempotency_key,
        "status":             "completed",
        "razorpay_contact_id": razorpay_data.get("contact_id"),
        "razorpay_fa_id":     razorpay_data.get("fund_account_id"),
        "razorpay_payout_id": razorpay_data.get("payout_id"),
        "razorpay_mode":      mode,
        "processed_at":       now,
        "created_at":         now,
        "updated_at":         now,
    }

    if col is None:
        return _json_create_payout(doc)

    await col.insert_one(doc)   # raises DuplicateKeyError on second call
    doc.pop("_id", None)
    return doc


# ══════════════════════════════════════════════════════════════
# OUTBOX EVENTS
# ══════════════════════════════════════════════════════════════

async def write_outbox_event(
    aggregate_id: str,
    event_type: str,
    topic: str,
    correlation_id: str,
    payload: dict,
    session=None,
) -> str:
    """
    Insert an outbox event (call within a MongoDB transaction).
    Returns the event_id.
    """
    from db.mongo import outbox_events_col
    col = outbox_events_col()

    event_id = str(uuid4())
    doc = {
        "event_id":       event_id,
        "aggregate_id":   aggregate_id,
        "event_type":     event_type,
        "topic":          topic,
        "correlation_id": correlation_id,
        "payload": {
            "event_id":       event_id,
            "event_type":     event_type,
            "event_version":  1,
            "timestamp":      datetime.utcnow().isoformat() + "Z",
            "correlation_id": correlation_id,
            "producer":       "clad-api",
            "payload":        payload,
        },
        "status":       "pending",
        "retry_count":  0,
        "error_message": None,
        "created_at":   datetime.utcnow(),
        "published_at": None,
    }

    if col is not None:
        if session:
            await col.insert_one(doc, session=session)
        else:
            await col.insert_one(doc)

    return event_id


async def get_pending_outbox_events(limit: int = 20) -> List[dict]:
    from db.mongo import outbox_events_col
    col = outbox_events_col()
    if col is None:
        return []
    cursor = col.find(
        {"status": "pending"},
        sort=[("created_at", 1)],
        limit=limit
    )
    docs = await cursor.to_list(length=limit)
    return [_strip_id(d) for d in docs]


async def mark_outbox_published(event_id: str) -> None:
    from db.mongo import outbox_events_col
    col = outbox_events_col()
    if col is None:
        return
    await col.update_one(
        {"event_id": event_id, "status": "pending"},
        {"$set": {"status": "published", "published_at": datetime.utcnow()}}
    )


async def increment_outbox_retry(event_id: str, error: str) -> None:
    from db.mongo import outbox_events_col
    col = outbox_events_col()
    if col is None:
        return
    await col.update_one(
        {"event_id": event_id},
        {"$inc": {"retry_count": 1},
         "$set": {"error_message": error[:500]}}
    )


# ══════════════════════════════════════════════════════════════
# PROCESSED EVENTS (Idempotency)
# ══════════════════════════════════════════════════════════════

async def is_event_processed(event_id: str, consumer_name: str) -> bool:
    """Returns True if this consumer already processed this event."""
    from db.mongo import processed_events_col
    col = processed_events_col()
    if col is None:
        return False   # fallback: assume not processed
    doc = await col.find_one(
        {"event_id": event_id, "consumer_name": consumer_name},
        {"_id": 1}
    )
    return doc is not None


async def mark_event_processed(event_id: str, consumer_name: str) -> bool:
    """
    Record that this consumer processed this event.
    Returns False if already recorded (duplicate) — caller should skip.
    Uses insert_one: DuplicateKeyError = already processed.
    """
    from db.mongo import processed_events_col
    from pymongo.errors import DuplicateKeyError
    col = processed_events_col()
    if col is None:
        return True
    try:
        await col.insert_one({
            "event_id":      event_id,
            "consumer_name": consumer_name,
            "processed_at":  datetime.utcnow(),
        })
        return True
    except DuplicateKeyError:
        return False   # already processed


# ══════════════════════════════════════════════════════════════
# JSON FALLBACK SHIMS (when MongoDB is not connected)
# These ensure the app still works in JSON mode.
# ══════════════════════════════════════════════════════════════

def _json_get_worker(name: str):
    from core.db import workers as _w
    return next((w for w in _w if w.get("name") == name), None)

def _json_get_all_workers():
    from core.db import workers as _w
    return list(_w)

def _json_create_worker(d: dict):
    from core.db import workers as _w, _save_state
    _w.append(d); _save_state(); return d

def _json_update_worker(name: str, fields: dict):
    from core.db import workers as _w, _save_state
    w = next((x for x in _w if x.get("name") == name), None)
    if w:
        w.update(fields); _save_state(); return True
    return False

def _json_toggle_pause(name: str):
    from core.db import workers as _w, _save_state
    w = next((x for x in _w if x.get("name") == name), None)
    if w:
        new = not bool(w.get("policy_paused", False))
        w["policy_paused"] = new; _save_state(); return new
    return None

def _json_inc_fraud_flags(name: str):
    from core.db import workers as _w, _save_state
    w = next((x for x in _w if x.get("name") == name), None)
    if w:
        w["fraudulent_flags"] = int(w.get("fraudulent_flags", 0)) + 1
        _save_state()

def _json_get_policy(name: str):
    from core.db import policies as _p
    return next((p for p in _p if p.get("user") == name or p.get("worker_name") == name), None)

def _json_get_all_policies():
    from core.db import policies as _p
    return list(_p)

def _json_upsert_policy(worker_name: str, plan: str):
    from core.db import policies as _p, _save_state
    existing = next((p for p in _p if p.get("user") == worker_name), None)
    if existing:
        existing["plan"] = plan; _save_state(); return existing
    p = {"id": len(_p)+1, "user": worker_name, "plan": plan, "status": "active",
         "worker_name": worker_name, "created_at": datetime.utcnow().isoformat()+"Z"}
    _p.append(p); _save_state(); return p

def _json_set_policy_status(worker_name: str, status: str):
    from core.db import policies as _p, _save_state
    p = next((x for x in _p if x.get("user") == worker_name), None)
    if p:
        p["status"] = status; _save_state()

def _json_get_claim(claim_id: str):
    from core.db import claims as _c
    return next((c for c in _c if str(c.get("claim_id","")) == claim_id or str(c.get("id","")) == claim_id), None)

def _json_get_claims_by_worker(name: str):
    from core.db import claims as _c
    return [c for c in _c if c.get("user") == name or c.get("worker_name") == name]

def _json_create_claim(doc: dict):
    from core.db import claims as _c, _save_state
    # Bridge: add legacy 'id' and 'user' keys for JSON compat
    doc["id"]   = len(_c) + 1
    doc["user"] = doc.get("worker_name", "")
    _c.append(doc); _save_state(); return doc

def _json_update_claim(claim_id: str, fields: dict):
    from core.db import claims as _c, _save_state
    c = next((x for x in _c if str(x.get("claim_id","")) == claim_id
              or str(x.get("id","")) == claim_id), None)
    if c:
        c.update(fields); _save_state(); return True
    return False

def _json_create_payout(doc: dict):
    return doc
