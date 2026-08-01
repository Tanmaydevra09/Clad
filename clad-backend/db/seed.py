"""
db/seed.py  —  Migrate db_state.json → MongoDB
================================================
Run once to populate MongoDB from the existing JSON file.
Safe to re-run — uses upsert (update_one with upsert=True),
so no duplicates if run multiple times.

Usage:
    python -m db.seed

Or call seed_from_json() programmatically.
"""

import asyncio
import json
import os
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger("clad.db.seed")

_DB_FILE = Path(__file__).parent.parent / "db_state.json"

# ── Plan metadata ──────────────────────────────────────────────
PLAN_META = {
    "basic": {"weekly_premium": 29, "weekly_cap": 800,  "payout_speed": "24hr reviewed"},
    "plus":  {"weekly_premium": 49, "weekly_cap": 1500, "payout_speed": "2hr auto"},
    "pro":   {"weekly_premium": 79, "weekly_cap": 2500, "payout_speed": "Instant"},
}


def _build_worker_doc(w: dict) -> dict:
    """Convert flat JSON worker → MongoDB document shape."""
    return {
        "name":         w["name"],
        "pincode":      str(w.get("pincode", "560034")),
        "plan":         w.get("plan", "plus"),
        "pan_number":   w.get("pan_number"),
        "pan_verified": bool(w.get("pan_verified", False)),
        "platform_links": w.get("platform_links", []),
        "policy_paused":  bool(w.get("policy_paused", False)),
        "delivery_profile": {
            "total_deliveries":    int(w.get("total_deliveries", 0)),
            "has_delivery_history": bool(w.get("has_delivery_history", False)),
            "account_age_days":    int(w.get("account_age_days", 90)),
            "delivery_consistency": float(w.get("delivery_consistency", 0.80)),
            "avg_daily_earning":   float(w.get("avg_daily_earning", 600.0)),
        },
        "risk_profile": {
            "claim_free_weeks":    int(w.get("claim_free_weeks", 0)),
            "past_claims_count":   int(w.get("past_claims_count", 0)),
            "location_honesty":    float(w.get("location_honesty", 0.85)),
            "claim_history_score": float(w.get("claim_history_score", 1.0)),
            "fraudulent_flags":    int(w.get("fraudulent_flags", 0)),
            "clad_score":          w.get("clad_score"),
            "integrity_score":     w.get("integrity_score"),
            "integrity_flags":     w.get("integrity_flags", []),
            "integrity_passes_gate": bool(w.get("integrity_passes_gate", True)),
        },
        "registered_at": _parse_dt(w.get("registered_at")),
        "updated_at":    datetime.utcnow(),
    }


def _build_policy_doc(p: dict) -> dict:
    plan = p.get("plan", "plus")
    meta = PLAN_META.get(plan, PLAN_META["plus"])
    return {
        "worker_name":    p["user"],
        "plan":           plan,
        "status":         p.get("status", "active"),
        "weekly_premium": meta["weekly_premium"],
        "weekly_cap":     meta["weekly_cap"],
        "created_at":     _parse_dt(p.get("created_at")),
        "updated_at":     _parse_dt(p.get("updated_at")) or datetime.utcnow(),
    }


def _build_claim_doc(c: dict) -> dict:
    fraud = {}
    if "fraud_score" in c:
        fraud = {
            "score":            int(c.get("fraud_score", 0)),
            "risk_level":       _score_to_risk(int(c.get("fraud_score", 0))),
            "approved":         c.get("status", "") == "approved",
            "layers_triggered": [],
            "layer_scores":     {},
        }
    return {
        "claim_id":       _make_claim_id(c),
        "worker_name":    c["user"],
        "pincode":        "",       # backfilled below if needed
        "amount":         float(c.get("amount", 0)),
        "reason":         c.get("reason", "manual claim"),
        "trigger_type":   c.get("trigger", "manual"),
        "status":         c.get("status", "pending"),
        "fraud_result":   fraud,
        "vision_result":  None,
        "payout_speed":   c.get("payout_speed", ""),
        "review_note":    c.get("review_note", ""),
        "photo_verified": bool(c.get("photo_verified", False)),
        "payout_processed": bool(c.get("payout_processed", False)),
        "payout_id":      c.get("payout_id"),
        "payout_upi":     c.get("payout_upi"),
        "payout_at":      _parse_dt(c.get("payout_at")),
        "created_at":     _parse_dt(c.get("created_at")) or datetime.utcnow(),
        "updated_at":     datetime.utcnow(),
    }


def _parse_dt(s) -> datetime:
    if not s:
        return datetime.utcnow()
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return datetime.utcnow()


def _score_to_risk(score: int) -> str:
    if score < 25:   return "LOW"
    if score < 50:   return "MEDIUM"
    if score < 70:   return "HIGH"
    return "CRITICAL"


def _make_claim_id(c: dict) -> str:
    # Create stable business ID from original integer ID
    dt = _parse_dt(c.get("created_at"))
    return f"CLM-{dt.strftime('%Y%m%d')}-{int(c.get('id', 0)):04d}"


async def seed_from_json(db) -> dict:
    """
    Load db_state.json and upsert into MongoDB.
    Returns counts of inserted/updated documents.
    """
    if not _DB_FILE.exists():
        logger.warning(f"db_state.json not found at {_DB_FILE}. Skipping seed.")
        return {"workers": 0, "policies": 0, "claims": 0}

    with open(_DB_FILE) as f:
        state = json.load(f)

    raw_workers  = state.get("workers", [])
    raw_policies = state.get("policies", [])
    raw_claims   = state.get("claims", [])

    # Build lookup: worker name → pincode (for claim backfill)
    name_to_pincode = {w["name"]: str(w.get("pincode", "")) for w in raw_workers}

    counts = {"workers": 0, "policies": 0, "claims": 0}

    # ── Workers ────────────────────────────────────────────────
    for w in raw_workers:
        doc = _build_worker_doc(w)
        result = await db.workers.update_one(
            {"name": doc["name"]},
            {"$set": doc},
            upsert=True
        )
        if result.upserted_id or result.modified_count:
            counts["workers"] += 1
    logger.info(f"Seeded {counts['workers']}/{len(raw_workers)} workers")

    # ── Policies ───────────────────────────────────────────────
    for p in raw_policies:
        doc = _build_policy_doc(p)
        result = await db.policies.update_one(
            {"worker_name": doc["worker_name"]},
            {"$set": doc},
            upsert=True
        )
        if result.upserted_id or result.modified_count:
            counts["policies"] += 1
    logger.info(f"Seeded {counts['policies']}/{len(raw_policies)} policies")

    # ── Claims ─────────────────────────────────────────────────
    for c in raw_claims:
        doc = _build_claim_doc(c)
        doc["pincode"] = name_to_pincode.get(c.get("user", ""), "")
        result = await db.claims.update_one(
            {"claim_id": doc["claim_id"]},
            {"$set": doc},
            upsert=True
        )
        if result.upserted_id or result.modified_count:
            counts["claims"] += 1
    logger.info(f"Seeded {counts['claims']}/{len(raw_claims)} claims")

    return counts


# ── CLI entry point ────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")

    async def _main():
        from db.mongo import init_db, get_db
        await init_db()
        db = get_db()
        if db is None:
            print("ERROR: MongoDB not connected. Check MONGO_URI env var.")
            sys.exit(1)
        counts = await seed_from_json(db)
        print(f"\n✅ Seed complete: {counts}")

    asyncio.run(_main())
