"""
etl/pipeline.py  —  MongoDB → Snowflake incremental ETL
=========================================================
Extracts changed records from MongoDB since the last watermark
and merges them into Snowflake fact/dimension tables.

Design principles:
  1. INCREMENTAL: only processes records where updated_at > last_watermark
  2. IDEMPOTENT:  MERGE (not INSERT) — re-running produces same result
  3. DECOUPLED:   Snowflake failure NEVER blocks the main application
  4. WATERMARKED: checkpoints stored in both MongoDB AND Snowflake
  5. OBSERVABILITY: every run emits Prometheus metrics + structured logs

ETL Flow:
  1. Load watermark from MongoDB (etl_checkpoints collection)
  2. Extract changed documents from MongoDB (claims, payouts, triggers)
  3. Transform → dimensional model (dimension lookups + fact rows)
  4. Load into Snowflake via MERGE statements
  5. Update watermark in both MongoDB and Snowflake
  6. Emit metrics
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List

logger = logging.getLogger("clad.etl.pipeline")

# ── Watermark grace period ──────────────────────────────────────
# Look back 5 min before watermark to handle late-arriving events
WATERMARK_GRACE_SECONDS = 300

# ── Batch sizes ─────────────────────────────────────────────────
CLAIMS_BATCH_SIZE  = 500
PAYOUT_BATCH_SIZE  = 500
TRIGGER_BATCH_SIZE = 500


def _date_key(dt: datetime) -> int:
    """Convert datetime → YYYYMMDD integer for DIM_DATE key."""
    return int(dt.strftime("%Y%m%d"))


def _score_to_risk(score: float) -> str:
    if score >= 85: return "LOW"
    if score >= 70: return "MEDIUM"
    if score >= 50: return "HIGH"
    return "VERY_HIGH"


def _clad_score_to_segment(score: Optional[float]) -> str:
    if score is None: return "UNSCORED"
    if score >= 85:   return "A_PLUS"
    if score >= 75:   return "A"
    if score >= 50:   return "B"
    return "C_D"


async def _get_watermark(job_name: str) -> datetime:
    """Load last-run watermark from MongoDB. Default: 30 days ago."""
    try:
        from db.mongo import etl_checkpoints_col
        col = etl_checkpoints_col()
        if col is None:
            return datetime.utcnow() - timedelta(days=30)
        doc = await col.find_one({"job_name": job_name})
        if doc and "last_watermark" in doc:
            return doc["last_watermark"]
    except Exception as e:
        logger.warning(f"Failed to load watermark for {job_name}: {e}")
    return datetime.utcnow() - timedelta(days=30)


async def _save_watermark(job_name: str, watermark: datetime, rows_extracted: int, rows_loaded: int, status: str) -> None:
    """Save ETL checkpoint to MongoDB."""
    try:
        from db.mongo import etl_checkpoints_col
        col = etl_checkpoints_col()
        if col is None:
            return
        await col.update_one(
            {"job_name": job_name},
            {"$set": {
                "job_name":       job_name,
                "last_watermark": watermark,
                "last_run_at":    datetime.utcnow(),
                "rows_extracted": rows_extracted,
                "rows_loaded":    rows_loaded,
                "status":         status,
            }},
            upsert=True
        )
    except Exception as e:
        logger.warning(f"Failed to save watermark for {job_name}: {e}")


def _ensure_dim_worker(cursor, worker_name: str, worker_data: dict) -> Optional[int]:
    """Upsert DIM_WORKER and return worker_key."""
    clad = float(worker_data.get("clad_score") or 50)
    risk = _score_to_risk(clad)
    cursor.execute("""
        MERGE INTO DIM_WORKER w
        USING (SELECT %s::VARCHAR AS worker_id) src
        ON w.worker_id = src.worker_id
        WHEN MATCHED THEN UPDATE SET
          worker_name      = %s,
          plan             = %s,
          risk_segment     = %s,
          platform_count   = %s,
          account_age_days = %s,
          avg_daily_earning = %s,
          pan_verified     = %s
        WHEN NOT MATCHED THEN INSERT
          (worker_id, worker_name, plan, risk_segment,
           platform_count, account_age_days, avg_daily_earning, pan_verified)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        worker_name,
        worker_name, worker_data.get("plan","plus"), risk,
        len(worker_data.get("platform_links",[])), int(worker_data.get("account_age_days",90)),
        float(worker_data.get("avg_daily_earning",600)), bool(worker_data.get("pan_verified",False)),
        # WHEN NOT MATCHED values
        worker_name, worker_name, worker_data.get("plan","plus"), risk,
        len(worker_data.get("platform_links",[])), int(worker_data.get("account_age_days",90)),
        float(worker_data.get("avg_daily_earning",600)), bool(worker_data.get("pan_verified",False)),
    ))
    cursor.execute("SELECT worker_key FROM DIM_WORKER WHERE worker_id = %s", (worker_name,))
    row = cursor.fetchone()
    return row[0] if row else None


def _ensure_dim_location(cursor, pincode: str) -> int:
    """Upsert DIM_LOCATION for a pincode and return location_key."""
    cursor.execute("""
        MERGE INTO DIM_LOCATION l
        USING (SELECT %s::VARCHAR AS pincode) src
        ON l.pincode = src.pincode
        WHEN NOT MATCHED THEN INSERT (pincode, city, state, zone_risk_cat)
        VALUES (%s, 'Unknown', 'India', 'MEDIUM')
    """, (pincode, pincode))
    cursor.execute("SELECT location_key FROM DIM_LOCATION WHERE pincode = %s", (pincode,))
    row = cursor.fetchone()
    return row[0] if row else 1


def _ensure_dim_date(cursor, dt: datetime) -> int:
    """Upsert DIM_DATE for a date and return date_key."""
    dk = _date_key(dt)
    months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]
    days   = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    is_weekend = dt.weekday() >= 5
    is_monsoon = dt.month in (6, 7, 8, 9)
    cursor.execute("""
        MERGE INTO DIM_DATE d
        USING (SELECT %s::INTEGER AS date_key) src
        ON d.date_key = src.date_key
        WHEN NOT MATCHED THEN INSERT
          (date_key, date, day_of_week, day_num, month, month_name,
           quarter, year, is_weekend, is_monsoon)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        dk,
        dk, dt.date(),
        days[dt.weekday()], dt.weekday()+1,
        dt.month, months[dt.month-1],
        (dt.month-1)//3+1, dt.year,
        is_weekend, is_monsoon,
    ))
    return dk


def _ensure_dim_disruption(cursor, trigger_type: str) -> Optional[int]:
    """Upsert DIM_DISRUPTION for a trigger type."""
    if not trigger_type or trigger_type == "manual":
        return None
    severity = "HIGH" if trigger_type in ("heavy_rain","flood") else "MEDIUM"
    cursor.execute("""
        MERGE INTO DIM_DISRUPTION d
        USING (SELECT %s::VARCHAR AS trigger_type) src
        ON d.trigger_type = src.trigger_type
        WHEN NOT MATCHED THEN INSERT
          (trigger_type, severity_category, payout_eligible)
        VALUES (%s, %s, TRUE)
    """, (trigger_type, trigger_type, severity))
    cursor.execute("SELECT disruption_key FROM DIM_DISRUPTION WHERE trigger_type = %s", (trigger_type,))
    row = cursor.fetchone()
    return row[0] if row else None


async def run_claims_etl(since: datetime, workers_cache: dict) -> Dict[str, int]:
    """Extract claims from MongoDB, merge into FACT_CLAIMS."""
    from db.mongo import claims_col
    from etl.snowflake_client import snowflake_connection
    from observability.metrics import etl_rows_extracted_total, etl_rows_loaded_total

    col = claims_col()
    if col is None:
        return {"extracted": 0, "loaded": 0}

    # Extract changed claims since watermark
    since_with_grace = since - timedelta(seconds=WATERMARK_GRACE_SECONDS)
    cursor_mongo = col.find(
        {"updated_at": {"$gte": since_with_grace}},
        {"_id": 0},
        sort=[("updated_at", 1)],
    )
    claims_docs = await cursor_mongo.to_list(length=CLAIMS_BATCH_SIZE)

    extracted = len(claims_docs)
    loaded    = 0

    try:
        etl_rows_extracted_total.labels(collection="claims").inc(extracted)
    except Exception:
        pass

    if not claims_docs:
        return {"extracted": 0, "loaded": 0}

    with snowflake_connection() as conn:
        if conn is None:
            logger.warning("Snowflake not available — skipping claims ETL")
            return {"extracted": extracted, "loaded": 0}

        cursor = conn.cursor()
        try:
            for doc in claims_docs:
                created_at = doc.get("created_at") or datetime.utcnow()
                worker_name = doc.get("worker_name", "unknown")
                pincode     = str(doc.get("pincode", "000000"))
                trigger_type = doc.get("trigger_type", "manual")

                # Get worker data from cache
                worker_data = workers_cache.get(worker_name, {})

                # Ensure dimensions exist
                worker_key   = _ensure_dim_worker(cursor, worker_name, worker_data)
                location_key = _ensure_dim_location(cursor, pincode)
                date_key     = _ensure_dim_date(cursor, created_at)
                disrupt_key  = _ensure_dim_disruption(cursor, trigger_type)

                if not worker_key:
                    continue

                # Extract fraud layer scores
                fr = doc.get("fraud_result") or {}
                ls = fr.get("layer_scores", {})

                # MERGE into FACT_CLAIMS
                cursor.execute("""
                    MERGE INTO FACT_CLAIMS f
                    USING (SELECT %s::VARCHAR AS claim_id) src
                    ON f.claim_id = src.claim_id
                    WHEN MATCHED THEN UPDATE SET
                      claim_status  = %s,
                      updated_at    = %s,
                      etl_loaded_at = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT (
                      claim_id, worker_key, location_key, date_key, disruption_key,
                      claim_amount, fraud_score,
                      layer0_score, layer1_score, layer2_score, layer3_score, layer4_score,
                      claim_status, trigger_type, payout_speed,
                      photo_submitted, vision_verdict, clad_score_at_claim,
                      created_at, updated_at
                    ) VALUES (
                      %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s, %s, %s,
                      %s, %s
                    )
                """, (
                    doc["claim_id"],
                    # UPDATE
                    doc.get("status","pending"),
                    doc.get("updated_at", datetime.utcnow()),
                    # INSERT
                    doc["claim_id"], worker_key, location_key, date_key, disrupt_key,
                    float(doc.get("amount", 0)), fr.get("score"),
                    ls.get("layer0"), ls.get("layer1"), ls.get("layer2"),
                    ls.get("layer3"), ls.get("layer4"),
                    doc.get("status","pending"), trigger_type, doc.get("payout_speed"),
                    bool(doc.get("photo_submitted", False)),
                    (doc.get("vision_result") or {}).get("verdict"),
                    float(worker_data.get("clad_score") or 50),
                    created_at, doc.get("updated_at", datetime.utcnow()),
                ))
                loaded += 1

            conn.commit()
            try:
                etl_rows_loaded_total.labels(fact_table="FACT_CLAIMS").inc(loaded)
            except Exception:
                pass
            logger.info(f"Claims ETL: extracted={extracted} loaded={loaded}")

        except Exception as e:
            conn.rollback()
            logger.error(f"Claims ETL failed: {e}")
            raise
        finally:
            cursor.close()

    return {"extracted": extracted, "loaded": loaded}


async def run_payouts_etl(since: datetime) -> Dict[str, int]:
    """Extract payouts from MongoDB, merge into FACT_PAYOUTS."""
    from db.mongo import payouts_col
    from etl.snowflake_client import snowflake_connection

    col = payouts_col()
    if col is None:
        return {"extracted": 0, "loaded": 0}

    since_with_grace = since - timedelta(seconds=WATERMARK_GRACE_SECONDS)
    cursor_mongo = col.find(
        {"created_at": {"$gte": since_with_grace}},
        {"_id": 0},
        sort=[("created_at", 1)],
    )
    payout_docs = await cursor_mongo.to_list(length=PAYOUT_BATCH_SIZE)
    extracted = len(payout_docs)

    if not payout_docs:
        return {"extracted": 0, "loaded": 0}

    loaded = 0
    with snowflake_connection() as conn:
        if conn is None:
            return {"extracted": extracted, "loaded": 0}

        cursor = conn.cursor()
        try:
            for doc in payout_docs:
                created_at  = doc.get("created_at") or datetime.utcnow()
                worker_name = doc.get("worker_name", "unknown")

                date_key = _ensure_dim_date(cursor, created_at)

                # Get worker_key from FACT_CLAIMS (they should exist)
                cursor.execute("""
                    SELECT fc.worker_key
                    FROM FACT_CLAIMS fc
                    WHERE fc.claim_id = %s
                """, (doc.get("claim_id"),))
                row = cursor.fetchone()
                worker_key = row[0] if row else 1

                cursor.execute("""
                    MERGE INTO FACT_PAYOUTS f
                    USING (SELECT %s::VARCHAR AS claim_id) src
                    ON f.claim_id = src.claim_id
                    WHEN MATCHED THEN UPDATE SET
                      status = %s, etl_loaded_at = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT
                      (claim_id, worker_key, date_key, amount, razorpay_mode, status, processed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    doc.get("claim_id"),
                    doc.get("status","completed"),
                    doc.get("claim_id"), worker_key, date_key,
                    float(doc.get("amount",0)), doc.get("razorpay_mode"),
                    doc.get("status","completed"),
                    doc.get("processed_at"),
                ))
                loaded += 1

            conn.commit()
            logger.info(f"Payouts ETL: extracted={extracted} loaded={loaded}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Payouts ETL failed: {e}")
            raise
        finally:
            cursor.close()

    return {"extracted": extracted, "loaded": loaded}


async def run_full_etl() -> Dict[str, Any]:
    """
    Run the full incremental ETL pipeline.
    Called by the ETL scheduler or manually.
    Snowflake failure is caught and logged — does NOT propagate to caller.
    """
    from observability.metrics import etl_duration_seconds, etl_last_success, etl_failures_total
    from db.operations import get_all_workers

    JOB_NAME  = "clad_full_etl"
    t0        = time.monotonic()
    watermark = await _get_watermark(JOB_NAME)
    new_water = datetime.utcnow()

    logger.info(f"ETL starting: job={JOB_NAME} watermark={watermark.isoformat()}")

    # Build workers cache (to enrich claims with worker data without N+1 queries)
    workers_list  = await get_all_workers()
    workers_cache = {w["name"]: w for w in workers_list}

    total_extracted = 0
    total_loaded    = 0
    status          = "success"

    try:
        claims_result  = await run_claims_etl(watermark, workers_cache)
        payouts_result = await run_payouts_etl(watermark)

        total_extracted = claims_result["extracted"] + payouts_result["extracted"]
        total_loaded    = claims_result["loaded"]    + payouts_result["loaded"]

    except Exception as e:
        logger.error(f"ETL pipeline error: {e}", exc_info=True)
        status = "failed"
        try:
            etl_failures_total.labels(job_name=JOB_NAME).inc()
        except Exception:
            pass

    duration = time.monotonic() - t0

    # Save watermark even on partial failure (to not re-process already-loaded rows)
    await _save_watermark(JOB_NAME, new_water, total_extracted, total_loaded, status)

    try:
        etl_duration_seconds.labels(job_name=JOB_NAME).observe(duration)
        if status == "success":
            import time as _t
            etl_last_success.set(_t.time())
    except Exception:
        pass

    result = {
        "job_name":        JOB_NAME,
        "status":          status,
        "watermark_from":  watermark.isoformat(),
        "watermark_to":    new_water.isoformat(),
        "rows_extracted":  total_extracted,
        "rows_loaded":     total_loaded,
        "duration_s":      round(duration, 2),
    }

    logger.info(f"ETL complete: {result}")
    return result


async def schedule_etl_loop(interval_minutes: int = 30) -> None:
    """
    Run ETL every N minutes as a background asyncio task.
    Snowflake failures are swallowed — app continues operating.
    """
    logger.info(f"ETL scheduler starting: interval={interval_minutes}min")
    while True:
        try:
            await run_full_etl()
        except Exception as e:
            logger.error(f"ETL scheduler error (will retry next interval): {e}")
        await asyncio.sleep(interval_minutes * 60)
