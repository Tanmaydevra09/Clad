"""
etl/snowflake_client.py  —  Snowflake connection management
============================================================
Wraps snowflake-connector-python for the ETL pipeline.

Config (all via env vars, never hardcoded):
  SNOWFLAKE_ACCOUNT   e.g. abc12345.us-east-1
  SNOWFLAKE_USER      your Snowflake username
  SNOWFLAKE_PASSWORD  your Snowflake password
  SNOWFLAKE_WAREHOUSE CLAD_ETL_WH
  SNOWFLAKE_DATABASE  CLAD_DB
  SNOWFLAKE_SCHEMA    ANALYTICS

Cost controls:
  - X-Small warehouse (1 credit/hr)
  - AUTO_SUSPEND = 60 seconds idle
  - AUTO_RESUME  = TRUE
  - Connection is opened per ETL run, closed immediately after
"""

import os
import logging
from typing import Optional
from contextlib import contextmanager

logger = logging.getLogger("clad.etl.snowflake")

# Snowflake DDL — run once to set up the warehouse, database, schema
SNOWFLAKE_SETUP_DDL = """
-- ── Cost-optimised warehouse (X-Small, auto-suspend 60s) ──────────────────
CREATE WAREHOUSE IF NOT EXISTS CLAD_ETL_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Clad ETL warehouse — auto-suspends after 60s idle';

CREATE WAREHOUSE IF NOT EXISTS CLAD_ANALYTICS_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Clad analytics query warehouse';

-- ── Database + schema ─────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS CLAD_DB
  COMMENT = 'Clad Insurance analytical data warehouse';

USE DATABASE CLAD_DB;

CREATE SCHEMA IF NOT EXISTS ANALYTICS
  COMMENT = 'Star schema for insurance analytics (OLAP)';

USE SCHEMA ANALYTICS;
"""

# ── Dimension Tables ────────────────────────────────────────────────────────
DIMENSION_DDL = """
-- DIM_DATE: pre-computed date dimension
CREATE TABLE IF NOT EXISTS DIM_DATE (
  date_key    INTEGER        NOT NULL PRIMARY KEY,  -- YYYYMMDD integer
  date        DATE           NOT NULL,
  day_of_week VARCHAR(10)    NOT NULL,
  day_num     INTEGER        NOT NULL,  -- 1=Mon .. 7=Sun
  month       INTEGER        NOT NULL,
  month_name  VARCHAR(10)    NOT NULL,
  quarter     INTEGER        NOT NULL,
  year        INTEGER        NOT NULL,
  is_weekend  BOOLEAN        NOT NULL,
  is_monsoon  BOOLEAN        NOT NULL   -- Jun-Sep in India
);

-- DIM_WORKER: gig worker profile snapshot at time of claim
CREATE TABLE IF NOT EXISTS DIM_WORKER (
  worker_key       INTEGER AUTOINCREMENT PRIMARY KEY,
  worker_id        VARCHAR(50)    NOT NULL,  -- business ID
  worker_name      VARCHAR(100)   NOT NULL,
  plan             VARCHAR(20)    NOT NULL,
  risk_segment     VARCHAR(20)    NOT NULL,  -- LOW/MEDIUM/HIGH/VERY_HIGH
  platform_count   INTEGER        NOT NULL DEFAULT 0,
  account_age_days INTEGER        NOT NULL DEFAULT 0,
  avg_daily_earning FLOAT         NOT NULL DEFAULT 0,
  pan_verified     BOOLEAN        NOT NULL DEFAULT FALSE,
  UNIQUE (worker_id)
);

-- DIM_LOCATION: pincode → city/state mapping
CREATE TABLE IF NOT EXISTS DIM_LOCATION (
  location_key     INTEGER AUTOINCREMENT PRIMARY KEY,
  pincode          VARCHAR(10)    NOT NULL,
  city             VARCHAR(100)   NOT NULL DEFAULT 'Unknown',
  state            VARCHAR(100)   NOT NULL DEFAULT 'India',
  zone_risk_cat    VARCHAR(20)    NOT NULL DEFAULT 'MEDIUM',  -- LOW/MEDIUM/HIGH/VERY_HIGH
  disruption_days_per_year INTEGER NOT NULL DEFAULT 30,
  UNIQUE (pincode)
);

-- DIM_DISRUPTION: trigger event type metadata
CREATE TABLE IF NOT EXISTS DIM_DISRUPTION (
  disruption_key   INTEGER AUTOINCREMENT PRIMARY KEY,
  trigger_type     VARCHAR(50)    NOT NULL UNIQUE,
  severity_category VARCHAR(20)   NOT NULL,  -- LOW/MEDIUM/HIGH/CRITICAL
  threshold_rain_mm FLOAT,
  threshold_aqi     INTEGER,
  payout_eligible   BOOLEAN       NOT NULL DEFAULT TRUE,
  description       VARCHAR(200)
);

-- DIM_POLICY: plan tier details
CREATE TABLE IF NOT EXISTS DIM_POLICY (
  policy_key       INTEGER AUTOINCREMENT PRIMARY KEY,
  plan_type        VARCHAR(20)    NOT NULL UNIQUE,
  weekly_premium   INTEGER        NOT NULL,
  weekly_cap       INTEGER        NOT NULL,
  payout_speed     VARCHAR(50)    NOT NULL
);
"""

# ── Fact Tables ─────────────────────────────────────────────────────────────
FACT_DDL = """
-- FACT_CLAIMS: one row per insurance claim
CREATE TABLE IF NOT EXISTS FACT_CLAIMS (
  claim_sk         INTEGER AUTOINCREMENT PRIMARY KEY,
  claim_id         VARCHAR(50)    NOT NULL UNIQUE,   -- MongoDB business ID
  worker_key       INTEGER        NOT NULL REFERENCES DIM_WORKER(worker_key),
  location_key     INTEGER        NOT NULL REFERENCES DIM_LOCATION(location_key),
  date_key         INTEGER        NOT NULL REFERENCES DIM_DATE(date_key),
  disruption_key   INTEGER        REFERENCES DIM_DISRUPTION(disruption_key),
  policy_key       INTEGER        REFERENCES DIM_POLICY(policy_key),

  -- Measures
  claim_amount     FLOAT          NOT NULL,
  fraud_score      INTEGER,
  layer0_score     FLOAT,           -- account integrity
  layer1_score     FLOAT,           -- rules engine
  layer2_score     FLOAT,           -- network graph
  layer3_score     FLOAT,           -- isolation forest
  layer4_score     FLOAT,           -- vision AI

  -- Categoricals
  claim_status         VARCHAR(30) NOT NULL,
  trigger_type         VARCHAR(50),
  payout_speed         VARCHAR(50),
  photo_submitted      BOOLEAN    NOT NULL DEFAULT FALSE,
  vision_verdict       VARCHAR(20),
  clad_score_at_claim  FLOAT,

  -- Audit
  created_at       TIMESTAMP_NTZ  NOT NULL,
  updated_at       TIMESTAMP_NTZ  NOT NULL,
  etl_loaded_at    TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- FACT_PAYOUTS: one row per payout transaction
CREATE TABLE IF NOT EXISTS FACT_PAYOUTS (
  payout_sk        INTEGER AUTOINCREMENT PRIMARY KEY,
  claim_id         VARCHAR(50)    NOT NULL UNIQUE REFERENCES FACT_CLAIMS(claim_id),
  worker_key       INTEGER        NOT NULL REFERENCES DIM_WORKER(worker_key),
  date_key         INTEGER        NOT NULL REFERENCES DIM_DATE(date_key),

  -- Measures
  amount           FLOAT          NOT NULL,
  razorpay_mode    VARCHAR(30),

  -- Status
  status           VARCHAR(20)    NOT NULL,
  processed_at     TIMESTAMP_NTZ,

  -- Audit
  etl_loaded_at    TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- FACT_TRIGGER_EVENTS: one row per environmental disruption detection
CREATE TABLE IF NOT EXISTS FACT_TRIGGER_EVENTS (
  trigger_sk            INTEGER AUTOINCREMENT PRIMARY KEY,
  trigger_id            VARCHAR(100)   NOT NULL UNIQUE,
  location_key          INTEGER        NOT NULL REFERENCES DIM_LOCATION(location_key),
  date_key              INTEGER        NOT NULL REFERENCES DIM_DATE(date_key),
  disruption_key        INTEGER        REFERENCES DIM_DISRUPTION(disruption_key),

  -- Measures
  rainfall_mm           FLOAT,
  aqi_reading           FLOAT,
  wind_speed_kmh        FLOAT,
  claims_triggered_count INTEGER        NOT NULL DEFAULT 0,
  payout_triggered_amount FLOAT         NOT NULL DEFAULT 0,

  -- Audit
  triggered_at          TIMESTAMP_NTZ  NOT NULL,
  etl_loaded_at         TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- ETL watermark table
CREATE TABLE IF NOT EXISTS ETL_CHECKPOINTS (
  job_name          VARCHAR(100)   NOT NULL PRIMARY KEY,
  last_watermark    TIMESTAMP_NTZ  NOT NULL,
  last_run_at       TIMESTAMP_NTZ  NOT NULL,
  rows_extracted    INTEGER        NOT NULL DEFAULT 0,
  rows_loaded       INTEGER        NOT NULL DEFAULT 0,
  status            VARCHAR(20)    NOT NULL DEFAULT 'success'  -- success | failed
);
"""

# ── Analytics Views ─────────────────────────────────────────────────────────
ANALYTICS_VIEWS_DDL = """
-- Claim performance by city
CREATE OR REPLACE VIEW V_CLAIMS_BY_CITY AS
SELECT
  l.city,
  l.pincode,
  COUNT(fc.claim_sk)                                          AS total_claims,
  SUM(fc.claim_amount)                                        AS total_payout_inr,
  AVG(fc.claim_amount)                                        AS avg_claim_inr,
  AVG(fc.fraud_score)                                         AS avg_fraud_score,
  SUM(CASE WHEN fc.claim_status = 'approved' THEN 1 ELSE 0 END)    AS approved_count,
  SUM(CASE WHEN fc.claim_status LIKE 'rejected%' THEN 1 ELSE 0 END) AS rejected_count,
  ROUND(approved_count / NULLIF(total_claims, 0) * 100, 1)    AS approval_rate_pct
FROM FACT_CLAIMS fc
JOIN DIM_LOCATION l ON fc.location_key = l.location_key
GROUP BY l.city, l.pincode
ORDER BY total_payout_inr DESC;

-- Monthly payout trends
CREATE OR REPLACE VIEW V_MONTHLY_PAYOUTS AS
SELECT
  d.year,
  d.month,
  d.month_name,
  COUNT(fp.payout_sk)   AS payout_count,
  SUM(fp.amount)         AS total_payout_inr,
  AVG(fp.amount)         AS avg_payout_inr,
  MIN(fp.amount)         AS min_payout_inr,
  MAX(fp.amount)         AS max_payout_inr
FROM FACT_PAYOUTS fp
JOIN DIM_DATE d ON fp.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;

-- Fraud analysis by disruption type
CREATE OR REPLACE VIEW V_FRAUD_BY_DISRUPTION AS
SELECT
  d.trigger_type,
  d.severity_category,
  COUNT(fc.claim_sk)                                               AS total_claims,
  AVG(fc.fraud_score)                                              AS avg_fraud_score,
  MAX(fc.fraud_score)                                              AS max_fraud_score,
  SUM(CASE WHEN fc.claim_status LIKE 'rejected%' THEN 1 ELSE 0 END) AS fraud_rejections,
  ROUND(fraud_rejections / NULLIF(total_claims, 0) * 100, 1)       AS fraud_rate_pct
FROM FACT_CLAIMS fc
JOIN DIM_DISRUPTION d ON fc.disruption_key = d.disruption_key
GROUP BY d.trigger_type, d.severity_category
ORDER BY fraud_rate_pct DESC;

-- Worker risk segment analysis
CREATE OR REPLACE VIEW V_WORKER_RISK_SEGMENTS AS
SELECT
  w.risk_segment,
  w.plan,
  COUNT(DISTINCT w.worker_key)                                   AS worker_count,
  AVG(fc.claim_amount)                                           AS avg_claim_amount,
  AVG(fc.fraud_score)                                            AS avg_fraud_score,
  SUM(CASE WHEN fc.claim_status = 'approved' THEN 1 ELSE 0 END)  AS approved_claims,
  SUM(CASE WHEN fc.claim_status LIKE 'rejected%' THEN 1 ELSE 0 END) AS rejected_claims
FROM DIM_WORKER w
LEFT JOIN FACT_CLAIMS fc ON w.worker_key = fc.worker_key
GROUP BY w.risk_segment, w.plan
ORDER BY w.risk_segment;
"""


def get_snowflake_connection():
    """
    Create a Snowflake connection using env vars.
    Returns None if credentials not set.
    """
    account  = os.getenv("SNOWFLAKE_ACCOUNT")
    user     = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")

    if not all([account, user, password]):
        logger.warning(
            "Snowflake credentials not set — ETL disabled. "
            "Set SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD env vars."
        )
        return None

    try:
        import snowflake.connector
        conn = snowflake.connector.connect(
            account   = account,
            user      = user,
            password  = password,
            warehouse = os.getenv("SNOWFLAKE_WAREHOUSE", "CLAD_ETL_WH"),
            database  = os.getenv("SNOWFLAKE_DATABASE", "CLAD_DB"),
            schema    = os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS"),
        )
        logger.info(f"Snowflake connected: account={account}")
        return conn
    except ImportError:
        logger.warning("snowflake-connector-python not installed")
        return None
    except Exception as e:
        logger.error(f"Snowflake connection failed: {e}")
        return None


@contextmanager
def snowflake_connection():
    """Context manager that opens and closes a Snowflake connection."""
    conn = get_snowflake_connection()
    if conn is None:
        yield None
        return
    try:
        yield conn
    finally:
        conn.close()


def setup_snowflake_schema() -> bool:
    """
    One-time setup: create warehouse, database, schema, tables, views.
    Run manually before first ETL run.
    """
    with snowflake_connection() as conn:
        if conn is None:
            logger.error("Cannot setup schema — no Snowflake connection")
            return False
        cursor = conn.cursor()
        try:
            for ddl_block in [DIMENSION_DDL, FACT_DDL, ANALYTICS_VIEWS_DDL]:
                for statement in ddl_block.strip().split(";"):
                    stmt = statement.strip()
                    if stmt:
                        cursor.execute(stmt)
            conn.commit()
            logger.info("Snowflake schema setup complete")
            return True
        except Exception as e:
            logger.error(f"Snowflake schema setup failed: {e}")
            return False
        finally:
            cursor.close()


def snowflake_readiness_check() -> dict:
    """Check Snowflake connectivity — used by /health/analytics endpoint."""
    try:
        with snowflake_connection() as conn:
            if conn is None:
                return {"status": "disconnected", "reason": "credentials not set"}
            cursor = conn.cursor()
            cursor.execute("SELECT CURRENT_TIMESTAMP()")
            ts = cursor.fetchone()[0]
            cursor.close()
            return {"status": "ok", "server_time": str(ts)}
    except Exception as e:
        return {"status": "error", "error": str(e)[:100]}
