"""
services/pricing_engine.py
===========================
Main pricing pipeline:
  Worker inputs → CladScore → Zone Risk → LightGBM → Calibration → Breakdown

This is what /premium calls. It's the real ML model — not the old rules stub.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from data.dynamic_zone_risk import compute_zone_risk_live, get_zone_risk_sync, DEFAULT_ZONE
from data.clad_score import compute_clad_score_simple
from src.predict import predict

# Base prices per plan tier — used as the ML anchor
PLAN_BASE_PRICE = {
    "basic": 29,
    "plus":  49,
    "pro":   79,
}

def compute_premium(user: dict) -> dict:
    # ── 1. Zone lookup (live for any Indian pincode) ───────────────
    pincode = str(user.get("pincode", "560034"))
    zone    = get_zone_risk_sync(pincode)

    # ── 2. Plan tier — determines coverage cap & base price ──
    plan       = str(user.get("plan", "plus")).lower()
    base_price = PLAN_BASE_PRICE.get(plan, 49)

    # ── 3. CladScore ─────────────────────────────────────────
    clad_score = compute_clad_score_simple(
        delivery_consistency_pct = float(user.get("delivery_consistency", 0.80)),
        location_honesty_pct     = float(user.get("location_honesty", 0.85)),
        claim_history_score      = float(user.get("claim_history_score", 1.0)),
        zone_disruption_days     = zone["disruption_days_per_year"],
        flood_frequency          = zone["flood_frequency"],
        account_age_days         = int(user.get("account_age_days", 90)),
        claim_free_weeks         = int(user.get("claim_free_weeks", 0)),
        fraudulent_flags         = int(user.get("fraudulent_flags", 0)),
    )

    # ── 3. Derive grade & payout speed from CladScore ────────
    if clad_score >= 85:
        grade, payout_speed = "A+", "Instant"
    elif clad_score >= 75:
        grade, payout_speed = "A",  "2hr auto"
    elif clad_score >= 62:
        grade, payout_speed = "B+", "2hr auto"
    elif clad_score >= 50:
        grade, payout_speed = "B",  "6hr hold"
    elif clad_score >= 35:
        grade, payout_speed = "C",  "24hr review"
    else:
        grade, payout_speed = "D",  "24hr review"

    # ── 4. Build ML feature vector ───────────────────────────
    month              = int(user.get("month", 4))
    avg_daily_earning  = float(user.get("avg_daily_earning", 700))  # real worker earning
    claim_free_weeks_v = int(user.get("claim_free_weeks", 0))
    past_claims        = int(user.get("past_claims_count", 0))

    is_monsoon    = int(month in [6, 7, 8, 9])
    is_aqi_season = int(month in [10, 11, 12, 1, 2])
    weekly_prob   = zone["disruption_days_per_year"] / 365 * 7
    if is_monsoon:
        weekly_prob *= 2.5

    model_input = {
        "base_premium":            base_price,          # plan-specific anchor (29/49/79)
        "account_age_days":        int(user.get("account_age_days", 90)),
        "clad_score":              clad_score,
        "delivery_consistency":    float(user.get("delivery_consistency", 0.80)),
        "avg_daily_earning":       avg_daily_earning,   # real worker earning
        "claim_free_weeks":        claim_free_weeks_v,
        "past_claims_count":       past_claims,
        "is_monsoon":              is_monsoon,
        "is_aqi_season":           is_aqi_season,
        "flood_frequency":         zone["flood_frequency"],
        "avg_rainfall_mm":         zone["avg_rainfall_mm"],
        "aqi_annual_avg":          zone["aqi_annual_avg"],
        "waterlogging_score":      zone["waterlogging_score"],
        "disruption_days_per_year": zone["disruption_days_per_year"],
        "weekly_disruption_prob":  weekly_prob,
        "expected_weekly_payout":  weekly_prob * avg_daily_earning * 0.5,
    }

    # ── 5. LightGBM prediction ────────────────────────────────
    ml_error = None
    try:
        raw_premium = predict(model_input)
        ml_used     = True
    except Exception as e:
        # Fallback to actuarial — catches RuntimeError (model missing),
        # version mismatches, import errors, etc.
        ml_error    = f"{type(e).__name__}: {str(e)[:120]}"
        raw_premium = (
            49
            + zone["flood_frequency"] * 18
            + zone["aqi_annual_avg"] / 220 * 6
            - ((clad_score - 50) / 50) * 12
            + (8 if is_monsoon else 0)
        )
        ml_used = False

    # ── 6. Calibration & safety bounds ───────────────────────
    # Clamp relative to the chosen plan's base price so Basic never exceeds Pro
    plan_min = max(20.0, base_price * 0.6)
    plan_max = base_price * 2.2
    premium = raw_premium * 0.7
    premium = round(max(plan_min, min(plan_max, premium)), 2)

    # ── 7. Explainability breakdown ───────────────────────────
    breakdown = []

    plan_label = {"basic": "Basic", "plus": "Plus", "pro": "Pro"}.get(plan, "Plus")
    breakdown.append({"factor": f"Base Premium (Clad {plan_label})", "amount": base_price, "direction": "base"})

    flood_adj = round(zone["flood_frequency"] * 18, 2)
    breakdown.append({"factor": f"Flood Risk — {zone.get('city', pincode)}", "amount": flood_adj, "direction": "increase"})

    aqi_adj = round(zone["aqi_annual_avg"] / 220 * 6, 2)
    breakdown.append({"factor": "Air Quality Index (AQI) Risk", "amount": aqi_adj, "direction": "increase"})

    disruption_adj = round((zone["disruption_days_per_year"] / 52) * 2.5, 2)
    breakdown.append({"factor": "Zone Disruption Frequency", "amount": disruption_adj, "direction": "increase"})

    if is_monsoon:
        breakdown.append({"factor": "Monsoon Season Surcharge", "amount": 8, "direction": "increase"})
    else:
        breakdown.append({"factor": "Non-Monsoon Stability", "amount": -5, "direction": "discount"})

    if is_aqi_season:
        breakdown.append({"factor": "Winter AQI Season Surcharge", "amount": 4, "direction": "increase"})

    clad_adj = round(((clad_score - 50) / 50) * 12, 2)
    if clad_adj > 0:
        breakdown.append({"factor": f"CladScore Discount (grade {grade})", "amount": -clad_adj, "direction": "discount"})
    else:
        breakdown.append({"factor": f"CladScore Risk Premium (grade {grade})", "amount": abs(clad_adj), "direction": "increase"})

    if claim_free_weeks_v > 0:
        streak_disc = round(min(1.0, claim_free_weeks_v / 12) * 8, 2)
        breakdown.append({"factor": f"No-Claim Streak ({claim_free_weeks_v} weeks)", "amount": -streak_disc, "direction": "discount"})

    account_age = int(user.get("account_age_days", 90))
    if account_age < 90:
        nu_surcharge = round((1 - account_age / 90) * 5, 2)
        breakdown.append({"factor": "New Account Surcharge", "amount": nu_surcharge, "direction": "increase"})

    # ── 8. ML reconciliation — make breakdown sum = final premium ─
    # Breakdown items are actuarial explainability factors; the final premium
    # comes from LightGBM × 0.7 calibration. Add an adjustment line so the
    # numbers the user sees always add up to exactly the quoted premium.
    if ml_used:
        breakdown_sum = round(sum(
            (item["amount"] if item["direction"] != "discount" else -abs(item["amount"]))
            for item in breakdown
        ), 2)
        ml_adj = round(premium - breakdown_sum, 2)
        if ml_adj != 0:
            breakdown.append({
                "factor":    "ML Personalization Adjustment",
                "amount":    abs(ml_adj),
                "direction": "increase" if ml_adj > 0 else "discount",
            })

    return {
        "predicted_premium": premium,
        "clad_score":        round(clad_score, 1),
        "clad_grade":        grade,
        "payout_speed":      payout_speed,
        "zone":              zone,
        "breakdown":         breakdown,
        "ml_used":           ml_used,
        "ml_error":          ml_error,  # None when LightGBM succeeds; error string for debugging
        "confidence":        "High" if ml_used else "Medium (actuarial fallback)",
        "model_version":     "LightGBM-v1" if ml_used else "actuarial-v1",
        "plan":              plan,
        "base_price":        base_price,
        "avg_daily_earning": avg_daily_earning,
    }