"""
data/dynamic_zone_risk.py
=========================
Replaces hardcoded zone_risk.py with LIVE computation for ANY Indian pincode.

Pipeline:
  1. Nominatim (OpenStreetMap) — pincode → lat/lon + city name  (free, no key)
  2. Open-Meteo Archive API   — last 365 days of daily rainfall  (free, no key)
  3. AQICN                    — current AQI as annual proxy       (uses AQICN_TOKEN)

Derived metrics:
  avg_rainfall_mm          = sum of 365 daily precipitation values
  disruption_days_per_year = days where rain > 7.5 mm (delivery-stopping threshold)
  flood_frequency          = disruption_days / 365   (0.0 – 1.0)
  waterlogging_score       = flood + heavy-rain weight (0.0 – 1.0)
  aqi_annual_avg           = live AQI from nearest station

Caching: results stored in-memory for 24 hours per pincode.
Fallback: DEFAULT_ZONE returned if all APIs fail.
"""

import os
import asyncio
import httpx
from datetime import datetime, timedelta
from typing import Optional

# ── In-memory cache: pincode → {data, computed_at} ────────────
_ZONE_CACHE: dict = {}
CACHE_TTL_HOURS   = 24

# ── Fast-path coords for common pincodes (skips geocoding) ────
KNOWN_COORDS = {
    "560034": {"lat": 12.9352, "lon": 77.6245, "city": "Koramangala, Bangalore"},
    "560038": {"lat": 12.9784, "lon": 77.6408, "city": "Indiranagar, Bangalore"},
    "560068": {"lat": 13.0359, "lon": 77.5970, "city": "Rajajinagar, Bangalore"},
    "560102": {"lat": 12.9116, "lon": 77.6473, "city": "HSR Layout, Bangalore"},
    "560066": {"lat": 12.9698, "lon": 77.7499, "city": "Whitefield, Bangalore"},
    "560078": {"lat": 12.9082, "lon": 77.5905, "city": "JP Nagar, Bangalore"},
    "560037": {"lat": 12.9591, "lon": 77.6974, "city": "Marathahalli, Bangalore"},
    "560001": {"lat": 12.9766, "lon": 77.5713, "city": "Bangalore MG Road"},
    "560029": {"lat": 12.9121, "lon": 77.6446, "city": "Bangalore JP Nagar"},
    "400001": {"lat": 18.9388, "lon": 72.8355, "city": "Mumbai Fort"},
    "400070": {"lat": 19.0728, "lon": 72.8826, "city": "Mumbai Andheri"},
    "110001": {"lat": 28.6315, "lon": 77.2167, "city": "Delhi Connaught Place"},
    "110092": {"lat": 28.6692, "lon": 77.3120, "city": "Delhi East"},
    "600001": {"lat": 13.0827, "lon": 80.2707, "city": "Chennai Central"},
    "600028": {"lat": 13.0418, "lon": 80.2341, "city": "Chennai T Nagar"},
    "603203": {"lat": 12.7828, "lon": 80.0162, "city": "Maraimalai Nagar, Chennai"},
    "603002": {"lat": 12.8231, "lon": 80.0444, "city": "Chengalpattu, Chennai"},
    "700001": {"lat": 22.5726, "lon": 88.3639, "city": "Kolkata BBD Bagh"},
    "500001": {"lat": 17.3850, "lon": 78.4867, "city": "Hyderabad Old City"},
    "411001": {"lat": 18.5204, "lon": 73.8567, "city": "Pune Shivajinagar"},
    "302001": {"lat": 26.9124, "lon": 75.7873, "city": "Jaipur"},
    "380001": {"lat": 23.0225, "lon": 72.5714, "city": "Ahmedabad"},
    "226001": {"lat": 26.8467, "lon": 80.9462, "city": "Lucknow"},
    "800001": {"lat": 25.5941, "lon": 85.1376, "city": "Patna"},
    "682001": {"lat": 9.9312,  "lon": 76.2673, "city": "Kochi"},
    "695001": {"lat": 8.5241,  "lon": 76.9366, "city": "Thiruvananthapuram"},
    "440001": {"lat": 21.1458, "lon": 79.0882, "city": "Nagpur"},
    "160017": {"lat": 30.7333, "lon": 76.7794, "city": "Chandigarh"},
}

DEFAULT_ZONE = {
    "flood_frequency":          0.40,
    "avg_rainfall_mm":          900.0,
    "aqi_annual_avg":           120.0,
    "waterlogging_score":       0.35,
    "disruption_days_per_year": 18,
    "city":                     "Unknown",
    "source":                   "default_fallback",
    "live":                     False,
    "computed_at":              None,
}


# ══════════════════════════════════════════════════════════════
# STEP 1 — Geocode pincode → lat/lon
# ══════════════════════════════════════════════════════════════
async def _geocode_pincode(pincode: str) -> Optional[dict]:
    """Nominatim (OpenStreetMap): Indian pincode → {lat, lon, city}"""
    if pincode in KNOWN_COORDS:
        return KNOWN_COORDS[pincode]

    url = (
        f"https://nominatim.openstreetmap.org/search"
        f"?postalcode={pincode}&countrycodes=in&format=json&limit=1&addressdetails=1"
    )
    try:
        async with httpx.AsyncClient(
            timeout=10.0,
            headers={"User-Agent": "CladInsurance/1.0 (tanmay@cladinsurance.in)"}
        ) as client:
            r = await client.get(url)
        if r.status_code == 200:
            data = r.json()
            if data:
                item    = data[0]
                addr    = item.get("address", {})
                city    = (
                    addr.get("suburb") or addr.get("city_district") or
                    addr.get("city") or addr.get("town") or
                    addr.get("state_district") or f"Pincode {pincode}"
                )
                return {
                    "lat":  float(item["lat"]),
                    "lon":  float(item["lon"]),
                    "city": city,
                }
    except Exception:
        pass
    return None


# ══════════════════════════════════════════════════════════════
# STEP 2 — Open-Meteo Archive: 365-day rainfall history
# ══════════════════════════════════════════════════════════════
async def _fetch_rainfall_history(lat: float, lon: float) -> Optional[dict]:
    """
    Fetches last 365 days of daily precipitation from Open-Meteo archive.
    Returns computed zone risk metrics.
    """
    end_date   = (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%d")
    start_date = (datetime.utcnow() - timedelta(days=367)).strftime("%Y-%m-%d")

    url = (
        f"https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&daily=precipitation_sum,rain_sum"
        f"&timezone=Asia%2FKolkata"
    )
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.get(url)
        if r.status_code == 200:
            data   = r.json()
            daily  = data.get("daily", {})
            precip = [float(v or 0) for v in daily.get("precipitation_sum", [])]

            if not precip:
                return None

            total_rain      = sum(precip)
            # Days where rain is severe enough to stop deliveries
            disruption_days = sum(1 for p in precip if p > 7.5)
            # Days with very heavy rain (increases waterlogging risk)
            heavy_days      = sum(1 for p in precip if p > 20.0)

            flood_freq      = round(min(disruption_days / max(len(precip), 1), 1.0), 3)
            # Waterlogging = flood days + extra weight for very heavy rain
            waterlogging    = round(min(
                (disruption_days + heavy_days * 0.5) / max(len(precip), 1),
                1.0
            ), 3)

            return {
                "avg_rainfall_mm":          round(total_rain, 1),
                "disruption_days_per_year": disruption_days,
                "flood_frequency":          flood_freq,
                "waterlogging_score":       waterlogging,
                "data_points":              len(precip),
                "source":                   "open-meteo-archive (live)",
            }
    except Exception:
        pass
    return None


# ══════════════════════════════════════════════════════════════
# STEP 3 — AQICN: live AQI for the location
# ══════════════════════════════════════════════════════════════
async def _fetch_aqi(lat: float, lon: float) -> float:
    """Returns current AQI from nearest AQICN station."""
    token = os.getenv("AQICN_TOKEN", "f01a354ce6bfcb14defbee7a1cbee54108f7a63f")
    url   = f"https://api.waqi.info/feed/geo:{lat};{lon}/?token={token}"
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            r = await client.get(url)
        if r.status_code == 200:
            d = r.json()
            if d.get("status") == "ok":
                return float(d["data"].get("aqi", 120) or 120)
    except Exception:
        pass
    return 120.0  # Indian national average fallback


# ══════════════════════════════════════════════════════════════
# MAIN: compute_zone_risk_live — the full pipeline
# ══════════════════════════════════════════════════════════════
async def compute_zone_risk_live(pincode: str) -> dict:
    """
    Full pipeline for ANY Indian pincode:
      pincode → geocode → archive rainfall → AQI → zone risk dict

    Returns dict compatible with ZONE_RISK_PROFILES format.
    Results cached for 24 hours.
    """
    pincode = str(pincode).strip()

    # ── Cache hit ────────────────────────────────────────────
    cached = _ZONE_CACHE.get(pincode)
    if cached:
        age_hours = (datetime.utcnow() - cached["computed_at"]).total_seconds() / 3600
        if age_hours < CACHE_TTL_HOURS:
            return cached["data"]

    # ── Step 1: Geocode ───────────────────────────────────────
    coords = await _geocode_pincode(pincode)
    if not coords:
        fallback = {
            **DEFAULT_ZONE,
            "city":   f"Pincode {pincode}",
            "source": "default_fallback (geocoding failed)",
        }
        return fallback

    lat, lon, city = coords["lat"], coords["lon"], coords["city"]

    # ── Steps 2 + 3: Archive rainfall + AQI (concurrent) ─────
    rainfall, aqi = await asyncio.gather(
        _fetch_rainfall_history(lat, lon),
        _fetch_aqi(lat, lon),
    )

    # Fallback if rainfall fetch failed
    if not rainfall:
        rainfall = {
            "avg_rainfall_mm":          DEFAULT_ZONE["avg_rainfall_mm"],
            "disruption_days_per_year": DEFAULT_ZONE["disruption_days_per_year"],
            "flood_frequency":          DEFAULT_ZONE["flood_frequency"],
            "waterlogging_score":       DEFAULT_ZONE["waterlogging_score"],
            "source":                   "default_fallback (archive API failed)",
        }

    zone = {
        "flood_frequency":          rainfall["flood_frequency"],
        "avg_rainfall_mm":          rainfall["avg_rainfall_mm"],
        "aqi_annual_avg":           round(aqi, 1),
        "waterlogging_score":       rainfall["waterlogging_score"],
        "disruption_days_per_year": rainfall["disruption_days_per_year"],
        "city":                     city,
        "lat":                      lat,
        "lon":                      lon,
        "pincode":                  pincode,
        "source":                   rainfall.get("source", "unknown"),
        "live":                     "open-meteo" in rainfall.get("source", ""),
        "computed_at":              datetime.utcnow().isoformat() + "Z",
    }

    # ── Cache it ──────────────────────────────────────────────
    _ZONE_CACHE[pincode] = {"data": zone, "computed_at": datetime.utcnow()}
    return zone


# ══════════════════════════════════════════════════════════════
# SYNC WRAPPER — for non-async callers (e.g. pricing_engine)
# ══════════════════════════════════════════════════════════════
def get_zone_risk_sync(pincode: str) -> dict:
    """
    Sync wrapper around compute_zone_risk_live.
    Checks cache first (no I/O if cache hit).
    If cache miss, runs the async pipeline in a new event loop.
    """
    pincode = str(pincode).strip()

    # Fast cache check — no async needed
    cached = _ZONE_CACHE.get(pincode)
    if cached:
        age_hours = (datetime.utcnow() - cached["computed_at"]).total_seconds() / 3600
        if age_hours < CACHE_TTL_HOURS:
            return cached["data"]

    # Cache miss — run async pipeline
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Inside an existing event loop (FastAPI) — use thread executor
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor() as pool:
                future = pool.submit(asyncio.run, compute_zone_risk_live(pincode))
                return future.result(timeout=30)
        else:
            return loop.run_until_complete(compute_zone_risk_live(pincode))
    except Exception:
        return {**DEFAULT_ZONE, "city": f"Pincode {pincode}", "source": "default_fallback (sync error)"}
