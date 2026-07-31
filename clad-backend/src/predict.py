import os
import numpy as np
import warnings

FEATURE_ORDER = [
    "base_premium", "account_age_days", "clad_score", "delivery_consistency",
    "avg_daily_earning", "claim_free_weeks", "past_claims_count", "is_monsoon",
    "is_aqi_season", "flood_frequency", "avg_rainfall_mm", "aqi_annual_avg",
    "waterlogging_score", "disruption_days_per_year", "weekly_disruption_prob",
    "expected_weekly_payout"
]

_model  = None
_scaler = None

def _load():
    global _model, _scaler
    if _model is None:
        import joblib

        # Try multiple candidate paths so this works both locally and on Render.
        # Render runs uvicorn from clad-backend/, so __file__ may resolve differently.
        _this_dir = os.path.dirname(os.path.abspath(__file__))   # .../src/
        _app_dir  = os.path.dirname(_this_dir)                    # .../clad-backend/
        candidates = [
            os.path.join(_this_dir, "models"),           # local: src/models/
            os.path.join(_app_dir,  "src", "models"),    # Render: clad-backend/src/models/
            os.path.join(os.getcwd(), "src", "models"),  # cwd-relative fallback
        ]
        base = next((p for p in candidates if os.path.exists(os.path.join(p, "premium_model.pkl"))), None)
        if base is None:
            checked = ", ".join(candidates)
            raise RuntimeError(f"Model not found in any of: {checked}")

        model_path  = os.path.join(base, "premium_model.pkl")
        scaler_path = os.path.join(base, "scaler.pkl")
        _model  = joblib.load(model_path)
        _scaler = joblib.load(scaler_path)
        # Strip feature names from scaler so it never warns
        if hasattr(_scaler, 'feature_names_in_'):
            del _scaler.feature_names_in_

def predict(data: dict) -> float:
    _load()
    values   = np.array([[data[f] for f in FEATURE_ORDER]], dtype=np.float64)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        X_scaled = _scaler.transform(values)
        result   = _model.predict(X_scaled)
    return float(result[0])