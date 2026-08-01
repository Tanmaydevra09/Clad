<div align="center">

<img src="https://img.shields.io/badge/v4.0-Distributed_Systems-111110?style=for-the-badge&labelColor=7B3F00&color=111110" />

# 🛡 Clad
### AI-Powered Parametric Income Insurance for Gig Delivery Workers · Prototype

**[🚀 Live Demo](https://clad-frontend.onrender.com)** &nbsp;|&nbsp; *Built Jul – Dec 2025 as a proof-of-concept*

<br/>

![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat-square&logo=fastapi&logoColor=white)
![React](https://img.shields.io/badge/React-18-61DAFB?style=flat-square&logo=react&logoColor=black)
![LightGBM](https://img.shields.io/badge/ML-LightGBM-9B59B6?style=flat-square)
![R2](https://img.shields.io/badge/Model_R%C2%B2-0.92-2ecc71?style=flat-square)
![Render](https://img.shields.io/badge/Backend-Render-46E3B7?style=flat-square&logo=render&logoColor=white)
![Render](https://img.shields.io/badge/Frontend-Render-46E3B7?style=flat-square&logo=render&logoColor=white)
![Claude](https://img.shields.io/badge/Claude_Vision-Anthropic-D4A574?style=flat-square)
![Razorpay](https://img.shields.io/badge/Payments-Razorpay-3395FF?style=flat-square)

<br/>

> *"Every Monday morning, 15 million gig workers open their apps.*
> *On rainy days, they see zero orders. Zero earnings. No safety net.*
> *Clad is a prototype that demonstrates how parametric insurance could change that — automatically, in under 4 seconds."*

<br/>

**Team 4AM Club**

</div>

---

## 📍 Prototype Demo Deployment

> **⚠️ Prototype Notice:** This is a proof-of-concept demo, not a production insurance product. All payouts are simulated (Razorpay sandbox), data is ephemeral (JSON file), and no real insurance policies are issued.

| | URL | Status |
|---|---|---|
| 🌐 **Frontend Demo** | [clad-frontend.onrender.com](https://clad-frontend.onrender.com) | ![Demo](https://img.shields.io/badge/status-demo-F59E0B?style=flat-square) |
| ⚙️ **Backend API** | [clad-backend.onrender.com](https://clad-backend.onrender.com) | ![Demo](https://img.shields.io/badge/status-demo-F59E0B?style=flat-square) |



## 🎯 The Problem

India has **15 million** gig delivery workers. On their worst days — rain, storms, AQI spikes — they earn **₹0**. Traditional insurance doesn't cover lost wages. Manual claims take 3 weeks. No product exists for this gap.

```
A typical delivery worker faces:

  🌧  Monsoon rain        →  6–10 zero-earning days/month
  😷  Hazardous AQI       →  30+ unsafe days/year
  🌊  Waterlogging        →  Roads impassable, no orders
  🌪  Cyclonic wind       →  Delivery halted by platform
  ⚠️  Strike / Curfew    →  Movement restricted

  Result: Up to 30% of monthly income lost
  Workers insured today: < 1%
  Total addressable market: ₹8,400 Cr/year
```

---

## ✅ The Solution

**Clad is parametric income insurance.** Objective weather data triggers automatic UPI payouts. No paperwork. No adjuster. No waiting.

```
Traditional Insurance          Clad
──────────────────────         ──────────────────────
File a claim manually    →     Trigger fires automatically
Wait 3 weeks             →     Approved in < 4 seconds
Adjuster reviews         →     5-layer AI fraud engine
Generic payout           →     Personalised via Earning DNA
Annual premium           →     Weekly ₹29–₹79 (pay-cycle match)
City-wide trigger        →     3km pincode precision
```

---

## 🏗 System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CLAD FULL SYSTEM                             │
├──────────────────┬──────────────────────┬───────────────────────────┤
│   REACT FRONTEND │   FASTAPI BACKEND    │   EXTERNAL SERVICES       │
│   (Render)       │   (Render)           │                           │
│                  │                      │                           │
│  Splash Screen   │   17 REST endpoints  │  🌦 Open-Meteo            │
│  Onboarding x4   │   FastAPI v3.2       │     Rain + Wind + Weather │
│  Building Screen │   Python 3.11        │                           │
│  Home + Alerts   │                      │  😷 AQICN                 │
│  Claim + Camera  │   ┌──────────────┐   │     Real-time AQI         │
│  Payout (UPI)    │   │  LightGBM    │   │     150+ Indian cities    │
│  Admin Dashboard │   │  ML Engine   │   │                           │
│                  │   │  400 trees   │   │  🌊 Tomorrow.io           │
│  Framer Motion   │   │  R² = 0.92   │   │     Flood + Wind alerts   │
│  Zustand State   │   └──────────────┘   │                           │
│  Bricolage Font  │                      │  💳 Razorpay              │
│                  │   ┌──────────────┐   │     Contact → Fund → UPI  │
│                  │   │  5-Layer     │   │     Test sandbox mode     │
│                  │   │  Fraud Engine│   │                           │
│                  │   └──────────────┘   │  👁 Claude Vision         │
│                  │                      │     Photo fraud detect    │
│                  │   db_state.json      │     claude-opus-4-5       │
│                  │   (JSON persistence) │                           │
└──────────────────┴──────────────────────┴───────────────────────────┘
```

---

## ⚡ The 5 Parametric Triggers

All triggers evaluate at **pincode level within a 3km radius** — never city-wide.

| # | Trigger | Threshold | Payout Rate | Data Source |
|---|---------|-----------|:-----------:|-------------|
| 🌧 | **Heavy Rain** | >7.5 mm/hr sustained 45+ min | 60% daily | Open-Meteo |
| 😷 | **AQI Spike** | AQI >200 for 3+ hours | 30–50% | AQICN |
| 🌊 | **Waterlogging** | Zone score >0.65 + rain >6mm/hr | 50% | Zone DB + Open-Meteo |
| 🌪 | **Cyclone/Wind** | Wind speed >60 km/h | 50% | Tomorrow.io |

### Payout Formula

```
Payout = Worker Earning DNA (that day/hour) × Disruption Rate

Example:
  Worker daily baseline:  ₹720  (Koramangala, Monday 3pm)
  Trigger:                Heavy Rain (60%)
  Plan:                   Clad Plus

  Payout = ₹720 × 60% = ₹432  →  UPI in 2 hours  ✓
```

---

## 🤖 ML Premium Engine

### LightGBM Model

```
Input: 16 features per worker
       ↓
┌─────────────────────────────────────┐
│         LightGBM Regressor          │
│  400 estimators · max_depth 6       │
│  Training samples: 8,000            │
│  Test R²:  0.92                     │
│  Test MAE: ₹2.50                    │
│  Calibration factor: 0.7            │
└─────────────────────────────────────┘
       ↓
Output: Personalised weekly premium (₹20 – ₹120)
```

### Feature Importance (Top 8)

```
expected_weekly_payout      ████████████████████  32%
weekly_disruption_prob      ████████████████      25%
flood_frequency             █████████████         20%
avg_daily_earning           ████████████          18%
clad_score                  ██████████            15%
waterlogging_score          ████████              12%
is_monsoon                  ███████               11%
disruption_days_per_year    ██████                 9%
```

### Premium Breakdown (live example)

```
Base premium (Plus plan)              ₹49.00
+ Flood risk — zone factor            +₹12.96
+ AQI annual average risk             + ₹2.60
+ Zone disruption frequency           + ₹1.35
+ Monsoon season surcharge            + ₹8.00
− CladScore discount (Grade A)        − ₹5.60
− No-claim streak bonus (8 weeks)     − ₹5.60
                                      ────────
Final weekly premium                  ₹43.87
```

---

## 🏆 CladScore — Trust & Risk Engine

```
CladScore = (C1 × 30%) + (C2 × 25%) + (C3 × 25%) + (C4 × 20%)
```

| Component | Weight | What it measures |
|-----------|:------:|-----------------|
| **C1** Delivery Consistency | 30% | Active days, streak length, platform tenure |
| **C2** Location Honesty | 25% | GPS consistency, zone adherence |
| **C3** Claim Integrity | 25% | Approval rate, fraud flags, claim-free streak |
| **C4** Zone Risk Inverse | 20% | Flood frequency, historical disruption days |

### Grade → Payout Speed

```
Score   0 ──── 35 ──── 50 ──── 62 ──── 75 ──── 85 ──── 100
        │  D  │   C  │   B  │  B+  │   A  │  A+  │
Speed   │24hr │ 24hr │  6hr │  2hr │  2hr │Instant│
        │rvw  │ rvw  │ hold │ auto │ auto │       │
Mod     │+20% │ +10% │ base │  −4% │  −8% │  −12% │
```

---

## 🛡 5-Layer Fraud Engine

> *"You can spoof your GPS. You cannot spoof your tax history, your delivery timestamps, or the sound of rain outside your window."*

```
CLAIM SUBMITTED
      │
      ▼
┌─────────────────────────────────────────────────┐
│  LAYER 0  🔐  Account Integrity                 │
│  PAN format validation (regex only, no OTP)     │
│  Account age < 3 days → REJECT                  │
│  No delivery history → REJECT                   │
└──────────────────────┬──────────────────────────┘
                       │ PASS
                       ▼
┌─────────────────────────────────────────────────┐
│  LAYER 1  📏  10-Signal Rules Engine             │
│  Amount > 90% daily avg      → FLAG             │
│  Filing at 3am               → FLAG             │
│  Round number amount         → FLAG             │
│  GPS velocity impossible     → FLAG             │
│  Reason ≠ active trigger     → FLAG             │
└──────────────────────┬──────────────────────────┘
                       │ PASS
                       ▼
┌─────────────────────────────────────────────────┐
│  LAYER 2  🕸  NetworkX Graph Analysis            │
│  >8 claims/hr from same zone → FRAUD RING       │
│  Worker in 3+ zones          → FLAG             │
│  Device shared across accts  → FLAG             │
│  Social graph isolation      → FLAG             │
└──────────────────────┬──────────────────────────┘
                       │ PASS
                       ▼
┌─────────────────────────────────────────────────┐
│  LAYER 3  🌲  Isolation Forest ML               │
│  6 features: amount_ratio, hour, DOW,           │
│  claim_freq, clad_score, account_age            │
│  scikit-learn anomaly detection                 │
└──────────────────────┬──────────────────────────┘
                       │ PASS
                       ▼
┌─────────────────────────────────────────────────┐
│  LAYER 4  👁  Claude Vision API (requires key)  │
│  Real photo vs stock image    → DETECT          │
│  AI-generated image           → REJECT          │
│  Weather evidence present     → VERIFY          │
│  Scene = Indian street scene  → CONFIRM         │
│  (Falls back to MANUAL_REVIEW if key not set)   │
└──────────────────────┬──────────────────────────┘
                       │ ALL CLEAR
                       ▼
                CLAIM APPROVED ✓
              CladScore updated +2
```

### Fraud Routing Lanes

| Lane | Condition | Action | Speed |
|------|-----------|--------|-------|
| 🟢 **Green** | Score ≥75, account >60 days, all signals clean | Auto-payout | 2hr |
| 🟡 **Yellow** | 1–2 inconclusive signals | 6hr hold → auto-approve | 6hr |
| 🔴 **Red** | Hard anomaly or new account | 24hr manual review | 24hr |

---

## 💳 Razorpay Payout Flow (Sandbox / Prototype Only)

> **Prototype Note:** Razorpay is running in sandbox/test mode only. No real money moves. In this prototype, payouts are fully simulated — generated payout IDs confirm the flow works end-to-end but no actual UPI transfer occurs. Production integration would require a live Razorpay account with full KYC.

```
Worker taps "Send to UPI"
         │
         ▼
Step 1: POST /v1/contacts
        { name, contact, type: "employee" }
         │
         ▼
Step 2: POST /v1/fund_accounts
        { contact_id, account_type: "vpa", vpa: "worker@upi" }
         │
         ▼
Step 3: POST /v1/payouts
        { fund_account_id, amount, currency: "INR",
          mode: "UPI", purpose: "payout" }
         │
         ▼
    Payout ID generated and stored
    Worker notified on screen
    (Live UPI transfer requires rzp_live credentials)
```

---

## 📱 Frontend Screens

| Screen | Purpose | Key Feature |
|--------|---------|-------------|
| **Splash** | Role selection | Worker 🛵 or Admin 🏢 |
| **OB1–OB4** | Onboarding | Name → PAN (format check only, OTP is UI-only) → Zone → Plan |
| **Building** | ML engine running | LightGBM inference live |
| **Home** | Coverage overview | Live trigger notification banner |
| **ManualClaim** | File a claim | Camera + Claude Vision verify |
| **Analyzing** | AI photo check | Real-time fraud detection |
| **Claiming** | Processing animation | 4-step pipeline visual |
| **Payout** | Receive money | UPI or Bank transfer |
| **ClaimRejected** | Fraud blocked | CladScore penalty applied |
| **Policy** | Coverage details | Active policy + plan info |
| **Profile** | Worker profile | Score, grade, history |
| **AdminLogin** | Insurer access | Password protected |
| **AdminDash** | Full analytics | 7-tab live dashboard |

---

## 🏢 Admin Dashboard — 7 Tabs

The insurer dashboard pulls live data from 4 backend endpoints simultaneously, auto-refreshing every 8 seconds.

| Tab | Data Source | What it shows |
|-----|-------------|---------------|
| 📊 **Overview** | `/dashboard/insurer` | KPIs, loss ratio bar, trigger breakdown, CladScore distribution |
| 👥 **Workers** | `/workers` | Full searchable registry, fraud flags, scores, earnings |
| 📋 **Claims** | `/claims` | Complete ledger, filter by status, payout reference IDs |
| 🛡 **Fraud** | `/workers` + `/claims` | 5-layer engine, fraud rate %, savings, flagged workers |
| 🔮 **Forecast** | `/dashboard/insurer` | 7-day ML forecast bars, plan distribution, TAM analysis |
| 🌦 **Live** | `/trigger/check` | Real-time zone scan, live weather readings, active alerts |
| ⚡ **APIs** | `/api/health` | All 9 integration statuses, HTTP codes, backend specs |

**Admin password:** `clad2026`

---

## 🔌 API Reference

Base URL: `https://clad-backend.onrender.com`

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/health` | Health check with live counts |
| `GET` | `/api/health` | All 9 integration statuses |
| `POST` | `/register` | Register a gig worker |
| `POST` | `/policy/create` | Create insurance policy |
| `POST` | `/premium` | ML-powered premium calculation |
| `GET` | `/trigger/check?pincode=` | Run all 5 disruption triggers |
| `GET` | `/trigger/simulate?pincode=&trigger=` | Simulate a specific trigger |
| `GET` | `/claims` | List all claims |
| `GET` | `/claims/{user}` | Claims for one worker |
| `POST` | `/claims/create` | Submit a manual claim |
| `POST` | `/vision/verify` | Claude Vision photo fraud check |
| `POST` | `/payout` | Razorpay UPI payout (3-step) |
| `GET` | `/dashboard/worker/{name}` | Worker analytics dashboard |
| `GET` | `/dashboard/insurer` | Full insurer analytics |
| `GET` | `/workers` | All registered workers |
| `POST` | `/admin/reset?confirm=yes` | Reset database |
| `GET` | `/docs` | Interactive Swagger UI |

---

## 🗂 Project Structure

```
clad/
├── clad-backend/
│   ├── app.py                      FastAPI — all 17 routes
│   ├── Procfile                    Railway deployment config
│   ├── requirements.txt            Python dependencies
│   ├── db_state.json               JSON persistence (workers/policies/claims)
│   │
│   ├── core/
│   │   └── db.py                   In-memory store + JSON read/write
│   │
│   ├── services/
│   │   ├── pricing_engine.py       CladScore → LightGBM → premium breakdown
│   │   ├── real_trigger_service.py 5 live weather/civic triggers
│   │   ├── claim_service.py        Claim creation + auto-routing
│   │   ├── fraud_engine.py         5-layer fraud pipeline
│   │   ├── vision_fraud.py         Claude Vision photo analysis
│   │   └── pricing_service.py      Payout formula calculator
│   │
│   ├── data/
│   │   ├── zone_risk.py            7 Bangalore pincode risk profiles
│   │   ├── clad_score.py           CladScore 4-component model
│   │   └── training_data.csv       8,000 training samples
│   │
│   └── src/
│       ├── predict.py              LightGBM inference (lazy-loaded)
│       ├── train_model.py          Model training script
│       ├── premium_model.pkl       Trained LightGBM model
│       └── scaler.pkl              StandardScaler
│
└── clad-frontend/
    ├── src/
    │   ├── api/
    │   │   └── clad.js             All API calls (VITE_API_URL aware)
    │   ├── store/
    │   │   └── useStore.js         Zustand global state
    │   ├── screens/
    │   │   ├── Splash.jsx          Role selection screen
    │   │   ├── Onboarding.jsx      OB1–OB4 registration
    │   │   ├── Building.jsx        ML engine + API calls
    │   │   ├── Home.jsx            Dashboard + trigger notifications
    │   │   ├── Claim.jsx           Full claim flow + vision
    │   │   ├── PolicyProfile.jsx   Policy + profile screens
    │   │   └── Admin.jsx           Full insurer dashboard
    │   └── components/
    │       └── UI.jsx              SBar, BNav, ScoreRing, icons
    ├── package.json
    └── vite.config.js
```

---

## 🧰 Tech Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Frontend** | React 18 + Vite | Mobile-first PWA |
| **Animations** | Framer Motion | Screen transitions |
| **State** | Zustand | Global app state |
| **Typography** | Bricolage Grotesque | Brand font |
| **Backend** | FastAPI (Python 3.11) | REST API, 17 endpoints |
| **ML Model** | LightGBM | Dynamic premium pricing |
| **Anomaly Detection** | scikit-learn IsolationForest | Fraud Layer 3 |
| **Graph Analysis** | NetworkX | Fraud Layer 2 |
| **AI Vision** | Claude Vision (claude-opus-4-5) | Photo fraud Layer 4 |
| **Payments** | Razorpay (test mode) | UPI/NEFT payouts |
| **Weather** | Open-Meteo | Rain, wind, weather code |
| **Air Quality** | AQICN | Real-time AQI, 150+ cities |
| **Flood/Wind** | Tomorrow.io | Cyclone and flood alerts |
| **Frontend Deploy** | Render (Static Site) | Auto-deploy from GitHub |
| **Backend Deploy** | Render (Web Service) | Python + render.yaml Blueprint |
| **Persistence** | JSON file (db_state.json) | Prototype-grade data store (PostgreSQL + Redis planned for production) |

---

## 📦 Plan Tiers

| | Clad Basic | Clad Plus | Clad Pro |
|---|:---:|:---:|:---:|
| **Weekly cost** | ₹29 | ₹49 | ₹79 |
| **Weekly cap** | ₹800 | ₹1,500 | ₹2,500 |
| **Payout speed** | 24hr | 2hr | Instant |
| **Flood cap boost** | — | +50% during alerts | +50% |
| **Best for** | New workers | Most workers | High earners |

---

## 🔒 Exclusion Clauses

| # | Exclusion | Rationale |
|---|-----------|-----------|
| 1 | War or armed conflict | Systemic and uninsurable |
| 2 | WHO-declared pandemics | Systemic and uninsurable |
| 3 | Nationwide lockdowns | Government action, systemic |
| 4 | Terrorism or nuclear events | Force majeure |
| 5 | Worker-caused disruptions | Moral hazard |
| 6 | Platform deactivation (policy violation) | Worker's responsibility |
| 7 | Events below minimum duration | Data integrity |
| 8 | Events outside registered zone | Outside monitoring scope |
| 9 | Claims filed 6+ hours after event | Data integrity |
| 10 | Weeks where policy is paused | Policy inactive |

---

## 🚀 Running Locally

### Backend

```bash
cd clad-backend

# Install dependencies
pip install -r requirements.txt

# Create .env file
cp .env.example .env
# Fill in: AQICN_TOKEN, TOMORROW_IO_KEY, RAZORPAY_KEY_ID,
#          RAZORPAY_KEY_SECRET, ANTHROPIC_API_KEY

# Start server
uvicorn app:app --reload --port 8000

# API docs available at:
# http://localhost:8000/docs
```

### Frontend

```bash
cd clad-frontend

# Install dependencies
npm install

# Create local env
echo "VITE_API_URL=http://127.0.0.1:8000" > .env.local

# Start dev server
npm run dev

# Opens at http://localhost:5173
```

---

## 🌍 Deployment

Both services deploy together via **Render Blueprint** — one click, both backend and frontend.

### Blueprint (Recommended — deploys both at once)

```
1. Push the repo to GitHub (render.yaml is already at the root)
2. Go to render.com → New → Blueprint
3. Connect your GitHub repo
4. Render detects render.yaml automatically
5. Fill in the prompted secret values:
   - ANTHROPIC_API_KEY   → your Anthropic key
   - RAZORPAY_KEY_SECRET → your Razorpay secret
   - ALLOWED_ORIGINS     → set to https://clad-frontend.onrender.com after frontend deploys
6. Click "Apply" — both services deploy simultaneously
7. VITE_API_URL is injected automatically from the backend URL
```

### Manual Deploy (individual services)

**Backend → Render Web Service**
```
1. Render → New → Web Service → connect repo
2. Root Directory: clad-backend
3. Build Command: pip install -r requirements.txt
4. Start Command: uvicorn app:app --host 0.0.0.0 --port $PORT
5. Add environment variables in dashboard (see .env.example)
```

**Frontend → Render Static Site**
```
1. Render → New → Static Site → connect repo
2. Root Directory: clad-frontend
3. Build Command: npm install && npm run build
4. Publish Directory: dist
5. Add: VITE_API_URL = https://clad-backend.onrender.com
```

---

## 📊 Key Metrics

| Metric | Value |
|--------|-------|
| ML Model R² | **0.92** |
| Training samples | **8,000** |
| Live APIs | **9** |
| Backend endpoints | **17** |
| Fraud detection layers | **5** |
| Payout speed (Plus plan) | **< 2 hours** |
| End-to-end trigger → approval | **< 4 seconds** |
| Demo loss ratio | **48.3%** (target <55%) |
| India gig workforce | **15 million** |
| Currently insured | **< 1%** |
| Addressable market | **₹8,400 Cr/year** |

---

## 🗺 Roadmap (from Prototype → Production)

```
PROTOTYPE ✅             PRODUCTION (6 MO)          SCALE (12 MO)
─────────────────         ──────────────────         ──────────────────
React frontend demo       PostgreSQL 15              Aadhaar eKYC
FastAPI backend POC       Redis + Celery queues      IRDAI sandbox
LightGBM ML engine        Real Razorpay prod keys    Insurance license
5-layer fraud engine      Platform webhooks          Reinsurance deal
Claude Vision verify      PWA push notifications     Series A raise
Razorpay sandbox only     IRDAI consultation         50K workers pilot
5 live trigger APIs       500-worker Zepto pilot
CladScore system
Render deploy (Blueprint)
```

---

## 👨‍💻 Team

**4AM Club**

Prototype built by Tanmay Devra across 6 weeks as a solo proof-of-concept — covering FastAPI backend, LightGBM ML engine, 5-layer fraud detection, Claude Vision integration, Razorpay sandbox payout flow, and React mobile frontend. Built to validate technical feasibility and demonstrate the full product vision end-to-end.

> *"We're called 4AM Club because that's when Ravi starts his shift. That's when we started building too."*

---

<div align="center">

**Built by Team 4AM Club**

<br/>

[![Live Demo](https://img.shields.io/badge/🚀_Live_Demo-clad--frontend.onrender.com-7B3F00?style=for-the-badge)](https://clad-frontend.onrender.com)

<br/>

*Clad — A prototype demonstrating what "always covered" could look like.*

</div>

---

## 🏗 V4 — Distributed Systems Architecture

> **What changed in v4:** Evolved from a prototype with JSON file persistence into a production-grade distributed system. All existing API contracts, fraud engine, and ML models are **unchanged** — only the infrastructure layer was upgraded.

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CLAD v4 ARCHITECTURE                          │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│   React Frontend (Vite)                                               │
│         │ HTTP                                                        │
│         ▼                                                             │
│   FastAPI Backend (app.py v4.0)                                       │
│   ├── Request ID middleware (X-Request-ID header)                     │
│   ├── Prometheus metrics middleware                                   │
│   ├── /health  /readiness  /metrics                                  │
│   ├── lifespan: MongoDB + Redis + Kafka startup                       │
│   │                                                                   │
│   ├── MongoDB (Motor async)          ← operational source of truth   │
│   │   ├── workers / policies                                          │
│   │   ├── claims  (UNIQUE: claim_id)                                  │
│   │   ├── payouts (UNIQUE: claim_id + idempotency_key)                │
│   │   ├── outbox_events              ← transactional outbox           │
│   │   └── processed_events          ← idempotency records            │
│   │                                                                   │
│   ├── Redis (aioredis)               ← short-lived env cache         │
│   │   └── weather/AQI/trigger/premium (TTL 2-10 min)                 │
│   │                                                                   │
│   └── Outbox Publisher (asyncio task)                                 │
│       └── polls outbox_events → publishes to Kafka                    │
│                                                                       │
│   Kafka (confluent-kafka)                                             │
│   ├── disruption.detected   (3 partitions, key=pincode)               │
│   ├── claim.created         (6 partitions, key=claim_id)              │
│   ├── claim.approved        (6 partitions, key=claim_id)              │
│   ├── claim.rejected        (3 partitions)                            │
│   ├── payout.requested      (6 partitions, key=claim_id)              │
│   ├── payout.completed      (3 partitions)                            │
│   └── *.dlq                 (Dead Letter Queues)                      │
│                                                                       │
│   Consumers (independent processes)                                   │
│   ├── FraudConsumer                                                   │
│   │   consumes: claim.created                                         │
│   │   runs: 5-layer fraud engine (unchanged)                          │
│   │   produces: claim.approved / claim.rejected                       │
│   └── PayoutConsumer                                                  │
│       consumes: claim.approved                                        │
│       calls: Razorpay API (idempotent key: CLAD-{claim_id})           │
│       produces: payout.completed                                      │
│                                                                       │
│   Snowflake (OLAP — analytics only)                                   │
│   ├── ETL Pipeline: MongoDB → Snowflake (incremental, watermark)      │
│   ├── FACT_CLAIMS / FACT_PAYOUTS / FACT_TRIGGER_EVENTS                │
│   ├── DIM_WORKER / DIM_LOCATION / DIM_DATE / DIM_DISRUPTION           │
│   └── Analytics views: V_CLAIMS_BY_CITY / V_MONTHLY_PAYOUTS          │
│                                                                       │
│   CRITICAL RULE: Snowflake failure NEVER blocks claim processing      │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
```

### New Files Added (v4)

```
clad-backend/
├── db/
│   ├── mongo.py        — Motor async client, init/shutdown hooks
│   ├── indexes.py      — All MongoDB index definitions
│   ├── operations.py   — Centralised CRUD (all DB calls go here)
│   └── seed.py         — db_state.json → MongoDB migration
├── cache/
│   └── redis_client.py — Async Redis with cache-aside helpers
├── kafka/
│   ├── topics.py       — Topic name constants (never hardcoded)
│   └── producer.py     — Standard event envelope + DLQ publishing
├── consumers/
│   ├── base.py         — Retry + idempotency + DLQ base class
│   ├── fraud_consumer.py   — claim.created → 5-layer fraud engine
│   └── payout_consumer.py  — claim.approved → Razorpay payout
├── outbox/
│   └── publisher.py    — Transactional outbox async publisher
├── etl/
│   ├── snowflake_client.py — DDL + connection management
│   └── pipeline.py     — Incremental ETL with watermarking
├── observability/
│   ├── metrics.py      — All Prometheus counters/histograms
│   └── logging_config.py — Structured JSON logging (structlog)
└── tests/
    └── test_idempotency.py — 13 tests, all passing
docker-compose.yml          — Full local stack
load_test.js                — k6 load test (3 scenarios)
.env.example                — All env vars documented
```

### Idempotency — Three Layers

| Layer | Mechanism | Code Location |
|-------|-----------|---------------|
| **Application** | `processed_events` collection: `(event_id, consumer_name)` UNIQUE | `db/operations.py::mark_event_processed()` |
| **Database** | `payouts.claim_id` UNIQUE index: raises `DuplicateKeyError` on duplicate | `db/indexes.py` |
| **External API** | Razorpay `X-Payout-Idempotency: CLAD-{claim_id}` (deterministic, no random) | `consumers/payout_consumer.py` |

> **Bug fixed:** The original code used `random.randint(1000,9999)` in the Razorpay idempotency key. This meant every retry generated a **different** key, creating duplicate payouts. v4 uses a deterministic `f"CLAD-{claim_id}"`.

### Transactional Outbox Pattern

```
POST /claims/create
  │
  ├── (with MongoDB) Start transaction
  │     ├── INSERT claims (claim document)
  │     └── INSERT outbox_events (pending event)
  │   COMMIT atomically
  │
  └── Background outbox publisher (100ms poll):
        ├── SELECT * FROM outbox_events WHERE status='pending'
        ├── Publish to Kafka
        └── UPDATE outbox_events SET status='published'

If Kafka is down:  events accumulate safely in MongoDB
When Kafka recovers:  publisher drains the backlog automatically
```

### MongoDB Index Strategy

| Collection | Index | Reason |
|-----------|-------|--------|
| `workers` | `UNIQUE(name)` | Business key |
| `claims` | `UNIQUE(claim_id)` | Business key |
| `claims` | `(worker_name, created_at DESC)` | Worker claim history query |
| `claims` | `(status, payout_processed)` | Payout queue query |
| `payouts` | `UNIQUE(claim_id)` | **Idempotency — DB layer** |
| `payouts` | `UNIQUE(idempotency_key)` | Razorpay dedup |
| `outbox_events` | `UNIQUE(event_id)` + partial on `status=pending` | Publisher poll |
| `processed_events` | `UNIQUE(event_id, consumer_name)` | **Idempotency — app layer** |

### Snowflake Dimensional Model

```
                    DIM_DATE ──────────┐
                    DIM_WORKER ────────┤
                    DIM_LOCATION ──────┼──► FACT_CLAIMS
                    DIM_DISRUPTION ────┘         │
                    DIM_POLICY ────────────────────┘
                                             │
                                        FACT_PAYOUTS
                                             │
                                  FACT_TRIGGER_EVENTS
```

**ETL design:**
- **Incremental**: only processes `updated_at > last_watermark`
- **Idempotent**: uses Snowflake `MERGE` (not `INSERT`) — re-runnable
- **Isolated**: Snowflake failure never propagates to API routes
- **Watermarks**: stored in both MongoDB and Snowflake for recovery

### Event Schema (Standard Envelope)

Every Kafka event produced by Clad follows this schema:
```json
{
  "event_id":      "uuid4",
  "event_type":    "claim.created",
  "event_version": 1,
  "timestamp":     "2026-08-01T18:00:00Z",
  "correlation_id": "uuid4",
  "producer":      "clad-api",
  "payload": {
    "claim_id":    "CLM-20260801-ABCD",
    "worker_name": "Alice",
    "amount":      500
  }
}
```

### Local Development (Docker Compose)

```bash
# 1. Clone & setup
git clone https://github.com/yourusername/clad
cd clad
cp .env.example .env   # fill in RAZORPAY_KEY_SECRET, ANTHROPIC_API_KEY

# 2. Start all infrastructure
docker compose up -d

# 3. Verify everything is healthy
curl http://localhost:8000/readiness

# 4. Seed from existing JSON (auto-runs on first startup if MongoDB is empty)
cd clad-backend && python3 -m db.seed

# 5. Run tests
python3 -m pytest tests/ -v

# 6. Load test (requires k6)
brew install k6
k6 run load_test.js
```

Services available locally:

| Service | URL |
|---------|-----|
| FastAPI API | http://localhost:8000 |
| API Docs | http://localhost:8000/docs |
| Prometheus metrics | http://localhost:8000/metrics |
| Readiness probe | http://localhost:8000/readiness |
| MongoDB | mongodb://localhost:27017 |
| Redis | redis://localhost:6379 |
| Kafka | localhost:9092 |

### Kafka Topic Design

| Topic | Partitions | Key | Consumers |
|-------|-----------|-----|-----------|
| `disruption.detected` | 3 | pincode | analytics |
| `claim.created` | 6 | claim_id | `fraud-processor` |
| `claim.approved` | 6 | claim_id | `payout-processor` |
| `claim.rejected` | 3 | claim_id | analytics |
| `payout.completed` | 3 | claim_id | analytics |
| `claim.processing.dlq` | 1 | — | ops alerts |
| `payout.processing.dlq` | 1 | — | ops alerts |

### Consumer Reliability

The `BaseConsumer` class provides:
1. **Idempotency check** before processing (skip duplicates)
2. **Exponential backoff** retry: 1s → 2s → 4s → 8s (4 attempts)
3. **DLQ publishing** after max retries
4. **Manual offset commit** after successful processing (at-least-once)
5. **Prometheus metrics** per event: consumed/errors/DLQ count

### New API Endpoints (v4)

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Infrastructure status (MongoDB/Redis/Kafka) |
| `GET /readiness` | Kubernetes readiness probe |
| `GET /metrics` | Prometheus metrics (text format) |
| `GET /admin/outbox/stats` | Outbox backlog: pending/published/failed |
| `GET /admin/dlq/{topic}` | Inspect DLQ events |

### Test Results

```
============================= test session starts ==============================
collected 13 items

tests/test_idempotency.py::TestDBLayerIdempotency::test_duplicate_payout_raises_duplicate_key_error PASSED
tests/test_idempotency.py::TestDBLayerIdempotency::test_idempotency_key_is_deterministic         PASSED
tests/test_idempotency.py::TestDBLayerIdempotency::test_old_buggy_key_was_non_deterministic      PASSED
tests/test_idempotency.py::TestAppLayerIdempotency::test_mark_event_processed_returns_false_on_duplicate PASSED
tests/test_idempotency.py::TestAppLayerIdempotency::test_is_event_processed_returns_true_for_existing   PASSED
tests/test_idempotency.py::TestClaimPayoutLifecycle::test_claim_id_format                        PASSED
tests/test_idempotency.py::TestClaimPayoutLifecycle::test_payout_blocked_if_claim_not_approved   PASSED
tests/test_idempotency.py::TestClaimPayoutLifecycle::test_outbox_event_has_required_fields       PASSED
tests/test_idempotency.py::TestClaimPayoutLifecycle::test_worker_document_flattening             PASSED
tests/test_idempotency.py::TestFraudEngineIntegrity::test_fraud_engine_returns_expected_keys     PASSED
tests/test_idempotency.py::TestFraudEngineIntegrity::test_fraud_engine_rejects_high_fraud_score  PASSED
tests/test_idempotency.py::TestETLIdempotency::test_date_key_is_stable                          PASSED
tests/test_idempotency.py::TestETLIdempotency::test_score_to_risk_segment                       PASSED

========================= 13 passed in 4.54s ==================================
```

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `MONGO_URI` | Yes | MongoDB connection string |
| `MONGO_DB_NAME` | No | Database name (default: `clad_insurance`) |
| `REDIS_URL` | No | Redis connection (caching disabled if unset) |
| `KAFKA_BROKERS` | No | Kafka brokers (events via outbox if unset) |
| `KAFKA_SASL_USERNAME` | No | For Upstash/cloud Kafka |
| `KAFKA_SASL_PASSWORD` | No | For Upstash/cloud Kafka |
| `SNOWFLAKE_ACCOUNT` | No | ETL disabled if unset |
| `SNOWFLAKE_USER` | No | Snowflake username |
| `SNOWFLAKE_PASSWORD` | No | Snowflake password |
| `RAZORPAY_KEY_ID` | Yes | Razorpay API key |
| `RAZORPAY_KEY_SECRET` | Yes | Razorpay secret |
| `ANTHROPIC_API_KEY` | Yes | Claude Vision key |
| `AQICN_TOKEN` | Yes | AQI API token |
| `TOMORROW_IO_KEY` | Yes | Tomorrow.io weather key |

### Graceful Degradation

The system is designed to work at reduced capacity if any non-critical service is unavailable:

| Service Down | Impact |
|--------------|--------|
| Redis | Cache misses → slower weather/AQI responses |
| Kafka | Claims processed synchronously (no async fraud consumer) |
| Snowflake | ETL paused, analytics stale — **core claims unaffected** |
| Outbox publisher | Events queue in MongoDB, publish on recovery |

MongoDB is the **only critical dependency**. If MongoDB is down, the app falls back to JSON file persistence automatically.
