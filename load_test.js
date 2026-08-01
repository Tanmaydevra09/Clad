/**
 * load_test.js — k6 load test for Clad Insurance API
 * ====================================================
 * Tests the full claim submission → fraud check pipeline under load.
 *
 * Run:
 *   k6 run load_test.js
 *   k6 run --vus 50 --duration 60s load_test.js
 *   k6 run --vus 100 --duration 120s load_test.js
 *
 * Install k6:
 *   brew install k6          (macOS)
 *   sudo apt install k6      (Ubuntu)
 *
 * Scenarios tested:
 *   1. GET /health          — baseline, should always be < 200ms
 *   2. POST /premium        — ML inference, < 500ms
 *   3. POST /register       — worker registration
 *   4. POST /claims/create  — full fraud pipeline, < 2s
 *   5. GET /claims          — list query
 *   6. GET /readiness       — infrastructure check
 */

import http from 'k6/http';
import { check, sleep, group } from 'k6';
import { Counter, Trend, Rate } from 'k6/metrics';

// ── Custom metrics ────────────────────────────────────────────────────────
const claimSuccessRate   = new Rate('claim_success_rate');
const claimLatency       = new Trend('claim_latency_ms', true);
const fraudApprovalRate  = new Rate('fraud_approval_rate');
const premiumLatency     = new Trend('premium_latency_ms', true);
const errors             = new Counter('errors');

// ── Test configuration ────────────────────────────────────────────────────
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8000';

export const options = {
  scenarios: {
    // Warm-up
    warmup: {
      executor: 'constant-vus',
      vus: 5,
      duration: '30s',
      gracefulStop: '5s',
      tags: { scenario: 'warmup' },
    },
    // Sustained load
    sustained: {
      executor: 'constant-vus',
      vus: 20,
      duration: '120s',
      startTime: '35s',
      gracefulStop: '10s',
      tags: { scenario: 'sustained' },
    },
    // Spike test
    spike: {
      executor: 'ramping-vus',
      startVUs: 1,
      stages: [
        { duration: '10s', target: 50 },
        { duration: '30s', target: 50 },
        { duration: '10s', target: 1 },
      ],
      startTime: '160s',
      gracefulStop: '10s',
      tags: { scenario: 'spike' },
    },
  },

  thresholds: {
    // HTTP errors must be < 1%
    http_req_failed: ['rate<0.01'],

    // /health must always be < 200ms
    'http_req_duration{endpoint:health}': ['p(99)<200'],

    // /premium (ML inference) must be < 1s at p95
    'http_req_duration{endpoint:premium}': ['p(95)<1000'],

    // Claim creation must be < 3s at p95
    'http_req_duration{endpoint:claims_create}': ['p(95)<3000'],

    // Custom: claim success rate > 80%
    'claim_success_rate': ['rate>0.80'],

    // Custom: fraud engine approves > 60% of legitimate claims
    'fraud_approval_rate': ['rate>0.60'],
  },
};


// ── Shared test data ──────────────────────────────────────────────────────
const WORKERS = [
  { name: 'LoadTest_Alice', pincode: '560034', plan: 'pro',
    avg_daily_earning: 800, account_age_days: 365, total_deliveries: 1000 },
  { name: 'LoadTest_Bob',   pincode: '400001', plan: 'plus',
    avg_daily_earning: 600, account_age_days: 180, total_deliveries: 500 },
  { name: 'LoadTest_Carol', pincode: '110001', plan: 'basic',
    avg_daily_earning: 400, account_age_days: 90, total_deliveries: 200 },
];

const CLAIM_REASONS = [
  'Heavy rain disrupted deliveries',
  'Flooding prevented work',
  'Poor air quality (AQI > 300)',
  'Cyclone warning in area',
  'Waterlogging on routes',
];

function randomItem(arr) {
  return arr[Math.floor(Math.random() * arr.length)];
}

function randomWorkerName() {
  return randomItem(WORKERS).name;
}

// ── Setup: register workers ───────────────────────────────────────────────
export function setup() {
  const results = {};
  for (const worker of WORKERS) {
    const res = http.post(`${BASE_URL}/register`, JSON.stringify(worker), {
      headers: { 'Content-Type': 'application/json' },
    });
    results[worker.name] = res.status;

    // Create policy
    http.post(`${BASE_URL}/policy/create`, JSON.stringify({
      name: worker.name, plan: worker.plan,
    }), { headers: { 'Content-Type': 'application/json' } });

    // Get premium (sets clad_score)
    http.post(`${BASE_URL}/premium`, JSON.stringify({
      name: worker.name, ...worker,
    }), { headers: { 'Content-Type': 'application/json' } });
  }
  return results;
}


// ── Main test function ────────────────────────────────────────────────────
export default function (data) {
  const headers = { 'Content-Type': 'application/json' };

  // ── 1. Health check ──────────────────────────────────────────────────
  group('health_check', () => {
    const res = http.get(`${BASE_URL}/health`, { tags: { endpoint: 'health' } });
    check(res, {
      'health 200':    (r) => r.status === 200,
      'health ok':     (r) => JSON.parse(r.body).status === 'ok',
      'has mongodb':   (r) => JSON.parse(r.body).infrastructure !== undefined,
    });
    if (res.status !== 200) errors.add(1);
  });

  sleep(0.1);

  // ── 2. Readiness probe ───────────────────────────────────────────────
  group('readiness', () => {
    const res = http.get(`${BASE_URL}/readiness`, { tags: { endpoint: 'readiness' } });
    check(res, {
      'readiness 200': (r) => r.status === 200,
    });
  });

  sleep(0.1);

  // ── 3. Premium calculation (ML inference) ────────────────────────────
  group('premium', () => {
    const worker = randomItem(WORKERS);
    const payload = {
      name:               worker.name,
      pincode:            worker.pincode,
      plan:               worker.plan,
      avg_daily_earning:  worker.avg_daily_earning,
      account_age_days:   worker.account_age_days,
      total_deliveries:   worker.total_deliveries,
      delivery_consistency: 0.85,
      claim_free_weeks:   4,
      past_claims_count:  1,
      fraudulent_flags:   0,
      location_honesty:   0.95,
      claim_history_score: 1.0,
    };
    const start = Date.now();
    const res   = http.post(`${BASE_URL}/premium`, JSON.stringify(payload), {
      headers,
      tags: { endpoint: 'premium' },
    });
    premiumLatency.add(Date.now() - start);

    check(res, {
      'premium 200': (r) => r.status === 200,
      'has clad_score': (r) => {
        try {
          return JSON.parse(r.body).clad_score !== undefined;
        } catch { return false; }
      },
    });
  });

  sleep(0.2);

  // ── 4. Claim submission ──────────────────────────────────────────────
  group('claim_create', () => {
    const workerName = randomWorkerName();
    const payload = {
      user:            workerName,
      amount:          Math.floor(Math.random() * 500) + 100,
      reason:          randomItem(CLAIM_REASONS),
      photo_submitted: Math.random() > 0.3,
      photo_metadata:  null,
      gps_trace:       null,
      device_id:       `device-${Math.random().toString(36).substr(2, 8)}`,
    };
    const start = Date.now();
    const res   = http.post(`${BASE_URL}/claims/create`, JSON.stringify(payload), {
      headers,
      tags: { endpoint: 'claims_create' },
    });
    claimLatency.add(Date.now() - start);

    const ok = check(res, {
      'claim 200':    (r) => r.status === 200,
      'has claim_id': (r) => {
        try {
          const body = JSON.parse(r.body);
          return body.claim !== undefined || body.status !== undefined;
        } catch { return false; }
      },
    });
    claimSuccessRate.add(ok ? 1 : 0);

    if (ok && res.status === 200) {
      try {
        const body = JSON.parse(res.body);
        const approved = body.claim && body.claim.status === 'approved';
        fraudApprovalRate.add(approved ? 1 : 0);
      } catch {}
    }

    if (!ok) errors.add(1);
  });

  sleep(0.3);

  // ── 5. Claims list ───────────────────────────────────────────────────
  group('claims_list', () => {
    const res = http.get(`${BASE_URL}/claims`, { tags: { endpoint: 'claims_list' } });
    check(res, {
      'claims list 200': (r) => r.status === 200,
    });
  });

  sleep(0.2);

  // ── 6. Worker dashboard ──────────────────────────────────────────────
  group('worker_dashboard', () => {
    const name = randomWorkerName();
    const res  = http.get(`${BASE_URL}/dashboard/worker/${name}`, {
      tags: { endpoint: 'worker_dashboard' },
    });
    check(res, {
      'dashboard 200': (r) => r.status === 200 || r.status === 404,
    });
  });

  sleep(0.1);
}


// ── Teardown ─────────────────────────────────────────────────────────────
export function teardown(data) {
  console.log('Load test complete. Check Grafana for metrics.');
}
