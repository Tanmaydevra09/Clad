"""
observability/metrics.py  —  Prometheus metrics for Clad
=========================================================
All counters and histograms defined here.
Import this module early so metrics are available everywhere.

Exposed at GET /metrics in Prometheus text format.
"""

try:
    from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, REGISTRY
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False


def _noop(*args, **kwargs):
    class _Stub:
        def inc(self, *a, **k): pass
        def observe(self, *a, **k): pass
        def set(self, *a, **k): pass
        def labels(self, *a, **k): return self
        def time(self): return self
        def __enter__(self): return self
        def __exit__(self, *a): pass
    return _Stub()


if PROMETHEUS_AVAILABLE:
    # ── HTTP ──────────────────────────────────────────────────
    http_requests_total = Counter(
        "clad_http_requests_total",
        "Total HTTP requests",
        ["method", "endpoint", "status_code"]
    )
    http_request_duration = Histogram(
        "clad_http_request_duration_seconds",
        "HTTP request duration",
        ["method", "endpoint"],
        buckets=[.01, .05, .1, .25, .5, 1, 2.5, 5, 10]
    )

    # ── Claims ────────────────────────────────────────────────
    claims_created_total   = Counter("clad_claims_created_total",   "Claims submitted")
    claims_approved_total  = Counter("clad_claims_approved_total",  "Claims approved")
    claims_rejected_total  = Counter("clad_claims_rejected_total",  "Claims rejected", ["reason"])
    fraud_score_histogram  = Histogram(
        "clad_fraud_score",
        "Fraud score distribution",
        buckets=[10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    )

    # ── Processing durations ───────────────────────────────────
    fraud_processing_duration = Histogram(
        "clad_fraud_processing_duration_seconds",
        "Time to run 5-layer fraud engine",
        buckets=[.05, .1, .25, .5, 1, 2, 5]
    )
    payout_processing_duration = Histogram(
        "clad_payout_processing_duration_seconds",
        "Time to process Razorpay payout",
        buckets=[.1, .5, 1, 2, 5, 10, 30]
    )
    vision_processing_duration = Histogram(
        "clad_vision_processing_duration_seconds",
        "Time for Claude Vision analysis",
        buckets=[.5, 1, 2, 5, 10, 30]
    )

    # ── External APIs ─────────────────────────────────────────
    external_api_calls_total = Counter(
        "clad_external_api_calls_total",
        "Calls to external APIs",
        ["service", "status"]
    )
    external_api_errors_total = Counter(
        "clad_external_api_errors_total",
        "External API errors",
        ["service", "error_type"]
    )

    # ── Kafka ─────────────────────────────────────────────────
    kafka_events_produced_total = Counter(
        "clad_kafka_events_produced_total",
        "Kafka events produced",
        ["topic"]
    )
    kafka_events_consumed_total = Counter(
        "clad_kafka_events_consumed_total",
        "Kafka events consumed",
        ["topic", "consumer_group"]
    )
    kafka_consumer_errors_total = Counter(
        "clad_kafka_consumer_errors_total",
        "Kafka consumer errors",
        ["topic", "error_type"]
    )
    kafka_dlq_events_total = Counter(
        "clad_kafka_dlq_events_total",
        "Events sent to DLQ",
        ["topic"]
    )

    # ── Redis ─────────────────────────────────────────────────
    redis_cache_hits_total   = Counter("clad_redis_cache_hits_total",   "Redis cache hits",   ["key_pattern"])
    redis_cache_misses_total = Counter("clad_redis_cache_misses_total", "Redis cache misses", ["key_pattern"])
    redis_cache_hit_rate     = Gauge("clad_redis_cache_hit_rate",       "Redis cache hit rate (0-100)")

    # ── MongoDB ───────────────────────────────────────────────
    mongodb_operation_duration = Histogram(
        "clad_mongodb_operation_duration_seconds",
        "MongoDB operation latency",
        ["collection", "operation"],
        buckets=[.001, .005, .01, .05, .1, .25, .5, 1]
    )
    mongodb_errors_total = Counter(
        "clad_mongodb_errors_total",
        "MongoDB operation errors",
        ["collection", "error_type"]
    )

    # ── Outbox ────────────────────────────────────────────────
    outbox_pending_events  = Gauge("clad_outbox_pending_events", "Outbox events awaiting Kafka publication")
    outbox_published_total = Counter("clad_outbox_published_total", "Outbox events successfully published")
    outbox_retries_total   = Counter("clad_outbox_retries_total",   "Outbox publish retries")

    # ── ETL ───────────────────────────────────────────────────
    etl_rows_extracted_total = Counter("clad_etl_rows_extracted_total", "ETL rows extracted from MongoDB", ["collection"])
    etl_rows_loaded_total    = Counter("clad_etl_rows_loaded_total",    "ETL rows loaded to Snowflake",   ["fact_table"])
    etl_duration_seconds     = Histogram("clad_etl_duration_seconds",   "ETL job duration", ["job_name"])
    etl_last_success         = Gauge("clad_etl_last_success_timestamp", "Unix timestamp of last successful ETL")
    etl_failures_total       = Counter("clad_etl_failures_total",       "ETL job failures", ["job_name"])

else:
    # Stub everything if prometheus_client not installed
    http_requests_total         = _noop()
    http_request_duration       = _noop()
    claims_created_total        = _noop()
    claims_approved_total       = _noop()
    claims_rejected_total       = _noop()
    fraud_score_histogram       = _noop()
    fraud_processing_duration   = _noop()
    payout_processing_duration  = _noop()
    vision_processing_duration  = _noop()
    external_api_calls_total    = _noop()
    external_api_errors_total   = _noop()
    kafka_events_produced_total = _noop()
    kafka_events_consumed_total = _noop()
    kafka_consumer_errors_total = _noop()
    kafka_dlq_events_total      = _noop()
    redis_cache_hits_total      = _noop()
    redis_cache_misses_total    = _noop()
    redis_cache_hit_rate        = _noop()
    mongodb_operation_duration  = _noop()
    mongodb_errors_total        = _noop()
    outbox_pending_events       = _noop()
    outbox_published_total      = _noop()
    outbox_retries_total        = _noop()
    etl_rows_extracted_total    = _noop()
    etl_rows_loaded_total       = _noop()
    etl_duration_seconds        = _noop()
    etl_last_success            = _noop()
    etl_failures_total          = _noop()
