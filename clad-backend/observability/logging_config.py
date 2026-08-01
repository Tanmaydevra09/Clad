"""
observability/logging_config.py  —  Structured logging for Clad
================================================================
Uses structlog to emit JSON log lines with consistent fields:
  timestamp, level, service, request_id, correlation_id,
  event_id, claim_id, message, duration_ms

Every log line is machine-parseable.
"""

import logging
import os
import sys

try:
    import structlog
    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False


def configure_logging() -> None:
    """Configure structured logging. Call once at application startup."""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    if STRUCTLOG_AVAILABLE:
        structlog.configure(
            processors=[
                structlog.contextvars.merge_contextvars,
                structlog.stdlib.filter_by_level,
                structlog.stdlib.add_log_level,
                structlog.stdlib.add_logger_name,
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.format_exc_info,
                structlog.processors.JSONRenderer(),
            ],
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            wrapper_class=structlog.stdlib.BoundLogger,
            cache_logger_on_first_use=True,
        )

    logging.basicConfig(
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        level=getattr(logging, log_level, logging.INFO),
        stream=sys.stdout,
    )

    # Suppress noisy third-party loggers
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("motor").setLevel(logging.WARNING)
    logging.getLogger("pymongo").setLevel(logging.WARNING)


def get_logger(name: str):
    """Get a logger — returns structlog logger if available, else stdlib."""
    if STRUCTLOG_AVAILABLE:
        return structlog.get_logger(name)
    return logging.getLogger(name)
