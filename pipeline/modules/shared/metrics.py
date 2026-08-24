# pipeline/modules/shared/metrics.py
import logging
from typing import Dict

logger = logging.getLogger(__name__)

# F-08: Prometheus metrics — lazy init to avoid import errors if prometheus_client not installed
_prom_initialized = False
_BATCH_PROCESSED = None
_EVENTS_DETECTED = None
_BATCH_ERRORS = None
_BATCH_LATENCY = None


def _init_prometheus():
    """Initialize Prometheus metrics if prometheus_client is available."""
    global _prom_initialized, _BATCH_PROCESSED, _EVENTS_DETECTED, _BATCH_ERRORS, _BATCH_LATENCY
    if _prom_initialized:
        return
    try:
        from prometheus_client import Counter, Histogram
        _BATCH_PROCESSED = Counter(
            "pipeline_batch_processed_total",
            "Tổng records đã xử lý",
            ["group_id"],
        )
        _EVENTS_DETECTED = Counter(
            "pipeline_events_detected_total",
            "Tổng swap event phát hiện",
            ["group_id"],
        )
        _BATCH_ERRORS = Counter(
            "pipeline_batch_errors_total",
            "Tổng records lỗi",
            ["group_id"],
        )
        _BATCH_LATENCY = Histogram(
            "pipeline_batch_latency_seconds",
            "Thời gian xử lý 1 batch",
            ["group_id"],
        )
        _prom_initialized = True
    except ImportError:
        logger.debug("prometheus_client not installed — Prometheus metrics disabled")
        _prom_initialized = True  # Don't retry


class ModuleMetrics:
    def __init__(self, name: str):
        self.name = name
        self.counters: Dict[str, int] = {
            "processed": 0,
            "success": 0,
            "ignored": 0,
            "events_detected": 0,
            "notifications_sent": 0,
            "notifications_failed": 0,
            "errors": 0,
        }
        # F-08: Initialize Prometheus counters
        _init_prometheus()

    def increment(self, metric: str, amount: int = 1):
        if metric in self.counters:
            self.counters[metric] += amount

        # F-08: Also increment Prometheus counters
        if metric == "processed" and _BATCH_PROCESSED is not None:
            _BATCH_PROCESSED.labels(group_id=self.name).inc(amount)
        elif metric == "events_detected" and _EVENTS_DETECTED is not None:
            _EVENTS_DETECTED.labels(group_id=self.name).inc(amount)
        elif metric == "errors" and _BATCH_ERRORS is not None:
            _BATCH_ERRORS.labels(group_id=self.name).inc(amount)

    def get(self, metric: str) -> int:
        return self.counters.get(metric, 0)

    def log_summary(self):
        logger.info(f"[{self.name} Metrics Summary] {self.counters}")
