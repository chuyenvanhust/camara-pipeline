# pipeline/modules/shared/metrics.py
import asyncio
import logging
from typing import Dict

logger = logging.getLogger(__name__)

# F-08: Prometheus metrics — lazy init to avoid import errors if prometheus_client not installed
_prom_initialized = False
_BATCH_PROCESSED = None
_EVENTS_DETECTED = None
_BATCH_ERRORS = None
_BATCH_LATENCY = None
_STAGE_RECORDS = None


def _init_prometheus():
    """Initialize Prometheus metrics if prometheus_client is available."""
    global _prom_initialized, _BATCH_PROCESSED, _EVENTS_DETECTED, _BATCH_ERRORS, _BATCH_LATENCY, _STAGE_RECORDS
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
        _STAGE_RECORDS = Counter(
            "pipeline_stage_records_total",
            "Records completed at each pipeline stage",
            ["group_id", "stage"],
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
            "dlq": 0,
            "batches": 0,
            "postgres_records": 0,
            "redis_records": 0,
        }
        self.processing_seconds = 0.0
        # F-08: Initialize Prometheus counters
        _init_prometheus()
        if _STAGE_RECORDS is not None:
            for stage in ("processed", "success", "dlq", "postgres_records", "redis_records"):
                _STAGE_RECORDS.labels(group_id=self.name, stage=stage).inc(0)

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
        if metric in {"processed", "success", "dlq", "postgres_records", "redis_records"} and _STAGE_RECORDS is not None:
            _STAGE_RECORDS.labels(group_id=self.name, stage=metric).inc(amount)

    def observe_batch(self, seconds: float) -> None:
        self.processing_seconds += seconds
        self.increment("batches")
        if _BATCH_LATENCY is not None:
            _BATCH_LATENCY.labels(group_id=self.name).observe(seconds)

    def get(self, metric: str) -> int:
        return self.counters.get(metric, 0)

    def log_summary(self):
        logger.info(f"[{self.name} Metrics Summary] {self.counters}")

    async def log_periodically(self, interval: float) -> None:
        previous = dict(self.counters)
        previous_seconds = self.processing_seconds
        while True:
            await asyncio.sleep(interval)
            current = dict(self.counters)
            batch_delta = current["batches"] - previous["batches"]
            seconds_delta = self.processing_seconds - previous_seconds
            logger.info(
                "stage=processing group=%s window=%.1fs "
                "kafka_received_total=%d kafka_rate=%.1f_rec_s "
                "success_total=%d success_rate=%.1f_rec_s ignored_total=%d "
                "dlq_total=%d errors_total=%d events_total=%d batches_total=%d avg_batch_ms=%.1f",
                self.name, interval,
                current["processed"], (current["processed"] - previous["processed"]) / interval,
                current["success"], (current["success"] - previous["success"]) / interval,
                current["ignored"], current["dlq"], current["errors"], current["events_detected"],
                current["batches"], (seconds_delta * 1000 / batch_delta) if batch_delta else 0.0,
            )
            logger.info(
                "stage=postgresql group=%s window=%.1fs records_total=%d write_rate=%.1f_rec_s",
                self.name, interval, current["postgres_records"],
                (current["postgres_records"] - previous["postgres_records"]) / interval,
            )
            logger.info(
                "stage=redis group=%s window=%.1fs mutations_total=%d write_rate=%.1f_rec_s",
                self.name, interval, current["redis_records"],
                (current["redis_records"] - previous["redis_records"]) / interval,
            )
            previous = current
            previous_seconds = self.processing_seconds
