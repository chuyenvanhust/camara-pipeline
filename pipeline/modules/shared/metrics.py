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
        event_name = self._event_name()
        logger.info(
            "[PROCESSING][%s][FINAL] received=%d success=%d ignored=%d %s=%d "
            "postgres=%d redis=%d errors=%d dlq=%d batches=%d",
            self.name, self.get("processed"), self.get("success"), self.get("ignored"),
            event_name, self.get("events_detected"), self.get("postgres_records"),
            self.get("redis_records"), self.get("errors"), self.get("dlq"), self.get("batches"),
        )

    def _event_name(self) -> str:
        if self.name == "cg-device-swap":
            return "device_swaps_total"
        if self.name == "cg-sim-swap":
            return "sim_swaps_total"
        return "mapping_events_total"

    async def log_periodically(self, interval: float) -> None:
        previous = dict(self.counters)
        previous_seconds = self.processing_seconds
        loop = asyncio.get_running_loop()
        previous_log_at = loop.time()
        while True:
            await asyncio.sleep(interval)
            now = loop.time()
            elapsed = max(now - previous_log_at, 1e-9)
            current = dict(self.counters)
            batch_delta = current["batches"] - previous["batches"]
            seconds_delta = self.processing_seconds - previous_seconds
            received_delta = current["processed"] - previous["processed"]
            success_delta = current["success"] - previous["success"]
            events_delta = current["events_detected"] - previous["events_detected"]
            errors_delta = current["errors"] - previous["errors"]
            dlq_delta = current["dlq"] - previous["dlq"]
            status = "ERROR" if errors_delta or dlq_delta else "OK"
            level = logging.ERROR if status == "ERROR" else logging.INFO
            event_name = self._event_name()
            logger.log(
                level,
                "[PROCESSING][%s][%s] window=%.1fs kafka=%.1f/s success=%.1f/s "
                "postgres=%.1f/s redis=%.1f/s batch_avg=%.1fms "
                "%s=%d(+%d) ignored=%d errors=%d(+%d) dlq=%d(+%d) "
                "totals(received=%d,success=%d,postgres=%d,redis=%d,batches=%d)",
                self.name, status, elapsed,
                received_delta / elapsed, success_delta / elapsed,
                (current["postgres_records"] - previous["postgres_records"]) / elapsed,
                (current["redis_records"] - previous["redis_records"]) / elapsed,
                (seconds_delta * 1000 / batch_delta) if batch_delta else 0.0,
                event_name, current["events_detected"], events_delta,
                current["ignored"], current["errors"], errors_delta,
                current["dlq"], dlq_delta, current["processed"], current["success"],
                current["postgres_records"], current["redis_records"], current["batches"],
            )
            previous = current
            previous_seconds = self.processing_seconds
            previous_log_at = now
