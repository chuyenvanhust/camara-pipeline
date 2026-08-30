# pipeline/modules/shared/metrics.py
import asyncio
import logging
import math
import os
import random
from collections import deque
from typing import Dict

logger = logging.getLogger(__name__)
SLA_E2E_P95_MS = float(os.getenv("PIPELINE_SLA_E2E_P95_MS", "100"))
E2E_WINDOW_SAMPLE_LIMIT = int(os.getenv("PIPELINE_E2E_WINDOW_SAMPLE_LIMIT", "100000"))

# F-08: Prometheus metrics — lazy init to avoid import errors if prometheus_client not installed
_prom_initialized = False
_BATCH_PROCESSED = None
_EVENTS_DETECTED = None
_BATCH_ERRORS = None
_BATCH_LATENCY = None
_STAGE_RECORDS = None
_STAGE_LATENCY = None
_KAFKA_LAG = None
_E2E_LATENCY = None
_REDIS_MGET_LATENCY = None
_POSTGRES_FALLBACK_LATENCY = None
_CACHE_HIT_RATIO = None
_DB_POOL_ACQUIRE_LATENCY = None
_PARTITION_QUEUE_RECORDS = None
_PARTITION_WORKERS = None
_PARTITIONS_PAUSED = None
_PARTITION_QUEUE_AGE = None
_OFFSET_COMMIT_LATENCY = None
_OFFSET_COMMIT_RECORDS = None
_OFFSET_COMMIT_ERRORS = None
_OFFSET_COMMIT_PENDING = None


def _init_prometheus():
    """Initialize Prometheus metrics if prometheus_client is available."""
    global _prom_initialized, _BATCH_PROCESSED, _EVENTS_DETECTED, _BATCH_ERRORS
    global _BATCH_LATENCY, _STAGE_RECORDS, _STAGE_LATENCY, _KAFKA_LAG, _E2E_LATENCY
    global _REDIS_MGET_LATENCY, _POSTGRES_FALLBACK_LATENCY, _CACHE_HIT_RATIO, _DB_POOL_ACQUIRE_LATENCY
    global _PARTITION_QUEUE_RECORDS, _PARTITION_WORKERS, _PARTITIONS_PAUSED
    global _PARTITION_QUEUE_AGE, _OFFSET_COMMIT_LATENCY, _OFFSET_COMMIT_RECORDS
    global _OFFSET_COMMIT_ERRORS, _OFFSET_COMMIT_PENDING
    if _prom_initialized:
        return
    try:
        from prometheus_client import Counter, Gauge, Histogram
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
        _STAGE_LATENCY = Histogram(
            "pipeline_stage_latency_seconds",
            "Latency of internal pipeline stages",
            ["group_id", "stage"],
        )
        _KAFKA_LAG = Gauge(
            "pipeline_kafka_lag_records",
            "Approximate records behind the Kafka high watermark",
            ["group_id", "member"],
        )
        _E2E_LATENCY = Histogram(
            "pipeline_e2e_message_lag_seconds",
            "Độ trễ từ khi gói tin vào Ingestion đến khi hoàn tất ghi DB/Redis",
            ["group_id"],
        )
        _REDIS_MGET_LATENCY = Histogram(
            "pipeline_redis_mget_latency_seconds",
            "Thời gian thực thi MGET Redis state",
            ["group_id"],
        )
        _POSTGRES_FALLBACK_LATENCY = Histogram(
            "pipeline_postgres_fallback_latency_seconds",
            "Thời gian truy vấn fallback PostgreSQL state",
            ["group_id"],
        )
        _CACHE_HIT_RATIO = Gauge(
            "pipeline_state_cache_hit_ratio",
            "Tỷ lệ cache hit Redis state lookup",
            ["group_id"],
        )
        _DB_POOL_ACQUIRE_LATENCY = Histogram(
            "pipeline_db_pool_acquire_latency_seconds",
            "Thời gian chờ lấy connection từ DatabasePool",
            ["group_id"],
        )
        _PARTITION_QUEUE_RECORDS = Gauge(
            "pipeline_partition_queue_records",
            "Records buffered in per-partition processing queues",
            ["group_id", "member"],
        )
        _PARTITION_WORKERS = Gauge(
            "pipeline_partition_workers",
            "Active single-writer partition workers",
            ["group_id", "member"],
        )
        _PARTITIONS_PAUSED = Gauge(
            "pipeline_partitions_paused",
            "Kafka partitions paused by local backpressure",
            ["group_id", "member"],
        )
        _PARTITION_QUEUE_AGE = Gauge(
            "pipeline_partition_queue_oldest_seconds",
            "Age of the oldest record waiting in a local partition queue",
            ["group_id", "member"],
        )
        _OFFSET_COMMIT_LATENCY = Histogram(
            "pipeline_offset_commit_latency_seconds",
            "Latency of a coalesced Kafka offset commit",
            ["group_id", "member"],
        )
        _OFFSET_COMMIT_RECORDS = Counter(
            "pipeline_offset_commit_records_total",
            "Records covered by successful coalesced offset commits",
            ["group_id", "member"],
        )
        _OFFSET_COMMIT_ERRORS = Counter(
            "pipeline_offset_commit_errors_total",
            "Failed Kafka offset commit requests",
            ["group_id", "member"],
        )
        _OFFSET_COMMIT_PENDING = Gauge(
            "pipeline_offset_commit_pending_records",
            "Durably processed records not yet covered by a committed Kafka offset",
            ["group_id", "member"],
        )
        _prom_initialized = True
    except ImportError:
        logger.debug("prometheus_client not installed — Prometheus metrics disabled")
        _prom_initialized = True  # Don't retry


class ModuleMetrics:
    def __init__(self, name: str):
        self.name = name
        self.member: str | None = None
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
            "cache_hits": 0,
            "cache_lookups": 0,
            "commit_requests": 0,
            "commit_records": 0,
            "commit_errors": 0,
        }
        self.processing_seconds = 0.0
        self.stage_seconds: Dict[str, float] = {}
        self.stage_calls: Dict[str, int] = {}
        self.e2e_lag_sum_ms = 0.0
        self.e2e_lag_max_ms = 0.0
        self.e2e_lag_count = 0
        self.e2e_lag_window_ms = deque(maxlen=max(1000, E2E_WINDOW_SAMPLE_LIMIT))
        self.kafka_lag = 0
        self.latest_cache_hit_ratio = 1.0
        self.partition_queue_records = 0
        self.partition_workers = 0
        self.partition_concurrency_limit = 0
        self.partitions_paused = 0
        self.partition_queue_oldest_ms = 0.0
        self.commit_pending_records = 0
        self.commit_latency_window_ms = deque(maxlen=10000)
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

    def observe_stage(self, stage: str, seconds: float) -> None:
        self.stage_seconds[stage] = self.stage_seconds.get(stage, 0.0) + seconds
        self.stage_calls[stage] = self.stage_calls.get(stage, 0) + 1
        if _STAGE_LATENCY is not None:
            _STAGE_LATENCY.labels(group_id=self.name, stage=stage).observe(seconds)

    def observe_redis_mget(self, seconds: float) -> None:
        self.observe_stage("redis_mget", seconds)
        if _REDIS_MGET_LATENCY is not None:
            _REDIS_MGET_LATENCY.labels(group_id=self.name).observe(seconds)

    def observe_postgres_fallback(self, seconds: float) -> None:
        self.observe_stage("postgres_fallback", seconds)
        if _POSTGRES_FALLBACK_LATENCY is not None:
            _POSTGRES_FALLBACK_LATENCY.labels(group_id=self.name).observe(seconds)

    def observe_db_pool_acquire(self, seconds: float) -> None:
        self.observe_stage("db_pool_acquire", seconds)
        if _DB_POOL_ACQUIRE_LATENCY is not None:
            _DB_POOL_ACQUIRE_LATENCY.labels(group_id=self.name).observe(seconds)

    def set_cache_hit_ratio(self, hits: int, total: int) -> None:
        if total > 0:
            ratio = hits / total
            self.latest_cache_hit_ratio = ratio
            self.increment("cache_hits", hits)
            self.increment("cache_lookups", total)
            if _CACHE_HIT_RATIO is not None:
                _CACHE_HIT_RATIO.labels(group_id=self.name).set(ratio)

    def observe_e2e_lag(self, lags_ms: list[float]) -> None:
        """
        Đo độ trễ bản tin từng gói tin từ lúc vào pipeline (ingest_timestamp) đến khi ghi DB/Redis.
        Tích lũy 100% sum/count/max cho log nội bộ, đồng thời sample 10% cho Prometheus Histogram
        để triệt tiêu CPU overhead tại tải cao (45.000 records/s).
        """
        if not lags_ms:
            return
        sum_ms = sum(lags_ms)
        count = len(lags_ms)
        max_ms = max(lags_ms)
        self.e2e_lag_sum_ms += sum_ms
        self.e2e_lag_count += count
        self.e2e_lag_max_ms = max(self.e2e_lag_max_ms, max_ms)
        self.e2e_lag_window_ms.extend(lags_ms)
        if _E2E_LATENCY is not None:
            lbl = _E2E_LATENCY.labels(group_id=self.name)
            # Random sampling 10% of records to prevent position-in-batch bias
            sample_size = max(1, count // 10)
            sampled = random.sample(lags_ms, min(count, sample_size))
            for lag in sampled:
                lbl.observe(lag / 1000.0)

    def set_kafka_lag(self, records: int) -> None:
        self.kafka_lag = max(0, records)
        if _KAFKA_LAG is not None:
            _KAFKA_LAG.labels(
                group_id=self.name, member=self.member or "1/1"
            ).set(self.kafka_lag)

    def set_partition_pipeline(
        self, queued_records: int, active_workers: int, paused_partitions: int,
        concurrency_limit: int, oldest_queue_ms: float = 0.0,
    ) -> None:
        self.partition_queue_records = max(0, queued_records)
        self.partition_workers = max(0, active_workers)
        self.partition_concurrency_limit = max(0, concurrency_limit)
        self.partitions_paused = max(0, paused_partitions)
        self.partition_queue_oldest_ms = max(0.0, oldest_queue_ms)
        labels = {"group_id": self.name, "member": self.member or "1/1"}
        if _PARTITION_QUEUE_RECORDS is not None:
            _PARTITION_QUEUE_RECORDS.labels(**labels).set(self.partition_queue_records)
        if _PARTITION_WORKERS is not None:
            _PARTITION_WORKERS.labels(**labels).set(self.partition_workers)
        if _PARTITIONS_PAUSED is not None:
            _PARTITIONS_PAUSED.labels(**labels).set(self.partitions_paused)
        if _PARTITION_QUEUE_AGE is not None:
            _PARTITION_QUEUE_AGE.labels(**labels).set(self.partition_queue_oldest_ms / 1000.0)

    def set_commit_pending(self, records: int) -> None:
        self.commit_pending_records = max(0, records)
        if _OFFSET_COMMIT_PENDING is not None:
            _OFFSET_COMMIT_PENDING.labels(
                group_id=self.name, member=self.member or "1/1"
            ).set(self.commit_pending_records)

    def observe_commit(
        self, seconds: float, records: int, pending: int, *, failed: bool
    ) -> None:
        labels = {"group_id": self.name, "member": self.member or "1/1"}
        self.counters["commit_requests"] += 1
        self.commit_latency_window_ms.append(max(0.0, seconds * 1000.0))
        if _OFFSET_COMMIT_LATENCY is not None:
            _OFFSET_COMMIT_LATENCY.labels(**labels).observe(max(0.0, seconds))
        if failed:
            self.counters["commit_errors"] += 1
            if _OFFSET_COMMIT_ERRORS is not None:
                _OFFSET_COMMIT_ERRORS.labels(**labels).inc()
        else:
            self.counters["commit_records"] += max(0, records)
            if _OFFSET_COMMIT_RECORDS is not None:
                _OFFSET_COMMIT_RECORDS.labels(**labels).inc(max(0, records))
        self.set_commit_pending(pending)

    def _stage_average_ms(
        self, stage: str, previous_seconds: Dict[str, float], previous_calls: Dict[str, int]
    ) -> float:
        seconds = self.stage_seconds.get(stage, 0.0) - previous_seconds.get(stage, 0.0)
        calls = self.stage_calls.get(stage, 0) - previous_calls.get(stage, 0)
        return seconds * 1000 / calls if calls else 0.0

    def get(self, metric: str) -> int:
        return self.counters.get(metric, 0)

    @staticmethod
    def _percentile(values: list[float], quantile: float) -> float:
        if not values:
            return 0.0
        ordered = sorted(values)
        index = min(len(ordered) - 1, max(0, math.ceil(len(ordered) * quantile) - 1))
        return ordered[index]

    def log_summary(self):
        event_name = self._event_name()
        data_loss = self.get("errors") + self.get("dlq")
        logger.info(
            "[PROCESSING][%s][member=%s][SUMMARY] "
            "received=%d | success=%d | ignored=%d | %s=%d | "
            "db_writes(pg=%d, rds=%d) | data_loss=%d (err=%d, dlq=%d) | batches=%d",
            self.name, self.member or "1/1",
            self.get("processed"), self.get("success"), self.get("ignored"),
            event_name, self.get("events_detected"),
            self.get("postgres_records"), self.get("redis_records"),
            data_loss, self.get("errors"), self.get("dlq"), self.get("batches"),
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
        previous_stage_seconds = dict(self.stage_seconds)
        previous_stage_calls = dict(self.stage_calls)
        previous_e2e_sum = self.e2e_lag_sum_ms
        previous_e2e_count = self.e2e_lag_count
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
            commit_requests_delta = current["commit_requests"] - previous["commit_requests"]
            commit_records_delta = current["commit_records"] - previous["commit_records"]
            commit_errors_delta = current["commit_errors"] - previous["commit_errors"]
            loss_delta = errors_delta + dlq_delta
            loss_total = current["errors"] + current["dlq"]

            e2e_count_window = self.e2e_lag_count - previous_e2e_count
            e2e_sum_window = self.e2e_lag_sum_ms - previous_e2e_sum
            e2e_avg_ms = (e2e_sum_window / e2e_count_window) if e2e_count_window else 0.0
            e2e_max_ms = self.e2e_lag_max_ms
            self.e2e_lag_max_ms = 0.0  # Reset max for next window
            e2e_window = list(self.e2e_lag_window_ms)
            self.e2e_lag_window_ms.clear()
            e2e_p95_ms = self._percentile(e2e_window, 0.95)
            commit_window = list(self.commit_latency_window_ms)
            self.commit_latency_window_ms.clear()
            commit_p95_ms = self._percentile(commit_window, 0.95)

            hits_delta = current["cache_hits"] - previous["cache_hits"]
            lookups_delta = current["cache_lookups"] - previous["cache_lookups"]
            hit_ratio_pct = (hits_delta / lookups_delta * 100.0) if lookups_delta else (self.latest_cache_hit_ratio * 100.0)

            if errors_delta or dlq_delta or commit_errors_delta:
                status = "ERROR"
                level = logging.ERROR
            elif e2e_count_window and e2e_p95_ms >= SLA_E2E_P95_MS:
                status = "SLO_BREACH"
                level = logging.WARNING
            else:
                status = "OK"
                level = logging.INFO
            event_name = self._event_name()
            logger.log(
                level,
                "[PROCESSING][%s][member=%s][%s] window=%.1fs | "
                "Throughput: recv=%.1f/s success=%.1f/s (pg=%.1f/s, rds=%.1f/s) | "
                "Latency: batch_avg=%.1frec/%.1fms stage(state=%.1fms[mget=%.1fms, pg_fb=%.1fms, hit=%.1f%%], pg=%.1fms, rds=%.1fms, persist_parallel=%.1fms, pool_acq=%.1fms) e2e_avg=%.1fms p95=%.1fms max=%.1fms slo_p95<%.0fms | "
                "Swaps/Events: %s=%d(+%d) ignored=%d | "
                "Flow: kafka_lag=%d partition_queue=%d oldest=%.1fms workers=%d concurrency_limit=%d paused=%d | "
                "OffsetCommit: pending=%d records=%.1f/s requests=%.1f/s p95=%.1fms errors=%d(+%d) | "
                "Quality/Loss: data_loss=%d(+%d) (err=%d, dlq=%d) | "
                "Totals: recv=%d, ok=%d, pg=%d, rds=%d, batches=%d",
                self.name, self.member or "1/1", status, elapsed,
                received_delta / elapsed, success_delta / elapsed,
                (current["postgres_records"] - previous["postgres_records"]) / elapsed,
                (current["redis_records"] - previous["redis_records"]) / elapsed,
                (received_delta / batch_delta) if batch_delta else 0.0,
                (seconds_delta * 1000 / batch_delta) if batch_delta else 0.0,
                self._stage_average_ms("state", previous_stage_seconds, previous_stage_calls),
                self._stage_average_ms("redis_mget", previous_stage_seconds, previous_stage_calls),
                self._stage_average_ms("postgres_fallback", previous_stage_seconds, previous_stage_calls),
                hit_ratio_pct,
                self._stage_average_ms("postgres", previous_stage_seconds, previous_stage_calls),
                self._stage_average_ms("redis", previous_stage_seconds, previous_stage_calls),
                self._stage_average_ms("persistence", previous_stage_seconds, previous_stage_calls),
                self._stage_average_ms("db_pool_acquire", previous_stage_seconds, previous_stage_calls),
                e2e_avg_ms, e2e_p95_ms, e2e_max_ms, SLA_E2E_P95_MS,
                event_name, current["events_detected"], events_delta, current["ignored"],
                self.kafka_lag, self.partition_queue_records, self.partition_queue_oldest_ms,
                self.partition_workers, self.partition_concurrency_limit, self.partitions_paused,
                self.commit_pending_records, commit_records_delta / elapsed,
                commit_requests_delta / elapsed, commit_p95_ms,
                current["commit_errors"], commit_errors_delta,
                loss_total, loss_delta, current["errors"], current["dlq"],
                current["processed"], current["success"], current["postgres_records"],
                current["redis_records"], current["batches"],
            )
            previous = current
            previous_seconds = self.processing_seconds
            previous_stage_seconds = dict(self.stage_seconds)
            previous_stage_calls = dict(self.stage_calls)
            previous_e2e_sum = self.e2e_lag_sum_ms
            previous_e2e_count = self.e2e_lag_count
            previous_log_at = now
