from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from aiokafka import AIOKafkaProducer

from pipeline.ingestion.csv_reader import LocalCSVReader
from pipeline.ingestion.packet_reader import PacketReader
from pipeline.modules.shared.events import InvalidMessageError, canonical_msisdn, parse_event_time

logger = logging.getLogger(__name__)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "radius.accounting.raw")
FLUSH_EVERY_N_RECORDS = int(os.getenv("INGESTION_FLUSH_EVERY_N_RECORDS", "1000"))
THROUGHPUT_LOG_INTERVAL_SECONDS = float(os.getenv("THROUGHPUT_LOG_INTERVAL_SECONDS", "10"))
UDP_QUEUE_MAX_RECORDS = int(os.getenv("RADIUS_UDP_QUEUE_MAX_RECORDS", "300000"))
UDP_KAFKA_BATCH_RECORDS = int(os.getenv("RADIUS_UDP_KAFKA_BATCH_RECORDS", "500"))
UDP_KAFKA_BATCH_WAIT_MS = int(os.getenv("RADIUS_UDP_KAFKA_BATCH_WAIT_MS", "5"))
UDP_KAFKA_MAX_INFLIGHT_BATCHES_PER_WORKER = int(
    os.getenv("RADIUS_UDP_KAFKA_MAX_INFLIGHT_BATCHES_PER_WORKER",
              os.getenv("RADIUS_UDP_KAFKA_MAX_INFLIGHT_BATCHES", "4"))
)
UDP_KAFKA_PRESSURE_INFLIGHT_BATCHES_PER_WORKER = int(
    os.getenv("RADIUS_UDP_KAFKA_PRESSURE_INFLIGHT_BATCHES_PER_WORKER", "6")
)
UDP_KAFKA_PRESSURE_QUEUE_RATIO = float(
    os.getenv("RADIUS_UDP_KAFKA_PRESSURE_QUEUE_RATIO", "0.5")
)
UDP_KAFKA_PRODUCERS = int(os.getenv("RADIUS_UDP_KAFKA_PRODUCERS", "4"))
INGESTION_METRICS_PORT = int(os.getenv("INGESTION_METRICS_PORT", "9201"))
INGESTION_KAFKA_PERSIST_WARN_MS = float(os.getenv("INGESTION_KAFKA_PERSIST_WARN_MS", "500"))
INGESTION_QUEUE_WARN_MS = float(os.getenv("INGESTION_QUEUE_WARN_MS", "1000"))


_PROM_INGESTION_INIT = False
_PROM_UDP_RECV = None
_PROM_KAFKA_PERSISTED = None
_PROM_QUEUE_DEPTH = None
_PROM_QUEUE_CAPACITY = None
_PROM_INVALID = None
_PROM_DLQ_PUBLISHED = None
_PROM_PUBLISH_FAILED = None
_PROM_QUEUE_DROPPED = None
_PROM_WORKER_QUEUE_DEPTH = None
_PROM_KAFKA_BATCH_LATENCY = None
_PROM_QUEUE_RESIDENCE = None
_PROM_INFLIGHT_WAIT = None
_PROM_WORKER_SLOT_WAIT = None


def _start_ingestion_metrics_server():
    """Start Prometheus metrics exporter server on INGESTION_METRICS_PORT."""
    global _PROM_INGESTION_INIT, _PROM_UDP_RECV, _PROM_KAFKA_PERSISTED, _PROM_QUEUE_DEPTH, _PROM_QUEUE_CAPACITY
    global _PROM_INVALID, _PROM_DLQ_PUBLISHED, _PROM_PUBLISH_FAILED, _PROM_QUEUE_DROPPED, _PROM_WORKER_QUEUE_DEPTH
    global _PROM_KAFKA_BATCH_LATENCY, _PROM_QUEUE_RESIDENCE, _PROM_INFLIGHT_WAIT, _PROM_WORKER_SLOT_WAIT
    if _PROM_INGESTION_INIT:
        return
    try:
        from prometheus_client import Counter, Gauge, Histogram, start_http_server
        _PROM_UDP_RECV = Counter("radius_ingestion_udp_received_total", "Total UDP packets received")
        _PROM_KAFKA_PERSISTED = Counter("radius_ingestion_kafka_persisted_total", "Total records persisted by Kafka")
        _PROM_QUEUE_DEPTH = Gauge("radius_ingestion_queue_depth_records", "Current depth of RAM queue")
        _PROM_QUEUE_CAPACITY = Gauge("radius_ingestion_queue_capacity_records", "Configured total capacity of RAM queues")
        _PROM_QUEUE_CAPACITY.set(UDP_QUEUE_MAX_RECORDS)
        _PROM_WORKER_QUEUE_DEPTH = Gauge("radius_ingestion_worker_queue_depth_records", "RAM queue depth per worker", ["worker"])
        _PROM_INVALID = Counter("radius_ingestion_invalid_total", "Records failed validation (sent to DLQ)")
        _PROM_DLQ_PUBLISHED = Counter("radius_ingestion_dlq_published_total", "Records successfully published to DLQ topic")
        _PROM_PUBLISH_FAILED = Counter("radius_ingestion_publish_failed_total", "Mirror records not persisted because Kafka publish failed")
        _PROM_QUEUE_DROPPED = Counter("radius_ingestion_queue_dropped_total", "Mirror records dropped because the RAM queue is full")
        _PROM_KAFKA_BATCH_LATENCY = Histogram(
            "radius_ingestion_kafka_batch_persist_seconds",
            "Kafka batch persistence latency in seconds",
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
        )
        _PROM_QUEUE_RESIDENCE = Histogram(
            "radius_ingestion_queue_residence_seconds",
            "Time from RAM queue admission until Kafka publish starts",
            buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
        )
        _PROM_INFLIGHT_WAIT = Histogram(
            "radius_ingestion_inflight_semaphore_wait_seconds",
            "Time waiting for the global Kafka in-flight batch limit",
            buckets=(0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
        )
        _PROM_WORKER_SLOT_WAIT = Histogram(
            "radius_ingestion_worker_slot_wait_seconds",
            "Time a publisher waits for one of its Kafka in-flight batch slots",
            ("worker",),
            buckets=(0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
        )
        for port in range(INGESTION_METRICS_PORT, INGESTION_METRICS_PORT + 10):
            try:
                start_http_server(port)
                _PROM_INGESTION_INIT = True
                if port != INGESTION_METRICS_PORT:
                    logger.warning("WARNING: Requested INGESTION_METRICS_PORT %d in use, bound to fallback port %d", INGESTION_METRICS_PORT, port)
                else:
                    logger.info("Ingestion Prometheus metrics exporter listening on port %d", port)
                break
            except OSError:
                continue
        _PROM_INGESTION_INIT = True
    except Exception as exc:
        logger.debug("Ingestion Prometheus metrics server not started: %s", exc)
        _PROM_INGESTION_INIT = True


@dataclass(frozen=True)
class QueueItem:
    topic: str
    key: Optional[str]
    value: Dict[str, Any]
    kind: str
    queued_at: float = field(default_factory=time.monotonic, compare=False)


class RadiusLogProducer:
    def __init__(self, bootstrap_servers: str | None = None, topic: str | None = None):
        if min(
            UDP_QUEUE_MAX_RECORDS, UDP_KAFKA_BATCH_RECORDS,
            UDP_KAFKA_MAX_INFLIGHT_BATCHES_PER_WORKER,
            UDP_KAFKA_PRESSURE_INFLIGHT_BATCHES_PER_WORKER,
            UDP_KAFKA_PRODUCERS,
        ) < 1:
            raise ValueError("RADIUS UDP queue and batch sizes must be positive")
        if not 0 < UDP_KAFKA_PRESSURE_QUEUE_RATIO < 1:
            raise ValueError("RADIUS_UDP_KAFKA_PRESSURE_QUEUE_RATIO must be between 0 and 1")
        if (
            UDP_KAFKA_PRESSURE_INFLIGHT_BATCHES_PER_WORKER
            < UDP_KAFKA_MAX_INFLIGHT_BATCHES_PER_WORKER
        ):
            raise ValueError("pressure inflight limit must be >= the normal per-worker limit")
        self.bootstrap_servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self.topic = topic or KAFKA_TOPIC_RAW
        self._producer: AIOKafkaProducer | None = None
        self._producers: list[AIOKafkaProducer] = []
        self._telemetry_task: asyncio.Task | None = None
        self._queue: asyncio.Queue[QueueItem] | None = None
        self._packet_reader: PacketReader | None = None
        self._source = "not-started"
        self._counts = {
            "received": 0, "queued": 0, "persisted": 0, "rejected": 0,
            "dlq": 0, "queue_dropped": 0, "publish_failed": 0,
            "kafka_batches": 0,
            "last_batch_records": 0, "last_batch_ms": 0.0,
            "last_batch_rate": 0.0,
        }
        self._worker_queues: list[asyncio.Queue[QueueItem]] = []
        self._global_inflight_semaphore: asyncio.Semaphore | None = None
        self._rr_counter: int = 0
        self._kafka_persist_ms: deque[float] = deque(maxlen=4096)
        self._queue_residence_ms: deque[float] = deque(maxlen=4096)
        self._inflight_wait_ms: deque[float] = deque(maxlen=4096)
        self._worker_slot_wait_ms: deque[float] = deque(maxlen=4096)
        self._batch_sizes: deque[int] = deque(maxlen=4096)

    @staticmethod
    def _percentile(values, quantile: float) -> float:
        ordered = sorted(values)
        if not ordered:
            return 0.0
        return ordered[min(int(len(ordered) * quantile), len(ordered) - 1)]

    def _build_kafka_producer(self) -> AIOKafkaProducer:
        compression = os.getenv("INGESTION_COMPRESSION_TYPE", "lz4").strip().lower()
        return AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
            key_serializer=lambda value: value.encode("utf-8") if value else None,
            max_batch_size=int(os.getenv("INGESTION_BATCH_SIZE_BYTES", str(256 * 1024))),
            linger_ms=int(os.getenv("INGESTION_LINGER_MS", "5")),
            compression_type=None if compression in {"", "none", "null"} else compression,
            max_request_size=int(os.getenv("INGESTION_MAX_REQUEST_SIZE", str(1024 * 1024))),
            acks="all", enable_idempotence=True, retry_backoff_ms=500,
        )

    async def start(self, producer_count: int = 1) -> None:
        if self._producers:
            if len(self._producers) != producer_count:
                raise RuntimeError(
                    f"Kafka producer pool already started with {len(self._producers)} producers; "
                    f"requested {producer_count}"
                )
            return
        started: list[AIOKafkaProducer] = []
        try:
            for _ in range(producer_count):
                producer = self._build_kafka_producer()
                await producer.start()
                started.append(producer)
        except Exception:
            await asyncio.gather(*(producer.stop() for producer in started), return_exceptions=True)
            raise
        self._producers = started
        self._producer = started[0]
        self._telemetry_task = asyncio.create_task(self._log_throughput(), name="producer-telemetry")
        logger.info(
            "Kafka producer pool ready producers=%d acks=all idempotence=true",
            len(self._producers),
        )

    async def stop(self) -> None:
        if self._telemetry_task is not None:
            self._telemetry_task.cancel()
            await asyncio.gather(self._telemetry_task, return_exceptions=True)
            self._telemetry_task = None
        if self._producers:
            await asyncio.gather(
                *(producer.stop() for producer in self._producers),
                return_exceptions=True,
            )
            self._producers.clear()
        self._producer = None
        logger.info(
            "[INGESTION][FINAL] source=%s received=%d kafka_persisted=%d queue_dropped=%d "
            "rejected=%d dlq=%d publish_failed=%d",
            self._source, self._counts["received"], self._counts["persisted"],
            self._counts["queue_dropped"], self._counts["rejected"],
            self._counts["dlq"], self._counts["publish_failed"],
        )

    async def _log_throughput(self) -> None:
        previous = dict(self._counts)
        interval = THROUGHPUT_LOG_INTERVAL_SECONDS
        loop = asyncio.get_running_loop()
        previous_log_at = loop.time()
        while True:
            await asyncio.sleep(interval)
            now = loop.time()
            elapsed = max(now - previous_log_at, 1e-9)
            current = dict(self._counts)
            queue_depth = sum(q.qsize() for q in self._worker_queues) if self._worker_queues else (self._queue.qsize() if self._queue is not None else 0)
            reader_stats = self._packet_reader.stats if self._packet_reader is not None else {}
            input_rate = (current["received"] - previous["received"]) / elapsed
            kafka_rate = (current["persisted"] - previous["persisted"]) / elapsed
            throughput_gap = input_rate - kafka_rate
            dropped_window = current["queue_dropped"] - previous["queue_dropped"]
            queue_percent = queue_depth * 100 / UDP_QUEUE_MAX_RECORDS
            status = "OVERLOAD" if dropped_window else "PRESSURE" if queue_percent >= 70 else "OK"
            data_loss = current["queue_dropped"] + current["publish_failed"]
            data_loss_delta = (current["queue_dropped"] - previous["queue_dropped"]) + (current["publish_failed"] - previous["publish_failed"])
            kafka_p50 = self._percentile(self._kafka_persist_ms, 0.50)
            kafka_p95 = self._percentile(self._kafka_persist_ms, 0.95)
            kafka_p99 = self._percentile(self._kafka_persist_ms, 0.99)
            queue_p95 = self._percentile(self._queue_residence_ms, 0.95)
            inflight_p95 = self._percentile(self._inflight_wait_ms, 0.95)
            worker_slot_p95 = self._percentile(self._worker_slot_wait_ms, 0.95)
            batch_avg = sum(self._batch_sizes) / len(self._batch_sizes) if self._batch_sizes else 0.0
            backlog_seconds = queue_depth / max(kafka_rate, 1.0)
            if status == "OK" and (
                kafka_p95 > INGESTION_KAFKA_PERSIST_WARN_MS
                or queue_p95 > INGESTION_QUEUE_WARN_MS
            ):
                status = "DEGRADED"
            level = logging.WARNING if status != "OK" else logging.INFO
            if _PROM_QUEUE_DEPTH is not None:
                _PROM_QUEUE_DEPTH.set(queue_depth)
            if _PROM_WORKER_QUEUE_DEPTH is not None and self._worker_queues:
                for i, q in enumerate(self._worker_queues):
                    _PROM_WORKER_QUEUE_DEPTH.labels(worker=str(i + 1)).set(q.qsize())
            logger.log(
                level,
                "[INGESTION][%s] window=%.1fs | "
                "Throughput: udp_in=%.1f/s kafka_persisted=%.1f/s gap=%+.1f/s | "
                "Queue: depth=%d/%d(%.1f%%) backlog=%.2fs | "
                "Kafka: batch_avg=%.1frec last=%drec/%.1fms persist(p50=%.1fms p95=%.1fms p99=%.1fms) "
                "queue_p95=%.1fms worker_slot_wait_p95=%.1fms global_wait_p95=%.1fms | "
                "Quality/Loss: data_loss=%d(+%d) (queue_dropped=%d, pub_failed=%d, dlq=%d, invalid=%d) | "
                "Totals: received=%d, kafka_persisted=%d",
                status, elapsed,
                input_rate, kafka_rate, throughput_gap,
                queue_depth, UDP_QUEUE_MAX_RECORDS, queue_percent, backlog_seconds,
                batch_avg, current["last_batch_records"], current["last_batch_ms"],
                kafka_p50, kafka_p95, kafka_p99, queue_p95, worker_slot_p95, inflight_p95,
                data_loss, data_loss_delta, current["queue_dropped"],
                current["publish_failed"], current["dlq"], reader_stats.get("rejected", 0),
                reader_stats.get("datagrams", current["received"]), current["persisted"],
            )
            # Text telemetry is a true interval window. Prometheus histograms
            # remain cumulative and are queried with rate()/histogram_quantile().
            self._kafka_persist_ms.clear()
            self._queue_residence_ms.clear()
            self._inflight_wait_ms.clear()
            self._worker_slot_wait_ms.clear()
            self._batch_sizes.clear()
            previous = current
            previous_log_at = now

    @staticmethod
    def _normalize(record: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
        msisdn = canonical_msisdn(record)
        occurred_at = parse_event_time(record)
        normalized = dict(record)
        normalized["msisdn"] = msisdn
        normalized["event_timestamp"] = occurred_at.isoformat()
        now_time = time.time()
        if "ingest_epoch_s" not in normalized:
            normalized["ingest_epoch_s"] = now_time
        if "ingest_timestamp" not in normalized:
            normalized["ingest_timestamp"] = datetime.fromtimestamp(now_time, timezone.utc).isoformat()
        return msisdn, normalized

    @staticmethod
    def _dlq_envelope(payload: Dict[str, Any], error: Exception, source: str) -> Dict[str, Any]:
        return {"producer": "radius-ingestion", "source": source,
                "error_type": type(error).__name__, "error": str(error), "payload": payload}

    async def _dlq(self, payload: Dict[str, Any], error: Exception, source: str) -> None:
        assert self._producer is not None
        await self._producer.send_and_wait(
            f"{self.topic}.dlq", value=self._dlq_envelope(payload, error, source)
        )
        self._counts["dlq"] += 1

    async def publish_csv(self, file_path: str) -> int:
        if self._producer is None:
            await self.start()
        assert self._producer is not None
        self._source = f"csv:{file_path}"
        persisted = rejected = 0
        pending: List[asyncio.Future] = []
        started = time.monotonic()
        for record in LocalCSVReader(file_path).read_records():
            self._counts["received"] += 1
            try:
                key, normalized = self._normalize(record)
            except InvalidMessageError as exc:
                await self._dlq(record, exc, file_path)
                rejected += 1
                self._counts["rejected"] += 1
                continue
            pending.append(await self._producer.send(self.topic, key=key, value=normalized))
            self._counts["queued"] += 1
            if len(pending) >= FLUSH_EVERY_N_RECORDS:
                await asyncio.gather(*pending)
                persisted += len(pending)
                self._counts["persisted"] += len(pending)
                self._counts["kafka_batches"] += 1
                pending.clear()
        if pending:
            await asyncio.gather(*pending)
            persisted += len(pending)
            self._counts["persisted"] += len(pending)
            self._counts["kafka_batches"] += 1
        await self._producer.flush()
        logger.info("CSV ingestion persisted=%d rejected=%d duration=%.2fs",
                    persisted, rejected, time.monotonic() - started)
        return persisted

    def _put_udp_item(self, item: QueueItem) -> bool:
        assert self._worker_queues
        if item.key:
            target_idx = hash(item.key) % len(self._worker_queues)
        else:
            self._rr_counter += 1
            target_idx = self._rr_counter % len(self._worker_queues)

        target_queue = self._worker_queues[target_idx]
        try:
            target_queue.put_nowait(item)
            self._counts["queued"] += 1
            return True
        except asyncio.QueueFull:
            self._counts["queue_dropped"] += 1
            if _PROM_QUEUE_DROPPED is not None:
                _PROM_QUEUE_DROPPED.inc()
            return False

    async def _receive_udp(self, reader: PacketReader, port: int) -> None:
        source = f"udp:{port}"
        async for record in reader.listen_radius_packets(port):
            self._counts["received"] += 1
            if _PROM_UDP_RECV is not None:
                _PROM_UDP_RECV.inc()
            try:
                key, normalized = self._normalize(record)
                self._put_udp_item(QueueItem(self.topic, key, normalized, "raw"))
            except InvalidMessageError as exc:
                self._counts["rejected"] += 1
                if _PROM_INVALID is not None:
                    _PROM_INVALID.inc()
                dlq_payload = self._dlq_envelope(record, exc, source)
                self._put_udp_item(
                    QueueItem(f"{self.topic}.dlq", None, dlq_payload, "dlq")
                )

    async def _next_kafka_batch(self, worker_queue: asyncio.Queue[QueueItem]) -> List[QueueItem]:
        batch = [await worker_queue.get()]
        deadline = asyncio.get_running_loop().time() + UDP_KAFKA_BATCH_WAIT_MS / 1000
        while len(batch) < UDP_KAFKA_BATCH_RECORDS:
            try:
                batch.append(worker_queue.get_nowait())
                continue
            except asyncio.QueueEmpty:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                try:
                    batch.append(await asyncio.wait_for(worker_queue.get(), timeout=remaining))
                except asyncio.TimeoutError:
                    break
        return batch

    async def _publish_udp_batches(
        self,
        worker_queue: asyncio.Queue[QueueItem],
        worker_id: int,
        producer: AIOKafkaProducer,
    ) -> None:
        inflight: set[asyncio.Task] = set()

        async def record_persistence(batch: List[QueueItem], futures: List[asyncio.Future], started: float) -> None:
            try:
                await asyncio.gather(*futures)
                raw_count = sum(item.kind == "raw" for item in batch)
                dlq_count = len(batch) - raw_count
                self._counts["persisted"] += raw_count
                if _PROM_KAFKA_PERSISTED is not None and raw_count > 0:
                    _PROM_KAFKA_PERSISTED.inc(raw_count)
                if _PROM_DLQ_PUBLISHED is not None and dlq_count > 0:
                    _PROM_DLQ_PUBLISHED.inc(dlq_count)
                self._counts["dlq"] += dlq_count
                self._counts["kafka_batches"] += 1
                elapsed = max(time.monotonic() - started, 1e-9)
                elapsed_ms = elapsed * 1000
                self._counts["last_batch_records"] = len(batch)
                self._counts["last_batch_ms"] = elapsed_ms
                self._counts["last_batch_rate"] = len(batch) / elapsed
                self._kafka_persist_ms.append(elapsed_ms)
                self._batch_sizes.append(len(batch))
                if _PROM_KAFKA_BATCH_LATENCY is not None:
                    _PROM_KAFKA_BATCH_LATENCY.observe(elapsed)
            except asyncio.CancelledError:
                raise
            except Exception:
                self._counts["publish_failed"] += len(batch)
                if _PROM_PUBLISH_FAILED is not None:
                    _PROM_PUBLISH_FAILED.inc(len(batch))
                logger.exception("Kafka UDP batch publish failed records=%d", len(batch))
                raise
            finally:
                for _ in batch:
                    worker_queue.task_done()
                if self._global_inflight_semaphore:
                    self._global_inflight_semaphore.release()

        try:
            while True:
                under_pressure = (
                    worker_queue.qsize()
                    >= worker_queue.maxsize * UDP_KAFKA_PRESSURE_QUEUE_RATIO
                )
                worker_inflight_limit = (
                    UDP_KAFKA_PRESSURE_INFLIGHT_BATCHES_PER_WORKER
                    if under_pressure
                    else UDP_KAFKA_MAX_INFLIGHT_BATCHES_PER_WORKER
                )
                if len(inflight) >= worker_inflight_limit:
                    slot_wait_started = time.monotonic()
                    done, inflight = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
                    slot_wait = time.monotonic() - slot_wait_started
                    self._worker_slot_wait_ms.append(slot_wait * 1000)
                    if _PROM_WORKER_SLOT_WAIT is not None:
                        _PROM_WORKER_SLOT_WAIT.labels(worker=str(worker_id)).observe(slot_wait)
                    for task in done:
                        task.result()

                batch = await self._next_kafka_batch(worker_queue)
                if self._global_inflight_semaphore:
                    wait_started = time.monotonic()
                    await self._global_inflight_semaphore.acquire()
                    inflight_wait = time.monotonic() - wait_started
                    self._inflight_wait_ms.append(inflight_wait * 1000)
                    if _PROM_INFLIGHT_WAIT is not None:
                        _PROM_INFLIGHT_WAIT.observe(inflight_wait)

                started = time.monotonic()
                # Cap telemetry work to 16 representative records per batch;
                # observing a Histogram for every packet would itself become
                # noticeable at 15k+ pkt/s.
                sample_step = max(1, len(batch) // 16)
                for item in batch[::sample_step]:
                    residence = max(0.0, started - item.queued_at)
                    self._queue_residence_ms.append(residence * 1000)
                    if _PROM_QUEUE_RESIDENCE is not None:
                        _PROM_QUEUE_RESIDENCE.observe(residence)
                try:
                    futures = [
                        await producer.send(item.topic, key=item.key, value=item.value)
                        for item in batch
                    ]
                except Exception:
                    self._counts["publish_failed"] += len(batch)
                    if _PROM_PUBLISH_FAILED is not None:
                        _PROM_PUBLISH_FAILED.inc(len(batch))
                    for _ in batch:
                        worker_queue.task_done()
                    if self._global_inflight_semaphore:
                        self._global_inflight_semaphore.release()
                    logger.exception("Kafka UDP batch enqueue failed records=%d", len(batch))
                    raise
                task = asyncio.create_task(
                    record_persistence(batch, futures, started),
                    name=f"kafka-persist-{self._counts['kafka_batches'] + len(inflight) + 1}",
                )
                inflight.add(task)
        finally:
            if inflight:
                await asyncio.gather(*inflight, return_exceptions=True)

    async def publish_packets(self, port: int = 1813,
                              stop_event: asyncio.Event | None = None) -> None:
        _start_ingestion_metrics_server()
        publisher_workers = max(1, int(os.getenv("RADIUS_UDP_PUBLISHER_WORKERS", "4")))
        producer_count = min(UDP_KAFKA_PRODUCERS, publisher_workers)
        if self._producer is None:
            await self.start(producer_count=producer_count)
        if len(self._producers) != producer_count:
            raise RuntimeError(
                f"UDP ingestion requires {producer_count} Kafka producers, "
                f"but {len(self._producers)} are active"
            )
        self._source = f"udp:{port}"
        per_worker_qsize = max(1000, UDP_QUEUE_MAX_RECORDS // publisher_workers)
        self._worker_queues = [asyncio.Queue(maxsize=per_worker_qsize) for _ in range(publisher_workers)]
        self._queue = self._worker_queues[0]  # backward compatibility alias

        total_inflight_limit = int(
            os.getenv("RADIUS_UDP_KAFKA_TOTAL_MAX_INFLIGHT_BATCHES",
                      str(publisher_workers * UDP_KAFKA_PRESSURE_INFLIGHT_BATCHES_PER_WORKER))
        )
        self._global_inflight_semaphore = asyncio.Semaphore(total_inflight_limit)

        self._packet_reader = PacketReader()
        receiver = asyncio.create_task(self._receive_udp(self._packet_reader, port), name="udp-receiver")
        publishers = [
            asyncio.create_task(
                self._publish_udp_batches(
                    self._worker_queues[i], i + 1, self._producers[i % producer_count]
                ),
                name=f"kafka-batch-publisher-{i+1}",
            )
            for i in range(publisher_workers)
        ]
        shutdown = asyncio.create_task(stop_event.wait(), name="ingestion-shutdown") if stop_event else None
        total_inflight_capacity = total_inflight_limit * UDP_KAFKA_BATCH_RECORDS
        logger.info(
            "UDP ingestion pipeline ready queue_capacity=%d kafka_batch_records=%d "
            "kafka_batch_wait_ms=%d kafka_inflight_per_worker=%d pressure_inflight_per_worker=%d "
            "pressure_queue_ratio=%.2f publisher_workers=%d "
            "kafka_producers=%d total_inflight_limit=%d "
            "total_inflight_capacity=%d mode=passive-mirror",
            UDP_QUEUE_MAX_RECORDS, UDP_KAFKA_BATCH_RECORDS, UDP_KAFKA_BATCH_WAIT_MS,
            UDP_KAFKA_MAX_INFLIGHT_BATCHES_PER_WORKER,
            UDP_KAFKA_PRESSURE_INFLIGHT_BATCHES_PER_WORKER,
            UDP_KAFKA_PRESSURE_QUEUE_RATIO, publisher_workers, producer_count,
            total_inflight_limit, total_inflight_capacity,
        )
        try:
            watched = (receiver, *publishers, shutdown) if shutdown else (receiver, *publishers)
            done, _ = await asyncio.wait(watched, return_when=asyncio.FIRST_COMPLETED)
            if shutdown is not None and shutdown in done:
                receiver.cancel()
                await asyncio.gather(receiver, return_exceptions=True)
                total_q = sum(q.qsize() for q in self._worker_queues)
                logger.info("UDP receiver stopped; draining total_queue_depth=%d", total_q)
                for q in self._worker_queues:
                    await asyncio.wait_for(q.join(), timeout=20)
                return
            task = next(iter(done))
            raise task.exception() or RuntimeError(f"critical ingestion task {task.get_name()} exited")
        finally:
            receiver.cancel()
            for p in publishers:
                p.cancel()
            if shutdown is not None:
                shutdown.cancel()
                await asyncio.gather(receiver, *publishers, shutdown, return_exceptions=True)
            else:
                await asyncio.gather(receiver, *publishers, return_exceptions=True)


async def _run(args: argparse.Namespace) -> None:
    producer = RadiusLogProducer()
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass
    try:
        if args.file:
            count = await producer.publish_csv(args.file)
            print(f"Persisted {count} CSV records")
        else:
            await producer.publish_packets(args.port, stop_event)
    finally:
        await producer.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="RADIUS CSV/UDP to Kafka ingestion")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--file")
    source.add_argument("--udp", action="store_true")
    parser.add_argument("--port", type=int, default=1813)
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    main()
