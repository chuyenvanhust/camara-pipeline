from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, List, Optional

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRebalanceListener, TopicPartition
from aiokafka.errors import CommitFailedError

from pipeline.modules.shared.db import DatabasePool
from pipeline.modules.shared.metrics import ModuleMetrics
from pipeline.modules.shared.redis_client import create_redis_client


logger = logging.getLogger(__name__)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")
BATCH_MAX_RECORDS = int(os.getenv("BATCH_MAX_RECORDS", "24"))
BATCH_TIMEOUT_MS = int(os.getenv("BATCH_TIMEOUT_MS", "1"))
MAX_BATCH_RETRIES = int(os.getenv("MAX_BATCH_RETRIES", "3"))
PARTITION_CONCURRENCY = int(os.getenv("PROCESSING_PARTITION_CONCURRENCY", "4"))
FETCH_MAX_RECORDS = int(os.getenv(
    "PROCESSING_FETCH_MAX_RECORDS",
    str(BATCH_MAX_RECORDS * max(1, PARTITION_CONCURRENCY)),
))
PARTITION_QUEUE_RECORDS = int(
    os.getenv("PROCESSING_PARTITION_QUEUE_RECORDS", str(max(64, BATCH_MAX_RECORDS * 4)))
)
PARTITION_QUEUE_HIGH_RATIO = float(os.getenv("PROCESSING_PARTITION_QUEUE_HIGH_RATIO", "0.75"))
PARTITION_QUEUE_LOW_RATIO = float(os.getenv("PROCESSING_PARTITION_QUEUE_LOW_RATIO", "0.25"))
PARTITION_QUEUE_MAX_AGE_MS = float(os.getenv("PROCESSING_PARTITION_QUEUE_MAX_AGE_MS", "7"))
PARTITION_QUEUE_RESUME_AGE_MS = float(os.getenv("PROCESSING_PARTITION_QUEUE_RESUME_AGE_MS", "3"))
COMMIT_INTERVAL_MS = float(os.getenv("PROCESSING_COMMIT_INTERVAL_MS", "5"))
COMMIT_MAX_RECORDS = int(os.getenv("PROCESSING_COMMIT_MAX_RECORDS", "256"))
COMMIT_MAX_FAILURES = int(os.getenv("PROCESSING_COMMIT_MAX_FAILURES", "5"))
COMMIT_RETRY_BACKOFF_MS = float(os.getenv("PROCESSING_COMMIT_RETRY_BACKOFF_MS", "25"))
SHUTDOWN_DRAIN_TIMEOUT_SECONDS = float(os.getenv("PROCESSING_SHUTDOWN_DRAIN_SECONDS", "10"))
THROUGHPUT_LOG_INTERVAL_SECONDS = float(os.getenv("THROUGHPUT_LOG_INTERVAL_SECONDS", "10"))


def _deserialize(raw: bytes) -> dict:
    try:
        value = json.loads(raw.decode("utf-8"))
        if not isinstance(value, dict):
            raise ValueError("payload must be a JSON object")
        return value
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        return {
            "_decode_error": str(exc),
            "_raw_base64": base64.b64encode(raw).decode("ascii"),
        }


@dataclass
class _QueuedChunk:
    records: List[Any]
    enqueued_at: float


@dataclass
class _PartitionPipeline:
    """FIFO owned by exactly one mutating worker for one Kafka partition."""

    topic_partition: TopicPartition
    queue: deque[_QueuedChunk] = field(default_factory=deque)
    queue_ready: asyncio.Event = field(default_factory=asyncio.Event)
    task: Optional[asyncio.Task] = None
    queued_records: int = 0
    processing_records: int = 0
    paused: bool = False
    revoked: bool = False


class _RebalanceListener(ConsumerRebalanceListener):
    def __init__(self, owner: "BaseKafkaConsumer") -> None:
        self.owner = owner

    async def on_partitions_revoked(self, revoked) -> None:
        await self.owner._on_partitions_revoked(set(revoked))

    async def on_partitions_assigned(self, assigned) -> None:
        await self.owner._on_partitions_assigned(set(assigned))


class BaseKafkaConsumer(ABC):
    """
    Process partitions in parallel while preserving FIFO inside every partition.

    Every assigned partition owns a record queue and exactly one mutating worker.
    Completed batches publish their highest durable offset to a member-local commit
    coordinator. The coordinator coalesces partitions and commits off the mutation
    critical path; a crash may replay only version-fenced/idempotent writes.
    """

    def __init__(
        self,
        topic: str,
        group_id: str,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        db: Optional[DatabasePool] = None,
    ):
        self.topic = topic
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.db = db or DatabasePool()
        self._owns_db = db is None
        self.redis: Optional[aioredis.Redis] = None
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.dlq_producer: Optional[AIOKafkaProducer] = None
        self.metrics = ModuleMetrics(group_id)
        self.running = False
        self._stopped = False
        self._telemetry_task: Optional[asyncio.Task] = None
        self._processed_offsets: dict[TopicPartition, int] = {}
        self._pending_commit_offsets: dict[TopicPartition, int] = {}
        self._pending_commit_records: dict[TopicPartition, int] = {}
        self._partition_pipelines: dict[TopicPartition, _PartitionPipeline] = {}
        self._processing_slots = asyncio.Semaphore(PARTITION_CONCURRENCY)
        self._fatal_error: Optional[BaseException] = None
        self._commit_lock = asyncio.Lock()
        self._commit_wakeup = asyncio.Event()
        self._commit_stop = asyncio.Event()
        self._commit_task: Optional[asyncio.Task] = None
        self._consecutive_commit_failures = 0

    async def initialize(self) -> None:
        if self._owns_db:
            await self.db.connect()
        self.redis = create_redis_client()
        await self.redis.ping()
        self.consumer = AIOKafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=_deserialize,
            fetch_max_bytes=4 * 1024 * 1024,
            max_partition_fetch_bytes=1024 * 1024,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000,
        )
        self.consumer.subscribe([self.topic], listener=_RebalanceListener(self))
        self.dlq_producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            acks="all",
            enable_idempotence=True,
            value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
        )
        await self.consumer.start()
        await self.dlq_producer.start()
        self._commit_task = asyncio.create_task(
            self._commit_loop(), name=f"{self.group_id}-offset-commit"
        )
        logger.info(
            "[%s] consuming %s with partition workers concurrency=%d queue=%d records "
            "fetch_max=%d queue_age=%.1fms commit_interval=%.1fms commit_max_records=%d",
            self.group_id, self.topic, PARTITION_CONCURRENCY, PARTITION_QUEUE_RECORDS,
            FETCH_MAX_RECORDS, PARTITION_QUEUE_MAX_AGE_MS, COMMIT_INTERVAL_MS,
            COMMIT_MAX_RECORDS,
        )

    async def send_to_dlq(self, record: Any, error: Exception) -> None:
        assert self.dlq_producer is not None
        await self.dlq_producer.send_and_wait(
            f"{self.topic}.dlq",
            value={
                "consumer_group": self.group_id,
                "source_topic": record.topic,
                "source_partition": record.partition,
                "source_offset": record.offset,
                "error_type": type(error).__name__,
                "error": str(error),
                "payload": record.value,
            },
        )
        self.metrics.increment("errors")
        self.metrics.increment("dlq")

    @abstractmethod
    async def process_batch(self, records: List[Any]) -> None:
        """Process one partition batch in ascending Kafka offset order."""

    async def _on_partitions_assigned(self, partitions: set[TopicPartition]) -> None:
        for topic_partition in partitions:
            if topic_partition in self._partition_pipelines:
                continue
            state = _PartitionPipeline(topic_partition=topic_partition)
            state.task = asyncio.create_task(
                self._partition_worker(state),
                name=f"{self.group_id}-{topic_partition.topic}-{topic_partition.partition}",
            )
            self._partition_pipelines[topic_partition] = state
        self._update_partition_metrics()
        if partitions:
            logger.info(
                "[%s] assigned partitions=%s",
                self.group_id, sorted(partition.partition for partition in partitions),
            )

    async def _on_partitions_revoked(self, partitions: set[TopicPartition]) -> None:
        # Queued records remain durable in Kafka and will be replayed by the next
        # owner. Persisted-but-uncommitted records are safe because writes are
        # idempotent/version fenced.
        states = [self._partition_pipelines.pop(partition, None) for partition in partitions]
        active_states = [state for state in states if state is not None]
        for state in active_states:
            state.revoked = True
            if state.task is not None:
                state.task.cancel()
        if active_states:
            await asyncio.gather(
                *(state.task for state in active_states if state.task is not None),
                return_exceptions=True,
            )
        # Persisted offsets are committed once as a group before ownership is
        # released. If the rebalance already invalidated the generation, replay is
        # safe because every store write is idempotent/version fenced.
        await self._flush_pending_commits(partitions, fatal=False)
        for topic_partition in partitions:
            self._processed_offsets.pop(topic_partition, None)
            self._pending_commit_offsets.pop(topic_partition, None)
            self._pending_commit_records.pop(topic_partition, None)
        self.metrics.set_commit_pending(sum(self._pending_commit_records.values()))
        self._update_partition_metrics()
        if partitions:
            logger.info(
                "[%s] revoked partitions=%s; uncommitted records will be replayed",
                self.group_id, sorted(partition.partition for partition in partitions),
            )

    async def _partition_worker(self, state: _PartitionPipeline) -> None:
        try:
            while not state.revoked:
                records = await self._collect_partition_batch(state)
                state.processing_records = len(records)
                self._maybe_resume_partition(state)
                self._update_partition_metrics()
                try:
                    async with self._processing_slots:
                        await self._process_partition_batch(state.topic_partition, records)
                    if state.revoked:
                        return
                    self._mark_partition_processed(
                        state.topic_partition, records[-1].offset + 1, len(records)
                    )
                finally:
                    state.processing_records = 0
                    self._update_partition_metrics()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._fatal_error = exc
            self.running = False
            logger.exception(
                "[%s] fatal partition worker failure topic=%s partition=%d",
                self.group_id, state.topic_partition.topic, state.topic_partition.partition,
            )

    async def _collect_partition_batch(self, state: _PartitionPipeline) -> List[Any]:
        """Coalesce adjacent fetch fragments while preserving exact FIFO order."""
        records: List[Any] = []
        while len(records) < BATCH_MAX_RECORDS:
            if not state.queue:
                if records:
                    break
                state.queue_ready.clear()
                if not state.queue:
                    await state.queue_ready.wait()
                if state.revoked:
                    raise asyncio.CancelledError
            if not state.queue:
                continue
            queued_chunk = state.queue.popleft()
            chunk = queued_chunk.records

            available = BATCH_MAX_RECORDS - len(records)
            consumed = min(available, len(chunk))
            records.extend(chunk[:consumed])
            state.queued_records = max(0, state.queued_records - consumed)
            if consumed < len(chunk):
                state.queue.appendleft(_QueuedChunk(
                    records=chunk[consumed:], enqueued_at=queued_chunk.enqueued_at
                ))
                break
        return records

    async def _process_partition_batch(
        self, topic_partition: TopicPartition, records: List[Any]
    ) -> None:
        self.metrics.increment("processed", len(records))
        batch_started = time.monotonic()
        for attempt in range(1, MAX_BATCH_RETRIES + 1):
            try:
                await self.process_batch(records)
                break
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "[%s] partition batch failed partition=%d offsets=%d-%d attempt=%d/%d",
                    self.group_id, topic_partition.partition, records[0].offset,
                    records[-1].offset, attempt, MAX_BATCH_RETRIES,
                )
                if attempt == MAX_BATCH_RETRIES:
                    raise
                await asyncio.sleep(min(2 ** attempt, 10))
        self.metrics.observe_batch(time.monotonic() - batch_started)
        self._observe_e2e(records)

    def _mark_partition_processed(
        self, topic_partition: TopicPartition, next_offset: int, record_count: int
    ) -> None:
        previous = self._pending_commit_offsets.get(topic_partition, -1)
        self._pending_commit_offsets[topic_partition] = max(previous, next_offset)
        self._pending_commit_records[topic_partition] = (
            self._pending_commit_records.get(topic_partition, 0) + record_count
        )
        pending = sum(self._pending_commit_records.values())
        self.metrics.set_commit_pending(pending)
        if pending >= COMMIT_MAX_RECORDS:
            self._commit_wakeup.set()

    async def _commit_loop(self) -> None:
        interval = max(COMMIT_INTERVAL_MS / 1000.0, 0.001)
        try:
            while not self._commit_stop.is_set():
                try:
                    await asyncio.wait_for(self._commit_wakeup.wait(), timeout=interval)
                except asyncio.TimeoutError:
                    pass
                self._commit_wakeup.clear()
                if self._pending_commit_offsets:
                    await self._flush_pending_commits()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._fatal_error = exc
            self.running = False
            logger.exception("[%s] fatal offset commit coordinator failure", self.group_id)

    async def _flush_pending_commits(
        self, partitions: Optional[set[TopicPartition]] = None, *, fatal: bool = True
    ) -> None:
        assert self.consumer is not None
        async with self._commit_lock:
            snapshot = {
                topic_partition: offset
                for topic_partition, offset in self._pending_commit_offsets.items()
                if partitions is None or topic_partition in partitions
            }
            if not snapshot:
                return
            snapshot_counts = {
                topic_partition: self._pending_commit_records.get(topic_partition, 0)
                for topic_partition in snapshot
            }
            commit_started = time.monotonic()
            try:
                await self.consumer.commit(snapshot)
            except CommitFailedError:
                self.metrics.observe_commit(
                    time.monotonic() - commit_started, 0,
                    sum(self._pending_commit_records.values()), failed=True,
                )
                assigned = self.consumer.assignment()
                for topic_partition in tuple(snapshot):
                    if topic_partition not in assigned:
                        self._pending_commit_offsets.pop(topic_partition, None)
                        self._pending_commit_records.pop(topic_partition, None)
                self.metrics.set_commit_pending(sum(self._pending_commit_records.values()))
                logger.warning(
                    "[%s] offset commit lost consumer generation; uncommitted records replay",
                    self.group_id,
                )
                return
            except Exception:
                self._consecutive_commit_failures += 1
                self.metrics.observe_commit(
                    time.monotonic() - commit_started, 0,
                    sum(self._pending_commit_records.values()), failed=True,
                )
                if fatal and self._consecutive_commit_failures >= COMMIT_MAX_FAILURES:
                    raise
                logger.exception(
                    "[%s] offset commit failed attempt=%d/%d",
                    self.group_id, self._consecutive_commit_failures, COMMIT_MAX_FAILURES,
                )
                await asyncio.sleep(max(COMMIT_RETRY_BACKOFF_MS / 1000.0, 0.001))
                return

            self._consecutive_commit_failures = 0
            committed_records = sum(snapshot_counts.values())
            for topic_partition, committed_offset in snapshot.items():
                self._processed_offsets[topic_partition] = max(
                    self._processed_offsets.get(topic_partition, -1), committed_offset
                )
                if self._pending_commit_offsets.get(topic_partition, -1) <= committed_offset:
                    self._pending_commit_offsets.pop(topic_partition, None)
                    self._pending_commit_records.pop(topic_partition, None)
                else:
                    self._pending_commit_records[topic_partition] = max(
                        0,
                        self._pending_commit_records.get(topic_partition, 0)
                        - snapshot_counts[topic_partition],
                    )
            pending = sum(self._pending_commit_records.values())
            self.metrics.observe_commit(
                time.monotonic() - commit_started, committed_records, pending, failed=False
            )
            self._refresh_kafka_lag()

    def _observe_e2e(self, records: List[Any]) -> None:
        now_epoch = time.time()
        now_utc = datetime.fromtimestamp(now_epoch, timezone.utc)
        lags_ms = []
        for record in records:
            value = getattr(record, "value", None)
            if not isinstance(value, dict):
                continue
            ingest_epoch = value.get("ingest_epoch_s")
            if ingest_epoch is not None:
                try:
                    lags_ms.append(max(0.0, (now_epoch - float(ingest_epoch)) * 1000.0))
                    continue
                except (ValueError, TypeError):
                    pass
            ingest_timestamp = value.get("ingest_timestamp")
            if ingest_timestamp:
                try:
                    timestamp = datetime.fromisoformat(str(ingest_timestamp).replace("Z", "+00:00"))
                    lags_ms.append(max(0.0, (now_utc - timestamp).total_seconds() * 1000.0))
                except (TypeError, ValueError):
                    pass
        if lags_ms:
            self.metrics.observe_e2e_lag(lags_ms)

    def _enqueue_partition_batch(
        self, topic_partition: TopicPartition, records: List[Any]
    ) -> None:
        state = self._partition_pipelines.get(topic_partition)
        if state is None or state.revoked:
            state = _PartitionPipeline(topic_partition=topic_partition)
            state.task = asyncio.create_task(
                self._partition_worker(state),
                name=f"{self.group_id}-{topic_partition.topic}-{topic_partition.partition}",
            )
            self._partition_pipelines[topic_partition] = state
        state.queue.append(_QueuedChunk(records=records, enqueued_at=time.monotonic()))
        state.queue_ready.set()
        state.queued_records += len(records)
        if self._should_pause_partition(state) and not state.paused:
            assert self.consumer is not None
            self.consumer.pause(topic_partition)
            state.paused = True
        self._update_partition_metrics()

    @property
    def _queue_high_watermark(self) -> int:
        return max(1, int(PARTITION_QUEUE_RECORDS * PARTITION_QUEUE_HIGH_RATIO))

    @property
    def _queue_low_watermark(self) -> int:
        return max(0, int(PARTITION_QUEUE_RECORDS * PARTITION_QUEUE_LOW_RATIO))

    def _maybe_resume_partition(self, state: _PartitionPipeline) -> None:
        if (
            not state.paused or state.revoked
            or state.queued_records > self._queue_low_watermark
            or self._partition_queue_age_ms(state) > PARTITION_QUEUE_RESUME_AGE_MS
        ):
            return
        assert self.consumer is not None
        if state.topic_partition in self.consumer.assignment():
            self.consumer.resume(state.topic_partition)
            state.paused = False

    @staticmethod
    def _partition_queue_age_ms(state: _PartitionPipeline) -> float:
        if not state.queue:
            return 0.0
        return max(0.0, (time.monotonic() - state.queue[0].enqueued_at) * 1000.0)

    def _should_pause_partition(self, state: _PartitionPipeline) -> bool:
        return (
            state.queued_records >= self._queue_high_watermark
            or self._partition_queue_age_ms(state) >= PARTITION_QUEUE_MAX_AGE_MS
        )

    def _refresh_kafka_lag(self) -> None:
        assert self.consumer is not None
        lag = 0
        for topic_partition in self.consumer.assignment():
            highwater = self.consumer.highwater(topic_partition)
            processed = self._processed_offsets.get(topic_partition)
            if highwater is not None and processed is not None:
                lag += max(0, highwater - processed)
        self.metrics.set_kafka_lag(lag)

    def _update_partition_metrics(self) -> None:
        self.metrics.set_partition_pipeline(
            queued_records=sum(state.queued_records for state in self._partition_pipelines.values()),
            active_workers=sum(
                1 for state in self._partition_pipelines.values()
                if state.task is not None and not state.task.done()
            ),
            paused_partitions=sum(1 for state in self._partition_pipelines.values() if state.paused),
            concurrency_limit=PARTITION_CONCURRENCY,
            oldest_queue_ms=max(
                (self._partition_queue_age_ms(state) for state in self._partition_pipelines.values()),
                default=0.0,
            ),
        )

    async def run(self) -> None:
        if PARTITION_CONCURRENCY < 1:
            raise ValueError("PROCESSING_PARTITION_CONCURRENCY must be positive")
        if PARTITION_QUEUE_RECORDS < BATCH_MAX_RECORDS:
            raise ValueError("PROCESSING_PARTITION_QUEUE_RECORDS must be >= BATCH_MAX_RECORDS")
        if FETCH_MAX_RECORDS < BATCH_MAX_RECORDS:
            raise ValueError("PROCESSING_FETCH_MAX_RECORDS must be >= BATCH_MAX_RECORDS")
        if not 0.0 < PARTITION_QUEUE_LOW_RATIO < PARTITION_QUEUE_HIGH_RATIO <= 1.0:
            raise ValueError("partition queue ratios must satisfy 0 < low < high <= 1")
        if not 0 <= PARTITION_QUEUE_RESUME_AGE_MS < PARTITION_QUEUE_MAX_AGE_MS:
            raise ValueError("partition queue age must satisfy 0 <= resume_age < max_age")
        if COMMIT_INTERVAL_MS <= 0 or COMMIT_MAX_RECORDS < 1 or COMMIT_MAX_FAILURES < 1:
            raise ValueError("commit coordinator settings must be positive")
        await self.initialize()
        self.running = True
        self._telemetry_task = asyncio.create_task(
            self.metrics.log_periodically(THROUGHPUT_LOG_INTERVAL_SECONDS),
            name=f"{self.group_id}-telemetry",
        )
        try:
            assert self.consumer is not None
            while self.running:
                if self._fatal_error is not None:
                    raise self._fatal_error
                batches = await self.consumer.getmany(
                    timeout_ms=BATCH_TIMEOUT_MS, max_records=FETCH_MAX_RECORDS,
                )
                for topic_partition, records in batches.items():
                    if records:
                        self._enqueue_partition_batch(topic_partition, records)
            if self._fatal_error is not None:
                raise self._fatal_error
        finally:
            await self._shutdown_partition_workers()
            await self.stop()

    async def _shutdown_partition_workers(self) -> None:
        states = list(self._partition_pipelines.values())
        if states and self._fatal_error is None:
            loop = asyncio.get_running_loop()
            deadline = loop.time() + SHUTDOWN_DRAIN_TIMEOUT_SECONDS
            while any(
                state.queued_records or state.processing_records for state in states
            ) and loop.time() < deadline:
                await asyncio.sleep(0.01)
            if any(state.queued_records or state.processing_records for state in states):
                logger.warning(
                    "[%s] partition drain exceeded %.1fs; remaining records will replay",
                    self.group_id, SHUTDOWN_DRAIN_TIMEOUT_SECONDS,
                )
        for state in states:
            state.revoked = True
            if state.task is not None:
                state.task.cancel()
        if states:
            await asyncio.gather(
                *(state.task for state in states if state.task is not None),
                return_exceptions=True,
            )
        await self._flush_pending_commits(fatal=False)
        self._partition_pipelines.clear()
        self._update_partition_metrics()

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        self.running = False
        if self._telemetry_task is not None:
            self._telemetry_task.cancel()
            await asyncio.gather(self._telemetry_task, return_exceptions=True)
        if self._commit_task is not None:
            self._commit_stop.set()
            self._commit_wakeup.set()
            await asyncio.gather(self._commit_task, return_exceptions=True)
            self._commit_task = None
        if self.consumer is not None:
            await self.consumer.stop()
        if self.dlq_producer is not None:
            await self.dlq_producer.stop()
        if self.redis is not None:
            await self.redis.aclose()
        if self._owns_db:
            await self.db.close()
        self.metrics.log_summary()
