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
from collections.abc import Awaitable
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
PARTITION_QUEUE_MAX_AGE_MS = float(os.getenv("PROCESSING_PARTITION_QUEUE_MAX_AGE_MS", "12"))
PARTITION_QUEUE_RESUME_AGE_MS = float(os.getenv("PROCESSING_PARTITION_QUEUE_RESUME_AGE_MS", "4"))
COMMIT_INTERVAL_MS = float(os.getenv("PROCESSING_COMMIT_INTERVAL_MS", "25"))
COMMIT_MAX_RECORDS = int(os.getenv("PROCESSING_COMMIT_MAX_RECORDS", "512"))
COMMIT_MAX_FAILURES = int(os.getenv("PROCESSING_COMMIT_MAX_FAILURES", "5"))
COMMIT_RETRY_BACKOFF_MS = float(os.getenv("PROCESSING_COMMIT_RETRY_BACKOFF_MS", "25"))
COMBINE_WAIT_MS = float(os.getenv("PROCESSING_COMBINE_WAIT_MS", "2"))
COMBINE_MAX_RECORDS = int(os.getenv("PROCESSING_COMBINE_MAX_RECORDS", "64"))
COMBINE_QUEUE_BATCHES = int(os.getenv("PROCESSING_COMBINE_QUEUE_BATCHES", "64"))
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


@dataclass
class _ProcessRequest:
    """One FIFO-safe partition batch waiting for process-level coalescing."""

    topic_partition: TopicPartition
    records: List[Any]
    completed: asyncio.Future


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
        self._durability_tasks: dict[TopicPartition, asyncio.Task[None]] = {}
        self._process_queue: asyncio.Queue[Optional[_ProcessRequest]] = asyncio.Queue(
            maxsize=COMBINE_QUEUE_BATCHES
        )
        self._process_combiner_task: Optional[asyncio.Task] = None

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
        self._process_combiner_task = asyncio.create_task(
            self._process_combiner_loop(), name=f"{self.group_id}-write-combiner"
        )
        self._commit_task = asyncio.create_task(
            self._commit_loop(), name=f"{self.group_id}-offset-commit"
        )
        logger.info(
            "[%s] consuming %s with partition workers concurrency=%d queue=%d records "
            "fetch_max=%d queue_age=%.1fms combine_wait=%.1fms "
            "combine_max_records=%d commit_interval=%.1fms commit_max_records=%d",
            self.group_id, self.topic, PARTITION_CONCURRENCY, PARTITION_QUEUE_RECORDS,
            FETCH_MAX_RECORDS, PARTITION_QUEUE_MAX_AGE_MS, COMBINE_WAIT_MS,
            COMBINE_MAX_RECORDS, COMMIT_INTERVAL_MS, COMMIT_MAX_RECORDS,
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
    async def process_batch(self, records: List[Any]) -> Optional[Awaitable[None]]:
        """Process one partition batch and optionally return deferred durability."""

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
        cancelled_durability: list[asyncio.Task[None]] = []
        for topic_partition in partitions:
            durability_task = self._durability_tasks.pop(topic_partition, None)
            if durability_task is not None:
                durability_task.cancel()
                cancelled_durability.append(durability_task)
            self._processed_offsets.pop(topic_partition, None)
            self._pending_commit_offsets.pop(topic_partition, None)
            self._pending_commit_records.pop(topic_partition, None)
        if cancelled_durability:
            await asyncio.gather(*cancelled_durability, return_exceptions=True)
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
                        durability = await self._process_partition_batch(
                            state.topic_partition, records
                        )
                    if state.revoked:
                        return
                    self._register_partition_completion(
                        state.topic_partition, records[-1].offset + 1,
                        len(records), durability,
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
    ) -> Optional[Awaitable[None]]:
        if self._process_combiner_task is None or self._process_combiner_task.done():
            raise RuntimeError("process-level write combiner is not running")
        completed = asyncio.get_running_loop().create_future()
        await self._process_queue.put(_ProcessRequest(
            topic_partition=topic_partition, records=records, completed=completed
        ))
        return await completed

    async def _process_combiner_loop(self) -> None:
        """Coalesce independent partition batches into one downstream write.

        A partition worker does not submit its next batch until its request future
        completes, preserving exact FIFO per partition. Kafka keys keep one entity
        on one partition, so records coalesced across partitions are independent.
        """
        try:
            while True:
                first = await self._process_queue.get()
                if first is None:
                    self._process_queue.task_done()
                    return
                requests = [first]
                record_count = len(first.records)
                stop_after_group = False
                deadline = asyncio.get_running_loop().time() + max(
                    0.0, COMBINE_WAIT_MS / 1000.0
                )
                while record_count < COMBINE_MAX_RECORDS:
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        break
                    try:
                        request = await asyncio.wait_for(
                            self._process_queue.get(), timeout=remaining
                        )
                    except asyncio.TimeoutError:
                        break
                    if request is None:
                        self._process_queue.task_done()
                        stop_after_group = True
                        break
                    if record_count + len(request.records) > COMBINE_MAX_RECORDS:
                        # Queue FIFO cannot push left. Including one oversized tail
                        # is preferable to reordering requests; max is a soft bound.
                        requests.append(request)
                        record_count += len(request.records)
                        break
                    requests.append(request)
                    record_count += len(request.records)

                records = [record for request in requests for record in request.records]
                durability: Optional[Awaitable[None]] = None
                failure: Optional[BaseException] = None
                try:
                    durability = await self._execute_process_batch(requests, records)
                    if durability is not None:
                        durability = asyncio.ensure_future(durability)
                except BaseException as exc:
                    failure = exc
                finally:
                    for request in requests:
                        self._process_queue.task_done()

                for request in requests:
                    if request.completed.done():
                        continue
                    if failure is not None:
                        request.completed.set_exception(failure)
                    else:
                        request.completed.set_result(durability)
                if stop_after_group:
                    return
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            self._fatal_error = exc
            self.running = False
            while True:
                try:
                    request = self._process_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                self._process_queue.task_done()
                if request is not None and not request.completed.done():
                    request.completed.set_exception(exc)
            logger.exception("[%s] process-level write combiner failed", self.group_id)

    async def _execute_process_batch(
        self, requests: List[_ProcessRequest], records: List[Any]
    ) -> Optional[Awaitable[None]]:
        self.metrics.observe_preprocess_lag(self._message_lags_ms(records))
        self.metrics.increment("processed", len(records))
        batch_started = time.monotonic()
        for attempt in range(1, MAX_BATCH_RETRIES + 1):
            try:
                durability = await self.process_batch(records)
                break
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "[%s] combined batch failed partitions=%s records=%d attempt=%d/%d",
                    self.group_id,
                    sorted({request.topic_partition.partition for request in requests}),
                    len(records), attempt, MAX_BATCH_RETRIES,
                )
                if attempt == MAX_BATCH_RETRIES:
                    raise
                await asyncio.sleep(min(2 ** attempt, 10))
        self.metrics.observe_batch(time.monotonic() - batch_started)
        self._observe_e2e(records)
        return durability

    def _register_partition_completion(
        self,
        topic_partition: TopicPartition,
        next_offset: int,
        record_count: int,
        durability: Optional[Awaitable[None]],
    ) -> None:
        previous = self._durability_tasks.get(topic_partition)

        async def complete_in_order() -> None:
            try:
                if previous is not None:
                    await previous
                if durability is not None:
                    # A rebalance may cancel this waiter, but must not cancel the
                    # shared checkpoint future used by other partition batches.
                    await asyncio.shield(durability)
                self._mark_partition_processed(
                    topic_partition, next_offset, record_count
                )
            except asyncio.CancelledError:
                raise
            except BaseException as exc:
                self._fatal_error = exc
                self.running = False
                logger.exception(
                    "[%s] deferred durability failed topic=%s partition=%d next_offset=%d",
                    self.group_id, topic_partition.topic,
                    topic_partition.partition, next_offset,
                )
            finally:
                current = asyncio.current_task()
                if self._durability_tasks.get(topic_partition) is current:
                    self._durability_tasks.pop(topic_partition, None)

        self._durability_tasks[topic_partition] = asyncio.create_task(
            complete_in_order(),
            name=(
                f"{self.group_id}-{topic_partition.partition}-"
                f"durable-{next_offset}"
            ),
        )

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

    @staticmethod
    def _message_lags_ms(records: List[Any]) -> List[float]:
        now_epoch = time.time()
        now_utc = datetime.fromtimestamp(now_epoch, timezone.utc)
        lags_ms: List[float] = []
        for record in records:
            value = getattr(record, "value", None)
            if not isinstance(value, dict):
                continue
            ingest_epoch_ns = value.get("ingest_epoch_ns")
            if ingest_epoch_ns is not None:
                try:
                    lags_ms.append(max(
                        0.0,
                        (now_epoch - int(ingest_epoch_ns) / 1_000_000_000) * 1000.0,
                    ))
                    continue
                except (ValueError, TypeError):
                    pass
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
        return lags_ms

    def _observe_e2e(self, records: List[Any]) -> None:
        lags_ms = self._message_lags_ms(records)
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
        if COMBINE_WAIT_MS < 0 or COMBINE_MAX_RECORDS < 1 or COMBINE_QUEUE_BATCHES < 1:
            raise ValueError("process combiner settings must be non-negative/positive")
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
        durability_tasks = list(self._durability_tasks.values())
        if durability_tasks and self._fatal_error is None:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*durability_tasks, return_exceptions=False),
                    timeout=SHUTDOWN_DRAIN_TIMEOUT_SECONDS,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "[%s] durability drain exceeded %.1fs; records will replay",
                    self.group_id, SHUTDOWN_DRAIN_TIMEOUT_SECONDS,
                )
            except BaseException as exc:
                self._fatal_error = self._fatal_error or exc
        for task in tuple(self._durability_tasks.values()):
            if not task.done():
                task.cancel()
        if self._durability_tasks:
            await asyncio.gather(
                *self._durability_tasks.values(), return_exceptions=True
            )
        self._durability_tasks.clear()
        await self._flush_pending_commits(fatal=False)
        self._partition_pipelines.clear()
        self._update_partition_metrics()

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        self.running = False
        if self._process_combiner_task is not None:
            if not self._process_combiner_task.done():
                await self._process_queue.put(None)
            await asyncio.gather(self._process_combiner_task, return_exceptions=True)
            self._process_combiner_task = None
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
