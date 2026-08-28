from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, List, Optional

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from pipeline.modules.shared.db import DatabasePool
from pipeline.modules.shared.metrics import ModuleMetrics
from pipeline.modules.shared.redis_client import create_redis_client


logger = logging.getLogger(__name__)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")
BATCH_MAX_RECORDS = int(os.getenv("BATCH_MAX_RECORDS", "500"))
BATCH_TIMEOUT_MS = int(os.getenv("BATCH_TIMEOUT_MS", "100"))
MAX_BATCH_RETRIES = int(os.getenv("MAX_BATCH_RETRIES", "3"))
PARTITION_CONCURRENCY = int(os.getenv("PROCESSING_PARTITION_CONCURRENCY", "4"))
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


class BaseKafkaConsumer(ABC):
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
        self._processed_offsets: dict[Any, int] = {}

    async def initialize(self) -> None:
        if self._owns_db:
            await self.db.connect()
        self.redis = create_redis_client()
        await self.redis.ping()
        self.consumer = AIOKafkaConsumer(
            self.topic,
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
        self.dlq_producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            acks="all",
            enable_idempotence=True,
            value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
        )
        await self.consumer.start()
        await self.dlq_producer.start()
        logger.info("[%s] consuming %s with manual commit", self.group_id, self.topic)

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
        """Process one shard; offsets remain ordered inside every partition."""

    async def run(self) -> None:
        if PARTITION_CONCURRENCY < 1:
            raise ValueError("PROCESSING_PARTITION_CONCURRENCY must be positive")
        await self.initialize()
        self.running = True
        self._telemetry_task = asyncio.create_task(
            self.metrics.log_periodically(THROUGHPUT_LOG_INTERVAL_SECONDS),
            name=f"{self.group_id}-telemetry",
        )
        try:
            assert self.consumer is not None
            while self.running:
                batches = await self.consumer.getmany(
                    timeout_ms=BATCH_TIMEOUT_MS,
                    max_records=BATCH_MAX_RECORDS,
                )
                partition_batches = [
                    (topic_partition, records)
                    for topic_partition, records in batches.items()
                    if records
                ]
                if not partition_batches:
                    continue

                shard_count = min(PARTITION_CONCURRENCY, len(partition_batches))
                shards: List[List[tuple[Any, List[Any]]]] = [[] for _ in range(shard_count)]
                for index, item in enumerate(partition_batches):
                    shards[index % shard_count].append(item)

                async def process_shard(shard: List[tuple[Any, List[Any]]]) -> None:
                    records = [record for _partition, part in shard for record in part]
                    partitions = [partition for partition, _records in shard]
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
                                "[%s] shard failed partitions=%s attempt=%d/%d",
                                self.group_id,
                                partitions,
                                attempt,
                                MAX_BATCH_RETRIES,
                            )
                            if attempt == MAX_BATCH_RETRIES:
                                raise
                            await asyncio.sleep(min(2 ** attempt, 10))
                    batch_duration = time.monotonic() - batch_started
                    self.metrics.observe_batch(batch_duration)
                    now_epoch = time.time()
                    now_utc = datetime.fromtimestamp(now_epoch, timezone.utc)
                    e2e_lags_ms = []
                    for record in records:
                        val = getattr(record, "value", None)
                        if isinstance(val, dict):
                            ingest_epoch = val.get("ingest_epoch_s")
                            if ingest_epoch is not None:
                                try:
                                    lag_ms = max(0.0, (now_epoch - float(ingest_epoch)) * 1000.0)
                                    e2e_lags_ms.append(lag_ms)
                                    continue
                                except (ValueError, TypeError):
                                    pass
                            ingest_ts = val.get("ingest_timestamp")
                            if ingest_ts:
                                try:
                                    dt = datetime.fromisoformat(str(ingest_ts).replace("Z", "+00:00"))
                                    lag_ms = max(0.0, (now_utc - dt).total_seconds() * 1000.0)
                                    e2e_lags_ms.append(lag_ms)
                                except Exception:
                                    pass
                    if e2e_lags_ms:
                        self.metrics.observe_e2e_lag(e2e_lags_ms)

                # Kafka key cố định partition, nên cùng một thuê bao không bị tách
                # qua hai shard. Mỗi partition vẫn được duyệt theo offset; chỉ thứ tự
                # giữa các partition độc lập là không xác định. Một shard tạo một
                # batch DB/Redis lớn thay vì một transaction cho từng partition.
                await asyncio.gather(*(process_shard(shard) for shard in shards))
                offsets = {
                    partition: records[-1].offset + 1
                    for partition, records in partition_batches
                }
                if offsets:
                    await self.consumer.commit(offsets)
                    self._processed_offsets.update(offsets)
                    lag = 0
                    for partition in self.consumer.assignment():
                        highwater = self.consumer.highwater(partition)
                        processed = self._processed_offsets.get(partition)
                        if highwater is not None and processed is not None:
                            lag += max(0, highwater - processed)
                    self.metrics.set_kafka_lag(lag)
        finally:
            await self.stop()

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        self.running = False
        if self._telemetry_task is not None:
            self._telemetry_task.cancel()
            await asyncio.gather(self._telemetry_task, return_exceptions=True)
        if self.consumer is not None:
            await self.consumer.stop()
        if self.dlq_producer is not None:
            await self.dlq_producer.stop()
        if self.redis is not None:
            await self.redis.aclose()
        if self._owns_db:
            await self.db.close()
        self.metrics.log_summary()
