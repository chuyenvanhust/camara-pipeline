from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Any, List, Optional

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from pipeline.modules.shared.db import DatabasePool
from pipeline.modules.shared.metrics import ModuleMetrics


logger = logging.getLogger(__name__)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")
BATCH_MAX_RECORDS = int(os.getenv("BATCH_MAX_RECORDS", "500"))
BATCH_TIMEOUT_MS = int(os.getenv("BATCH_TIMEOUT_MS", "100"))
MAX_BATCH_RETRIES = int(os.getenv("MAX_BATCH_RETRIES", "3"))


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

    async def initialize(self) -> None:
        if self._owns_db:
            await self.db.connect()
        self.redis = aioredis.Redis(
            host=os.getenv("REDIS_HOST", "camara-redis"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            db=int(os.getenv("REDIS_DB", "0")),
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
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

    @abstractmethod
    async def process_batch(self, records: List[Any]) -> None:
        """Process records from one TopicPartition in offset order."""

    async def run(self) -> None:
        await self.initialize()
        self.running = True
        try:
            assert self.consumer is not None
            while self.running:
                batches = await self.consumer.getmany(
                    timeout_ms=BATCH_TIMEOUT_MS,
                    max_records=BATCH_MAX_RECORDS,
                )
                for topic_partition, records in batches.items():
                    if not records:
                        continue
                    self.metrics.increment("processed", len(records))
                    for attempt in range(1, MAX_BATCH_RETRIES + 1):
                        try:
                            await self.process_batch(records)
                            break
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            logger.exception(
                                "[%s] batch failed partition=%s attempt=%d/%d",
                                self.group_id,
                                topic_partition,
                                attempt,
                                MAX_BATCH_RETRIES,
                            )
                            if attempt == MAX_BATCH_RETRIES:
                                raise
                            await asyncio.sleep(min(2 ** attempt, 10))
                    await self.consumer.commit(
                        {topic_partition: records[-1].offset + 1}
                    )
        finally:
            await self.stop()

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        self.running = False
        if self.consumer is not None:
            await self.consumer.stop()
        if self.dlq_producer is not None:
            await self.dlq_producer.stop()
        if self.redis is not None:
            await self.redis.aclose()
        if self._owns_db:
            await self.db.close()
        self.metrics.log_summary()
