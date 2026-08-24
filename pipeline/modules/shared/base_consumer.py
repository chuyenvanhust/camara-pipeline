# pipeline/modules/shared/base_consumer.py
import asyncio
import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from pipeline.modules.shared.db import DatabasePool
from pipeline.modules.shared.metrics import ModuleMetrics

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "camara-redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# Adaptive batch configuration
BATCH_MAX_RECORDS = int(os.getenv("BATCH_MAX_RECORDS", "500"))
BATCH_TIMEOUT_MS = int(os.getenv("BATCH_TIMEOUT_MS", "100"))

# F-01: Retry configuration
MAX_BATCH_RETRIES = int(os.getenv("MAX_BATCH_RETRIES", "3"))


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
        self.consumer: Optional[AIOKafkaConsumer] = None
        # F-09: Allow injecting a shared DB pool instead of each consumer creating its own
        self.db = db or DatabasePool()
        self._owns_db = db is None  # Only close() if we created it ourselves
        self.redis: Optional[aioredis.Redis] = None
        self.metrics = ModuleMetrics(group_id)
        self.running = False
        self._dlq_producer: Optional[AIOKafkaProducer] = None

    async def initialize(self):
        logger.info(f"[{self.group_id}] Initializing connections...")
        if self._owns_db:
            await self.db.connect()

        self.redis = aioredis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True,
        )
        await self.redis.ping()

        # F-01: Disable auto-commit — we commit manually after successful processing
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            # Tối ưu Kafka fetch — Tầng 2
            fetch_max_bytes=4 * 1024 * 1024,         # 4MB (mặc định 1MB)
            max_partition_fetch_bytes=1048576,         # 1MB per partition
            session_timeout_ms=30000,                  # tránh rebalance khi batch lớn
            heartbeat_interval_ms=10000,
        )
        await self.consumer.start()
        logger.info(f"[{self.group_id}] Consumer started for topic '{self.topic}' with group '{self.group_id}'")

    async def stop(self):
        logger.info(f"[{self.group_id}] Stopping consumer...")
        self.running = False
        if self.consumer:
            await self.consumer.stop()
        if self._dlq_producer:
            await self._dlq_producer.stop()
            self._dlq_producer = None
        if self.redis:
            await self.redis.aclose()
        # F-09: Only close DB pool if we own it (created it ourselves)
        if self._owns_db:
            await self.db.close()
        self.metrics.log_summary()
        logger.info(f"[{self.group_id}] Consumer stopped.")

    @abstractmethod
    async def process_message(self, message: Dict[str, Any]):
        """Override in subclasses to implement specific module logic."""
        pass

    async def process_batch(self, messages: List[Dict[str, Any]]):
        """
        Override in subclasses for optimized batch processing.
        Default: falls back to processing messages one-by-one (backward compatible).
        """
        for msg in messages:
            try:
                await self.process_message(msg)
                self.metrics.increment("success")
            except Exception as exc:
                self.metrics.increment("errors")
                logger.error(f"[{self.group_id}] Error processing message: {exc}", exc_info=True)

    async def _send_to_dlq(self, tp, tp_messages, exc: Exception):
        """
        F-01: Ghi batch lỗi vượt quá retry vào topic DLQ,
        kèm raw payload + lỗi, để không mất dữ liệu.
        """
        if self._dlq_producer is None:
            self._dlq_producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            await self._dlq_producer.start()

        dlq_topic = f"{self.topic}.dlq"
        for m in tp_messages:
            await self._dlq_producer.send(
                dlq_topic,
                value={
                    "original_topic": m.topic,
                    "partition": m.partition,
                    "offset": m.offset,
                    "raw_value": m.value,
                    "error": str(exc),
                    "consumer_group": self.group_id,
                },
            )
        await self._dlq_producer.flush()
        logger.error(
            f"[{self.group_id}] Đã đẩy {len(tp_messages)} message vào {dlq_topic} "
            f"sau {MAX_BATCH_RETRIES} lần retry thất bại."
        )

    async def run(self):
        await self.initialize()
        self.running = True

        # F-06: KHÔNG đăng ký signal handler ở đây nữa.
        # Orchestrator (run_pipeline.py) là nơi DUY NHẤT bắt signal.

        try:
            assert self.consumer is not None
            while self.running:
                # ============================================================
                # Adaptive Batch: getmany()
                # - Khi throughput cao: gom đủ max_records (500) rồi flush batch
                # - Khi throughput thấp/thưa: timeout_ms (100ms) tự flush batch nhỏ
                # ============================================================
                data = await self.consumer.getmany(
                    timeout_ms=BATCH_TIMEOUT_MS,
                    max_records=BATCH_MAX_RECORDS,
                )
                if not data:
                    continue

                # F-01: Process per-partition for correct offset tracking
                for tp, tp_messages in data.items():
                    if not tp_messages:
                        continue

                    batch = [m.value for m in tp_messages]
                    batch_size = len(batch)
                    self.metrics.increment("processed", batch_size)

                    # F-01: Retry loop with exponential backoff
                    attempt = 0
                    while True:
                        try:
                            await self.process_batch(batch)
                            break
                        except Exception as exc:
                            attempt += 1
                            self.metrics.increment("errors", batch_size)
                            logger.error(
                                f"[{self.group_id}] Batch lỗi trên {tp} "
                                f"(lần {attempt}/{MAX_BATCH_RETRIES}): {exc}",
                                exc_info=True,
                            )
                            if attempt >= MAX_BATCH_RETRIES:
                                await self._send_to_dlq(tp, tp_messages, exc)
                                break
                            await asyncio.sleep(min(2 ** attempt, 10))

                    # F-01: Commit offset SAU KHI batch xử lý xong (hoặc đã vào DLQ)
                    from aiokafka import TopicPartition as _TP
                    last_offset = tp_messages[-1].offset
                    await self.consumer.commit(
                        {tp: last_offset + 1}
                    )

        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()
