# pipeline/modules/shared/base_consumer.py
import asyncio
import json
import logging
import os
import signal
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List

import redis.asyncio as aioredis
from aiokafka import AIOKafkaConsumer

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


class BaseKafkaConsumer(ABC):
    def __init__(
        self,
        topic: str,
        group_id: str,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    ):
        self.topic = topic
        self.group_id = group_id
        self.bootstrap_servers = bootstrap_servers
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.db = DatabasePool()
        self.redis: Optional[aioredis.Redis] = None
        self.metrics = ModuleMetrics(group_id)
        self.running = False

    async def initialize(self):
        logger.info(f"[{self.group_id}] Initializing connections...")
        await self.db.connect()

        self.redis = aioredis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True,
        )
        await self.redis.ping()

        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
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
        if self.redis:
            await self.redis.aclose()
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

    async def run(self):
        await self.initialize()
        self.running = True

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.stop()))
            except NotImplementedError:
                # Signal handlers not implemented on Windows for some loops
                pass

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

                # Gom tất cả messages từ mọi partition thành 1 batch phẳng
                batch: List[Dict[str, Any]] = []
                for tp, messages in data.items():
                    for msg in messages:
                        batch.append(msg.value)

                if not batch:
                    continue

                batch_size = len(batch)
                self.metrics.increment("processed", batch_size)

                try:
                    await self.process_batch(batch)
                except Exception as exc:
                    self.metrics.increment("errors", batch_size)
                    logger.error(
                        f"[{self.group_id}] Error processing batch of {batch_size}: {exc}",
                        exc_info=True,
                    )

        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()
