from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal

from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError
from prometheus_client import start_http_server

from pipeline.modules.device_swap.consumer import DeviceSwapConsumer
from pipeline.modules.ip_msisdn.consumer import IPMsisdnConsumer
from pipeline.modules.shared.db import DatabasePool
from pipeline.modules.sim_swap.consumer import SimSwapConsumer

logger = logging.getLogger(__name__)


async def ensure_topics(bootstrap_servers: str, topics: list[str]) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        for topic in topics:
            try:
                await admin.create_topics([NewTopic(
                    topic, num_partitions=int(os.getenv("KAFKA_TOPIC_PARTITIONS", "8")),
                    replication_factor=int(os.getenv("KAFKA_REPLICATION_FACTOR", "1")),
                )])
                logger.info("Created Kafka topic %s", topic)
            except TopicAlreadyExistsError:
                pass
    finally:
        await admin.close()


async def run_pipeline(duration: int | None = None) -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")
    raw_topic = os.getenv("KAFKA_TOPIC_RAW", "radius.accounting.raw")
    dlq_topic = f"{raw_topic}.dlq"
    start_http_server(int(os.getenv("METRICS_PORT", "9200")))
    await ensure_topics(bootstrap, [raw_topic, dlq_topic])

    database = DatabasePool()
    await database.connect()

    # F-PARALLEL: CONSUMERS_PER_GROUP instance ĐỘC LẬP cho mỗi group — mỗi
    # instance là 1 AIOKafkaConsumer/1 member riêng trong CÙNG group_id, nên
    # Kafka rebalance chia partition thật ra cho từng instance (trước đây luôn
    # đúng 1 instance/group ôm hết mọi partition, dù chạy 8-12 partition cũng
    # chỉ xử lý tuần tự trong 1 task). Đặt KAFKA_TOPIC_PARTITIONS >= số instance
    # nhiều nhất trong 1 group, nếu không instance dư sẽ rảnh (idle member).
    consumers_per_group = max(1, int(os.getenv("CONSUMERS_PER_GROUP", "1")))
    consumer_specs = [
        (IPMsisdnConsumer, "cg-ip-msisdn"),
        (DeviceSwapConsumer, "cg-device-swap"),
        (SimSwapConsumer, "cg-sim-swap"),
    ]
    consumers = [
        cls(raw_topic, group_id, database)
        for cls, group_id in consumer_specs
        for _ in range(consumers_per_group)
    ]
    tasks = {
        asyncio.create_task(consumer.run(), name=f"{consumer.group_id}-{i}"): consumer
        for i, consumer in enumerate(consumers)
    }
    stop_task = asyncio.create_task(stop_event.wait(), name="shutdown-signal")
    duration_task = asyncio.create_task(asyncio.sleep(duration), name="duration") if duration else None
    watched = [*tasks, stop_task] + ([duration_task] if duration_task else [])
    failure: BaseException | None = None
    logger.info(
        "Pipeline ready: %s (consumers_per_group=%d, %d task tổng cộng)",
        ", ".join(group_id for _, group_id in consumer_specs), consumers_per_group, len(consumers),
    )
    try:
        done, _ = await asyncio.wait(watched, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            if task in tasks and not stop_event.is_set():
                if task.cancelled():
                    failure = RuntimeError(f"critical task {task.get_name()} was cancelled")
                else:
                    failure = task.exception() or RuntimeError(f"critical task {task.get_name()} exited unexpectedly")
                break
    finally:
        stop_event.set()
        for consumer in consumers:
            consumer.running = False
        pending_consumers = [task for task in tasks if not task.done()]
        if pending_consumers:
            try:
                await asyncio.wait_for(asyncio.gather(*pending_consumers, return_exceptions=True), timeout=15)
            except asyncio.TimeoutError:
                for task in pending_consumers:
                    task.cancel()
                await asyncio.gather(*pending_consumers, return_exceptions=True)
        for task in (stop_task, duration_task):
            if task and not task.done():
                task.cancel()
        await database.close()
    if failure:
        raise failure


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the three RADIUS processing consumers")
    parser.add_argument("--duration", type=int)
    args = parser.parse_args()
    asyncio.run(run_pipeline(args.duration))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    main()