#!/usr/bin/env python3
"""
run_pipeline.py

RADIUS Pipeline Orchestrator (Refactored)

Luồng xử lý:
1. Tạo topic Kafka: `radius.accounting.raw`
2. Nếu truyền `--input <file.csv>`: chạy Stage 1 Ingestion Producer đẩy CSV vào `radius.accounting.raw`
3. Khởi chạy 3 Module Consumer song song:
   - Module 1: IP-MSISDN Processing (Consumer group: cg-ip-msisdn) -> Redis ip-ggsn:<ip>
   - Module 2: Device Swap Processing (Consumer group: cg-device-swap) -> msisdn_device, device_swap_history, audit_log, notification
   - Module 3: SIM Swap Processing (Consumer group: cg-sim-swap) -> msisdn_sim, sim_swap_history, audit_log, notification
"""

import argparse
import asyncio
import os
import signal
import sys
import time
from typing import List

from dotenv import load_dotenv

WORKSPACE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(WORKSPACE)
if WORKSPACE not in sys.path:
    sys.path.insert(0, WORKSPACE)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pipeline.modules.ip_msisdn.consumer import IPMsisdnConsumer
from pipeline.modules.device_swap.consumer import DeviceSwapConsumer
from pipeline.modules.sim_swap.consumer import SimSwapConsumer
from pipeline.ingestion.producer import RadiusLogProducer


def _log(msg: str) -> None:
    print(msg, flush=True)


def load_project_env() -> None:
    dotenv_path = os.path.join(PROJECT_ROOT, ".env")
    if os.path.isfile(dotenv_path):
        load_dotenv(dotenv_path=dotenv_path, override=True)
    else:
        load_dotenv(override=True)


def resolve_input_path(path: str) -> str:
    if os.path.isabs(path):
        abs_path = os.path.abspath(path)
    else:
        abs_path = os.path.abspath(os.path.join(PROJECT_ROOT, path))

    if not os.path.isfile(abs_path):
        raise FileNotFoundError(f"Input CSV not found: {abs_path}")

    return abs_path


async def ensure_kafka_topics(
    bootstrap_servers: str,
    topics: List[str],
    num_partitions: int = 4,
    replication_factor: int = 1,
) -> None:
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic
    from aiokafka.errors import TopicAlreadyExistsError

    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        new_topics = [
            NewTopic(
                name=t,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
            )
            for t in topics
        ]
        await admin.create_topics(new_topics)
        _log(f">>> Created Kafka topics: {topics}")
    except TopicAlreadyExistsError:
        _log(f">>> Kafka topics already exist: {topics}")
    except Exception as exc:
        _log(f">>> Kafka topic check warning: {exc}")
    finally:
        await admin.close()


async def run_pipeline_async(input_file: str = None, duration: int = None):
    load_project_env()
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")
    raw_topic = os.getenv("KAFKA_TOPIC_RAW", "radius.accounting.raw")

    _log("==================================================")
    _log("CAMARA RADIUS Pipeline Orchestrator (Refactored)")
    _log(f"Kafka Broker : {bootstrap_servers}")
    _log(f"Topic        : {raw_topic}")
    _log("==================================================")

    await ensure_kafka_topics(bootstrap_servers, [raw_topic], num_partitions=4)

    ip_consumer = IPMsisdnConsumer(topic=raw_topic, group_id="cg-ip-msisdn")
    device_consumer = DeviceSwapConsumer(topic=raw_topic, group_id="cg-device-swap")
    sim_consumer = SimSwapConsumer(topic=raw_topic, group_id="cg-sim-swap")

    _log(">>> Starting 3 parallel processing modules...")
    c1_task = asyncio.create_task(ip_consumer.run())
    c2_task = asyncio.create_task(device_consumer.run())
    c3_task = asyncio.create_task(sim_consumer.run())

    # Chờ consumer khởi động
    await asyncio.sleep(2)

    # Nếu có file CSV -> Ingest qua Stage 1
    if input_file:
        _log(f">>> [S1] Ingesting CSV file: {input_file}")
        producer = RadiusLogProducer(bootstrap_servers=bootstrap_servers, topic=raw_topic)
        t0 = time.time()
        count = await producer.publish_csv(input_file)
        await producer.stop()
        _log(f">>> [S1] Ingest completed: {count} records in {time.time() - t0:.2f}s")

    _log("\n>>> Pipeline running. 3 parallel modules active:")
    _log("   1. cg-ip-msisdn  -> IP-MSISDN Redis Table")
    _log("   2. cg-device-swap -> Device Swap History & Callbacks")
    _log("   3. cg-sim-swap    -> SIM Swap History & Callbacks\n")

    # Initialize tracking variables for throughput calculation
    last_ip = 0
    last_device = 0
    last_sim = 0
    last_time = time.time()

    async def log_heartbeat():
        nonlocal last_ip, last_device, last_sim, last_time
        now = time.time()
        dt = max(now - last_time, 1e-6)

        curr_ip = ip_consumer.metrics.get('processed')
        curr_device = device_consumer.metrics.get('processed')
        curr_sim = sim_consumer.metrics.get('processed')

        ip_tput = (curr_ip - last_ip) / dt
        device_tput = (curr_device - last_device) / dt
        sim_tput = (curr_sim - last_sim) / dt

        _log(
            f"   [Heartbeat]\n"
            f"     * cg-ip-msisdn  : {curr_ip:,} msgs processed | throughput: {ip_tput:.0f} rec/s\n"
            f"     * cg-device-swap: {curr_device:,} msgs processed | throughput: {device_tput:.0f} rec/s | swaps: {device_consumer.metrics.get('events_detected'):,} detected\n"
            f"     * cg-sim-swap    : {curr_sim:,} msgs processed | throughput: {sim_tput:.0f} rec/s | swaps: {sim_consumer.metrics.get('events_detected'):,} detected"
        )

        last_ip = curr_ip
        last_device = curr_device
        last_sim = curr_sim
        last_time = now

    if duration:
        _log(f">>> Running for {duration} seconds...")
        elapsed = 0
        interval = 5
        try:
            while elapsed < duration:
                sleep_time = min(interval, duration - elapsed)
                await asyncio.sleep(sleep_time)
                elapsed += sleep_time
                await log_heartbeat()
        except asyncio.CancelledError:
            pass
        _log(">>> Time limit reached. Stopping consumers...")
        ip_consumer.running = False
        device_consumer.running = False
        sim_consumer.running = False
    else:
        # Loop until interrupted
        try:
            while True:
                await asyncio.sleep(5)
                await log_heartbeat()
        except asyncio.CancelledError:
            pass

    await asyncio.gather(c1_task, c2_task, c3_task, return_exceptions=True)
    _log(">>> All pipeline modules stopped successfully.")


def main():
    parser = argparse.ArgumentParser(description="CAMARA RADIUS Pipeline Orchestrator")
    parser.add_argument("--input", help="CSV input file to ingest into Kafka")
    parser.add_argument("--duration", type=int, help="Run duration in seconds (optional)")
    args = parser.parse_args()

    input_path = resolve_input_path(args.input) if args.input else None

    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run_pipeline_async(input_file=input_path, duration=args.duration))
    except KeyboardInterrupt:
        _log("\n>>> Interrupt received. Shutting down pipeline...")
    finally:
        _log(">>> Pipeline exit.")


if __name__ == "__main__":
    main()
