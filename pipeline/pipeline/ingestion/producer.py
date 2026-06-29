#!/usr/bin/env python3
# Bootstrap: thêm thư mục gốc project vào sys.path để
# `import pipeline.*` hoạt động khi chạy trực tiếp với python3 script.py
import sys, os as _os
sys.path.insert(0, _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "..", "..")))

"""
Stage 1 — CSV → radius.raw

Đọc file CSV từ đường dẫn truyền vào, stream từng record lên Kafka topic
`radius.raw`. Đây là điểm đầu vào duy nhất của pipeline.

Input : file CSV (các trường: acct_status_type, acct_session_id, msisdn,
        imsi, imei, event_timestamp, ...)
Output: Kafka topic radius.raw (JSON)
"""

import os
import json
import asyncio

from dotenv import load_dotenv
from aiokafka import AIOKafkaProducer
from pipeline.ingestion.csv_reader import LocalCSVReader

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")
KAFKA_TOPIC_RAW         = os.getenv("KAFKA_TOPIC_RAW",         "radius.raw")
INGESTION_BATCH_SIZE    = int(os.getenv("INGESTION_BATCH_SIZE", 500))
INGESTION_LINGER_MS     = int(os.getenv("INGESTION_LINGER_MS",  10))


class RadiusLogProducer:
    def __init__(
        self,
        bootstrap_servers: str = None,
        topic: str = None,
    ):
        self.bootstrap_servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self.topic             = topic             or KAFKA_TOPIC_RAW
        self.batch_size        = INGESTION_BATCH_SIZE
        self.linger_ms         = INGESTION_LINGER_MS
        self._producer         = None

    async def start(self):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            max_batch_size=self.batch_size,
            linger_ms=self.linger_ms,
        )
        await self._producer.start()

    async def stop(self):
        if self._producer:
            await self._producer.stop()

    async def publish_csv(self, file_path: str) -> int:
        """Đọc CSV và đẩy toàn bộ records lên radius.raw."""
        if not self._producer:
            await self.start()

        reader = LocalCSVReader(file_path)
        count = 0
        for record in reader.read_records():
            partition_key = record.get("msisdn", "")
            await self._producer.send(
                topic=self.topic,
                key=partition_key,
                value=record,
            )
            count += 1
            if count % 10_000 == 0:
                await asyncio.sleep(0.01)   # nhường CPU
        return count


# ---------------------------------------------------------------------------
# Entry-point CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Stage 1: CSV → radius.raw")
    parser.add_argument("--file", required=True, help="Đường dẫn tới file CSV đầu vào")
    args = parser.parse_args()

    producer = RadiusLogProducer()
    loop = asyncio.get_event_loop()
    try:
        n = loop.run_until_complete(producer.publish_csv(args.file))
        print(f"[S1] Đã đẩy {n} records lên topic '{producer.topic}'.")
    finally:
        if producer._producer is not None:
            loop.run_until_complete(producer.stop())
