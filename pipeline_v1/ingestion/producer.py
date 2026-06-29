#!/usr/bin/env python3
#pipeline\ingestion\producer.py
import os
import json
import asyncio
from dotenv import load_dotenv
from aiokafka import AIOKafkaProducer
from pipeline_v1.ingestion.csv_reader import LocalCSVReader

load_dotenv()

class RadiusLogProducer:
    def __init__(self, bootstrap_servers: str = None, topic: str = None):
        # Ưu tiên tham số truyền vào trực tiếp, nếu không có mới bốc từ .env/mặc định
        self.bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")
        self.topic = topic or os.getenv("KAFKA_TOPIC_RAW", "radius.raw")
        self.batch_size = int(os.getenv("INGESTION_BATCH_SIZE", 500))
        self.linger_ms = int(os.getenv("INGESTION_LINGER_MS", 10))
        self.producer = None

    async def start(self):
        """Khởi tạo kết nối bất đồng bộ tới Kafka Broker Cluster"""
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            max_batch_size=self.batch_size,
            linger_ms=self.linger_ms
        )
        await self.producer.start()

    async def stop(self):
        """Ngắt kết nối an toàn, flush toàn bộ message còn tồn trong queue"""
        if self.producer:
            await self.producer.stop()

    async def publish_csv_to_kafka(self, file_path: str) -> int:
        """Đọc tệp tin CSV từ simulator và stream trực tiếp lên Kafka topic"""
        if not self.producer:
            await self.start()
            
        reader = LocalCSVReader(file_path)
        count = 0
        
        for record in reader.read_records():
            partition_key = record.get("msisdn", "")
            await self.producer.send(
                topic=self.topic,
                key=partition_key,
                value=record
            )
            count += 1
            
            if count % 10000 == 0:
                await asyncio.sleep(0.01)
                
        return count

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Kafka CSV Producer")
    parser.add_argument(
        "--file",
        required=True,
        help="Path to CSV file"
    )

    args = parser.parse_args()

    producer = RadiusLogProducer()

    print(f"Starting ingestion for file: {args.file}")

    loop = asyncio.get_event_loop()

    try:
        inserted = loop.run_until_complete(
            producer.publish_csv_to_kafka(args.file)
        )
        print(
            f"Successfully ingested {inserted} records into Kafka topic '{producer.topic}'."
        )
    finally:
        if producer.producer is not None:
            loop.run_until_complete(producer.stop())