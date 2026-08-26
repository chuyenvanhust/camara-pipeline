from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List

from aiokafka import AIOKafkaProducer

from pipeline.ingestion.csv_reader import LocalCSVReader
from pipeline.ingestion.packet_reader import PacketReader
from pipeline.modules.shared.events import InvalidMessageError, canonical_msisdn, parse_event_time

logger = logging.getLogger(__name__)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "radius.accounting.raw")
FLUSH_EVERY_N_RECORDS = int(os.getenv("INGESTION_FLUSH_EVERY_N_RECORDS", "1000"))


class RadiusLogProducer:
    def __init__(self, bootstrap_servers: str | None = None, topic: str | None = None):
        self.bootstrap_servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self.topic = topic or KAFKA_TOPIC_RAW
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
            key_serializer=lambda value: value.encode("utf-8") if value else None,
            max_batch_size=int(os.getenv("INGESTION_BATCH_SIZE_BYTES", str(256 * 1024))),
            linger_ms=int(os.getenv("INGESTION_LINGER_MS", "20")),
            compression_type=os.getenv("INGESTION_COMPRESSION_TYPE", "lz4"),
            max_request_size=int(os.getenv("INGESTION_MAX_REQUEST_SIZE", str(1024 * 1024))),
            acks="all", enable_idempotence=True, retry_backoff_ms=500,
        )
        await self._producer.start()
        logger.info("Kafka producer ready (acks=all, idempotence=true)")

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    @staticmethod
    def _normalize(record: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
        msisdn = canonical_msisdn(record)
        occurred_at = parse_event_time(record)
        normalized = dict(record)
        normalized["msisdn"] = msisdn
        normalized["event_timestamp"] = occurred_at.isoformat()
        return msisdn, normalized

    async def _dlq(self, payload: Dict[str, Any], error: Exception, source: str) -> None:
        assert self._producer is not None
        await self._producer.send_and_wait(
            f"{self.topic}.dlq",
            value={"producer": "radius-ingestion", "source": source,
                   "error_type": type(error).__name__, "error": str(error), "payload": payload},
        )

    async def publish_csv(self, file_path: str) -> int:
        if self._producer is None:
            await self.start()
        assert self._producer is not None
        acknowledged = rejected = 0
        pending: List[asyncio.Future] = []
        started = time.monotonic()
        for record in LocalCSVReader(file_path).read_records():
            try:
                key, normalized = self._normalize(record)
            except InvalidMessageError as exc:
                await self._dlq(record, exc, file_path)
                rejected += 1
                continue
            pending.append(await self._producer.send(self.topic, key=key, value=normalized))
            if len(pending) >= FLUSH_EVERY_N_RECORDS:
                await asyncio.gather(*pending)
                acknowledged += len(pending)
                pending.clear()
        if pending:
            await asyncio.gather(*pending)
            acknowledged += len(pending)
        await self._producer.flush()
        logger.info("CSV ingestion acknowledged=%d rejected=%d duration=%.2fs",
                    acknowledged, rejected, time.monotonic() - started)
        return acknowledged

    async def publish_packets(self, port: int = 1813) -> None:
        if self._producer is None:
            await self.start()
        assert self._producer is not None
        reader = PacketReader()
        acknowledged = 0
        async for record in reader.listen_radius_packets(port):
            try:
                key, normalized = self._normalize(record)
            except InvalidMessageError as exc:
                await self._dlq(record, exc, f"udp:{port}")
                continue
            await self._producer.send_and_wait(self.topic, key=key, value=normalized)
            acknowledged += 1
            if acknowledged % 1000 == 0:
                logger.info("UDP packets acknowledged by Kafka: %d", acknowledged)


async def _run(args: argparse.Namespace) -> None:
    producer = RadiusLogProducer()
    try:
        if args.file:
            count = await producer.publish_csv(args.file)
            print(f"Acknowledged {count} CSV records")
        else:
            await producer.publish_packets(args.port)
    finally:
        await producer.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="RADIUS CSV/UDP to Kafka ingestion")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--file")
    source.add_argument("--udp", action="store_true")
    parser.add_argument("--port", type=int, default=1813)
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    main()
