from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from aiokafka import AIOKafkaProducer

from pipeline.ingestion.csv_reader import LocalCSVReader
from pipeline.ingestion.packet_reader import PacketReader, RadiusEnvelope
from pipeline.modules.shared.events import InvalidMessageError, canonical_msisdn, parse_event_time

logger = logging.getLogger(__name__)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "radius.accounting.raw")
FLUSH_EVERY_N_RECORDS = int(os.getenv("INGESTION_FLUSH_EVERY_N_RECORDS", "1000"))
THROUGHPUT_LOG_INTERVAL_SECONDS = float(os.getenv("THROUGHPUT_LOG_INTERVAL_SECONDS", "10"))
UDP_QUEUE_MAX_RECORDS = int(os.getenv("RADIUS_UDP_QUEUE_MAX_RECORDS", "100000"))
UDP_KAFKA_BATCH_RECORDS = int(os.getenv("RADIUS_UDP_KAFKA_BATCH_RECORDS", "500"))
UDP_KAFKA_BATCH_WAIT_MS = int(os.getenv("RADIUS_UDP_KAFKA_BATCH_WAIT_MS", "10"))
UDP_KAFKA_MAX_INFLIGHT_BATCHES = int(os.getenv("RADIUS_UDP_KAFKA_MAX_INFLIGHT_BATCHES", "8"))
RADIUS_ACK_CACHE_MAX_RECORDS = int(os.getenv("RADIUS_ACK_CACHE_MAX_RECORDS", "500000"))
RADIUS_ACK_CACHE_TTL_SECONDS = float(os.getenv("RADIUS_ACK_CACHE_TTL_SECONDS", "120"))


@dataclass(frozen=True)
class QueueItem:
    topic: str
    key: Optional[str]
    value: Dict[str, Any]
    kind: str
    envelope: RadiusEnvelope | None = None


class RadiusLogProducer:
    def __init__(self, bootstrap_servers: str | None = None, topic: str | None = None):
        if min(UDP_QUEUE_MAX_RECORDS, UDP_KAFKA_BATCH_RECORDS, UDP_KAFKA_MAX_INFLIGHT_BATCHES) < 1:
            raise ValueError("RADIUS UDP queue and batch sizes must be positive")
        self.bootstrap_servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self.topic = topic or KAFKA_TOPIC_RAW
        self._producer: AIOKafkaProducer | None = None
        self._telemetry_task: asyncio.Task | None = None
        self._queue: asyncio.Queue[QueueItem] | None = None
        self._packet_reader: PacketReader | None = None
        self._source = "not-started"
        self._counts = {
            "received": 0, "queued": 0, "acknowledged": 0, "rejected": 0,
            "dlq": 0, "queue_dropped": 0, "publish_failed": 0,
            "kafka_batches": 0, "queue_high_watermark": 0,
            "last_batch_records": 0, "last_batch_ms": 0.0,
            "last_batch_rate": 0.0,
            "responses_sent": 0, "responses_failed": 0,
            "duplicates_acked": 0, "duplicates_inflight": 0,
            "responses_withheld": 0,
        }
        self._radius_inflight: set[str] = set()
        self._radius_ack_cache: OrderedDict[str, float] = OrderedDict()

    async def start(self) -> None:
        compression = os.getenv("INGESTION_COMPRESSION_TYPE", "lz4").strip().lower()
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
            key_serializer=lambda value: value.encode("utf-8") if value else None,
            max_batch_size=int(os.getenv("INGESTION_BATCH_SIZE_BYTES", str(256 * 1024))),
            linger_ms=int(os.getenv("INGESTION_LINGER_MS", "20")),
            compression_type=None if compression in {"", "none", "null"} else compression,
            max_request_size=int(os.getenv("INGESTION_MAX_REQUEST_SIZE", str(1024 * 1024))),
            acks="all", enable_idempotence=True, retry_backoff_ms=500,
        )
        await self._producer.start()
        self._telemetry_task = asyncio.create_task(self._log_throughput(), name="producer-telemetry")
        logger.info("Kafka producer ready (single producer, acks=all, idempotence=true)")

    async def stop(self) -> None:
        if self._telemetry_task is not None:
            self._telemetry_task.cancel()
            await asyncio.gather(self._telemetry_task, return_exceptions=True)
            self._telemetry_task = None
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None
        logger.info(
            "[INGESTION][FINAL] source=%s received=%d kafka_acked=%d dropped=%d "
            "radius_responses=%d duplicate_responses=%d response_failed=%d rejected=%d dlq=%d publish_failed=%d",
            self._source, self._counts["received"], self._counts["acknowledged"],
            self._counts["queue_dropped"], self._counts["responses_sent"],
            self._counts["duplicates_acked"], self._counts["responses_failed"],
            self._counts["rejected"], self._counts["dlq"], self._counts["publish_failed"],
        )

    async def _log_throughput(self) -> None:
        previous = dict(self._counts)
        interval = THROUGHPUT_LOG_INTERVAL_SECONDS
        while True:
            await asyncio.sleep(interval)
            current = dict(self._counts)
            queue_depth = self._queue.qsize() if self._queue is not None else 0
            reader_stats = self._packet_reader.stats if self._packet_reader is not None else {}
            input_rate = (current["received"] - previous["received"]) / interval
            kafka_rate = (current["acknowledged"] - previous["acknowledged"]) / interval
            dropped_window = current["queue_dropped"] - previous["queue_dropped"]
            queue_percent = queue_depth * 100 / UDP_QUEUE_MAX_RECORDS
            loss_percent = current["queue_dropped"] * 100 / max(current["received"], 1)
            status = "OVERLOAD" if dropped_window else "PRESSURE" if queue_percent >= 70 else "OK"
            level = logging.WARNING if status != "OK" else logging.INFO
            logger.log(
                level,
                "[INGESTION][%s] window=%.0fs input=%.1f/s kafka_ack=%.1f/s "
                "queue=%d/%d(%.1f%%) inflight_radius=%d dropped_window=%d dropped_total=%d(%.4f%%) "
                "radius_response_total=%d duplicate_acked=%d duplicate_waiting=%d response_failed=%d "
                "rejected=%d dlq=%d publish_failed=%d totals(received=%d,kafka_acked=%d)",
                status, interval, input_rate, kafka_rate,
                queue_depth, UDP_QUEUE_MAX_RECORDS, queue_percent, len(self._radius_inflight),
                dropped_window, current["queue_dropped"], loss_percent,
                current["responses_sent"], current["duplicates_acked"], current["duplicates_inflight"],
                current["responses_failed"], reader_stats.get("rejected", 0), current["dlq"],
                current["publish_failed"], reader_stats.get("datagrams", current["received"]),
                current["acknowledged"],
            )
            previous = current

    @staticmethod
    def _normalize(record: Dict[str, Any]) -> tuple[str, Dict[str, Any]]:
        msisdn = canonical_msisdn(record)
        occurred_at = parse_event_time(record)
        normalized = dict(record)
        normalized["msisdn"] = msisdn
        normalized["event_timestamp"] = occurred_at.isoformat()
        return msisdn, normalized

    @staticmethod
    def _dlq_envelope(payload: Dict[str, Any], error: Exception, source: str) -> Dict[str, Any]:
        return {"producer": "radius-ingestion", "source": source,
                "error_type": type(error).__name__, "error": str(error), "payload": payload}

    async def _dlq(self, payload: Dict[str, Any], error: Exception, source: str) -> None:
        assert self._producer is not None
        await self._producer.send_and_wait(
            f"{self.topic}.dlq", value=self._dlq_envelope(payload, error, source)
        )
        self._counts["dlq"] += 1

    async def publish_csv(self, file_path: str) -> int:
        if self._producer is None:
            await self.start()
        assert self._producer is not None
        self._source = f"csv:{file_path}"
        acknowledged = rejected = 0
        pending: List[asyncio.Future] = []
        started = time.monotonic()
        for record in LocalCSVReader(file_path).read_records():
            self._counts["received"] += 1
            try:
                key, normalized = self._normalize(record)
            except InvalidMessageError as exc:
                await self._dlq(record, exc, file_path)
                rejected += 1
                self._counts["rejected"] += 1
                continue
            pending.append(await self._producer.send(self.topic, key=key, value=normalized))
            self._counts["queued"] += 1
            if len(pending) >= FLUSH_EVERY_N_RECORDS:
                await asyncio.gather(*pending)
                acknowledged += len(pending)
                self._counts["acknowledged"] += len(pending)
                self._counts["kafka_batches"] += 1
                pending.clear()
        if pending:
            await asyncio.gather(*pending)
            acknowledged += len(pending)
            self._counts["acknowledged"] += len(pending)
            self._counts["kafka_batches"] += 1
        await self._producer.flush()
        logger.info("CSV ingestion acknowledged=%d rejected=%d duration=%.2fs",
                    acknowledged, rejected, time.monotonic() - started)
        return acknowledged

    def _put_udp_item(self, item: QueueItem) -> bool:
        assert self._queue is not None
        try:
            self._queue.put_nowait(item)
            self._counts["queued"] += 1
            self._counts["queue_high_watermark"] = max(
                self._counts["queue_high_watermark"], self._queue.qsize()
            )
            return True
        except asyncio.QueueFull:
            self._counts["queue_dropped"] += 1
            self._counts["responses_withheld"] += 1
            return False

    def _cache_radius_ack(self, event_id: str) -> None:
        now = time.monotonic()
        self._radius_ack_cache[event_id] = now
        self._radius_ack_cache.move_to_end(event_id)
        while len(self._radius_ack_cache) > RADIUS_ACK_CACHE_MAX_RECORDS:
            self._radius_ack_cache.popitem(last=False)

    def _is_radius_duplicate(self, event_id: str) -> bool:
        now = time.monotonic()
        while self._radius_ack_cache:
            oldest_id, created_at = next(iter(self._radius_ack_cache.items()))
            if now - created_at <= RADIUS_ACK_CACHE_TTL_SECONDS:
                break
            self._radius_ack_cache.pop(oldest_id, None)
        return event_id in self._radius_ack_cache

    async def _send_radius_response(self, envelope: RadiusEnvelope, duplicate: bool = False) -> None:
        assert self._packet_reader is not None
        try:
            await self._packet_reader.send_accounting_response(envelope)
            self._counts["responses_sent"] += 1
            if duplicate:
                self._counts["duplicates_acked"] += 1
        except Exception:
            self._counts["responses_failed"] += 1
            logger.exception("Không gửi được RADIUS Accounting-Response tới %s", envelope.address)

    async def _receive_udp(self, reader: PacketReader, port: int) -> None:
        source = f"udp:{port}"
        async for envelope in reader.listen_radius_packets(port):
            self._counts["received"] += 1
            record = envelope.record
            if self._is_radius_duplicate(envelope.event_id):
                await self._send_radius_response(envelope, duplicate=True)
                continue
            if envelope.event_id in self._radius_inflight:
                self._counts["duplicates_inflight"] += 1
                continue
            self._radius_inflight.add(envelope.event_id)
            try:
                key, normalized = self._normalize(record)
                queued = self._put_udp_item(QueueItem(self.topic, key, normalized, "raw", envelope))
            except InvalidMessageError as exc:
                self._counts["rejected"] += 1
                dlq_payload = self._dlq_envelope(record, exc, source)
                queued = self._put_udp_item(
                    QueueItem(f"{self.topic}.dlq", None, dlq_payload, "dlq", envelope)
                )
            if not queued:
                self._radius_inflight.discard(record.get("radius_event_id", ""))

    async def _next_kafka_batch(self) -> List[QueueItem]:
        assert self._queue is not None
        batch = [await self._queue.get()]
        deadline = asyncio.get_running_loop().time() + UDP_KAFKA_BATCH_WAIT_MS / 1000
        while len(batch) < UDP_KAFKA_BATCH_RECORDS:
            try:
                batch.append(self._queue.get_nowait())
                continue
            except asyncio.QueueEmpty:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                try:
                    batch.append(await asyncio.wait_for(self._queue.get(), timeout=remaining))
                except asyncio.TimeoutError:
                    break
        return batch

    async def _publish_udp_batches(self) -> None:
        assert self._producer is not None and self._queue is not None
        inflight: set[asyncio.Task] = set()

        async def acknowledge(batch: List[QueueItem], futures: List[asyncio.Future], started: float) -> None:
            try:
                await asyncio.gather(*futures)
                raw_count = sum(item.kind == "raw" for item in batch)
                self._counts["acknowledged"] += raw_count
                self._counts["dlq"] += len(batch) - raw_count
                self._counts["kafka_batches"] += 1
                elapsed = max(time.monotonic() - started, 1e-9)
                self._counts["last_batch_records"] = len(batch)
                self._counts["last_batch_ms"] = elapsed * 1000
                self._counts["last_batch_rate"] = len(batch) / elapsed
                response_items = [item for item in batch if item.envelope is not None]
                for item in response_items:
                    assert item.envelope is not None
                    self._radius_inflight.discard(item.envelope.event_id)
                    self._cache_radius_ack(item.envelope.event_id)
                if response_items:
                    await asyncio.gather(*(
                        self._send_radius_response(item.envelope)  # type: ignore[arg-type]
                        for item in response_items
                    ))
            except asyncio.CancelledError:
                raise
            except Exception:
                self._counts["publish_failed"] += len(batch)
                logger.exception("Kafka UDP batch publish failed records=%d", len(batch))
                raise
            finally:
                for _ in batch:
                    self._queue.task_done()

        try:
            while True:
                if len(inflight) >= UDP_KAFKA_MAX_INFLIGHT_BATCHES:
                    done, inflight = await asyncio.wait(inflight, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        task.result()

                batch = await self._next_kafka_batch()
                started = time.monotonic()
                futures = [
                    await self._producer.send(item.topic, key=item.key, value=item.value)
                    for item in batch
                ]
                task = asyncio.create_task(
                    acknowledge(batch, futures, started),
                    name=f"kafka-ack-{self._counts['kafka_batches'] + len(inflight) + 1}",
                )
                inflight.add(task)
        finally:
            if inflight:
                await asyncio.gather(*inflight, return_exceptions=True)

    async def publish_packets(self, port: int = 1813,
                              stop_event: asyncio.Event | None = None) -> None:
        if self._producer is None:
            await self.start()
        self._source = f"udp:{port}"
        self._queue = asyncio.Queue(maxsize=UDP_QUEUE_MAX_RECORDS)
        self._packet_reader = PacketReader()
        receiver = asyncio.create_task(self._receive_udp(self._packet_reader, port), name="udp-receiver")
        publisher = asyncio.create_task(self._publish_udp_batches(), name="kafka-batch-publisher")
        shutdown = asyncio.create_task(stop_event.wait(), name="ingestion-shutdown") if stop_event else None
        logger.info(
            "UDP ingestion pipeline ready queue_capacity=%d kafka_batch_records=%d "
            "kafka_batch_wait_ms=%d kafka_max_inflight_batches=%d",
            UDP_QUEUE_MAX_RECORDS, UDP_KAFKA_BATCH_RECORDS, UDP_KAFKA_BATCH_WAIT_MS,
            UDP_KAFKA_MAX_INFLIGHT_BATCHES,
        )
        try:
            watched = (receiver, publisher, shutdown) if shutdown else (receiver, publisher)
            done, _ = await asyncio.wait(watched, return_when=asyncio.FIRST_COMPLETED)
            if shutdown is not None and shutdown in done:
                receiver.cancel()
                await asyncio.gather(receiver, return_exceptions=True)
                logger.info("UDP receiver stopped; draining queue_depth=%d", self._queue.qsize())
                await asyncio.wait_for(self._queue.join(), timeout=20)
                return
            task = next(iter(done))
            raise task.exception() or RuntimeError(f"critical ingestion task {task.get_name()} exited")
        finally:
            receiver.cancel()
            publisher.cancel()
            if shutdown is not None:
                shutdown.cancel()
                await asyncio.gather(receiver, publisher, shutdown, return_exceptions=True)
            else:
                await asyncio.gather(receiver, publisher, return_exceptions=True)


async def _run(args: argparse.Namespace) -> None:
    producer = RadiusLogProducer()
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass
    try:
        if args.file:
            count = await producer.publish_csv(args.file)
            print(f"Acknowledged {count} CSV records")
        else:
            await producer.publish_packets(args.port, stop_event)
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
