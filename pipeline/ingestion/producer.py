#!/usr/bin/env python3
#pipeline\pipeline\ingestion\producer.py
# Bootstrap: thêm thư mục gốc project vào sys.path để
# `import pipeline.*` hoạt động khi chạy trực tiếp với python3 script.py
import sys, os as _os
sys.path.insert(0, _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "..", "..")))

"""
Stage 1 — CSV → radius.accounting.raw

Đọc file CSV từ đường dẫn truyền vào, stream từng record lên Kafka topic
`radius.accounting.raw`. Đây là điểm đầu vào duy nhất của pipeline.

Input : file CSV (các trường: acct_status_type, acct_session_id, msisdn,
        imsi, imei, event_timestamp, ...)
Output: Kafka topic radius.accounting.raw (JSON)


===========================================================================
"""

import os
import json
import asyncio
import logging
import time
from typing import List

from dotenv import load_dotenv
from aiokafka import AIOKafkaProducer
from pipeline.ingestion.csv_reader import LocalCSVReader
from pipeline.ingestion.packet_reader import PacketReader

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ==============================================================================
# CAU HINH (tunable qua env, co default hop ly cho may laptop + Docker)
# ==============================================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")
KAFKA_TOPIC_RAW         = os.getenv("KAFKA_TOPIC_RAW",         "radius.accounting.raw")



INGESTION_BATCH_SIZE_BYTES = int(os.getenv("INGESTION_BATCH_SIZE_BYTES", 256 * 1024))


INGESTION_LINGER_MS = int(os.getenv("INGESTION_LINGER_MS", 50))

INGESTION_ACKS = os.getenv("INGESTION_ACKS", "all")
if INGESTION_ACKS not in ("0", "1", "all"):
    INGESTION_ACKS = "all"
elif INGESTION_ACKS in ("0", "1"):
    INGESTION_ACKS = int(INGESTION_ACKS)


INGESTION_COMPRESSION_TYPE = os.getenv("INGESTION_COMPRESSION_TYPE", "lz4")
if INGESTION_COMPRESSION_TYPE.lower() in ("none", "", "null"):
    INGESTION_COMPRESSION_TYPE = None


ENABLE_IDEMPOTENCE = os.getenv("INGESTION_ENABLE_IDEMPOTENCE", "true").lower() == "true"

INGESTION_MAX_REQUEST_SIZE = int(
    os.getenv("INGESTION_MAX_REQUEST_SIZE", max(INGESTION_BATCH_SIZE_BYTES * 2, 1024 * 1024))
)

FLUSH_EVERY_N_RECORDS = int(os.getenv("INGESTION_FLUSH_EVERY_N_RECORDS", 10_000))


class RadiusLogProducer:
    def __init__(
        self,
        bootstrap_servers: str = None,
        topic: str = None,
    ):
        self.bootstrap_servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self.topic             = topic             or KAFKA_TOPIC_RAW
        self._producer         = None
        self._metrics_failed   = 0  

    async def start(self):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,

            
            max_batch_size=INGESTION_BATCH_SIZE_BYTES,
            linger_ms=INGESTION_LINGER_MS,
            compression_type=INGESTION_COMPRESSION_TYPE,
            max_request_size=INGESTION_MAX_REQUEST_SIZE,
            acks=INGESTION_ACKS,
            enable_idempotence=ENABLE_IDEMPOTENCE,  
            retry_backoff_ms=500,
        )
        await self._producer.start()
        logger.info(
            "Producer started | batch=%dKB linger=%dms compression=%s acks=%s "
            "idempotence=%s flush_every=%d records",
            INGESTION_BATCH_SIZE_BYTES // 1024, INGESTION_LINGER_MS,
            INGESTION_COMPRESSION_TYPE, INGESTION_ACKS,
            ENABLE_IDEMPOTENCE, FLUSH_EVERY_N_RECORDS,
        )

    async def stop(self):
        if self._producer:
            await self._producer.stop()

    async def publish_csv(self, file_path: str) -> int:
        """
        Doc CSV va day toan bo records len radius.raw.

        
        - Reject record thiếu msisdn (partition key rỗng phá ordering)
        - Kiểm tra kết quả gather() thay vì nuốt exception
        - Báo lỗi rõ ràng nếu có record thất bại
        """
        if not self._producer:
            await self.start()

        reader = LocalCSVReader(file_path)
        count = 0
        skipped_count = 0
        self._metrics_failed = 0
        pending: List[asyncio.Future] = []
        t_start = time.time()
        t_last_log = t_start

        for record in reader.read_records():
           
            partition_key = record.get("msisdn")
            if not partition_key:
                logger.warning(
                    "[S1] Bỏ qua record thiếu msisdn, không đảm bảo được partition ordering: %s",
                    {k: record.get(k) for k in ("acct_status_type", "acct_session_id", "timestamp")},
                )
                skipped_count += 1
                continue

            fut = await self._producer.send(
                topic=self.topic,
                key=partition_key,
                value=record,
            )
            pending.append(fut)
            count += 1

            if count % FLUSH_EVERY_N_RECORDS == 0:
                
                results = await asyncio.gather(*pending, return_exceptions=True)
                failed = [r for r in results if isinstance(r, Exception)]
                if failed:
                    self._metrics_failed += len(failed)
                    logger.error(
                        "[S1] %d/%d record trong batch gửi thất bại (ví dụ lỗi đầu: %s)",
                        len(failed), len(results), failed[0],
                    )
                pending.clear()

                now = time.time()
                window_rate = FLUSH_EVERY_N_RECORDS / max(now - t_last_log, 1e-6)
                overall_rate = count / max(now - t_start, 1e-6)
                logger.info(
                    
                    "[S1-THROUGHPUT] Produced %d records | window=%.0f rec/s overall=%.0f rec/s",
                    count, window_rate, overall_rate,
                )
                t_last_log = now

        if pending:
            results = await asyncio.gather(*pending, return_exceptions=True)
            failed = [r for r in results if isinstance(r, Exception)]
            if failed:
                self._metrics_failed += len(failed)
                logger.error(
                    "[S1] %d/%d record trong batch cuối gửi thất bại (ví dụ lỗi đầu: %s)",
                    len(failed), len(results), failed[0],
                )
            pending.clear()

       
        await self._producer.flush()

        duration = time.time() - t_start
        
        if self._metrics_failed > 0:
            logger.error(
                "[S1] Ingest hoàn tất NHƯNG có %d record gửi thất bại + %d record bị skip (thiếu msisdn) "
                "— cần kiểm tra trước khi coi là an toàn.",
                self._metrics_failed, skipped_count,
            )
        else:
            logger.info(
                "[S1-THROUGHPUT] S1 Finished: %d records in %.2fs (%.0f rec/s overall), %d skipped",
                count, duration, count / max(duration, 1e-6), skipped_count,
            )
        return count

    async def publish_packets(self, port: int = 1813):
        """
        Lắng nghe UDP packets và stream lên Kafka theo thời gian thực.
        """
        if not self._producer:
            await self.start()

        reader = PacketReader()
        count = 0
        t_start = time.time()
        
        logger.info("[S1] Starting UDP Packet Ingestion on port %d...", port)

        # Vì reader.listen_radius_packets là một generator đồng bộ (blocking IO)
        # Trong môi trường production, ta nên chạy nó trong thread hoặc dùng non-blocking socket.
        # Ở đây ta sử dụng loop.run_in_executor để không làm nghẽn event loop của asyncio.
        
        loop = asyncio.get_event_loop()
        
        # Hàm wrapper để chạy generator trong background
        def get_packets():
            return reader.listen_radius_packets(port=port)

        # Chạy generator
        for record in await loop.run_in_executor(None, get_packets):
            partition_key = record.get("msisdn") or record.get("Calling_Station_Id")
            
            if not partition_key:
                # Đối với gói tin RADIUS thật, nếu không có MSISDN ta có thể dùng Acct-Session-Id làm key tạm
                partition_key = record.get("acct_session_id", "unknown")

            try:
                # Gửi lên Kafka
                await self._producer.send(
                    topic=self.topic,
                    key=str(partition_key),
                    value=record
                )
                count += 1
                
                # In log định kỳ mỗi 1000 gói tin
                if count % 1000 == 0:
                    elapsed = time.time() - t_start
                    logger.info("[S1-LIVE] Streamed %d packets | Rate: %.0f pkts/s", count, count/elapsed)
            
            except Exception as e:
                logger.error("[S1] Failed to send packet to Kafka: %s", e)


# ---------------------------------------------------------------------------
# Entry-point CLI
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Stage 1: Ingestion (CSV or UDP) → Kafka")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--file", help="Đường dẫn tới file CSV đầu vào")
    group.add_argument("--udp", action="store_true", help="Chế độ lắng nghe UDP (Packet Reader)")
    
    parser.add_argument("--port", type=int, default=1813, help="Cổng UDP (mặc định 1813)")
    
    args = parser.parse_args()

    producer = RadiusLogProducer()
    loop = asyncio.get_event_loop()
    
    try:
        if args.file:
            # Chế độ đọc file CSV
            n = loop.run_until_complete(producer.publish_csv(args.file))
            print(f"[S1] Hoàn tất CSV: Đã đẩy {n} records.")
        else:
            # Chế độ lắng nghe UDP
            loop.run_until_complete(producer.publish_packets(port=args.port))
            
    except KeyboardInterrupt:
        print("\n[!] Stopped by user.")
    finally:
        if producer._producer is not None:
            loop.run_until_complete(producer.stop())