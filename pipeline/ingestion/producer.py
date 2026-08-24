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

# F-04: Default acks="all" cho durability, cho phép override qua env cho dev/lab
INGESTION_ACKS = os.getenv("INGESTION_ACKS", "all")
if INGESTION_ACKS not in ("0", "1", "all"):
    INGESTION_ACKS = "all"
elif INGESTION_ACKS in ("0", "1"):
    INGESTION_ACKS = int(INGESTION_ACKS)

# F-04: Default compression=lz4 cho bandwidth efficiency
INGESTION_COMPRESSION_TYPE = os.getenv("INGESTION_COMPRESSION_TYPE", "lz4")
if INGESTION_COMPRESSION_TYPE.lower() in ("none", "", "null"):
    INGESTION_COMPRESSION_TYPE = None

# F-04: Enable idempotence by default khi acks=all
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
        self._metrics_failed   = 0  # F-04: Track failed records across flush cycles

    async def start(self):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,

            # --- CAU HINH HIEU NANG (xem giai thich o dau file) ---
            max_batch_size=INGESTION_BATCH_SIZE_BYTES,
            linger_ms=INGESTION_LINGER_MS,
            compression_type=INGESTION_COMPRESSION_TYPE,
            max_request_size=INGESTION_MAX_REQUEST_SIZE,
            acks=INGESTION_ACKS,
            enable_idempotence=ENABLE_IDEMPOTENCE,  # F-04: tránh duplicate do retry
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

        F-04 changes:
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
            # F-14: Reject record thiếu msisdn — không gửi với key rỗng
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
                # F-04: Kiểm tra thật kết quả gather thay vì nuốt exception
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

        # Gui not phan con lai chua du 1 chu ky flush
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
        # F-04: Log rõ ràng nếu có record thất bại
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