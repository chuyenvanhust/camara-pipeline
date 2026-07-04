#!/usr/bin/env python3
#pipeline\pipeline\ingestion\producer.py
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
KAFKA_TOPIC_RAW         = os.getenv("KAFKA_TOPIC_RAW",         "radius.raw")


INGESTION_BATCH_SIZE_BYTES = int(os.getenv("INGESTION_BATCH_SIZE_BYTES", 256 * 1024))


INGESTION_LINGER_MS = int(os.getenv("INGESTION_LINGER_MS", 50))


INGESTION_ACKS = os.getenv("INGESTION_ACKS", "1")
if INGESTION_ACKS not in ("0", "1", "all"):
    INGESTION_ACKS = "1"
elif INGESTION_ACKS in ("0", "1"):
    INGESTION_ACKS = int(INGESTION_ACKS)


INGESTION_COMPRESSION_TYPE = os.getenv("INGESTION_COMPRESSION_TYPE", "none")
if INGESTION_COMPRESSION_TYPE.lower() in ("none", "", "null"):
    INGESTION_COMPRESSION_TYPE = None


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
            retry_backoff_ms=500,
        )
        await self._producer.start()
        logger.info(
            "Producer started | batch=%dKB linger=%dms compression=%s acks=%s "
            "flush_every=%d records",
            INGESTION_BATCH_SIZE_BYTES // 1024, INGESTION_LINGER_MS,
            INGESTION_COMPRESSION_TYPE, INGESTION_ACKS, FLUSH_EVERY_N_RECORDS,
        )

    async def stop(self):
        if self._producer:
            await self._producer.stop()

    async def publish_csv(self, file_path: str) -> int:
        """
        Doc CSV va day toan bo records len radius.raw.

        Chien luoc "fire, collect, flush theo chu ky":
            - Gui record (await send() chi cho toi khi message duoc dua
              vao accumulator noi bo -- nhanh, KHONG doi ack tu broker).
            - Gom Future tra ve vao 1 buffer nho.
            - Cu moi FLUSH_EVERY_N_RECORDS record: gather() cac Future do
              (doi ack thuc su), xoa buffer, log tien do + toc do hien tai.
            - flush() cuoi cung dam bao phan con lai (chua du 1 chu ky)
              cung duoc gui het truoc khi ham tra ve.

        Neu goi gather() cho TAT CA record chi 1 lan o cuoi (nhu cach lam
        don gian), buffer futures se phinh to theo kich thuoc file va
        khong co progress log giua chung -- khong phu hop voi file lon.
        """
        if not self._producer:
            await self.start()

        reader = LocalCSVReader(file_path)
        count = 0
        pending: List[asyncio.Future] = []
        t_start = time.time()
        t_last_log = t_start

        for record in reader.read_records():
            partition_key = record.get("msisdn", "")
            fut = await self._producer.send(
                topic=self.topic,
                key=partition_key,
                value=record,
            )
            pending.append(fut)
            count += 1

            if count % FLUSH_EVERY_N_RECORDS == 0:
                await asyncio.gather(*pending, return_exceptions=True)
                pending.clear()

                now = time.time()
                window_rate = FLUSH_EVERY_N_RECORDS / max(now - t_last_log, 1e-6)
                overall_rate = count / max(now - t_start, 1e-6)
                logger.info(
                    "Produced %d records | window=%.0f rec/s overall=%.0f rec/s",
                    count, window_rate, overall_rate,
                )
                t_last_log = now

        # Gui not phan con lai chua du 1 chu ky flush
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
            pending.clear()

       
        await self._producer.flush()

        duration = time.time() - t_start
        logger.info(
            "S1 Finished: %d records in %.2fs (%.0f rec/s overall)",
            count, duration, count / max(duration, 1e-6),
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