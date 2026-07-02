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
[FIX HIEU NANG] Ban goc chi dat ~90 rec/s. Nguyen nhan goc:

    max_batch_size=self.batch_size  # = INGESTION_BATCH_SIZE = 500

`max_batch_size` cua aiokafka la SO BYTE toi da moi batch, KHONG PHAI so
record. Ten bien env "INGESTION_BATCH_SIZE=500" gay hieu lam la "500
record/batch", nhung thuc chat ep moi batch chi chua ~500 BYTE -- voi 1
record JSON RADIUS (~200-350 byte), moi batch chi nhet duoc 1-2 record.
Ket qua: gan nhu MOI RECORD 1 NETWORK ROUND-TRIP RIENG toi Kafka broker
trong Docker (overhead ao hoa network cua Docker Desktop thuong 3-10ms/
round-trip) -> ~90-150 rec/s, khop chinh xac voi trieu chung quan sat duoc.

FIX: dat max_batch_size dung don vi BYTE hop ly (mac dinh 256KB, config
duoc qua INGESTION_BATCH_SIZE_BYTES), ket hop linger_ms de gom batch that
su lon truoc khi gui, thay vi gui gan nhu tung record mot.

Cac cai tien khac ap dung trong ban nay:
    - compression_type="lz4" (KHONG dung "gzip"): tren mang Docker noi bo
      (localhost/bridge), bang thong gan nhu du thua -> nghen nam o CPU
      chu khong phai network. gzip la codec nen CHAM NHAT trong cac lua
      chon cua Kafka; nen CPU-bound tu gzip co the an het loi ich vua lay
      lai duoc tu viec sua batch size. lz4 nhanh hon nhieu, phu hop hon
      cho khoi luong lon tren may local.
    - Fire-and-collect futures: KHONG await cho ack tung record trong
      vong lap (se lam thong luong bi bound boi network RTT tung record),
      ma gom Future roi flush theo CHU KY (moi FLUSH_EVERY_N_RECORDS
      record) thay vi gom het toan bo file roi moi gather 1 lan -- gioi
      han bo nho khi file lon (hang trieu dong) va cho progress log thuc
      te thay vi im lang toi cuoi.
    - max_request_size dat khop voi max_batch_size de tranh loi khi batch
      gop lai vuot gioi han request mac dinh.
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

#: [FIX] Don vi BYTE, khong phai record. Mac dinh 256KB -- du lon de gom
#: hang tram record JSON RADIUS (~200-350 byte) vao 1 batch, giam manh so
#: network round-trip so voi ban goc (500 byte/batch).
INGESTION_BATCH_SIZE_BYTES = int(os.getenv("INGESTION_BATCH_SIZE_BYTES", 256 * 1024))

#: Thoi gian cho toi da (ms) de gom them record truoc khi gui 1 batch,
#: ke ca khi batch chua day. 50-100ms la muc can bang tot giua throughput
#: (gom duoc nhieu record/batch) va latency (khong cho qua lau).
INGESTION_LINGER_MS = int(os.getenv("INGESTION_LINGER_MS", 50))

#: acks=1: chi can leader broker xac nhan da nhan (khong doi toan bo ISR
#: replicate). Phu hop cho pipeline ingest hang loat tren may local,
#: single-broker -- doi acks="all" khong tang do tin cay (chi co 1 broker)
#: nhung se cham hon dang ke.
INGESTION_ACKS = os.getenv("INGESTION_ACKS", "1")
if INGESTION_ACKS not in ("0", "1", "all"):
    INGESTION_ACKS = "1"
elif INGESTION_ACKS in ("0", "1"):
    INGESTION_ACKS = int(INGESTION_ACKS)

#: [FIX] "lz4" can thu vien native rieng (python-lz4) MA CONTAINER
#: MAC DINH KHONG CO -- se lam AIOKafkaProducer bao
#: "RuntimeError: Compression library for lz4 not found" ngay luc start().
#: "gzip" thi luon co san (dung module chuan cua Python) nhung LA CODEC
#: CHAM NHAT, de bien CPU thanh nut that (xem giai thich dau file).
#: -> Mac dinh AN TOAN: KHONG NEN (None). Tren mang Docker noi bo bang
#: thong gan nhu du thua, nen loi ich cua nen (giam byte truyen) khong
#: bu duoc chi phi CPU/dependency. Neu muon dung lz4, phai
#: `pip install lz4` (hoac python-lz4) trong image truoc, roi moi set
#: INGESTION_COMPRESSION_TYPE=lz4.
INGESTION_COMPRESSION_TYPE = os.getenv("INGESTION_COMPRESSION_TYPE", "none")
if INGESTION_COMPRESSION_TYPE.lower() in ("none", "", "null"):
    INGESTION_COMPRESSION_TYPE = None

#: [KHONG DUNG] aiokafka KHONG co tham so "buffer_memory" trong constructor
#: cua AIOKafkaProducer (day la ten tham so cua client Java / kafka-python,
#: truyen vao se bao TypeError: unexpected keyword argument). Bo dem noi
#: bo cua aiokafka duoc kiem soat gian tiep qua max_batch_size + so luong
#: batch dang cho gui, khong co config rieng cho tong dung luong buffer.

#: Kich thuoc request toi da gui len broker -- phai >= max_batch_size,
#: neu khong se loi khi 1 batch gop lai vuot gioi han request mac dinh.
INGESTION_MAX_REQUEST_SIZE = int(
    os.getenv("INGESTION_MAX_REQUEST_SIZE", max(INGESTION_BATCH_SIZE_BYTES * 2, 1024 * 1024))
)

#: [FIX] Flush + xoa futures theo chu ky thay vi gom het toan bo file roi
#: gather 1 lan cuoi -- gioi han bo nho khi file co hang trieu dong, va
#: cho progress log thuc te trong luc chay thay vi im lang toi cuoi.
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

        # Dam bao Kafka day het du lieu con trong buffer noi bo truoc khi
        # ham tra ve (khong chi tin vao gather() phia tren -- flush() la
        # cam ket manh hon o tang producer client).
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