#!/usr/bin/env python3
# Bootstrap sys.path
import sys, os as _os
sys.path.insert(0, _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "..", "..")))

"""
Stage 2 — radius.raw → radius.clean

Gộp 3 bước xử lý trong 1 Spark Structured Streaming job:
  (a) Validation         : 6 rules — loại record lỗi
  (b) Deduplication      : dict in-memory TTL 3600s theo (acct_session_id, acct_status_type)
  (c) Conflict resolution: phân loại A/B/C — loại A+B, giữ C

Input : Kafka topic radius.raw
Output: Kafka topic radius.clean

===========================================================================
BOTTLENECK ĐÃ FIX (4 vấn đề):

[BN-1] applyInPandasWithState bên trong foreachBatch
  → Spark KHÔNG hỗ trợ stateful operator trong foreachBatch callback.
    Spark cố chạy một micro-batch-inside-micro-batch, treo vĩnh viễn.
  FIX: thay bằng dict in-memory DEDUP_STATE trong driver process.
       foreachBatch chỉ được phép dùng batch DataFrame operations hoặc
       collect() + Python thuần — không được lồng streaming operator.

[BN-2] asyncio.new_event_loop() tạo mới mỗi batch, không đóng đúng cách
  → Gây leak event loop + thread, tích lũy qua nhiều batch → OOM / freeze.
  FIX: dùng asyncio.run() — tự quản lý vòng đời loop, đóng sạch sau mỗi lần gọi.

[BN-3] httpx.AsyncClient() tạo mới MỖI RECORD (trong _run_validation_async)
  → N records = N connection pool init/teardown, cực kỳ tốn kém.
  FIX: tạo 1 AsyncClient duy nhất PER BATCH, chia sẻ connection pool cho
       toàn bộ records trong batch đó.

[BN-4] clean_df.count() sau khi đã .save() lên Kafka
  → Trigger thêm 1 Spark job chỉ để log số dòng — lãng phí.
  FIX: đếm bằng len(clean_rows) trước khi ghi (đã collect() rồi).
===========================================================================
"""

import os
import asyncio
import logging
import time
from typing import Dict, List, Optional, Tuple, Any

import httpx
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType, BooleanType,
)
from pyspark.sql.window import Window

from pipeline.validation.rules import execute_validation_pipeline
from pipeline.deduplication.state_manager import DedupStateManager
from pipeline.spark_jars import KAFKA_PACKAGE, configure_spark_jars

logger = logging.getLogger(__name__)

# ==============================================================================
# 1. CONSTANTS
# ==============================================================================

RAW_RADIUS_SCHEMA = StructType([
    StructField("acct_status_type", StringType(), True),
    StructField("acct_session_id",  StringType(), True),
    StructField("msisdn",           StringType(), True),
    StructField("imsi",             StringType(), True),
    StructField("imei",             StringType(), True),
    StructField("event_timestamp",  StringType(), True),
])

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")
KAFKA_TOPIC_RAW         = os.getenv("KAFKA_TOPIC_RAW",   "radius.raw")
KAFKA_TOPIC_CLEAN       = os.getenv("KAFKA_TOPIC_CLEAN", "radius.clean")
WATERMARK_THRESHOLD     = os.getenv("WATERMARK_VALIDATION", "7200 seconds")
CHECKPOINT_LOCATION     = os.getenv(
    "PROCESSING_CHECKPOINT_DIR",
    "/tmp/spark-pipeline-processing-checkpoint",
)

TTL_SECONDS = DedupStateManager.TTL_SECONDS  # 3600

# ==============================================================================
# 2. PURE LOGIC
# ==============================================================================

# ── 2a. Validation ─────────────────────────────────────────────────────────

async def _run_validation_batch(records: List[Dict]) -> List[Tuple]:
    """
    [FIX BN-3] 1 AsyncClient duy nhất cho toàn batch.
    Tất cả records chạy song song qua asyncio.gather().
    """
    async with httpx.AsyncClient() as client:
        tasks = [execute_validation_pipeline(r, client) for r in records]
        return await asyncio.gather(*tasks)


def _filter_valid(records: List[Dict], results: List[Tuple]) -> List[Dict]:
    """Giữ record is_valid=True; gắn warn_code nếu có."""
    valid = []
    for record, (res, warn) in zip(records, results):
        if res.is_valid:
            payload = dict(record)
            if warn:
                payload["warn_code"] = warn
            valid.append(payload)
    return valid


# ── 2b. Deduplication (in-memory, driver-side) ─────────────────────────────
#
# [FIX BN-1] Thay applyInPandasWithState bằng dict in-memory.
# DEDUP_STATE: { (acct_session_id, acct_status_type) -> last_seen_epoch_ms }
# Đủ cho bài toán RADIUS vì số session đang hoạt động đồng thời thực tế
# nằm trong khoảng vài chục nghìn → memory footprint ~vài MB.
# Nếu cần scale lên multi-node → chuyển sang Redis với TTL native.

DEDUP_STATE: Dict[Tuple[str, str], int] = {}
_DEDUP_LAST_CLEANUP: float = time.time()
_DEDUP_CLEANUP_INTERVAL = 300  # dọn expired entries mỗi 5 phút


def _dedup_cleanup_expired() -> None:
    """Xóa các entries đã quá TTL khỏi DEDUP_STATE để tránh memory leak."""
    global _DEDUP_LAST_CLEANUP
    now_ms = int(time.time() * 1000)
    ttl_ms = TTL_SECONDS * 1000
    expired = [k for k, v in DEDUP_STATE.items() if (now_ms - v) > ttl_ms]
    for k in expired:
        del DEDUP_STATE[k]
    _DEDUP_LAST_CLEANUP = time.time()
    if expired:
        logger.debug("Dedup cleanup: removed %d expired entries", len(expired))


def _dedup_filter(records: List[Dict]) -> List[Dict]:
    """
    Lọc duplicate trong batch hiện tại + so với state từ batch trước.
    Record mới hoặc ngoài TTL → pass, cập nhật state.
    Record trong TTL → bỏ.
    """
    global _DEDUP_LAST_CLEANUP

    # Dọn expired định kỳ
    if time.time() - _DEDUP_LAST_CLEANUP > _DEDUP_CLEANUP_INTERVAL:
        _dedup_cleanup_expired()

    now_ms = int(time.time() * 1000)
    ttl_ms = TTL_SECONDS * 1000
    unique = []

    for record in records:
        key = (
            str(record.get("acct_session_id", "")),
            str(record.get("acct_status_type", "")),
        )
        last_ms = DEDUP_STATE.get(key)
        ts_raw  = record.get("event_timestamp", "")
        try:
            event_ms = int(str(ts_raw).strip()) * 1000
        except (ValueError, TypeError):
            event_ms = now_ms

        if last_ms is None or (event_ms - last_ms) > ttl_ms:
            DEDUP_STATE[key] = event_ms
            unique.append(record)
        # else: duplicate → bỏ

    return unique


# ── 2c. Conflict resolution (pandas, driver-side) ──────────────────────────

def _resolve_conflicts(records: List[Dict]) -> List[Dict]:
    """
    Phân loại conflict A/B/C bằng pandas (đã collect() rồi, batch nhỏ).
    Loại A+B, giữ C (SIM Swap hợp lệ nghiệp vụ).

    Conflict A: cùng session, imsi/msisdn thay đổi sau Start.
    Conflict B: cùng imsi có 2+ Start chưa Stop.
    Conflict C: cùng msisdn nhưng imsi thay đổi (SIM Swap signal).
    """
    if not records:
        return []

    df = pd.DataFrame(records)

    # Đảm bảo cột timestamp tồn tại
    if "event_timestamp_ts" not in df.columns:
        df["event_timestamp_ts"] = pd.to_datetime(
            df["event_timestamp"].apply(
                lambda x: int(x) if str(x).strip().isdigit() else None
            ),
            unit="s", utc=True, errors="coerce",
        )

    df = df.sort_values("event_timestamp_ts").reset_index(drop=True)

    # ── Conflict A ──────────────────────────────────────────────────────────
    session_first = (
        df.groupby("acct_session_id")[["imsi", "msisdn"]]
          .first()
          .rename(columns={"imsi": "first_imsi", "msisdn": "first_msisdn"})
    )
    df = df.merge(session_first, on="acct_session_id", how="left")
    df["is_conflict_a"] = (
        (df["acct_status_type"] != "Start") &
        ((df["imsi"] != df["first_imsi"]) | (df["msisdn"] != df["first_msisdn"]))
    )

    # ── Conflict B ──────────────────────────────────────────────────────────
    start_mask = (df["acct_status_type"] == "Start") & (~df["is_conflict_a"])
    df["_imsi_start_rank"] = (
        df[start_mask]
          .groupby("imsi")
          .cumcount()
    )
    df["is_conflict_b"] = (
        (~df["is_conflict_a"]) &
        (df["acct_status_type"] == "Start") &
        (df["_imsi_start_rank"].fillna(0) > 0)
    )

    # ── Conflict C ──────────────────────────────────────────────────────────
    df["prev_imsi"] = df.groupby("msisdn")["imsi"].shift(1)
    df["is_conflict_c"] = (
        (~df["is_conflict_a"]) &
        (~df["is_conflict_b"]) &
        df["prev_imsi"].notna() &
        (df["imsi"] != df["prev_imsi"])
    )

    # Loại A+B, giữ C
    clean = df[~df["is_conflict_a"] & ~df["is_conflict_b"]].copy()
    clean = clean.drop(
        columns=[
            "first_imsi", "first_msisdn", "is_conflict_a",
            "is_conflict_b", "_imsi_start_rank", "prev_imsi", "is_conflict_c",
            "event_timestamp_ts",
        ],
        errors="ignore",
    )
    return clean.to_dict(orient="records")


# ==============================================================================
# 3. SPARK I/O
# ==============================================================================

def build_stream(spark: SparkSession) -> DataFrame:
    """Đọc radius.raw → parse JSON → áp watermark."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_RAW)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "2000")
        .option("kafka.metadata.max.age.ms", "10000")
        .load()
        .selectExpr("CAST(value AS STRING) AS raw_value")
        .select(F.from_json(F.col("raw_value"), RAW_RADIUS_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_timestamp_ts", F.to_timestamp(F.col("event_timestamp")))
        .withWatermark("event_timestamp_ts", WATERMARK_THRESHOLD)
    )


def make_callback(spark: SparkSession):
    """
    foreachBatch callback: collect → validate → dedup → conflict → ghi Kafka.
    Không có bất kỳ streaming operator nào bên trong (fix BN-1).
    """
    def _callback(batch_df: DataFrame, batch_id: int) -> None:
        t0 = time.time()

        # ── Collect ──────────────────────────────────────────────────────────
        rows = [r.asDict() for r in batch_df.collect()]
        if not rows:
            return

        # ── (a) Validation — [FIX BN-2] asyncio.run() thay new_event_loop() ──
        val_results = asyncio.run(_run_validation_batch(rows))
        valid_rows  = _filter_valid(rows, val_results)

        logger.info(
            "Batch %d | total=%d valid=%d (%.0fms)",
            batch_id, len(rows), len(valid_rows), (time.time()-t0)*1000,
        )
        if not valid_rows:
            return

        # ── (b) Dedup — [FIX BN-1] dict in-memory thay applyInPandasWithState ──
        deduped_rows = _dedup_filter(valid_rows)
        logger.info(
            "Batch %d | after dedup=%d", batch_id, len(deduped_rows),
        )
        if not deduped_rows:
            return

        # ── (c) Conflict resolution — pandas, không lồng Spark job ──────────
        clean_rows = _resolve_conflicts(deduped_rows)
        if not clean_rows:
            return

        # ── Ghi ra radius.clean ───────────────────────────────────────────────
        clean_df = spark.createDataFrame(clean_rows)
        (
            clean_df
            .selectExpr("to_json(struct(*)) AS value")
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("topic", KAFKA_TOPIC_CLEAN)
            .save()
        )

        # [FIX BN-4] dùng len() thay clean_df.count() — không trigger thêm Spark job
        logger.info(
            "Batch %d | → %s rows=%d (total %.0fms)",
            batch_id, KAFKA_TOPIC_CLEAN, len(clean_rows), (time.time()-t0)*1000,
        )

    return _callback


def main() -> None:
    builder = (
        SparkSession.builder
        .appName("Camara-Processing-Job")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
    )
    spark = configure_spark_jars(builder, KAFKA_PACKAGE).getOrCreate()

    # RocksDB không cần nữa (dedup chuyển sang in-memory)
    # nhưng vẫn set checkpoint location cho Spark streaming
    spark.conf.set(
        "spark.sql.streaming.checkpointLocation",
        CHECKPOINT_LOCATION,
    )

    query = (
        build_stream(spark)
        .writeStream
        .foreachBatch(make_callback(spark))
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="5 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()