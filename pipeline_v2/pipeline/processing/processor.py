#!/usr/bin/env python3
"""
Stage 2 — radius.raw → radius.clean

Spark Structured Streaming job gộp 3 bước xử lý:

  (a) Validation    : chạy 6 rules (rules.py) — loại record lỗi
  (b) Deduplication : stateful TTL 3600s theo (acct_session_id, acct_status_type)
  (c) Conflict resolution: phân loại A / B / C, loại A+B, giữ C

Input : Kafka topic radius.raw
Output: Kafka topic radius.clean (record sạch, có thể chứa conflict-C)

Kiến trúc 3 lớp (pattern giữ nguyên từ codebase gốc):
  1. CONSTANTS   — export, tránh hard-code drift giữa code và test
  2. PURE LOGIC  — unit-testable, không phụ thuộc Spark/Kafka/Network
  3. SPARK I/O   — wiring: đọc Kafka → thuần logic → ghi Kafka
"""

import os
import json
import asyncio
import logging

import httpx
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    LongType, BooleanType,
)
from pyspark.sql.window import Window

from pipeline.validation.rules import execute_validation_pipeline
from pipeline.deduplication.state_manager import DedupStateManager

logger = logging.getLogger(__name__)

# ==============================================================================
# 1. CONSTANTS
# ==============================================================================

#: Schema JSON record thô từ radius.raw
RAW_RADIUS_SCHEMA = StructType([
    StructField("acct_status_type", StringType(), True),
    StructField("acct_session_id",  StringType(), True),
    StructField("msisdn",           StringType(), True),
    StructField("imsi",             StringType(), True),
    StructField("imei",             StringType(), True),
    StructField("event_timestamp",  StringType(), True),
])

#: Kafka
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")
KAFKA_TOPIC_RAW         = os.getenv("KAFKA_TOPIC_RAW",   "radius.raw")
KAFKA_TOPIC_CLEAN       = os.getenv("KAFKA_TOPIC_CLEAN", "radius.clean")

#: Watermark cho bước validation (7200s = 2 × late-arrival-threshold)
WATERMARK_VALIDATION    = os.getenv("WATERMARK_VALIDATION", "7200 seconds")

#: Checkpoint riêng cho job này
CHECKPOINT_LOCATION     = os.getenv(
    "PROCESSING_CHECKPOINT_DIR",
    "/tmp/spark-pipeline-processing-checkpoint",
)


# ==============================================================================
# 2. PURE LOGIC — không phụ thuộc Spark/Kafka
# ==============================================================================

# ---------- 2a. Validation -------------------------------------------------

async def _run_validation_async(records: list[dict]) -> list[tuple]:
    """
    Chạy execute_validation_pipeline() song song cho cả batch.
    Trả về list[(ValidationResult, warn_code)] cùng thứ tự với `records`.
    """
    async with httpx.AsyncClient() as client:
        tasks = [execute_validation_pipeline(r, client) for r in records]
        return await asyncio.gather(*tasks)


def _filter_valid_records(
    records: list[dict],
    validation_results: list[tuple],
) -> list[dict]:
    """
    Giữ lại chỉ các record is_valid=True.
    Gắn thêm warn_code nếu circuit-breaker bypass.
    Record lỗi bị loại bỏ (không ghi invalid_log trong pipeline rút gọn này;
    có thể thêm sink riêng sau nếu cần).

    Raises:
        ValueError: nếu len không khớp.
    """
    if len(records) != len(validation_results):
        raise ValueError(
            f"records ({len(records)}) ≠ validation_results ({len(validation_results)})"
        )

    valid = []
    for record, (res, warn) in zip(records, validation_results):
        if res.is_valid:
            payload = dict(record)
            if warn:
                payload["warn_code"] = warn
            valid.append(payload)
    return valid


# ---------- 2b. Deduplication (stateful pandas UDF) ------------------------

#: Schema state lưu trong RocksDB
_DEDUP_STATE_SCHEMA = StructType([
    StructField("last_seen_ms", LongType(), True)
])


def _dedup_pandas_state_func(key, pdf_group: pd.DataFrame, state) -> pd.DataFrame:
    """
    Stateful dedup theo (acct_session_id, acct_status_type).
    TTL = DedupStateManager.TTL_SECONDS (3600s).
    Record đến trong vòng TTL kể từ lần gần nhất → is_duplicate=True.
    """
    pdf_group = pdf_group.copy().sort_values("event_timestamp_ts").reset_index(drop=True)

    last_seen_ms = state.get[0] if state.exists else None
    ttl_ms = DedupStateManager.TTL_SECONDS * 1000
    flags = []

    for _, row in pdf_group.iterrows():
        ts = row["event_timestamp_ts"]
        if ts is None or pd.isnull(ts):
            flags.append(True)          # timestamp lỗi → bỏ
            continue
        event_ms = int(ts.timestamp() * 1000)
        if last_seen_ms is not None and (event_ms - last_seen_ms) <= ttl_ms:
            flags.append(True)
        else:
            flags.append(False)
            last_seen_ms = event_ms

    state.update((last_seen_ms,))
    pdf_group["is_duplicate"] = flags
    return pdf_group


# ---------- 2c. Conflict resolution (batch DataFrame) ----------------------

def _resolve_conflicts(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Phân loại A / B / C trên batch DataFrame (giữ nguyên logic gốc).

    Returns:
        (clean_df, conflict_log_df)
        - clean_df      : record sạch + conflict-C (giữ lại vì hợp lệ nghiệp vụ)
        - conflict_log_df: record A+B+C để audit
    """
    w_session = Window.partitionBy("acct_session_id").orderBy("event_timestamp_ts")
    w_imsi    = Window.partitionBy("imsi").orderBy("event_timestamp_ts")
    w_msisdn  = Window.partitionBy("msisdn").orderBy("event_timestamp_ts")

    df = (
        df
        .withColumn("first_imsi",   F.first("imsi").over(w_session))
        .withColumn("first_msisdn", F.first("msisdn").over(w_session))
        .withColumn("is_conflict_a",
            F.when(
                (F.col("acct_status_type") != "Start") &
                (
                    (F.col("imsi")   != F.col("first_imsi"))  |
                    (F.col("msisdn") != F.col("first_msisdn"))
                ),
                True,
            ).otherwise(False),
        )
        .withColumn("is_conflict_b",
            F.when(
                (F.col("is_conflict_a") == False) &
                (F.col("acct_status_type") == "Start") &
                (F.row_number().over(w_imsi) > 1),
                True,
            ).otherwise(False),
        )
        .withColumn("prev_imsi", F.lag("imsi", 1).over(w_msisdn))
        .withColumn("is_conflict_c",
            F.when(
                (F.col("is_conflict_a") == False) &
                (F.col("is_conflict_b") == False) &
                F.col("prev_imsi").isNotNull() &
                (F.col("imsi") != F.col("prev_imsi")),
                True,
            ).otherwise(False),
        )
    )

    conflict_log_df = (
        df.filter(
            F.col("is_conflict_a") | F.col("is_conflict_b") | F.col("is_conflict_c")
        )
        .withColumn("conflict_type",
            F.when(F.col("is_conflict_a"), "A")
             .when(F.col("is_conflict_b"), "B")
             .otherwise("C"),
        )
        .select("acct_session_id", "msisdn", "imsi", "event_timestamp_ts", "conflict_type")
    )

    # Loại A và B, giữ C (hợp lệ nghiệp vụ: SIM Swap)
    clean_df = df.filter(
        ~F.col("is_conflict_a") & ~F.col("is_conflict_b")
    ).drop("first_imsi", "first_msisdn", "is_conflict_a", "is_conflict_b",
           "prev_imsi", "is_conflict_c")

    return clean_df, conflict_log_df


# ==============================================================================
# 3. SPARK I/O
# ==============================================================================

def _write_to_kafka(spark: SparkSession, payloads: list[dict], topic: str) -> None:
    """Ghi list[dict] ra Kafka topic (dùng tạm thời trong foreachBatch)."""
    if not payloads:
        return
    df = spark.createDataFrame(payloads)
    (
        df.selectExpr("to_json(struct(*)) AS value")
          .write
          .format("kafka")
          .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
          .option("topic", topic)
          .save()
    )


def build_processing_stream(spark: SparkSession) -> DataFrame:
    """
    Đọc radius.raw → parse JSON → cast timestamp → áp watermark.
    Tách riêng để integration test inject DataFrame mock.
    """
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_RAW)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", "2000")
        .load()
        .selectExpr("CAST(value AS STRING) AS raw_value")
        .select(F.from_json(F.col("raw_value"), RAW_RADIUS_SCHEMA).alias("d"))
        .select("d.*")
    )

    # Cast timestamp để dùng cho watermark và dedup
    parsed = raw.withColumn(
        "event_timestamp_ts",
        F.to_timestamp(F.col("event_timestamp")),
    )
    return parsed.withWatermark("event_timestamp_ts", WATERMARK_VALIDATION)


def make_processing_callback(spark: SparkSession):
    """
    Trả về foreachBatch callback thực hiện tuần tự:
      (a) validation  — loại record lỗi
      (b) dedup       — loại duplicate trong TTL 1h
      (c) conflict    — loại conflict A+B, giữ C
    rồi ghi kết quả ra radius.clean.

    Closure: `spark` — tránh SparkSession.getActiveSession() pattern không an toàn.
    """

    # Schema state cho applyInPandasWithState
    dedup_output_schema = StructType(
        RAW_RADIUS_SCHEMA.fields
        + [
            StructField("event_timestamp_ts", TimestampType(), True),
            StructField("warn_code",          StringType(),   True),
            StructField("is_duplicate",       BooleanType(),  True),
        ]
    )

    def _callback(batch_df: DataFrame, batch_id: int) -> None:
        # ── (a) Validation ────────────────────────────────────────────────
        rows = [r.asDict() for r in batch_df.collect()]
        if not rows:
            return

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            val_results = loop.run_until_complete(_run_validation_async(rows))
        finally:
            loop.close()

        valid_records = _filter_valid_records(rows, val_results)
        if not valid_records:
            logger.info("Batch %d: 0 records passed validation.", batch_id)
            return

        # ── (b) Dedup (stateful qua applyInPandasWithState) ───────────────
        valid_df = spark.createDataFrame(valid_records)

        # Đảm bảo cột timestamp tồn tại sau khi tạo lại DataFrame
        if "event_timestamp_ts" not in valid_df.columns:
            valid_df = valid_df.withColumn(
                "event_timestamp_ts",
                F.to_timestamp(F.col("event_timestamp")),
            )

        valid_df = valid_df.withWatermark("event_timestamp_ts", "1 hour")

        dedup_stream = valid_df.groupby(
            DedupStateManager.DEDUP_KEY_FIELDS
        ).applyInPandasWithState(
            func=_dedup_pandas_state_func,
            outputStructType=dedup_output_schema,
            stateStructType=_DEDUP_STATE_SCHEMA,
            outputMode="Append",
            timeoutConf="NoTimeout",
        )

        deduped_df = dedup_stream.filter(F.col("is_duplicate") == False).drop("is_duplicate")

        # ── (c) Conflict resolution ───────────────────────────────────────
        clean_df, _conflict_log = _resolve_conflicts(deduped_df)

        # Drop helper column trước khi xuất
        clean_df = clean_df.drop("event_timestamp_ts")

        # Ghi ra radius.clean
        (
            clean_df
            .selectExpr("to_json(struct(*)) AS value")
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("topic", KAFKA_TOPIC_CLEAN)
            .save()
        )

        logger.info(
            "Batch %d: %d records → radius.clean", batch_id, clean_df.count()
        )

    return _callback


def main() -> None:
    """
    Entry point Stage 2:
      radius.raw (Kafka) → [validate + dedup + conflict] → radius.clean (Kafka)
    """
    spark = (
        SparkSession.builder
        .appName("Camara-Processing-Job")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
        .config("spark.jars.ivy", "/tmp/ivy2")
        .getOrCreate()
    )

    DedupStateManager.configure_rocksdb(spark, CHECKPOINT_LOCATION)

    stream_df = build_processing_stream(spark)

    query = (
        stream_df.writeStream
        .foreachBatch(make_processing_callback(spark))
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="5 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
