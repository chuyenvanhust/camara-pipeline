#!/usr/bin/env python3
# Bootstrap sys.path
import sys, os as _os
sys.path.insert(0, _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "..", "..")))

"""
Stage 3 — radius.clean → PostgreSQL

Spark Structured Streaming: consume radius.clean → INSERT vào radius_sessions
bằng psycopg2 executemany + ON CONFLICT DO NOTHING (idempotent).

Input : Kafka topic radius.clean
Output: PostgreSQL bảng radius_sessions

Giữ nguyên 3-layer pattern từ codebase gốc:
  1. CONSTANTS   — export, đọc từ .env
  2. PURE LOGIC  — build_dsn, build_upsert_sql, extract_rows_from_batch
  3. SPARK I/O   — write_micro_batch (foreachBatch), start_storage_stream
"""

import os
import logging

import psycopg2
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,LongType
)

from pipeline.storage.models import RadiusSession
from pipeline.spark_jars import KAFKA_PG_PACKAGES, configure_spark_jars
from typing import List,Dict,Tuple

logger = logging.getLogger(__name__)


# ==============================================================================
# 1. CONSTANTS
# ==============================================================================

DB_HOST     = os.getenv("DB_HOST",     "postgres")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))
DB_NAME     = os.getenv("DB_NAME",     "camara_db")
DB_USER     = os.getenv("DB_USER",     "camara")
DB_PASSWORD = os.getenv("DB_PASSWORD", "camara")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")
KAFKA_TOPIC_CLEAN       = os.getenv("KAFKA_TOPIC_CLEAN", "radius.clean")

SPARK_JDBC_BATCH_SIZE    = int(os.getenv("SPARK_JDBC_BATCH_SIZE",         "1000"))
SPARK_COMMIT_INTERVAL    = os.getenv("SPARK_COMMIT_INTERVAL_SECONDS",     "5")

CHECKPOINT_LOCATION = os.getenv(
    "STORAGE_CHECKPOINT_DIR",
    "/tmp/spark-pipeline-storage-checkpoint",
)

#: Schema JSON từ radius.clean — khớp RadiusSession.INSERT_COLUMNS
CLEAN_RECORD_SCHEMA = StructType([
    StructField("acct_status_type", StringType(), True),
    StructField("acct_session_id",  StringType(), True),
    StructField("acct_session_time", LongType(), True),
    StructField("event_timestamp",  StringType(), True),
    StructField("ingest_timestamp",  StringType(), True),
    StructField("msisdn",           StringType(), True),
    StructField("imsi",             StringType(), True),
    StructField("imei",             StringType(), True),
    StructField("rat_type",         StringType(), True),
    StructField("framed_ip",        StringType(), True),
    StructField("nas_ip",           StringType(), True),
    StructField("mcc_mnc",          StringType(), True),
    StructField("late_arrival",     BooleanType(), True),
])


# ==============================================================================
# 2. PURE LOGIC
# ==============================================================================

def build_dsn(
    host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
    user=DB_USER, password=DB_PASSWORD,
) -> dict:
    """Build psycopg2 connection DSN dict."""
    return dict(host=host, port=port, dbname=dbname, user=user, password=password)


def build_insert_sql(table_name, columns) -> str:
    cols = ", ".join(columns)
    phs = ", ".join(["%s"] * len(columns))
    return f"INSERT INTO {table_name} ({cols}) VALUES ({phs})"


def extract_rows_from_batch(rows: List[Dict], columns) -> List[Tuple]:
    """Convert list[dict] → list[tuple] theo thứ tự columns."""
    return [tuple(row.get(col) for col in columns) for row in rows]


# ==============================================================================
# 3. SPARK I/O
# ==============================================================================

def write_micro_batch(dsn: dict, batch_size: int = SPARK_JDBC_BATCH_SIZE):
    # Dùng insert_sql thay vì upsert_sql
    insert_sql = build_insert_sql(RadiusSession.__tablename__, RadiusSession.INSERT_COLUMNS)

    def _callback(batch_df: DataFrame, batch_id: int) -> None:
        rows = [r.asDict() for r in batch_df.collect()]
        if not rows: return
        data = extract_rows_from_batch(rows, RadiusSession.INSERT_COLUMNS)
        conn = psycopg2.connect(**dsn)
        try:
            with conn.cursor() as cur:
                # Trigger trong DB sẽ tự xử lý trùng, Python chỉ việc gửi data
                cur.executemany(insert_sql, data)
            conn.commit()
            logger.info("S3 batch %d: %d rows written", batch_id, len(data))
        except Exception as e:
            conn.rollback()
            logger.error("S3 batch %d failed: %s", batch_id, e)
        finally:
            conn.close()
    return _callback


def start_storage_stream(spark: SparkSession):
    """
    Entry point Stage 3: radius.clean → radius_sessions (PostgreSQL).
    Trả về StreamingQuery để caller gọi awaitTermination().
    """
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_CLEAN)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("kafka.metadata.max.age.ms", "10000")
        .load()
    )

    parsed_df = (
        raw_stream
        .select(F.from_json(F.col("value").cast("string"), CLEAN_RECORD_SCHEMA).alias("d"))
        .select("d.*")
        # 1. Ép kiểu thời lượng session về số nguyên
        .withColumn("acct_session_time", F.col("acct_session_time").cast("integer"))
        
        # 2. Chuyển ISO String của sự kiện sang Timestamp chuẩn
        .withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))
        
        # 3. BỔ SUNG: Chuyển ISO String của thời điểm nạp sang Timestamp chuẩn
        .withColumn("ingest_timestamp", F.to_timestamp(F.col("ingest_timestamp")))
        
        # 4. Đảm bảo late_arrival không bị Null (gán False nếu không có)
        .withColumn("late_arrival", F.coalesce(F.col("late_arrival"), F.lit(False)))
    )

    dsn   = build_dsn()
    query = (
        parsed_df.writeStream
        .foreachBatch(write_micro_batch(dsn))
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime=f"{SPARK_COMMIT_INTERVAL} seconds")
        .start()
    )

    logger.info(
        "S3 Storage stream started: %s → %s (every %ss)",
        KAFKA_TOPIC_CLEAN, RadiusSession.__tablename__, SPARK_COMMIT_INTERVAL,
    )
    return query


def main() -> None:
    builder = (
        SparkSession.builder
        .appName("Camara-Storage-Job")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
    )
    spark = configure_spark_jars(builder, KAFKA_PG_PACKAGES).getOrCreate()

    query = start_storage_stream(spark)
    query.awaitTermination()


if __name__ == "__main__":
    main()