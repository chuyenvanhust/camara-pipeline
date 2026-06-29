#!/usr/bin/env python3
"""
pipeline/storage/writer.py

Stage S5 — Spark Structured Streaming: consume radius.clean → PostgreSQL.

Ghi records đã clean vào bảng radius_sessions bằng JDBC bulk insert
(psycopg2 executemany) với INSERT ... ON CONFLICT DO NOTHING để đảm bảo
idempotent khi retry sau crash.

Architecture layers (cùng pattern validator.py / dedup_job.py):

1. CONSTANTS — DB connection, Kafka topic, batch config (đọc từ .env)
2. PURE LOGIC — build_dsn(), build_upsert_sql(), extract_rows_from_batch()
   → unit test trực tiếp, không cần Spark/Kafka/PostgreSQL
3. SPARK I/O — write_micro_batch() foreachBatch callback, start_storage_stream()
"""

import os
import json
import logging

import psycopg2
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType,
)

from pipeline_v1.storage.models import RadiusSession


logger = logging.getLogger(__name__)


# ==============================================================================
# 1. CONSTANTS — export để test import, đọc từ env với fallback mặc định
# ==============================================================================

#: PostgreSQL connection — khớp với .env.example trong README
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "camara_db")
DB_USER = os.getenv("DB_USER", "camara")
DB_PASSWORD = os.getenv("DB_PASSWORD", "camara")

#: Kafka source topic — output của S4 conflict_resolution
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")
KAFKA_TOPIC_CLEAN = os.getenv("KAFKA_TOPIC_CLEAN", "radius.clean")

#: Batch & commit config — xem pipeline/storage/README.md
SPARK_JDBC_BATCH_SIZE = int(os.getenv("SPARK_JDBC_BATCH_SIZE", "1000"))
SPARK_COMMIT_INTERVAL = os.getenv("SPARK_COMMIT_INTERVAL_SECONDS", "30")

#: Checkpoint cho Structured Streaming exactly-once guarantee
CHECKPOINT_LOCATION = os.getenv(
    "STORAGE_CHECKPOINT_DIR", "/tmp/spark-pipeline-storage-checkpoint"
)

#: Schema JSON từ Kafka topic radius.clean (output S4 conflict resolution)
#: Phải khớp với RadiusSession.INSERT_COLUMNS — test verify điều này.
CLEAN_RECORD_SCHEMA = StructType([
    StructField("acct_session_id", StringType(), True),
    StructField("acct_status_type", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("msisdn", StringType(), True),
    StructField("imsi", StringType(), True),
    StructField("imei", StringType(), True),
    StructField("rat_type", StringType(), True),
    StructField("framed_ip", StringType(), True),
    StructField("nas_ip", StringType(), True),
    StructField("mcc_mnc", StringType(), True),
    StructField("late_arrival", BooleanType(), True),
])


# ==============================================================================
# 2. PURE LOGIC — unit test trực tiếp, không cần Spark/Kafka/PostgreSQL
# ==============================================================================

def build_dsn(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
              user=DB_USER, password=DB_PASSWORD):
    """
    Build psycopg2 connection DSN dict từ các tham số.

    Tách riêng thành pure function để:
    - Unit test verify format DSN không cần connect thật
    - Override trong integration test (trỏ sang test DB)

    Returns:
        dict: DSN cho ``psycopg2.connect(**dsn)``
    """
    return {
        "host": host,
        "port": port,
        "dbname": dbname,
        "user": user,
        "password": password,
    }


def build_upsert_sql(table_name, columns, conflict_columns):
    """
    Generate INSERT ... ON CONFLICT (...) DO NOTHING SQL statement.

    Args:
        table_name: tên bảng PostgreSQL (e.g. ``"radius_sessions"``)
        columns: tuple/list tên cột INSERT (e.g. ``RadiusSession.INSERT_COLUMNS``)
        conflict_columns: tuple/list cột UNIQUE constraint cho ON CONFLICT

    Returns:
        str: SQL statement với ``%s`` placeholders cho ``executemany``

    Example::

        >>> build_upsert_sql("t", ("a", "b"), ("a",))
        'INSERT INTO t (a, b) VALUES (%s, %s) ON CONFLICT (a) DO NOTHING'
    """
    cols_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    conflict_str = ", ".join(conflict_columns)
    return (
        f"INSERT INTO {table_name} ({cols_str}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT ({conflict_str}) DO NOTHING"
    )


def extract_rows_from_batch(rows, columns):
    """
    Convert list of row dicts thành list of tuples cho psycopg2 executemany.

    Thứ tự giá trị trong mỗi tuple khớp với thứ tự ``columns`` —
    quan trọng vì ``executemany`` dùng positional ``%s`` placeholders.

    Args:
        rows: list[dict] — mỗi dict là 1 record (đã ``.asDict()`` từ Spark Row)
        columns: tuple/list tên cột, quyết định thứ tự giá trị

    Returns:
        list[tuple]: mỗi tuple chứa giá trị theo đúng thứ tự columns,
            ``None`` nếu column không tồn tại trong dict (nullable columns)
    """
    return [
        tuple(row.get(col) for col in columns)
        for row in rows
    ]


# ==============================================================================
# 3. SPARK I/O — foreachBatch callback + streaming entry point
# ==============================================================================

def write_micro_batch(dsn, batch_size=SPARK_JDBC_BATCH_SIZE):
    """
    Trả về foreachBatch callback, đóng (closure) dsn và batch_size.

    Callback thực hiện:
    1. Collect rows từ Spark DataFrame (micro-batch)
    2. Convert sang tuples via ``extract_rows_from_batch``
    3. ``executemany`` INSERT ... ON CONFLICT DO NOTHING vào PostgreSQL
    4. Commit transaction; rollback nếu lỗi

    Pattern: closure trả callback — giống ``process_micro_batch()`` trong
    validator.py. Tạo connection mới mỗi batch thay vì giữ global connection
    để tránh connection leak khi Spark restart foreachBatch callback.

    Args:
        dsn: dict — output của ``build_dsn()``, dùng để connect psycopg2
        batch_size: int — số rows mỗi lần ``executemany`` (chunk size)

    Returns:
        Callable[[DataFrame, int], None] — foreachBatch callback
    """
    upsert_sql = build_upsert_sql(
        RadiusSession.__tablename__,
        RadiusSession.INSERT_COLUMNS,
        RadiusSession.CONFLICT_COLUMNS,
    )

    def _callback(batch_df: DataFrame, batch_id: int):
        # 1. Collect micro-batch từ Spark executor về driver
        rows = [row.asDict() for row in batch_df.collect()]
        if not rows:
            return

        # 2. Convert sang tuples theo đúng thứ tự INSERT_COLUMNS
        tuples_data = extract_rows_from_batch(rows, RadiusSession.INSERT_COLUMNS)

        # 3. Ghi vào PostgreSQL với chunked executemany
        conn = psycopg2.connect(**dsn)
        try:
            with conn.cursor() as cur:
                for i in range(0, len(tuples_data), batch_size):
                    chunk = tuples_data[i : i + batch_size]
                    cur.executemany(upsert_sql, chunk)
            conn.commit()
            logger.info(
                "S5 batch %d: inserted %d rows into %s",
                batch_id, len(tuples_data), RadiusSession.__tablename__,
            )
        except Exception:
            conn.rollback()
            logger.exception(
                "S5 batch %d: failed to write to PostgreSQL", batch_id
            )
            raise
        finally:
            conn.close()

    return _callback


def start_storage_stream(spark: SparkSession):
    """
    Entry point Stage S5: đọc radius.clean từ Kafka → ghi vào PostgreSQL.

    Trigger: ``processingTime`` = SPARK_COMMIT_INTERVAL (mặc định 30s).
    Checkpoint: CHECKPOINT_LOCATION (cho exactly-once với ON CONFLICT DO NOTHING).

    Args:
        spark: SparkSession đã được khởi tạo bởi pipeline runner

    Returns:
        StreamingQuery: Spark streaming query handle (caller gọi ``.awaitTermination()``)
    """
    # Đọc stream từ Kafka topic radius.clean
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_CLEAN)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse JSON value → struct theo CLEAN_RECORD_SCHEMA, cast timestamp
    parsed_df = (
        raw_stream
        .select(
            F.from_json(
                F.col("value").cast("string"), CLEAN_RECORD_SCHEMA
            ).alias("data")
        )
        .select("data.*")
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
    )

    # Khởi tạo foreachBatch callback với DSN mặc định
    dsn = build_dsn()
    query = (
        parsed_df.writeStream
        .foreachBatch(write_micro_batch(dsn))
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime=f"{SPARK_COMMIT_INTERVAL} seconds")
        .start()
    )

    logger.info(
        "S5 Storage stream started: %s → %s (commit every %ss)",
        KAFKA_TOPIC_CLEAN, RadiusSession.__tablename__, SPARK_COMMIT_INTERVAL,
    )

    return query
