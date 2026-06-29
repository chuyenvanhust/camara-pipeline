#!/usr/bin/env python3
"""
Stage S2 — Spark Structured Streaming job: consume `radius.raw`,
chạy 6 validation rules (rules.py), route record sang
`radius.valid` hoặc `radius.invalid`.

Thiết kế module này tách thành 3 lớp rõ ràng để dễ test:

1. CONSTANTS — các giá trị cấu hình (watermark, schema, topic names)
   được export để test import dùng lại, tránh hard-code trùng lặp
   giữa code thật và test (xem README.md).

2. PURE LOGIC — `run_validation_async()` và `route_records()`.
   Không phụ thuộc SparkSession, không phụ thuộc Kafka.
   Nhận list[dict] vào, trả list[dict] ra. Unit test trực tiếp,
   không cần Spark.

3. SPARK I/O — `process_micro_batch()` và `main()`.
   Đây là lớp "wiring": lấy SparkSession qua closure (không dùng
   SparkSession.getActiveSession() — pattern này không an toàn
   trong foreachBatch context), gọi PURE LOGIC, rồi viết kết quả
   ra Kafka qua write_to_kafka(). write_to_kafka() là 1 hàm mỏng,
   dễ patch trong integration test mà không cần mock sâu vào
   pyspark.sql.DataFrame.write.
"""

import os
import json
import asyncio
import httpx
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StringType, StructType, StructField

from pipeline_v1.validation.rules import execute_validation_pipeline
from typing import List, Dict, Tuple, Set

# ==============================================================================
# CONSTANTS — export để test import, tránh drift giữa code và test
# ==============================================================================

#: Schema của record thô đọc từ Kafka topic radius.raw (sau khi parse JSON)
RAW_RADIUS_SCHEMA = StructType([
    StructField("acct_status_type", StringType(), True),
    StructField("acct_session_id", StringType(), True),
    StructField("msisdn", StringType(), True),
    StructField("imsi", StringType(), True),
    StructField("imei", StringType(), True),
    StructField("event_timestamp", StringType(), True),
])

#: Tên cột timestamp sau khi parse sang TimestampType — dùng cho withWatermark
EVENT_TIME_COLUMN = "event_time_parsed"

#: Watermark = 2 x LATE_ARRIVAL_THRESHOLD_SECONDS (3600s) = 7200s.
#: Record có event_timestamp cũ hơn watermark hiện tại sẽ bị Spark drop
#: trước khi vào process_micro_batch (xem README mục "Watermark").
WATERMARK_THRESHOLD = "7200 seconds"

#: Tên các Kafka topic output, đọc từ env với fallback mặc định
KAFKA_TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "radius.raw")
KAFKA_TOPIC_VALID = os.getenv("KAFKA_TOPIC_VALID", "radius.valid")
KAFKA_TOPIC_INVALID = os.getenv("KAFKA_TOPIC_INVALID", "radius.invalid")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")

#: Checkpoint location cho Structured Streaming query
CHECKPOINT_LOCATION = os.getenv(
    "VALIDATION_CHECKPOINT_DIR", "/tmp/spark-pipeline-validation-checkpoint"
)


# ==============================================================================
# PURE LOGIC -- khong phu thuoc Spark/Kafka, unit test truc tiep
# ==============================================================================

async def run_validation_async(records: List[Dict]) -> List[Tuple]:
    """
    Chạy execute_validation_pipeline() song song cho toàn bộ records
    trong 1 micro-batch, dùng 1 AsyncClient duy nhất (connection pool
    chung cho cả batch).

    Args:
        records: list các dict, mỗi dict là 1 row đã convert từ
            Spark Row (qua .asDict()), theo RAW_RADIUS_SCHEMA.

    Returns:
        list[tuple[ValidationResult, Optional[str]]] -- cùng thứ tự
        với `records`, mỗi phần tử là (ValidationResult, warn_code).

    Note:
        Hàm này là async vì execute_validation_pipeline() gọi HTTP
        tới 3 mock services. Caller (process_micro_batch) chịu trách
        nhiệm tạo/đóng event loop -- hàm này không tự quản lý loop để
        có thể compose/test độc lập với asyncio.run() hoặc
        loop.run_until_complete().
    """
    async with httpx.AsyncClient() as client:
        tasks = [execute_validation_pipeline(r, client) for r in records]
        return await asyncio.gather(*tasks)


def route_records(
    records: List[Dict],
    validation_results: List[Tuple],
) -> Tuple[List[Dict], List[Dict]]:
    """
    Phân tuyến records thành (valid_payloads, invalid_payloads) dựa
    trên kết quả validation -- KHÔNG đụng tới Spark/Kafka, chỉ xử lý
    dict thuần. Đây là phần logic cốt lõi cần test kỹ nhất vì nó
    quyết định record nào đi đâu.

    Args:
        records: list dict gốc (đã .asDict() từ Spark Row).
        validation_results: output của run_validation_async(),
            cùng thứ tự với `records`.

    Returns:
        (valid_payloads, invalid_payloads): 2 list dict.
        - valid_payloads: record gốc, có thêm "warn_code" nếu rule
          bị circuit-breaker bypass (WARN_RULE_BYPASSED).
        - invalid_payloads: record gốc, có thêm "error_code" và
          "error_message" từ ValidationResult.

    Raises:
        ValueError: nếu len(records) != len(validation_results) --
            báo lỗi sớm thay vì zip() âm thầm cắt ngắn danh sách.
    """
    if len(records) != len(validation_results):
        raise ValueError(
            f"records và validation_results phải cùng độ dài: "
            f"{len(records)} != {len(validation_results)}"
        )

    valid_payloads: List[Dict] = []
    invalid_payloads: List[Dict] = []

    for record, (res, warn) in zip(records, validation_results):
        # Copy để không mutate input gốc -- tránh side-effect bất ngờ
        # nếu caller tái sử dụng `records` sau khi gọi route_records().
        payload = dict(record)

        if res.is_valid:
            if warn:
                payload["warn_code"] = warn
            valid_payloads.append(payload)
        else:
            payload["error_code"] = res.error_code
            payload["error_message"] = res.error_message
            invalid_payloads.append(payload)

    return valid_payloads, invalid_payloads


# ==============================================================================
# SPARK I/O -- wiring layer, mock o muc ham (khong mock sau DataFrame.write)
# ==============================================================================

def write_to_kafka(spark: SparkSession, payloads: List[dict], topic: str) -> None:
    if not payloads:
        return

    df = spark.createDataFrame(payloads)

    df.selectExpr("to_json(struct(*)) AS value") \
      .write \
      .format("kafka") \
      .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
      .option("topic", topic) \
      .save()


def process_micro_batch(spark: SparkSession):
    """
    Trả về callback cho `foreachBatch`, đóng (closure) sẵn `spark`
    session -- tránh dùng SparkSession.getActiveSession() bên trong
    callback, vì giá trị này có thể là None trong foreachBatch
    context tùy phiên bản Spark (pattern dễ lỗi đã biết).

    Cách dùng trong main():
        query = watermarked_df.writeStream \\
            .foreachBatch(process_micro_batch(spark)) \\
            .option("checkpointLocation", CHECKPOINT_LOCATION) \\
            .start()

    Args:
        spark: SparkSession hiện hành, được tạo trong main() và
            truyền vào qua closure.

    Returns:
        Hàm `_callback(batch_df, batch_id)` đúng signature mà
        foreachBatch yêu cầu.
    """

    def _callback(batch_df: DataFrame, batch_id: int) -> None:
        """
        Callback thực thi cho mỗi micro-batch:
        1. collect() records về driver (batch nhỏ -- chấp nhận được).
        2. Chạy validation song song (run_validation_async).
        3. Phân tuyến valid/invalid (route_records -- pure, dễ test).
        4. Ghi 2 nhánh ra Kafka qua write_to_kafka().

        Args:
            batch_df: DataFrame của micro-batch hiện tại, theo
                RAW_RADIUS_SCHEMA (+ cột EVENT_TIME_COLUMN).
            batch_id: ID micro-batch do Spark cấp, dùng cho logging/debug.
        """
        records = [row.asDict() for row in batch_df.collect()]
        if not records:
            return

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            validation_results = loop.run_until_complete(
                run_validation_async(records)
            )
        finally:
            loop.close()

        valid_payloads, invalid_payloads = route_records(records, validation_results)

        write_to_kafka(spark, valid_payloads, KAFKA_TOPIC_VALID)
        write_to_kafka(spark, invalid_payloads, KAFKA_TOPIC_INVALID)

    return _callback


def build_watermarked_stream(spark: SparkSession) -> DataFrame:
    """
    Đọc Kafka topic radius.raw, parse JSON theo RAW_RADIUS_SCHEMA,
    convert event_timestamp sang TimestampType, và áp watermark
    WATERMARK_THRESHOLD trên cột EVENT_TIME_COLUMN.

    Tách riêng khỏi main() để integration test có thể build DataFrame
    tương đương mà không cần mở kết nối Kafka thật (test dùng
    spark.createDataFrame() với cùng schema/transform).

    Args:
        spark: SparkSession đang active.

    Returns:
        DataFrame streaming đã parse + có cột EVENT_TIME_COLUMN +
        watermark áp dụng, sẵn sàng cho .writeStream.foreachBatch(...).
    """
    raw_stream_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC_RAW) \
        .option("startingOffsets", "latest") \
        .option("maxOffsetsPerTrigger", "2000") \
        .load() \
        .selectExpr("CAST(value AS STRING) as raw_value") \
        .select(from_json(col("raw_value"), RAW_RADIUS_SCHEMA).alias("data")) \
        .select("data.*")

    parsed_df = raw_stream_df.withColumn(
        EVENT_TIME_COLUMN,
        to_timestamp(col("event_timestamp")),
    )

    return parsed_df.withWatermark(EVENT_TIME_COLUMN, WATERMARK_THRESHOLD)


def main() -> None:
    """
    Entry point: khởi tạo SparkSession, build streaming pipeline
    (build_watermarked_stream), đăng ký foreachBatch
    (process_micro_batch(spark)), và chạy liên tục.
    """
    spark = (
        SparkSession.builder
        .appName("Camara-Validation-Job")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
        )
        .config("spark.jars.ivy", "/tmp/ivy2")
        .getOrCreate()
    )

    watermarked_df = build_watermarked_stream(spark)

    query = watermarked_df.writeStream \
        .foreachBatch(process_micro_batch(spark)) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .trigger(processingTime="5 seconds") \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()