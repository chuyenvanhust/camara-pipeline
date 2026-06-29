#!/usr/bin/env python3
# pipeline/deduplication/dedup_job.py

"""
Stage S3 — Dedup 2 lớp:

  1. FAST PATH (module này): Spark Structured Streaming + RocksDB state,
     TTL = 3600s. Bắt duplicate đến trong vòng 1 giờ mà không cần round-trip
     Postgres. Trade-off: duplicate đến sau 1h sẽ KHÔNG bị lớp này phát hiện
     — đây là quyết định chấp nhận được, xem ADR-004 (phạm vi: chỉ áp dụng
     cho fast path, không phải toàn hệ thống).

  2. LONG-TERM BACKSTOP (storage/migrations/004_dedup_trigger.sql): trigger
     Postgres trên radius_sessions, dùng chính bảng này làm long-term
     storage để bắt các duplicate đến muộn (>1h) mà fast path bỏ lỡ. Hoạt
     động độc lập với Spark — áp dụng cho mọi đường insert vào
     radius_sessions.

Hai lớp này độc lập, không cái nào thay thế cái nào.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType, BooleanType
from pipeline_v1.deduplication.state_manager import DedupStateManager
import pandas as pd
from typing import List, Dict, Tuple, Set

# 1. Định nghĩa Schema cho State lưu trong RocksDB
# Lưu lại timestamp của bản ghi đầu tiên để tính toán TTL (1 giờ)
state_schema = StructType([
    StructField("first_seen_timestamp", LongType(), True)
])

# 2. Định nghĩa Schema cho Output sau khi qua State Processor
def get_output_schema(input_schema: StructType) -> StructType:
    """Schema output = input schema (đã cast event_timestamp sang Timestamp)
    + cột is_duplicate (Boolean)."""
    fields = input_schema.fields.copy()
    fields.append(StructField("is_duplicate", BooleanType(), True))
    return StructType(fields)



def dedup_pandas_state_func(key, pdf_group: pd.DataFrame, state) -> pd.DataFrame:
    """
    Với mỗi group (acct_session_id, acct_status_type):
    - Sort theo event_timestamp tăng dần.
    - Với mỗi row, so sánh event_timestamp với last_seen_ms (từ state
      hoặc từ row trước đó trong cùng batch):
        - Nếu chênh lệch <= TTL_SECONDS (3600s) -> is_duplicate=True
        - Ngược lại -> is_duplicate=False, cập nhật last_seen_ms = row hiện tại
    - Lưu last_seen_ms cuối cùng vào state.
    """
    pdf_group = pdf_group.copy()
    pdf_group = pdf_group.sort_values("event_timestamp").reset_index(drop=True)

    if state.exists:
        (last_seen_ms,) = state.get
    else:
        last_seen_ms = None

    ttl_ms = DedupStateManager.TTL_SECONDS * 1000
    is_dup_flags = []

    for _, row in pdf_group.iterrows():
        event_time_ms = int(row["event_timestamp"].timestamp() * 1000)

        if last_seen_ms is not None and (event_time_ms - last_seen_ms) <= ttl_ms:
            is_dup_flags.append(True)
        else:
            is_dup_flags.append(False)
            last_seen_ms = event_time_ms

    state.update((last_seen_ms,))
    pdf_group["is_duplicate"] = is_dup_flags
    return pdf_group


def process_deduplication(valid_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Stateful Deduplication theo (acct_session_id, acct_status_type):
    với mỗi group, so sánh event_timestamp của record mới với
    last_seen_timestamp trong state — nếu <= TTL_SECONDS (3600s)
    thì là duplicate.

    Lưu ý: state tồn tại vĩnh viễn theo (acct_session_id, acct_status_type)
    -- KHÔNG theo timeout watermark. Số lượng key bị chặn (bounded) bởi
    số subscriber x 3 status_type (~300k key cho 100k subscriber),
    chấp nhận được cho lab. Nếu cần tự dọn state theo thời gian, dùng
    GroupStateTimeout.EventTimeTimeout (cải tiến sau, ngoài scope hiện tại).
    """
    # Cast timestamp TRƯỚC khi build output_schema, để schema có đúng TimestampType
    valid_df = valid_df.withColumn("event_timestamp", F.col("event_timestamp").cast("timestamp"))

    output_schema = get_output_schema(valid_df.schema)

    # Watermark đặt sau cast, không đổi schema, chỉ phục vụ Spark dọn input buffer
    valid_df = valid_df.withWatermark("event_timestamp", "1 hour")

    processed_stream = valid_df.groupby(DedupStateManager.DEDUP_KEY_FIELDS).applyInPandasWithState(
        func=dedup_pandas_state_func,
        outputStructType=output_schema,
        stateStructType=state_schema,
        outputMode="Append",
        timeoutConf="NoTimeout"
    )

    dedup_df = processed_stream.filter(F.col("is_duplicate") == False).drop("is_duplicate")
    duplicate_log_df = processed_stream.filter(F.col("is_duplicate") == True)

    return dedup_df, duplicate_log_df


def write_to_sinks(dedup_df: DataFrame, duplicate_log_df: DataFrame, bootstrap_servers: str, checkpoint_dir: str):
    """
    Quản lý việc ghi song song ra các Sink (Kafka & PostgreSQL)
    """
    # Sink 1: Ghi dữ liệu sạch ra Kafka radius.dedup
    kafka_query = dedup_df \
        .selectExpr("CAST(acct_session_id AS STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("topic", "radius.dedup") \
        .option("checkpointLocation", f"{checkpoint_dir}/dedup_kafka/") \
        .outputMode("append") \
        .start()

    # Sink 2: Ghi dữ liệu trùng lặp ra PostgreSQL sử dụng foreachBatch hàng tĩnh
    def send_to_postgres(batch_df: DataFrame, batch_id: int):
        if batch_df.count() == 0:
            return
            
        # Thống kê tổng hợp số lượng trùng lặp theo từng session trong batch này
        summary_df = batch_df.groupBy("acct_session_id").agg(
            F.count("acct_session_id").alias("duplicate_count")
        ).withColumn("logged_at", F.current_timestamp())
        
        # Ghi vào PostgreSQL thông qua JDBC
        summary_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5432/radius_db") \
            .option("dbtable", "duplicate_log") \
            .option("user", "postgres") \
            .option("password", "secret") \
            .mode("append") \
            .save()

    postgres_query = duplicate_log_df \
        .writeStream \
        .foreachBatch(send_to_postgres) \
        .option("checkpointLocation", f"{checkpoint_dir}/dedup_postgres/") \
        .start()


def start_dedup_stream(spark: SparkSession, bootstrap_servers: str, checkpoint_dir: str):
    """
    Khởi chạy toàn bộ luồng xử lý streaming kiến trúc Stage S3
    """
    # Khởi tạo RocksDB State Store thông qua State Manager
    DedupStateManager.configure_rocksdb(spark, checkpoint_dir)
    
    # Đọc dữ liệu thô từ Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", "radius.valid") \
        .load()
    
    # Giả sử value từ Kafka là chuỗi JSON chứa các trường, cần phân tách schema ra trước
    # (Thay thế đoạn select này bằng schema thực tế của dự án bạn)
    json_schema = StructType([
        StructField("acct_session_id", StringType(), True),
        StructField("acct_status_type", StringType(), True),
        StructField("event_timestamp", StringType(), True)
    ])
    
    parsed_df = raw_stream.select(
        F.from_json(F.col("value").cast("string"), json_schema).alias("data")
    ).select("data.*")
    
    # Xử lý lọc trùng dựa trên thuật toán Stateful
    dedup_df, duplicate_log_df = process_deduplication(parsed_df)
    
    # Kích hoạt cổng ghi dữ liệu ra các sink độc lập
    write_to_sinks(dedup_df, duplicate_log_df, bootstrap_servers, checkpoint_dir)
    
    # Chờ đợi tiến trình kết thúc
    spark.streams.awaitAnyTermination()