#!/usr/bin/env python3
# Bootstrap sys.path
# pipeline\pipeline\processing\processor.py
import sys, os as _os
sys.path.insert(0, _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "..", "..")))

"""
Stage 2 — radius.raw → radius.clean  (DRIVER-SIDE ORCHESTRATION)

===========================================================================
  [FIX-PICKLE] 2026-07-05 — TÁCH FILE ĐỂ SỬA "cannot pickle '_thread.lock'"
===========================================================================
  Toàn bộ logic CHẠY TRÊN EXECUTOR (validation, dedup, conflict A/B/C/D,
  ghi Postgres/Kafka, connection cache) đã được CHUYỂN sang module riêng:

      pipeline/processing/partition_worker.py  (hàm `process_partition`)

  LÝ DO: file processor.py này được chạy trực tiếp như __main__
  (spark-submit .../processor.py). Với hàm định nghĩa trong module
  __main__, cloudpickle KHÔNG pickle được bằng tham chiếu — nó phải
  serialize hàm BẰNG GIÁ TRỊ, kéo theo toàn bộ global mà hàm (và các
  hàm nó gọi) sử dụng. Bản trước có 1 `threading.Lock()` ở cấp module
  để bảo vệ connection cache — Lock là object C-level KHÔNG BAO GIỜ
  pickle được -> mọi batch crash ngay ở `foreachPartition(...)`, TRƯỚC
  khi dữ liệu thật sự được gửi tới executor (numInputRows=0 trong log).

  Khi `process_partition` nằm trong `partition_worker.py` (module import
  được bình thường), cloudpickle chỉ lưu "import pipeline.processing.
  partition_worker, lấy tên process_partition" — không đụng tới bất kỳ
  global non-picklable nào. Executor unpickle bằng cách import lại module
  đó từ đầu, tạo Lock/dict cache HOÀN TOÀN MỚI trong đúng tiến trình của
  nó. Đây là pattern chuẩn của PySpark: logic chạy trên executor luôn nên
  nằm trong module import-able, không nằm trực tiếp trong script __main__.
===========================================================================

Gộp 4 bước xử lý trong 1 Spark Structured Streaming job:
  (a) Validation         : 6 rules — loại record lỗi → invalid_log
  (b) Late arrival check : đánh dấu trước watermark → invalid_log
  (c) Deduplication      : Redis SET NX TTL 3600s theo (acct_session_id, acct_status_type)
  (d) Conflict resolution: phân loại A/B/C/D
        - A, B → conflict_log, loại khỏi luồng sạch
        - C, D → giữ trong luồng sạch + ghi thẳng swap_event (Redis global
                 state là nguồn xác nhận — không gọi HLR/HSS)

KIẾN TRÚC I/O: EXECUTOR-BASED — KHÔNG collect() VỀ DRIVER.
  batch_df được repartition theo `msisdn` rồi xử lý bằng foreachPartition
  (hàm `process_partition` trong partition_worker.py): validation + dedup
  + conflict A/B/C/D + ghi Postgres (invalid_log/conflict_log/swap_event)
  + ghi Kafka (radius.clean) ĐỀU CHẠY TRÊN EXECUTOR.

  Vì sao repartition theo msisdn là đủ:
    - Conflict C/D so sánh theo msisdn -> hiển nhiên đúng.
    - Conflict A so sánh theo acct_session_id: 1 session RADIUS luôn
      thuộc về đúng 1 msisdn trong toàn vòng đời của nó.
    - Conflict B so sánh theo imsi: 1 IMSI hầu như luôn gắn với đúng 1
      msisdn tại một thời điểm (trừ chính lúc SIM swap) -> chấp nhận được.

Input : Kafka topic radius.raw
Output:
  - Kafka topic radius.clean       (record hợp lệ, bao gồm conflict C/D)
  - PostgreSQL invalid_log         (validation fail + late arrival)
  - PostgreSQL conflict_log        (conflict A/B)
  - PostgreSQL swap_event          (conflict C/D — xác nhận bằng Redis global state)

===========================================================================
"""

import os
import logging
import time

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import StructType, StructField, StringType

from pipeline.spark_jars import KAFKA_PACKAGE, configure_spark_jars

# [FIX-PICKLE] process_partition và close_all_worker_resources được định
# nghĩa trong module import-able riêng — xem giải thích ở đầu file.
from pipeline.processing.partition_worker import (
    process_partition,
    close_all_worker_resources,
)

logger = logging.getLogger(__name__)

# ==============================================================================
# 1. CONSTANTS (chỉ những gì driver cần — hằng số dùng bên executor nằm
#    trong partition_worker.py, đọc env độc lập ở đó)
# ==============================================================================

RAW_RADIUS_SCHEMA = StructType([
    StructField("acct_status_type", StringType(), True),
    StructField("acct_session_id",  StringType(), True),
    StructField("acct_session_time", StringType(), True),
    StructField("event_timestamp",  StringType(), True),
    StructField("ingest_timestamp", StringType(), True),
    StructField("msisdn",           StringType(), True),
    StructField("imsi",             StringType(), True),
    StructField("imei",             StringType(), True),
    StructField("rat_type",         StringType(), True),
    StructField("framed_ip",        StringType(), True),
    StructField("nas_ip",           StringType(), True),
    StructField("mcc_mnc",          StringType(), True),
])

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")
KAFKA_TOPIC_RAW         = os.getenv("KAFKA_TOPIC_RAW",   "radius.raw")
WATERMARK_THRESHOLD     = os.getenv("WATERMARK_VALIDATION", "7200 seconds")
CHECKPOINT_LOCATION     = os.getenv(
    "PROCESSING_CHECKPOINT_DIR",
    "/tmp/spark-pipeline-processing-checkpoint",
)

VALIDATION_PARTITIONS = int(os.getenv("VALIDATION_PARTITIONS", "8"))

# Nếu 1 batch fail ngay ở tầng driver (hiếm — vd Kafka broker rớt lúc
# repartition), có raise tiếp để dừng hẳn job hay chỉ log rồi thử batch
# tiếp theo. Mặc định: log rồi tiếp tục (khớp với hành vi bên
# partition_worker.RAISE_ON_PARTITION_FAILURE).
RAISE_ON_PARTITION_FAILURE = os.getenv("RAISE_ON_PARTITION_FAILURE", "0") == "1"


# ==============================================================================
# 2. SPARK I/O
# ==============================================================================

def build_stream(spark: SparkSession) -> DataFrame:
    """
    Đọc radius.raw, parse JSON theo RAW_RADIUS_SCHEMA, áp watermark.

    Watermark chỉ ảnh hưởng tới việc Spark dọn state nội bộ — late
    arrival detection thực sự nằm trong process_partition (partition_worker.py),
    chạy TRƯỚC khi watermark có cơ hội drop bất kỳ điều gì.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_RAW)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", "20000")
        .option("kafka.metadata.max.age.ms", "10000")
        .load()
        .selectExpr("CAST(value AS STRING) AS raw_value")
        .select(F.from_json(F.col("raw_value"), RAW_RADIUS_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_timestamp_ts", F.to_timestamp(F.col("event_timestamp")))
        .withWatermark("event_timestamp_ts", WATERMARK_THRESHOLD)
    )


def make_callback():
    """
    foreachBatch callback: repartition theo msisdn -> foreachPartition
    (process_partition, định nghĩa trong partition_worker.py — module
    import-able, KHÔNG phải __main__, để cloudpickle pickle bằng tham
    chiếu thay vì bằng giá trị — xem chú thích [FIX-PICKLE] đầu file).

    Bọc try/except: nếu 1 batch lỗi ở tầng driver, log CRITICAL đầy đủ
    thay vì để exception bay thẳng ra làm crash query mà không rõ lý do.
    """

    def _callback(batch_df: DataFrame, batch_id: int) -> None:
        t0 = time.time()
        try:
            keyed = batch_df.repartition(VALIDATION_PARTITIONS, "msisdn")
            keyed.foreachPartition(process_partition)
            logger.info(
                "Batch %d dispatched to %d executor partitions, khong collect (%.0fms)",
                batch_id, VALIDATION_PARTITIONS, (time.time() - t0) * 1000,
            )
        except Exception:
            logger.critical(
                "Batch %d THAT BAI O TANG DRIVER (foreachBatch). Day la loi nghiem "
                "trong hon loi tung partition - kiem tra ket noi Kafka/Spark cluster. "
                "Query se tiep tuc thu batch tiep theo.",
                batch_id, exc_info=True,
            )
            if RAISE_ON_PARTITION_FAILURE:
                raise

    return _callback


class _ProgressListener(StreamingQueryListener):
    """
    Log tiến độ mỗi trigger NGAY TRONG job, để biết pipeline đang
    consume/xử lý bao nhiêu mà không cần poll offset từ bên ngoài, và để
    phát hiện ngay lập tức khi query bị terminate ngoài ý muốn.
    """

    def onQueryStarted(self, event):
        logger.info("StreamingQuery STARTED: id=%s name=%s", event.id, event.name)

    def onQueryProgress(self, event):
        p = event.progress
        try:
            num_input = p.numInputRows
            rate_in = p.inputRowsPerSecond
            rate_proc = p.processedRowsPerSecond
            batch_id = p.batchId
            duration = p.durationMs.get("triggerExecution") if p.durationMs else None
            logger.info(
                "HEARTBEAT batch=%s numInputRows=%s inputRowsPerSecond=%.1f "
                "processedRowsPerSecond=%.1f triggerExecutionMs=%s",
                batch_id, num_input, rate_in or 0.0, rate_proc or 0.0, duration,
            )
        except Exception:
            logger.debug("Khong parse duoc progress event: %s", p)

    def onQueryTerminated(self, event):
        if event.exception:
            logger.critical(
                "StreamingQuery TERMINATED VOI LOI: id=%s exception=%s",
                event.id, event.exception,
            )
        else:
            logger.info("StreamingQuery TERMINATED binh thuong: id=%s", event.id)

    def onQueryIdle(self, event):
        pass


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    local_cores = os.getenv("SPARK_LOCAL_CORES", "8")
    driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "4G")

    builder = (
        SparkSession.builder
        .appName("Camara-Processing-Job")
        .master(f"local[{local_cores}]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(VALIDATION_PARTITIONS))
        .config("spark.default.parallelism", str(VALIDATION_PARTITIONS))
        .config("spark.task.maxFailures", os.getenv("SPARK_TASK_MAX_FAILURES", "4"))
    )
    spark = configure_spark_jars(builder, KAFKA_PACKAGE).getOrCreate()

    spark.conf.set("spark.sql.streaming.checkpointLocation", CHECKPOINT_LOCATION)
    spark.streams.addListener(_ProgressListener())

    logger.info(
        "Spark session: local[%s], driver.memory=%s, partitions=%d (executor-based, "
        "khong collect) | process_partition tu module: %s",
        local_cores, driver_memory, VALIDATION_PARTITIONS,
        process_partition.__module__,
    )

    query = (
        build_stream(spark)
        .writeStream
        .foreachBatch(make_callback())
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="2000 milliseconds")
        .start()
    )

    try:
        query.awaitTermination()
    except Exception:
        logger.critical(
            "STREAMING QUERY DA DUNG VI LOI KHONG BAT DUOC. Day la ly do "
            "pipeline dung khi chua xu ly xong toan bo topic.",
            exc_info=True,
        )
        raise
    finally:
        close_all_worker_resources()


if __name__ == "__main__":
    main()