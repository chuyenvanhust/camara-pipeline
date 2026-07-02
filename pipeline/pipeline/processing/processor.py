#!/usr/bin/env python3
# Bootstrap sys.path
#pipeline\pipeline\processing\processor.py
import sys, os as _os
sys.path.insert(0, _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "..", "..")))

"""
Stage 2 — radius.raw → radius.clean

Gộp 4 bước xử lý trong 1 Spark Structured Streaming job:
  (a) Validation         : 6 rules — loại record lỗi → invalid_log
  (b) Late arrival check : đánh dấu trước watermark → invalid_log
  (c) Deduplication      : dict in-memory TTL 3600s theo (acct_session_id, acct_status_type)
  (d) Conflict resolution: phân loại A/B/C
        - A, B → conflict_log, loại khỏi luồng sạch
        - C    → giữ trong luồng sạch + gọi SwapDetector xác minh qua HLR/HSS → swap_event

Input : Kafka topic radius.raw
Output:
  - Kafka topic radius.clean       (record hợp lệ, bao gồm conflict C)
  - PostgreSQL invalid_log         (validation fail + late arrival)
  - PostgreSQL conflict_log        (conflict A/B)
  - PostgreSQL swap_event          (conflict C đã được HLR/HSS xác nhận)

===========================================================================
BOTTLENECK ĐÃ FIX (4 vấn đề, giữ nguyên từ bản gốc):

[BN-1] applyInPandasWithState bên trong foreachBatch
  → Spark KHÔNG hỗ trợ stateful operator trong foreachBatch callback.
  FIX: dict in-memory DEDUP_STATE trong driver process.

[BN-2] asyncio.new_event_loop() tạo mới mỗi batch, không đóng đúng cách
  FIX: dùng asyncio.run() — tự quản lý vòng đời loop.

[BN-3] httpx.AsyncClient() tạo mới MỖI RECORD
  FIX: 1 AsyncClient duy nhất PER PARTITION (xem BN-6 bên dưới — trước
  đây là "per batch", nay mỗi partition tự tạo client riêng vì chạy
  trên process khác nhau).

[BN-4] clean_df.count() sau khi đã .save() lên Kafka
  FIX: đếm bằng len(clean_rows) trước khi ghi.

[BN-5] Validation gọi API (GSMA/HLR/ITU) RIÊNG LẺ cho từng record
  (vd 200 record → tối đa 600 HTTP call/batch, TAC tra cứu lại dù đã biết).
  FIX: "Hybrid Prefetch" — trước khi validate, gom cả batch lại và gọi
  MỖI service đúng 1 lần (batch API), rồi validate từng record dựa trên
  kết quả đã prefetch. Xem pipeline/validation/rules.py::
  execute_validation_pipeline_batch() để biết chi tiết chiến lược.

[BN-6] .collect() TOÀN BỘ batch về driver rồi validate TUẦN TỰ trong 1
  process duy nhất — dù validate đã dùng Hybrid Prefetch (BN-5), toàn bộ
  công việc I/O-bound (gọi GSMA/HLR/ITU) vẫn chạy trên đúng 1 CPU core
  (driver), không tận dụng được nhiều core của máy (kể cả khi cấu hình
  local[N] với N lớn — trong kiến trúc CŨ, local[N] KHÔNG có tác dụng gì
  cho bước validate vì mọi thứ đã bị .collect() về 1 process trước khi
  validate chạy).

  FIX: tách VALIDATE ra khỏi phần .collect() ban đầu, dùng
  `batch_df.rdd.mapPartitions(_validate_partition)` để Spark tự chia
  batch thành N partition và validate chúng SONG SONG trên N Python
  worker subprocess (N quyết định bởi spark.sql.shuffle.partitions /
  repartition, chạy trên các luồng của local[N]). Đây MỚI là chỗ
  local[N] thực sự phát huy tác dụng, vì bước validate là I/O-bound
  (chờ mạng) — nhiều core/process chạy song song nhiều request HTTP
  cùng lúc sẽ giảm wall-clock time gần tuyến tính theo số partition.

  QUAN TRỌNG — TẠI SAO KHÔNG mapPartitions CHO CẢ DEDUP + CONFLICT
  RESOLUTION: 2 bước đó cần NHÌN THẤY TOÀN BỘ batch trong 1 nơi để
  đúng đắn:
    - Dedup dựa vào DEDUP_STATE (dict Python toàn cục, có TTL xuyên
      suốt nhiều batch). Nếu phân tán, mỗi worker process có 1 bản
      dict RIÊNG — 2 record trùng nhau rơi vào 2 partition khác nhau
      sẽ KHÔNG bị phát hiện là trùng → dữ liệu trùng lọt qua, SAI.
    - Conflict resolution cần groupby(session_id/imsi/msisdn) + sắp
      xếp theo thời gian trên TOÀN BỘ batch (pandas). Nếu 1 session bị
      chia sang 2 partition, phép so sánh "record trước đó" sẽ thiếu
      dữ liệu, phát hiện sai/sót conflict.
  => Do đó: SAU KHI mapPartitions validate xong, kết quả (đã gắn
  error_code, nhỏ hơn nhiều so với lúc trước vì record lỗi sẽ được lọc
  khỏi luồng dedup/conflict ngay sau đó) được .collect() về driver 1
  lần để làm dedup + conflict resolution TUẦN TỰ như cũ — phần này
  KHÔNG thể và KHÔNG NÊN phân tán.

  ĐÁNH ĐỔI CHẤP NHẬN ĐƯỢC: TAC_CACHE và circuit breaker (failed_counters)
  trong pipeline/validation/rules.py là global-per-process — khi chạy
  phân tán qua mapPartitions, mỗi worker process (Python subprocess do
  PySpark spawn) có bản cache/counter RIÊNG, không share giữa các
  partition. Hệ quả: TAC_CACHE kém hiệu quả hơn (gọi GSMA lặp lại nhiều
  hơn giữa các worker), circuit breaker "cục bộ" hơn (mỗi worker tự mở
  breaker độc lập). Đây KHÔNG phải lỗi đúng/sai — chỉ là giảm hiệu quả
  tối ưu hoá, kết quả validate vẫn ĐÚNG. Nếu muốn cache/breaker share
  toàn cục qua nhiều worker, cần store ngoài (Redis) — nằm ngoài phạm
  vi thay đổi này.

===========================================================================
GAP ĐÃ FIX (5 vấn đề, phát hiện khi báo cáo Data Quality luôn trống):

[GAP-1] invalid_rows chỉ ghi Kafka radius.invalid, không ai consume lại
  FIX: ghi trực tiếp vào PostgreSQL invalid_log trong cùng _callback,
       không cần round-trip qua Kafka.

[GAP-2] Conflict A/B bị .drop() trong pandas — biến mất, không ghi đâu cả
  FIX: _resolve_conflicts() tách riêng nhánh conflicted, ghi conflict_log.

[GAP-3] Late arrival bị Spark watermark âm thầm drop, không log được
  FIX: đánh dấu cờ late_arrival TRƯỚC khi watermark filter áp dụng,
       tách ra ghi invalid_log với error_code=ERR_LATE_ARRIVAL.

[GAP-4] SwapDetector tồn tại nhưng KHÔNG được import/gọi ở đâu cả
  → conflict C trôi thẳng vào radius_sessions, swap_event luôn trống.
  FIX: gọi SwapDetector.verify_and_emit_swap() cho từng conflict C row,
       ghi swap_event nếu HLR/HSS xác nhận.

[GAP-5] quality_report.py query sai tên error_code
  (ERR_IMEI_LUHN thay vì ERR_IMEI_LUHN_FAIL — sửa ở file quality_report.py riêng)
===========================================================================
"""

import os
import asyncio
import logging
import time
from typing import Dict, List, Optional, Tuple, Any, Iterator

import pandas as pd
import psycopg2
from pyspark.sql import SparkSession, DataFrame, Row
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType, BooleanType,
)
from pyspark.sql.window import Window

from pipeline.deduplication.state_manager import DedupStateManager
from pipeline.conflict_resolution.swap_detector import SwapDetector, write_swap_events
from pipeline.spark_jars import KAFKA_PACKAGE, configure_spark_jars

logger = logging.getLogger(__name__)

# ==============================================================================
# 1. CONSTANTS
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
KAFKA_TOPIC_CLEAN       = os.getenv("KAFKA_TOPIC_CLEAN", "radius.clean")
WATERMARK_THRESHOLD     = os.getenv("WATERMARK_VALIDATION", "7200 seconds")
CHECKPOINT_LOCATION     = os.getenv(
    "PROCESSING_CHECKPOINT_DIR",
    "/tmp/spark-pipeline-processing-checkpoint",
)

#: Ngưỡng coi là "late arrival" — record đến trễ hơn N giây so với event_timestamp.
#: Khớp với LATE_ARRIVAL_THRESHOLD_SECONDS trong README pipeline (3600s = 1h).
LATE_ARRIVAL_THRESHOLD_SECONDS = int(os.getenv("LATE_ARRIVAL_THRESHOLD_SECONDS", "3600"))

TTL_SECONDS = DedupStateManager.TTL_SECONDS  # 3600

#: DSN dùng chung cho mọi ghi PostgreSQL trong module này (invalid_log,
#: conflict_log, swap_event). writer.py (Stage 3) dùng DSN riêng cho
#: radius_sessions — không trùng connection pool.
DB_DSN = dict(
    host=os.getenv("DB_HOST", "postgres"),
    port=int(os.getenv("DB_PORT", "5432")),
    dbname=os.getenv("DB_NAME", "camara_db"),
    user=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASSWORD", "camara"),
)

HLR_HSS_URL = os.getenv("HLR_HSS_SERVICE_URL", "http://camara-mock-hlr-hss:8200")

# Singleton SwapDetector — tái sử dụng giữa các batch, tránh tạo lại
# HTTP session mỗi lần (xem swap_detector.py). Chỉ dùng ở driver (bước
# conflict C xử lý tuần tự), KHÔNG dùng trong mapPartitions.
_swap_detector = SwapDetector(hlr_mock_url=HLR_HSS_URL)

#: [BN-6] Số partition Spark dùng để chia batch trước khi mapPartitions
#: validate. Đây là "độ song song" thực sự cho bước validate — nên đặt
#: bằng (hoặc gần bằng) số CPU core khả dụng cho local[N]. Mặc định 4
#: (an toàn cho máy yếu); tăng lên bằng số core thật của máy (vd 8, 12)
#: qua env var để tận dụng đa nhân mà KHÔNG cần sửa code — đúng tinh
#: thần "nhất quán, mang sang máy khác chỉ cần đổi env" của kế hoạch
#: song song hoá.
VALIDATION_PARTITIONS = int(os.getenv("VALIDATION_PARTITIONS", "8"))


# ==============================================================================
# 2. PURE LOGIC
# ==============================================================================

# ── 2a. Validation (mapPartitions — chạy phân tán trên N worker) ───────────

def _validate_partition(rows_iter: Iterator[Row]) -> Iterator[Dict[str, Any]]:
    """
    [BN-6] Hàm chạy TRÊN TỪNG PARTITION, trong 1 Python worker subprocess
    RIÊNG BIỆT do PySpark tự spawn (kể cả ở local[N], mapPartitions vẫn
    chạy qua worker subprocess thật, không chỉ là thread — nên đây là
    đa TIẾN TRÌNH thật, tận dụng nhiều core CPU vật lý).

    Vì chạy trên process riêng, KHÔNG được closure qua bất kỳ object nào
    tạo ở driver (httpx.AsyncClient, DB connection...) — mọi thứ (kể cả
    import module) phải được tạo/import MỚI bên trong hàm này.

    Import trễ (deferred import) + tự bootstrap sys.path bên trong hàm:
    worker subprocess là 1 process Python HOÀN TOÀN MỚI, không tự thừa
    hưởng `sys.path.insert(...)` mà driver đã chạy lúc import module —
    nếu không tự bootstrap lại ở đây, `import pipeline.validation.rules`
    sẽ ném ModuleNotFoundError trên worker. Cách này tự chứa (self
    contained), hoạt động đúng bất kể PYTHONPATH của container worker có
    được set sẵn hay không.

    Args:
        rows_iter: iterator các pyspark.sql.Row của 1 partition.

    Yields:
        dict — record gốc + (nếu invalid) error_code/error_details,
        hoặc (nếu valid) warn_code nếu có circuit breaker bypass.
        KHÔNG lọc valid/invalid ở đây — driver sẽ tự tách sau khi
        collect() (xem _split_validated).
    """
    records = [r.asDict() for r in rows_iter]
    if not records:
        return iter([])

    # --- Bootstrap sys.path + import trễ, chỉ chạy trong worker process ---
    import sys as _sys, os as _os2
    _project_root = _os2.path.abspath(
        _os2.path.join(_os2.path.dirname(__file__), "..", "..")
    )
    if _project_root not in _sys.path:
        _sys.path.insert(0, _project_root)

    import asyncio as _asyncio
    import httpx as _httpx
    from pipeline.validation.rules import execute_validation_pipeline_batch

    async def _run():
        limits = _httpx.Limits(max_connections=20, max_keepalive_connections=10)
        async with _httpx.AsyncClient(limits=limits) as client:
            return await execute_validation_pipeline_batch(records, client)

    try:
        results = _asyncio.run(_run())
    except Exception:
        # An toàn: nếu 1 partition gặp lỗi bất ngờ (vd mạng đứt hoàn
        # toàn), KHÔNG để cả Spark task/batch fail — coi toàn bộ record
        # trong partition này là lỗi hạ tầng, vẫn trả về (ghi invalid_log)
        # thay vì làm sập cả micro-batch.
        logger.exception(
            "mapPartitions validate: loi khong luong truoc, danh dau %d "
            "record trong partition nay la ERR_PARTITION_VALIDATION_FAILED",
            len(records),
        )
        for record in records:
            payload = dict(record)
            payload["error_code"] = "ERR_PARTITION_VALIDATION_FAILED"
            payload["error_details"] = "Unhandled exception during partition validation"
            yield payload
        return

    for record, (res, warn) in zip(records, results):
        payload = dict(record)
        if res.is_valid:
            if warn:
                payload["warn_code"] = warn
        else:
            payload["error_code"] = res.error_code
            payload["error_details"] = res.error_message
        yield payload


def _split_validated(annotated_rows: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """
    Tách rows đã được _validate_partition gắn nhãn thành (valid, invalid),
    dựa trên viec co "error_code" hay khong (thay cho _filter_valid cu,
    vi annotation gio duoc gan tu trong mapPartitions, khong con
    ValidationResult object o day nua sau khi collect ve driver).
    """
    valid: List[Dict] = []
    invalid: List[Dict] = []
    for row in annotated_rows:
        if row.get("error_code"):
            invalid.append(row)
        else:
            valid.append(row)
    return valid, invalid


# ── 2b. Late arrival detection ──────────────────────────────────────────────

def _split_late_arrival(records: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """
    [GAP-3] Tách record đến trễ hơn LATE_ARRIVAL_THRESHOLD_SECONDS so với
    event_timestamp, TRƯỚC khi Spark watermark có cơ hội âm thầm drop nó.

    So sánh ingest_timestamp (lúc simulator/producer gửi đi) với
    event_timestamp (lúc sự kiện RADIUS thực sự xảy ra). Cả 2 đều là
    Unix timestamp dạng string trong RAW_RADIUS_SCHEMA.

    Returns:
        (on_time_records, late_records)
        late_records có shape khớp invalid_log: acct_session_id, msisdn,
        error_code='ERR_LATE_ARRIVAL', error_details mô tả độ trễ.
    """
    on_time: List[Dict] = []
    late: List[Dict] = []

    for r in records:
        try:
            event_ts = int(str(r.get("event_timestamp", "")).strip())
            ingest_ts = int(str(r.get("ingest_timestamp", "")).strip())
            delay_seconds = ingest_ts - event_ts
        except (ValueError, TypeError):
            # Không parse được timestamp -> để nguyên, validation rule khác
            # (ERR_INVALID_TIMESTAMP) sẽ bắt lỗi này, không phải late arrival.
            on_time.append(r)
            continue

        if delay_seconds > LATE_ARRIVAL_THRESHOLD_SECONDS:
            payload = dict(r)
            payload["error_code"] = "ERR_LATE_ARRIVAL"
            payload["error_details"] = (
                f"Delayed {delay_seconds}s "
                f"(threshold={LATE_ARRIVAL_THRESHOLD_SECONDS}s), "
                f"event_timestamp={event_ts}, ingest_timestamp={ingest_ts}"
            )
            late.append(payload)
        else:
            on_time.append(r)

    return on_time, late


# ── 2c. Deduplication (in-memory, driver-side, BẮT BUỘC tuần tự) ───────────
#
# [FIX BN-1] Thay applyInPandasWithState bằng dict in-memory.
# DEDUP_STATE: { (acct_session_id, acct_status_type) -> last_seen_epoch_ms }
#
# [BN-6] KHÔNG được chuyển bước này sang mapPartitions — xem giải thích
# đầy đủ trong docstring đầu file. DEDUP_STATE phải là 1 dict DUY NHẤT
# trong 1 process để phát hiện đúng record trùng.

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

    if time.time() - _DEDUP_LAST_CLEANUP > _DEDUP_CLEANUP_INTERVAL:
        _dedup_cleanup_expired()

    now_ms = int(time.time() * 1000)
    ttl_ms = TTL_SECONDS * 1000
    unique: List[Dict] = []

    for record in records:
        key = (
            str(record.get("acct_session_id", "")),
            str(record.get("acct_status_type", "")),
        )
        last_ms = DEDUP_STATE.get(key)
        ts_raw = record.get("event_timestamp", "")
        try:
            event_ms = int(str(ts_raw).strip()) * 1000
        except (ValueError, TypeError):
            event_ms = now_ms

        if last_ms is None or (event_ms - last_ms) > ttl_ms:
            DEDUP_STATE[key] = event_ms
            unique.append(record)
        # else: duplicate -> bỏ

    return unique


# ── 2d. Conflict resolution (pandas, driver-side, BẮT BUỘC tuần tự) ────────

def _resolve_conflicts(records: List[Dict]) -> Tuple[List[Dict], List[Dict], List[Dict]]:
    """
    Phân loại conflict A/B/C bằng pandas (đã collect() rồi, batch nhỏ).

    Conflict A: cùng session, imsi/msisdn thay đổi sau Start.
    Conflict B: cùng imsi có 2+ Start chưa Stop.
    Conflict C: cùng msisdn nhưng imsi thay đổi (SIM Swap signal).

    [BN-6] KHÔNG được chuyển bước này sang mapPartitions — groupby +
    so sánh thời gian cần TOÀN BỘ batch trong 1 nơi (xem giải thích đầu
    file).

    [GAP-2] [GAP-4] Trả về 3 nhóm thay vì chỉ "clean":
        clean_records       — record sạch, BAO GỒM conflict C (đúng thiết kế
                               nghiệp vụ: C là business event hợp lệ, không
                               phải lỗi dữ liệu).
        conflict_records     — chỉ A/B, khớp schema bảng conflict_log
                               (session_id, conflict_type, details, error_code).
        conflict_c_records   — chỉ C, dùng để feed cho SwapDetector xác minh
                               qua HLR/HSS trước khi ghi swap_event.
    """
    if not records:
        return [], [], []

    df = pd.DataFrame(records)

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
# ── Conflict B ──────────────────────────────────────────────────────────
    # [FIX] Bản cũ flag MỌI Start thứ 2 trở đi của cùng imsi bằng cumcount(),
    # kể cả khi Start trước đó ĐÃ Stop từ lâu -> sai định nghĩa README
    # ("2 Start CHƯA có Stop tương ứng"). Sửa: duyệt tuần tự theo thời gian
    # cho từng imsi, theo dõi tập session_id đang "mở" (đã Start, chưa Stop).
    # Một Start mới chỉ là Conflict B khi tại thời điểm đó imsi này đang có
    # >=1 session KHÁC còn mở (double active thật sự, có chồng lấn thời gian).
    def _flag_conflict_b(group: pd.DataFrame) -> pd.Series:
        open_sessions: set = set()
        flags = []
        for _, row in group.iterrows():
            if row["is_conflict_a"]:
                flags.append(False)
                continue
            status, sess_id = row["acct_status_type"], row["acct_session_id"]
            if status == "Start":
                if sess_id in open_sessions:
                    flags.append(False)  # duplicate Start của chính session này, không phải B
                elif len(open_sessions) > 0:
                    flags.append(True)   # đã có session khác mở -> double active
                else:
                    flags.append(False)
                    open_sessions.add(sess_id)
            elif status in ("Stop", "Interim"):
                open_sessions.discard(sess_id)
                flags.append(False)
            else:
                flags.append(False)
        return pd.Series(flags, index=group.index)

    if df.empty:
        df["is_conflict_b"] = False
    else:
        df["is_conflict_b"] = (
            df.groupby("msisdn" if False else "imsi", group_keys=False)  # group theo imsi (đúng README B)
              .apply(_flag_conflict_b)
              .reindex(df.index)
              .fillna(False)
        )

    # ── Conflict C ──────────────────────────────────────────────────────────
    # [FIX] Chỉ tính prev_imsi từ các bản ghi KHÔNG phải conflict A/B, tránh
    # so sánh với 1 imsi đã "hỏng" do Conflict A (session inconsistency) gây
    # false positive/negative cho C.
    legit_mask = (~df["is_conflict_a"]) & (~df["is_conflict_b"])
    df["prev_imsi"] = df[legit_mask].groupby("msisdn")["imsi"].shift(1)
    df["is_conflict_c"] = (
        legit_mask &
        df["prev_imsi"].notna() &
        (df["imsi"] != df["prev_imsi"])
    )

    # ── Tách 3 nhóm ───────────────────────────────────────────────────────────
    is_conflicted_ab = df["is_conflict_a"] | df["is_conflict_b"]

    clean = df[~is_conflicted_ab].copy()

    # Conflict C nằm TRONG clean (giữ nguyên thiết kế) — lấy riêng ra để
    # feed SwapDetector mà không loại khỏi luồng sạch.
    conflict_c_records: List[Dict] = []
    if "is_conflict_c" in clean.columns:
        c_only = clean[clean["is_conflict_c"] == True].copy()
        if not c_only.empty:
            conflict_c_records = c_only.drop(
                columns=[
                    "first_imsi", "first_msisdn", "is_conflict_a",
                    "is_conflict_b", "_imsi_start_rank", "prev_imsi",
                    "is_conflict_c", "event_timestamp_ts",
                ],
                errors="ignore",
            ).to_dict(orient="records")

    clean = clean.drop(
        columns=[
            "first_imsi", "first_msisdn", "is_conflict_a",
            "is_conflict_b", "_imsi_start_rank", "prev_imsi", "is_conflict_c",
            "event_timestamp_ts",
        ],
        errors="ignore",
    )

    conflicted_ab = df[is_conflicted_ab].copy()
    conflict_records: List[Dict] = []
    if not conflicted_ab.empty:
        conflicted_ab["conflict_type"] = conflicted_ab.apply(
            lambda r: "A" if r["is_conflict_a"] else "B", axis=1
        )
        conflicted_ab["details"] = conflicted_ab.apply(
            lambda r: (
                f"Session {r['acct_session_id']}: IMSI/MSISDN thay đổi sau Start"
                if r["is_conflict_a"]
                else f"IMSI {r['imsi']}: 2+ Start chưa Stop"
            ),
            axis=1,
        )
        conflicted_ab["error_code"] = conflicted_ab["conflict_type"].apply(
            lambda t: f"CONFLICT_{t}"
        )
        conflicted_ab = conflicted_ab.rename(columns={"acct_session_id": "session_id"})
        conflict_records = conflicted_ab[
            ["session_id", "conflict_type", "details", "error_code"]
        ].to_dict(orient="records")

    return clean.to_dict(orient="records"), conflict_records, conflict_c_records


# ── 2e. PostgreSQL writers (invalid_log, conflict_log) ─────────────────────
#
# [GAP-1] [GAP-2] Ghi trực tiếp vào PostgreSQL trong cùng foreachBatch,
# không cần round-trip qua Kafka. swap_event writer nằm trong
# swap_detector.py (write_swap_events) vì nó cần I/O đồng bộ với HLR/HSS.

def _write_invalid_log(rows: List[Dict]) -> None:
    """
    Ghi invalid_rows (validation fail + late arrival) vào bảng invalid_log.
    Khớp đúng cột trong storage/migrations/001_init_schema.sql:
        session_id, msisdn, error_code, details
    """
    if not rows:
        return

    sql = """
        INSERT INTO invalid_log (session_id, msisdn, error_code, details)
        VALUES (%s, %s, %s, %s)
    """
    data = [
        (
            r.get("acct_session_id"),
            r.get("msisdn"),
            r.get("error_code"),
            r.get("error_details"),
        )
        for r in rows
    ]

    conn = psycopg2.connect(**DB_DSN)
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, data)
        conn.commit()
        logger.info("Wrote %d rows to invalid_log", len(data))
    except Exception:
        conn.rollback()
        logger.exception("Failed to write invalid_log")
        raise
    finally:
        conn.close()


def _write_conflict_log(rows: List[Dict]) -> None:
    """
    Ghi conflict A/B vào bảng conflict_log.
    Khớp đúng cột trong storage/migrations/001_init_schema.sql:
        session_id, conflict_type, details, error_code
    """
    if not rows:
        return

    sql = """
        INSERT INTO conflict_log (session_id, conflict_type, details, error_code)
        VALUES (%s, %s, %s, %s)
    """
    data = [
        (r["session_id"], r["conflict_type"], r["details"], r["error_code"])
        for r in rows
    ]

    conn = psycopg2.connect(**DB_DSN)
    try:
        with conn.cursor() as cur:
            cur.executemany(sql, data)
        conn.commit()
        logger.info("Wrote %d rows to conflict_log", len(data))
    except Exception:
        conn.rollback()
        logger.exception("Failed to write conflict_log")
        raise
    finally:
        conn.close()


def _process_sim_swap_signals(conflict_c_rows: List[Dict]) -> None:
    """
    [GAP-4] Với mỗi record conflict C, gọi HLR/HSS xác nhận lịch sử IMSI
    qua SwapDetector, ghi swap_event nếu được xác nhận.

    I/O đồng bộ (requests, không async) — chạy tuần tự, chấp nhận được
    vì conflict C hiếm trong dataset (xem README: tổng conflict mặc định
    1%, trong đó C chỉ là 1 phần của 3 loại A/B/C).
    """
    if not conflict_c_rows:
        return

    confirmed_events: List[Dict] = []
    for row in conflict_c_rows:
        try:
            event = _swap_detector.verify_and_emit_swap(row)
            if event:
                confirmed_events.append(event)
        except Exception:
            logger.exception(
                "SwapDetector failed for msisdn=%s imsi=%s",
                row.get("msisdn"), row.get("imsi"),
            )

    if confirmed_events:
        write_swap_events(confirmed_events, DB_DSN)
        logger.info(
            "Confirmed %d/%d SIM Swap events -> swap_event",
            len(confirmed_events), len(conflict_c_rows),
        )
    else:
        logger.info(
            "0/%d conflict C records confirmed by HLR/HSS (false positive or "
            "HLR unreachable)",
            len(conflict_c_rows),
        )


# ==============================================================================
# 3. SPARK I/O
# ==============================================================================

def build_stream(spark: SparkSession) -> DataFrame:
    """
    Đọc radius.raw, parse JSON theo RAW_RADIUS_SCHEMA, áp watermark.

    Watermark chỉ ảnh hưởng tới việc Spark dọn state nội bộ (không liên
    quan tới _split_late_arrival ở pure logic) — late arrival detection
    thực sự nằm trong make_callback._callback, chạy TRƯỚC khi watermark
    có cơ hội drop bất kỳ điều gì.
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


def make_callback(spark: SparkSession):
    """
    foreachBatch callback: mapPartitions-validate (SONG SONG) -> collect
    -> late-arrival split -> dedup -> conflict resolution (TUẦN TỰ) ->
    ghi 4 đích (Kafka + 3 bảng Postgres).
    """

    def _callback(batch_df: DataFrame, batch_id: int) -> None:
        t0 = time.time()

        # ── 1. Validation — SONG SONG qua mapPartitions ─────────────────────
        # [BN-6] KHÔNG .collect() truoc khi validate nua. Repartition batch
        # thanh VALIDATION_PARTITIONS phan de Spark chia deu cong viec HTTP
        # (I/O-bound) cho nhieu worker subprocess chay dong thoi, tan dung
        # nhieu core CPU that su -- day la cho local[N] thuc su co tac dung
        # (khac voi kien truc cu, moi thu da bi .collect() truoc khi validate).
        #
        # repartition() voi batch nho hon VALIDATION_PARTITIONS van hoat dong
        # dung, chi la mot so partition rong -> mapPartitions bo qua rong,
        # khong ton hao gi dang ke.
        repartitioned = batch_df.repartition(VALIDATION_PARTITIONS)
        annotated_rows = repartitioned.rdd.mapPartitions(_validate_partition).collect()

        if not annotated_rows:
            return

        # ── 2. Tach valid / invalid tu ket qua da annotate ──────────────────
        valid_rows, invalid_rows = _split_validated(annotated_rows)

        # ── 3. Late arrival — tách khỏi valid_rows TRƯỚC dedup/conflict ──────
        on_time_rows, late_rows = _split_late_arrival(valid_rows)

        logger.info(
            "Batch %d | total=%d valid=%d invalid=%d late_arrival=%d "
            "(validate_partitions=%d, %.0fms)",
            batch_id, len(annotated_rows), len(valid_rows), len(invalid_rows),
            len(late_rows), VALIDATION_PARTITIONS, (time.time() - t0) * 1000,
        )

        # ── 4. Ghi invalid_log: validation fail + late arrival ───────────────
        all_invalid_rows = invalid_rows + late_rows
        if all_invalid_rows:
            try:
                _write_invalid_log(all_invalid_rows)
            except Exception as e:
                logger.error("Batch %d | Failed to write invalid_log: %s", batch_id, e)

        if not on_time_rows:
            return

        # ── 5. Deduplication (TUẦN TỰ, driver-side — xem BN-6) ───────────────
        deduped_rows = _dedup_filter(on_time_rows)
        logger.info("Batch %d | after dedup=%d", batch_id, len(deduped_rows))
        if not deduped_rows:
            return

        # ── 6. Conflict resolution (TUẦN TỰ, driver-side — xem BN-6) ─────────
        clean_rows, conflict_rows, conflict_c_rows = _resolve_conflicts(deduped_rows)

        # ── 7. Ghi conflict_log (A/B) ─────────────────────────────────────────
        if conflict_rows:
            try:
                _write_conflict_log(conflict_rows)
            except Exception as e:
                logger.error("Batch %d | Failed to write conflict_log: %s", batch_id, e)

        # ── 8. Xác minh SIM Swap (C) qua HLR/HSS, ghi swap_event ──────────────
        if conflict_c_rows:
            try:
                _process_sim_swap_signals(conflict_c_rows)
            except Exception as e:
                logger.error(
                    "Batch %d | Failed to process SIM Swap signals: %s", batch_id, e
                )

        if not clean_rows:
            return

        # ── 9. Ghi radius.clean (record sạch, bao gồm conflict C) ────────────
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

        logger.info(
            "Batch %d | -> clean=%d conflict_ab=%d conflict_c=%d invalid=%d "
            "late=%d (total %.0fms)",
            batch_id, len(clean_rows), len(conflict_rows), len(conflict_c_rows),
            len(invalid_rows), len(late_rows), (time.time() - t0) * 1000,
        )

    return _callback


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    #: [BN-6] So luong "luong" local Spark dung -- nay MOI thuc su co y
    #: nghia vi mapPartitions validate can nhieu worker subprocess song
    #: song. Dat qua env SPARK_LOCAL_CORES de linh hoat theo may (vd 4
    #: tren laptop yeu, 12 tren may nhieu nhan) MA KHONG can sua code --
    #: nen dong bo voi VALIDATION_PARTITIONS o tren (nen bang hoac gan
    #: bang nhau de khong "thua" partition khong co luong xu ly).
    local_cores = os.getenv("SPARK_LOCAL_CORES", "8")
    #: spark.executor.memory KHONG duoc dat o day vi VO NGHIA trong che
    #: do local[N] (khong co executor process rieng, driver va "executor"
    #: dung chung 1 JVM) -- chi spark.driver.memory co tac dung thuc te.
    driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "4G")

    builder = (
        SparkSession.builder
        .appName("Camara-Processing-Job")
        .master(f"local[{local_cores}]")
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", str(VALIDATION_PARTITIONS))
        .config("spark.default.parallelism", str(VALIDATION_PARTITIONS))
    )
    spark = configure_spark_jars(builder, KAFKA_PACKAGE).getOrCreate()

    spark.conf.set(
        "spark.sql.streaming.checkpointLocation",
        CHECKPOINT_LOCATION,
    )

    logger.info(
        "Spark session: local[%s], driver.memory=%s, validation_partitions=%d",
        local_cores, driver_memory, VALIDATION_PARTITIONS,
    )

    query = (
        build_stream(spark)
        .writeStream
        .foreachBatch(make_callback(spark))
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime="2000 milliseconds")
        .start()
    )


    query.awaitTermination()


if __name__ == "__main__":
    main()