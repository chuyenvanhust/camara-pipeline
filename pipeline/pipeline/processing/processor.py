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
  (d) Conflict resolution: phân loại A/B/C/D
        - A, B → conflict_log, loại khỏi luồng sạch
        - C, D → giữ trong luồng sạch + gọi SwapDetector xác minh qua HLR/HSS → swap_event
                 (candidate C/D được phát hiện bằng cách so sánh với
                 trạng thái GLOBAL lưu trong Redis — xuyên suốt mọi
                 batch — KHÔNG còn giới hạn trong 1 micro-batch)

Input : Kafka topic radius.raw
Output:
  - Kafka topic radius.clean       (record hợp lệ, bao gồm conflict C)
  - PostgreSQL invalid_log         (validation fail + late arrival)
  - PostgreSQL conflict_log        (conflict A/B)
  - PostgreSQL swap_event          (conflict C đã được HLR/HSS xác nhận)


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
from pipeline.state.redis_state_manager import RedisStateManager
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
from pipeline.conflict_resolution.swap_detector import SwapDetector, write_swap_events

_sim_swap_detector = SwapDetector(identity_type="imsi", hlr_mock_url=HLR_HSS_URL)
_device_swap_detector = SwapDetector(identity_type="imei", hlr_mock_url=HLR_HSS_URL)

VALIDATION_PARTITIONS = int(os.getenv("VALIDATION_PARTITIONS", "8"))

# ── Redis Global State Store (Conflict C/D — xem state/redis_state_manager.py) ──
# [FIX-4] Trước đây Conflict C/D dùng df.groupby("msisdn").shift(1) của
# Pandas -> chỉ "nhìn thấy" 2 bản ghi NẰM CHUNG 1 micro-batch (~2s), bỏ
# sót >95% swap thật (swap thường cách nhau vài phút -> vài ngày) và mất
# sạch "trí nhớ" mỗi khi Spark job restart. Thay bằng Redis: trạng thái
# last_imsi/last_imei của MỌI msisdn được lưu bền, xuyên-batch, sống sót
# qua restart. Mọi lệnh đọc/ghi đều đi qua pipeline() theo LÔ
# REDIS_BATCH_SIZE (200-500), KHÔNG gọi lẻ từng msisdn.
REDIS_HOST = os.getenv("REDIS_HOST", "camara-redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_BATCH_SIZE = int(os.getenv("REDIS_BATCH_SIZE", "300"))

_redis_state = RedisStateManager(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    batch_size=REDIS_BATCH_SIZE,
)


# ==============================================================================
# 2. PURE LOGIC
# ==============================================================================

# ── 2a. Validation (mapPartitions — chạy phân tán trên N worker) ───────────

def _validate_partition(rows_iter: Iterator[Row]) -> Iterator[Dict[str, Any]]:
    """
   

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

def _resolve_conflicts(
    records: List[Dict],
    redis_state: Optional[RedisStateManager] = None,
) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
    """
    Phân loại conflict A/B/C/D. CẢ 4 LOẠI đều so sánh với trạng thái
    TOÀN CỤC lưu trong Redis (xem module docstring + [FIX-4]) — không
    còn loại nào bị giới hạn trong phạm vi 1 micro-batch.

    Conflict A: bản ghi non-Start có imsi/msisdn khác baseline Start ĐÃ LƯU
                (Redis hoặc trong cùng batch) của session đó.
    Conflict B: IMSI đang có 1 session khác CHƯA ĐÓNG (đã Start, chưa
                Stop/Interim) — kiểm tra theo trạng thái GLOBAL, không chỉ
                trong batch hiện tại.
    Conflict C: msisdn xuất hiện IMSI khác với last_imsi đã lưu (SIM Swap signal).
    Conflict D: msisdn xuất hiện IMEI khác với last_imei đã lưu (Device Swap signal).

    [FIX-4] CẢ 4 loại conflict trước đây đều mắc cùng 1 lỗi kiến trúc:
    coi Structured Streaming là dữ liệu tĩnh trong 1 micro-batch (~2s),
    dùng .shift()/.groupby()/biến cục bộ bị reset mỗi lần gọi hàm. Vì
    Start/Stop của 1 session, hay 2 lần Start cùng IMSI, hay swap SIM/máy
    trong thực tế đều có thể cách nhau vài phút -> vài ngày (vượt xa 1
    batch), toàn bộ 4 loại đều bị False Negative nghiêm trọng. Từ nay cả
    4 đều so sánh với trạng thái TOÀN CỤC lưu trong Redis (xem
    state/redis_state_manager.py), sống xuyên-batch và sống sót qua
    restart job — mọi truy vấn Redis đều theo LÔ 200-500 record/round-trip.

    Args:
        redis_state: RedisStateManager dùng để fetch/update trạng thái
            toàn cục (session baseline, open sessions, last imsi/imei).
            None -> bỏ qua so sánh Redis, mọi conflict coi msisdn/session
            là "lần đầu thấy" trong phạm vi records truyền vào (dùng cho
            unit test thuần pandas, không cần Redis chạy).
    """
    if not records:
        return [], [], [], []

    df = pd.DataFrame(records)

    if "event_timestamp_ts" not in df.columns:
        df["event_timestamp_ts"] = pd.to_datetime(
            df["event_timestamp"].apply(
                lambda x: int(x) if str(x).strip().isdigit() else None
            ),
            unit="s", utc=True, errors="coerce",
        )

    df = df.sort_values("event_timestamp_ts").reset_index(drop=True)

    # ── FETCH state toàn cục cho A + B (2 round-trip theo LÔ, không gọi lẻ) ──
    session_ids_all = [s for s in df["acct_session_id"].dropna().unique().tolist() if s]
    imsis_all = [s for s in df["imsi"].dropna().unique().tolist() if s]

    redis_session_baseline = (
        redis_state.fetch_session_baselines(session_ids_all) if redis_state else {}
    )
    redis_open_sessions = (
        redis_state.fetch_open_sessions(imsis_all) if redis_state else {}
    )

    # session_id -> {"imsi":..., "msisdn":...} — baseline Start "sự thật",
    # nạp từ Redis rồi bổ sung dần theo record MỚI trong batch này.
    session_baseline_state: Dict[str, Dict[str, Optional[str]]] = {
        sid: {"imsi": v.get("start_imsi"), "msisdn": v.get("start_msisdn")}
        for sid, v in redis_session_baseline.items()
    }
    # imsi -> set(session_id đang mở) — nạp từ Redis rồi cập nhật dần.
    open_sessions_state: Dict[str, set] = {
        imsi: set(v) for imsi, v in redis_open_sessions.items()
    }

    new_session_baselines: Dict[str, Dict[str, Optional[str]]] = {}
    touched_imsis: set = set()
    conflict_a_flags: Dict[Any, bool] = {}
    conflict_b_flags: Dict[Any, bool] = {}

    # ── 1 PASS TUẦN TỰ THEO THỜI GIAN cho Conflict A + B ─────────────────────
    # Bắt buộc tuần tự (không vectorize được) vì B phụ thuộc kết quả A của
    # CHÍNH record đó, và cả A lẫn B đều phụ thuộc trạng thái đang "chạy"
    # được cập nhật record-by-record trong cùng batch.
    for idx, row in df.iterrows():
        sid = row["acct_session_id"]
        imsi = row["imsi"]
        msisdn = row["msisdn"]
        status = row["acct_status_type"]

        # ---- Conflict A ----
        baseline = session_baseline_state.get(sid) if sid else None
        if baseline is None:
            # Lần đầu tiên toàn hệ thống thấy session này (không có trong
            # Redis, cũng chưa xuất hiện trước đó trong batch) -> LẤY LÀM
            # baseline, không tính là conflict (kể cả nếu status != Start,
            # do dữ liệu vào muộn/mất Start — không thể làm gì tốt hơn).
            is_a = False
            if sid:
                session_baseline_state[sid] = {"imsi": imsi, "msisdn": msisdn}
                if sid not in redis_session_baseline:
                    new_session_baselines[sid] = {"start_imsi": imsi, "start_msisdn": msisdn}
        else:
            is_a = bool(
                status != "Start"
                and ((imsi != baseline["imsi"]) or (msisdn != baseline["msisdn"]))
            )
        conflict_a_flags[idx] = is_a

        # ---- Conflict B (chỉ xét trên record KHÔNG bị conflict A) ─────────
        is_b = False
        if not is_a and imsi:
            open_set = open_sessions_state.setdefault(imsi, set())
            if status == "Start":
                if sid in open_set:
                    pass  # Start lặp lại của chính session đó -> không phải conflict
                elif len(open_set) > 0:
                    is_b = True  # IMSI này đang có 1 session KHÁC chưa đóng
                else:
                    open_set.add(sid)
            elif status in ("Stop", "Interim"):
                open_set.discard(sid)
            touched_imsis.add(imsi)
        conflict_b_flags[idx] = is_b

    df["is_conflict_a"] = df.index.map(lambda i: conflict_a_flags.get(i, False)).astype(bool)
    df["is_conflict_b"] = df.index.map(lambda i: conflict_b_flags.get(i, False)).astype(bool)

    # Ghi lại state A/B mới về Redis — theo LÔ, không gọi lẻ.
    if redis_state:
        if new_session_baselines:
            redis_state.save_session_baselines([
                {"session_id": sid, **v} for sid, v in new_session_baselines.items()
            ])
        if touched_imsis:
            redis_state.save_open_sessions({
                imsi: open_sessions_state.get(imsi, set()) for imsi in touched_imsis
            })

    # ── Conflict C/D (SIM/Device Swap) — SO SÁNH VỚI REDIS GLOBAL STATE ──────
    # Baseline so sánh = last_imsi/last_imei của msisdn, LẤY TỪ REDIS
    # (một lần fetch theo LÔ 200-500 msisdn/round-trip cho toàn bộ batch
    # — KHÔNG gọi lẻ), sau đó "chạy" tuần tự theo thời gian NGAY TRONG
    # batch hiện tại để vẫn bắt được swap xảy ra 2 lần trong cùng batch.
    legit_mask = (~df["is_conflict_a"]) & (~df["is_conflict_b"])
    legit_df = df[legit_mask].sort_values("event_timestamp_ts")

    unique_msisdns = [m for m in legit_df["msisdn"].dropna().unique().tolist() if m]
    redis_prev_state = redis_state.fetch_batch(unique_msisdns) if redis_state else {}

    # running_state: nạp baseline từ Redis, rồi cập nhật dần theo thứ tự
    # thời gian của batch hiện tại.
    running_state: Dict[str, Dict[str, Optional[str]]] = {
        msisdn: {"imsi": prev.get("last_imsi") or None, "imei": prev.get("last_imei") or None}
        for msisdn, prev in redis_prev_state.items()
    }

    conflict_c_flags: Dict[Any, bool] = {}
    conflict_d_flags: Dict[Any, bool] = {}
    redis_updates: Dict[str, Dict] = {}  # msisdn -> state MỚI NHẤT trong batch này

    for idx, row in legit_df.iterrows():
        msisdn = row["msisdn"]
        if not msisdn:
            conflict_c_flags[idx] = False
            conflict_d_flags[idx] = False
            continue

        state = running_state.setdefault(msisdn, {"imsi": None, "imei": None})
        cur_imsi, cur_imei = row["imsi"], row["imei"]

        # Chỉ tính là conflict khi ĐÃ CÓ baseline trước đó (từ Redis hoặc
        # từ 1 record khác đứng trước trong cùng batch) và giá trị mới
        # THỰC SỰ khác — msisdn lần đầu xuất hiện (baseline=None) không
        # phải là swap, chỉ là "đăng ký" IMSI/IMEI lần đầu.
        conflict_c_flags[idx] = bool(
            state["imsi"] is not None and cur_imsi is not None and cur_imsi != state["imsi"]
        )
        conflict_d_flags[idx] = bool(
            state["imei"] is not None and cur_imei is not None and cur_imei != state["imei"]
        )

        # Dù bị đánh dấu conflict hay không, IMSI/IMEI MỚI vẫn trở thành
        # trạng thái hiện hành cho record kế tiếp (đúng hành vi thực tế
        # sau swap) — cả trong running_state lẫn state sẽ ghi lại Redis.
        if cur_imsi is not None:
            state["imsi"] = cur_imsi
        if cur_imei is not None:
            state["imei"] = cur_imei

        redis_updates[msisdn] = {
            "msisdn": msisdn,
            "last_imsi": state["imsi"],
            "last_imei": state["imei"],
            "last_session_id": row.get("acct_session_id"),
            "last_status": row.get("acct_status_type"),
            "last_event_ts": str(row.get("event_timestamp", "")),
        }

    df["is_conflict_c"] = df.index.map(lambda i: conflict_c_flags.get(i, False)).astype(bool)
    df["is_conflict_d"] = df.index.map(lambda i: conflict_d_flags.get(i, False)).astype(bool)

    # Ghi đè state MỚI NHẤT của từng msisdn về Redis — 1 record/msisdn
    # (bản ghi cuối cùng theo event_timestamp trong batch), theo LÔ
    # 200-500/round-trip, để các batch TIẾP THEO (dù cách xa bao lâu,
    # dù job có restart giữa chừng) vẫn so sánh đúng.
    if redis_state and redis_updates:
        redis_state.update_batch(list(redis_updates.values()))

    # ── Tách nhóm (clean được GÁN ở đây, mọi thứ dùng `clean` phải nằm SAU dòng này) ─
    is_conflicted_ab = df["is_conflict_a"] | df["is_conflict_b"]
    clean = df[~is_conflicted_ab].copy()

    _DROP_COLS = [
        "first_imsi", "first_msisdn", "is_conflict_a", "is_conflict_b",
        "is_conflict_c", "is_conflict_d", "event_timestamp_ts",
    ]

    conflict_c_records: List[Dict] = []
    if "is_conflict_c" in clean.columns:
        c_only = clean[clean["is_conflict_c"] == True].copy()
        if not c_only.empty:
            conflict_c_records = c_only.drop(columns=_DROP_COLS, errors="ignore").to_dict(orient="records")

    conflict_d_records: List[Dict] = []
    if "is_conflict_d" in clean.columns:
        d_only = clean[clean["is_conflict_d"] == True].copy()
        if not d_only.empty:
            conflict_d_records = d_only.drop(columns=_DROP_COLS, errors="ignore").to_dict(orient="records")

    clean = clean.drop(columns=_DROP_COLS, errors="ignore")

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
        conflicted_ab["error_code"] = conflicted_ab["conflict_type"].apply(lambda t: f"CONFLICT_{t}")
        conflicted_ab = conflicted_ab.rename(columns={"acct_session_id": "session_id"})
        conflict_records = conflicted_ab[
            ["session_id", "conflict_type", "details", "error_code"]
        ].to_dict(orient="records")

    return clean.to_dict(orient="records"), conflict_records, conflict_c_records, conflict_d_records


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


def _process_swap_signals(conflict_c_rows: List[Dict], conflict_d_rows: List[Dict]) -> None:
    """Xác minh Conflict C + D — 2 request batch (không phải N request/record)."""
    confirmed: List[Dict] = []
    try:
        confirmed += _sim_swap_detector.verify_batch(conflict_c_rows)
    except Exception:
        logger.exception("SIM Swap batch verify failed (%d rows)", len(conflict_c_rows))
    try:
        confirmed += _device_swap_detector.verify_batch(conflict_d_rows)
    except Exception:
        logger.exception("Device Swap batch verify failed (%d rows)", len(conflict_d_rows))

    if confirmed:
        write_swap_events(confirmed, DB_DSN)
    logger.info(
        "Swap verify: confirmed=%d (candidate C=%d, D=%d)",
        len(confirmed), len(conflict_c_rows), len(conflict_d_rows),
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
        #     C/D dùng Redis global state (batched, xem _redis_state) thay
        #     vì chỉ so sánh trong nội bộ batch này.
        clean_rows, conflict_rows, conflict_c_rows, conflict_d_rows = _resolve_conflicts(
            deduped_rows, _redis_state
        )

        # ── 7. Ghi conflict_log (A/B) ─────────────────────────────────────────
        if conflict_rows:
            try:
                _write_conflict_log(conflict_rows)
            except Exception as e:
                logger.error("Batch %d | Failed to write conflict_log: %s", batch_id, e)

        # ── 8. Xác minh SIM Swap (C) qua HLR/HSS, ghi swap_event ──────────────
        if conflict_c_rows or conflict_d_rows:
            try:
                _process_swap_signals(conflict_c_rows, conflict_d_rows)
            except Exception as e:
                logger.error("Batch %d | Failed to process swap signals: %s", batch_id, e)

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
            "Batch %d | -> clean=%d conflict_ab=%d conflict_c=%d conflict_d=%d invalid=%d "
            "late=%d (total %.0fms)",
            batch_id, len(clean_rows), len(conflict_rows), len(conflict_c_rows), len(conflict_d_rows),
            len(invalid_rows), len(late_rows), (time.time() - t0) * 1000,
        )

    return _callback


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