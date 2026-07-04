#!/usr/bin/env python3
"""
pipeline/pipeline/processing/partition_worker.py

[FIX-PICKLE] 2026-07-05 — TÁCH RIÊNG KHỎI processor.py ĐỂ SỬA LỖI
"TypeError: cannot pickle '_thread.lock' object"
===========================================================================
NGUYÊN NHÂN: khi processor.py được chạy trực tiếp (spark-submit hoặc
`python processor.py`), Python nạp nó với __name__ == "__main__". Với
hàm định nghĩa trong module __main__, cloudpickle KHÔNG THỂ pickle bằng
tham chiếu (import module + tên hàm) như module thường — nó bắt buộc
serialize hàm BẰNG GIÁ TRỊ, tức chụp lại toàn bộ global mà hàm đó (và
các hàm nó gọi) tham chiếu tới. `_process_partition` gọi
`_get_worker_resources()`, hàm này dùng 1 `threading.Lock()` ở cấp
module để bảo vệ việc khởi tạo connection cache lần đầu. `threading.Lock`
là object C-level KHÔNG BAO GIỜ pickle được -> mọi batch crash ngay ở
tầng driver, TRƯỚC KHI dữ liệu được gửi tới executor (numInputRows=0).

CÁCH SỬA: đưa `process_partition` (và mọi thứ nó cần: cache kết nối,
Lock, pure-logic, Postgres writers...) vào MODULE RIÊNG có thể import
được bình thường (không phải __main__). Khi `foreachPartition` nhận
1 hàm từ module import được, cloudpickle chỉ lưu "import module X, lấy
tên hàm Y" — KHÔNG đụng tới bất kỳ global nào (kể cả Lock). Khi executor
unpickle, nó import module này lại từ đầu, tạo Lock/dict cache HOÀN TOÀN
MỚI trong tiến trình của chính nó — đúng ý đồ thiết kế ban đầu và an
toàn tuyệt đối với pickle.

processor.py (driver script, chạy như __main__) chỉ còn:
  - dựng SparkSession, build_stream, StreamingQueryListener
  - `from pipeline.processing.partition_worker import process_partition`
  - `keyed.foreachPartition(process_partition)`
===========================================================================
"""

import os
import json
import logging
import threading
import time
from typing import Dict, List, Optional, Tuple, Any, Iterator

import pandas as pd
import psycopg2
import redis as redis_lib
from kafka import KafkaProducer
from pyspark.sql import Row

from pipeline.state.redis_state_manager import RedisStateManager

logger = logging.getLogger(__name__)

# ==============================================================================
# CONSTANTS (đọc độc lập từ env — module này KHÔNG import processor.py để
# tránh circular import; processor.py mới là bên import module này)
# ==============================================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "camara-kafka:9092")
KAFKA_TOPIC_CLEAN       = os.getenv("KAFKA_TOPIC_CLEAN", "radius.clean")

LATE_ARRIVAL_THRESHOLD_SECONDS = int(os.getenv("LATE_ARRIVAL_THRESHOLD_SECONDS", "3600"))
TTL_SECONDS = int(os.getenv("DEDUP_TTL_SECONDS", "3600"))

DB_DSN = dict(
    host=os.getenv("DB_HOST", "postgres"),
    port=int(os.getenv("DB_PORT", "5432")),
    dbname=os.getenv("DB_NAME", "camara_db"),
    user=os.getenv("DB_USER", "postgres"),
    password=os.getenv("DB_PASSWORD", "camara"),
    connect_timeout=int(os.getenv("DB_CONNECT_TIMEOUT", "5")),
)

REDIS_HOST = os.getenv("REDIS_HOST", "camara-redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_BATCH_SIZE = int(os.getenv("REDIS_BATCH_SIZE", "300"))

RAISE_ON_PARTITION_FAILURE = os.getenv("RAISE_ON_PARTITION_FAILURE", "0") == "1"
CONN_RETRY_ATTEMPTS = int(os.getenv("CONN_RETRY_ATTEMPTS", "3"))
CONN_RETRY_BACKOFF_SECONDS = float(os.getenv("CONN_RETRY_BACKOFF_SECONDS", "1.0"))


# ==============================================================================
# 1. PURE LOGIC (không phụ thuộc Spark — dùng được cả trong unit test)
# ==============================================================================

def split_validated(annotated_rows: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """Tách rows đã được validate thành (valid, invalid) dựa trên error_code."""
    valid: List[Dict] = []
    invalid: List[Dict] = []
    for row in annotated_rows:
        if row.get("error_code"):
            invalid.append(row)
        else:
            valid.append(row)
    return valid, invalid


def split_late_arrival(records: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
    """
    Tách record đến trễ hơn LATE_ARRIVAL_THRESHOLD_SECONDS so với
    event_timestamp, TRƯỚC khi Spark watermark có cơ hội âm thầm drop nó.
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


def dedup_filter_redis(records: List[Dict], r: "redis_lib.Redis") -> List[Dict]:
    """
    key = dedup:{acct_session_id}:{acct_status_type}
    SET ... NX PX <ttl_ms> trả True nếu key MỚI (chưa tồn tại) -> giữ lại;
    False nếu đã tồn tại trong TTL -> duplicate, loại bỏ.
    """
    if not records:
        return []

    ttl_ms = TTL_SECONDS * 1000
    pipe = r.pipeline(transaction=False)
    for record in records:
        key = f"dedup:{record.get('acct_session_id','')}:{record.get('acct_status_type','')}"
        pipe.set(key, "1", nx=True, px=ttl_ms)
    results = pipe.execute()

    return [record for record, is_new in zip(records, results) if is_new]


def resolve_conflicts(
    records: List[Dict],
    redis_state: Optional[RedisStateManager] = None,
) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
    """
    Phân loại conflict A/B/C/D. CẢ 4 LOẠI đều so sánh với trạng thái
    TOÀN CỤC lưu trong Redis — không loại nào bị giới hạn trong phạm vi
    1 micro-batch hay 1 partition riêng lẻ.

    Conflict A: bản ghi non-Start có imsi/msisdn khác baseline Start ĐÃ LƯU
                (Redis hoặc trong cùng batch) của session đó.
    Conflict B: IMSI đang có 1 session khác CHƯA ĐÓNG (đã Start, chưa
                Stop/Interim) — kiểm tra theo trạng thái GLOBAL.
    Conflict C: msisdn xuất hiện IMSI khác với last_imsi đã lưu (SIM Swap signal).
    Conflict D: msisdn xuất hiện IMEI khác với last_imei đã lưu (Device Swap signal).
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

    session_ids_all = [s for s in df["acct_session_id"].dropna().unique().tolist() if s]
    imsis_all = [s for s in df["imsi"].dropna().unique().tolist() if s]

    redis_session_baseline = (
        redis_state.fetch_session_baselines(session_ids_all) if redis_state else {}
    )
    redis_open_sessions = (
        redis_state.fetch_open_sessions(imsis_all) if redis_state else {}
    )

    session_baseline_state: Dict[str, Dict[str, Optional[str]]] = {
        sid: {"imsi": v.get("start_imsi"), "msisdn": v.get("start_msisdn")}
        for sid, v in redis_session_baseline.items()
    }
    open_sessions_state: Dict[str, set] = {
        imsi: set(v) for imsi, v in redis_open_sessions.items()
    }

    new_session_baselines: Dict[str, Dict[str, Optional[str]]] = {}
    touched_imsis: set = set()
    conflict_a_flags: Dict[Any, bool] = {}
    conflict_b_flags: Dict[Any, bool] = {}

    for idx, row in df.iterrows():
        sid = row["acct_session_id"]
        imsi = row["imsi"]
        msisdn = row["msisdn"]
        status = row["acct_status_type"]

        baseline = session_baseline_state.get(sid) if sid else None
        if baseline is None:
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

        is_b = False
        if not is_a and imsi:
            open_set = open_sessions_state.setdefault(imsi, set())
            if status == "Start":
                if sid in open_set:
                    pass
                elif len(open_set) > 0:
                    is_b = True
                else:
                    open_set.add(sid)
            elif status in ("Stop", "Interim"):
                open_set.discard(sid)
            touched_imsis.add(imsi)
        conflict_b_flags[idx] = is_b

    df["is_conflict_a"] = df.index.map(lambda i: conflict_a_flags.get(i, False)).astype(bool)
    df["is_conflict_b"] = df.index.map(lambda i: conflict_b_flags.get(i, False)).astype(bool)

    if redis_state:
        if new_session_baselines:
            redis_state.save_session_baselines([
                {"session_id": sid, **v} for sid, v in new_session_baselines.items()
            ])
        if touched_imsis:
            redis_state.save_open_sessions({
                imsi: open_sessions_state.get(imsi, set()) for imsi in touched_imsis
            })

    legit_mask = (~df["is_conflict_a"]) & (~df["is_conflict_b"])
    legit_df = df[legit_mask].sort_values("event_timestamp_ts")

    unique_msisdns = [m for m in legit_df["msisdn"].dropna().unique().tolist() if m]
    redis_prev_state = redis_state.fetch_batch(unique_msisdns) if redis_state else {}

    running_state: Dict[str, Dict[str, Optional[str]]] = {
        msisdn: {"imsi": prev.get("last_imsi") or None, "imei": prev.get("last_imei") or None}
        for msisdn, prev in redis_prev_state.items()
    }

    conflict_c_flags: Dict[Any, bool] = {}
    conflict_d_flags: Dict[Any, bool] = {}
    old_value_map: Dict[Any, Dict[str, Optional[str]]] = {}
    redis_updates: Dict[str, Dict] = {}

    for idx, row in legit_df.iterrows():
        msisdn = row["msisdn"]
        if not msisdn:
            conflict_c_flags[idx] = False
            conflict_d_flags[idx] = False
            continue

        state = running_state.setdefault(msisdn, {"imsi": None, "imei": None})
        cur_imsi, cur_imei = row["imsi"], row["imei"]

        old_value_map[idx] = {"old_imsi": state["imsi"], "old_imei": state["imei"]}

        conflict_c_flags[idx] = bool(
            state["imsi"] is not None and cur_imsi is not None and cur_imsi != state["imsi"]
        )
        conflict_d_flags[idx] = bool(
            state["imei"] is not None and cur_imei is not None and cur_imei != state["imei"]
        )

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
    df["old_imsi"] = df.index.map(lambda i: old_value_map.get(i, {}).get("old_imsi"))
    df["old_imei"] = df.index.map(lambda i: old_value_map.get(i, {}).get("old_imei"))

    if redis_state and redis_updates:
        redis_state.update_batch(list(redis_updates.values()))

    is_conflicted_ab = df["is_conflict_a"] | df["is_conflict_b"]
    clean = df[~is_conflicted_ab].copy()

    _DROP_COLS_CD = [
        "first_imsi", "first_msisdn", "is_conflict_a", "is_conflict_b",
        "is_conflict_c", "is_conflict_d", "event_timestamp_ts",
    ]
    _DROP_COLS_CLEAN = _DROP_COLS_CD + ["old_imsi", "old_imei"]

    conflict_c_records: List[Dict] = []
    if "is_conflict_c" in clean.columns:
        c_only = clean[clean["is_conflict_c"] == True].copy()
        if not c_only.empty:
            conflict_c_records = c_only.drop(columns=_DROP_COLS_CD, errors="ignore").to_dict(orient="records")

    conflict_d_records: List[Dict] = []
    if "is_conflict_d" in clean.columns:
        d_only = clean[clean["is_conflict_d"] == True].copy()
        if not d_only.empty:
            conflict_d_records = d_only.drop(columns=_DROP_COLS_CD, errors="ignore").to_dict(orient="records")

    clean = clean.drop(columns=_DROP_COLS_CLEAN, errors="ignore")

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


# ==============================================================================
# 2. POSTGRESQL WRITERS
# ==============================================================================

def _write_invalid_log_conn(rows: List[Dict], conn) -> None:
    if not rows:
        return
    sql = """
        INSERT INTO invalid_log (session_id, msisdn, error_code, details)
        VALUES (%s, %s, %s, %s)
    """
    data = [
        (r.get("acct_session_id"), r.get("msisdn"), r.get("error_code"), r.get("error_details"))
        for r in rows
    ]
    with conn.cursor() as cur:
        cur.executemany(sql, data)
    conn.commit()


def _write_conflict_log_conn(rows: List[Dict], conn) -> None:
    if not rows:
        return
    sql = """
        INSERT INTO conflict_log (session_id, conflict_type, details, error_code)
        VALUES (%s, %s, %s, %s)
    """
    data = [(r["session_id"], r["conflict_type"], r["details"], r["error_code"]) for r in rows]
    with conn.cursor() as cur:
        cur.executemany(sql, data)
    conn.commit()


def _write_swap_event_conn(rows: List[Dict], conn) -> None:
    """
    KHÔNG xác minh qua HLR/HSS — Redis global state (last_imsi/last_imei
    xuyên-batch) chính là nguồn xác nhận, nên detected_at = confirmed_at
    = NOW(), source = 'redis_state'.
    """
    if not rows:
        return
    sql = """
        INSERT INTO swap_event
            (msisdn, old_imsi, new_imsi, old_imei, new_imei, imei,
             swap_type, detected_at, confirmed_at, source)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), %s)
    """
    data = []
    for r in rows:
        is_c = r.get("swap_type") == "C"
        data.append((
            r.get("msisdn"),
            r.get("old_imsi") if is_c else None,
            r.get("imsi") if is_c else None,
            r.get("old_imei") if not is_c else None,
            r.get("imei") if not is_c else None,
            r.get("imei"),
            "SIM_SWAP" if is_c else "DEVICE_SWAP",
            "redis_state",
        ))
    with conn.cursor() as cur:
        cur.executemany(sql, data)
    conn.commit()


def _process_swap_signals(conflict_c_rows: List[Dict], conflict_d_rows: List[Dict], conn) -> None:
    rows: List[Dict] = []
    for r in conflict_c_rows:
        row = dict(r); row["swap_type"] = "C"; rows.append(row)
    for r in conflict_d_rows:
        row = dict(r); row["swap_type"] = "D"; rows.append(row)

    if rows:
        _write_swap_event_conn(rows, conn)

    logger.info(
        "Swap (Redis-only, khong goi HLR/HSS): total=%d (C=%d, D=%d)",
        len(rows), len(conflict_c_rows), len(conflict_d_rows),
    )


# ==============================================================================
# 3. WORKER-PROCESS-SCOPED CONNECTION CACHE
# ==============================================================================
#
# QUAN TRỌNG: các global dưới đây (_WORKER_RESOURCES, _RESOURCE_LOCK,
# _WORKER_COUNTERS) chỉ AN TOÀN với pickle vì module này được IMPORT
# BÌNH THƯỜNG (không phải __main__) — process_partition được pickle
# BẰNG THAM CHIẾU (tên module + tên hàm), cloudpickle không bao giờ
# động tới các global này lúc pickle. Khi executor unpickle, nó import
# lại module này từ đầu -> Lock/dict được TẠO MỚI trong đúng tiến trình
# đó, không có gì bị serialize qua mạng.

_WORKER_RESOURCES: Dict[int, Dict[str, Any]] = {}
_RESOURCE_LOCK = threading.Lock()
_WORKER_COUNTERS: Dict[int, Dict[str, int]] = {}


def _retry(fn, what: str, attempts: int = CONN_RETRY_ATTEMPTS, backoff: float = CONN_RETRY_BACKOFF_SECONDS):
    """Gọi fn() với retry + backoff tuyến tính, log rõ từng lần thất bại."""
    last_exc = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 - cố ý bắt rộng để retry hạ tầng
            last_exc = exc
            logger.warning(
                "[FIX-CONN] Thu ket noi that bai (%s) lan %d/%d: %s",
                what, attempt, attempts, exc,
            )
            if attempt < attempts:
                time.sleep(backoff * attempt)
    raise last_exc


def _new_kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        request_timeout_ms=30000,
        max_block_ms=15000,
    )


def _get_worker_resources() -> Dict[str, Any]:
    """
    Trả về dict {redis_client, redis_state, pg_conn, kafka_producer} cho
    worker process hiện tại, tạo mới (có retry) nếu chưa có, hoặc
    reconnect từng phần nếu kết nối cũ đã chết.
    """
    pid = os.getpid()
    with _RESOURCE_LOCK:
        res = _WORKER_RESOURCES.get(pid)
        if res is None:
            logger.info("[FIX-CONN] Worker pid=%d: khoi tao ket noi lan dau", pid)
            res = {}
            res["redis_client"] = _retry(
                lambda: redis_lib.Redis(
                    host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
                    socket_timeout=5, socket_connect_timeout=5,
                    health_check_interval=30,
                ),
                "redis_client",
            )
            res["redis_state"] = _retry(
                lambda: RedisStateManager(
                    host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
                    batch_size=REDIS_BATCH_SIZE,
                ),
                "redis_state",
            )
            res["pg_conn"] = _retry(lambda: psycopg2.connect(**DB_DSN), "pg_conn")
            res["kafka_producer"] = _retry(_new_kafka_producer, "kafka_producer")
            _WORKER_RESOURCES[pid] = res
            _WORKER_COUNTERS[pid] = dict(
                total=0, valid=0, invalid=0, late=0, deduped=0,
                clean=0, conflict_ab=0, conflict_c=0, conflict_d=0, batches=0,
            )
        else:
            try:
                if res["pg_conn"].closed:
                    logger.warning("[FIX-CONN] pg_conn worker pid=%d da dong, reconnect", pid)
                    res["pg_conn"] = _retry(lambda: psycopg2.connect(**DB_DSN), "pg_conn-reconnect")
            except Exception:
                logger.warning("[FIX-CONN] pg_conn worker pid=%d loi khi kiem tra, reconnect", pid)
                res["pg_conn"] = _retry(lambda: psycopg2.connect(**DB_DSN), "pg_conn-reconnect")

            try:
                res["redis_client"].ping()
            except Exception:
                logger.warning("[FIX-CONN] redis_client worker pid=%d mat ket noi, reconnect", pid)
                res["redis_client"] = _retry(
                    lambda: redis_lib.Redis(
                        host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
                        socket_timeout=5, socket_connect_timeout=5,
                        health_check_interval=30,
                    ),
                    "redis_client-reconnect",
                )

        return res


def close_all_worker_resources() -> None:
    """Đóng toàn bộ kết nối đang cache (gọi từ driver lúc shutdown / test)."""
    with _RESOURCE_LOCK:
        for pid, res in list(_WORKER_RESOURCES.items()):
            for key in ("pg_conn", "kafka_producer", "redis_client"):
                obj = res.get(key)
                try:
                    if obj is not None:
                        obj.close()
                except Exception:
                    logger.exception("[FIX-CONN] Loi khi dong %s cho worker pid=%d", key, pid)
        _WORKER_RESOURCES.clear()
        _WORKER_COUNTERS.clear()


# ==============================================================================
# 4. EXECUTOR-SIDE PARTITION PROCESSOR (hàm được truyền vào foreachPartition)
# ==============================================================================

def process_partition(rows_iter: Iterator[Row]) -> None:
    """
    Chạy TRÊN EXECUTOR. batch_df PHẢI được repartition theo msisdn TRƯỚC
    khi gọi hàm này — mọi record cùng 1 msisdn đảm bảo nằm cùng 1
    partition/task, nên dedup + conflict A/B/C/D xử lý ĐÚNG và ĐỦ mà
    không cần gom dữ liệu về driver.

    [FIX-PICKLE] Hàm này (và toàn bộ helper nó dùng) nằm trong module
    import-able `pipeline.processing.partition_worker`, KHÔNG phải
    trong script __main__ — nhờ vậy cloudpickle pickle nó BẰNG THAM
    CHIẾU (module + tên hàm), không bao giờ đụng tới _RESOURCE_LOCK
    hay bất kỳ global non-picklable nào. Đây là điểm mấu chốt để
    `foreachPartition(process_partition)` không còn ném
    "cannot pickle '_thread.lock' object".

    [FIX-CONN] Kết nối được lấy từ cache theo worker process thay vì mở
    mới mỗi lần gọi. Toàn bộ thân hàm được bọc try/except: nếu
    RAISE_ON_PARTITION_FAILURE=0 (mặc định), lỗi được log CRITICAL đầy đủ
    rồi hàm return êm — batch của riêng partition này bị bỏ qua nhưng
    STREAMING QUERY VẪN SỐNG để xử lý các batch/partition tiếp theo.
    """
    records = [r.asDict() for r in rows_iter]
    if not records:
        return

    import sys as _sys, os as _os2
    _root = _os2.path.abspath(_os2.path.join(_os2.path.dirname(__file__), "..", ".."))
    if _root not in _sys.path:
        _sys.path.insert(0, _root)

    import asyncio as _asyncio
    import httpx as _httpx
    from pipeline.validation.rules import execute_validation_pipeline_batch

    t0 = time.time()
    pid = _os2.getpid()

    try:
        res = _get_worker_resources()
    except Exception:
        logger.critical(
            "[FIX-CONN] pid=%d: KHONG THE khoi tao ket noi (Redis/Postgres/Kafka) "
            "sau %d lan retry. Bo qua %d record cua partition nay. "
            "=> KIEM TRA: Postgres max_connections, Redis maxclients, "
            "Kafka broker reachability.",
            pid, CONN_RETRY_ATTEMPTS, len(records),
            exc_info=True,
        )
        if RAISE_ON_PARTITION_FAILURE:
            raise
        return

    redis_client = res["redis_client"]
    redis_state = res["redis_state"]
    pg_conn = res["pg_conn"]
    kafka_producer = res["kafka_producer"]

    try:
        # ── 1. Validation ────────────────────────────────────────────────
        try:
            async def _run():
                limits = _httpx.Limits(max_connections=20, max_keepalive_connections=10)
                async with _httpx.AsyncClient(limits=limits) as client:
                    return await execute_validation_pipeline_batch(records, client)
            results = _asyncio.run(_run())

            annotated: List[Dict] = []
            for record, (res_val, warn) in zip(records, results):
                payload = dict(record)
                if res_val.is_valid:
                    if warn:
                        payload["warn_code"] = warn
                else:
                    payload["error_code"] = res_val.error_code
                    payload["error_details"] = res_val.error_message
                annotated.append(payload)
        except Exception:
            logger.exception(
                "[pid=%d] Partition validate: loi khong luong truoc, danh dau %d "
                "record la ERR_PARTITION_VALIDATION_FAILED", pid, len(records),
            )
            annotated = []
            for record in records:
                payload = dict(record)
                payload["error_code"] = "ERR_PARTITION_VALIDATION_FAILED"
                payload["error_details"] = "Unhandled exception during partition validation"
                annotated.append(payload)

        valid_rows, invalid_rows = split_validated(annotated)

        # ── 2. Late arrival ──────────────────────────────────────────────
        on_time_rows, late_rows = split_late_arrival(valid_rows)

        all_invalid_rows = invalid_rows + late_rows
        if all_invalid_rows:
            try:
                _write_invalid_log_conn(all_invalid_rows, pg_conn)
            except Exception:
                logger.exception("[pid=%d] Failed to write invalid_log", pid)
                pg_conn.rollback()

        if not on_time_rows:
            logger.info(
                "[pid=%d] Partition | total=%d valid=%d invalid=%d late=%d "
                "-> khong con record on-time, dung tai day (%.0fms)",
                pid, len(records), len(valid_rows), len(invalid_rows), len(late_rows),
                (time.time() - t0) * 1000,
            )
            return

        # ── 3. Dedup qua Redis ───────────────────────────────────────────
        deduped_rows = dedup_filter_redis(on_time_rows, redis_client)
        if not deduped_rows:
            logger.info(
                "[pid=%d] Partition | total=%d on_time=%d -> tat ca la duplicate, dung tai day",
                pid, len(records), len(on_time_rows),
            )
            return

        # ── 4. Conflict resolution A/B/C/D ───────────────────────────────
        clean_rows, conflict_rows, conflict_c_rows, conflict_d_rows = resolve_conflicts(
            deduped_rows, redis_state
        )

        if conflict_rows:
            try:
                _write_conflict_log_conn(conflict_rows, pg_conn)
            except Exception:
                logger.exception("[pid=%d] Failed to write conflict_log", pid)
                pg_conn.rollback()

        if conflict_c_rows or conflict_d_rows:
            try:
                _process_swap_signals(conflict_c_rows, conflict_d_rows, pg_conn)
            except Exception:
                logger.exception("[pid=%d] Failed to process swap signals", pid)
                pg_conn.rollback()

        # ── 5. Ghi radius.clean trực tiếp từ executor ────────────────────
        if clean_rows:
            try:
                for row in clean_rows:
                    kafka_producer.send(KAFKA_TOPIC_CLEAN, value=row)
                kafka_producer.flush(timeout=30)
            except Exception:
                logger.exception(
                    "[pid=%d] Failed to send %d clean records to Kafka topic %s",
                    pid, len(clean_rows), KAFKA_TOPIC_CLEAN,
                )
                if RAISE_ON_PARTITION_FAILURE:
                    raise

        # ── 6. Log chi tiết theo partition + cap nhat bo dem worker ──────
        counters = _WORKER_COUNTERS.setdefault(pid, dict(
            total=0, valid=0, invalid=0, late=0, deduped=0,
            clean=0, conflict_ab=0, conflict_c=0, conflict_d=0, batches=0,
        ))
        counters["total"] += len(records)
        counters["valid"] += len(valid_rows)
        counters["invalid"] += len(invalid_rows)
        counters["late"] += len(late_rows)
        counters["deduped"] += len(deduped_rows)
        counters["clean"] += len(clean_rows)
        counters["conflict_ab"] += len(conflict_rows)
        counters["conflict_c"] += len(conflict_c_rows)
        counters["conflict_d"] += len(conflict_d_rows)
        counters["batches"] += 1

        logger.info(
            "[pid=%d] Partition | total=%d valid=%d invalid=%d late=%d deduped=%d "
            "clean=%d conflict_ab=%d conflict_c=%d conflict_d=%d (%.0fms) | "
            "CUM(worker)=%s",
            pid, len(records), len(valid_rows), len(invalid_rows), len(late_rows),
            len(deduped_rows), len(clean_rows), len(conflict_rows),
            len(conflict_c_rows), len(conflict_d_rows), (time.time() - t0) * 1000,
            counters,
        )

    except Exception:
        logger.critical(
            "[pid=%d] LOI KHONG LUONG TRUOC trong process_partition, "
            "bo qua %d record cua batch nay de KHONG lam crash streaming query. "
            "Bat RAISE_ON_PARTITION_FAILURE=1 de fail-fast khi debug.",
            pid, len(records), exc_info=True,
        )
        if RAISE_ON_PARTITION_FAILURE:
            raise
        try:
            pg_conn.rollback()
        except Exception:
            pass