#!/usr/bin/env python3
#pipeline\ingestion\radius_udp_sender.py
#
# Bootstrap: thêm thư mục gốc project vào sys.path để `import pipeline.*`
# hoạt động khi chạy trực tiếp với python3 script.py
import sys, os as _os
sys.path.insert(0, _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "..", "..")))

"""
Stage 1 (thay thế) — CSV → gói tin UDP RADIUS Accounting-Request thật

Đọc file CSV (cùng schema với producer.py) qua LocalCSVReader, ĐÓNG GÓI từng
dòng thành 1 gói tin RADIUS nhị phân đúng chuẩn RFC 2866 (Code=4 Accounting-
Request) + Vendor-Specific Attribute 3GPP (RFC/3GPP TS 29.061), rồi gửi qua
UDP tới `--host:--port`.

Mục đích: giả lập 1 thiết bị GGSN/NAS thật đang gửi RADIUS accounting request
qua mạng — dùng để test đầu vào pipeline.ingestion.packet_reader.PacketReader
(listener UDP đang chạy sẵn), KHÔNG đi qua đường Kafka producer trực tiếp
như pipeline.ingestion.producer.RadiusLogProducer.publish_csv().

Cấu trúc byte tạo ra đối xứng 1-1 với PacketReader.decode_radius() —
xem pipeline/ingestion/packet_reader.py để đối chiếu FIELD_SCHEMA.

Usage:
    python -m pipeline.ingestion.radius_udp_sender \\
        --csv data/radius_sample.csv --host 127.0.0.1 --port 1813 --rate 50

    # Lặp lại vô hạn (stress-test / demo liên tục):
    python -m pipeline.ingestion.radius_udp_sender --csv data/radius_sample.csv --loop
"""

import os
import csv as _csv
import socket
import struct
import hashlib
import logging
import time
import argparse
import heapq
import queue
import random
import threading
from dataclasses import dataclass
from functools import lru_cache
from typing import Dict, Any, List, Tuple, Optional

from pipeline.ingestion.csv_reader import LocalCSVReader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# ==============================================================================
# Bảng mã thuộc tính RADIUS — PHẢI khớp 100% với FIELD_SCHEMA trong
# pipeline/ingestion/packet_reader.py, nếu sửa 1 bên phải sửa bên kia.
# ==============================================================================

ATTR_ACCT_STATUS_TYPE  = 0x28  # 40 — Integer
ATTR_ACCT_SESSION_ID   = 0x2c  # 44 — String
ATTR_ACCT_SESSION_TIME = 0x2d  # 45 — Integer
ATTR_CALLING_STATION   = 0x1f  # 31 — String (msisdn)
ATTR_FRAMED_IP         = 0x08  # 8  — IPv4
ATTR_NAS_IP            = 0x04  # 4  — IPv4
ATTR_NAS_IDENTIFIER    = 0x20  # 32 — String
ATTR_VENDOR_SPECIFIC   = 0x1a  # 26 — Vendor-Specific (RFC 2865 §5.26)

VENDOR_ID_3GPP = 10415  # 0x28AF — đúng như trong pcap mẫu

VENDOR_SUBTYPE_IMSI     = 0x01
VENDOR_SUBTYPE_IMEI     = 0x14  # 20
VENDOR_SUBTYPE_RAT_TYPE = 0x15  # 21
VENDOR_SUBTYPE_MCC_MNC  = 0x08  # 8

# Acct-Status-Type value theo RFC 2866 §5.1
ACCT_STATUS_TYPE_MAP = {
    "start":            1,
    "stop":             2,
    "interim-update":   3,
    "interim_update":   3,
    "accounting-on":    7,
    "accounting_on":    7,
    "accounting-off":   8,
    "accounting_off":   8,
}

RADIUS_CODE_ACCOUNTING_REQUEST = 0x04
DEFAULT_SHARED_SECRET = os.getenv("RADIUS_SHARED_SECRET", "camara-radius-dev-secret")


@dataclass
class PendingPacket:
    packet: bytes
    identifier: int
    authenticator: bytes
    deadline: float
    retries: int = 0


def _pick(record: Dict[str, Any], *keys: str) -> Optional[str]:
    """Lấy giá trị không rỗng đầu tiên trong các key thay thế nhau
    (cùng 1 field nhưng dữ liệu CSV có thể đặt tên snake_case hoặc PascalCase,
    giống convention trong pipeline/modules/ip_msisdn/consumer.py)."""
    for k in keys:
        v = record.get(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return None


def _encode_avp(attr_type: int, value: bytes) -> bytes:
    """Đóng gói 1 Attribute-Value-Pair chuẩn RADIUS: Type(1) + Length(1) + Value.
    Length tính luôn 2 byte header, tối đa 255 (RFC 2865 §5)."""
    total_len = 2 + len(value)
    if total_len > 255:
        raise ValueError(
            f"AVP type=0x{attr_type:02x} dài {total_len} byte, vượt giới hạn 255 của RADIUS"
        )
    return bytes([attr_type, total_len]) + value


def _encode_str_avp(attr_type: int, value: str) -> bytes:
    return _encode_avp(attr_type, value.encode("utf-8"))


def _encode_int_avp(attr_type: int, value: int) -> bytes:
    return _encode_avp(attr_type, struct.pack("!I", int(value) & 0xFFFFFFFF))


def _encode_ip_avp(attr_type: int, ip_str: str) -> bytes:
    try:
        ip_bytes = socket.inet_aton(ip_str)
    except OSError:
        raise ValueError(f"Giá trị IP không hợp lệ cho AVP 0x{attr_type:02x}: {ip_str!r}")
    return _encode_avp(attr_type, ip_bytes)


@lru_cache(maxsize=131_072)
def _cached_str_avp(attr_type: int, value: str) -> bytes:
    """Cache AVP có giá trị lặp lại giữa các accounting record.

    Session id và session time không dùng cache vì thường có cardinality cao.
    Giới hạn LRU ngăn sender giữ toàn bộ file lớn trong RAM.
    """
    return _encode_str_avp(attr_type, value)


@lru_cache(maxsize=65_536)
def _cached_ip_avp(attr_type: int, value: str) -> bytes:
    return _encode_ip_avp(attr_type, value)


def _encode_vendor_specific(vendor_id: int, sub_attrs: List[Tuple[int, str]]) -> bytes:
    """Đóng gói AVP Vendor-Specific (0x1a): Vendor-Id (4 byte) + chuỗi sub-AVP,
    mỗi sub-AVP là Subtype(1) + Sublen(1) + giá trị — đúng layout mà
    PacketReader.decode_radius() đọc ở nhánh `if attr_type == vendor_specific["type"]`."""
    body = struct.pack("!I", vendor_id)
    for sub_type, sub_value in sub_attrs:
        value_bytes = sub_value.encode("utf-8")
        sub_len = 2 + len(value_bytes)
        if sub_len > 255:
            raise ValueError(f"Vendor sub-AVP subtype=0x{sub_type:02x} quá dài ({sub_len} byte)")
        body += bytes([sub_type, sub_len]) + value_bytes
    return _encode_avp(ATTR_VENDOR_SPECIFIC, body)


@lru_cache(maxsize=131_072)
def _cached_vendor_specific(vendor_id: int, sub_attrs: Tuple[Tuple[int, str], ...]) -> bytes:
    return _encode_vendor_specific(vendor_id, list(sub_attrs))


@lru_cache(maxsize=32)
def _secret_bytes(secret: str) -> bytes:
    return secret.encode("utf-8")


def build_radius_packet(record: Dict[str, Any], identifier: int, secret: str) -> bytes:
    """Đóng gói 1 record CSV thành 1 gói tin RADIUS Accounting-Request nhị phân
    hoàn chỉnh, sẵn sàng gửi qua UDP tới cổng 1813."""

    avps = bytearray()

    # --- Acct-Status-Type (bắt buộc) ---
    raw_status = _pick(record, "acct_status_type")
    if raw_status is None:
        raise ValueError("Record thiếu acct_status_type, không thể đóng gói")
    status_key = raw_status.strip().lower()
    if status_key in ACCT_STATUS_TYPE_MAP:
        status_int = ACCT_STATUS_TYPE_MAP[status_key]
    elif raw_status.isdigit():
        status_int = int(raw_status)
    else:
        raise ValueError(f"Không nhận diện được acct_status_type={raw_status!r}")
    avps += _encode_int_avp(ATTR_ACCT_STATUS_TYPE, status_int)

    # --- Acct-Session-Id ---
    session_id = _pick(record, "acct_session_id")
    if session_id:
        avps += _encode_str_avp(ATTR_ACCT_SESSION_ID, session_id)

    # --- Acct-Session-Time ---
    session_time = _pick(record, "acct_session_time")
    if session_time is not None:
        try:
            avps += _encode_int_avp(ATTR_ACCT_SESSION_TIME, int(float(session_time)))
        except ValueError:
            logger.warning("Bỏ qua acct_session_time không hợp lệ: %r", session_time)

    # --- Calling-Station-Id (msisdn) ---
    msisdn = _pick(record, "msisdn", "Calling_Station_Id")
    if msisdn:
        avps += _cached_str_avp(ATTR_CALLING_STATION, msisdn)

    # --- Framed-IP-Address ---
    framed_ip = _pick(record, "framed_ip", "Framed_IP_Address")
    if framed_ip:
        try:
            avps += _cached_ip_avp(ATTR_FRAMED_IP, framed_ip)
        except ValueError as e:
            logger.warning("%s — bỏ qua Framed-IP-Address", e)

    # --- NAS-IP-Address ---
    nas_ip = _pick(record, "nas_ip")
    if nas_ip:
        try:
            avps += _cached_ip_avp(ATTR_NAS_IP, nas_ip)
        except ValueError as e:
            logger.warning("%s — bỏ qua NAS-IP-Address", e)

    # --- NAS-Identifier ---
    nas_identifier = _pick(record, "nas_identifier", "NAS_Identifier")
    if nas_identifier:
        avps += _cached_str_avp(ATTR_NAS_IDENTIFIER, nas_identifier)

    # --- Vendor-Specific 3GPP (imsi, imei, rat_type, mcc_mnc) ---
    sub_attrs: List[Tuple[int, str]] = []
    imsi = _pick(record, "imsi")
    if imsi:
        sub_attrs.append((VENDOR_SUBTYPE_IMSI, imsi))
    imei = _pick(record, "imei")
    if imei:
        sub_attrs.append((VENDOR_SUBTYPE_IMEI, imei))
    rat_type = _pick(record, "rat_type")
    if rat_type:
        sub_attrs.append((VENDOR_SUBTYPE_RAT_TYPE, rat_type))
    mcc_mnc = _pick(record, "mcc_mnc")
    if mcc_mnc:
        sub_attrs.append((VENDOR_SUBTYPE_MCC_MNC, mcc_mnc))
    if sub_attrs:
        avps += _cached_vendor_specific(VENDOR_ID_3GPP, tuple(sub_attrs))

    # --- Header: Code(1) + Identifier(1) + Length(2) + Authenticator(16) ---
    total_length = 20 + len(avps)
    if total_length > 4096:
        raise ValueError(f"Gói tin dài {total_length} byte, vượt giới hạn thực tế 4096")

    header_wo_auth = bytes([RADIUS_CODE_ACCOUNTING_REQUEST, identifier & 0xFF]) \
        + struct.pack("!H", total_length)

    # Request Authenticator cho Accounting-Request (RFC 2866 §4.1):
    # MD5(Code + Identifier + Length + 16 byte 0x00 + Attributes + Shared-Secret)
    zero_auth = b"\x00" * 16
    authenticator = hashlib.md5(header_wo_auth + zero_auth + bytes(avps) + _secret_bytes(secret)).digest()

    return header_wo_auth + authenticator + bytes(avps)


def send_csv_as_radius(
    csv_path: str,
    host: str = "127.0.0.1",
    port: int = 1813,
    rate: float = 50.0,
    secret: str = DEFAULT_SHARED_SECRET,
    loop: bool = False,
    queue_size: int = 50_000,
    pacing_window_ms: float = 2.0,
    max_packets: int = 0,
    max_catchup_ms: float = 100.0,
    require_ack: bool = False,
    ack_timeout_ms: float = 1000.0,
    max_retries: int = 5,
    max_pending: int = 50_000,
    ack_drain_seconds: float = 15.0,
) -> None:
    """Đọc CSV, đóng gói từng dòng thành gói tin RADIUS, bắn UDP tới host:port.

    rate: số gói/giây mong muốn (giả lập tốc độ 1 thiết bị NAS thật gửi
    accounting request — KHÔNG phải chế độ bulk-load throughput cao như
    producer.publish_csv()). rate <= 0 nghĩa là gửi nhanh nhất có thể."""

    if queue_size < 1 or pacing_window_ms <= 0 or max_catchup_ms < pacing_window_ms:
        raise ValueError("queue_size > 0 và max_catchup_ms >= pacing_window_ms > 0")
    if ack_timeout_ms <= 0 or max_retries < 0 or max_pending < 1 or ack_drain_seconds < 0:
        raise ValueError("Cấu hình RADIUS ACK/retry không hợp lệ")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4 * 1024 * 1024)
    sock.connect((host, port))

    packet_queue: queue.Queue[object] = queue.Queue(maxsize=queue_size)
    sentinel = object()
    stop_event = threading.Event()
    counters = {
        "encoded": 0, "failed": 0, "queue_high_watermark": 0,
        "acked": 0, "responses_invalid": 0, "retries": 0, "retry_exhausted": 0,
    }
    pending: Dict[bytes, PendingPacket] = {}
    pending_by_identifier: Dict[int, Dict[bytes, PendingPacket]] = {}
    retry_heap: List[Tuple[float, bytes]] = []
    pending_lock = threading.Lock()
    ack_receiver_stop = threading.Event()

    def pending_count() -> int:
        with pending_lock:
            return len(pending)

    def receive_responses() -> None:
        sock.settimeout(0.1)
        secret_bytes = _secret_bytes(secret)
        while not ack_receiver_stop.is_set():
            try:
                response = sock.recv(4096)
            except socket.timeout:
                continue
            except OSError:
                return
            if len(response) < 20 or response[0] != 5 or int.from_bytes(response[2:4], "big") != len(response):
                counters["responses_invalid"] += 1
                continue
            identifier = response[1]
            matched_auth: bytes | None = None
            with pending_lock:
                candidates = list(pending_by_identifier.get(identifier, {}).items())
            for request_auth, _candidate in candidates:
                expected = hashlib.md5(response[:4] + request_auth + response[20:] + secret_bytes).digest()
                if hmac.compare_digest(response[4:20], expected):
                    matched_auth = request_auth
                    break
            if matched_auth is None:
                counters["responses_invalid"] += 1
                continue
            with pending_lock:
                item = pending.pop(matched_auth, None)
                identifier_items = pending_by_identifier.get(identifier)
                if identifier_items is not None:
                    identifier_items.pop(matched_auth, None)
                    if not identifier_items:
                        pending_by_identifier.pop(identifier, None)
            if item is not None:
                counters["acked"] += 1

    ack_receiver: threading.Thread | None = None
    if require_ack:
        ack_receiver = threading.Thread(
            target=receive_responses, name="radius-response-receiver", daemon=True
        )
        ack_receiver.start()

    def track_packet(packet: bytes) -> None:
        now = time.perf_counter()
        authenticator = packet[4:20]
        item = PendingPacket(
            packet=packet,
            identifier=packet[1],
            authenticator=authenticator,
            deadline=now + ack_timeout_ms / 1000.0,
        )
        with pending_lock:
            pending[authenticator] = item
            pending_by_identifier.setdefault(item.identifier, {})[authenticator] = item
        heapq.heappush(retry_heap, (item.deadline, authenticator))

    def service_retries() -> None:
        now = time.perf_counter()
        while retry_heap and retry_heap[0][0] <= now:
            scheduled_deadline, authenticator = heapq.heappop(retry_heap)
            with pending_lock:
                item = pending.get(authenticator)
                if item is None or item.deadline != scheduled_deadline:
                    continue
                if item.retries >= max_retries:
                    pending.pop(authenticator, None)
                    identifier_items = pending_by_identifier.get(item.identifier)
                    if identifier_items is not None:
                        identifier_items.pop(authenticator, None)
                        if not identifier_items:
                            pending_by_identifier.pop(item.identifier, None)
                    counters["retry_exhausted"] += 1
                    continue
            sock.send(item.packet)
            counters["retries"] += 1
            item.retries += 1
            backoff = (ack_timeout_ms / 1000.0) * (2 ** min(item.retries, 4))
            item.deadline = time.perf_counter() + backoff + random.uniform(0, backoff * 0.1)
            heapq.heappush(retry_heap, (item.deadline, authenticator))

    def put_interruptibly(item: object) -> bool:
        while not stop_event.is_set():
            try:
                packet_queue.put(item, timeout=0.1)
                return True
            except queue.Full:
                continue
        return False

    def encode_records() -> None:
        identifier = 0
        try:
            while not stop_event.is_set():
                for record in LocalCSVReader(csv_path).read_records():
                    if stop_event.is_set():
                        return
                    if max_packets > 0 and counters["encoded"] >= max_packets:
                        return
                    try:
                        packet = build_radius_packet(record, identifier, secret)
                    except ValueError as exc:
                        counters["failed"] += 1
                        logger.warning("Bỏ qua record không đóng gói được: %s", exc)
                        identifier = (identifier + 1) & 0xFF
                        continue
                    if not put_interruptibly(packet):
                        return
                    counters["encoded"] += 1
                    counters["queue_high_watermark"] = max(
                        counters["queue_high_watermark"], packet_queue.qsize()
                    )
                    identifier = (identifier + 1) & 0xFF
                if not loop:
                    return
        finally:
            put_interruptibly(sentinel)

    burst_size = max(1, round(rate * pacing_window_ms / 1000.0)) if rate > 0 else 512
    encoder = threading.Thread(target=encode_records, name="radius-packet-encoder", daemon=True)
    encoder.start()

    # Prefill tách chi phí đọc CSV/mã hóa khỏi nhịp gửi. Nếu bắt đầu đồng hồ ngay
    # khi encoder còn giành GIL, scheduler sẽ tích lũy "nợ" rồi bắn burst quá trần.
    prefill_target = min(
        queue_size,
        max_packets if max_packets > 0 else max(5_000, burst_size * 20),
    )
    prefill_started = time.perf_counter()
    while encoder.is_alive() and packet_queue.qsize() < prefill_target:
        time.sleep(0.001)
    prefill_duration = time.perf_counter() - prefill_started

    total_sent = 0
    t_start = time.perf_counter()
    last_log_at = t_start
    last_log_sent = 0
    next_deadline = t_start

    logger.info(
        "[RADIUS-UDP] Bắt đầu gửi từ %s tới %s:%d | rate=%s | loop=%s | "
        "prefetch_queue=%d | prefilled=%d | prefill_ms=%.1f | pacing_window_ms=%.1f | "
        "burst=%d | max_catchup_ms=%.1f | max_packets=%d | require_ack=%s | "
        "ack_timeout_ms=%.0f | max_retries=%d | max_pending=%d | secret=%s",
        csv_path, host, port,
        f"{rate} pkt/s" if rate > 0 else "không giới hạn",
        loop, queue_size, packet_queue.qsize(), prefill_duration * 1000,
        pacing_window_ms, burst_size, max_catchup_ms, max_packets, require_ack,
        ack_timeout_ms, max_retries, max_pending,
        "(mặc định)" if secret == DEFAULT_SHARED_SECRET else "(tuỳ biến)",
    )

    try:
        while True:
            batch: List[bytes] = []
            finished = False
            while len(batch) < burst_size:
                item = packet_queue.get()
                if item is sentinel:
                    finished = True
                    break
                batch.append(item)  # type: ignore[arg-type]

            for packet in batch:
                while require_ack and pending_count() >= max_pending:
                    service_retries()
                    time.sleep(0.001)
                sock.send(packet)
                if require_ack:
                    track_packet(packet)
            total_sent += len(batch)

            if require_ack:
                service_retries()

            if rate > 0 and batch:
                next_deadline += len(batch) / rate
                now_before_sleep = time.perf_counter()
                # Không bù backlog quá một pacing window: UDP burst vượt mục tiêu
                # dễ làm đầy kernel receive buffer dù tốc độ trung bình vẫn đẹp.
                max_lag = max_catchup_ms / 1000.0
                if next_deadline < now_before_sleep - max_lag:
                    next_deadline = now_before_sleep
                remaining = next_deadline - now_before_sleep
                if remaining > 0:
                    time.sleep(remaining)

            now = time.perf_counter()
            log_elapsed = now - last_log_at
            if log_elapsed >= 1.0 or finished:
                pending_now = pending_count()
                status = "PRESSURE" if require_ack and pending_now >= max_pending * 0.7 else "OK"
                logger.info(
                    "[SENDER][%s] target=%.0f/s actual=%.1f/s sent=%d encoded_queue=%d/%d "
                    "pending_ack=%d/%d acked=%d retries=%d retry_exhausted=%d "
                    "invalid_responses=%d encode_failed=%d",
                    status, rate,
                    (total_sent - last_log_sent) / max(log_elapsed, 1e-9), total_sent,
                    packet_queue.qsize(), queue_size, pending_now, max_pending,
                    counters["acked"], counters["retries"], counters["retry_exhausted"],
                    counters["responses_invalid"], counters["failed"],
                )
                last_log_at = now
                last_log_sent = total_sent

            if finished:
                break
    except KeyboardInterrupt:
        logger.info("[RADIUS-UDP] Dừng theo yêu cầu người dùng.")
    finally:
        stop_event.set()
        encoder.join(timeout=2.0)

    send_duration = time.perf_counter() - t_start
    if require_ack and pending_count() and ack_drain_seconds > 0:
        drain_deadline = time.perf_counter() + ack_drain_seconds
        logger.info(
            "[SENDER][DRAIN] đã gửi hết nguồn, chờ ACK pending=%d tối đa %.1fs",
            pending_count(), ack_drain_seconds,
        )
        while pending_count() and time.perf_counter() < drain_deadline:
            service_retries()
            time.sleep(0.01)

    unacked = pending_count() + counters["retry_exhausted"]
    ack_receiver_stop.set()
    sock.close()
    if ack_receiver is not None:
        ack_receiver.join(timeout=1.0)

    logger.info(
        "[SENDER][FINAL] sent=%d acked=%d unacked=%d retries=%d invalid_responses=%d "
        "encode_failed=%d send_duration=%.2fs send_rate=%.1f/s",
        total_sent, counters["acked"], unacked, counters["retries"],
        counters["responses_invalid"], counters["failed"], send_duration,
        total_sent / max(send_duration, 1e-6),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Đọc RADIUS CSV, đóng gói thành gói tin RADIUS thật, gửi UDP giả lập thiết bị NAS/GGSN."
    )
    parser.add_argument("--csv", required=True, help="Đường dẫn file CSV đầu vào (cùng schema producer.py)")
    parser.add_argument("--host", default="127.0.0.1", help="Host đích (mặc định 127.0.0.1)")
    parser.add_argument("--port", type=int, default=1813, help="Cổng UDP đích (mặc định 1813)")
    parser.add_argument("--rate", type=float, default=50.0, help="Số gói/giây, <=0 nghĩa là gửi nhanh nhất có thể")
    parser.add_argument("--secret", default=DEFAULT_SHARED_SECRET, help="Shared secret dùng tính Request Authenticator")
    parser.add_argument("--loop", action="store_true", help="Lặp lại vô hạn khi đọc hết file CSV")
    parser.add_argument("--queue-size", type=int, default=50_000,
                        help="Số packet đã mã hóa được prefetch trong RAM (mặc định 50000)")
    parser.add_argument("--pacing-window-ms", type=float, default=2.0,
                        help="Cửa sổ micro-burst để pacing chính xác ở tốc độ cao (mặc định 2ms)")
    parser.add_argument("--max-packets", type=int, default=0,
                        help="Dừng sau N packet; 0 nghĩa là gửi hết file hoặc chạy theo --loop")
    parser.add_argument("--max-catchup-ms", type=float, default=100.0,
                        help="Giới hạn nợ pacing được bù để tránh burst lớn (mặc định 100ms)")
    parser.add_argument("--require-ack", action="store_true",
                        help="Chờ RADIUS Accounting-Response và retry packet chưa được ACK")
    parser.add_argument("--ack-timeout-ms", type=float, default=1000.0)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--max-pending", type=int, default=50_000)
    parser.add_argument("--ack-drain-seconds", type=float, default=15.0)
    args = parser.parse_args()

    send_csv_as_radius(
        csv_path=args.csv,
        host=args.host,
        port=args.port,
        rate=args.rate,
        secret=args.secret,
        loop=args.loop,
        queue_size=args.queue_size,
        pacing_window_ms=args.pacing_window_ms,
        max_packets=args.max_packets,
        max_catchup_ms=args.max_catchup_ms,
        require_ack=args.require_ack,
        ack_timeout_ms=args.ack_timeout_ms,
        max_retries=args.max_retries,
        max_pending=args.max_pending,
        ack_drain_seconds=args.ack_drain_seconds,
    )
