#!/usr/bin/env python3
#pipeline/ingestion/radius_udp_sender.py
#
# Bootstrap: thêm thư mục gốc project vào sys.path để `import pipeline.*`
# hoạt động khi chạy trực tiếp với python3 script.py
import sys, os as _os
sys.path.insert(0, _os.path.abspath(_os.path.join(_os.path.dirname(__file__), "..", "..")))

"""
Load-test tool — CSV → mirrored UDP RADIUS Accounting-Request

Đọc file CSV (cùng schema với producer.py) qua LocalCSVReader, ĐÓNG GÓI từng
dòng thành 1 gói tin RADIUS nhị phân đúng chuẩn RFC 2866 (Code=4 Accounting-
Request) + Vendor-Specific Attribute 3GPP (RFC/3GPP TS 29.061), rồi gửi qua
UDP tới `--host:--port`.

Mục đích: giả lập capture server bên ngoài đang mirror RADIUS accounting request
qua mạng — dùng để test đầu vào pipeline.ingestion.packet_reader.PacketReader
(listener UDP đang chạy sẵn), KHÔNG đi qua đường Kafka producer trực tiếp
như pipeline.ingestion.producer.RadiusLogProducer.publish_csv().

Sender luôn fire-and-forget: không nhận Accounting-Response, không chờ ACK và
không retry. Độ bền/replay thuộc capture server thật nằm ngoài repo.

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
import queue
import threading
from datetime import datetime, timezone
from functools import lru_cache
from typing import Dict, Any, List, Tuple, Optional

from pipeline.ingestion.csv_reader import LocalCSVReader

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

ATTR_ACCT_STATUS_TYPE  = 0x28  # 40 — Integer
ATTR_ACCT_DELAY_TIME   = 0x29  # 41 — Integer
ATTR_ACCT_SESSION_ID   = 0x2c  # 44 — String
ATTR_ACCT_SESSION_TIME = 0x2d  # 45 — Integer
ATTR_EVENT_TIMESTAMP   = 0x37  # 55 — Date (Unix timestamp)
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


class TokenBucket:
    """Single-owner rate limiter for the passive UDP load sender."""
    def __init__(self, rate: float):
        self.rate = rate
        self.tokens = 0.0
        self.last_update = time.perf_counter()
        self.capacity = max(10.0, rate * 0.05) if rate > 0 else 0.0

    def acquire(self, num_tokens: int = 1, timeout: Optional[float] = None) -> bool:
        if self.rate <= 0:
            return True
        deadline = time.perf_counter() + timeout if timeout is not None else None
        while True:
            now = time.perf_counter()
            elapsed = max(0.0, now - self.last_update)
            self.last_update = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            if self.tokens >= num_tokens:
                self.tokens -= num_tokens
                return True
            needed = num_tokens - self.tokens
            wait_seconds = needed / self.rate
            if deadline is not None and now + wait_seconds > deadline:
                return False
            time.sleep(min(wait_seconds, 0.001))

def _pick(record: Dict[str, Any], *keys: str) -> Optional[str]:
    """Lấy giá trị không rỗng đầu tiên trong các key thay thế nhau."""
    for k in keys:
        v = record.get(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return None


def _encode_avp(attr_type: int, value: bytes) -> bytes:
    """Đóng gói 1 Attribute-Value-Pair chuẩn RADIUS: Type(1) + Length(1) + Value."""
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


def _parse_event_epoch(value: str) -> int:
    try:
        return int(float(value))
    except ValueError:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())


@lru_cache(maxsize=131_072)
def _cached_str_avp(attr_type: int, value: str) -> bytes:
    return _encode_str_avp(attr_type, value)


@lru_cache(maxsize=65_536)
def _cached_ip_avp(attr_type: int, value: str) -> bytes:
    return _encode_ip_avp(attr_type, value)


def _encode_vendor_specific(vendor_id: int, sub_attrs: List[Tuple[int, str]]) -> bytes:
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


def _patch_packet_identifier(packet: bytes, identifier: int, secret_bytes: bytes) -> bytes:
    """Cập nhật identifier byte (byte 1) và tính lại MD5 Request Authenticator (bytes 4:20)."""
    header_wo_auth = bytes([RADIUS_CODE_ACCOUNTING_REQUEST, identifier & 0xFF]) + packet[2:4]
    zero_auth = b"\x00" * 16
    auth = hashlib.md5(header_wo_auth + zero_auth + packet[20:] + secret_bytes).digest()
    return header_wo_auth + auth + packet[20:]


def build_radius_packet(record: Dict[str, Any], identifier: int, secret: str) -> bytes:
    """Đóng gói 1 record CSV thành 1 gói tin RADIUS Accounting-Request nhị phân."""
    avps = bytearray()

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

    session_id = _pick(record, "acct_session_id")
    if session_id:
        avps += _encode_str_avp(ATTR_ACCT_SESSION_ID, session_id)

    session_time = _pick(record, "acct_session_time")
    if session_time is not None:
        try:
            avps += _encode_int_avp(ATTR_ACCT_SESSION_TIME, int(float(session_time)))
        except ValueError:
            logger.warning("Bỏ qua acct_session_time không hợp lệ: %r", session_time)

    event_timestamp = _pick(record, "event_timestamp", "timestamp")
    if event_timestamp:
        try:
            avps += _encode_int_avp(ATTR_EVENT_TIMESTAMP, _parse_event_epoch(event_timestamp))
        except (ValueError, OverflowError):
            logger.warning("Bỏ qua event_timestamp không hợp lệ: %r", event_timestamp)

    acct_delay_time = _pick(record, "acct_delay_time")
    if acct_delay_time is not None:
        try:
            avps += _encode_int_avp(ATTR_ACCT_DELAY_TIME, max(0, int(float(acct_delay_time))))
        except ValueError:
            logger.warning("Bỏ qua acct_delay_time không hợp lệ: %r", acct_delay_time)

    msisdn = _pick(record, "msisdn", "Calling_Station_Id")
    if msisdn:
        avps += _cached_str_avp(ATTR_CALLING_STATION, msisdn)

    framed_ip = _pick(record, "framed_ip", "Framed_IP_Address")
    if framed_ip:
        try:
            avps += _cached_ip_avp(ATTR_FRAMED_IP, framed_ip)
        except ValueError as e:
            logger.warning("%s — bỏ qua Framed-IP-Address", e)

    nas_ip = _pick(record, "nas_ip")
    if nas_ip:
        try:
            avps += _cached_ip_avp(ATTR_NAS_IP, nas_ip)
        except ValueError as e:
            logger.warning("%s — bỏ qua NAS-IP-Address", e)

    nas_identifier = _pick(record, "nas_identifier", "NAS_Identifier")
    if nas_identifier:
        avps += _cached_str_avp(ATTR_NAS_IDENTIFIER, nas_identifier)

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

    total_length = 20 + len(avps)
    if total_length > 4096:
        raise ValueError(f"Gói tin dài {total_length} byte, vượt giới hạn thực tế 4096")

    header_wo_auth = bytes([RADIUS_CODE_ACCOUNTING_REQUEST, identifier & 0xFF]) \
        + struct.pack("!H", total_length)

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
    queue_size: int = 100_000,
    pacing_window_ms: float = 2.0,
    max_packets: int = 0,
    max_catchup_ms: float = 100.0,
    num_sockets: int = 1,
) -> None:
    if queue_size < 1 or pacing_window_ms <= 0 or max_catchup_ms < pacing_window_ms:
        raise ValueError("queue_size > 0 và max_catchup_ms >= pacing_window_ms > 0")
    num_sockets = max(1, num_sockets)

    sockets: List[socket.socket] = []
    for _ in range(num_sockets):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4 * 1024 * 1024)
        s.connect((host, port))
        sockets.append(s)
    _sock_idx = 0
    per_socket_identifiers = [0] * num_sockets
    secret_bytes = _secret_bytes(secret)
    rate_limiter = TokenBucket(rate)

    packet_queue: queue.Queue[object] = queue.Queue(maxsize=queue_size)
    sentinel = object()
    stop_event = threading.Event()
    counters = {
        "encoded": 0, "failed": 0, "queue_high_watermark": 0,
        "send_failed": 0,
    }

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
                        failed = counters["failed"]
                        if failed <= 5 or failed % 1000 == 0:
                            logger.warning(
                                "Bỏ qua record không đóng gói được: %s encode_failed_total=%d",
                                exc, failed,
                            )
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
    # Preserve bounded catch-up credit across short OS scheduling pauses.
    if rate > 0:
        catchup_tokens = rate * max_catchup_ms / 1000.0
        rate_limiter.capacity = float(max(burst_size, round(catchup_tokens)))
    encoder = threading.Thread(target=encode_records, name="radius-packet-encoder", daemon=True)
    encoder.start()

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
    rate_limiter.tokens = 0.0
    rate_limiter.last_update = t_start
    last_log_at = t_start
    last_log_sent = 0

    logger.info(
        "[RADIUS-MIRROR] source=%s target=%s:%d rate=%s loop=%s sockets=%d "
        "queue=%d prefilled=%d prefill_ms=%.1f pacing_window_ms=%.1f burst=%d "
        "max_catchup_ms=%.1f max_packets=%d mode=fire-and-forget",
        csv_path, host, port,
        f"{rate} pkt/s" if rate > 0 else "unlimited",
        loop, num_sockets, queue_size, packet_queue.qsize(), prefill_duration * 1000,
        pacing_window_ms, burst_size, max_catchup_ms, max_packets,
    )

    try:
        while True:
            batch: List[bytes] = []
            finished = False
            while len(batch) < burst_size:
                queued = packet_queue.get()
                if queued is sentinel:
                    finished = True
                    break
                batch.append(queued)  # type: ignore[arg-type]

            if batch:
                rate_limiter.acquire(len(batch))
            for raw_packet in batch:
                socket_idx = _sock_idx % num_sockets
                _sock_idx += 1
                identifier = per_socket_identifiers[socket_idx]
                per_socket_identifiers[socket_idx] = (identifier + 1) & 0xFF
                packet = _patch_packet_identifier(raw_packet, identifier, secret_bytes)
                try:
                    sockets[socket_idx].send(packet)
                except OSError:
                    counters["send_failed"] += 1
                    raise
                total_sent += 1

            now = time.perf_counter()
            log_elapsed = now - last_log_at
            if log_elapsed >= 1.0 or (finished and total_sent != last_log_sent):
                actual_rate = (total_sent - last_log_sent) / max(log_elapsed, 1e-9)
                status = "DEGRADED" if rate > 0 and actual_rate < rate * 0.9 else "OK"
                logger.info(
                    "[SENDER][%s] target=%.0f/s actual=%.1f/s sent=%d "
                    "encoded_queue=%d/%d encode_failed=%d send_failed=%d",
                    status, rate, actual_rate, total_sent,
                    packet_queue.qsize(), queue_size, counters["failed"], counters["send_failed"],
                )
                last_log_at = now
                last_log_sent = total_sent

            if finished:
                break
    except KeyboardInterrupt:
        logger.info("[RADIUS-MIRROR] Dừng theo yêu cầu người dùng.")
    finally:
        stop_event.set()
        encoder.join(timeout=2.0)
        for sock in sockets:
            sock.close()

    send_duration = time.perf_counter() - t_start
    logger.info(
        "[SENDER][FINAL] sent=%d encode_failed=%d send_failed=%d duration=%.2fs rate=%.1f/s",
        total_sent, counters["failed"], counters["send_failed"], send_duration,
        total_sent / max(send_duration, 1e-6),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Đọc RADIUS CSV, đóng gói và phát UDP fire-and-forget để giả lập capture mirror."
    )
    parser.add_argument("--csv", required=True, help="Đường dẫn file CSV đầu vào (cùng schema producer.py)")
    parser.add_argument("--host", default="127.0.0.1", help="Host đích (mặc định 127.0.0.1)")
    parser.add_argument("--port", type=int, default=1813, help="Cổng UDP đích (mặc định 1813)")
    parser.add_argument("--rate", type=float, default=50.0, help="Số gói/giây, <=0 nghĩa là gửi nhanh nhất có thể")
    parser.add_argument("--secret", default=DEFAULT_SHARED_SECRET, help="Shared secret dùng tính Request Authenticator")
    parser.add_argument("--loop", action="store_true", help="Lặp lại vô hạn khi đọc hết file CSV")
    parser.add_argument("--queue-size", type=int, default=100_000,
                        help="Số packet đã mã hóa được prefetch trong RAM (mặc định 100000)")
    parser.add_argument("--pacing-window-ms", type=float, default=2.0,
                        help="Cửa sổ micro-burst để pacing chính xác ở tốc độ cao (mặc định 2ms)")
    parser.add_argument("--max-packets", type=int, default=0,
                        help="Dừng sau N packet; 0 nghĩa là gửi hết file hoặc chạy theo --loop")
    parser.add_argument("--max-catchup-ms", type=float, default=100.0,
                        help="Giới hạn nợ pacing được bù để tránh burst lớn (mặc định 100ms)")
    parser.add_argument("--num-sockets", type=int, default=8,
                        help="Số lượng UDP client socket (source ports) dùng để load-balance qua SO_REUSEPORT (mặc định 8)")
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
        num_sockets=args.num_sockets,
    )
