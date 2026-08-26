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
        avps += _encode_str_avp(ATTR_CALLING_STATION, msisdn)

    # --- Framed-IP-Address ---
    framed_ip = _pick(record, "framed_ip", "Framed_IP_Address")
    if framed_ip:
        try:
            avps += _encode_ip_avp(ATTR_FRAMED_IP, framed_ip)
        except ValueError as e:
            logger.warning("%s — bỏ qua Framed-IP-Address", e)

    # --- NAS-IP-Address ---
    nas_ip = _pick(record, "nas_ip")
    if nas_ip:
        try:
            avps += _encode_ip_avp(ATTR_NAS_IP, nas_ip)
        except ValueError as e:
            logger.warning("%s — bỏ qua NAS-IP-Address", e)

    # --- NAS-Identifier ---
    nas_identifier = _pick(record, "nas_identifier", "NAS_Identifier")
    if nas_identifier:
        avps += _encode_str_avp(ATTR_NAS_IDENTIFIER, nas_identifier)

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
        avps += _encode_vendor_specific(VENDOR_ID_3GPP, sub_attrs)

    # --- Header: Code(1) + Identifier(1) + Length(2) + Authenticator(16) ---
    total_length = 20 + len(avps)
    if total_length > 4096:
        raise ValueError(f"Gói tin dài {total_length} byte, vượt giới hạn thực tế 4096")

    header_wo_auth = bytes([RADIUS_CODE_ACCOUNTING_REQUEST, identifier & 0xFF]) \
        + struct.pack("!H", total_length)

    # Request Authenticator cho Accounting-Request (RFC 2866 §4.1):
    # MD5(Code + Identifier + Length + 16 byte 0x00 + Attributes + Shared-Secret)
    zero_auth = b"\x00" * 16
    authenticator = hashlib.md5(header_wo_auth + zero_auth + bytes(avps) + secret.encode()).digest()

    return header_wo_auth + authenticator + bytes(avps)


def send_csv_as_radius(
    csv_path: str,
    host: str = "127.0.0.1",
    port: int = 1813,
    rate: float = 50.0,
    secret: str = DEFAULT_SHARED_SECRET,
    loop: bool = False,
) -> None:
    """Đọc CSV, đóng gói từng dòng thành gói tin RADIUS, bắn UDP tới host:port.

    rate: số gói/giây mong muốn (giả lập tốc độ 1 thiết bị NAS thật gửi
    accounting request — KHÔNG phải chế độ bulk-load throughput cao như
    producer.publish_csv()). rate <= 0 nghĩa là gửi nhanh nhất có thể."""

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    identifier = 0
    interval = 1.0 / rate if rate > 0 else 0.0

    total_sent = 0
    total_failed = 0
    t_start = time.time()

    def run_once() -> Tuple[int, int]:
        nonlocal identifier
        sent, failed = 0, 0
        reader = LocalCSVReader(csv_path)
        for record in reader.read_records():
            try:
                packet = build_radius_packet(record, identifier, secret)
            except ValueError as e:
                failed += 1
                logger.warning("Bỏ qua record không đóng gói được: %s", e)
                identifier = (identifier + 1) % 256
                continue

            sock.sendto(packet, (host, port))
            sent += 1
            identifier = (identifier + 1) % 256

            if sent % 500 == 0:
                logger.info("[RADIUS-UDP] Đã gửi %d gói tới %s:%d", sent, host, port)

            if interval > 0:
                time.sleep(interval)
        return sent, failed

    logger.info(
        "[RADIUS-UDP] Bắt đầu gửi từ %s tới %s:%d | rate=%s | loop=%s | secret=%s",
        csv_path, host, port,
        f"{rate} pkt/s" if rate > 0 else "không giới hạn",
        loop, "(mặc định)" if secret == DEFAULT_SHARED_SECRET else "(tuỳ biến)",
    )

    try:
        while True:
            sent, failed = run_once()
            total_sent += sent
            total_failed += failed
            if not loop:
                break
            logger.info("[RADIUS-UDP] Hết file, lặp lại (--loop đang bật)...")
    except KeyboardInterrupt:
        logger.info("[RADIUS-UDP] Dừng theo yêu cầu người dùng.")
    finally:
        sock.close()

    duration = time.time() - t_start
    logger.info(
        "[RADIUS-UDP] Hoàn tất: %d gói gửi thành công, %d record bị bỏ qua, %.2fs (%.1f pkt/s)",
        total_sent, total_failed, duration, total_sent / max(duration, 1e-6),
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
    args = parser.parse_args()

    send_csv_as_radius(
        csv_path=args.csv,
        host=args.host,
        port=args.port,
        rate=args.rate,
        secret=args.secret,
        loop=args.loop,
    )
