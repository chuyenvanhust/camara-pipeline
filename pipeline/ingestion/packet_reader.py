from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import socket
import struct
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, Tuple

logger = logging.getLogger(__name__)


class PacketDecodeError(ValueError):
    pass


@dataclass(frozen=True)
class RadiusEnvelope:
    record: Dict[str, Any]
    address: Tuple[str, int]
    identifier: int
    request_authenticator: bytes
    event_id: str


class PacketReader:
    DEFAULT_RADIUS_PORT = 1813
    VENDOR_3GPP = 10415
    STANDARD = {40: "acct_status_type", 44: "acct_session_id", 45: "acct_session_time",
                31: "msisdn", 8: "framed_ip", 4: "nas_ip", 32: "nas_identifier"}
    VENDOR = {1: "imsi", 20: "imei", 21: "rat_type", 8: "mcc_mnc"}
    STATUS = {1: "start", 2: "stop", 3: "interim-update", 7: "accounting-on", 8: "accounting-off"}

    def __init__(self, shared_secret: str | None = None):
        secret = shared_secret if shared_secret is not None else os.getenv("RADIUS_SHARED_SECRET")
        if not secret:
            raise ValueError("RADIUS_SHARED_SECRET is required")
        self.secret = secret.encode("utf-8")
        self.stats = {"datagrams": 0, "decoded": 0, "rejected": 0}
        self._socket: socket.socket | None = None

    def build_accounting_response(self, envelope: RadiusEnvelope) -> bytes:
        header = bytes([5, envelope.identifier]) + struct.pack("!H", 20)
        authenticator = hashlib.md5(
            header + envelope.request_authenticator + self.secret
        ).digest()
        return header + authenticator

    async def send_accounting_response(self, envelope: RadiusEnvelope) -> None:
        if self._socket is None:
            raise RuntimeError("RADIUS listener socket is not available")
        loop = asyncio.get_running_loop()
        await loop.sock_sendto(
            self._socket,
            self.build_accounting_response(envelope),
            envelope.address,
        )

    @staticmethod
    def _text(value: bytes) -> str:
        try:
            result = value.decode("utf-8").strip()
        except UnicodeDecodeError as exc:
            raise PacketDecodeError("attribute is not valid UTF-8") from exc
        if not result:
            raise PacketDecodeError("empty string attribute")
        return result

    def decode_radius(self, packet: bytes) -> Dict[str, Any]:
        if len(packet) < 20:
            raise PacketDecodeError("packet shorter than RADIUS header")
        if packet[0] != 4:
            raise PacketDecodeError("only Accounting-Request (code 4) is accepted")
        declared = int.from_bytes(packet[2:4], "big")
        if declared != len(packet) or declared > 4096:
            raise PacketDecodeError("invalid RADIUS packet length")
        expected = hashlib.md5(packet[:4] + (b"\x00" * 16) + packet[20:] + self.secret).digest()
        if not hmac.compare_digest(packet[4:20], expected):
            raise PacketDecodeError("invalid Accounting-Request authenticator")

        result: Dict[str, Any] = {}
        offset = 20
        while offset < declared:
            if offset + 2 > declared:
                raise PacketDecodeError("truncated attribute header")
            attr_type, attr_len = packet[offset], packet[offset + 1]
            if attr_len < 2 or offset + attr_len > declared:
                raise PacketDecodeError("invalid attribute length")
            value = packet[offset + 2:offset + attr_len]
            if attr_type == 26:
                self._decode_vendor(value, result)
            elif attr_type in self.STANDARD:
                name = self.STANDARD[attr_type]
                if attr_type in {4, 8}:
                    if len(value) != 4:
                        raise PacketDecodeError(f"{name} must contain four bytes")
                    result[name] = socket.inet_ntoa(value)
                elif attr_type in {40, 45}:
                    if len(value) != 4:
                        raise PacketDecodeError(f"{name} must contain a 32-bit integer")
                    number = int.from_bytes(value, "big")
                    result[name] = self.STATUS.get(number, number) if attr_type == 40 else number
                else:
                    result[name] = self._text(value)
            offset += attr_len

        now = datetime.now(timezone.utc).isoformat()
        result["event_timestamp"] = now
        result["ingest_timestamp"] = now
        return result

    def _decode_vendor(self, value: bytes, result: Dict[str, Any]) -> None:
        if len(value) < 6:
            raise PacketDecodeError("truncated Vendor-Specific attribute")
        if int.from_bytes(value[:4], "big") != self.VENDOR_3GPP:
            return
        offset = 4
        while offset < len(value):
            if offset + 2 > len(value):
                raise PacketDecodeError("truncated vendor attribute")
            vendor_type, vendor_len = value[offset], value[offset + 1]
            if vendor_len < 2 or offset + vendor_len > len(value):
                raise PacketDecodeError("invalid vendor attribute length")
            if vendor_type in self.VENDOR:
                result[self.VENDOR[vendor_type]] = self._text(value[offset + 2:offset + vendor_len])
            offset += vendor_len

    async def listen_radius_packets(self, port: int = DEFAULT_RADIUS_PORT) -> AsyncIterator[RadiusEnvelope]:
        loop = asyncio.get_running_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # F-PARALLEL: SO_REUSEPORT cho phép NHIỀU tiến trình/container cùng bind
        # đúng 1 port UDP — kernel Linux tự chia datagram đến giữa chúng (round-
        # robin theo hash), cho phép chạy N instance radius-ingestion song song
        # THẬT (mỗi instance 1 process riêng, không bị GIL chia sẻ) khi chuyển
        # sang host Linux nhiều core (`docker compose up --scale radius-ingestion=N`
        # + `network_mode: host`, xem docker-compose.prod.yml). Trên Windows/macOS
        # Docker Desktop không hỗ trợ SO_REUSEPORT -> tự động bỏ qua, không lỗi.
        so_reuseport_enabled = os.getenv("RADIUS_UDP_SO_REUSEPORT", "true").strip().lower() not in ("0", "false", "")
        if so_reuseport_enabled and hasattr(socket, "SO_REUSEPORT"):
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                logger.warning("SO_REUSEPORT không khả dụng trên nền tảng này, bỏ qua")
        requested_buffer = int(os.getenv("RADIUS_UDP_RECEIVE_BUFFER_BYTES", str(16 * 1024 * 1024)))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, requested_buffer)
        actual_buffer = sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        sock.setblocking(False)
        sock.bind(("0.0.0.0", port))
        self._socket = sock
        logger.info(
            "RADIUS listener ready on UDP/%d receive_buffer_requested=%d receive_buffer_actual=%d "
            "so_reuseport=%s",
            port, requested_buffer, actual_buffer,
            so_reuseport_enabled and hasattr(socket, "SO_REUSEPORT"),
        )
        try:
            while True:
                packet, address = await loop.sock_recvfrom(sock, 4096)
                self.stats["datagrams"] += 1
                try:
                    decoded = self.decode_radius(packet)
                    self.stats["decoded"] += 1
                    request_authenticator = packet[4:20]
                    event_id = f"radius:{request_authenticator.hex()}"
                    decoded["radius_event_id"] = event_id
                    yield RadiusEnvelope(
                        record=decoded,
                        address=(address[0], address[1]),
                        identifier=packet[1],
                        request_authenticator=request_authenticator,
                        event_id=event_id,
                    )
                except PacketDecodeError as exc:
                    self.stats["rejected"] += 1
                    rejected = self.stats["rejected"]
                    if rejected <= 5 or rejected % 1000 == 0:
                        logger.warning(
                            "Rejected RADIUS packet from %s: %s rejected_total=%d",
                            address, exc, rejected,
                        )
        finally:
            self._socket = None
            sock.close()


if __name__ == "__main__":
    async def _main() -> None:
        async for envelope in PacketReader().listen_radius_packets():
            print(envelope.record)
    asyncio.run(_main())
