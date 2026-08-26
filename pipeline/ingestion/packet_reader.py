from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import socket
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict

logger = logging.getLogger(__name__)


class PacketDecodeError(ValueError):
    pass


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

    async def listen_radius_packets(self, port: int = DEFAULT_RADIUS_PORT) -> AsyncIterator[Dict[str, Any]]:
        loop = asyncio.get_running_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setblocking(False)
        sock.bind(("0.0.0.0", port))
        logger.info("RADIUS listener ready on UDP/%d", port)
        try:
            while True:
                packet, address = await loop.sock_recvfrom(sock, 4096)
                try:
                    yield self.decode_radius(packet)
                except PacketDecodeError as exc:
                    logger.warning("Rejected RADIUS packet from %s: %s", address, exc)
        finally:
            sock.close()


if __name__ == "__main__":
    async def _main() -> None:
        async for decoded in PacketReader().listen_radius_packets():
            print(decoded)
    asyncio.run(_main())
