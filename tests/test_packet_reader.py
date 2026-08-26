"""Tests for RADIUS UDP packet decoding."""

from __future__ import annotations

import hashlib
import socket
import struct

import pytest

from pipeline.ingestion.packet_reader import PacketDecodeError, PacketReader


SECRET = b"camara-radius-dev-secret"


def _build_accounting_packet(attributes: bytes) -> bytes:
    code = 4
    identifier = 1
    length = 20 + len(attributes)
    header = struct.pack("!BBH", code, identifier, length)
    authenticator = b"\x00" * 16
    body = header + authenticator + attributes
    digest = hashlib.md5(body[:4] + b"\x00" * 16 + body[20:] + SECRET).digest()
    return body[:4] + digest + body[20:]


def _attr_string(attr_type: int, value: str) -> bytes:
    encoded = value.encode("utf-8")
    return struct.pack("!BB", attr_type, 2 + len(encoded)) + encoded


def _attr_int(attr_type: int, value: int) -> bytes:
    return struct.pack("!BBI", attr_type, 6, value)


def _attr_ip(attr_type: int, ip: str) -> bytes:
    return struct.pack("!BB", attr_type, 6) + socket.inet_aton(ip)


@pytest.fixture
def reader() -> PacketReader:
    return PacketReader(shared_secret=SECRET.decode())


class TestPacketReader:
    def test_decode_start_with_string_status(self, reader: PacketReader):
        attrs = (
            _attr_string(31, "+84901234567")
            + _attr_int(40, 1)
            + _attr_int(45, 12345)
            + _attr_ip(8, "10.0.0.1")
        )
        packet = _build_accounting_packet(attrs)
        result = reader.decode_radius(packet)
        assert result["msisdn"] == "+84901234567"
        assert result["acct_status_type"] == "start"
        assert result["framed_ip"] == "10.0.0.1"

    def test_invalid_authenticator_raises(self, reader: PacketReader):
        packet = _build_accounting_packet(_attr_string(31, "+84901234567"))
        bad = bytearray(packet)
        bad[4] ^= 0xFF
        with pytest.raises(PacketDecodeError, match="authenticator"):
            reader.decode_radius(bytes(bad))
