"""Tests for Kafka payload deserialization and DLQ-safe handling."""

from __future__ import annotations

import base64
import json

import pytest

from pipeline.modules.shared.base_consumer import _deserialize


class TestDeserialize:
    def test_valid_json_object(self):
        payload = {"msisdn": "+84901234567", "imsi": "452010000000001"}
        result = _deserialize(json.dumps(payload).encode("utf-8"))
        assert result == payload

    def test_malformed_json_returns_error_marker(self):
        result = _deserialize(b"{not json")
        assert "_decode_error" in result
        assert "_raw_base64" in result
        assert base64.b64decode(result["_raw_base64"]) == b"{not json"

    def test_non_object_json_raises_marker(self):
        result = _deserialize(b"[1, 2, 3]")
        assert "_decode_error" in result

    def test_invalid_utf8_returns_error_marker(self):
        result = _deserialize(b"\xff\xfe")
        assert "_decode_error" in result
