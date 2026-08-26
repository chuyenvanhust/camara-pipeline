"""Unit tests for pipeline event parsing and validation."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from pipeline.modules.shared.events import (
    InvalidMessageError,
    canonical_msisdn,
    event_id,
    normalize_status,
    parse_event_time,
    required_text,
)


class TestEventId:
    def test_event_id_from_record(self):
        record = SimpleNamespace(topic="radius.accounting.raw", partition=3, offset=42)
        assert event_id(record) == "radius.accounting.raw:3:42"


class TestParseEventTime:
    def test_iso_string(self):
        result = parse_event_time({"event_timestamp": "2024-06-01T12:00:00+00:00"})
        assert result == datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc)

    def test_unix_timestamp_int(self):
        ts = 1717243200
        result = parse_event_time({"event_timestamp": ts})
        assert result.tzinfo == timezone.utc

    def test_unix_timestamp_string(self):
        result = parse_event_time({"timestamp": "1717243200"})
        assert result.tzinfo == timezone.utc

    def test_missing_timestamp_raises(self):
        with pytest.raises(InvalidMessageError, match="missing event_timestamp"):
            parse_event_time({})

    def test_invalid_timestamp_raises(self):
        with pytest.raises(InvalidMessageError, match="invalid event_timestamp"):
            parse_event_time({"event_timestamp": "not-a-date"})


class TestNormalizeStatus:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            (1, "start"),
            (2, "stop"),
            (8, "accounting-off"),
            ("Start", "start"),
            ("accounting_off", "accounting-off"),
        ],
    )
    def test_known_status(self, raw, expected):
        assert normalize_status(raw) == expected

    def test_unknown_status_raises(self):
        with pytest.raises(InvalidMessageError):
            normalize_status("unknown-status")


class TestCanonicalMsisdn:
    def test_e164_format(self):
        assert canonical_msisdn({"msisdn": "+84901234567"}) == "+84901234567"

    def test_00_prefix_normalized(self):
        assert canonical_msisdn({"msisdn": "0084901234567"}) == "+84901234567"

    def test_missing_raises(self):
        with pytest.raises(InvalidMessageError):
            canonical_msisdn({})


class TestRequiredText:
    def test_first_present_key(self):
        assert required_text({"imei": " 123 ", "imsi": "456"}, "imei", "imsi") == "123"

    def test_all_missing_raises(self):
        with pytest.raises(InvalidMessageError, match="missing required field"):
            required_text({}, "imei", "imsi")
