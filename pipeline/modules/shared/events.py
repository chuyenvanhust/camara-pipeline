from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping


class InvalidMessageError(ValueError):
    """A payload is readable but does not satisfy the pipeline contract."""


def event_id(record: Any) -> str:
    return f"{record.topic}:{record.partition}:{record.offset}"


def required_text(message: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = message.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    raise InvalidMessageError(f"missing required field: {'/'.join(keys)}")


def canonical_msisdn(message: Mapping[str, Any]) -> str:
    value = required_text(
        message, "msisdn", "Calling_Station_Id", "Calling-StationId"
    )
    compact = value.replace(" ", "")
    if compact.startswith("00"):
        compact = "+" + compact[2:]
    if not compact.startswith("+") or not compact[1:].isdigit():
        raise InvalidMessageError("msisdn must use E.164 format")
    if not 8 <= len(compact[1:]) <= 15:
        raise InvalidMessageError("msisdn length is outside E.164 bounds")
    return compact


def parse_event_time(message: Mapping[str, Any]) -> datetime:
    value = message.get("event_timestamp") or message.get("timestamp")
    if value is None or value == "":
        raise InvalidMessageError("missing event_timestamp")
    try:
        if isinstance(value, (int, float)) or str(value).isdigit():
            parsed = datetime.fromtimestamp(float(value), tz=timezone.utc)
        else:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (ValueError, TypeError, OverflowError) as exc:
        raise InvalidMessageError("invalid event_timestamp") from exc
    if parsed.tzinfo is None:
        raise InvalidMessageError("event_timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def normalize_status(value: Any) -> str:
    numeric = {1: "start", 2: "stop", 3: "interim-update", 7: "accounting-on", 8: "accounting-off"}
    if isinstance(value, int) or (isinstance(value, str) and value.strip().isdigit()):
        status = numeric.get(int(value))
        if status:
            return status
    status = str(value or "").strip().lower().replace("_", "-")
    if status in {"start", "stop", "interim-update", "accounting-on", "accounting-off"}:
        return status
    raise InvalidMessageError(f"unsupported acct_status_type: {value!r}")
