# tests/unit/pipeline/storage/test_models.py
"""
Unit tests cho pipeline/storage/models.py

Test mức SQLAlchemy metadata — không cần PostgreSQL chạy thật.
Verify: table names, column names/types, nullable constraints,
INSERT_COLUMNS, CONFLICT_COLUMNS, VALID constants.
"""

import pytest
from sqlalchemy import inspect

from pipeline_v1.storage.models import (
    Base,
    RadiusSession,
    SwapEvent,
    DuplicateLog,
    ConflictLog,
    InvalidLog,
    ALL_MODELS,
    ALL_TABLE_NAMES,
)


# ==============================================================================
# RadiusSession — bảng chính pipeline
# ==============================================================================

class TestRadiusSession:

    def test_table_name(self):
        assert RadiusSession.__tablename__ == "radius_sessions"

    def test_insert_columns_is_tuple(self):
        assert isinstance(RadiusSession.INSERT_COLUMNS, tuple)

    def test_insert_columns_count(self):
        """11 cột INSERT: 6 bắt buộc + 5 optional, loại trừ id và ingest_timestamp."""
        assert len(RadiusSession.INSERT_COLUMNS) == 11

    def test_insert_columns_excludes_auto_generated(self):
        """id (auto-increment) và ingest_timestamp (server_default) không nên INSERT."""
        assert "id" not in RadiusSession.INSERT_COLUMNS
        assert "ingest_timestamp" not in RadiusSession.INSERT_COLUMNS

    def test_conflict_columns(self):
        """ON CONFLICT sử dụng unique constraint (acct_session_id, event_timestamp)."""
        assert RadiusSession.CONFLICT_COLUMNS == (
            "acct_session_id", "event_timestamp"
        )

    def test_required_columns_exist_in_model(self):
        """Các cột nghiệp vụ bắt buộc phải tồn tại trong model."""
        mapper = inspect(RadiusSession)
        col_names = {c.key for c in mapper.columns}
        required = {
            "id", "acct_session_id", "acct_status_type", "event_timestamp",
            "ingest_timestamp", "msisdn", "imsi", "imei",
        }
        assert required.issubset(col_names)

    def test_optional_columns_exist_in_model(self):
        mapper = inspect(RadiusSession)
        col_names = {c.key for c in mapper.columns}
        optional = {"rat_type", "framed_ip", "nas_ip", "mcc_mnc", "late_arrival"}
        assert optional.issubset(col_names)

    def test_non_nullable_columns(self):
        """Các cột chính phải NOT NULL."""
        mapper = inspect(RadiusSession)
        col_map = {c.key: c for c in mapper.columns}
        for col_name in (
            "acct_session_id", "acct_status_type",
            "event_timestamp", "msisdn", "imsi", "imei",
        ):
            assert col_map[col_name].nullable is False, (
                f"{col_name} should be NOT NULL"
            )

    def test_nullable_columns(self):
        """Các cột optional phải nullable."""
        mapper = inspect(RadiusSession)
        col_map = {c.key: c for c in mapper.columns}
        for col_name in ("rat_type", "framed_ip", "nas_ip", "mcc_mnc"):
            assert col_map[col_name].nullable is True, (
                f"{col_name} should be nullable"
            )

    def test_insert_columns_are_valid_model_columns(self):
        """Mọi cột trong INSERT_COLUMNS phải thực sự tồn tại trong model."""
        mapper = inspect(RadiusSession)
        model_cols = {c.key for c in mapper.columns}
        for col in RadiusSession.INSERT_COLUMNS:
            assert col in model_cols, (
                f"INSERT_COLUMNS chứa '{col}' nhưng không có trong model"
            )

    def test_repr(self):
        session = RadiusSession(
            id=1,
            acct_session_id="ABC-123",
            msisdn="+84971111111",
            event_timestamp="2026-06-14",
        )
        r = repr(session)
        assert "RadiusSession" in r
        assert "ABC-123" in r


# ==============================================================================
# SwapEvent — SIM Swap / Device Swap
# ==============================================================================

class TestSwapEvent:

    def test_table_name(self):
        assert SwapEvent.__tablename__ == "swap_event"

    def test_valid_swap_types(self):
        assert "SIM_SWAP" in SwapEvent.VALID_SWAP_TYPES
        assert "DEVICE_SWAP" in SwapEvent.VALID_SWAP_TYPES
        assert len(SwapEvent.VALID_SWAP_TYPES) == 2

    def test_required_columns_not_nullable(self):
        mapper = inspect(SwapEvent)
        col_map = {c.key: c for c in mapper.columns}
        for col_name in ("msisdn", "swap_type", "detected_at", "source"):
            assert col_map[col_name].nullable is False, (
                f"{col_name} should be NOT NULL"
            )

    def test_optional_imsi_imei_nullable(self):
        """old_imsi/new_imsi/old_imei/new_imei có thể null (chỉ 1 loại swap)."""
        mapper = inspect(SwapEvent)
        col_map = {c.key: c for c in mapper.columns}
        for col_name in ("old_imsi", "new_imsi", "old_imei", "new_imei"):
            assert col_map[col_name].nullable is True

    def test_repr(self):
        event = SwapEvent(id=1, msisdn="+84971111111", swap_type="SIM_SWAP")
        assert "SwapEvent" in repr(event)
        assert "SIM_SWAP" in repr(event)


# ==============================================================================
# DuplicateLog — audit bản ghi trùng lặp
# ==============================================================================

class TestDuplicateLog:

    def test_table_name(self):
        assert DuplicateLog.__tablename__ == "duplicate_log"

    def test_has_required_columns(self):
        mapper = inspect(DuplicateLog)
        col_names = {c.key for c in mapper.columns}
        assert {"id", "acct_session_id", "duplicate_count", "logged_at"}.issubset(
            col_names
        )

    def test_acct_session_id_not_nullable(self):
        mapper = inspect(DuplicateLog)
        col_map = {c.key: c for c in mapper.columns}
        assert col_map["acct_session_id"].nullable is False

    def test_repr(self):
        log = DuplicateLog(acct_session_id="SESS-001", duplicate_count=3)
        assert "DuplicateLog" in repr(log)


# ==============================================================================
# ConflictLog — audit bản ghi xung đột A/B/C
# ==============================================================================

class TestConflictLog:

    def test_table_name(self):
        assert ConflictLog.__tablename__ == "conflict_log"

    def test_valid_conflict_types(self):
        assert ConflictLog.VALID_CONFLICT_TYPES == ("A", "B", "C")

    def test_conflict_type_not_nullable(self):
        mapper = inspect(ConflictLog)
        col_map = {c.key: c for c in mapper.columns}
        assert col_map["conflict_type"].nullable is False

    def test_has_required_columns(self):
        mapper = inspect(ConflictLog)
        col_names = {c.key for c in mapper.columns}
        assert {"acct_session_id", "conflict_type", "logged_at"}.issubset(col_names)

    def test_repr(self):
        log = ConflictLog(acct_session_id="SESS-001", conflict_type="A")
        assert "ConflictLog" in repr(log)


# ==============================================================================
# InvalidLog — audit bản ghi không hợp lệ
# ==============================================================================

class TestInvalidLog:

    def test_table_name(self):
        assert InvalidLog.__tablename__ == "invalid_log"

    def test_error_code_not_nullable(self):
        mapper = inspect(InvalidLog)
        col_map = {c.key: c for c in mapper.columns}
        assert col_map["error_code"].nullable is False

    def test_error_message_nullable(self):
        """error_message là Text, có thể null (chỉ cần error_code để phân loại)."""
        mapper = inspect(InvalidLog)
        col_map = {c.key: c for c in mapper.columns}
        assert col_map["error_message"].nullable is True

    def test_session_fields_nullable(self):
        """Record invalid có thể thiếu session/msisdn (R1 missing fields)."""
        mapper = inspect(InvalidLog)
        col_map = {c.key: c for c in mapper.columns}
        for col_name in ("acct_session_id", "msisdn", "imsi"):
            assert col_map[col_name].nullable is True

    def test_repr(self):
        log = InvalidLog(acct_session_id="SESS-ERR", error_code="ERR_R1")
        assert "InvalidLog" in repr(log)


# ==============================================================================
# Module-level exports
# ==============================================================================

class TestModuleExports:

    def test_all_models_count(self):
        assert len(ALL_MODELS) == 5

    def test_all_table_names_content(self):
        expected = (
            "radius_sessions", "swap_event",
            "duplicate_log", "conflict_log", "invalid_log",
        )
        assert ALL_TABLE_NAMES == expected

    def test_all_models_inherit_base(self):
        for model in ALL_MODELS:
            assert issubclass(model, Base)

    def test_all_models_have_id_column(self):
        """Mọi bảng đều có cột id BIGSERIAL làm primary key."""
        for model in ALL_MODELS:
            mapper = inspect(model)
            col_names = {c.key for c in mapper.columns}
            assert "id" in col_names, f"{model.__tablename__} thiếu cột id"
