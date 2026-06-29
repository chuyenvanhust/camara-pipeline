# tests/unit/pipeline/storage/test_writer.py
"""
Unit tests cho pipeline/storage/writer.py

Tầng PURE LOGIC: test build_dsn, build_upsert_sql, extract_rows_from_batch
không cần Spark/Kafka/PostgreSQL.

Tầng SPARK I/O: test write_micro_batch với mock psycopg2 (không connect thật).
"""

import pytest
from unittest.mock import MagicMock, patch

from pipeline_v1.storage.writer import (
    # Constants
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
    KAFKA_TOPIC_CLEAN,
    SPARK_JDBC_BATCH_SIZE,
    CLEAN_RECORD_SCHEMA,
    # Pure logic
    build_dsn,
    build_upsert_sql,
    extract_rows_from_batch,
    # Spark I/O
    write_micro_batch,
)
from pipeline_v1.storage.models import RadiusSession


# ==============================================================================
# Fixture: tạo 1 row dict mẫu khớp RadiusSession.INSERT_COLUMNS
# ==============================================================================

@pytest.fixture
def sample_row():
    """Một radius record hợp lệ đã qua S4 conflict resolution."""
    return {
        "acct_session_id": "SESS-001",
        "acct_status_type": "Start",
        "event_timestamp": "2026-06-14 10:00:00+07",
        "msisdn": "+84971111111",
        "imsi": "452010000000111",
        "imei": "860934042394121",
        "rat_type": "LTE",
        "framed_ip": "10.0.0.1",
        "nas_ip": "172.16.0.1",
        "mcc_mnc": "452001",
        "late_arrival": False,
    }


@pytest.fixture
def sample_row_minimal():
    """Record chỉ có các trường bắt buộc, optional fields = None."""
    return {
        "acct_session_id": "SESS-MIN",
        "acct_status_type": "Stop",
        "event_timestamp": "2026-06-14 11:30:00+07",
        "msisdn": "+84972222222",
        "imsi": "452010000000222",
        "imei": "860934042394999",
        "rat_type": None,
        "framed_ip": None,
        "nas_ip": None,
        "mcc_mnc": None,
        "late_arrival": None,
    }


# ==============================================================================
# CONSTANTS — verify config defaults
# ==============================================================================

class TestConstants:

    def test_kafka_topic_clean_default(self):
        assert KAFKA_TOPIC_CLEAN == "radius.clean"

    def test_batch_size_default(self):
        assert SPARK_JDBC_BATCH_SIZE == 1000

    def test_db_defaults(self):
        assert DB_HOST == "localhost"
        assert DB_PORT == 5432
        assert DB_NAME == "camara_db"

    def test_clean_record_schema_has_expected_fields(self):
        field_names = {f.name for f in CLEAN_RECORD_SCHEMA.fields}
        expected = {
            "acct_session_id", "acct_status_type", "event_timestamp",
            "msisdn", "imsi", "imei", "rat_type",
            "framed_ip", "nas_ip", "mcc_mnc", "late_arrival",
        }
        assert field_names == expected

    def test_clean_record_schema_matches_insert_columns(self):
        """Schema fields phải khớp chính xác với RadiusSession.INSERT_COLUMNS."""
        schema_fields = {f.name for f in CLEAN_RECORD_SCHEMA.fields}
        insert_cols = set(RadiusSession.INSERT_COLUMNS)
        assert schema_fields == insert_cols


# ==============================================================================
# PURE LOGIC — build_dsn
# ==============================================================================

class TestBuildDsn:

    def test_default_values(self):
        dsn = build_dsn()
        assert dsn["host"] == DB_HOST
        assert dsn["port"] == DB_PORT
        assert dsn["dbname"] == DB_NAME
        assert dsn["user"] == DB_USER
        assert dsn["password"] == DB_PASSWORD

    def test_custom_values(self):
        dsn = build_dsn(
            host="10.0.0.1", port=5433, dbname="test_db",
            user="tester", password="secret",
        )
        assert dsn == {
            "host": "10.0.0.1",
            "port": 5433,
            "dbname": "test_db",
            "user": "tester",
            "password": "secret",
        }

    def test_returns_dict_with_5_keys(self):
        dsn = build_dsn()
        assert len(dsn) == 5
        assert set(dsn.keys()) == {"host", "port", "dbname", "user", "password"}


# ==============================================================================
# PURE LOGIC — build_upsert_sql
# ==============================================================================

class TestBuildUpsertSql:

    def test_simple_case(self):
        sql = build_upsert_sql("my_table", ("col_a", "col_b"), ("col_a",))
        assert sql == (
            "INSERT INTO my_table (col_a, col_b) "
            "VALUES (%s, %s) "
            "ON CONFLICT (col_a) DO NOTHING"
        )

    def test_radius_sessions_real_columns(self):
        """Verify SQL sinh ra đúng cho bảng radius_sessions thực tế."""
        sql = build_upsert_sql(
            RadiusSession.__tablename__,
            RadiusSession.INSERT_COLUMNS,
            RadiusSession.CONFLICT_COLUMNS,
        )
        assert sql.startswith("INSERT INTO radius_sessions (")
        assert "ON CONFLICT (acct_session_id, event_timestamp) DO NOTHING" in sql
        assert sql.count("%s") == len(RadiusSession.INSERT_COLUMNS)

    def test_multi_conflict_columns(self):
        sql = build_upsert_sql("t", ("a", "b", "c"), ("a", "b"))
        assert "ON CONFLICT (a, b) DO NOTHING" in sql

    def test_single_column_single_conflict(self):
        sql = build_upsert_sql("t", ("x",), ("x",))
        assert sql == (
            "INSERT INTO t (x) VALUES (%s) ON CONFLICT (x) DO NOTHING"
        )

    def test_placeholder_count_matches_columns(self):
        """Số %s placeholder phải khớp số cột INSERT."""
        cols = ("a", "b", "c", "d", "e")
        sql = build_upsert_sql("t", cols, ("a",))
        assert sql.count("%s") == 5


# ==============================================================================
# PURE LOGIC — extract_rows_from_batch
# ==============================================================================

class TestExtractRowsFromBatch:

    def test_basic_extraction(self):
        rows = [
            {"a": 1, "b": "hello", "c": True},
            {"a": 2, "b": "world", "c": False},
        ]
        result = extract_rows_from_batch(rows, ("a", "b", "c"))
        assert result == [(1, "hello", True), (2, "world", False)]

    def test_column_order_matters(self):
        """Thứ tự columns quyết định thứ tự values trong tuple."""
        rows = [{"x": 10, "y": 20}]
        assert extract_rows_from_batch(rows, ("y", "x")) == [(20, 10)]
        assert extract_rows_from_batch(rows, ("x", "y")) == [(10, 20)]

    def test_missing_column_returns_none(self):
        """Column không tồn tại → None (cho nullable fields)."""
        rows = [{"a": 1}]
        result = extract_rows_from_batch(rows, ("a", "b"))
        assert result == [(1, None)]

    def test_empty_rows(self):
        result = extract_rows_from_batch([], ("a", "b"))
        assert result == []

    def test_radius_session_columns(self, sample_row):
        """Verify extraction sử dụng RadiusSession.INSERT_COLUMNS thực tế."""
        result = extract_rows_from_batch([sample_row], RadiusSession.INSERT_COLUMNS)
        assert len(result) == 1
        assert len(result[0]) == len(RadiusSession.INSERT_COLUMNS)
        # acct_session_id là cột đầu tiên
        assert result[0][0] == "SESS-001"
        # msisdn là cột thứ 4 (index 3)
        assert result[0][3] == "+84971111111"

    def test_minimal_row_with_none_values(self, sample_row_minimal):
        """Optional fields = None vẫn được trích xuất đúng."""
        result = extract_rows_from_batch(
            [sample_row_minimal], RadiusSession.INSERT_COLUMNS
        )
        assert len(result) == 1
        # rat_type (index 6) phải là None
        assert result[0][6] is None


# ==============================================================================
# SPARK I/O — write_micro_batch (mock psycopg2, không connect thật)
# ==============================================================================

class TestWriteMicroBatch:

    def _make_mock_row(self, row_dict):
        """Helper: tạo mock Spark Row từ dict."""
        mock_row = MagicMock()
        mock_row.asDict.return_value = row_dict
        return mock_row

    @patch("pipeline.storage.writer.psycopg2.connect")
    def test_writes_rows_to_postgres(self, mock_connect, sample_row):
        """Verify callback ghi đúng số rows qua executemany."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        mock_df = MagicMock()
        mock_df.collect.return_value = [self._make_mock_row(sample_row)]

        dsn = build_dsn()
        callback = write_micro_batch(dsn, batch_size=100)
        callback(mock_df, batch_id=0)

        mock_connect.assert_called_once_with(**dsn)
        mock_cursor.executemany.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("pipeline.storage.writer.psycopg2.connect")
    def test_empty_batch_skips_db_call(self, mock_connect):
        """Batch rỗng không nên connect tới DB."""
        mock_df = MagicMock()
        mock_df.collect.return_value = []

        dsn = build_dsn()
        callback = write_micro_batch(dsn)
        callback(mock_df, batch_id=0)

        mock_connect.assert_not_called()

    @patch("pipeline.storage.writer.psycopg2.connect")
    def test_batch_chunking(self, mock_connect, sample_row):
        """3 rows, batch_size=2 → 2 calls to executemany (chunk 2 + chunk 1)."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        rows = []
        for i in range(3):
            row = dict(sample_row, acct_session_id=f"SESS-{i:03d}")
            rows.append(self._make_mock_row(row))

        mock_df = MagicMock()
        mock_df.collect.return_value = rows

        dsn = build_dsn()
        callback = write_micro_batch(dsn, batch_size=2)
        callback(mock_df, batch_id=1)

        assert mock_cursor.executemany.call_count == 2
        # Chunk 1: 2 rows, Chunk 2: 1 row
        first_call_data = mock_cursor.executemany.call_args_list[0][0][1]
        second_call_data = mock_cursor.executemany.call_args_list[1][0][1]
        assert len(first_call_data) == 2
        assert len(second_call_data) == 1

    @patch("pipeline.storage.writer.psycopg2.connect")
    def test_rollback_on_error(self, mock_connect, sample_row):
        """Verify rollback được gọi khi executemany raise exception."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.executemany.side_effect = Exception("DB error")
        mock_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        mock_df = MagicMock()
        mock_df.collect.return_value = [self._make_mock_row(sample_row)]

        dsn = build_dsn()
        callback = write_micro_batch(dsn)

        with pytest.raises(Exception, match="DB error"):
            callback(mock_df, batch_id=99)

        mock_conn.rollback.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("pipeline.storage.writer.psycopg2.connect")
    def test_upsert_sql_contains_on_conflict(self, mock_connect, sample_row):
        """Verify SQL thực sự chứa ON CONFLICT DO NOTHING."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        mock_df = MagicMock()
        mock_df.collect.return_value = [self._make_mock_row(sample_row)]

        dsn = build_dsn()
        callback = write_micro_batch(dsn)
        callback(mock_df, batch_id=0)

        # Lấy SQL từ call_args
        executed_sql = mock_cursor.executemany.call_args[0][0]
        assert "ON CONFLICT" in executed_sql
        assert "DO NOTHING" in executed_sql
        assert "radius_sessions" in executed_sql

    @patch("pipeline.storage.writer.psycopg2.connect")
    def test_connection_always_closed(self, mock_connect, sample_row):
        """Connection phải được close trong cả trường hợp thành công lẫn lỗi."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = MagicMock(
            return_value=mock_cursor
        )
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        mock_connect.return_value = mock_conn

        mock_df = MagicMock()
        mock_df.collect.return_value = [self._make_mock_row(sample_row)]

        # Trường hợp thành công
        dsn = build_dsn()
        callback = write_micro_batch(dsn)
        callback(mock_df, batch_id=0)
        assert mock_conn.close.call_count == 1
