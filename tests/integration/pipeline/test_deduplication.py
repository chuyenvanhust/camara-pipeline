import pytest
import asyncpg
from datetime import datetime, timedelta
import uuid

pytestmark = [pytest.mark.pipeline]


@pytest.mark.happy_path
async def test_exact_duplicate_one_kept_one_dropped(db_client: asyncpg.Connection):
    """
    TC23: Trùng lặp tuyệt đối (Exact duplicate)
    Kết quả mong muốn: Hệ thống chỉ giữ lại 1 record chính thức,
    record còn lại bị trigger long-term dedup chặn và ghi vào duplicate_log.

    
    """
    session_id = uuid.uuid4()
    ts = datetime.utcnow()

    await db_client.execute(
        "INSERT INTO radius_sessions (acct_session_id, acct_status_type, event_timestamp, msisdn, ingest_timestamp) VALUES ($1, $2, $3, $4, $5)",
        str(session_id), "Start", ts, "+84901234561", ts
    )

   
    await db_client.execute(
        "INSERT INTO radius_sessions (acct_session_id, acct_status_type, event_timestamp, msisdn, ingest_timestamp) VALUES ($1, $2, $3, $4, $5)",
        str(session_id), "Start", ts, "+84901234561", ts
    )

    processed_count = await db_client.fetchval(
        "SELECT COUNT(*) FROM radius_sessions WHERE acct_session_id = $1", str(session_id)
    )
    duplicate_count = await db_client.fetchval(
        "SELECT COUNT(*) FROM duplicate_log WHERE session_id = $1", str(session_id)
    )

    assert processed_count == 1
    assert duplicate_count == 1


@pytest.mark.happy_path
async def test_same_session_different_timestamp_both_kept(db_client: asyncpg.Connection):
    """
    TC24: Cùng session_id nhưng lệch nhau 1ms
    Kết quả mong muốn: Cả hai bản ghi đều hợp lệ và được giữ lại.
    """
    session_id = uuid.uuid4()
    ts1 = datetime.utcnow()
    ts2 = ts1 + timedelta(seconds=1)

  
    await db_client.execute(
        "INSERT INTO radius_sessions (acct_session_id, acct_status_type, event_timestamp, msisdn, ingest_timestamp) VALUES ($1, $2, $3, $4, $5)",
        str(session_id), "Start", ts1, "+84901234561", ts1
    )

    await db_client.execute(
        "INSERT INTO radius_sessions (acct_session_id, acct_status_type, event_timestamp, msisdn, ingest_timestamp) VALUES ($1, $2, $3, $4, $5)",
        str(session_id), "Interim-Update", ts2, "+84901234561", ts2
    )

    count = await db_client.fetchval(
        "SELECT COUNT(*) FROM radius_sessions WHERE acct_session_id = $1", str(session_id)
    )
    assert count == 2


@pytest.mark.late_arrival
async def test_duplicate_arriving_after_late_window_still_detected(db_client: asyncpg.Connection):
    """
    TC25: Bản ghi trùng lặp xuất hiện muộn ngoài cửa sổ cấu hình (late arrival window)
    Kết quả mong muốn: Vẫn phải detect được trùng lặp dựa trên lịch sử lưu trữ dài hạn.
    """
    session_id = uuid.uuid4()
    old_ts = datetime.utcnow() - timedelta(hours=26)  # Vượt quá window 24h thông thường
    new_ts = datetime.utcnow()

    # Bản ghi cũ đã nằm trong DB từ trước
    await db_client.execute(
        """INSERT INTO radius_sessions 
        (acct_session_id, acct_status_type, event_timestamp, msisdn,ingest_timestamp) 
        VALUES ($1, $2, $3, $4 ,$5)""",
        str(session_id), 
        "Start", 
        old_ts, 
        "+84901234561",
        old_ts
    )

    # Bản ghi mới đến muộn nhưng nội dung trùng lặp hoàn toàn
    await db_client.execute(
        "INSERT INTO radius_sessions (acct_session_id, acct_status_type, event_timestamp, msisdn, ingest_timestamp) VALUES ($1, $2, $3, $4, $5)",
        str(session_id), "Start", new_ts, "+84901234561", new_ts
    )

    duplicate_exists = await db_client.fetchval(
        "SELECT EXISTS(SELECT 1 FROM duplicate_log WHERE session_id = $1)", str(session_id)
    )
    assert duplicate_exists is True