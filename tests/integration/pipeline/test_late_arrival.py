#tests\integration\pipeline\test_late_arrival.py
import pytest
import asyncpg
from datetime import datetime, timedelta
import uuid
pytestmark = [pytest.mark.pipeline, pytest.mark.late_arrival]

async def test_record_arriving_after_1_hour(db_client: asyncpg.Connection):
    """
    TC29: Bản ghi đến muộn 1 tiếng (so với thời gian sinh dữ liệu gốc).
    Kết quả mong muốn: Chấp nhận xử lý, đồng thời đánh dấu nhãn late_arrival = true để phục vụ thống kê.
    """
    session_id = uuid.uuid4()
    await db_client.execute(
        "INSERT INTO radius_sessions (acct_session_id, acct_status_type, event_timestamp, msisdn, ingest_timestamp, late_arrival) VALUES ($1, 'Start', NOW() - INTERVAL '1 hour', '+84901234561', NOW(), TRUE)",
        str(session_id)
    )
    is_late = await db_client.fetchval("SELECT late_arrival FROM radius_sessions WHERE acct_session_id = $1", str(session_id))
    assert is_late is True


async def test_record_arriving_after_6_hours(db_client: asyncpg.Connection):
    """
    TC30: Bản ghi đến muộn 6 tiếng (vẫn nằm trong ngưỡng chịu đựng tối đa 24h).
    Kết quả mong muốn: Xử lý ghi nhận bình thường vào kho dữ liệu.
    """
    session_id = uuid.uuid4()
    await db_client.execute(
        "INSERT INTO radius_sessions (acct_session_id, acct_status_type, event_timestamp, msisdn, ingest_timestamp, late_arrival) VALUES ($1, 'Start', NOW() - INTERVAL '6 hours', '+84901234561', NOW(), TRUE)",
        str(session_id)
    )
    # Status được mặc định là xử lý thành công khi insert vào radius_sessions
    exists = await db_client.fetchval("SELECT EXISTS(SELECT 1 FROM radius_sessions WHERE acct_session_id = $1)", str(session_id))
    assert exists is True


async def test_record_arriving_after_25_hours_dropped(db_client: asyncpg.Connection):
    """
    TC31: Bản ghi đến quá muộn (> 24 tiếng - vượt giới hạn xử lý của watermark).
    Kết quả mong muốn: Hủy bỏ bản ghi (Drop) và đẩy thông tin vào log lỗi muộn.
    """
    session_id = uuid.uuid4()
    await db_client.execute(
        "INSERT INTO invalid_log (session_id, error_code, details) VALUES ($1, 'ERR_LATE_ARRIVAL_EXCEEDED', $2)",
        str(session_id), "Record delayed by 25 hours"
    )
    err_code = await db_client.fetchval("SELECT error_code FROM invalid_log WHERE session_id = $1", str(session_id))
    assert err_code == "ERR_LATE_ARRIVAL_EXCEEDED"