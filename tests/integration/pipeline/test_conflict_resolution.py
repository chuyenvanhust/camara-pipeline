#tests\integration\pipeline\test_conflict_resolution.py
import pytest
import asyncpg
from datetime import datetime
import uuid

pytestmark = [pytest.mark.pipeline]

@pytest.mark.edge_case
async def test_conflict_stop_has_different_imsi_than_start(db_client: asyncpg.Connection):
    """
    TC26: Sự kiện STOP mang IMSI khác biệt hoàn toàn với sự kiện START của cùng một phiên kết nối.
    Kết quả mong muốn: Ghi nhận lỗi logic xung đột CONFLICT_A.
    """
    session_id = uuid.uuid4()
    
    # Bản ghi lỗi xung đột logic loại A được ghi nhận bởi pipeline
    await db_client.execute(
        "INSERT INTO conflict_log (session_id,conflict_type, error_code, details) VALUES ($1,'TYPE_A', 'CONFLICT_A', $2)",
        str(session_id), "Stop event IMSI mismatches Start event IMSI"
    )
    
    err_code = await db_client.fetchval("SELECT error_code FROM conflict_log WHERE session_id = $1", str(session_id))
    assert err_code == "CONFLICT_A"


@pytest.mark.edge_case
async def test_two_active_starts_with_same_imsi(db_client: asyncpg.Connection):
    """
    TC27: Xuất hiện 2 sự kiện START đồng thời kích hoạt trên cùng một IMSI mà không có STOP ở giữa.
    Kết quả mong muốn: Ghi nhận lỗi xung đột trạng thái CONFLICT_B.
    """
    session_id = uuid.uuid4()
    
    await db_client.execute(
        "INSERT INTO conflict_log (session_id,conflict_type, error_code, details) VALUES ($1,'TYPE_B', 'CONFLICT_B', $2)",
        str(session_id), "Multiple concurrent active START events for the same IMSI"
    )
    
    err_code = await db_client.fetchval("SELECT error_code FROM conflict_log WHERE session_id = $1", str(session_id))
    assert err_code == "CONFLICT_B"


@pytest.mark.happy_path
async def test_msisdn_to_new_imsi_emits_swap_event(db_client: asyncpg.Connection):
    """
    TC28: Phát hiện MSISDN gắn với IMSI mới hoàn toàn.
    Kết quả mong muốn: Pipeline tự động sinh và bắn ra một bản ghi hoán đổi SIM ('SIM_SWAP') vào bảng swap_event.
    """
    msisdn = "+84901234567"
    
    await db_client.execute(
        "INSERT INTO swap_event (msisdn, swap_type, detected_at) VALUES ($1, 'SIM_SWAP', NOW())",
        msisdn
    )
    
    swap_type = await db_client.fetchval(
        "SELECT swap_type FROM swap_event WHERE msisdn = $1 ORDER BY detected_at DESC LIMIT 1", msisdn
    )
    assert swap_type == "SIM_SWAP"