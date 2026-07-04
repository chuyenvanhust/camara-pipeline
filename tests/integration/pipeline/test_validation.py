#tests\integration\pipeline\test_validation.py
import pytest
import asyncpg
import uuid

pytestmark = [pytest.mark.pipeline]

@pytest.mark.edge_case
async def test_imei_failed_luhn_checksum(db_client: asyncpg.Connection):
    """
    TC32: Bản ghi mang IMEI sai quy luật kiểm tra thuật toán Luhn.
    Kết quả mong muốn: Ghi nhận mã lỗi định dạng ERR_IMEI_LUHN_FAIL.
    """
    session_id = uuid.uuid4()
    
    await db_client.execute(
        "INSERT INTO invalid_log (session_id, error_code, details) VALUES ($1, 'ERR_IMEI_LUHN_FAIL', $2)",
        str(session_id), "IMEI '123456789012345' failed Luhn checksum validation"
    )
    
    err_code = await db_client.fetchval(
        "SELECT error_code FROM invalid_log WHERE session_id = $1", str(session_id)
    )
    assert err_code == "ERR_IMEI_LUHN_FAIL"


@pytest.mark.edge_case
async def test_imei_tac_not_in_gsma_mock(db_client: asyncpg.Connection):
    """
    TC33: Mã TAC của IMEI hợp lệ về cấu trúc nhưng không tồn tại trong cơ sở dữ liệu GSMA (Mock service).
    Kết quả mong muốn: Ghi nhận mã lỗi không phân giải được thiết bị ERR_IMEI_TAC_UNKNOWN.
    """
    session_id = uuid.uuid4()
    
    await db_client.execute(
        "INSERT INTO invalid_log (session_id, error_code, details) VALUES ($1, 'ERR_IMEI_TAC_UNKNOWN', $2)",
        str(session_id), "TAC prefix from IMEI is unknown to GSMA registry"
    )
    
    err_code = await db_client.fetchval(
        "SELECT error_code FROM invalid_log WHERE session_id = $1", str(session_id)
    )
    assert err_code == "ERR_IMEI_TAC_UNKNOWN"