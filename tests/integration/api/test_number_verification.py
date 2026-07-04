#tests\integration\api\test_number_verification.py
import pytest
from datetime import datetime, timedelta
import uuid

pytestmark = [pytest.mark.number_verification]


async def _insert_session(db, msisdn: str, status: str, offset_seconds: int = 0):
    """Helper: inject record vào radius_sessions — đúng bảng mà router query."""
    await db.execute(
        """INSERT INTO radius_sessions
           (acct_session_id, acct_status_type, msisdn, imsi, imei,
            event_timestamp, ingest_timestamp)
           VALUES ($1, $2, $3, '452010000000001', '860934042394121',
                   NOW() - $4 * INTERVAL '1 second',
                   NOW() - $4 * INTERVAL '1 second')""",
        str(uuid.uuid4()), status, msisdn, offset_seconds
    )


@pytest.mark.happy_path
async def test_tc17_session_active_within_24h(api_client, db_client):
    """TC17: Session Start trong 24h, chưa có Stop → verified=True."""
    msisdn = "+84921234560"
    await _insert_session(db_client, msisdn, "Start", offset_seconds=3600)

    response = await api_client.post(
        "/number-verification/v0/verify",
        json={"phoneNumber": msisdn},
        headers={"X-API-Key": "test_key"}
    )
    assert response.status_code == 200
    assert response.json()["devicePhoneNumberVerified"] is True


@pytest.mark.happy_path
async def test_tc18_session_already_stopped(api_client, db_client):
    """TC18: Có Start và Stop cùng session → verified=False."""
    msisdn = "+84921234561"
    session_id = str(uuid.uuid4())
    
    # Định nghĩa thời gian cụ thể bằng Python để đảm bảo không trùng lặp
    base_time = datetime.utcnow()
    ts_start = base_time - timedelta(hours=2)
    ts_stop = base_time - timedelta(hours=1)
    
    # Chèn Start
    await db_client.execute(
        """INSERT INTO radius_sessions 
        (acct_session_id, acct_status_type, msisdn, imsi, imei, event_timestamp, ingest_timestamp, late_arrival) 
        VALUES ($1, 'Start', $2, '452010000000002', '860934042394121', $3, $3, FALSE)""",
        session_id, msisdn, ts_start
    )
    
    # Chèn Stop
    await db_client.execute(
        """INSERT INTO radius_sessions 
        (acct_session_id, acct_status_type, msisdn, imsi, imei, event_timestamp, ingest_timestamp, late_arrival) 
        VALUES ($1, 'Stop', $2, '452010000000002', '860934042394121', $3, $3, FALSE)""",
        session_id, msisdn, ts_stop
    )

    response = await api_client.post(
        "/number-verification/v0/verify",
        json={"phoneNumber": msisdn},
        headers={"X-API-Key": "test_key"}
    )
    assert response.status_code == 200
    assert response.json()["devicePhoneNumberVerified"] is False

@pytest.mark.happy_path
async def test_tc19_msisdn_not_exists_returns_false(api_client):
    """TC19: MSISDN không có trong DB → verified=False, không phải 404."""
    response = await api_client.post(
        "/number-verification/v0/verify",
        json={"phoneNumber": "+84929999999"},
        headers={"X-API-Key": "test_key"}
    )
    assert response.status_code == 200
    assert response.json()["devicePhoneNumberVerified"] is False


@pytest.mark.edge_case
async def test_tc20_session_started_less_than_1_minute(api_client, db_client):
    """TC20: Start xảy ra < 1 phút → không có tolerance, vẫn verified=True."""
    msisdn = "+84921234563"
    await _insert_session(db_client, msisdn, "Start", offset_seconds=30)

    response = await api_client.post(
        "/number-verification/v0/verify",
        json={"phoneNumber": msisdn},
        headers={"X-API-Key": "test_key"}
    )
    assert response.status_code == 200
    assert response.json()["devicePhoneNumberVerified"] is True


@pytest.mark.edge_case
async def test_tc21_overlapping_multiple_active_sessions(api_client, db_client):
    """TC21: Nhiều session Start chưa Stop → ít nhất 1 active → verified=True."""
    msisdn = "+84921234564"
    await _insert_session(db_client, msisdn, "Start", offset_seconds=3600)
    await _insert_session(db_client, msisdn, "Start", offset_seconds=60)

    response = await api_client.post(
        "/number-verification/v0/verify",
        json={"phoneNumber": msisdn},
        headers={"X-API-Key": "test_key"}
    )
    assert response.status_code == 200
    assert response.json()["devicePhoneNumberVerified"] is True


@pytest.mark.edge_case
async def test_tc22_msisdn_format_invalid(api_client):
    """TC22: phoneNumber không đúng E.164 → 422 (FastAPI Pydantic validation)."""
    response = await api_client.post(
        "/number-verification/v0/verify",
        json={"phoneNumber": "invalid_number_123"},
        headers={"X-API-Key": "test_key"}
    )
    assert response.status_code == 422
    assert response.json()["error"] == "INVALID_ARGUMENT"