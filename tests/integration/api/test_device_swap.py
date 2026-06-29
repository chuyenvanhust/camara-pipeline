import pytest
from datetime import datetime, timedelta

pytestmark = [pytest.mark.device_swap]

async def _set_device_swap(db, msisdn: str, age_days: float):
    detected_at = datetime.utcnow() - timedelta(days=age_days)
    await db.execute(
        "INSERT INTO swap_event (msisdn, swap_type, detected_at) VALUES ($1, 'DEVICE_SWAP', $2)",
        msisdn, detected_at
    )

@pytest.mark.happy_path
async def test_tc10_device_swap_1_day_ago(api_client, db_client):
    msisdn = "+84911234560"
    await _set_device_swap(db_client, msisdn, age_days=1)
    
    response = await api_client.post("/device-swap/v0/check", json={"phoneNumber": msisdn, "maxAge": 30}, headers={"X-API-Key": "test_key"})
    assert response.status_code == 200
    assert response.json()["deviceSwapped"] is True

@pytest.mark.happy_path
async def test_tc11_device_not_swapped(api_client):
    msisdn = "+84911234561"
    response = await api_client.post("/device-swap/v0/check", json={"phoneNumber": msisdn, "maxAge": 30}, headers={"X-API-Key": "test_key"})
    assert response.status_code == 200
    assert response.json()["deviceSwapped"] is False

@pytest.mark.happy_path
async def test_tc12_device_swap_35_days_ago(api_client, db_client):
    msisdn = "+84911234562"
    await _set_device_swap(db_client, msisdn, age_days=35)
    
    response = await api_client.post("/device-swap/v0/check", json={"phoneNumber": msisdn, "maxAge": 30}, headers={"X-API-Key": "test_key"})
    assert response.status_code == 200
    assert response.json()["deviceSwapped"] is False

@pytest.mark.edge_case
async def test_tc13_device_swap_less_than_1_minute(api_client, db_client):
    msisdn = "+84911234563"
    detected_at = datetime.utcnow() - timedelta(seconds=30)
    await db_client.execute("INSERT INTO swap_event (msisdn, swap_type, detected_at) VALUES ($1, 'DEVICE_SWAP', $2)", msisdn, detected_at)
    
    response = await api_client.post("/device-swap/v0/check", json={"phoneNumber": msisdn, "maxAge": 30}, headers={"X-API-Key": "test_key"})
    assert response.status_code == 200
    assert response.json()["deviceSwapped"] is True

@pytest.mark.edge_case
async def test_tc14_device_msisdn_not_found(api_client):
    """TC14: MSISDN không tồn tại → deviceSwapped=False, HTTP 200."""
    response = await api_client.post(
        "/device-swap/v0/check",
        json={"phoneNumber": "+84999999998", "maxAge": 30},
        headers={"X-API-Key": "test_key"}
    )
    assert response.status_code == 200
    assert response.json()["deviceSwapped"] is False

@pytest.mark.edge_case
async def test_tc15_device_boundary_exact(api_client, db_client):
    msisdn = "+84911234565"
    db_now = await db_client.fetchval("SELECT NOW()")
    detected_at = db_now - timedelta(days=30) + timedelta(seconds=10)  # giữ nguyên buffer gốc: 10s
    await db_client.execute(
        "INSERT INTO swap_event (msisdn, swap_type, detected_at) VALUES ($1, 'DEVICE_SWAP', $2)",
        msisdn, detected_at
    )

    response = await api_client.post(
        "/device-swap/v0/check",
        json={"phoneNumber": msisdn, "maxAge": 30},
        headers={"X-API-Key": "test_key"}
    )
    assert response.status_code == 200
    assert response.json()["deviceSwapped"] is True

@pytest.mark.edge_case
async def test_tc16_device_phone_format_invalid(api_client):
    response = await api_client.post(
        "/device-swap/v0/check",
        json={"phoneNumber": "090abc123", "maxAge": 30},
        headers={"X-API-Key": "test_key"}
    )
    assert response.status_code == 422
    assert response.json()["error"] == "INVALID_ARGUMENT"