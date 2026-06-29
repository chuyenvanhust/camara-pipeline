import pytest
from datetime import datetime, timedelta

pytestmark = [pytest.mark.sim_swap]

async def _set_sim_swap(db, msisdn: str, age_days: float = None, age_minutes: float = None):
    if age_days is not None:
        detected_at = datetime.utcnow() - timedelta(days=age_days)
    elif age_minutes is not None:
        detected_at = datetime.utcnow() - timedelta(minutes=age_minutes)
    else:
        detected_at = datetime.utcnow()
        
    await db.execute(
        "INSERT INTO swap_event (msisdn, swap_type, detected_at) VALUES ($1, 'SIM_SWAP', $2)",
        msisdn, detected_at
    )

@pytest.mark.happy_path
async def test_tc01_swap_1_day_ago(api_client, db_client):
    msisdn = "+84901234561"
    await _set_sim_swap(db_client, msisdn, age_days=1)
    
    response = await api_client.post("/sim-swap/v0/check", json={"phoneNumber": msisdn, "maxAge": 30}, headers={"X-API-Key": "test_key"})
    assert response.status_code == 200
    assert response.json()["swapped"] is True

@pytest.mark.happy_path
async def test_tc02_swap_7_days_ago(api_client, db_client):
    msisdn = "+84901234562"
    await _set_sim_swap(db_client, msisdn, age_days=7)
    
    response = await api_client.post("/sim-swap/v0/check", json={"phoneNumber": msisdn, "maxAge": 30}, headers={"X-API-Key": "test_key"})
    assert response.status_code == 200
    assert response.json()["swapped"] is True

@pytest.mark.happy_path
async def test_tc03_swap_31_days_ago(api_client, db_client):
    msisdn = "+84901234563"
    await _set_sim_swap(db_client, msisdn, age_days=31)
    
    response = await api_client.post("/sim-swap/v0/check", json={"phoneNumber": msisdn, "maxAge": 30}, headers={"X-API-Key": "test_key"})
    assert response.status_code == 200
    assert response.json()["swapped"] is False

@pytest.mark.happy_path
async def test_tc04_never_swapped(api_client):
    msisdn = "+84900000004"
    response = await api_client.post("/sim-swap/v0/retrieve-date", json={"phoneNumber": msisdn, "maxAge": 30}, headers={"X-API-Key": "test_key"})
    assert response.status_code == 200
    assert response.json()["latestSimChange"] is None

@pytest.mark.edge_case
async def test_tc05_swap_less_than_1_minute_ago(api_client, db_client):
    msisdn = "+84901234565"
    await _set_sim_swap(db_client, msisdn, age_minutes=0.5)
    
    response = await api_client.post("/sim-swap/v0/check", json={"phoneNumber": msisdn, "maxAge": 30}, headers={"X-API-Key": "test_key"})
    assert response.status_code == 200
    assert response.json()["swapped"] is True

@pytest.mark.edge_case
async def test_tc06_swap_exact_boundary(api_client, db_client):
    msisdn = "+84901234566"
    db_now = await db_client.fetchval("SELECT NOW()")
    detected_at = db_now - timedelta(days=30) + timedelta(minutes=1)  # giữ nguyên buffer gốc: 1 phút
    await db_client.execute(
        "INSERT INTO swap_event (msisdn, swap_type, detected_at) VALUES ($1, 'SIM_SWAP', $2)",
        msisdn, detected_at
    )

    response = await api_client.post(
        "/sim-swap/v0/check",
        json={"phoneNumber": msisdn, "maxAge": 30},
        headers={"X-API-Key": "test_key"}
    )
    assert response.status_code == 200
    assert response.json()["swapped"] is True

@pytest.mark.edge_case
async def test_tc07_swap_just_outside_boundary(api_client, db_client):
    msisdn = "+84901234567"
    detected_at = datetime.utcnow() - timedelta(days=30) - timedelta(seconds=5)
    await db_client.execute("INSERT INTO swap_event (msisdn, swap_type, detected_at) VALUES ($1, 'SIM_SWAP', $2)", msisdn, detected_at)
    
    response = await api_client.post("/sim-swap/v0/check", json={"phoneNumber": msisdn, "maxAge": 30}, headers={"X-API-Key": "test_key"})
    assert response.status_code == 200
    assert response.json()["swapped"] is False

@pytest.mark.edge_case
async def test_tc08_msisdn_not_found(api_client):
    """TC08: MSISDN không tồn tại → swapped=False, HTTP 200.
    Không phải 404 — theo CAMARA spec, không có swap record ≠ resource error."""
    response = await api_client.post(
        "/sim-swap/v0/check",
        json={"phoneNumber": "+84999999999", "maxAge": 30},
        headers={"X-API-Key": "test_key"}
    )
    assert response.status_code == 200
    assert response.json()["swapped"] is False

@pytest.mark.edge_case
async def test_tc09_max_age_zero(api_client, db_client):
    msisdn = "+84901234569"
    await _set_sim_swap(db_client, msisdn, age_days=1)
    
    response = await api_client.post("/sim-swap/v0/check", json={"phoneNumber": msisdn, "maxAge": 0}, headers={"X-API-Key": "test_key"})
    assert response.status_code == 200
    assert response.json()["swapped"] is False