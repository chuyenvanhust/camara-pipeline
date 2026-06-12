import pytest
from mock_services.itu_e164.seed import load_itu_data_to_memory

def test_router_validate_valid_e164(client, setup_itu_test_environment):
    cc_path, op_path = setup_itu_test_environment
    load_itu_data_to_memory(cc_path=cc_path, op_path=op_path)
    
    response = client.post("/validate", json={"phone_number": "+84912345678"})
    assert response.status_code == 200
    assert response.json()["is_valid"] is True
    assert response.json()["country_name"] == "Vietnam"

def test_router_validate_invalid_format(client):
    response = client.post("/validate", json={"phone_number": "0912345678"})
    assert response.status_code == 200
    assert response.json()["is_valid"] is False
    assert response.json()["error_code"] == "INVALID_FORMAT"

def test_router_validate_unknown_country(client, setup_itu_test_environment):
    cc_path, op_path = setup_itu_test_environment
    load_itu_data_to_memory(cc_path=cc_path, op_path=op_path)
    
    response = client.post("/validate", json={"phone_number": "+99912345678"})
    assert response.status_code == 200
    assert response.json()["is_valid"] is False
    assert response.json()["error_code"] == "UNKNOWN_COUNTRY"

def test_router_validate_batch(client, setup_itu_test_environment):
    cc_path, op_path = setup_itu_test_environment
    load_itu_data_to_memory(cc_path=cc_path, op_path=op_path)
    
    # +84912345678 (Hợp lệ), 0912345678 (Sai format), +12025550143 (Hợp lệ)
    payload = {"phone_numbers": ["+84912345678", "0912345678", "+12025550143"]}
    response = client.post("/validate/batch", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 3
    assert data["valid_count"] == 2