import pytest
from mock_services.gsma_tac.seed import load_tac_csv_to_memory

def test_router_get_tac_success(client, setup_test_csv_data):
    # Đảm bảo RAM load dữ liệu test trước khi gọi Client
    load_tac_csv_to_memory(file_path=setup_test_csv_data)
    response = client.get("/tac/352099")
    assert response.status_code == 200
    assert response.json()["manufacturer"] == "Samsung"

def test_router_get_tac_not_found(client, setup_test_csv_data):
    load_tac_csv_to_memory(file_path=setup_test_csv_data)
    response = client.get("/tac/999999")
    assert response.status_code == 404
    assert response.json()["error"] == "NOT_FOUND"

def test_router_get_tac_invalid_input(client):
    response = client.get("/tac/12345") 
    assert response.status_code == 422
    assert response.json()["error"] == "INVALID_INPUT"

def test_router_post_batch(client, setup_test_csv_data):
    load_tac_csv_to_memory(file_path=setup_test_csv_data)
    payload = {"tac_codes": ["352099", "490154", "999999"]}
    response = client.post("/tac/batch", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["results"]["352099"]["found"] is True
    assert data["results"]["999999"]["found"] is False
    assert data["found"] == 2

def test_router_list_tac_with_filters_and_pagination(client, setup_test_csv_data):
    load_tac_csv_to_memory(file_path=setup_test_csv_data)
    response = client.get("/tac?page=1&page_size=1&device_type=smartphone")
    assert response.status_code == 200
    data = response.json()
    assert len(data["items"]) == 1
    assert data["items"][0]["device_type"] == "smartphone"

def test_router_fault_injection(client, setup_test_csv_data):
    load_tac_csv_to_memory(file_path=setup_test_csv_data)
    res_500 = client.get("/tac/352099", headers={"x-mock-fault": "500"})
    assert res_500.status_code == 500
    
    res_timeout = client.get("/tac/352099", headers={"x-mock-fault": "timeout"})
    assert res_timeout.status_code == 504