import pytest
from mock_services.hlr_hss.seed import load_subscribers_to_memory

def test_router_get_by_imsi_found_and_404(client, setup_hlr_test_environment):
    load_subscribers_to_memory(file_path=setup_hlr_test_environment)
    
    # 200 OK Case
    res = client.get("/subscribers/by-imsi/452010000000000")
    assert res.status_code == 200
    assert res.json()["operator"] == "Viettel"
    
    # 404 Case
    res_404 = client.get("/subscribers/by-imsi/000000000000000")
    assert res_404.status_code == 404

def test_router_get_by_msisdn_found(client, setup_hlr_test_environment):
    load_subscribers_to_memory(file_path=setup_hlr_test_environment)
    res = client.get("/subscribers/by-msisdn/%2B84970000000")
    assert res.status_code == 200
    assert res.json()["imsi"] == "452010000000000"

def test_router_imsi_history_sim_swap(client, setup_hlr_test_environment):
    load_subscribers_to_memory(file_path=setup_hlr_test_environment)
    
    # Quét qua RAM để tìm số có lịch sử SIM Swap (độ dài msisdn array > 1)
    from mock_services.hlr_hss.seed import SUBSCRIBERS_BY_MSISDN
    swap_msisdn = None
    for msisdn, records in SUBSCRIBERS_BY_MSISDN.items():
        if len(records) > 1:
            swap_msisdn = msisdn
            break
            
    assert swap_msisdn is not None, "Seed 42 with count 50 must yield at least 1 swap record"
    
    response = client.get(f"/subscribers/{swap_msisdn}/imsi-history")
    assert response.status_code == 200
    data = response.json()
    assert data["total"] >= 2
    assert data["sim_swap_count"] >= 1
    assert data["history"][0]["is_current"] is True

def test_router_batch_lookup_mixed(client, setup_hlr_test_environment):
    load_subscribers_to_memory(file_path=setup_hlr_test_environment)
    
    payload = {
        "lookups": [
            {"type": "imsi", "value": "452010000000000"},
            {"type": "msisdn", "value": "+84970000000"},
            {"type": "imsi", "value": "000000000000000"} # 404 item
        ]
    }
    response = client.post("/subscribers/batch-lookup", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 3
    assert data["found"] == 2
    assert data["not_found"] == 1