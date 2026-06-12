import pytest
import os
import csv
from datetime import datetime
from fastapi.testclient import TestClient
from mock_services.hlr_hss.app import app
from mock_services.hlr_hss.seed import generate_mock_subscribers_csv

@pytest.fixture(scope="session", autouse=True)
def setup_hlr_test_environment():
    """Tạo DB môi trường Test độc lập và chủ động inject bản ghi SIM Swap cố định"""
    test_db_path = "tests/unit/mock_services/hlr_hss/test_data/subscribers.csv"
    
    # 1. Sinh dữ liệu ngẫu nhiên nền (count=50)
    generate_mock_subscribers_csv(test_db_path, count=50, seed_value=42)
    
    # 2. Ép thêm 2 dòng dữ liệu SIM Swap cố định của cùng 1 số điện thoại vào file test
    # Điều này đảm bảo 100% kịch bản test_router_imsi_history_sim_swap sẽ tìm thấy dữ liệu mẫu
    with open(test_db_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        
        # IMSI ban đầu gán cho số +84979999999
        writer.writerow([
            "452010999999991", "+84979999999", "active", "452", "01", "Viettel",
            "true", "false", "true",
            "2022-03-15T08:00:00Z", "2022-03-15T08:00:00Z", "initial_activation"
        ])
        # IMSI mới (Bị Swap) cũng gán cho số +84979999999 vào thời gian sau đó
        writer.writerow([
            "452010999999992", "+84979999999", "active", "452", "01", "Viettel",
            "true", "false", "true",
            "2024-10-20T09:15:00Z", "2024-10-20T09:15:00Z", "customer_request"
        ])
    
    yield test_db_path
    
    if os.path.exists(test_db_path):
        os.remove(test_db_path)
        try:
            os.rmdir(os.path.dirname(test_db_path))
        except:
            pass

@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c