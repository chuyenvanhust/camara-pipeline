import pytest
import os
import csv
from fastapi.testclient import TestClient
from mock_services.itu_e164.app import app
from mock_services.itu_e164.seed import load_itu_data_to_memory

@pytest.fixture(scope="session", autouse=True)
def setup_itu_test_environment():
    """Tạo DB CSV độc lập cho môi trường test"""
    data_dir = "tests/unit/mock_services/itu_e164/test_data"
    os.makedirs(data_dir, exist_ok=True)
    
    cc_path = os.path.join(data_dir, "country_codes.csv")
    op_path = os.path.join(data_dir, "operator_prefixes.csv")
    
    with open(cc_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["country_code", "country_name"])
        writer.writerows([["84", "Vietnam"], ["1", "USA"]])
        
    with open(op_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["country_code", "prefix"])
        writer.writerows([["84", "91"], ["1", "202"]])
    
    # Ép hệ thống dùng DB test này trong bộ nhớ RAM suốt quá trình chạy test
    # Khắc phục lỗi bất đối xứng dữ liệu khi load_itu_data_to_memory chạy đè
    yield (cc_path, op_path)
    
    if os.path.exists(cc_path): os.remove(cc_path)
    if os.path.exists(op_path): os.remove(op_path)

@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c