import pytest
import os
import csv
from fastapi.testclient import TestClient
from mock_services.gsma_tac.app import app
from mock_services.gsma_tac.seed import load_tac_csv_to_memory

@pytest.fixture(scope="session", autouse=True)
def setup_test_csv_data():
    """Tạo file CSV mini độc lập riêng cho quá trình test độc lập cấu trúc"""
    test_path = "tests/unit/mock_services/gsma_tac/test_tac_records.csv"
    os.makedirs(os.path.dirname(test_path), exist_ok=True)
    
    with open(test_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["tac_code", "manufacturer", "model", "device_type", "operating_system", "band_support", "approved_date", "status"])
        writer.writerow(["352099", "Samsung", "Galaxy S23", "smartphone", "Android", "LTE;NR", "2023-01-15", "active"])
        writer.writerow(["490154", "Apple", "iPhone 14", "smartphone", "iOS", "LTE;NR", "2022-09-10", "active"])
        writer.writerow(["123456", "Huawei", "MediaPad", "tablet", "HarmonyOS", "LTE", "2021-05-20", "active"])
        
    load_tac_csv_to_memory(file_path=test_path)
    yield test_path
    
    if os.path.exists(test_path):
        os.remove(test_path)

@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c