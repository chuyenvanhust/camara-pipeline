import pytest
import os
import csv
from pipeline_v1.ingestion.csv_reader import LocalCSVReader

@pytest.fixture
def temp_mock_csv():
    """Tạo file CSV mẫu dung lượng nhỏ để kiểm thử bộ đọc cô lập"""
    file_path = "tests/unit/pipeline/ingestion/mock_small_radius.csv"
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    headers = ["acct_status_type", "acct_session_id", "msisdn", "imsi"]
    data = [
        ["Start", "SESS_001", "+84970000001", "452010000000001"],
        ["Stop", "SESS_001", "+84970000001", "452010000000001"],
        ["Start", "SESS_002", "+84970000002", "452010000000002"]
    ]
    
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(data)
        
    yield file_path
    
    if os.path.exists(file_path):
        os.remove(file_path)

def test_csv_reader_yields_correct_structure_and_count(temp_mock_csv):
    reader = LocalCSVReader(temp_mock_csv)
    records = list(reader.read_records())
    
    # Kiểm tra số lượng bản ghi bóc tách ra
    assert len(records) == 3
    
    # Kiểm tra cấu trúc dòng dữ liệu đầu tiên
    first_record = records[0]
    assert first_record["acct_status_type"] == "Start"
    assert first_record["acct_session_id"] == "SESS_001"
    assert first_record["msisdn"] == "+84970000001"
    assert "imsi" in first_record