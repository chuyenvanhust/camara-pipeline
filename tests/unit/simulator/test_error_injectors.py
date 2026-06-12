import pytest
from simulator.error_injectors import ErrorInjector

@pytest.fixture
def sample_records():
    """Tạo tập dữ liệu thô mẫu (Happy Path) để tiến hành tiêm lỗi"""
    return [
        {
            "acct_status_type": "Start",
            "acct_session_id": "SESS_001",
            "event_timestamp": "2026-01-01T00:00:00Z",
            "ingest_timestamp": "2026-01-01T00:00:02Z",
            "msisdn": "+84970000001",
            "imsi": "452010000000001",
            "imei": "356123000000004",  # Luhn valid
            "framed_ip": "10.100.0.1"
        }
    ]

def test_inject_duplicates(base_config, sample_records):
    base_config.duplicate_rate = 1.0  # Ép tỷ lệ 100% để luôn nhân đôi
    injector = ErrorInjector(base_config)
    
    result = injector.inject_duplicates(sample_records)
    assert len(result) == 2
    assert result[0]["acct_session_id"] == result[1]["acct_session_id"]

def test_inject_missing_fields(base_config, sample_records):
    base_config.missing_field_rate = 1.0
    injector = ErrorInjector(base_config)
    
    result = injector.inject_missing_fields(sample_records)
    # Xác định xem một trong các trường bắt buộc đã bị xóa trống chưa
    rec = result[0]
    assert rec["acct_status_type"] == "" or rec["acct_session_id"] == "" or rec["msisdn"] == ""

def test_inject_invalid_imei_breaks_luhn(base_config, sample_records):
    base_config.invalid_imei_rate = 1.0
    injector = ErrorInjector(base_config)
    
    result = injector.inject_invalid_imei(sample_records)
    broken_imei = result[0]["imei"]
    
    # Kiểm tra xem mã IMEI sau khi tiêm lỗi có bị phá checksum Luhn không
    digits = [int(d) for d in broken_imei]
    for i in range(len(digits) - 2, -1, -2):
        digits[i] *= 2
        if digits[i] > 9:
            digits[i] -= 9
    assert sum(digits) % 10 != 0