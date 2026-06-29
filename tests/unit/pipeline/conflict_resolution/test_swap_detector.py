#tests\unit\pipeline\conflict_resolution\test_swap_detector.py
import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import Row
from pipeline_v1.conflict_resolution.swap_detector import SwapDetector

@pytest.fixture
def mock_row_conflict_c():
    return {
        "acct_session_id": "SESS_C02",
        "imsi": "IMSI_444",
        "msisdn": "9999333",
        "event_timestamp": "2026-06-14 12:30:00"
    }

@patch("pipeline.conflict_resolution.swap_detector.requests.get")
def test_swap_detector_confirmed_by_hlr(mock_get, mock_row_conflict_c):
    """
    Test Case 1: HLR/HSS xác nhận thông tin đổi SIM trùng khớp 
    -> Hàm phải trả về cấu trúc payload swap_event hoàn chỉnh.
    """
    # Thiết lập dữ liệu giả lập mà HLR Mock Server sẽ trả về
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "msisdn": "9999333",
        "old_imsi": "IMSI_333",
        "history": [
            {"imsi": "IMSI_333", "assigned_at": "2025-01-01 00:00:00"},
            {"imsi": "IMSI_444", "assigned_at": "2026-06-14 12:25:00"} # Khớp với new_imsi
        ]
    }
    mock_get.return_value = mock_response

    detector = SwapDetector(hlr_mock_url="http://fake-hlr:8200")
    event_output = detector.verify_and_emit_swap(mock_row_conflict_c)

    # Kiểm tra tính đúng đắn của cấu trúc bàn giao đầu ra cho module sau
    assert event_output is not None
    assert event_output["msisdn"] == "9999333"
    assert event_output["old_imsi"] == "IMSI_333"
    assert event_output["new_imsi"] == "IMSI_444"
    assert event_output["swap_type"] == "SIM_SWAP"
    assert event_output["confirmed_at"] == "2026-06-14 12:25:00"
    assert event_output["source"] == "RADIUS_CONFLICT_C"


@patch("pipeline.conflict_resolution.swap_detector.requests.get")
def test_swap_detector_rejected_by_hlr(mock_get, mock_row_conflict_c):
    """
    Test Case 2: HLR/HSS không tìm thấy lịch sử gán IMSI mới này 
    -> Coi như báo động giả, hàm trả về None (Không phát tán sự kiện).
    """
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "msisdn": "9999333",
        "history": [
            {"imsi": "IMSI_333", "assigned_at": "2025-01-01 00:00:00"}
            # Không có thông tin gì về IMSI_444 mới
        ]
    }
    mock_get.return_value = mock_response

    detector = SwapDetector(hlr_mock_url="http://fake-hlr:8200")
    event_output = detector.verify_and_emit_swap(mock_row_conflict_c)

    # Kết quả bắt buộc phải hủy bỏ sự kiện
    assert event_output is None