import pytest
from pydantic import ValidationError
from mock_services.gsma_tac.models import TacRecord, BatchRequest

def test_models_tac_record_valid():
    record = TacRecord(
        tac="352099", manufacturer="Samsung", model="Galaxy S23",
        device_type="smartphone", operating_system="Android",
        band_support=["LTE", "NR"], approved_date="2023-01-15", status="active"
    )
    assert record.tac == "352099"

def test_models_batch_request_too_many_items():
    # Test giới hạn max 100 items của cấu trúc payload
    large_list = [f"{i:06d}" for i in range(101)]
    with pytest.raises(ValidationError):
        BatchRequest(tac_codes=large_list)