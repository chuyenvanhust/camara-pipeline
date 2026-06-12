import pytest
from pydantic import ValidationError
from mock_services.hlr_hss.models import BatchLookupRequest

def test_models_batch_request_validation():
    # Thử nghiệm giới hạn chặn batch lookup khi vượt ngưỡng 200 bản ghi
    overflow_queries = [{"type": "imsi", "value": f"452010123456{i:03d}"} for i in range(210)]
    with pytest.raises(ValidationError):
        BatchLookupRequest(lookups=overflow_queries)