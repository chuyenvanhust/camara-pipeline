import pytest
from pydantic import ValidationError
from mock_services.itu_e164.models import SingleValidationRequest, BatchValidationRequest

def test_models_single_request_validation():
    req = SingleValidationRequest(phone_number="+84912345678")
    assert req.phone_number == "+84912345678"

def test_models_batch_request_overflow():
    large_batch = [f"+849123456{i:02d}" for i in range(105)]
    with pytest.raises(ValidationError):
        BatchValidationRequest(phone_numbers=large_batch)