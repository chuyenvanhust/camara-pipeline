# tests/unit/api/test_schemas.py
import pytest
from pydantic import ValidationError
from api.schemas.common import PhoneNumber, ErrorResponse
from api.schemas.sim_swap import SimSwapCheckRequest
from api.schemas.device_swap import DeviceSwapCheckRequest
from api.schemas.number_verification import NumberVerifyRequest


# --- PhoneNumber ---

def test_phone_number_valid_e164():
    pn = PhoneNumber.validate("+84971234567")
    assert pn == "+84971234567"

def test_phone_number_missing_plus_fails():
    with pytest.raises(ValueError):
        PhoneNumber.validate("84971234567")

def test_phone_number_too_short_fails():
    with pytest.raises(ValueError):
        PhoneNumber.validate("+1234")  # < 7 chữ số sau +

def test_phone_number_strips_whitespace():
    pn = PhoneNumber.validate("  +84971234567  ")
    assert pn == "+84971234567"


# --- SimSwapCheckRequest ---

def test_sim_swap_request_valid():
    req = SimSwapCheckRequest(phoneNumber="+84971234567", maxAge=7)
    assert req.maxAge == 7

def test_sim_swap_request_default_max_age():
    req = SimSwapCheckRequest(phoneNumber="+84971234567")
    assert req.maxAge == 30

def test_sim_swap_request_max_age_zero_valid():
    req = SimSwapCheckRequest(phoneNumber="+84971234567", maxAge=0)
    assert req.maxAge == 0

def test_sim_swap_request_negative_max_age_fails():
    with pytest.raises(ValidationError):
        SimSwapCheckRequest(phoneNumber="+84971234567", maxAge=-1)

def test_sim_swap_request_invalid_phone_fails():
    with pytest.raises(ValidationError):
        SimSwapCheckRequest(phoneNumber="not-a-phone", maxAge=30)


# --- DeviceSwapCheckRequest (cùng pattern SIM Swap) ---

def test_device_swap_request_valid():
    req = DeviceSwapCheckRequest(phoneNumber="+84971234567", maxAge=14)
    assert req.maxAge == 14

def test_device_swap_request_default_max_age():
    req = DeviceSwapCheckRequest(phoneNumber="+84971234567")
    assert req.maxAge == 30


# --- NumberVerifyRequest ---

def test_number_verify_request_valid():
    req = NumberVerifyRequest(phoneNumber="+84971234567")
    assert req.phoneNumber == "+84971234567"

def test_number_verify_request_invalid_phone_fails():
    with pytest.raises(ValidationError):
        NumberVerifyRequest(phoneNumber="invalid")