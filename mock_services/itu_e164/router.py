#mock_services\itu_e164\router.py
from fastapi import APIRouter, Header, status
from fastapi.responses import JSONResponse
import uuid
from mock_services.itu_e164.models import SingleValidationRequest, ValidationResult, BatchValidationRequest, BatchValidationResponse
from mock_services.itu_e164.seed import COUNTRY_DB, OPERATOR_DB

router = APIRouter()

def validate_e164_logic(phone: str) -> ValidationResult:
    """Hàm lõi xử lý thuật toán phân tách đầu số E.164"""
    # 1. Kiểm tra format cơ bản
    if not phone.startswith("+") or not phone[1:].isdigit() or len(phone) < 11 or len(phone) > 16:
        return ValidationResult(phone_number=phone, valid=False, error_code="INVALID_FORMAT", error_detail="Number must start with '+' followed by 10-15 digits")

    raw_num = phone[1:]
    matched_cc = None

    # 2. Tìm Country Code
    for i in range(1, 4):
        possible_cc = raw_num[:i]
        if possible_cc in COUNTRY_DB:
            matched_cc = possible_cc
            break

    if not matched_cc:
        return ValidationResult(phone_number=phone, valid=False, error_code="UNKNOWN_COUNTRY", error_detail="Country code is not supported by ITU database")

    remains = raw_num[len(matched_cc):]
    if len(remains) < 7:
        return ValidationResult(phone_number=phone, valid=False, error_code="INVALID_FORMAT", error_detail="Subscriber number part is too short")

    # 3. Phân tách nhà mạng
    allowed_prefixes = OPERATOR_DB.get(matched_cc, set())
    matched_prefix = None

    for j in range(2, 4):
        possible_prefix = remains[:j]
        if possible_prefix in allowed_prefixes:
            matched_prefix = possible_prefix
            break

    if not matched_prefix:
        return ValidationResult(phone_number=phone, valid=False, error_code="UNKNOWN_OPERATOR", error_detail="Unknown network operator prefix")

    return ValidationResult(
        phone_number=phone,
        valid=True,
        country_code=matched_cc,
        country_name=COUNTRY_DB[matched_cc],
        subscriber_number=remains,
        operator=f"Operator_{matched_prefix}",
        e164_format=phone,
    )

@router.post("/validate", response_model=ValidationResult)
def validate_single_number(payload: SingleValidationRequest):
    # [Kế thừa Phase 1]: Hạ tầng sẽ tự động can thiệp X-Inject-Fault qua Shared Middleware ở app.py
    result = validate_e164_logic(payload.phone_number)
    return result

@router.post("/validate/batch", response_model=BatchValidationResponse)
def validate_batch_numbers(payload: BatchValidationRequest):
    results = {}
    valid_cnt = 0
    
    for phone in payload.phone_numbers:
        res = validate_e164_logic(phone)
        results[phone] = res
        if res.valid:
            valid_cnt += 1
            
    total = len(payload.phone_numbers)
    return BatchValidationResponse(
        results=results, total=total, valid_count=valid_cnt, invalid_count=total - valid_cnt
    )