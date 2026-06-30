#!/usr/bin/env python3
"""
Stage S2 -- Ap dung 6 validation rules tren moi record.
Record hop le -> `radius.valid`. Record loi -> `radius.invalid` + ghi `invalid_log`.

Cac rule duoc dinh nghia o file nay deu la pure async function, nhan vao
1 dict record (+ httpx.AsyncClient cho rule can goi mock service) va tra
ve ValidationResult. Orchestrator execute_validation_pipeline() chay tuan
tu R1->R6, dung lai (fail-fast) ngay khi gap rule dau tien fail.
"""

import re
import os
import httpx
import asyncio
from typing import Tuple, Optional, Dict, Any
from dataclasses import dataclass
TAC_CACHE = {}
IMSI_CACHE = {}

@dataclass
class ValidationResult:
    """Ket qua cua 1 rule validation.

    Attributes:
        is_valid: True neu record pass rule nay.
        error_code: Ma loi (vd ERR_MISSING_FIELD) khi is_valid=False.
        error_message: Mo ta chi tiet loi, dung cho invalid_log.
        warn_code: WARN_RULE_BYPASSED khi circuit breaker mo va rule
            bi bo qua (record van duoc tinh la valid).
    """
    is_valid: bool
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    warn_code: Optional[str] = None


# Cau hinh Endpoints tu moi truong (xem mock_services/README.md)
ITU_E164_SERVICE_URL = os.getenv("ITU_E164_SERVICE_URL", "http://camara-mock-itu-e164:8300")
HLR_HSS_SERVICE_URL = os.getenv("HLR_HSS_SERVICE_URL", "http://camara-mock-hlr-hss:8200")
GSMA_TAC_SERVICE_URL = os.getenv("GSMA_TAC_SERVICE_URL", "http://camara-mock-gsma-tac:8100")

# --- HA TANG RESILIENCE (RETRY BACKOFF & CIRCUIT BREAKER) ---

#: So lan fail lien tiep toi da truoc khi circuit breaker mo (Open)
CIRCUIT_BREAKER_LIMIT = 100

#: Bo dem fail hien tai cho moi mock service. Reset ve 0 khi 1 request
#: thanh cong (ke ca khi response la loi nghiep vu 400/404 -- van la
#: "thanh cong ve mat ha tang"). Day la state GLOBAL, test phai reset
#: truoc/sau moi test case (xem tests/.../README.md).
failed_counters: Dict[str, int] = {
    "ITU_E164": 0,
    "HLR_HSS": 0,
    "GSMA_TAC": 0,
}


async def invoke_external_api_with_resilience(
    service_key: str,
    client: httpx.AsyncClient,
    method: str,
    url: str,
    **kwargs,
) -> Tuple[Optional[httpx.Response], Optional[str]]:
    """Goi 1 mock service voi circuit breaker + retry/backoff.

    Quy trinh:
        1. Neu failed_counters[service_key] >= CIRCUIT_BREAKER_LIMIT
           -> breaker dang "Open" -> tra ve (None, "WARN_RULE_BYPASSED")
           ngay, KHONG goi network.
        2. Nguoc lai, goi client.get/post voi timeout=2.0s.
           - Thanh cong (bat ke status code) -> reset counter ve 0,
             tra ve (response, None).
           - TimeoutException / RequestError -> retry toi da 2 lan
             voi exponential backoff (0.5s -> 1.0s). Het retry ->
             tang failed_counters[service_key], tra ve
             (None, "ERR_EXTERNAL_TIMEOUT" | "ERR_EXTERNAL_CONN_FAIL").

    Args:
        service_key: 1 trong "ITU_E164", "HLR_HSS", "GSMA_TAC" --
            key trong failed_counters.
        client: httpx.AsyncClient dung chung cho ca batch.
        method: "GET" hoac "POST".
        url: full URL cua mock service endpoint.
        **kwargs: forward toi client.get/post (vd json=...).

    Returns:
        (response, error_code):
            - (Response, None) neu goi thanh cong.
            - (None, "WARN_RULE_BYPASSED") neu breaker dang Open.
            - (None, "ERR_EXTERNAL_TIMEOUT" | "ERR_EXTERNAL_CONN_FAIL")
              neu het retry van loi.
    """
    global failed_counters

    if failed_counters[service_key] >= CIRCUIT_BREAKER_LIMIT:
        return None, "WARN_RULE_BYPASSED"

    try:
        if method.upper() == "POST":
            response = await client.post(url, timeout=5.0, **kwargs)
        else:
            response = await client.get(url, timeout=5.0, **kwargs)

        failed_counters[service_key] = 0
        return response, None

    except httpx.TimeoutException:
        failed_counters[service_key] += 1
        return None, "ERR_EXTERNAL_TIMEOUT"

    except httpx.RequestError:
        failed_counters[service_key] += 1
        return None, "ERR_EXTERNAL_CONN_FAIL"


# ==============================================================================
# RULE 1: Mandatory fields khong null
# ==============================================================================
async def validate_mandatory_fields(record: Dict[str, Any]) -> ValidationResult:
    """R1: kiem tra cac field bat buoc khong rong/null.

    Cac field bat buoc, kiem theo thu tu (bao loi tai field dau tien
    bi thieu): acct_status_type, acct_session_id, msisdn, imsi, imei,
    event_timestamp.

    Returns:
        ValidationResult(is_valid=False, error_code="ERR_MISSING_FIELD")
        neu thieu field; nguoc lai is_valid=True.
    """
    mandatory_fields = [
        "acct_status_type", "acct_session_id", "msisdn",
        "imsi", "imei", "event_timestamp",
    ]
    for field in mandatory_fields:
        if field not in record or record[field] is None or str(record[field]).strip() == "":
            return ValidationResult(
                is_valid=False,
                error_code="ERR_MISSING_FIELD",
                error_message=f"{field} is required",
            )
    return ValidationResult(is_valid=True)


# ==============================================================================
# RULE 2: MSISDN format E.164 + operator prefix hop le
# ==============================================================================
async def validate_msisdn_format(record: Dict[str, Any], client: httpx.AsyncClient) -> ValidationResult:
    """R2: kiem tra format E.164 (regex local), sau do goi ITU E.164
    Mock API (POST /validate) de xac nhan country code + operator
    prefix + do dai subscriber number hop le.

    Returns:
        - ERR_INVALID_MSISDN neu regex fail HOAC mock tra valid=False.
        - WARN_RULE_BYPASSED (is_valid=True) neu circuit breaker mo.
        - ERR_EXTERNAL_* neu mock service loi ha tang.
        - is_valid=True neu mock tra valid=True.
    """
    msisdn = str(record.get("msisdn", "")).strip()
    if not re.match(r"^\+[1-9]\d{1,14}$", msisdn):
        return ValidationResult(is_valid=False, error_code="ERR_INVALID_MSISDN", error_message="Violate E.164 regex")

    url = f"{ITU_E164_SERVICE_URL}/validate"
    res, err = await invoke_external_api_with_resilience("ITU_E164", client, "POST", url, json={"phone_number": msisdn})

    if err == "WARN_RULE_BYPASSED":
        return ValidationResult(is_valid=True, warn_code="WARN_RULE_BYPASSED")
    if err:
        return ValidationResult(is_valid=False, error_code=err, error_message="ITU service unavailable")

    if res.status_code == 200 and res.json().get("valid") is True:
        return ValidationResult(is_valid=True)
    return ValidationResult(is_valid=False, error_code="ERR_INVALID_MSISDN", error_message="Invalid prefix/operator")


# ==============================================================================
# RULE 3: IMSI ton tai trong HLR
# ==============================================================================
async def validate_imsi_in_hlr(record: Dict[str, Any], client: httpx.AsyncClient) -> ValidationResult:
    imsi = str(record.get("imsi", "")).strip()
    
    # 1. Kiểm tra Cache
    if imsi in IMSI_CACHE:
        return IMSI_CACHE[imsi]

    url = f"{HLR_HSS_SERVICE_URL}/subscribers/by-imsi/{imsi}"
    res, err = await invoke_external_api_with_resilience("HLR_HSS", client, "GET", url)

    # 2. Xử lý lỗi hạ tầng (Infrastructure Errors)
    # Nếu là lỗi mạng/timeout, trả về ngay và KHÔNG cache
    if err in ["ERR_EXTERNAL_TIMEOUT", "ERR_EXTERNAL_CONN_FAIL"]:
        return ValidationResult(
            is_valid=False, 
            error_code=err, 
            error_message="HLR service unavailable due to infrastructure error"
        )
    
    # Nếu Circuit Breaker mở (Bypass)
    if err == "WARN_RULE_BYPASSED":
        return ValidationResult(is_valid=True, warn_code="WARN_RULE_BYPASSED")

    # 3. Xử lý kết quả nghiệp vụ (Business Logic)
    # Kiểm tra status code và dữ liệu trả về
    if res and res.status_code == 200 and res.json().get("exists") is True:
        result = ValidationResult(is_valid=True)
        IMSI_CACHE[imsi] = result  # Cache kết quả hợp lệ
        return result
    
    # 4. Nếu 404 hoặc exists=False -> Đây mới là lỗi nghiệp vụ (Dữ liệu không tồn tại)
    result = ValidationResult(
        is_valid=False, 
        error_code="ERR_IMSI_NOT_IN_HLR", 
        error_message="IMSI not found in HLR"
    )
    IMSI_CACHE[imsi] = result # Cache lỗi nghiệp vụ để tránh gọi lại request vô ích
    return result

# ==============================================================================
# RULE 4a: IMEI pass Luhn algorithm
# ==============================================================================
async def validate_imei_luhn(record: Dict[str, Any]) -> ValidationResult:
    imei = str(record.get("imei", "")).strip()
    if not imei.isdigit() or len(imei) != 15:
        return ValidationResult(is_valid=False, error_code="ERR_IMEI_LUHN_FAIL", error_message="IMEI must be 15 digits")

    # LOGIC MỚI: Đồng bộ với Simulator (duyệt từ phải qua trái)
    digits = [int(d) for d in imei]
    # Dùng 14 số đầu để tính checksum, so sánh với số thứ 15
    check_part = digits[:14]
    
    # Duyệt từ phải qua trái (index 12, 10, 8, 6, 4, 2, 0)
    for i in range(len(check_part) - 1, -1, -2):
        val = check_part[i] * 2
        check_part[i] = val if val < 10 else val - 9
        
    total = sum(check_part)
    expected_checksum = (10 - (total % 10)) % 10
    
    if expected_checksum != digits[14]:
        return ValidationResult(is_valid=False, error_code="ERR_IMEI_LUHN_FAIL", error_message="Luhn check fail")
        
    return ValidationResult(is_valid=True)

# ==============================================================================
# RULE 4b: TAC (6 chu so dau IMEI) co trong GSMA TAC DB
# ==============================================================================
async def validate_imei_tac(record: Dict[str, Any], client: httpx.AsyncClient) -> ValidationResult:
    imei = str(record.get("imei", "")).strip()
    tac = imei[:6]
    
    # [MỚI] Kiểm tra Cache
    if tac in TAC_CACHE:
        return TAC_CACHE[tac]

    url = f"{GSMA_TAC_SERVICE_URL}/tac/{tac}"
    res, err = await invoke_external_api_with_resilience("GSMA_TAC", client, "GET", url)

    # [MỚI] Lưu kết quả vào Cache trước khi return
    if err == "WARN_RULE_BYPASSED":
        return ValidationResult(is_valid=True, warn_code="WARN_RULE_BYPASSED")
    if err:
        return ValidationResult(is_valid=False, error_code=err, error_message="GSMA service unavailable")

    if res.status_code == 200 and res.json().get("exists") is True:
        result = ValidationResult(is_valid=True)
        TAC_CACHE[tac] = result # Lưu vào cache
        return result
    
    result = ValidationResult(is_valid=False, error_code="ERR_IMEI_TAC_UNKNOWN", error_message="Unknown TAC")
    TAC_CACHE[tac] = result # Cache cả trường hợp không tồn tại
    return result


# ==============================================================================
# RULE 5: acct_status_type in {Start, Stop, Interim-Update}
# ==============================================================================
async def validate_acct_status_type(record: Dict[str, Any]) -> ValidationResult:
    """R5: acct_status_type phai thuoc {Start, Stop, Interim-Update}.

    Returns:
        ERR_INVALID_STATUS neu khong khop; nguoc lai is_valid=True.
    """
    if str(record.get("acct_status_type", "")).strip() not in {"Start", "Stop", "Interim-Update"}:
        return ValidationResult(is_valid=False, error_code="ERR_INVALID_STATUS", error_message="Invalid status type")
    return ValidationResult(is_valid=True)


# ==============================================================================
# RULE 6: event_timestamp trong khoang hop le
# ==============================================================================
from datetime import datetime

async def validate_event_timestamp(record: Dict[str, Any]) -> ValidationResult:
    """
    R6: Kiểm tra tính hợp lệ của cả event_timestamp và ingest_timestamp.
    Chấp nhận chuỗi ISO (2026-03-29T07:12:00Z) hoặc Unix timestamp.
    """
    
    # Hàm phụ để tái sử dụng logic parse
    def get_unix_timestamp(field_name: str) -> int:
        val = str(record.get(field_name, "")).strip()
        if not val:
            raise ValueError(f"{field_name} is empty")
            
        if "T" in val:
            # Parse định dạng ISO 8601
            dt = datetime.fromisoformat(val.replace('Z', '+00:00'))
            return int(dt.timestamp())
        else:
            # Parse định dạng số nguyên
            return int(val)

    try:
        # 1. Kiểm tra event_timestamp (Quan trọng nhất cho logic Partition)
        event_ts = get_unix_timestamp("event_timestamp")
        if not (946684800 <= event_ts <= 4102444800):
            return ValidationResult(
                is_valid=False, 
                error_code="ERR_INVALID_TIMESTAMP", 
                error_message="event_timestamp out of bounds"
            )

        # 2. Kiểm tra ingest_timestamp (Quan trọng để ghi vào Database không bị lỗi)
        # Chúng ta chỉ cần đảm bảo nó parse được, không cần check range quá kỹ
        get_unix_timestamp("ingest_timestamp")

        return ValidationResult(is_valid=True)

    except Exception as e:
        # Trả về thông báo lỗi cụ thể để dễ debug
        return ValidationResult(
            is_valid=False, 
            error_code="ERR_INVALID_TIMESTAMP", 
            error_message=f"Timestamp format error: {str(e)}"
        )
# ==============================================================================
# DONG CO DIEU PHOI CHUOI TUAN TU (ORCHESTRATOR)
# ==============================================================================
async def execute_validation_pipeline(record: Dict[str, Any], client: httpx.AsyncClient) -> Tuple[ValidationResult, Optional[str]]:
    """Chay tuan tu toan bo 6 Rules (R1 -> R2 -> R3 -> R4a -> R4b ->
    R5 -> R6). Gap rule dau tien fail -> dung ngay (fail-fast),
    KHONG chay cac rule sau.

    Neu mot rule pass nhung kem warn_code (circuit breaker bypass),
    warn_code duoc tich luy va tra ve cung ket qua cuoi (record van
    duoc tinh la valid neu khong rule nao fail).

    Args:
        record: dict 1 row RADIUS (theo RAW_RADIUS_SCHEMA).
        client: httpx.AsyncClient dung chung cho ca batch.

    Returns:
        (ValidationResult, warn_code):
            - Neu mot rule fail: (ValidationResult cua rule do, None).
            - Neu tat ca pass: (ValidationResult(is_valid=True),
              warn_code cuoi cung neu co, None neu khong).
    """
    pipeline_rules = [
        (validate_mandatory_fields, False),
        (validate_msisdn_format, True),
        (validate_imsi_in_hlr, True),
        (validate_imei_luhn, False),
        (validate_imei_tac, True),
        (validate_acct_status_type, False),
        (validate_event_timestamp, False),
    ]

    accumulated_warn = None

    for rule_func, requires_client in pipeline_rules:
        if requires_client:
            res = await rule_func(record, client)
        else:
            res = await rule_func(record)

        if not res.is_valid:
            return res, None

        if res.warn_code:
            accumulated_warn = res.warn_code

    return ValidationResult(is_valid=True), accumulated_warn