#!/usr/bin/env python3
#pipeline\pipeline\validation\rules.py
"""
Stage S2 -- Ap dung 6 validation rules tren moi record.
Record hop le -> `radius.valid`. Record loi -> `radius.invalid` + ghi `invalid_log`.

[CAP NHAT - HYBRID PREFETCH BATCH]
File nay ho tro 2 che do:
  1. Che do CU (single-call): moi rule tu goi API rieng cho tung record.
     Van con giu lai de tuong thich nguoc / test don le.
  2. Che do MOI (batch, khuyen dung): goi execute_validation_pipeline_batch()
     cho ca mot List[Dict] records. Ham nay se:
       - Loc TAC chua co trong TAC_CACHE -> goi POST /tac/batch (GSMA)
       - Gom tat ca IMSI trong batch -> goi POST /subscribers/batch-lookup (HLR)
       - Gom tat ca MSISDN trong batch -> goi POST /validate/batch (ITU)
       - Goi ca 3 API tren dong thoi bang asyncio.gather
       - Sau do chay validate tuan tu 6 rules cho tung record, nhung R2/R3/R4b
         se tra cuu ket qua tu cac map da prefetch thay vi tu goi API.

Cac rule van la pure async function; rule can du lieu tu batch (R2, R3, R4b)
nhan them 1 tham so optional (itu_map / hlr_map) - neu duoc truyen vao thi
uu tien tra cuu map, khong goi API don le nua.
"""

import re
import os
import httpx
import asyncio
import logging
from typing import Tuple, Optional, Dict, Any, List
from dataclasses import dataclass

logger = logging.getLogger(__name__)

TAC_CACHE = {}


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

# Gioi han so luong item toi da moi request Batch.
# [FIX] Xac nhan thuc te qua log loi 422 cua tung mock (rat quan trong:
# KHONG DUNG CHUNG 1 CON SO CHO CA 3, moi mock co gioi han rieng):
#   - GSMA /tac/batch               : toi da 100 item/request
#   - ITU  /validate/batch          : toi da 100 item/request
#   - HLR  /subscribers/batch-lookup: toi da 200 item/request (RIENG, cao
#     hon 2 cai tren -- da xac nhan qua log, luon "200 OK" voi 200 IMSI)
GSMA_BATCH_MAX = 500     # POST /tac/batch
HLR_BATCH_MAX = 500      # POST /subscribers/batch-lookup
ITU_BATCH_MAX = 500      # POST /validate/batch -- RIENG BIET, thap hon GSMA/HLR

# [BUOC 4 - KE HOACH SONG SONG HOA] So luong request Batch (chunk) toi da
# duoc phep chay DONG THOI cho 1 rule (GSMA/HLR/ITU) trong 1 micro-batch.
# Khi so IMSI/MSISDN/TAC unique trong batch vuot xa gioi han moi request
# (100-200), so chunk co the len toi vai chuc (vd maxOffsetsPerTrigger=5000
# -> co the ~50 chunk cho 1 rule). Neu gui TAT CA chunk cung luc cho CA 3
# rule (GSMA+HLR+ITU dung chung 1 httpx.AsyncClient), tong so ket noi dong
# thoi co the vuot gioi han mac dinh cua httpx (max_connections=100),
# gay xep hang cho ket noi thay vi loi that su -- nhung van nen gioi han
# de tranh lam qua tai mock service / mang Docker. Dung asyncio.Semaphore
# de gioi han so chunk chay dong thoi MOI RULE, khong gioi han tong so
# chunk (moi rule co semaphore rieng).
BATCH_FETCH_MAX_CONCURRENCY = int(os.getenv("BATCH_FETCH_MAX_CONCURRENCY", 8))

# --- HA TANG RESILIENCE (RETRY BACKOFF & CIRCUIT BREAKER) ---

#: So lan fail lien tiep toi da truoc khi circuit breaker mo (Open)
CIRCUIT_BREAKER_LIMIT = 20

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
    """... (docstring giữ nguyên, bổ sung đoạn dưới)

    [FIX] Bo sung nhan dien FAULT INJECTION qua status code (x-mock-fault
    cua cac mock service tra ve 500/502/503/504 thay vi that su lam
    httpx nem TimeoutException/RequestError). Truoc day, code chi coi la
    "loi ha tang" khi bat duoc exception transport-level -- moi response
    HTTP hop le (ke ca 5xx do fault-inject) deu bi tinh la "thanh cong",
    lam:
      1. failed_counters bi reset ve 0 -> circuit breaker khong bao gio
         mo du mock dang fault lien tuc.
      2. Cac ham fetch_*_batch() gan nham response 5xx do fault-inject
         thanh loi nghiep vu rieng tung service (VD:
         ERR_EXTERNAL_HLR_BATCH_INVALID) thay vi loi ha tang CHUNG.

    Sau fix: bat ky response nao co status_code thuoc {500, 502, 503, 504}
    deu duoc coi la FAULT HA TANG (giong timeout) -> tang
    failed_counters, tra ve (None, "ERR_EXTERNAL_TIMEOUT"), KHONG tra ve
    response nua (de fetch_*_batch khong the vo tinh doc duoc body cua
    response fault). Chi con status 4xx (vd 422 validation that) moi duoc
    coi la "goi thanh cong ve mat ha tang" va di tiep xuong tang xu ly
    nghiep vu cua tung fetch_*_batch.
    """
    global failed_counters

    if failed_counters[service_key] >= CIRCUIT_BREAKER_LIMIT:
        return None, "WARN_RULE_BYPASSED"

    # Status code coi la FAULT HA TANG (mock gia lap qua x-mock-fault),
    # KHONG PHAI loi nghiep vu -- xu ly giong timeout/conn-fail.
    INFRA_FAULT_STATUS_CODES = {500, 502, 503, 504}

    try:
        if method.upper() == "POST":
            response = await client.post(url, timeout=10.0, **kwargs)
        else:
            response = await client.get(url, timeout=10.0, **kwargs)

        # [FIX] Kiem tra status code TRUOC khi coi la thanh cong.
        if response.status_code in INFRA_FAULT_STATUS_CODES:
            failed_counters[service_key] += 1
            return None, "ERR_EXTERNAL_TIMEOUT"

        failed_counters[service_key] = 0
        return response, None

    except httpx.TimeoutException:
        failed_counters[service_key] += 1
        return None, "ERR_EXTERNAL_TIMEOUT"

    except httpx.RequestError:
        failed_counters[service_key] += 1
        return None, "ERR_EXTERNAL_CONN_FAIL"


def _chunked(items: List[Any], size: int) -> List[List[Any]]:
    """Chia 1 list thanh cac chunk co do dai toi da `size`."""
    return [items[i:i + size] for i in range(0, len(items), size)]


async def _gather_chunks(chunks: List[List[Any]], call_fn) -> List[Dict[str, ValidationResult]]:
    """
    [BUOC 4 - SONG SONG HOA CHUNK] Goi `call_fn(chunk)` cho TAT CA cac
    chunk DONG THOI (thay vi vong lap `for chunk in chunks: await ...`
    tuan tu nhu truoc), gioi han boi Semaphore(BATCH_FETCH_MAX_CONCURRENCY)
    de khong lam qua tai connection pool cua httpx.AsyncClient / mock
    service khi so chunk lon (vd batch co hang nghin IMSI/MSISDN/TAC
    unique -> hang chuc chunk).

    Luu y ve circuit breaker: `invoke_external_api_with_resilience` doc
    `failed_counters[service_key]` truoc moi request. Vi cac chunk chay
    DONG THOI, nhieu chunk co the cung "thay" breaker dang dong (chua
    vuot CIRCUIT_BREAKER_LIMIT) truoc khi bat ky chunk nao kip that bai
    va tang counter -- nghia la breaker co the mo cham hon 1 chut so voi
    chay tuan tu (vai request "lot" qua truoc khi mo). Day la danh doi
    chap nhan duoc (bulkhead pattern), khong anh huong tinh dung dan cua
    KET QUA, chi anh huong toc do phan ung cua breaker.

    Args:
        chunks: danh sach cac chunk (moi chunk la 1 list item).
        call_fn: async function nhan 1 chunk, tra ve Dict[key, ValidationResult]
            cho CHINH chunk do (khong duoc gop chung nhieu chunk).

    Returns:
        List cac Dict ket qua, cung thu tu voi `chunks` (se duoc gop lai
        boi ham goi).
    """
    if not chunks:
        return []

    semaphore = asyncio.Semaphore(BATCH_FETCH_MAX_CONCURRENCY)

    async def _bounded_call(chunk):
        async with semaphore:
            return await call_fn(chunk)

    return await asyncio.gather(*[_bounded_call(c) for c in chunks])



# ==============================================================================
# BATCH FETCHERS -- goi 1 lan cho toan bo Batch Spark (thay vi tung record)
# ==============================================================================
async def fetch_gsma_batch(tac_codes: List[str], client: httpx.AsyncClient) -> Dict[str, ValidationResult]:
    """Tra cuu nhieu TAC cung luc qua POST /tac/batch (GSMA Mock).

    Chi nen duoc goi voi cac TAC CHUA co trong TAC_CACHE (xem
    execute_validation_pipeline_batch). Tu dong chia nho theo
    GSMA_BATCH_MAX (100 TAC/request, gioi han rieng cua GSMA) neu vuot gioi han.

    [FIX] Phan biet ro 2 loai loi khi status != 200:
      - Fault ha tang (mock tra ve 500/502/503/504 qua x-mock-fault, hoac
        timeout/conn-fail o tang transport) -> da duoc
        invoke_external_api_with_resilience() chan lai va tra ve
        err="ERR_EXTERNAL_TIMEOUT"/"ERR_EXTERNAL_CONN_FAIL" o day, KHONG
        con roi xuong nhanh else ben duoi nua -> gan mot ma loi CHUNG,
        khong phan biet theo tung service.
      - Loi validation THAT (mock tra ve 422 vi request body sai schema)
        -> van la HTTP response hop le (status=422, response != None),
        roi vao nhanh else -> gan ma loi rieng ERR_EXTERNAL_REQUEST_REJECTED
        (khong con dat ten theo tung service nhu truoc).

    Args:
        tac_codes: danh sach TAC (6 chu so) can tra cuu, nen la list(set(...))
            de tranh trung lap.
        client: httpx.AsyncClient dung chung cho ca batch.

    Returns:
        Dict {tac_code: ValidationResult}. Ket qua nay se duoc nap vao
        TAC_CACHE (global) boi ham goi.
    """
    results: Dict[str, ValidationResult] = {}
    if not tac_codes:
        return results

    # Loc TAC dung dinh dang (6 chu so) TRUOC khi goi API -- 1 TAC sai
    # dinh dang trong list se khien Pydantic tu choi CA REQUEST (422),
    # nen loc truoc de tranh goi API vo ich.
    to_call: List[str] = []
    for tac in tac_codes:
        if re.match(r"^\d{6}$", tac):
            to_call.append(tac)
        else:
            results[tac] = ValidationResult(
                is_valid=False, error_code="ERR_IMEI_TAC_UNKNOWN",
                error_message="TAC malformed (khong du 6 chu so, thuong do IMEI thieu/sai)",
            )

    chunks = _chunked(to_call, GSMA_BATCH_MAX)

    async def _call_chunk(chunk: List[str]) -> Dict[str, ValidationResult]:
        """Goi 1 chunk TAC, tra ve Dict ket qua CHI CHO chunk nay (khong
        ghi truc tiep vao `results` cua ham ngoai -- de an toan khi chay
        dong thoi voi cac chunk khac qua asyncio.gather)."""
        chunk_results: Dict[str, ValidationResult] = {}
        url = f"{GSMA_TAC_SERVICE_URL}/tac/batch"
        res, err = await invoke_external_api_with_resilience(
            "GSMA_TAC", client, "POST", url, json={"tac_codes": chunk}
        )

        # [FIX] err o day GIO CHI con la: WARN_RULE_BYPASSED (breaker mo)
        # hoac ERR_EXTERNAL_TIMEOUT/ERR_EXTERNAL_CONN_FAIL (fault ha tang,
        # bao gom ca 5xx tu x-mock-fault) -- vi invoke_external_api_with_resilience
        # da chan 5xx tu tang duoi, khong con tra ve response=5xx nua.
        if err == "WARN_RULE_BYPASSED":
            for tac in chunk:
                chunk_results[tac] = ValidationResult(is_valid=True, warn_code="WARN_RULE_BYPASSED")
            return chunk_results

        if err:
            # Fault ha tang CHUNG (khong phan biet GSMA/HLR/ITU trong ma loi)
            for tac in chunk:
                chunk_results[tac] = ValidationResult(
                    is_valid=False, error_code=err, error_message="GSMA service unavailable (infrastructure fault)"
                )
            return chunk_results

        if res.status_code == 200:
            payload = res.json().get("results", {})
            for tac in chunk:
                info = payload.get(tac)
                if info and info.get("found"):
                    chunk_results[tac] = ValidationResult(is_valid=True)
                else:
                    chunk_results[tac] = ValidationResult(
                        is_valid=False, error_code="ERR_IMEI_TAC_UNKNOWN", error_message="Unknown TAC"
                    )
        else:
            # [FIX] Nhanh nay GIO CHI con nhan status 4xx that (vd 422
            # validation loi vi body sai schema) -- KHONG con the la 5xx
            # fault-inject nua vi da bi chan o tang invoke_external_api_with_resilience.
            # Doi ten ma loi thanh CHUNG (khong con "_GSMA_BATCH_INVALID"
            # rieng theo service) de nhat quan voi HLR/ITU.
            logger.error(
                "GSMA /tac/batch tra ve HTTP %s (request bi tu choi, khong phai fault ha tang) "
                "cho %d TAC. Body: %.1000s | tac_codes[0:3]=%s",
                res.status_code, len(chunk), res.text, chunk[:3],
            )
            for tac in chunk:
                chunk_results[tac] = ValidationResult(
                    is_valid=False,
                    error_code="ERR_EXTERNAL_REQUEST_REJECTED",
                    error_message=f"GSMA batch endpoint rejected request HTTP {res.status_code}: {res.text[:300]}",
                )

        return chunk_results

    for chunk_result in await _gather_chunks(chunks, _call_chunk):
        results.update(chunk_result)

    return results


async def fetch_hlr_batch(imsis: List[str], client: httpx.AsyncClient) -> Dict[str, ValidationResult]:
    """Tra cuu nhieu IMSI cung luc qua POST /subscribers/batch-lookup (HLR Mock).

    Khong dung Cache cho HLR (du lieu thue bao la "dong" -- SIM Swap, khoa
    thue bao co the xay ra bat ky luc nao), nen luon goi voi TOAN BO IMSI
    unique trong Batch hien tai. Tu dong chia nho theo HLR_BATCH_MAX
    (200 lookups/request).

    [FIX] Xem docstring cua fetch_gsma_batch -- ap dung nguyen tac tuong tu:
    fault ha tang (bao gom 5xx tu x-mock-fault) da duoc chan o tang
    invoke_external_api_with_resilience va tra ve qua nhanh `if err:` voi
    ma loi CHUNG; nhanh `else` ben duoi GIO CHI con nhan 4xx that.

    Args:
        imsis: danh sach IMSI can tra cuu, nen la list(set(...)).
        client: httpx.AsyncClient dung chung cho ca batch.

    Returns:
        Dict {imsi: ValidationResult}.
    """
    results: Dict[str, ValidationResult] = {}
    if not imsis:
        return results

    chunks = _chunked(imsis, HLR_BATCH_MAX)

    async def _call_chunk(chunk: List[str]) -> Dict[str, ValidationResult]:
        chunk_results: Dict[str, ValidationResult] = {}
        url = f"{HLR_HSS_SERVICE_URL}/subscribers/batch-lookup"
        lookups = [{"type": "imsi", "value": imsi} for imsi in chunk]
        res, err = await invoke_external_api_with_resilience(
            "HLR_HSS", client, "POST", url, json={"lookups": lookups}
        )

        # [FIX] Gop lam 1 nhanh duy nhat cho MOI loai fault ha tang
        # (timeout / conn-fail / 5xx tu x-mock-fault) -- truoc day tach
        # rieng ("ERR_EXTERNAL_TIMEOUT"/"ERR_EXTERNAL_CONN_FAIL") voi
        # "khong cache" con WARN_RULE_BYPASSED xu ly rieng; gio don gian
        # hoa: ca 2 truong hop nay deu la ket qua tu
        # invoke_external_api_with_resilience, khong can phan biet
        # "khong cache" nua vi HLR/ITU von di khong dung cache.
        if err == "WARN_RULE_BYPASSED":
            for imsi in chunk:
                chunk_results[imsi] = ValidationResult(is_valid=True, warn_code="WARN_RULE_BYPASSED")
            return chunk_results

        if err:
            # Fault ha tang CHUNG (bao gom ca 5xx do x-mock-fault, gio da
            # duoc invoke_external_api_with_resilience quy ve cung 1 loai
            # voi timeout/conn-fail that su o tang transport).
            for imsi in chunk:
                chunk_results[imsi] = ValidationResult(
                    is_valid=False, error_code=err,
                    error_message="HLR service unavailable (infrastructure fault)",
                )
            return chunk_results

        if res.status_code == 200:
            items = res.json().get("results", [])
            found_map = {
                it.get("query", {}).get("value"): it
                for it in items
                if it.get("query", {}).get("type") == "imsi"
            }

            for imsi in chunk:
                item = found_map.get(imsi)
                if item and item.get("found") and item.get("subscriber", {}).get("imsi") == imsi:
                    chunk_results[imsi] = ValidationResult(is_valid=True)
                else:
                    chunk_results[imsi] = ValidationResult(
                        is_valid=False, error_code="ERR_IMSI_NOT_IN_HLR", error_message="IMSI not found in HLR"
                    )
        else:
            # [FIX] Nhanh nay GIO CHI con nhan 4xx that (vd 422 validation
            # loi body). Doi ten ma loi thanh CHUNG, khong con
            # "_HLR_BATCH_INVALID" rieng theo service.
            logger.error(
                "HLR /subscribers/batch-lookup tra ve HTTP %s (request bi tu choi, khong phai fault ha tang) "
                "cho %d IMSI. Body: %.1000s | imsis[0:3]=%s",
                res.status_code, len(chunk), res.text, chunk[:3],
            )
            for imsi in chunk:
                chunk_results[imsi] = ValidationResult(
                    is_valid=False,
                    error_code="ERR_EXTERNAL_REQUEST_REJECTED",
                    error_message=f"HLR batch endpoint rejected request HTTP {res.status_code}: {res.text[:300]}",
                )

        return chunk_results

    for chunk_result in await _gather_chunks(chunks, _call_chunk):
        results.update(chunk_result)

    return results


async def fetch_itu_batch(msisdns: List[str], client: httpx.AsyncClient) -> Dict[str, ValidationResult]:
    """Validate nhieu MSISDN cung luc qua POST /validate/batch (ITU E.164 Mock).

    Khong dung Cache (giong HLR). Kiem tra regex E.164 local truoc de
    khong gui len API nhung so ro rang sai dinh dang; chi gui len Batch
    nhung so da qua duoc regex. Tu dong chia nho theo ITU_BATCH_MAX
    (100 numbers/request -- gioi han RIENG cua ITU, thap hon GSMA/HLR).

    [FIX] Xem docstring cua fetch_gsma_batch -- ap dung nguyen tac tuong
    tu: fault ha tang (bao gom 5xx tu x-mock-fault) da duoc chan o tang
    invoke_external_api_with_resilience va tra ve qua nhanh `if err:` voi
    ma loi CHUNG; nhanh `else` ben duoi GIO CHI con nhan 4xx that.

    Args:
        msisdns: danh sach MSISDN can validate, nen la list(set(...)).
        client: httpx.AsyncClient dung chung cho ca batch.

    Returns:
        Dict {msisdn: ValidationResult}.
    """
    results: Dict[str, ValidationResult] = {}
    if not msisdns:
        return results

    to_call: List[str] = []
    for msisdn in msisdns:
        if not re.match(r"^\+[1-9]\d{1,14}$", msisdn):
            results[msisdn] = ValidationResult(
                is_valid=False, error_code="ERR_INVALID_MSISDN", error_message="Violate E.164 regex"
            )
        else:
            to_call.append(msisdn)

    chunks = _chunked(to_call, ITU_BATCH_MAX)

    async def _call_chunk(chunk: List[str]) -> Dict[str, ValidationResult]:
        chunk_results: Dict[str, ValidationResult] = {}
        url = f"{ITU_E164_SERVICE_URL}/validate/batch"
        res, err = await invoke_external_api_with_resilience(
            "ITU_E164", client, "POST", url, json={"phone_numbers": chunk}
        )

        if err == "WARN_RULE_BYPASSED":
            for msisdn in chunk:
                chunk_results[msisdn] = ValidationResult(is_valid=True, warn_code="WARN_RULE_BYPASSED")
            return chunk_results

        if err:
            # Fault ha tang CHUNG (bao gom ca 5xx do x-mock-fault).
            for msisdn in chunk:
                chunk_results[msisdn] = ValidationResult(
                    is_valid=False, error_code=err, error_message="ITU service unavailable (infrastructure fault)"
                )
            return chunk_results

        # Mock ITU thuc te tra ve "results" la DICT keyed theo chinh so
        # dien thoai (giong cau truc /tac/batch cua GSMA):
        #   {"results": {"+84971234567": {"valid": true, ...}, ...},
        #    "total": N, "valid_count": X, "invalid_count": Y}
        if res.status_code == 200:
            payload = res.json()
            result_map: Dict[str, Any] = payload.get("results", {})

            for msisdn in chunk:
                item = result_map.get(msisdn)
                if item and item.get("valid"):
                    chunk_results[msisdn] = ValidationResult(is_valid=True)
                else:
                    detail = (
                        item.get("error_detail") if item else None
                    ) or "Invalid or missing in ITU batch result"
                    chunk_results[msisdn] = ValidationResult(
                        is_valid=False, error_code="ERR_INVALID_MSISDN", error_message=detail
                    )
        else:
            # [FIX] Nhanh nay GIO CHI con nhan 4xx that (vd 422
            # Unprocessable Entity vi request body sai schema). Doi ten
            # ma loi thanh CHUNG, khong con "_ITU_BATCH_INVALID" rieng
            # theo service.
            logger.error(
                "ITU /validate/batch tra ve HTTP %s (request bi tu choi, khong phai fault ha tang) "
                "cho %d so. Body: %.1000s | Payload da gui: phone_numbers[0:3]=%s",
                res.status_code, len(chunk), res.text, chunk[:3],
            )
            for msisdn in chunk:
                chunk_results[msisdn] = ValidationResult(
                    is_valid=False,
                    error_code="ERR_EXTERNAL_REQUEST_REJECTED",
                    error_message=f"ITU batch endpoint rejected request HTTP {res.status_code}: {res.text[:300]}",
                )

        return chunk_results

    for chunk_result in await _gather_chunks(chunks, _call_chunk):
        results.update(chunk_result)

    return results


def update_tac_cache(gsma_results: Optional[Dict[str, ValidationResult]]) -> None:
    """Nap ket qua GSMA batch vao TAC_CACHE (global, ton tai xuyen suot cac Batch)."""
    if not gsma_results:
        return
    TAC_CACHE.update(gsma_results)


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
async def validate_msisdn_format(
    record: Dict[str, Any],
    client: httpx.AsyncClient,
    itu_map: Optional[Dict[str, ValidationResult]] = None,
) -> ValidationResult:
    """R2: kiem tra format E.164 (regex local), sau do xac nhan country
    code + operator prefix + do dai subscriber number hop le.

    Neu `itu_map` duoc truyen vao (tu execute_validation_pipeline_batch,
    da prefetch bang fetch_itu_batch), rule se TRA CUU trong map nay
    thay vi tu goi API don le -- day la duong di khuyen dung khi chay
    theo Batch Spark.

    Neu khong co itu_map (goi don le / test rieng rule nay), rule van
    fallback ve cach cu: tu goi POST /validate cho 1 MSISDN.

    Returns:
        - ERR_INVALID_MSISDN neu regex fail HOAC ket qua tra ve valid=False.
        - WARN_RULE_BYPASSED (is_valid=True) neu circuit breaker mo.
        - ERR_EXTERNAL_* neu mock service loi ha tang.
        - is_valid=True neu hop le.
    """
    msisdn = str(record.get("msisdn", "")).strip()
    if not re.match(r"^\+[1-9]\d{1,14}$", msisdn):
        return ValidationResult(is_valid=False, error_code="ERR_INVALID_MSISDN", error_message="Violate E.164 regex")

    # --- Duong di MOI: tra cuu tu ket qua Batch da prefetch ---
    if itu_map is not None:
        return itu_map.get(
            msisdn,
            ValidationResult(is_valid=False, error_code="ERR_INVALID_MSISDN", error_message="Missing in batch result"),
        )

    # --- Duong di CU: goi API don le (fallback / tuong thich nguoc) ---
    url = f"{ITU_E164_SERVICE_URL}/validate"
    res, err = await invoke_external_api_with_resilience("ITU_E164", client, "POST", url, json={"phone_number": msisdn})

    if err == "WARN_RULE_BYPASSED":
        return ValidationResult(is_valid=True, warn_code="WARN_RULE_BYPASSED")
    if err:
        return ValidationResult(is_valid=False, error_code=err, error_message="ITU service unavailable")

    if res.status_code == 200:
        data = res.json()
        if data.get("valid"):
            return ValidationResult(is_valid=True)
    return ValidationResult(is_valid=False, error_code="ERR_INVALID_MSISDN", error_message="Invalid prefix/operator")


# ==============================================================================
# RULE 3: IMSI ton tai trong HLR
# ==============================================================================
async def validate_imsi_in_hlr(
    record: Dict[str, Any],
    client: httpx.AsyncClient,
    hlr_map: Optional[Dict[str, ValidationResult]] = None,
) -> ValidationResult:
    """R3: IMSI phai ton tai trong HLR.

    Neu `hlr_map` duoc truyen vao (tu execute_validation_pipeline_batch,
    da prefetch bang fetch_hlr_batch), rule se TRA CUU trong map nay
    thay vi tu goi API don le.

    Khong dung Cache cho rule nay (kem theo trong ca 2 duong di) vi du
    lieu thue bao la "dong" (SIM Swap, khoa thue bao...).
    """
    imsi = str(record.get("imsi", "")).strip()

    # --- Duong di MOI: tra cuu tu ket qua Batch da prefetch ---
    if hlr_map is not None:
        return hlr_map.get(
            imsi,
            ValidationResult(is_valid=False, error_code="ERR_IMSI_NOT_IN_HLR", error_message="Missing in batch result"),
        )

    # --- Duong di CU: goi API don le (fallback / tuong thich nguoc) ---
    url = f"{HLR_HSS_SERVICE_URL}/subscribers/by-imsi/{imsi}"
    res, err = await invoke_external_api_with_resilience("HLR_HSS", client, "GET", url)

    if err in ["ERR_EXTERNAL_TIMEOUT", "ERR_EXTERNAL_CONN_FAIL"]:
        return ValidationResult(
            is_valid=False,
            error_code=err,
            error_message="HLR service unavailable due to infrastructure error"
        )

    if err == "WARN_RULE_BYPASSED":
        return ValidationResult(is_valid=True, warn_code="WARN_RULE_BYPASSED")

    if res and res.status_code == 200:
        data = res.json()
        if data.get("imsi") == imsi:
            return ValidationResult(is_valid=True)

    return ValidationResult(
        is_valid=False,
        error_code="ERR_IMSI_NOT_IN_HLR",
        error_message="IMSI not found in HLR"
    )


# ==============================================================================
# RULE 4a: IMEI pass Luhn algorithm
# ==============================================================================
async def validate_imei_luhn(record: Dict[str, Any]) -> ValidationResult:
    imei = str(record.get("imei", "")).strip()
    if not imei.isdigit() or len(imei) != 15:
        return ValidationResult(is_valid=False, error_code="ERR_IMEI_LUHN_FAIL", error_message="IMEI must be 15 digits")

    # Dong bo voi Simulator (duyet tu phai qua trai)
    digits = [int(d) for d in imei]
    check_part = digits[:14]

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
    """R4b: TAC phai ton tai trong GSMA TAC DB.

    TAC la du lieu "tinh" (khong doi theo thoi gian) nen van dung
    TAC_CACHE (global) nhu truoc. Trong duong di Batch, TAC_CACHE da
    duoc nap san boi execute_validation_pipeline_batch() (goi
    fetch_gsma_batch roi update_tac_cache) TRUOC KHI vong lap validate
    tung record chay toi day -- nen o day luon se cache-hit neu TAC do
    da nam trong batch hien tai. Neu khong (vd goi rule nay don le,
    ngoai luong batch), no se tu fallback goi GET /tac/{tac} nhu cu.
    """
    imei = str(record.get("imei", "")).strip()
    tac = imei[:6]

    if tac in TAC_CACHE:
        return TAC_CACHE[tac]

    # Fallback: goi API don le (chi xay ra neu TAC nay chua tung duoc
    # prefetch -- vi du goi rule nay ngoai luong execute_validation_pipeline_batch)
    url = f"{GSMA_TAC_SERVICE_URL}/tac/{tac}"
    res, err = await invoke_external_api_with_resilience("GSMA_TAC", client, "GET", url)

    if err == "WARN_RULE_BYPASSED":
        return ValidationResult(is_valid=True, warn_code="WARN_RULE_BYPASSED")
    if err:
        return ValidationResult(is_valid=False, error_code=err, error_message="GSMA service unavailable")

    if res.status_code == 200:
        result = ValidationResult(is_valid=True)
        TAC_CACHE[tac] = result
        return result

    result = ValidationResult(is_valid=False, error_code="ERR_IMEI_TAC_UNKNOWN", error_message="Unknown TAC")
    TAC_CACHE[tac] = result
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
    R6: Kiem tra tinh hop le cua ca event_timestamp va ingest_timestamp.
    Chap nhan chuoi ISO (2026-03-29T07:12:00Z) hoac Unix timestamp.
    """

    def get_unix_timestamp(field_name: str) -> int:
        val = str(record.get(field_name, "")).strip()
        if not val:
            raise ValueError(f"{field_name} is empty")

        if "T" in val:
            dt = datetime.fromisoformat(val.replace('Z', '+00:00'))
            return int(dt.timestamp())
        else:
            return int(val)

    try:
        event_ts = get_unix_timestamp("event_timestamp")
        if not (946684800 <= event_ts <= 4102444800):
            return ValidationResult(
                is_valid=False,
                error_code="ERR_INVALID_TIMESTAMP",
                error_message="event_timestamp out of bounds"
            )

        get_unix_timestamp("ingest_timestamp")

        return ValidationResult(is_valid=True)

    except Exception as e:
        return ValidationResult(
            is_valid=False,
            error_code="ERR_INVALID_TIMESTAMP",
            error_message=f"Timestamp format error: {str(e)}"
        )


# ==============================================================================
# DONG CO DIEU PHOI CHUOI TUAN TU (ORCHESTRATOR) -- 1 RECORD
# ==============================================================================
async def execute_validation_pipeline(
    record: Dict[str, Any],
    client: httpx.AsyncClient,
    hlr_map: Optional[Dict[str, ValidationResult]] = None,
    itu_map: Optional[Dict[str, ValidationResult]] = None,
) -> Tuple[ValidationResult, Optional[str]]:
    """Chay tuan tu toan bo 6 Rules (R1 -> R2 -> R3 -> R4a -> R4b ->
    R5 -> R6). Gap rule dau tien fail -> dung ngay (fail-fast),
    KHONG chay cac rule sau.

    Neu `hlr_map` / `itu_map` duoc truyen vao (tu
    execute_validation_pipeline_batch), R3/R2 se tra cuu trong cac map
    nay thay vi tu goi API. Neu khong truyen (goi don le), moi rule tu
    fallback goi API rieng nhu truoc.

    Args:
        record: dict 1 row RADIUS (theo RAW_RADIUS_SCHEMA).
        client: httpx.AsyncClient dung chung cho ca batch.
        hlr_map: Dict {imsi: ValidationResult} da prefetch (optional).
        itu_map: Dict {msisdn: ValidationResult} da prefetch (optional).

    Returns:
        (ValidationResult, warn_code):
            - Neu mot rule fail: (ValidationResult cua rule do, None).
            - Neu tat ca pass: (ValidationResult(is_valid=True),
              warn_code cuoi cung neu co, None neu khong).
    """
    r1 = await validate_mandatory_fields(record)
    if not r1.is_valid:
        return r1, None

    accumulated_warn = None

    r2 = await validate_msisdn_format(record, client, itu_map=itu_map)
    if not r2.is_valid:
        return r2, None
    if r2.warn_code:
        accumulated_warn = r2.warn_code

    r3 = await validate_imsi_in_hlr(record, client, hlr_map=hlr_map)
    if not r3.is_valid:
        return r3, None
    if r3.warn_code:
        accumulated_warn = r3.warn_code

    r4a = await validate_imei_luhn(record)
    if not r4a.is_valid:
        return r4a, None

    r4b = await validate_imei_tac(record, client)
    if not r4b.is_valid:
        return r4b, None
    if r4b.warn_code:
        accumulated_warn = r4b.warn_code

    r5 = await validate_acct_status_type(record)
    if not r5.is_valid:
        return r5, None

    r6 = await validate_event_timestamp(record)
    if not r6.is_valid:
        return r6, None

    return ValidationResult(is_valid=True), accumulated_warn


# ==============================================================================
# DONG CO DIEU PHOI HYBRID PREFETCH -- CA BATCH (KHUYEN DUNG)
# ==============================================================================
async def execute_validation_pipeline_batch(
    records: List[Dict[str, Any]],
    client: httpx.AsyncClient,
) -> List[Tuple[ValidationResult, Optional[str]]]:
    """Validate ca mot Batch (vd 200 records tu Spark micro-batch) bang
    chien luoc "Hybrid Prefetch":

        - GSMA (TAC): chi fetch nhung TAC CHUA co trong TAC_CACHE
          (du lieu tinh -> dung Cache toan cuc, xuyen suot cac batch).
        - HLR (IMSI) va ITU (MSISDN): fetch TOAN BO unique
          IMSI/MSISDN trong batch nay, KHONG dung Cache (du lieu dong).
        - Ca 3 API duoc goi DONG THOI qua asyncio.gather.
        - Sau khi co ket qua, validate tuan tu tung record bang
          execute_validation_pipeline(), truyen hlr_map/itu_map de
          rule R2/R3 tra cuu thay vi goi API rieng; R4b tra cuu
          TAC_CACHE (da duoc nap boi buoc prefetch).

    Args:
        records: danh sach dict record RADIUS trong 1 micro-batch.
        client: httpx.AsyncClient dung chung cho ca batch.

    Returns:
        List[(ValidationResult, warn_code)] cung thu tu voi `records`.
    """
    if not records:
        return []

    # --- BUOC 1: XAC DINH DU LIEU CAN FETCH ---
    needed_tacs = list({
        str(r.get("imei", "")).strip()[:6]
        for r in records
        if str(r.get("imei", "")).strip()[:6] not in TAC_CACHE
    })
    needed_imsis = list({str(r.get("imsi", "")).strip() for r in records})
    needed_msisdns = list({str(r.get("msisdn", "")).strip() for r in records})

    # --- BUOC 2: GOI 3 API BATCH DONG THOI ---
    gsma_result, hlr_map, itu_map = await asyncio.gather(
        fetch_gsma_batch(needed_tacs, client),
        fetch_hlr_batch(needed_imsis, client),
        fetch_itu_batch(needed_msisdns, client),
    )

    # Nap ket qua GSMA vao TAC_CACHE (global) TRUOC KHI validate tung record
    update_tac_cache(gsma_result)

    # --- BUOC 3: VALIDATE TUAN TU TUNG RECORD, DUNG MAP DA PREFETCH ---
    results: List[Tuple[ValidationResult, Optional[str]]] = []
    for record in records:
        res = await execute_validation_pipeline(record, client, hlr_map=hlr_map, itu_map=itu_map)
        results.append(res)

    return results