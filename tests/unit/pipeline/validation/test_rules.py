import pytest
import httpx

from pipeline_v1.validation.rules import (
    validate_mandatory_fields,
    validate_imei_luhn,
    execute_validation_pipeline,
    ValidationResult,
    failed_counters,
    CIRCUIT_BREAKER_LIMIT,
)


# ==============================================================================
# FIXTURES
# ==============================================================================

@pytest.fixture
def happy_path_record():
    """Bản ghi mẫu hoàn hảo, vượt qua tất cả các lớp phòng thủ."""
    return {
        "acct_status_type": "Start",
        "acct_session_id": "SESS_OK_123",
        "msisdn": "+84971111111",
        "imsi": "452010000000111",
        "imei": "860934042394121",  # IMEI 15 số, pass Luhn (xem docstring module)
        "event_timestamp": "1781258400",  # năm 2026, hợp lệ trong R6
    }


@pytest.fixture(autouse=True)
def reset_circuit_breaker_counters():
    """Tự động reset bộ đếm lỗi của Circuit Breaker trước mỗi test case,
    tránh test sau bị ảnh hưởng bởi state global do test trước để lại."""
    for key in failed_counters:
        failed_counters[key] = 0
    yield
    for key in failed_counters:
        failed_counters[key] = 0


@pytest.fixture
def mock_external_ok(mocker):
    """
    Tạo mock httpx.AsyncClient trả 200 cho cả get() và post(),
    với payload {"valid": True, "exists": True} -- đủ để pass R2, R3, R4b.

    Trả về (mock_client, mock_response) để test có thể tùy biến
    mock_response.json.return_value cho từng rule riêng nếu cần.
    """
    mock_client = mocker.MagicMock(spec=httpx.AsyncClient)
    mock_resp = mocker.MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"valid": True, "exists": True}
    mock_client.get.return_value = mock_resp
    mock_client.post.return_value = mock_resp
    return mock_client, mock_resp


# ==============================================================================
# UNIT TESTS -- TỪNG RULE ĐỘC LẬP (KHÔNG CẦN NETWORK)
# ==============================================================================

@pytest.mark.asyncio
async def test_r1_missing_fields_detects_missing_imsi_and_session_id():
    """R1: thiếu acct_session_id và imsi -> ERR_MISSING_FIELD,
    báo đúng field bị thiếu đầu tiên theo thứ tự mandatory_fields."""
    bad_record = {
        "acct_status_type": "Start",
        "msisdn": "+84971111111",
        "imei": "860934042394121",
        "event_timestamp": "1781258400",
    }
    res = await validate_mandatory_fields(bad_record)
    assert res.is_valid is False
    assert res.error_code == "ERR_MISSING_FIELD"
    # acct_session_id đứng trước imsi trong mandatory_fields -> báo trước
    assert "acct_session_id" in res.error_message


@pytest.mark.asyncio
async def test_r1_all_fields_present_passes(happy_path_record):
    """R1: record đủ field -> pass, is_valid=True, không có error_code."""
    res = await validate_mandatory_fields(happy_path_record)
    assert res.is_valid is True
    assert res.error_code is None


@pytest.mark.asyncio
async def test_r4a_imei_valid_luhn_passes(happy_path_record):
    """R4a: IMEI "860934042394121" phải PASS Luhn (đây là điều kiện
    tiên quyết để các test orchestrator phía dưới đi hết được R4a)."""
    res = await validate_imei_luhn(happy_path_record)
    assert res.is_valid is True


@pytest.mark.asyncio
async def test_r4a_imei_luhn_algorithm_fail():
    """R4a: đổi 1 chữ số cuối -> check digit sai -> ERR_IMEI_LUHN_FAIL."""
    bad_record = {"imei": "860934042394120"}  # check digit sai (đúng phải là 1)
    res = await validate_imei_luhn(bad_record)
    assert res.is_valid is False
    assert res.error_code == "ERR_IMEI_LUHN_FAIL"


@pytest.mark.asyncio
async def test_r4a_imei_wrong_length_fails():
    """R4a: IMEI không đủ 15 số -> ERR_IMEI_LUHN_FAIL (fail sớm trước
    khi chạy thuật toán Luhn)."""
    res = await validate_imei_luhn({"imei": "12345"})
    assert res.is_valid is False
    assert res.error_code == "ERR_IMEI_LUHN_FAIL"


# ==============================================================================
# INTEGRATION TESTS -- ORCHESTRATOR (execute_validation_pipeline)
# ==============================================================================

@pytest.mark.asyncio
async def test_pipeline_orchestrator_happy_path(happy_path_record, mock_external_ok):
    """Kịch bản hoàn hảo: cả 3 mock service trả 200 + valid/exists=True,
    record pass toàn bộ 6 rule -> is_valid=True, không có warn."""
    mock_client, _ = mock_external_ok

    res, warn = await execute_validation_pipeline(happy_path_record, mock_client)

    assert res.is_valid is True
    assert res.error_code is None
    assert warn is None


@pytest.mark.asyncio
async def test_pipeline_orchestrator_fail_fast_at_r2(happy_path_record, mocker):
    """
    Fail-fast: R2 (ITU E.164) trả valid=False -> dừng ngay tại R2,
    KHÔNG gọi tiếp R3 (HLR, dùng client.get).

    Đây là test quan trọng nhất cho thiết kế "fail-fast" -- nếu sau
    này ai đó đổi pipeline_rules thành chạy song song / không dừng
    sớm, test này sẽ catch được qua assert_not_called().
    """
    mock_client = mocker.MagicMock(spec=httpx.AsyncClient)

    mock_resp_r2_fail = mocker.MagicMock()
    mock_resp_r2_fail.status_code = 200
    mock_resp_r2_fail.json.return_value = {"valid": False, "reason": "Prefix Operator Unknown"}
    mock_client.post.return_value = mock_resp_r2_fail

    res, _ = await execute_validation_pipeline(happy_path_record, mock_client)

    assert res.is_valid is False
    assert res.error_code == "ERR_INVALID_MSISDN"

    # R3 (validate_imsi_in_hlr) gọi client.get -- không được gọi vì
    # pipeline đã dừng tại R2.
    mock_client.get.assert_not_called()


@pytest.mark.asyncio
async def test_pipeline_orchestrator_fail_fast_at_r5_after_r4(happy_path_record, mock_external_ok):
    """
    Fail-fast ở rule không cần network (R5): record có
    acct_status_type sai -> dừng tại R5, R6 (cũng không cần network)
    không được kiểm tra -- res trả về đúng error_code của R5,
    không bị R6 override.
    """
    mock_client, _ = mock_external_ok
    bad_record = dict(happy_path_record)
    bad_record["acct_status_type"] = "WrongStatusType"

    res, _ = await execute_validation_pipeline(bad_record, mock_client)

    assert res.is_valid is False
    assert res.error_code == "ERR_INVALID_STATUS"


@pytest.mark.asyncio
async def test_pipeline_circuit_breaker_tripped_and_bypassed(happy_path_record, mocker):
    """
    Circuit Breaker: failed_counters["ITU_E164"] đã đạt
    CIRCUIT_BREAKER_LIMIT (5) trước khi chạy -> R2 bị bypass
    (is_valid=True, warn_code=WARN_RULE_BYPASSED), KHÔNG gọi
    client.post (vì invoke_external_api_with_resilience return
    sớm trước khi gọi network).

    Pipeline tiếp tục chạy R3 (HLR via get) bình thường và overall
    is_valid=True nhờ R2 được bypass thay vì fail.
    """
    mock_client = mocker.MagicMock(spec=httpx.AsyncClient)

    # Set sẵn breaker đã "open" cho ITU_E164
    failed_counters["ITU_E164"] = CIRCUIT_BREAKER_LIMIT

    mock_resp_ok = mocker.MagicMock()
    mock_resp_ok.status_code = 200
    mock_resp_ok.json.return_value = {"exists": True}
    mock_client.get.return_value = mock_resp_ok

    res, warn = await execute_validation_pipeline(happy_path_record, mock_client)

    assert res.is_valid is True
    assert warn == "WARN_RULE_BYPASSED"
    # R2 bị bypass trước khi gọi network -> post không được gọi
    mock_client.post.assert_not_called()
    # R3 vẫn chạy bình thường -> get được gọi (ít nhất 1 lần cho R3,
    # có thể thêm 1 lần nữa cho R4b nếu GSMA_TAC breaker đóng)
    mock_client.get.assert_called()


@pytest.mark.asyncio
async def test_pipeline_orchestrator_external_timeout_fails_record(happy_path_record, mocker):
    """
    Khi mock service timeout liên tục (hết retry) và circuit breaker
    CHƯA mở (lần đầu tiên gặp lỗi) -> rule trả is_valid=False với
    error_code=ERR_EXTERNAL_TIMEOUT, record bị coi là invalid
    (không phải warn/bypass).

    Phân biệt rõ với test circuit breaker ở trên: breaker "đóng"
    (lần đầu fail) -> record FAIL; breaker "mở" (đã fail >= 5 lần
    trước đó) -> record được BYPASS (valid).
    """
    mock_client = mocker.MagicMock(spec=httpx.AsyncClient)
    mock_client.post.side_effect = httpx.TimeoutException("timeout")

    res, warn = await execute_validation_pipeline(happy_path_record, mock_client)

    assert res.is_valid is False
    assert res.error_code == "ERR_EXTERNAL_TIMEOUT"
    assert warn is None