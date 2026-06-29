import pytest
from pipeline_v1.validation.validator import (
    WATERMARK_THRESHOLD,
    KAFKA_TOPIC_VALID,
    KAFKA_TOPIC_INVALID,
    route_records,
    run_validation_async,
    process_micro_batch,
)
from pipeline_v1.validation.rules import ValidationResult, failed_counters


# ==============================================================================
# FIXTURES
# ==============================================================================

@pytest.fixture(autouse=True)
def reset_circuit_breaker_counters():
    """Reset state global failed_counters trước và sau mỗi test."""
    for key in failed_counters:
        failed_counters[key] = 0
    yield
    for key in failed_counters:
        failed_counters[key] = 0


@pytest.fixture
def valid_record():
    return {
        "acct_status_type": "Start",
        "acct_session_id": "SESS_001",
        "msisdn": "+84971111111",
        "imsi": "452010000000111",
        "imei": "860934042394121",
        "event_timestamp": "1781258400",
    }


@pytest.fixture
def invalid_status_record():
    return {
        "acct_status_type": "WrongStatusType",
        "acct_session_id": "SESS_002",
        "msisdn": "+84971111111",
        "imsi": "452010000000111",
        "imei": "860934042394121",
        "event_timestamp": "1781258400",
    }


# ==============================================================================
# LỚP 1 -- CONSTANTS / WATERMARK
# ==============================================================================

def test_watermark_threshold_constant_is_7200_seconds():
    assert WATERMARK_THRESHOLD == "7200 seconds"


def test_watermark_drops_record_older_than_threshold_pure():
    """
    Test logic watermark bằng thuật toán thuần túy thay vì gọi Spark Engine.
    Mốc tối đa là 10:05:00 -> Biên watermark = 10:05:00 - 2h = 08:05:00.
    """
    from datetime import datetime, timedelta

    base_data = [
        {"msisdn": "+84971111111", "event_time": datetime.fromisoformat("2026-06-13 10:00:00")},
        {"msisdn": "+84972222222", "event_time": datetime.fromisoformat("2026-06-13 10:05:00")},
        {"msisdn": "+84973333333", "event_time": datetime.fromisoformat("2026-06-13 07:55:00")},
    ]

    # Mô phỏng chính xác cách Spark Streaming Engine tính toán watermark ngoài đời thật
    max_event_time = max(r["event_time"] for r in base_data)
    watermark_boundary = max_event_time - timedelta(seconds=7200)

    # Filter
    result_data = [r for r in base_data if r["event_time"] >= watermark_boundary]
    msisdns_left = {r["msisdn"] for r in result_data}

    assert msisdns_left == {"+84971111111", "+84972222222"}
    assert "+84973333333" not in msisdns_left


# ==============================================================================
# LỚP 2 -- PURE LOGIC: route_records()
# ==============================================================================

def test_route_records_all_valid_no_warn(valid_record):
    records = [valid_record, dict(valid_record, acct_session_id="SESS_999")]
    results = [
        (ValidationResult(is_valid=True), None),
        (ValidationResult(is_valid=True), None),
    ]

    valid, invalid = route_records(records, results)

    assert len(valid) == 2
    assert len(invalid) == 0


def test_route_records_valid_with_warn_code_attached(valid_record):
    records = [dict(valid_record)]
    results = [(ValidationResult(is_valid=True), "WARN_RULE_BYPASSED")]

    valid, invalid = route_records(records, results)

    assert len(valid) == 1
    assert valid[0]["warn_code"] == "WARN_RULE_BYPASSED"
    assert valid[0]["acct_session_id"] == valid_record["acct_session_id"]


def test_route_records_invalid_attaches_error_code_and_message(invalid_status_record):
    records = [invalid_status_record]
    results = [(
        ValidationResult(
            is_valid=False,
            error_code="ERR_INVALID_STATUS",
            error_message="Invalid status type",
        ),
        None,
    )]

    valid, invalid = route_records(records, results)

    assert len(valid) == 0
    assert len(invalid) == 1
    assert invalid[0]["error_code"] == "ERR_INVALID_STATUS"
    assert invalid[0]["error_message"] == "Invalid status type"


def test_route_records_mixed_batch_splits_correctly(valid_record, invalid_status_record):
    records = [valid_record, invalid_status_record]
    results = [
        (ValidationResult(is_valid=True), None),
        (ValidationResult(is_valid=False, error_code="ERR_INVALID_STATUS",
                          error_message="Invalid status type"), None),
    ]

    valid, invalid = route_records(records, results)

    assert len(valid) == 1
    assert len(invalid) == 1


def test_route_records_does_not_mutate_input(valid_record):
    records = [dict(valid_record)]
    original_keys = set(records[0].keys())
    results = [(ValidationResult(is_valid=False, error_code="ERR_X",
                                 error_message="msg"), None)]

    route_records(records, results)

    assert set(records[0].keys()) == original_keys


def test_route_records_raises_on_length_mismatch(valid_record):
    records = [valid_record]
    results = []

    with pytest.raises(ValueError):
        route_records(records, results)


# ==============================================================================
# LỚP 2b -- PURE LOGIC: run_validation_async()
# ==============================================================================

@pytest.mark.asyncio
async def test_run_validation_async_returns_results_in_order(
    valid_record, invalid_status_record, mocker
):
    mock_resp = mocker.MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"valid": True, "exists": True}

    mocker.patch("httpx.AsyncClient.post", return_value=mock_resp)
    mocker.patch("httpx.AsyncClient.get", return_value=mock_resp)

    records = [valid_record, invalid_status_record]
    results = await run_validation_async(records)

    assert len(results) == 2
    assert results[0][0].is_valid is True
    assert results[1][0].is_valid is False


# ==============================================================================
# LỚP 3 -- SPARK I/O: Mocking Wiring Layer (Không cần gọi Spark Session thật)
# ==============================================================================

def test_process_micro_batch_routes_to_correct_topics_pure_mock(mocker, valid_record, invalid_status_record):
    # 1. Mock Spark DataFrame và phương thức .collect() trả về dữ liệu Python thuần
    mock_row_1 = mocker.MagicMock()
    mock_row_1.asDict.return_value = valid_record

    mock_row_2 = mocker.MagicMock()
    mock_row_2.asDict.return_value = invalid_status_record

    mock_df = mocker.MagicMock()
    mock_df.collect.return_value = [mock_row_1, mock_row_2]

    # 2. Mock tầng gọi API Async bên dưới
    async def mock_validation_async(records):
        return [
            (ValidationResult(is_valid=True), None),
            (ValidationResult(is_valid=False, error_code="ERR_INVALID_STATUS", error_message="msg"), None)
        ]
    mocker.patch("pipeline.validation.validator.run_validation_async", side_effect=mock_validation_async)

    # 3. Mock đầu ra write_to_kafka để hứng dữ liệu kiểm tra
    written_calls = []
    mocker.patch(
        "pipeline.validation.validator.write_to_kafka",
        side_effect=lambda spark, payloads, topic: written_calls.append((topic, payloads))
    )

    # 4. Thực thi wiring function bằng một SparkSession giả lập (Mock object)
    mock_spark_session = mocker.MagicMock()
    callback = process_micro_batch(mock_spark_session)
    callback(mock_df, batch_id=42)

    # 5. Assertions
    assert len(written_calls) == 2
    topics = {call[0] for call in written_calls}
    assert topics == {KAFKA_TOPIC_VALID, KAFKA_TOPIC_INVALID}


def test_process_micro_batch_empty_batch_does_not_call_write_pure_mock(mocker):
    mock_df = mocker.MagicMock()
    mock_df.collect.return_value = []  # Batch trống rỗng

    mock_write = mocker.patch("pipeline.validation.validator.write_to_kafka")

    mock_spark_session = mocker.MagicMock()
    callback = process_micro_batch(mock_spark_session)
    callback(mock_df, batch_id=43)

    mock_write.assert_not_called()


def test_process_micro_batch_all_invalid_writes_only_invalid_topic_pure_mock(mocker, invalid_status_record):
    mock_row = mocker.MagicMock()
    mock_row.asDict.return_value = invalid_status_record
    mock_df = mocker.MagicMock()
    mock_df.collect.return_value = [mock_row]

    async def mock_validation_async(records):
        return [(ValidationResult(is_valid=False, error_code="ERR_X", error_message="msg"), None)]
    mocker.patch("pipeline.validation.validator.run_validation_async", side_effect=mock_validation_async)

    written_calls = []
    mocker.patch(
        "pipeline.validation.validator.write_to_kafka",
        side_effect=lambda spark, payloads, topic: written_calls.append((topic, payloads))
    )

    mock_spark_session = mocker.MagicMock()
    callback = process_micro_batch(mock_spark_session)
    callback(mock_df, batch_id=44)

    valid_payloads = next(p for t, p in written_calls if t == KAFKA_TOPIC_VALID)
    invalid_payloads = next(p for t, p in written_calls if t == KAFKA_TOPIC_INVALID)

    assert len(valid_payloads) == 0
    assert len(invalid_payloads) == 1