# tests/unit/pipeline/validation/

Unit & integration test cho `pipeline/validation/rules.py` và
`pipeline/validation/validator.py`.

## Files

| File | Test gì |
|------|---------|
| `test_rules.py` | 6 validation rules (R1–R6) + orchestrator `execute_validation_pipeline` |
| `test_validator.py` | Constants/watermark, pure logic `route_records`/`run_validation_async`, Spark I/O `process_micro_batch` |

---

## Chạy test

```bash
# Toàn bộ
pytest tests/unit/pipeline/validation -v

# Chỉ rules.py
pytest tests/unit/pipeline/validation/test_rules.py -v

# Chỉ validator.py
pytest tests/unit/pipeline/validation/test_validator.py -v
```

Yêu cầu: `pytest-asyncio`, `pytest-mock`, `pyspark`, `httpx`.

---

## ⚠ Yêu cầu môi trường: PYSPARK_PYTHON (Windows)

PySpark mặc định spawn worker bằng executable `python3`. Trên Windows
(cài Python qua python.org), không có `python3.exe` — chỉ có
`python.exe` — dẫn tới lỗi:

```
java.io.IOException: Cannot run program "python3":
CreateProcess error=2, The system cannot find the file specified
```

**Fix:** `tests/conftest.py` (root) set `PYSPARK_PYTHON` và
`PYSPARK_DRIVER_PYTHON` = `sys.executable` **trước khi import pyspark**.
Không cần làm gì thêm khi chạy `pytest` — conftest tự áp dụng cho
toàn bộ suite. Linux/Mac không bị ảnh hưởng (giá trị mặc định đã đúng).

---

## test_rules.py — chi tiết

### IMEI dùng trong fixture: `"860934042394121"`

Đây là IMEI 15 số **pass Luhn checksum** theo đúng thuật toán trong
`validate_imei_luhn()`. Cách tính:

```
14 số đầu:  86093404239412
Áp Luhn (nhân đôi vị trí lẻ, trừ 9 nếu > 9):
  sum = 69
  check digit = (10 - (69 % 10)) % 10 = 1
=> IMEI hợp lệ = 86093404239412 + "1" = 860934042394121
```

Nếu sửa `validate_imei_luhn()` (đổi thuật toán, đổi độ dài IMEI...),
**phải tính lại IMEI fixture** và update test — `test_r4a_imei_valid_luhn_passes`
sẽ fail đầu tiên nếu fixture không còn hợp lệ, đóng vai trò canary.

### Phân nhóm test

| Test | Loại | Mục đích |
|------|------|---------|
| `test_r1_missing_fields_detects_missing_imsi_and_session_id` | Unit | R1 báo đúng field thiếu đầu tiên |
| `test_r1_all_fields_present_passes` | Unit | R1 pass khi đủ field |
| `test_r4a_imei_valid_luhn_passes` | Unit | Canary — xác nhận IMEI fixture hợp lệ |
| `test_r4a_imei_luhn_algorithm_fail` | Unit | Check digit sai → `ERR_IMEI_LUHN_FAIL` |
| `test_r4a_imei_wrong_length_fails` | Unit | IMEI không đủ 15 số |
| `test_pipeline_orchestrator_happy_path` | Integration | Toàn bộ 6 rule pass |
| `test_pipeline_orchestrator_fail_fast_at_r2` | Integration | R2 fail → R3 (HTTP GET) không được gọi |
| `test_pipeline_orchestrator_fail_fast_at_r5_after_r4` | Integration | Fail tại rule không cần network (R5) |
| `test_pipeline_circuit_breaker_tripped_and_bypassed` | Integration | Breaker mở → R2 bypass, record vẫn valid |
| `test_pipeline_orchestrator_external_timeout_fails_record` | Integration | Breaker đóng (lần đầu fail) → record fail, KHÔNG bypass |

### Phân biệt quan trọng: Circuit Breaker mở vs đóng

| Trạng thái breaker | Hành vi rule | is_valid | warn_code |
|-------------------|-------------|----------|-----------|
| Đóng, lần đầu timeout | Trả lỗi thật | `False` | `None` (error_code=`ERR_EXTERNAL_TIMEOUT`) |
| Mở (≥ `CIRCUIT_BREAKER_LIMIT` lần fail) | Bypass, không gọi network | `True` | `"WARN_RULE_BYPASSED"` |

`reset_circuit_breaker_counters` (autouse fixture) reset `failed_counters`
trước **và sau** mỗi test, đảm bảo state global không leak giữa các test
trong cùng file lẫn sang `test_validator.py`.

---

## test_validator.py — chi tiết

### Refactor `validator.py` để test được đúng

So với bản gốc, `validator.py` được tách thành 3 lớp:

```
CONSTANTS        RAW_RADIUS_SCHEMA, EVENT_TIME_COLUMN,
                 WATERMARK_THRESHOLD, KAFKA_TOPIC_*
                 → test import trực tiếp, không hard-code lại

PURE LOGIC       run_validation_async(records) -> results
                 route_records(records, results) -> (valid, invalid)
                 → test bằng list[dict] thuần, không cần Spark/Kafka

SPARK I/O        write_to_kafka(spark, payloads, topic)
                 process_micro_batch(spark) -> callback(df, batch_id)
                 build_watermarked_stream(spark) -> DataFrame
                 → test với SparkSession thật, patch write_to_kafka
```

**Lý do tách:** bản gốc dùng `SparkSession.activeSession` (sai —
phải là `getActiveSession()`, và kể cả vậy vẫn không an toàn trong
`foreachBatch` context) và mock `pyspark.sql.DataFrame.write` qua
`new_property` (mong manh, dễ vỡ khi đổi Spark version, có thể
không patch đúng instance DataFrame được tạo bên trong hàm).

`process_micro_batch(spark)` giờ là **factory**: nhận `spark` qua
closure tại thời điểm đăng ký `foreachBatch`, trả về callback đúng
signature `(df, batch_id) -> None`. `write_to_kafka()` là hàm Python
thuần do module tự định nghĩa — patch
`pipeline.validation.validator.write_to_kafka` ổn định, không phụ
thuộc internal API của Spark.

### Phân nhóm test

| Test | Lớp | Mục đích |
|------|-----|---------|
| `test_watermark_threshold_constant_is_7200_seconds` | Constants | Khẳng định giá trị watermark = spec |
| `test_watermark_drops_record_older_than_threshold` | Constants + Spark | Watermark dùng **constant thật** từ validator.py, không phải số copy-paste |
| `test_route_records_all_valid_no_warn` | Pure logic | 2 record valid → cả 2 vào valid, không key thừa |
| `test_route_records_valid_with_warn_code_attached` | Pure logic | warn_code được gắn đúng vào payload valid |
| `test_route_records_invalid_attaches_error_code_and_message` | Pure logic | error_code/error_message gắn đúng vào payload invalid |
| `test_route_records_mixed_batch_splits_correctly` | Pure logic | Batch hỗn hợp, đúng record đi đúng nhánh |
| `test_route_records_does_not_mutate_input` | Pure logic | Không side-effect lên `records` gốc |
| `test_route_records_raises_on_length_mismatch` | Pure logic | Bảo vệ khỏi lỗi `zip()` âm thầm cắt ngắn |
| `test_run_validation_async_returns_results_in_order` | Pure logic + mock httpx | `asyncio.gather` giữ đúng thứ tự |
| `test_process_micro_batch_routes_to_correct_topics` | Spark I/O | End-to-end: batch → routing đúng → `write_to_kafka` gọi đúng 2 lần với đúng topic/payload |
| `test_process_micro_batch_empty_batch_does_not_call_write` | Spark I/O | Batch rỗng → return sớm, không gọi write |
| `test_process_micro_batch_all_invalid_writes_only_invalid_topic` | Spark I/O | valid_payloads rỗng, invalid_payloads đúng nội dung |

### Vì sao watermark test giờ "đúng luồng" hơn

Bản gốc: test tự build lại watermark logic **giống** `main()`, hard-code
`"7200 seconds"` — nếu `validator.py` đổi watermark mà quên sửa test,
test vẫn pass (false confidence).

Bản hiện tại: `test_watermark_drops_record_older_than_threshold` import
`EVENT_TIME_COLUMN` và `WATERMARK_THRESHOLD` trực tiếp từ
`validator.py` để build DataFrame test. Đổi watermark trong code →
test tự dùng giá trị mới → nếu giá trị mới làm sai kỳ vọng dữ liệu
mẫu, test fail đúng cách. `test_watermark_threshold_constant_is_7200_seconds`
là lớp bảo vệ thứ 2, khẳng định giá trị cụ thể khớp spec README.

### Giới hạn còn lại

`build_watermarked_stream()` (đọc Kafka thật) **không** được test trực
tiếp trong unit test này — cần Kafka chạy để test streaming source.
Phần này thuộc về integration test full-stack (`tests/pipeline/`,
xem root `tests/README.md`), không phải unit test.