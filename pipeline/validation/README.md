# pipeline/validation/

Stage S2 — áp dụng 6 validation rules trên mỗi record.
Record hợp lệ → `radius.valid`. Record lỗi → `radius.invalid` + ghi `invalid_log`.

## Files

| File | Vai trò |
|------|---------|
| `validator.py` | Spark Structured Streaming job: consume `radius.raw`, chạy rules, route output |
| `rules.py` | 6 pure async functions R1–R6, mỗi function trả `ValidationResult` |

## 6 Rules

| Rule | Kiểm tra | Gọi mock service | Error code |
|------|----------|-----------------|------------|
| R1 | Mandatory fields không null | Không | `ERR_MISSING_FIELD` |
| R2 | MSISDN format E.164 + operator prefix hợp lệ | ITU E.164 `:8300` `POST /validate` | `ERR_INVALID_MSISDN` |
| R3 | IMSI tồn tại trong HLR | HLR/HSS `:8200` `GET /subscribers/by-imsi/{imsi}` | `ERR_IMSI_NOT_IN_HLR` |
| R4a | IMEI pass Luhn algorithm | Không | `ERR_IMEI_LUHN_FAIL` |
| R4b | TAC (6 chữ số đầu IMEI) có trong GSMA TAC DB | GSMA TAC `:8100` `GET /tac/{tac}` | `ERR_IMEI_TAC_UNKNOWN` |
| R5 | `acct_status_type` ∈ {Start, Stop, Interim-Update} | Không | `ERR_INVALID_STATUS` |
| R6 | `event_timestamp` trong khoảng hợp lệ | Không | `ERR_INVALID_TIMESTAMP` |

Rules được áp dụng tuần tự. Record dừng tại rule đầu tiên fail — không tiếp tục check rule sau.

## HTTP Client cho mock services

`rules.py` dùng `httpx.AsyncClient` với:
- `timeout=2.0s` — nếu mock service không trả lời trong 2s → `ERR_EXTERNAL_TIMEOUT`
- `retry=2` lần với exponential backoff
- Circuit breaker: nếu mock service liên tục fail → bypass rule đó, ghi `WARN_RULE_BYPASSED`

## Watermark

Spark watermark = `2 × LATE_ARRIVAL_THRESHOLD_SECONDS` = 7.200s.
Record có `event_timestamp` quá cũ so với watermark bị drop trước khi vào validation.
