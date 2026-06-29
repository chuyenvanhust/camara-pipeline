# pipeline/deduplication/

Stage S3 — phát hiện và loại bỏ record trùng lặp, gồm 2 lớp độc lập:

## Lớp 1 — Fast path (Spark, module này)

Sliding window 1 giờ, dùng RocksDB state. Record đầu tiên giữ lại;
bản sao đến trong vòng 1h bị drop, ghi vào `duplicate_log`.

### Files
| File | Vai trò |
|------|---------|
| `dedup_job.py` | Spark Structured Streaming job: stateful dedup với RocksDB state store |
| `state_manager.py` | Quản lý RocksDB state: định nghĩa key schema, TTL, cleanup expired state |

### Dedup key (fast path)
key = (acct_session_id, acct_status_type)
*(Trước đây tài liệu này ghi nhầm là 3 field gồm cả `event_timestamp`
— đã sửa lại cho khớp `DEDUP_KEY_FIELDS` trong state_manager.py.
`event_timestamp` chỉ dùng để tính khoảng cách thời gian giữa 2 record
cùng key, không phải một phần của key.)*

Nếu key đã tồn tại trong state store và khoảng cách thời gian ≤ TTL
→ duplicate → drop. TTL mỗi key = 3.600s (1 giờ).

### Trade-off đã biết (ADR-004)
Duplicate đến sau 1 giờ so với bản gốc sẽ **không** bị lớp fast path
này phát hiện — chấp nhận được vì đã có lớp backstop (xem dưới) xử lý
trường hợp này, nên trade-off không còn là một gap thật của hệ thống.

## Lớp 2 — Long-term backstop (Postgres trigger)

`storage/migrations/004_dedup_trigger.sql` — trigger `BEFORE INSERT`
trên `radius_sessions`, dùng chính bảng này (lưu lịch sử đầy đủ) làm
long-term storage. Bắt duplicate với cùng `(acct_session_id,
acct_status_type)` bất kể đến muộn bao lâu, không phụ thuộc Spark/Kafka
có đang chạy hay không.

Giới hạn đã biết: scan không giới hạn theo thời gian trong toàn bộ
`radius_sessions` còn tồn tại — nếu partition cũ bị archive/drop sau
này, duplicate quá cũ so với partition còn lại sẽ không còn được bắt.
Chưa cần xử lý ở scope hiện tại.

## Output
- `radius.dedup` — records không trùng (qua fast path)
- `duplicate_log` (PostgreSQL) — ghi nhận từ cả 2 lớp, phân biệt qua
  cột `reason` (vd: từ fast path vs `LATE_DUPLICATE_LONG_TERM` từ trigger)