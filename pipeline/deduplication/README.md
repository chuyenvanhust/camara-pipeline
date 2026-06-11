# pipeline/deduplication/

Stage S3 — phát hiện và loại bỏ record trùng lặp trong sliding window 1 giờ.
Record đầu tiên được giữ lại; các bản sao sau bị drop và ghi vào `duplicate_log`.

## Files

| File | Vai trò |
|------|---------|
| `dedup_job.py` | Spark Structured Streaming job: stateful dedup với RocksDB state store |
| `state_manager.py` | Quản lý RocksDB state: định nghĩa key schema, TTL, cleanup expired state |

## Dedup key

```
key = (acct_session_id, acct_status_type, event_timestamp)
```

Nếu key đã tồn tại trong state store → record là duplicate → drop.
Nếu key chưa tồn tại → ghi vào state store với TTL = window size → forward.

## State store

Spark RocksDB (`RocksDBStateStoreProvider`).
State được persist tại `SPARK_CHECKPOINT_DIR/dedup/`.
TTL của mỗi key = 3.600s (1 giờ) — bằng `LATE_ARRIVAL_THRESHOLD_SECONDS`.

## Window

Sliding window 1 giờ tính theo `event_timestamp` (không phải `ingest_timestamp`).
Duplicate đến sau 1 giờ so với bản gốc sẽ không bị phát hiện —
đây là trade-off chấp nhận được (xem ADR-004).

## Output

- `radius.dedup` — records không trùng lặp
- `duplicate_log` (PostgreSQL) — bản ghi bị drop kèm `original_session_id`, `duplicate_count`
