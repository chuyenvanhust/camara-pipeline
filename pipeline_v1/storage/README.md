# pipeline/storage/

Stage S5 — ghi records đã làm sạch từ Kafka `radius.clean` vào PostgreSQL.

## Files

| File | Vai trò |
|------|---------|
| `writer.py` | Spark micro-batch JDBC writer: commit mỗi 30s, upsert vào `radius_sessions` |
| `models.py` | SQLAlchemy ORM models cho 5 bảng: `radius_sessions`, `swap_event`, `duplicate_log`, `conflict_log`, `invalid_log` |

## Cơ chế ghi

Spark Structured Streaming với `trigger(processingTime='30 seconds')`.
Mỗi micro-batch dùng JDBC bulk insert (`executemany`) thay vì insert từng row.
Conflict với record đã tồn tại (retry sau crash): `INSERT ... ON CONFLICT DO NOTHING`.

## Bảng ghi

| Bảng | Ghi từ stage | Ghi khi nào |
|------|-------------|------------|
| `radius_sessions` | S5 | Mọi record qua `radius.clean` |
| `swap_event` | S4 | Khi `swap_detector` emit conflict C |
| `duplicate_log` | S3 | Khi dedup drop record |
| `conflict_log` | S4 | Khi resolver phân loại conflict A/B/C |
| `invalid_log` | S2 | Khi validation fail rule bất kỳ |

## Config

```
DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD  ← từ .env
SPARK_JDBC_BATCH_SIZE=1000
SPARK_COMMIT_INTERVAL_SECONDS=30
```
