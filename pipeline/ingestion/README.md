# pipeline/ingestion/

Stage S1 — đọc CSV và publish từng record vào Kafka topic `radius.raw`.

## Files

| File | Vai trò |
|------|---------|
| `producer.py` | Kafka Producer: đọc CSV theo batch, serialize sang JSON, publish với partition key `hash(acct_session_id)` |
| `csv_reader.py` | Parse CSV → `dict`, validate schema header, yield theo batch configurable |

## Luồng xử lý

```
data/radius_log.csv
    │
    ▼ csv_reader.py (batch_size=500)
[ {record}, {record}, ... ]   ← 500 records / batch
    │
    ▼ producer.py
Kafka topic: radius.raw
  partition key = hash(acct_session_id)
  value         = JSON string của record
```

## Partition key

`hash(acct_session_id)` đảm bảo tất cả record của cùng 1 session
(Start, Interim, Stop) luôn vào cùng 1 Kafka partition.
Điều này cần thiết để Stage S3 (deduplication) và S4 (conflict resolution)
có thể xử lý stateful trên cùng partition mà không cần shuffle.

## Config

Đọc từ `.env`:

```
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_RAW=radius.raw
INGESTION_BATCH_SIZE=500
INGESTION_LINGER_MS=10
```
