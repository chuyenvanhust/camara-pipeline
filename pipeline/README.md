# pipeline/

Xử lý dữ liệu RADIUS Accounting qua 5 stage tuần tự,
từ raw Kafka topic đến PostgreSQL đã làm sạch.

## Kiến trúc 5 stage

```
CSV file
    │
    ▼ S1 – Ingestion
Kafka: radius.raw
    │
    ▼ S2 – Validation  ──────────────────► radius.invalid
Kafka: radius.valid          (ERR_MISSING_FIELD, ERR_INVALID_IMEI…)
    │
    ▼ S3 – Deduplication ───────────────► duplicate_log (PostgreSQL)
Kafka: radius.dedup
    │
    ▼ S4 – Conflict Resolution ─────────► conflict_log + swap_event (PostgreSQL)
Kafka: radius.clean
    │
    ▼ S5 – Storage Insert
PostgreSQL: radius_sessions
```

## Files

| File | Vai trò |
|------|---------|
| `run_pipeline.py` | Entry point: khởi động cả 5 stage theo thứ tự, handle graceful shutdown |
| `base_job.py` | Base class `SparkStreamingJob`: khởi tạo SparkSession, cấu hình checkpoint, metrics |

## Chạy pipeline

```bash
# Toàn bộ pipeline (sau khi Kafka và PostgreSQL đã up)
python pipeline/run_pipeline.py --input data/radius_log.csv

# Chạy riêng từng stage để debug
python pipeline/ingestion/producer.py   --file data/radius_log.csv
python pipeline/validation/validator.py
python pipeline/deduplication/dedup_job.py
python pipeline/conflict_resolution/resolver.py
python pipeline/storage/writer.py
```

## Phụ thuộc external

Stage S2 (Validation) gọi 3 mock services qua HTTP:

| Rule | Mock Service | Endpoint |
|------|-------------|----------|
| R2 – MSISDN | ITU E.164 Mock `:8300` | `POST /validate` |
| R3 – IMSI | HLR/HSS Mock `:8200` | `GET /subscribers/by-imsi/{imsi}` |
| R4 – IMEI TAC | GSMA TAC Mock `:8100` | `GET /tac/{tac_code}` |

Stage S4 (Conflict Resolution) gọi HLR/HSS mock để xác nhận lịch sử SIM Swap:

| Bước | Mock Service | Endpoint |
|------|-------------|----------|
| Xác nhận swap | HLR/HSS Mock `:8200` | `GET /subscribers/{msisdn}/imsi-history` |

**Tất cả mock services phải đang chạy trước khi khởi động pipeline.**

## Throughput mục tiêu

≥ 5.000 records/giây end-to-end (Kafka ingest → PostgreSQL insert).
Theo dõi tại Spark UI: http://localhost:4040

## Sub-modules

- [`ingestion/`](ingestion/README.md)
- [`validation/`](validation/README.md)
- [`deduplication/`](deduplication/README.md)
- [`conflict_resolution/`](conflict_resolution/README.md)
- [`storage/`](storage/README.md)
