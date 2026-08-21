# ADR 0001: Bỏ Spark Streaming, dùng 3 Kafka Consumer Modules

- **Trạng thái**: Accepted
- **Ngày**: 2026-08-21
- **Quyết định bởi**: Project owner

---

## Bối cảnh

`BUILD_ORDER.md` (phiên bản cũ) mô tả kiến trúc pipeline dùng **Spark Streaming** với 5 stage:

```
S1 Ingestion (CSV → Kafka) → S2 Validation (rules + Spark) → S3 Deduplication
(RocksDB) → S4 Conflict Resolution → S5 Storage (PostgreSQL)
```

Cùng với 3 mock services (`gsma_tac`, `itu_e164`, `hlr_hss`) chạy ở port 8100/8200/8300 phục vụ
validation rules ở stage S2.

## Quyết định

Chuyển sang kiến trúc **3 Kafka consumer modules thuần** (Python asyncio + aiokafka):

- `cg-ip-msisdn` → cache IP↔MSISDN↔GGSN trên Redis
- `cg-device-swap` → phát hiện IMEI change, ghi `msisdn_device` + `device_swap_history`
- `cg-sim-swap` → phát hiện IMSI change, ghi `msisdn_sim` + `sim_swap_history`

Cả 3 modules cùng subscribe `radius.accounting.raw` và chạy song song.

## Lý do

| Tiêu chí | Spark Streaming (cũ) | Kafka Consumer (mới) |
|---|---|---|
| **Độ phức tạp vận hành** | Spark cluster + RocksDB State Store + 3 mock services | 1 Python container + Redis + Postgres |
| **Thời gian khởi động** | ~30s (Spark context + JVM) | <2s (asyncio consumer) |
| **Debug cycle** | Phải restart Spark job, log khó đọc | Python traceback trực tiếp, có thể `pdb` |
| **Phù hợp với data scale** | Overkill cho ~10K records/giờ | Đủ và headroom cho 100K+ records/giờ |
| **Mock services** | 3 mock APIs (gsma_tac, itu_e164, hlr_hss) — chưa bao giờ chạy production | Không cần — dùng data thực từ simulator |
| **Alignment với RADIUS flow** | Spark micro-batch không tự nhiên với RADIUS accounting | Kafka consumer là pattern chuẩn cho RADIUS ingest |
| **Code size** | ~3000 LOC (Spark + RocksDB + mock services) | ~1200 LOC (3 consumer modules) |

## Hệ quả

### Đã xoá

- `mock_services/{gsma_tac,itu_e164,hlr_hss,shared}/` — toàn bộ mock API stack
- `pipeline/pipeline/` (nested) — Spark code cũ
- `storage/migrations/{002_indexes.sql,003_partitions.sql,004_dedup_trigger.sql}` — partition
  & trigger không còn cần vì không có Spark job
- `spark/Dockerfile` — không còn Spark container
- `pipeline/spark_jars.py` — JAR loader không cần

### Đã thêm

- `pipeline/modules/{ip_msisdn,device_swap,sim_swap,shared}/` — 3 consumer modules
  với `base_consumer.py` async chung
- `pipeline/Dockerfile` — container mới cho pipeline service
- Batch processing optimization ở `process_batch()` — Redis MGET + Postgres batch SELECT
  + batch UPSERT/COPY INSERT + Redis MSET, giảm round-trips xuống còn ~5/batch

### Đã giữ (skeleton)

- `pipeline/{ingestion,validation,deduplication,conflict_resolution,processing,state,storage}/`
  — folders rỗng từ refactor dang dở, **không được import** bởi `pipeline/run_pipeline.py`.
  Giữ tạm để không phải sửa các file tham chiếu cũ, sẽ xoá ở commit kế tiếp.

## Tham khảo

- `BUILD_ORDER.md` (lỗi thời) — tài liệu cũ mô tả kiến trúc Spark
- `README.md` section 0 — mô tả kiến trúc hiện tại
- Commit xoá Spark: xem `git log --diff-filter=D -- pipeline/pipeline/`
