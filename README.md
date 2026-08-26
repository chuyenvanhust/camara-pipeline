# CAMARA Network API Data Pipeline

> **Lab / Miniproject** — Data Pipeline phục vụ CAMARA Network API
> (SIM Swap · Device Swap · Number Verification)
> từ dữ liệu **GGSN RADIUS Accounting Request** (RFC 2866 + 3GPP TS 29.061 VSA)

Dự án mô phỏng luồng dữ liệu mạng di động thật: file CSV RADIUS accounting được nạp vào
Kafka, 3 consumer module xử lý song song để phát hiện sự kiện đổi SIM / đổi máy / thay đổi
IP-MSISDN, ghi state + lịch sử vào PostgreSQL/Redis, rồi expose qua FastAPI theo chuẩn
CAMARA Network API. Callback tới bên thứ 3 được xử lý qua outbox pattern độc lập với hot path.

---

## 0. Trạng thái dự án & lịch sử kiến trúc

**Kiến trúc đang chạy (active)**: 3 Kafka consumer module thuần Python/asyncio (mô tả ở
mục 1). **Không** dùng Spark.

Trước đây dự án dùng Spark Streaming 5-stage (Ingestion → Validation → Deduplication →
Conflict Resolution → Storage) cùng 3 mock service ngoại vi (gsma_tac, itu_e164, hlr_hss).
Kiến trúc đó đã bị loại bỏ vì quá nặng so với quy mô dữ liệu lab (~10K record), khó debug,
và không khớp với thực tế là RADIUS accounting đã có sẵn đường Kafka ingest. Lý do đầy đủ
nằm ở [`docs/adr/0001-drop-spark-use-kafka-consumer.md`](docs/adr/0001-drop-spark-use-kafka-consumer.md).

**Về các thư mục còn sót lại trong `pipeline/`:**

| Thư mục | Trạng thái | Được import bởi runtime? |
|---|---|---|
| `pipeline/ingestion/` | ✅ **Đang dùng** | Có — `run_pipeline.py` import `RadiusLogProducer` từ đây để nạp CSV vào Kafka (Stage 1) |
| `pipeline/modules/` | ✅ **Đang dùng** | Có — toàn bộ logic 3 consumer module |
| `pipeline/dispatcher/` | ✅ **Đang dùng** | Chạy độc lập (`python -m pipeline.dispatcher.notification_dispatcher`), không nằm trong `run_pipeline.py` |
| `pipeline/validation/`, `deduplication/`, `conflict_resolution/`, `processing/`, `state/`, `storage/` | ❌ **Skeleton chết** | Không — chỉ còn `__init__.py` rỗng (hoặc file mồ côi như `state/redis_state_manager.py`, `storage/models.py`) từ kiến trúc Spark cũ, không được import ở bất kỳ đâu trong `pipeline/run_pipeline.py` hay `pipeline/modules/` |

> Lưu ý sửa so với tài liệu cũ: một phiên bản README trước đây liệt kê nhầm
> `pipeline/ingestion/` vào nhóm "skeleton không dùng nữa" — thực tế module này vẫn là
> đường ingest CSV→Kafka duy nhất của pipeline. Bảng trên đã xác minh lại bằng cách grep
> import thực tế trong code, không dựa trên tên thư mục.

`mock_services/` (3 mock API cũ) đã bị xoá hoàn toàn khỏi repo.

---

## 1. Kiến trúc tổng thể

```
CSV RADIUS log
      │
      ▼  (pipeline/ingestion — Stage 1, tuỳ chọn, chỉ chạy khi có --input)
Kafka topic: radius.accounting.raw (4 partitions, key=msisdn)
      │
      ├──────────────┬──────────────┬──────────────┐
      ▼              ▼              ▼
cg-ip-msisdn    cg-device-swap   cg-sim-swap      (3 Kafka consumer group,
      │              │              │              chạy song song trong 1 process)
      ▼              ▼              ▼
   Redis          Redis+Postgres  Redis+Postgres
 (ip-ggsn:*)    (msisdn_device,   (msisdn_sim,
                 device_swap_     sim_swap_
                 history,         history,
                 audit_log,       audit_log,
                 notification_    notification_
                 log)             log)
                       │              │
                       └──────┬───────┘
                              ▼
              pipeline/dispatcher (process riêng biệt)
              Poll notification_log → HTTP callback → Open Gateway subscriber
                              │
                              ▼
                    FastAPI (api/) — CAMARA endpoints
                    /sim-swap, /device-swap, /number-verification
```

**Nguyên tắc thiết kế cốt lõi:**

- **1 process, 3 consumer task song song** — không phải 3 process riêng biệt, để dùng
  chung 1 connection pool Postgres (`shared_db` trong `run_pipeline.py`) thay vì mỗi
  consumer tự mở pool.
- **Kafka offset chỉ commit sau khi batch xử lý xong** (manual commit, `enable_auto_commit=False`)
  — tắt lỗi mất dữ liệu khi consumer crash giữa chừng.
- **Ghi DB theo transaction đơn** cho mỗi batch: state hiện tại + lịch sử + audit log +
  notification outbox cùng commit hoặc cùng rollback — không bao giờ có state mới mà
  thiếu history tương ứng.
- **Consumer không gọi HTTP** — mọi callback ra ngoài (Open Gateway) chỉ ghi vào bảng
  `notification_log` với status `PENDING`, một process `dispatcher` riêng biệt mới thực sự
  gửi HTTP. Nhờ vậy 1 subscriber chậm/down không bao giờ làm nghẽn throughput Kafka consumer.
- **Redis là cache có thể tái tạo lại**, không phải nguồn sự thật — chỉ update Redis
  **sau khi** Postgres commit thành công.

Chi tiết từng thành phần: xem [`pipeline/README.md`](pipeline/README.md) và README riêng
của từng module trong `pipeline/modules/*/README.md`.

---

## 2. Cấu trúc thư mục

```
.
├── api/                    # FastAPI — CAMARA Network API endpoints (xem api/README.md)
├── pipeline/                # Toàn bộ logic xử lý dữ liệu — xem pipeline/README.md
│   ├── ingestion/            # Stage 1: CSV → Kafka
│   ├── modules/               # 3 consumer module + code dùng chung
│   │   ├── shared/              # BaseKafkaConsumer, DatabasePool, metrics
│   │   ├── ip_msisdn/            # Module 1: IP↔MSISDN mapping (Redis-only)
│   │   ├── device_swap/          # Module 2: phát hiện đổi IMEI
│   │   └── sim_swap/             # Module 3: phát hiện đổi IMSI
│   ├── dispatcher/            # Notification outbox dispatcher (process riêng)
│   └── run_pipeline.py         # Orchestrator: khởi động 3 consumer + (tuỳ chọn) ingest CSV
├── simulator/               # Sinh dữ liệu RADIUS accounting giả lập
├── storage/                 # SQL schema + migrations Postgres
├── infra/                  # Prometheus, Grafana, docker network config
├── reporting/               # Script/báo cáo phân tích dữ liệu sau xử lý
├── tests/                  # Unit + integration test
├── scripts/                 # Script vận hành (run_pipeline.sh, seed, v.v.)
├── docs/                   # ADR và tài liệu kiến trúc
├── docker-compose.yml
└── DE_NGHI_SUA_CHUA_GO_LIVE.md   # Đánh giá go-live nội bộ (F-01 → F-20)
```

Mỗi thư mục có README riêng — xem [`api/README.md`](api/README.md),
[`simulator/README.md`](simulator/README.md), [`storage/README.md`](storage/README.md),
[`infra/README.md`](infra/README.md), [`scripts/README.md`](scripts/README.md),
[`tests/integration/README.md`](tests/integration/README.md).

---

## 3. Yêu cầu hệ thống

- Docker + Docker Compose
- Python 3.11+ (nếu chạy ngoài container để dev/debug)
- ~4GB RAM rảnh cho Kafka + PostgreSQL + Redis + Zookeeper trong Docker

---

## 4. Khởi động nhanh

```bash
# 1. Copy file env mẫu và chỉnh nếu cần
cp .env.example .env   # nếu chưa có .env.example, xem storage/README.md và docker-compose.yml

# 2. Dựng toàn bộ hạ tầng (Kafka, Postgres, Redis, Prometheus, Grafana)
docker compose up -d

# 3. Chạy migration Postgres
bash scripts/run_pipeline.sh   # hoặc chạy tay các file storage/migrations/*.sql theo thứ tự

# 4. Sinh dữ liệu RADIUS giả lập (tuỳ chọn — xem simulator/README.md)
python simulator/simulator.py --output data/radius_sample.csv --records 10000

# 5. Chạy pipeline, nạp CSV và xử lý
python -m pipeline.run_pipeline --input data/radius_sample.csv

# 6. Chạy dispatcher gửi callback (process riêng biệt, bắt buộc nếu cần notification thật)
python -m pipeline.dispatcher.notification_dispatcher

# 7. Chạy API
uvicorn api.main:app --reload
```

Xem chi tiết biến môi trường ở mục 6 và trong từng README con.

---

## 5. Go-live readiness

Dự án đã trải qua một vòng tự-review go-live nội bộ, tài liệu đầy đủ ở
[`DE_NGHI_SUA_CHUA_GO_LIVE.md`](DE_NGHI_SUA_CHUA_GO_LIVE.md) (20 mục F-01 → F-20, phân loại
P0/P1/P2). Tóm tắt trạng thái hiện tại:

- **Các mục P0 (chặn go-live)** — đã sửa và có thể kiểm chứng trực tiếp trong code: manual
  offset commit + DLQ (F-01), ghi DB atomic bằng transaction (F-02), tách callback khỏi hot
  path qua outbox (F-03), Kafka producer `acks=all` + idempotence (F-04), Redis
  `maxmemory-policy=noeviction` (F-07), Prometheus metrics thật (F-08), tách liveness/readiness
  + cấm secret mặc định ở production (F-12).
- **Còn treo (P1/P2)**: chưa có benchmark batch-write thật trên phần cứng production (F-10),
  chưa có retention/partition job cho `audit_log`/`*_history` (F-11 — mới có index), migration
  runner vẫn là shell script không version-tracking (F-13).

Trước khi go-live thật, bắt buộc chạy load test (`load_tests/*.js`, k6) nhắm vào môi trường
giống production và đo lại benchmark batch-write — repo hiện **chưa có kết quả benchmark nào
được lưu lại**, các con số hiện tại chỉ là tính toán lý thuyết.

---

## 6. Cấu hình & biến môi trường chính

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `camara-kafka:9092` | Địa chỉ Kafka broker |
| `KAFKA_TOPIC_RAW` | `radius.accounting.raw` | Topic chứa RADIUS accounting record thô |
| `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME` | xem `docker-compose.yml` | Kết nối Postgres |
| `REDIS_HOST`, `REDIS_PORT`, `REDIS_DB` | `camara-redis`, `6379`, `0` | Kết nối Redis |
| `BATCH_MAX_RECORDS` | `500` | Số record tối đa gom trong 1 batch trước khi flush |
| `BATCH_TIMEOUT_MS` | `100` | Thời gian chờ tối đa trước khi flush batch dở |
| `MAX_BATCH_RETRIES` | `3` | Số lần retry 1 batch lỗi trước khi đẩy vào topic `.dlq` |
| `DB_POOL_MIN` / `DB_POOL_MAX` | `4` / `12` | Kích thước pool Postgres dùng chung cho cả 3 consumer |
| `METRICS_PORT` | `9200` | Cổng expose `/metrics` Prometheus của pipeline |
| `DISPATCHER_BATCH_SIZE` / `DISPATCHER_POLL_INTERVAL` / `DISPATCHER_MAX_ATTEMPTS` | `50` / `2.0` / `5` | Cấu hình notification dispatcher |

Danh sách đầy đủ và biến riêng của từng thành phần: xem README con tương ứng
<<<<<<< ours
(`pipeline/README.md`, `pipeline/modules/*/README.md`, `api/README.md`).
=======
(`pipeline/README.md`, `pipeline/modules/*/README.md`, `api/README.md`).
>>>>>>> theirs
