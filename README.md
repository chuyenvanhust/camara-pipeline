# CAMARA Network API Data Pipeline

> **Lab / Miniproject** — Data Pipeline phục vụ CAMARA Network API
> (SIM Swap · Device Swap · Number Verification)
> từ dữ liệu **GGSN RADIUS Accounting Request** (RFC 2866 + 3GPP TS 29.061 VSA)

> **⚠️ Trạng thái dự án — tài liệu lịch sử**
>
> Tài liệu này mô tả **kiến trúc hiện tại** (Kafka consumer modules, không dùng Spark).
>
> - `BUILD_ORDER.md` đã **lỗi thời**: Phase 4 mô tả Spark Streaming 5-stage đã được thay bằng
>   kiến trúc 3 Kafka consumer modules. Xem `docs/adr/0001-drop-spark-use-kafka-consumer.md`
>   để biết lý do và so sánh kiến trúc.
> - Working tree hiện tại có các folder `pipeline/{ingestion,validation,deduplication,
>   conflict_resolution,processing,state,storage}/` ở root — đây là skeleton/legacy
>   từ kiến trúc Spark cũ và **không còn được dùng** bởi runtime hiện tại
>   (`pipeline/run_pipeline.py` chỉ import từ `pipeline/modules/...`).
> - Mock services (`mock_services/`) đã được loại bỏ hoàn toàn.

---

## Mục lục

0. [Trạng thái dự án & lịch sử kiến trúc](#0-trạng-thái-dự-án--lịch-sử-kiến-trúc)
1. [Kiến trúc tổng thể](#1-kiến-trúc-tổng-thể)
2. [Cấu trúc thư mục](#2-cấu-trúc-thư-mục)
3. [Yêu cầu hệ thống](#3-yêu-cầu-hệ-thống)
4. [Khởi động nhanh](#4-khởi-động-nhanh)
5. [Chi tiết pipeline các module](#5-chi-tiết-pipeline-các-module)
6. [Storage layer](#6-storage-layer)
7. [CAMARA API](#7-camara-api)
8. [Cấu hình & biến môi trường](#8-cấu-hình--biến-môi-trường)

---

## 0. Trạng thái dự án & lịch sử kiến trúc

### Kiến trúc hiện tại (active)

```
Simulator (CSV) ──▶ Kafka topic: radius.accounting.raw
                          │
            ┌─────────────┼─────────────┐
            ▼             ▼             ▼
       cg-ip-msisdn  cg-device-swap  cg-sim-swap
            │             │             │
            ▼             ▼             ▼
         Redis        PostgreSQL ──── Open Gateway
                       (history)       (callback)
                            │
                            ▼
                  FastAPI CAMARA endpoints
                  /sim-swap, /device-swap, /number-verification
```

- **3 Kafka consumer modules** chạy song song, mỗi module xử lý 1 loại sự kiện swap
- **Redis** dùng cho state cache nóng (`ip-ggsn:*`, `sim:*`, `device:*`)
- **PostgreSQL** lưu state hiện tại + lịch sử swap + audit/notification log
- **Open Gateway callback** được dispatch qua bảng `subscription` + retry queue

### Kiến trúc cũ (deprecated)

Trước đây dự án dùng **Spark Streaming** với 5 stage (Ingestion → Validation → Deduplication → Conflict Resolution → Storage) và 3 mock services (gsma_tac, itu_e164, hlr_hss).

Kiến trúc này đã được **loại bỏ** vì:

- **Quá nặng cho lab/miniproject**: Spark + RocksDB State Store + 3 mock services phức tạp
  không tương xứng với quy mô dữ liệu giả lập (~10K records)
- **Mock services chưa bao giờ chạy production-ready**: chỉ dùng cho unit test
- **Khó debug**: mỗi lần sửa rule phải restart Spark job, log khó đọc
- **Không align với RADIUS data flow**: RADIUS accounting thực tế đã có Kafka ingest sẵn,
  nên pipeline chỉ cần consumer modules thuần

Xem chi tiết trong [`docs/adr/0001-drop-spark-use-kafka-consumer.md`](docs/adr/0001-drop-spark-use-kafka-consumer.md).

### Trạng thái các thư mục cũ

| Folder | Trạng thái | Ghi chú |
|---|---|---|
| `mock_services/` | ❌ Đã xoá | Không còn mock API nào trong stack |
| `pipeline/pipeline/` | ❌ Đã xoá | Spark code cũ |
| `pipeline/ingestion/`, `validation/`, `deduplication/`, `conflict_resolution/`, `processing/`, `state/`, `storage/` | ⚠️ Skeleton (rỗng) | Stubs từ refactor — không import, không chạy |
| `storage/migrations/002_indexes.sql`, `003_partitions.sql`, `004_dedup_trigger.sql` | ❌ Đã xoá | Chỉ `001_init_schema.sql` còn dùng |
| `spark/Dockerfile` | ❌ Đã xoá | Spark container không còn build |

---

## 1. Kiến trúc tổng thể

```
┌───────────────────────────────────────────────────────────────────────────┐
│                        CAMARA Pipeline – Lab                              │
│                                                                           │
│  Simulator (RADIUS Log Generator)                                         │
│       │                                                                   │
│       ▼                                                                   │
│  Kafka Topic: radius.accounting.raw                                       │
│       │                                                                   │
│       ├───────────────────────┼───────────────────────┐                   │
│       ▼                       ▼                       ▼                   │
│  ┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐          │
│  │ Module IP-MSISDN│   │ Module Device   │   │ Module SIM Swap │          │
│  │   Processing    │   │ Swap Processing │   │   Processing    │          │
│  │ (cg-ip-msisdn)  │   │(cg-device-swap) │   │  (cg-sim-swap)  │          │
│  └────────┬────────┘   └────────┬────────┘   └────────┬────────┘          │
│           │                     │                     │                   │
│           ▼                     ▼                     ▼                   │
│     ┌───────────┐         ┌───────────┐         ┌───────────┐             │
│     │   Redis   │         │PostgreSQL │         │OpenGateway│             │
│     │   Cache   │         │ Database  │         │ Callbacks │             │
│     └─────┬─────┘         └─────┬─────┘         └───────────┘             │
│           │                     │                                         │
│           └──────────┬──────────┘                                         │
│                      ▼                                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                     CAMARA API LAYER                                │  │
│  │  FastAPI :8000 (X-API-Key)                                          │  │
│  │  ├── /sim-swap/v0        ──┐                                        │  │
│  │  ├── /device-swap/v0     ──┼──▶ Query PostgreSQL / Redis            │  │
│  │  └── /number-verification/v0 ─┘                                     │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                           │
│  ┌───────────────────────┐                                                │
│  │ Prometheus + Grafana  │                                                │
│  │ :9090 / :3000         │                                                │
│  └───────────────────────┘                                                │
└───────────────────────────────────────────────────────────────────────────┘
```

### Các module xử lý dữ liệu

| Consumer Group | Module | Input Topic | Chức năng chính | Output / Destination |
|---|---|---|---|---|
| `cg-ip-msisdn` | Module IP-MSISDN Processing | `radius.accounting.raw` | Duy trì bảng ánh xạ IP–MSISDN–GGSN phục vụ truy vấn realtime | Redis (`ip-ggsn:<framed_ip>`, `ggsn-ips:<nas_id>`) |
| `cg-device-swap` | Module Device Swap Processing | `radius.accounting.raw` | Phát hiện đổi thiết bị (IMEI change), cập nhật DB & cache, gửi callback Open Gateway | PostgreSQL (`msisdn_device`, `device_swap_history`, `audit_log`), Redis, Webhook Callback |
| `cg-sim-swap` | Module SIM Swap Processing | `radius.accounting.raw` | Phát hiện đổi SIM (IMSI change), cập nhật DB & cache, gửi callback Open Gateway | PostgreSQL (`msisdn_sim`, `sim_swap_history`, `audit_log`), Redis, Webhook Callback |

---

## 2. Cấu trúc thư mục

```
camara-pipeline/
│
├── simulator/                       # RADIUS Log Simulator (Offline & Deterministic)
│   ├── simulator.py                 # Entry point CLI
│   ├── generators.py                # Sinh MSISDN/IMSI/IMEI/TAC offline
│   ├── error_injectors.py           # Inject late arrival/invalid IMEI/conflict/missing field
│   ├── config.py                    # Cấu hình SimulatorConfig
│   └── README.md                    # Hướng dẫn sử dụng simulator
│
├── pipeline/                        # Pipeline Processing Core (Kafka consumer-based)
│   ├── run_pipeline.py              # Orchestrator điều phối 3 modules song song
│   ├── Dockerfile                   # Container Image cho pipeline service
│   └── modules/
│       ├── shared/                  # Shared base consumer, database pool, metrics, notification
│       │   ├── base_consumer.py     # BaseKafkaConsumer — asyncio consumer + batch processing
│       │   ├── db.py                # asyncpg pool + batch UPSERT/INSERT
│       │   ├── notification.py      # Open Gateway callback dispatcher
│       │   └── metrics.py           # Prometheus counters
│       ├── ip_msisdn/               # Module IP-MSISDN Processing (cg-ip-msisdn)
│       │   ├── consumer.py          # Redis cache IP↔MSISDN↔GGSN
│       │   └── redis_store.py       # Redis operations
│       ├── device_swap/             # Module Device Swap Processing (cg-device-swap)
│       │   ├── consumer.py          # Phát hiện IMEI change
│       │   └── notifier.py          # Subscription lookup + callback
│       └── sim_swap/                # Module SIM Swap Processing (cg-sim-swap)
│           ├── consumer.py          # Phát hiện IMSI change
│           └── notifier.py          # Subscription lookup + callback
│
│   # ⚠️ Skeleton từ kiến trúc Spark cũ — KHÔNG import bởi runtime hiện tại
│   ├── ingestion/                   # (empty — kept for backward ref)
│   ├── validation/                  # (empty)
│   ├── deduplication/               # (empty)
│   ├── conflict_resolution/         # (empty)
│   ├── processing/                  # (empty)
│   ├── state/                       # (empty)
│   └── storage/                     # (empty)
│
├── api/                             # CAMARA Network API Layer (FastAPI)
│   ├── main.py                      # App factory, lifespan, exception handlers
│   ├── config.py                    # Settings (env-driven)
│   ├── Dockerfile                   # Container image cho API service
│   ├── README.md                    # Hướng dẫn sử dụng API
│   ├── routers/                     # FastAPI routers
│   │   ├── sim_swap.py              # POST /sim-swap/v0/{check,retrieve-date}
│   │   ├── device_swap.py           # POST /device-swap/v0/{check,retrieve-date}
│   │   ├── number_verification.py   # POST /number-verification/v0/verify
│   │   └── health.py                # GET /health
│   ├── schemas/                     # Pydantic request/response models
│   │   ├── common.py                # ErrorResponse, Page
│   │   ├── sim_swap.py
│   │   ├── device_swap.py
│   │   └── number_verification.py
│   └── dependencies/                # FastAPI dependencies
│       ├── auth.py                  # X-API-Key validation
│       └── database.py              # asyncpg pool injection
│
├── storage/                         # Database Schema & Migrations
│   └── migrations/
│       └── 001_init_schema.sql      # Toàn bộ schema PostgreSQL (7 tables)
│
├── reporting/                       # Data Quality Report (HTML)
│   ├── metrics_collector.py         # Thu thập số liệu từ audit_log
│   ├── quality_report.py            # Sinh HTML report
│   ├── templates/
│   │   └── report.html.jinja2       # Jinja2 template cho HTML output
│   └── README.md
│
├── infra/                           # Prometheus + Grafana config
│   ├── prometheus/
│   │   └── prometheus.yml
│   ├── grafana/
│   │   ├── dashboards/              # Pre-built dashboards
│   │   └── provisioning/            # Datasource + dashboard provisioning
│   └── kafka/                       # Kafka client config
│
├── shared/                          # Shared utilities dùng bởi nhiều module
│   └── seed_config.py               # Centralized seed values (HLR/HSS, simulator, ...)
│
├── tests/                           # Pytest test suite
│   └── (test_*.py theo marker: happy_path, edge_case, sim_swap, ...)
│
├── load_tests/                      # k6 load test scripts
│   └── (k6 scripts cho 3 API endpoints)
│
├── scripts/                         # Wrapper bash scripts
│   ├── run.sh                       # Master entry: up / sim / pipeline / down / logs / reset
│   ├── run_simulator.sh             # Sinh CSV → data/radius_log.csv
│   ├── run_pipeline.sh              # Chạy pipeline container
│   ├── run_load_test.sh             # Chạy k6 load test
│   ├── generate_report.sh           # Sinh quality report
│   └── reset_db.sh                  # Reset DB + re-apply migrations
│
├── data/                            # Generated CSV logs (gitignored)
├── reports/                         # Generated HTML reports (gitignored)
│
├── docs/                            # Tài liệu
│   ├── openapi/                     # OpenAPI specs
│   └── adr/                         # Architecture Decision Records
│       └── 0001-drop-spark-use-kafka-consumer.md
│
├── docker-compose.yml               # Core stack: zookeeper, kafka, postgres, redis,
│                                    # fastapi, pipeline, prometheus, grafana
├── docker-compose.test.yml          # Test stack riêng
├── Makefile                         # Quick commands: up, sim, pipeline, load-test, report
├── pyproject.toml                   # Pytest config + markers
├── requirements.txt                 # Python dependencies
├── BUILD_ORDER.md                   # ⚠️ LỖI THỜI — xem section 0
└── README.md                        # File này
```

---

## 3. Yêu cầu hệ thống

| Công cụ | Phiên bản |
|---|---|
| Docker | ≥ 24.0 |
| Docker Compose | ≥ 2.20 (plugin) |
| Python | ≥ 3.11 |
| RAM | ≥ 8 GB |
| Disk | ≥ 5 GB |
| CPU | ≥ 4 core |

---

## 4. Khởi động nhanh

### Bước 1 — Cấu hình môi trường

```bash
cp .env.test .env
```

### Bước 2 — Khởi động Docker Stack

```bash
docker network create camara-network || true
docker compose up -d
```

### Bước 3 — Sinh dữ liệu giả lập & Chạy pipeline

```bash
# Sinh dữ liệu CSV mẫu
python -m simulator.simulator --records 1000 --output data/radius_log.csv

# Chạy pipeline nạp dữ liệu từ CSV vào Kafka và khởi chạy 3 modules xử lý
python pipeline/run_pipeline.py --input data/radius_log.csv
```

### Bước 4 — Gọi CAMARA API

```bash
# Kiểm tra SIM Swap
curl -X POST http://localhost:8000/sim-swap/v0/check \
  -H "X-API-Key: dev-secret" \
  -H "Content-Type: application/json" \
  -d '{"phoneNumber": "+84970000000", "maxAge": 30}'
```

### Dashboard & Services

| Service | URL |
|---|---|
| CAMARA API | http://localhost:8000 (Swagger: `/docs`) |
| Grafana | http://localhost:3000 (admin/admin) |
| Prometheus | http://localhost:9090 |

---

## 5. Chi tiết pipeline các module

### 5.1 Module IP-MSISDN Processing (`cg-ip-msisdn`)
- **Consumer group**: `cg-ip-msisdn`
- **Topic**: `radius.accounting.raw`
- **Luồng xử lý**:
  - Trích xuất: `Framed_IP_Address`, `Calling_Station_Id` (msisdn), `NAS_Identifier`, `acct_status_type`, `timestamp`.
  - **Start / Interim-Update**:
    - Upsert Key `ip-ggsn:<framed_ip>` với Value `{"msisdn": "...", "timestamp": "..."}` vào Redis với TTL 24h.
    - Làm mới TTL mỗi lần có bản tin `Interim-Update`.
    - Thêm `framed_ip` vào key phụ `ggsn-ips:<nas_identifier>` (SET) phục vụ xóa hàng loạt.
  - **Stop**: Xóa key `ip-ggsn:<framed_ip>` sau khi kiểm tra msisdn tương ứng trùng khớp.
  - **Accounting-Off**: Lấy danh sách IP từ key phụ `ggsn-ips:<nas_identifier>` và thực hiện xóa hàng loạt.

### 5.2 Module Device Swap Processing (`cg-device-swap`)
- **Consumer group**: `cg-device-swap`
- **Topic**: `radius.accounting.raw`
- **Luồng xử lý**:
  - Trích xuất: `msisdn`, `imei`, `timestamp`.
  - Truy vấn Redis/PostgreSQL (`msisdn_device`) lấy `imei_current`.
  - So sánh:
    - **Không thay đổi**: Bỏ qua.
    - **Có thay đổi**:
      1. Cập nhật `msisdn_device` (`imei_current`, `updated_at`) và lưu bản ghi cũ vào `device_swap_history`.
      2. Cập nhật Redis cache `device:{msisdn}`.
      3. Truy vấn bảng `subscription` tìm các subscription đang hoạt động đăng ký `DEVICE_SWAP` cho msisdn này.
      4. Gửi HTTP callback tới Open Gateway endpoint với payload: `msisdn`, `imei_old`, `imei_new`, `event_time`.
      5. Ghi log sự kiện vào bảng `audit_log`.
  - **Xử lý lỗi callback**: Áp dụng retry với exponential backoff và hàng đợi retry Redis (`retry:device_swap`).

### 5.3 Module SIM Swap Processing (`cg-sim-swap`)
- **Consumer group**: `cg-sim-swap`
- **Topic**: `radius.accounting.raw`
- **Luồng xử lý**:
  - Trích xuất: `msisdn`, `imsi`, `timestamp`.
  - Tra cứu IMSI hiện tại từ Redis/PostgreSQL (`msisdn_sim`).
  - Khi phát hiện thay đổi IMSI (SIM Swap):
    1. Cập nhật `msisdn_sim` (`imsi_current`, `updated_at`) và lưu vào `sim_swap_history`.
    2. Cập nhật Redis cache `sim:{msisdn}` với `last_time_sim_change`.
    3. Truy vấn bảng `subscription` đăng ký sự kiện `SIM_SWAP` cho msisdn.
    4. Gửi HTTP callback tới Subscriber (Open Gateway) kèm payload: `MSISDN`, `LastTimeSIMChange`.
    5. Ghi log xử lý vào `audit_log` và cập nhật metrics.

---

## 6. Storage layer

### 6.1 Redis (In-Memory Data Store)
- **Key IP-MSISDN**: `ip-ggsn:<framed_ip>` -> JSON `{"msisdn": "...", "timestamp": "..."}` (TTL 24h, refresh on Interim-Update).
- **Secondary Index**: `ggsn-ips:<nas_identifier>` -> SET of `<framed_ip>`.
- **Cache Device Swap**: `device:{msisdn}` -> `{"imei_current": "...", "updated_at": "..."}`.
- **Cache SIM Swap**: `sim:{msisdn}` -> `{"imsi_current": "...", "last_time_sim_change": "..."}`.
- **Retry Queues**: `retry:device_swap`, `retry:sim_swap` (LIST).

### 6.2 PostgreSQL (Relational Persistence Store)

Schema định nghĩa tại `storage/migrations/001_init_schema.sql`:

| Bảng | Vai trò | Primary Key |
|---|---|---|
| `msisdn_device` | Lưu trạng thái IMEI hiện tại theo MSISDN | `msisdn` |
| `msisdn_sim` | Lưu trạng thái IMSI hiện tại theo MSISDN | `msisdn` |
| `device_swap_history` | Nhật ký lịch sử các lần thay đổi IMEI | `id` (BIGSERIAL) |
| `sim_swap_history` | Nhật ký lịch sử các lần thay đổi IMSI/SIM | `id` (BIGSERIAL) |
| `subscription` | Quản lý subscription đăng ký webhook từ Open Gateway | `subscription_id` (UUID) |
| `audit_log` | Nhật ký ghi nhận các sự kiện swap và xử lý hệ thống | `id` (BIGSERIAL) |
| `notification_log` | Theo dõi trạng thái gửi notification & lịch sử retry | `id` (BIGSERIAL) |

---

## 7. CAMARA API

FastAPI service xác thực qua HTTP header `X-API-Key`.

| Endpoint | Method | Mô tả | Nguồn dữ liệu |
|---|---|---|---|
| `/sim-swap/v0/check` | POST | Kiểm tra SIM Swap đã xảy ra trong N ngày qua | `sim_swap_history` |
| `/sim-swap/v0/retrieve-date` | POST | Lấy thời điểm SIM Swap gần nhất | `sim_swap_history` |
| `/device-swap/v0/check` | POST | Kiểm tra thiết bị (IMEI) thay đổi trong N ngày qua | `device_swap_history` |
| `/device-swap/v0/retrieve-date` | POST | Lấy thời điểm đổi thiết bị gần nhất | `device_swap_history` |
| `/number-verification/v0/verify` | POST | Xác minh MSISDN active trên mạng | `msisdn_sim` |

---

## 8. Cấu hình & biến môi trường

```bash
# --- API ---
API_KEY=dev-secret

# --- DATABASE ---
DB_HOST=camara-postgres
DB_PORT=5432
DB_NAME=camara_db
DB_USER=postgres
DB_PASSWORD=camara

# --- KAFKA ---
KAFKA_BOOTSTRAP_SERVERS=camara-kafka:9092
KAFKA_TOPIC_RAW=radius.accounting.raw

# --- REDIS ---
REDIS_HOST=camara-redis
REDIS_PORT=6379
REDIS_DB=0
```

---

## Tham khảo

| Tài liệu | URL |
|---|---|
| RFC 2866 – RADIUS Accounting | https://datatracker.ietf.org/doc/html/rfc2866 |
| 3GPP TS 29.061 – GGSN VSA | https://www.3gpp.org/ftp/Specs/archive/29_series/29.061/ |
| CAMARA SIM Swap API Spec | https://github.com/camaraproject/SimSwap |
| CAMARA Number Verification | https://github.com/camaraproject/NumberVerification |
| Apache Kafka | https://kafka.apache.org/documentation/ |
| FastAPI | https://fastapi.tiangolo.com/ |
| Redis | https://redis.io/docs/latest/ |