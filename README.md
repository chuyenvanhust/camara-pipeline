# CAMARA Network API Data Pipeline

> **Lab / Miniproject** — Data Pipeline phục vụ CAMARA Network API
> (SIM Swap · Device Swap · Number Verification)
> từ dữ liệu **GGSN RADIUS Accounting Request** (RFC 2866 + 3GPP TS 29.061 VSA)

---

## Mục lục

1. [Kiến trúc tổng thể](#1-kiến-trúc-tổng-thể)
2. [Cấu trúc thư mục](#2-cấu-trúc-thư-mục)
3. [Yêu cầu hệ thống](#3-yêu-cầu-hệ-thống)
4. [Khởi động nhanh](#4-khởi-động-nhanh)
5. [Chi tiết pipeline](#5-chi-tiết-pipeline)
6. [Storage layer](#6-storage-layer)
7. [CAMARA API](#7-camara-api)
8. [Test suite](#8-test-suite)
9. [Cấu hình & biến môi trường](#9-cấu-hình--biến-môi-trường)

---

## 1. Kiến trúc tổng thể

```
┌───────────────────────────────────────────────────────────────────────────┐
│                        CAMARA Pipeline – Lab                        │
│                                                                           │
│                                                                           │
│  ┌───────────────────────────────────────────────────────────────────┐  │
│  │                    PROCESSING PIPELINE (3 stage)                   │  │
│  │                                                                     │  │
│  │  Simulator ──▶ Kafka:radius.raw ──▶ [S2 Spark, executor-side] ──▶  │  │
│  │  (CSV, gọi         │                validation (mock services)     │  │
│  │   GSMA TAC để      │                + late-arrival check           │  │
│  │   lấy TAC hợp lệ)  │                + dedup (Redis SETNX, TTL 1h)  │  │
│  │                    │                + conflict A/B/C/D (Redis      │  │
│  │                    │                  global state theo msisdn)   │  │
│  │                    │                                    │          │  │
│  │                    │                Kafka:radius.clean ─┘          │  │
│  │                    │                        │                       │  │
│  │                    │                        ▼                       │  │
│  │                    │            [S3 Spark] JDBC micro-batch ──▶     │  │
│  │                    │                        PostgreSQL              │  │
│  └────────────────────┼────────────────────────┼───────────────────────┘  │
│                        │                        ▼                          │
│                        │        radius_sessions · swap_event ·             │
│                        │        conflict_log · invalid_log ·               │
│                        │        duplicate_log (trigger backstop)           │
│                        ▼                        │                          │
│  ┌───────────────────────────────────────────────┼───────────────────┐    │
│  │                     CAMARA API LAYER            ▼                  │    │
│  │  FastAPI :8000 (X-API-Key)                                         │    │
│  │  ├── /sim-swap/v0        ──┐                                       │    │
│  │  ├── /device-swap/v0     ──┼──▶ query PostgreSQL                   │    │
│  │  └── /number-verification/v0 ─┘                                    │    │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                            │
│  ┌───────────────────────┐   ┌───────────────────────────────────────┐   │
│  │ Prometheus + Grafana  │   │ Data Quality Report (HTML, 4 section) │   │
│  │ :9090 / :3000         │   │ reporting/quality_report.py           │   │
│  └───────────────────────┘   └───────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────────┘
```

### Pipeline stage

| Stage | Tên | Input → Output | Công nghệ |
|---|---|---|---|
| S1 | Ingestion | CSV → Kafka `radius.raw` | Kafka Producer (Python, `kafka-python`) |
| S2 | Processing (validation + dedup + conflict) | `radius.raw` → Kafka `radius.clean` + PostgreSQL (`invalid_log`, `conflict_log`, `swap_event`) | Spark Structured Streaming, executor-side `foreachPartition`, Redis |
| S3 | Storage Insert | `radius.clean` → PostgreSQL `radius_sessions` | Spark Structured Streaming, JDBC/psycopg2 micro-batch |

---

## 2. Cấu trúc thư mục

```
camara-pipeline/
│
├── simulator/                       # D1 – RADIUS Log Simulator
│   ├── simulator.py                 # Entry point CLI
│   ├── generators.py                # Sinh MSISDN/IMSI/IMEI; gọi GSMA TAC mock để lấy TAC hợp lệ
│   ├── error_injectors.py           # Inject duplicate/late arrival/invalid IMEI/conflict/missing field
│   └── config.py                    # SimulatorConfig dataclass
│
├── pipeline/                        # D2 – Processing Pipeline (3 stage thật)
│   ├── run_pipeline.py              # Orchestrator: start S2 → S3 → S1, chờ drain, tự shutdown
│   └── pipeline/
│       ├── spark_jars.py            # Cấu hình Ivy/jar Kafka+JDBC cho Spark
│       ├── ingestion/
│       │   ├── producer.py          # S1: CSV → Kafka radius.raw
│       │   └── csv_reader.py        # Đọc CSV theo batch
│       ├── validation/
│       │   └── rules.py             # R1–R6; R2/R3/R4 gọi ITU/HLR/GSMA mock qua HTTP batch
│       ├── state/
│       │   └── redis_state_manager.py  # State toàn cục theo msisdn: dedup TTL + last_imsi/last_imei cho conflict A/B/C/D
│       ├── processing/
│       │   ├── processor.py         # S2: entry point Spark Structured Streaming (driver-side)
│       │   └── partition_worker.py  # Logic thật chạy trên executor: validate + dedup + conflict + ghi Postgres/Kafka
│       └── storage/
│           ├── writer.py            # S3: Spark JDBC/psycopg2 micro-batch writer
│           └── models.py            # RadiusSession — mapping cột INSERT
│
├── mock_services/                   # Mock 3 external API mà pipeline thật sự gọi (không hardcode)
│   ├── docker-compose.mock.yml
│   ├── gsma_tac/                    # GSMA TAC Allocation DB — dùng bởi simulator (sinh IMEI) + R4 (validation)
│   ├── hlr_hss/                     # HLR/HSS Subscriber Registry — dùng bởi R3 (validation)
│   ├── itu_e164/                    # ITU E.164 Number Plan — dùng bởi R2 (validation)
│   └── shared/                      # health check, pagination, fault-injection middleware dùng chung
│
├── storage/                         # D4 – PostgreSQL Schema
│   ├── migrations/
│   │   ├── 001_init_schema.sql      # 5 bảng: radius_sessions, swap_event, duplicate_log, conflict_log, invalid_log
│   │   ├── 002_indexes.sql          # Index theo msisdn/imsi/imei + timestamp
│   │   ├── 003_partitions.sql       # RANGE partition theo tháng cho radius_sessions
│   │   └── 004_dedup_trigger.sql    # Trigger backstop dài hạn: dedup_seen_keys → duplicate_log
│   └── docs/schema_design.md        # So sánh RANGE vs HASH partitioning
│
├── tests/integration/               # D6 – 36 test case
│   ├── conftest.py, fixtures/
│   ├── api/          # TC01–TC22, TC34–TC36
│   └── pipeline/     # TC23–TC33
│
├── reporting/                       # D3 – Data Quality Report (HTML, 4 section)
│   ├── quality_report.py
│   ├── metrics_collector.py
│   └── templates/report.html.jinja2
│
├── load_tests/                      # k6 script đo p95 latency 3 API
│   ├── sim_swap.js · device_swap.js · number_verification.js · all_endpoint.js
│
├── infra/                           # Prometheus + Grafana config
├── docs/openapi/                    # OpenAPI spec 3 API + ADR
├── scripts/                         # Wrapper shell: run_simulator.sh, run_pipeline.sh, generate_report.sh, run_load_test.sh, reset_db.sh
├── docker-compose.yml                # Core stack: zookeeper, kafka, postgres, redis, fastapi, spark-master, prometheus, grafana
├── docker-compose.test.yml           # Stack test riêng (Postgres test DB + API)
├── Makefile
└── requirements.txt
```

---

## 3. Yêu cầu hệ thống

| Công cụ | Phiên bản |
|---|---|
| Docker | ≥ 24.0 |
| Docker Compose | ≥ 2.20 (plugin) |
| Python | ≥ 3.11 (chỉ cần nếu chạy ngoài Docker) |
| RAM | ≥ 8 GB |
| Disk | ≥ 5 GB |
| CPU | ≥ 4 core |

---

## 4. Khởi động nhanh

### Bước 1 — Clone & cấu hình

```bash
git clone https://github.com/chuyenvanhust/camara-pipeline camara-pipeline
cd camara-pipeline
cp .env.test .env
```

### Bước 2 — Tạo network dùng chung rồi khởi động **cả 2** stack

`docker-compose.yml` khai báo network `camara-network` là `external: true`, nên phải tạo
trước; sau đó khởi động core stack và mock stack (đây là điểm README cũ thiếu — chỉ chạy
`docker-compose.mock.yml` sẽ không có Kafka/Postgres/Redis):

```bash
docker network create camara-network
docker compose up -d
docker compose -f mock_services/docker-compose.mock.yml up -d
```

### Bước 3 — Seed dữ liệu cho mock services (chạy 1 lần)

```bash
docker exec -e PYTHONPATH=. -w /workspace camara-mock-gsma-tac python -m mock_services.gsma_tac.seed --count 2000 --seed 42
docker exec -e PYTHONPATH=. -w /workspace camara-mock-hlr-hss  python -m mock_services.hlr_hss.seed  --count 100000 --seed 42
docker exec -e PYTHONPATH=. -w /workspace camara-mock-itu-e164 python -m mock_services.itu_e164.seed --count 1000 --seed 42
```

### Bước 4 — Sinh dữ liệu

```bash
bash scripts/run_simulator.sh
```

### Bước 5 — Chạy pipeline

```bash
bash scripts/run_pipeline.sh
```

### Bước 6 — Kiểm tra kết quả

```bash
bash scripts/generate_report.sh

curl -X POST http://localhost:8000/sim-swap/v0/check \
  -H "X-API-Key: dev-secret" \
  -H "Content-Type: application/json" \
  -d '{"phoneNumber": "+84971234567", "maxAge": 30}'
```

### Services sau khi up

| Service | URL |
|---|---|
| CAMARA API | http://localhost:8000 (Swagger: `/docs`) |
| GSMA TAC Mock | http://localhost:8100/docs |
| HLR/HSS Mock | http://localhost:8200/docs |
| ITU E.164 Mock | http://localhost:8300/docs |
| Grafana | http://localhost:3000 (admin/admin) |
| Spark UI | http://localhost:4040 (S2) / :4041 (S3) |
| Prometheus | http://localhost:9090 |

---

## 5. Chi tiết pipeline

### S1 — Ingestion (`pipeline/ingestion/producer.py`)

Đọc dữ liệu từ tệp CSV theo từng lô (batch) thông qua `csv_reader.py`. Sử dụng
`KafkaProducer` để đẩy dữ liệu vào topic `radius.raw`. Quá trình này đảm bảo tốc độ nạp dữ
liệu cao và khả năng chịu tải tốt khi file đầu vào có kích thước hàng triệu bản ghi.

### S2 — Processing (`pipeline/processing/processor.py` + `partition_worker.py`)

Sử dụng Spark Structured Streaming với Trigger Interval 2 giây. Đây là "linh hồn" của hệ
thống với kiến trúc Zero Driver Bottleneck: Driver chỉ điều phối, toàn bộ logic nặng được
đẩy xuống Executors xử lý song song qua `foreachPartition`:

- **Distributed Validation**: Thực hiện 6 Rules kiểm tra (R1–R6) bằng thư viện `asyncio` và
  `httpx` để gọi đồng thời các Mock Services (ITU, GSMA, HLR). Bản ghi lỗi được gắn nhãn và
  đẩy vào `invalid_log`.
- **Deduplication (2 lớp)**:
  - *Fast path (Real-time)*: Sử dụng Redis `SET NX` với TTL 3600s để chặn trùng lặp ngay
    lập tức giữa các luồng xử lý phân tán.
  - *Backstop (Long-term)*: Trigger `fn_dedup_long_term_check` tại PostgreSQL kiểm tra lại
    dựa trên bảng `dedup_seen_keys` để bắt các bản ghi trùng lặp đến sau 1 giờ.
- **Conflict Resolution A/B/C/D (Redis Global State)**:
  - Sử dụng Redis làm bộ nhớ trạng thái toàn cục theo `msisdn`, cho phép so sánh xuyên suốt
    mọi batch dữ liệu.
  - *Conflict A & B*: Loại bỏ các phiên (session) không nhất quán hoặc trùng lặp định danh.
  - *Conflict C (SIM Swap) & D (Device Swap)*: Khi phát hiện thay đổi IMSI/IMEI so với
    trạng thái cuối cùng trong Redis, hệ thống tự động xác nhận và ghi vào `swap_event` mà
    không cần gọi lại API bên ngoài, giúp tối ưu hóa 80% thời gian xử lý.
- **Parallel Multi-sink Write**: Executors ghi trực tiếp kết quả vào Kafka `radius.clean` và
  PostgreSQL (`invalid_log`, `conflict_log`, `swap_event`), tận dụng tối đa băng thông kết
  nối của Database.

### S3 — Storage Insert (`pipeline/storage/writer.py`)

Đọc dữ liệu sạch từ Kafka `radius.clean` và thực hiện đổ vào kho dữ liệu chính:

- **Parallel COPY Ingestion**: Thay vì dùng lệnh `INSERT` tuần tự, Stage 3 sử dụng cơ chế
  ghi phân tán `foreachPartition` kết hợp lệnh `COPY` chuyên dụng của PostgreSQL.
- **High Performance Tuning**: Tận dụng cấu hình `synchronous_commit=off` giúp tốc độ ghi
  đạt hàng chục nghìn dòng mỗi giây, đảm bảo dữ liệu sẵn sàng cho API Layer gần như ngay lập
  tức (Sub-second latency).

---

## 6. Storage layer

Hệ thống sử dụng PostgreSQL 15 làm kho lưu trữ trung tâm với thiết kế tối ưu cho truy vấn
phân tích thời gian thực.

### Cấu trúc bảng (`storage/migrations/001_init_schema.sql`)

| Bảng | Vai trò | Chiến lược Partition |
|---|---|---|
| `radius_sessions` | Lưu trữ toàn bộ phiên RADIUS hợp lệ | RANGE theo `event_timestamp` (theo tháng) |
| `swap_event` | Lưu lịch sử đổi SIM/Thiết bị đã xác nhận | Không phân vùng (Dữ liệu tinh gọn) |
| `conflict_log` | Nhật ký các bản ghi bị xung đột A/B | Không phân vùng |
| `invalid_log` | Nhật ký lỗi Validation và bản ghi đến muộn | Không phân vùng |
| `dedup_seen_keys` | Lưu Metadata để phục vụ trigger lọc trùng | Không phân vùng |

### Chiến lược Partitioning & Indexing

**RANGE Partitioning**: Được áp dụng cho bảng `radius_sessions` vì đặc thù truy vấn của
CAMARA API luôn tập trung vào cửa sổ thời gian gần nhất (ví dụ: kiểm tra Swap trong 30 ngày
qua). Việc chia nhỏ theo tháng giúp:

- **Partition Pruning**: Postgres chỉ quét dữ liệu trong tháng liên quan, tăng tốc truy vấn
  gấp nhiều lần.
- **Data Lifecycle**: Dễ dàng thực hiện `DROP PARTITION` để xóa dữ liệu cũ mà không gây lock
  bảng hoặc tốn tài nguyên Vacuum.

**Composite Indexing**:

- `idx_msisdn_ts`: `(msisdn, event_timestamp DESC)` — Tối ưu cho API xác thực số điện thoại
  và tra cứu phiên gần nhất.
- `idx_swap_msisdn`: `(msisdn, detected_at DESC)` — Phục vụ trực tiếp cho `/sim-swap` API
  với tốc độ phản hồi p95 < 200ms.

### Cơ chế Integrity Backstop

Tất cả các bảng log và bảng chính đều hỗ trợ cơ chế `ON CONFLICT DO NOTHING`. Điều này đảm
bảo tính Idempotent (bất biến): nếu Spark Job bị restart và xử lý lại dữ liệu cũ, Database
sẽ không bị rác hoặc trùng lặp thông tin.

---

## 7. CAMARA API

FastAPI, auth bằng header `X-API-Key` (biến môi trường `API_KEY`).

| Endpoint | Method | SLA p95 | Nguồn |
|---|---|---|---|
| `/sim-swap/v0/check`, `/sim-swap/v0/retrieve-date` | POST | ≤ 200ms | CAMARA spec chính thức |
| `/device-swap/v0/check`, `/device-swap/v0/retrieve-date` | POST | ≤ 200ms | Tự thiết kế theo pattern SIM Swap |
| `/number-verification/v0/verify` | POST | ≤ 100ms | CAMARA spec chính thức (proxy: active session 24h) |

---

## 8. Test suite

```bash
docker compose -f docker-compose.test.yml up -d
pip install -e ".[test]"

pytest tests/integration -v                     # toàn bộ 36 TC
pytest tests/integration/api/      -v           # TC01–TC22, TC34–TC36
pytest tests/integration/pipeline/ -v           # TC23–TC33
```

Test data được insert thẳng vào PostgreSQL test DB (không qua Kafka/Spark) để mỗi case
chạy dưới 1 giây.

---

## 9. Cấu hình & biến môi trường

```bash
API_KEY=dev-secret
DB_HOST=camara-postgres
DB_PORT=5432
DB_NAME=camara_db
DB_USER=postgres
DB_PASSWORD=camara

KAFKA_BOOTSTRAP_SERVERS=camara-kafka:9092
KAFKA_TOPIC_RAW=radius.raw
KAFKA_TOPIC_CLEAN=radius.clean

REDIS_HOST=camara-redis
REDIS_PORT=6379

LATE_ARRIVAL_THRESHOLD_SECONDS=3600
WATERMARK_VALIDATION=7200 seconds

GSMA_TAC_SERVICE_URL=http://camara-mock-gsma-tac:8100
HLR_HSS_SERVICE_URL=http://camara-mock-hlr-hss:8200
ITU_E164_SERVICE_URL=http://camara-mock-itu-e164:8300
```

---

## Tham khảo

| Tài liệu | URL |
|---|---|
| RFC 2866 – RADIUS Accounting | https://datatracker.ietf.org/doc/html/rfc2866 |
| 3GPP TS 29.061 – GGSN VSA | https://www.3gpp.org/ftp/Specs/archive/29_series/29.061/ |
| GSMA TAC Database | https://www.gsma.com/services/tac-allocation/ |
| CAMARA SIM Swap API Spec | https://github.com/camaraproject/SimSwap |
| CAMARA Number Verification | https://github.com/camaraproject/NumberVerification |
| Apache Kafka | https://kafka.apache.org/documentation/ |
| Spark Structured Streaming | https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html |
| PostgreSQL Partitioning | https://www.postgresql.org/docs/current/ddl-partitioning.html |
| FastAPI | https://fastapi.tiangolo.com/ |
| Redis | https://redis.io/docs/latest/ |