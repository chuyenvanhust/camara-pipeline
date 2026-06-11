# CAMARA Network API Data Pipeline

> **Lab / Miniproject** — Xây dựng Data Pipeline phục vụ CAMARA Network API  
> (SIM Swap · Device Swap · Number Verification)  
> từ dữ liệu **GGSN RADIUS Accounting Request** (RFC 2866 + 3GPP TS 29.061 VSA)

---

## Mục lục

1. [Kiến trúc tổng thể](#1-kiến-trúc-tổng-thể)
2. [Cấu trúc thư mục](#2-cấu-trúc-thư-mục)
3. [Mock External Services](#3-mock-external-services)
4. [Yêu cầu hệ thống](#4-yêu-cầu-hệ-thống)
5. [Khởi động nhanh](#5-khởi-động-nhanh)
6. [Chi tiết từng module](#6-chi-tiết-từng-module)
7. [Chạy test suite](#7-chạy-test-suite)
8. [Đo hiệu năng](#8-đo-hiệu-năng)
9. [Cấu hình & biến môi trường](#9-cấu-hình--biến-môi-trường)
10. [Tài liệu kỹ thuật](#10-tài-liệu-kỹ-thuật)

---

## 1. Kiến trúc tổng thể

```
┌────────────────────────────────────────────────────────────────────────────────┐
│                             CAMARA Pipeline – Lab                              │
│                                                                                │
│  ┌──────────────────────────────────────────────────────────────────────────┐  │
│  │  MOCK EXTERNAL SERVICES  (giữ nguyên bản chất API thực tế)              │  │
│  │                                                                          │  │
│  │  ┌─────────────────┐  ┌──────────────────┐  ┌───────────────────────┐  │  │
│  │  │ GSMA TAC Mock   │  │  HLR/HSS Mock    │  │  ITU E.164 Mock       │  │  │
│  │  │ :8100           │  │  :8200           │  │  :8300                │  │  │
│  │  │                 │  │                  │  │                       │  │  │
│  │  │ GET /tac/{tac}  │  │ GET /by-imsi/{}  │  │ POST /validate        │  │  │
│  │  │ POST /tac/batch │  │ GET /by-msisdn/{}│  │ POST /validate/batch  │  │  │
│  │  │ GET /tac        │  │ GET /{}/imsi-    │  │ GET /country-codes    │  │  │
│  │  │                 │  │     history      │  │ GET /{cc}/operators   │  │  │
│  │  └────────┬────────┘  └────────┬─────────┘  └───────────┬───────────┘  │  │
│  └───────────┼────────────────────┼────────────────────────┼──────────────┘  │
│              │  R4: TAC lookup    │  R3: IMSI exist        │  R2: MSISDN fmt  │
│              │  Device info       │  Conflict C confirm    │                  │
│              ▼                    ▼                         ▼                  │
│  ┌────────────────────────────────────────────────────────────────────────┐   │
│  │                        PROCESSING PIPELINE                             │   │
│  │                                                                        │   │
│  │  ┌──────────┐    ┌───────────┐    ┌─────────┐    ┌─────────────────┐  │   │
│  │  │Simulator │───▶│  Kafka    │───▶│  Spark  │───▶│   PostgreSQL    │  │   │
│  │  │          │    │           │    │   SSS   │    │ (partitioned)   │  │   │
│  │  │generators│    │radius.raw │    │ 5 stage │    │                 │  │   │
│  │  │  ↕ TAC   │    │radius.valid    │         │    │ radius_sessions │  │   │
│  │  │  lookup  │    │radius.dedup    │ S2 calls│    │ swap_event      │  │   │
│  │  │          │    │radius.clean    │ mock    │    │ duplicate_log   │  │   │
│  │  └──────────┘    │radius.invalid  │ services│    │ conflict_log    │  │   │
│  │                  └───────────┘    └─────────┘    │ invalid_log     │  │   │
│  │                                                   └────────┬────────┘  │   │
│  └────────────────────────────────────────────────────────────┼───────────┘   │
│                                                               │               │
│  ┌────────────────────────────────────────────────────────────┼───────────┐   │
│  │                     CAMARA API LAYER                        │           │   │
│  │                                                             ▼           │   │
│  │  FastAPI :8000          ┌──────────────────────────────────┐            │   │
│  │  ├── /sim-swap          │           query                  │            │   │
│  │  ├── /device-swap  ─────►      PostgreSQL                  │            │   │
│  │  └── /number-verify     └──────────────────────────────────┘            │   │
│  └────────────────────────────────────────────────────────────────────────┘   │
│                                                                                │
│  ┌─────────────────────────┐   ┌──────────────────────────────────────────┐   │
│  │  Prometheus + Grafana   │   │   Data Quality Report (HTML)             │   │
│  │  :9090 / :3000          │   │   reporting/quality_report.py            │   │
│  └─────────────────────────┘   └──────────────────────────────────────────┘   │
└────────────────────────────────────────────────────────────────────────────────┘
```

### Pipeline stages

| Stage | Tên | Input → Output | Gọi mock service |
|-------|-----|---------------|-----------------|
| S1 | Ingestion | CSV → `radius.raw` | Không |
| S2 | Validation | `radius.raw` → `radius.valid` + `radius.invalid` | GSMA TAC (R4), HLR/HSS (R3), ITU E.164 (R2) |
| S3 | Deduplication | `radius.valid` → `radius.dedup` | Không |
| S4 | Conflict Resolution | `radius.dedup` → `radius.clean` + `swap_event` | HLR/HSS (xác nhận conflict C) |
| S5 | Storage Insert | `radius.clean` → PostgreSQL | Không |

### CAMARA API endpoints

| Endpoint | Method | SLA p95 | Spec |
|----------|--------|---------|------|
| `/sim-swap/v0/check` | POST | ≤ 200ms | CAMARA chính thức |
| `/sim-swap/v0/retrieve-date` | POST | ≤ 200ms | CAMARA chính thức |
| `/device-swap/v0/check` | POST | ≤ 200ms | Custom (ADR-005) |
| `/device-swap/v0/retrieve-date` | POST | ≤ 200ms | Custom (ADR-005) |
| `/number-verification/v0/verify` | POST | ≤ 100ms | CAMARA chính thức |

---

## 2. Cấu trúc thư mục

```
camara-pipeline/
│
├── simulator/                  # D1 – RADIUS Log Simulator
│   ├── README.md
│   ├── simulator.py            # Entry point CLI
│   ├── generators.py           # Sinh MSISDN/IMSI/IMEI; gọi GSMA TAC mock để lấy TAC hợp lệ
│   ├── error_injectors.py      # Inject duplicate/late arrival/invalid IMEI/conflict
│   └── config.py               # SimulatorConfig dataclass
│
├── pipeline/                   # D2 – Processing Pipeline (5 stage)
│   ├── README.md
│   ├── run_pipeline.py         # Entry point: khởi động cả 5 stage
│   ├── base_job.py             # Base class SparkStreamingJob
│   ├── ingestion/
│   │   ├── README.md
│   │   ├── producer.py         # Kafka Producer: CSV → radius.raw
│   │   └── csv_reader.py       # Parse CSV theo batch
│   ├── validation/
│   │   ├── README.md
│   │   ├── validator.py        # Spark SSS job, route valid/invalid
│   │   └── rules.py            # R1–R6; R2/R3/R4 gọi mock services qua HTTP
│   ├── deduplication/
│   │   ├── README.md
│   │   ├── dedup_job.py        # Spark stateful dedup, RocksDB state
│   │   └── state_manager.py    # RocksDB key schema, TTL
│   ├── conflict_resolution/
│   │   ├── README.md
│   │   ├── resolver.py         # Phân loại conflict A/B/C
│   │   └── swap_detector.py    # Conflict C → gọi HLR/HSS mock → emit swap_event
│   └── storage/
│       ├── README.md
│       ├── writer.py           # Spark JDBC micro-batch writer
│       └── models.py           # SQLAlchemy models
│
├── mock_services/              # ★ Mock External APIs (không dùng thao tác giản lược)
│   ├── README.md               # Tổng quan 3 mock services + cách pipeline gọi
│   ├── docker-compose.mock.yml
│   ├── gsma_tac/               # Mô phỏng GSMA TAC Allocation Database
│   │   ├── README.md           # Spec endpoints, schema, fault injection
│   │   ├── app.py              # FastAPI app, load CSV vào dict khi startup
│   │   ├── router.py           # GET /tac/{tac} · POST /tac/batch · GET /tac
│   │   ├── models.py           # TacRecord, TacLookupResponse, BatchRequest
│   │   ├── seed.py             # Sinh tac_records.csv (2000 TAC, seed=42)
│   │   ├── data/tac_records.csv
│   │   └── Dockerfile
│   ├── hlr_hss/                # Mô phỏng HLR/HSS Subscriber Registry
│   │   ├── README.md
│   │   ├── app.py              # FastAPI app
│   │   ├── router.py           # GET /by-imsi · /by-msisdn · /imsi-history · batch
│   │   ├── models.py           # SubscriberProfile, ImsiHistoryEntry, BatchLookup
│   │   ├── seed.py             # Sinh subscribers.csv (100k, seed=42 — khớp simulator)
│   │   ├── data/subscribers.csv
│   │   └── Dockerfile
│   ├── itu_e164/               # Mô phỏng ITU E.164 Number Plan Registry
│   │   ├── README.md
│   │   ├── app.py              # FastAPI app
│   │   ├── router.py           # GET /country-codes · POST /validate · /validate/batch
│   │   ├── models.py           # PhoneNumber, ValidationResult, CountryCode
│   │   ├── seed.py             # Sinh country_codes.csv + operator_prefixes.csv (static)
│   │   ├── data/country_codes.csv
│   │   ├── data/operator_prefixes.csv
│   │   └── Dockerfile
│   └── shared/                 # Shared utilities dùng chung cho 3 mock
│       ├── README.md
│       ├── health.py           # Standard health check response
│       ├── pagination.py       # Pydantic generics Page[T]
│       └── errors.py           # Error format + X-Inject-Fault middleware
│
├── storage/                    # D4 – PostgreSQL Schema
│   ├── README.md
│   ├── migrations/
│   │   ├── 001_init_schema.sql
│   │   ├── 002_indexes.sql
│   │   └── 003_partitions.sql
│   └── docs/
│       └── schema_design.md
│
├── api/                        # D5 – CAMARA API (FastAPI)
│   ├── README.md
│   ├── main.py
│   ├── config.py
│   ├── routers/
│   │   ├── sim_swap.py
│   │   ├── device_swap.py
│   │   ├── number_verification.py
│   │   └── health.py
│   ├── schemas/
│   │   ├── sim_swap.py
│   │   ├── device_swap.py
│   │   ├── number_verification.py
│   │   └── common.py
│   └── dependencies/
│       ├── auth.py
│       └── database.py
│
├── tests/                      # D6 – Integration Test Suite (36 TC)
│   ├── README.md               # Danh sách đầy đủ TC01–TC36
│   ├── conftest.py
│   ├── fixtures/
│   │   ├── seed_data.sql
│   │   └── edge_cases.sql
│   ├── api/
│   │   ├── test_sim_swap.py         # TC01–TC09
│   │   ├── test_device_swap.py      # TC10–TC16
│   │   ├── test_number_verification.py  # TC17–TC22
│   │   ├── test_auth.py             # TC35
│   │   └── test_error_handling.py   # TC34, TC36
│   └── pipeline/
│       ├── test_validation.py       # TC32–TC33
│       ├── test_deduplication.py    # TC23–TC25
│       ├── test_conflict_resolution.py  # TC26–TC28
│       └── test_late_arrival.py     # TC29–TC31
│
├── reporting/                  # D3 – Data Quality Report
│   ├── README.md
│   ├── quality_report.py
│   ├── metrics_collector.py
│   └── templates/report.html.jinja2
│
├── infra/                      # Cấu hình hạ tầng Docker
│   ├── README.md
│   ├── kafka/kafka_config.properties
│   ├── prometheus/prometheus.yml
│   └── grafana/dashboards/pipeline_dashboard.json
│
├── docs/                       # Tài liệu kỹ thuật
│   ├── README.md
│   ├── openapi/
│   │   ├── sim_swap.yaml
│   │   ├── device_swap.yaml
│   │   └── number_verification.yaml
│   └── adr/
│       ├── ADR-001-input-format.md
│       ├── ADR-002-conflict-definition.md
│       ├── ADR-003-storage-partitioning.md
│       ├── ADR-004-dedup-state-store.md
│       └── ADR-005-device-swap-api-design.md
│
├── scripts/                    # Shell script wrappers
│   ├── README.md
│   ├── run_simulator.sh
│   ├── run_pipeline.sh
│   ├── run_load_test.sh
│   ├── generate_report.sh
│   └── reset_db.sh
│
├── docker-compose.yml          # Stack đầy đủ: 10 services (bao gồm 3 mock)
├── docker-compose.test.yml     # Stack test: PostgreSQL isolated + API
├── pyproject.toml
├── Makefile
├── .env.example
├── .gitignore
└── README.md                   # File này
```

---

## 3. Mock External Services

Ba service mô phỏng API bên ngoài mà pipeline phụ thuộc.
Mỗi service giữ **đúng contract** của API thực — không dùng thao tác giản lược
(không hardcode return value, không bypass HTTP, không mock ở code level).

| Service | Port | Thực thể thật | Dùng bởi |
|---------|------|--------------|---------|
| [GSMA TAC Mock](mock_services/gsma_tac/README.md) | 8100 | GSMA TAC Allocation Database | `simulator/generators.py`, `pipeline/validation/rules.py` R4 |
| [HLR/HSS Mock](mock_services/hlr_hss/README.md) | 8200 | 3GPP HLR/HSS Subscriber Registry | `pipeline/validation/rules.py` R3, `pipeline/conflict_resolution/swap_detector.py` |
| [ITU E.164 Mock](mock_services/itu_e164/README.md) | 8300 | ITU-T E.164 Number Plan | `pipeline/validation/rules.py` R2 |

### Đồng bộ seed data

Tất cả 3 mock services và simulator dùng **cùng seed=42**:

```
seed=42
  ├── simulator/generators.py       → sinh MSISDN/IMSI/IMEI từ cùng pool
  ├── mock_services/gsma_tac/seed.py    → TAC trong mock = TAC simulator dùng
  └── mock_services/hlr_hss/seed.py    → subscriber trong mock = subscriber simulator sinh
```

Điều này đảm bảo:
- IMEI hợp lệ do simulator sinh ra → TAC tồn tại trong GSMA TAC mock → pass R4
- MSISDN/IMSI hợp lệ do simulator sinh ra → có trong HLR/HSS mock → pass R3
- `InvalidImeiInjector` sinh IMEI với TAC **không** có trong mock → fail R4 → ghi `invalid_log`

### Fault injection

Mỗi mock service hỗ trợ header `X-Inject-Fault` để test pipeline resilience:

```bash
# Giả lập TAC service chậm 500ms
curl -H "X-Inject-Fault: delay=500" http://localhost:8100/tac/352099

# Giả lập 20% request lỗi ngẫu nhiên
curl -H "X-Inject-Fault: error_rate=0.2" http://localhost:8100/tac/352099
```

---

## 4. Yêu cầu hệ thống

| Công cụ | Phiên bản |
|---------|-----------|
| Docker | ≥ 24.0 |
| Docker Compose | ≥ 2.20 (plugin, không phải `docker-compose` cũ) |
| Python | ≥ 3.11 (chỉ cần nếu chạy ngoài Docker) |
| RAM | ≥ 8 GB (Spark ~4 GB + các service còn lại) |
| Disk | ≥ 5 GB (CSV 2M records ~800 MB + Docker images) |
| CPU | ≥ 4 core |

---

## 5. Khởi động nhanh

### Bước 1 — Clone & cấu hình

```bash
git clone <repo-url> camara-pipeline
cd camara-pipeline
cp .env.example .env
```

### Bước 2 — Khởi động toàn bộ stack (10 services)

```bash
make up
# docker compose up --build -d
```

Bao gồm: Kafka, Zookeeper, PostgreSQL, Spark, FastAPI,
Prometheus, Grafana, **và 3 mock services**.

### Bước 3 — Seed mock services (chạy 1 lần)

```bash
# Seed phải đúng thứ tự: TAC trước, HLR/HSS sau
python mock_services/gsma_tac/seed.py  --count 2000   --seed 42
python mock_services/hlr_hss/seed.py   --count 100000 --seed 42
python mock_services/itu_e164/seed.py  # static data, không cần seed
```

### Bước 4 — Sinh dữ liệu

```bash
make sim
# python simulator/simulator.py --records 2000000 --seed 42 --output data/radius_log.csv
```

Simulator gọi GSMA TAC mock (`GET /tac`) khi khởi động để tải danh sách TAC hợp lệ.

### Bước 5 — Chạy pipeline

```bash
make pipeline
# python pipeline/run_pipeline.py --input data/radius_log.csv
```

Pipeline stage S2 gọi 3 mock services qua HTTP trong quá trình validation.
Stage S4 gọi HLR/HSS mock khi phát hiện conflict C.

### Bước 6 — Kiểm tra kết quả

```bash
# Data Quality Report
make report
# Mở reports/quality_report_<timestamp>.html

# Gọi thử API
curl -X POST http://localhost:8000/sim-swap/v0/check \
  -H "X-API-Key: dev-secret" \
  -H "Content-Type: application/json" \
  -d '{"phoneNumber": "+84971234567", "maxAge": 30}'
```

### Services sau khi up

| Service | URL | Credential |
|---------|-----|-----------|
| CAMARA API | http://localhost:8000 | Header: `X-API-Key: dev-secret` |
| Swagger UI | http://localhost:8000/docs | — |
| GSMA TAC Mock | http://localhost:8100/docs | — |
| HLR/HSS Mock | http://localhost:8200/docs | — |
| ITU E.164 Mock | http://localhost:8300/docs | — |
| Grafana | http://localhost:3000 | admin / admin |
| Spark UI | http://localhost:4040 | — |
| Prometheus | http://localhost:9090 | — |

---

## 6. Chi tiết từng module

| Module | README | Mô tả |
|--------|--------|-------|
| `simulator/` | [README](simulator/README.md) | Sinh 2M RADIUS records; gọi GSMA TAC mock để lấy TAC hợp lệ |
| `pipeline/` | [README](pipeline/README.md) | Tổng quan 5 stage + dependency vào mock services |
| `pipeline/ingestion/` | [README](pipeline/ingestion/README.md) | CSV → Kafka `radius.raw` |
| `pipeline/validation/` | [README](pipeline/validation/README.md) | 6 rules; R2/R3/R4 gọi mock services |
| `pipeline/deduplication/` | [README](pipeline/deduplication/README.md) | Spark RocksDB stateful dedup, window 1h |
| `pipeline/conflict_resolution/` | [README](pipeline/conflict_resolution/README.md) | Conflict A/B/C; SIM Swap detection |
| `pipeline/storage/` | [README](pipeline/storage/README.md) | Spark JDBC → PostgreSQL |
| `mock_services/` | [README](mock_services/README.md) | Tổng quan 3 mock + fault injection |
| `mock_services/gsma_tac/` | [README](mock_services/gsma_tac/README.md) | GSMA TAC API: TAC lookup, batch, pagination |
| `mock_services/hlr_hss/` | [README](mock_services/hlr_hss/README.md) | HLR/HSS: subscriber profile, IMSI/SIM swap history |
| `mock_services/itu_e164/` | [README](mock_services/itu_e164/README.md) | ITU E.164: country code, operator prefix, MSISDN validate |
| `mock_services/shared/` | [README](mock_services/shared/README.md) | Health, pagination, error format, fault injection middleware |
| `storage/` | [README](storage/README.md) | Schema, migrations, index/partition design |
| `api/` | [README](api/README.md) | 5 CAMARA endpoints, query logic, auth |
| `tests/` | [README](tests/README.md) | 36 TC, fixture strategy, markers |
| `reporting/` | [README](reporting/README.md) | HTML Data Quality Report, 6 sections |
| `infra/` | [README](infra/README.md) | 10 services, ports, Kafka topics |
| `docs/` | [README](docs/README.md) | OpenAPI specs, 5 ADR |
| `scripts/` | [README](scripts/README.md) | Shell wrappers, thứ tự chạy lần đầu |

---

## 7. Chạy test suite

```bash
# Setup stack test
docker compose -f docker-compose.test.yml up -d
pip install -e ".[test]"

# Tất cả 36 test case
pytest tests/ -v

# Theo nhóm
pytest tests/api/      -v          # TC01–TC22, TC34–TC36
pytest tests/pipeline/ -v          # TC23–TC33

# Theo marker
pytest -m edge_case    -v
pytest -m late_arrival -v
pytest -m pipeline     -v
```

Chi tiết danh sách TC01–TC36: [`tests/README.md`](tests/README.md)

---

## 8. Đo hiệu năng

### Throughput pipeline

```bash
make pipeline-bench
# Mục tiêu: ≥ 5.000 records/giây end-to-end
```

### Latency API (k6)

```bash
make load-test
# 100 VU · ramp 30s · sustain 60s
```

| API | SLA p95 | Cách đạt |
|-----|---------|---------|
| SIM Swap | ≤ 200ms | Index `idx_swap_msisdn` trên `swap_event` |
| Device Swap | ≤ 200ms | Index `idx_swap_imei` trên `swap_event` |
| Number Verification | ≤ 100ms | Index `idx_msisdn_ts` + query trên partition tháng hiện tại |

Kết quả hiển thị trên Grafana: http://localhost:3000

---

## 9. Cấu hình & biến môi trường

Copy `.env.example` → `.env`:

```bash
# API
API_KEY=dev-secret
API_PORT=8000

# PostgreSQL
DB_HOST=localhost
DB_PORT=5432
DB_NAME=camara_db
DB_USER=camara
DB_PASSWORD=camara

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# Pipeline
LATE_ARRIVAL_THRESHOLD_SECONDS=3600
SPARK_MASTER=local[*]

# Mock Services
GSMA_TAC_API_URL=http://localhost:8100
HLR_HSS_API_URL=http://localhost:8200
ITU_E164_API_URL=http://localhost:8300

# Simulator
SIM_DEFAULT_SEED=42
SIM_DEFAULT_RECORDS=2000000
SIM_DEFAULT_SUBSCRIBERS=100000
```

---

## 10. Tài liệu kỹ thuật

| Tài liệu | Vị trí |
|----------|--------|
| Schema design + so sánh partitioning | [`storage/docs/schema_design.md`](storage/docs/schema_design.md) |
| OpenAPI – SIM Swap | [`docs/openapi/sim_swap.yaml`](docs/openapi/sim_swap.yaml) |
| OpenAPI – Device Swap | [`docs/openapi/device_swap.yaml`](docs/openapi/device_swap.yaml) |
| OpenAPI – Number Verification | [`docs/openapi/number_verification.yaml`](docs/openapi/number_verification.yaml) |
| ADR-001: Input format | [`docs/adr/ADR-001-input-format.md`](docs/adr/ADR-001-input-format.md) |
| ADR-002: Conflict definition | [`docs/adr/ADR-002-conflict-definition.md`](docs/adr/ADR-002-conflict-definition.md) |
| ADR-003: Storage partitioning | [`docs/adr/ADR-003-storage-partitioning.md`](docs/adr/ADR-003-storage-partitioning.md) |
| ADR-004: Dedup state store | [`docs/adr/ADR-004-dedup-state-store.md`](docs/adr/ADR-004-dedup-state-store.md) |
| ADR-005: Device Swap API design | [`docs/adr/ADR-005-device-swap-api-design.md`](docs/adr/ADR-005-device-swap-api-design.md) |

---

## Makefile targets

```bash
make up              # Khởi động 10 services (bao gồm 3 mock)
make down            # Dừng và xóa volumes
make seed-mocks      # Seed dữ liệu cho 3 mock services
make sim             # Chạy simulator (2M records, seed=42)
make pipeline        # Chạy full pipeline
make pipeline-bench  # Pipeline + đo throughput
make test            # pytest tests/ -v (36 TC)
make load-test       # k6 load test 3 API
make report          # Sinh Data Quality Report HTML
make reset-db        # Drop + recreate schema (dev only)
make logs            # docker compose logs -f
make lint            # ruff check + mypy
```

---

## Tham khảo

| Tài liệu | URL |
|----------|-----|
| RFC 2866 – RADIUS Accounting | https://datatracker.ietf.org/doc/html/rfc2866 |
| 3GPP TS 29.061 – GGSN VSA | https://www.3gpp.org/ftp/Specs/archive/29_series/29.061/ |
| GSMA TAC Database | https://www.gsma.com/services/tac-allocation/ |
| CAMARA SIM Swap API Spec | https://github.com/camaraproject/SimSwap |
| CAMARA Number Verification | https://github.com/camaraproject/NumberVerification |
| ITU-T E.164 | https://www.itu.int/rec/T-REC-E.164/en |
| ITU-T E.212 | https://www.itu.int/rec/T-REC-E.212/en |
| Apache Kafka | https://kafka.apache.org/documentation/ |
| Spark Structured Streaming | https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html |
| PostgreSQL Partitioning | https://www.postgresql.org/docs/current/ddl-partitioning.html |
| FastAPI | https://fastapi.tiangolo.com/ |
