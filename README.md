# CAMARA Network API Data Pipeline

> **Lab / Miniproject** — Data Pipeline phục vụ CAMARA Network API
> (SIM Swap · Device Swap · Number Verification)
> từ dữ liệu **GGSN RADIUS Accounting Request** (RFC 2866 + 3GPP TS 29.061 VSA)


---

## Mục lục

1. [Kiến trúc tổng thể](#1-kiến-trúc-tổng-thể)
2. [Cấu trúc thư mục](#2-cấu-trúc-thư-mục)
3. [Mock External Services](#3-mock-external-services)
4. [Yêu cầu hệ thống](#4-yêu-cầu-hệ-thống)
5. [Khởi động nhanh](#5-khởi-động-nhanh)
6. [Chi tiết pipeline (3 stage thật)](#6-chi-tiết-pipeline-3-stage-thật)
7. [Storage layer](#7-storage-layer)
8. [CAMARA API](#8-camara-api)
9. [Test suite](#9-test-suite)
10. [Cấu hình & biến môi trường](#10-cấu-hình--biến-môi-trường)
11. [Module đã loại bỏ khỏi luồng chạy thật](#11-module-đã-loại-bỏ-khỏi-luồng-chạy-thật)

---

## 1. Kiến trúc tổng thể

```
┌───────────────────────────────────────────────────────────────────────────┐
│                        CAMARA Pipeline – Lab (thật)                       │
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
# mở reports/quality_report_<timestamp>.html

curl -X POST http://localhost:8000/sim-swap/v0/check \
  -H "X-API-Key: dev-secret" \
  -H "Content-Type: application/json" \
  -d '{"phoneNumber": "+84971234567", "maxAge": 30}'
```

### Services sau khi up

| Service | URL |
|---|---|
| CAMARA API | http://localhost:8000 (Swagger: `/docs`) |
| Grafana | http://localhost:3000 (admin/admin) |
| Spark UI | http://localhost:4040 (S2) / :4041 (S3) |
| Prometheus | http://localhost:9090 |

---

## 6. Chi tiết pipeline 

### S1 — Ingestion (`pipeline/ingestion/producer.py`)

Đọc CSV theo batch (`csv_reader.py`), publish vào Kafka topic `radius.raw`.

### S2 — Processing (`pipeline/processing/processor.py` + `partition_worker.py`)

1 job Spark Structured Streaming duy nhất, trigger 2 giây. `foreachBatch` repartition theo
`msisdn` rồi gọi `foreachPartition(process_partition)`cho từng **executor** xử lí phân tán:

1. **Validation ** — `validation/rules.py`, kiểm tra sự đầy đủ trường ,chekc luhn— record fail bất kỳ rule nào → `invalid_log`.

2. **Deduplication (2 lớp)**:
   - Fast path: Redis `SET NX` + TTL 3600s trên key `(acct_session_id, acct_status_type)`.
   - Backstop dài hạn: Postgres trigger `fn_dedup_long_term_check` (bảng `dedup_seen_keys`)
     bắt các duplicate đến sau khi TTL Redis đã hết → ghi `duplicate_log`.
2. **Conflict resolution A/B/C/D** — dùng Redis global state (`last_imsi`/`last_imei` theo
   `msisdn`), xử lý tuần tự A → B → C/D:
   - A (Session Inconsistency), B (Double Active Session) → `conflict_log`, loại khỏi luồng sạch.
   - C (SIM Swap), D (Device Swap) → giữ trong luồng sạch, ghi thẳng `swap_event`.
4. Ghi Kafka `radius.clean` (record hợp lệ, gồm cả C/D) + Postgres (`invalid_log`,
   `conflict_log`, `swap_event`) trực tiếp từ executor.

### S3 — Storage Insert (`pipeline/storage/writer.py`)

Spark Structured Streaming đọc `radius.clean`, `INSERT ... ON CONFLICT DO NOTHING` (idempotent)
vào `radius_sessions` bằng `psycopg2.executemany`, commit interval cấu hình qua
`SPARK_COMMIT_INTERVAL_SECONDS`.

---

## 7. Storage layer

5 bảng chính (`storage/migrations/001_init_schema.sql`):

| Bảng | Vai trò | Partition |
|---|---|---|
| `radius_sessions` | Record đã qua full pipeline | RANGE theo `event_timestamp`, mỗi tháng |
| `swap_event` | SIM Swap (C) / Device Swap (D) đã phát hiện | Không partition |
| `conflict_log` | Record bị đánh dấu conflict A/B | Không partition |
| `invalid_log` | Record fail validation (R1–R6) + late arrival | Không partition |
| `duplicate_log` + `dedup_seen_keys` | Duplicate bị bắt bởi trigger backstop dài hạn | Không partition |

Index chính: B-Tree `(msisdn, event_timestamp DESC)`, `(imsi, event_timestamp DESC)`,
`(imei, event_timestamp DESC)` trên `radius_sessions`; `(msisdn, detected_at DESC)`,
`(imei, detected_at DESC)` trên `swap_event` — phục vụ trực tiếp 3 API bên dưới.

RANGE partitioning theo tháng được chọn thay vì HASH theo `imsi` vì CAMARA API luôn query
theo cửa sổ thời gian gần đây (N ngày qua) — RANGE cho phép partition pruning và
`DROP PARTITION` để purge dữ liệu cũ, điều HASH không hỗ trợ tốt.

---

## 8. CAMARA API

FastAPI, auth bằng header `X-API-Key` (biến môi trường `API_KEY`).

| Endpoint | Method | SLA p95 | Nguồn |
|---|---|---|---|
| `/sim-swap/v0/check`, `/sim-swap/v0/retrieve-date` | POST | ≤ 200ms | CAMARA spec chính thức |
| `/device-swap/v0/check`, `/device-swap/v0/retrieve-date` | POST | ≤ 200ms | Tự thiết kế theo pattern SIM Swap |
| `/number-verification/v0/verify` | POST | ≤ 100ms | CAMARA spec chính thức (proxy: active session 24h) |

---

## 9. Test suite

```bash
docker compose -f docker-compose.test.yml up -d
pip install -e ".[test]"

pytest tests/integration -v                     # toàn bộ 36 TC
pytest tests/integration/api/      -v           # TC01–TC22, TC34–TC36
pytest tests/integration/pipeline/ -v           # TC23–TC33
```

Test data được insert thẳng vào PostgreSQL test DB (không qua Kafka/Spark) để thuận tiện cho việc xem xét kĩ thuật tại vấn đề quan tâm .

---

## 10. Cấu hình & biến môi trường

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