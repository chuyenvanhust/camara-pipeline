# CAMARA Network API Data Pipeline

> **Production-grade Data Pipeline** phục vụ các chuẩn **CAMARA Network API** (SIM Swap · Device Swap · Number Verification) từ luồng dữ liệu **GGSN/PGW RADIUS Accounting** (RFC 2866 + 3GPP TS 29.061 VSA).

Dự án mô phỏng và xử lý luồng dữ liệu mạng viễn thông di động: các gói tin RADIUS Accounting (hoặc file CSV log) được đưa vào hệ thống Ingestion qua UDP/1813, đẩy vào Apache Kafka theo cơ chế phân vùng theo thuê bao (`key=msisdn`), được xử lý song song bởi 3 Consumer Modules độc lập để phát hiện các sự kiện đổi SIM (IMSI), đổi thiết bị (IMEI), ánh xạ IP↔MSISDN, ghi nhận trạng thái vào PostgreSQL/Redis theo các giao dịch nguyên tử (atomic transactions), và phát thông báo webhook (HTTP Callback) tới các đối tác Open Gateway thông qua Transactional Outbox Pattern.

---

## 1. Kiến trúc tổng thể hệ thống

```mermaid
flowchart TD
    subgraph SOURCELAYER["Nguồn Dữ Liệu"]
        direction TB
        GGSN["Thiết Bị Mạng GGSN / PGW / NAS<br/>(RADIUS UDP / RFC 2866)"]
        CSV["File Log Batch<br/>(CSV Radius Accounting)"]
    end

    subgraph INGESTIONLAYER["Stage 1: Ingestion Layer"]
        direction TB
        SENDER["radius_udp_sender.py<br/>(Traffic Generator / Test Tool)"]
        PKTREADER["PacketReader<br/>(UDP/1813 Listener, Binary Decoder)"]
        PRODUCER["RadiusLogProducer<br/>(Async Buffer + Batch Ingestion)"]
        ACKCACHE[("ACK Deduplication Cache<br/>LRU RAM")]
    end

    subgraph KAFKALAYER["Message Broker: Apache Kafka"]
        direction TB
        TOPIC["Topic: radius.accounting.raw<br/>(12 Partitions, Key = MSISDN)"]
        DLQ["Topic: radius.accounting.raw.dlq<br/>(Dead Letter Queue)"]
    end

    subgraph CONSUMERLAYER["Stage 2: Processing Pipeline (run_pipeline.py)"]
        direction TB
        subgraph CG1["Consumer Group: cg-ip-msisdn"]
            IPM1["Member 1"]
            IPM2["Member 2"]
        end
        subgraph CG2["Consumer Group: cg-device-swap"]
            DEV1["Member 1"]
            DEV2["Member 2"]
        end
        subgraph CG3["Consumer Group: cg-sim-swap"]
            SIM1["Member 1"]
            SIM2["Member 2"]
        end
    end

    subgraph STORAGELAYER["Storage & State Layer"]
        direction TB
        REDIS[("Redis / Redis Sentinel<br/>- ip-ggsn:* / ggsn-ips:*<br/>- device:* / sim:* cache")]
        POSTGRES[("PostgreSQL Database<br/>- msisdn_device / msisdn_sim<br/>- device_swap_history / sim_swap_history<br/>- radius_session_state<br/>- audit_log<br/>- notification_log (Outbox)")]
    end

    subgraph DISPATCHERLAYER["Stage 3: Event Dispatcher"]
        direction TB
        DISPATCHER["notification_dispatcher.py<br/>(Transactional Outbox Worker)"]
    end

    subgraph APILAYER["Stage 4: CAMARA API Gateway"]
        direction TB
        FASTAPI["FastAPI Application (api/)<br/>- /sim-swap/v0/*<br/>- /device-swap/v0/*<br/>- /number-verification/v0/*"]
        OPENGATEWAY["Subscribers / Open Gateway<br/>(Webhooks Callback)"]
    end

    %% Flow connections
    GGSN -->|UDP Datagrams| PKTREADER
    CSV -->|Read CSV| PRODUCER
    CSV -.->|Simulate UDP| SENDER
    SENDER -->|UDP/1813| PKTREADER

    PKTREADER --> PRODUCER
    PRODUCER <--> ACKCACHE
    PRODUCER -->|Produce acks=all| TOPIC
    PRODUCER -.->|Malformed Msg| DLQ
    PRODUCER -.->|RADIUS Accounting-Response| GGSN

    TOPIC --> CG1
    TOPIC --> CG2
    TOPIC --> CG3

    CG1 -->|Atomic Lua Scripts| REDIS
    CG1 -->|Session Batch Upsert| POSTGRES
    CG2 -->|Batch MGET/MSET| REDIS
    CG2 -->|Atomic Batch Tx| POSTGRES
    CG3 -->|Batch MGET/MSET| REDIS
    CG3 -->|Atomic Batch Tx| POSTGRES

    POSTGRES -.->|FOR UPDATE SKIP LOCKED| DISPATCHER
    DISPATCHER -->|HTTP POST Callback| OPENGATEWAY

    REDIS <--> FASTAPI
    POSTGRES <--> FASTAPI
```

---

## 2. Luồng hoạt động chi tiết (Sequence Flow)

```mermaid
sequenceDiagram
    autonumber
    participant NAS as GGSN / NAS / Sender
    participant ING as RadiusLogProducer
    participant KFK as Kafka Broker
    participant CSM as Consumer Modules
    participant RDS as Redis
    participant PGS as PostgreSQL
    participant DSP as NotificationDispatcher
    participant SUB as Open Gateway Subscriber

    Note over NAS,ING: Giai đoạn Ingestion & Xác thực
    NAS->>ING: UDP Accounting-Request (Code=4, Authenticator, AVPs)
    ING->>ING: Decode binary, tính MD5 HMAC, kiểm tra Deduplication Cache
    alt Đã từng xử lý (Duplicate)
        ING-->>NAS: Gửi lại Accounting-Response (Code=5) ngay lập tức
    else Tin mới (New Event)
        ING->>ING: Đưa vào Bounded RAM Queue
        ING->>KFK: Batch Produce (key=MSISDN, acks=all)
        KFK-->>ING: Kafka ACK
        ING->>ING: Lưu event_id vào ACK Cache
        ING-->>NAS: Trả Accounting-Response (Code=5)
    end

    Note over KFK,PGS: Giai đoạn Xử lý sự kiện (Parallel Consumers)
    KFK->>CSM: Poll batch records theo từng Partition
    par Module IP-MSISDN
        CSM->>PGS: Upsert radius_session_state
        CSM->>RDS: Chạy Lua Script cập nhật ip-ggsn:* & ggsn-ips:*
    and Module Device Swap
        CSM->>RDS: MGET cache device:MSISDN
        opt Cache Miss
            CSM->>PGS: Query msisdn_device
        end
        CSM->>CSM: So sánh IMEI cũ vs IMEI mới
        opt Có sự kiện đổi máy (Device Swap)
            CSM->>PGS: Transaction (Upsert State + Insert History + Audit + Outbox PENDING)
            CSM->>RDS: Cập nhật cache device:MSISDN
        end
    and Module SIM Swap
        CSM->>RDS: MGET cache sim:MSISDN
        opt Cache Miss
            CSM->>PGS: Query msisdn_sim
        end
        CSM->>CSM: So sánh IMSI cũ vs IMSI mới
        opt Có sự kiện đổi SIM (SIM Swap)
            CSM->>PGS: Transaction (Upsert State + Insert History + Audit + Outbox PENDING)
            CSM->>RDS: Cập nhật cache sim:MSISDN (kèm last_time_sim_change)
        end
    end
    CSM->>KFK: Commit Kafka Offsets (Manual Commit sau khi hoàn tất)

    Note over PGS,SUB: Giai đoạn Gửi Webhook (Outbox Dispatcher)
    loop Định kỳ mỗi poll interval
        DSP->>PGS: Claim notifications (FOR UPDATE SKIP LOCKED)
        PGS-->>DSP: Trả về danh sách notification PENDING
        DSP->>SUB: HTTP POST Webhook (kèm Idempotency-Key)
        alt Thành công (HTTP 2xx)
            DSP->>PGS: Update status = 'SENT'
        else Thất bại (Timeout / Error)
            DSP->>PGS: Update status = 'FAILED' + tính exponential backoff
        end
    end
```

---

## 3. Nguyên tắc thiết kế cốt lõi

1. **Bảo toàn thứ tự theo thuê bao (Key-based Partitioning)**:
   - Tất cả bản ghi đều dùng `key=msisdn` khi gửi vào Kafka. Mọi sự kiện của cùng 1 thuê bao luôn rơi vào đúng 1 partition xác định.
   - Khi xử lý song song, offset trong từng partition luôn được duyệt tuần tự theo thời gian, chống race condition.

2. **Xử lý sự kiện nguyên tử (Atomic Database Transactions)**:
   - Toàn bộ 4 thao tác ghi của một batch phát hiện Swap (`msisdn_*` state, `*_history`, `audit_log`, `notification_log`) được thực hiện trong **cùng một transaction duy nhất** (`asyncpg.transaction()`).
   - Đảm bảo tính toàn vẹn: không bao giờ có state đổi mà thiếu history, hoặc tạo notification cho sự kiện bị rollback.

3. **Transactional Outbox Pattern**:
   - Consumer **không bao giờ gọi HTTP** ra ngoài. Mọi webhook được ghi vào bảng `notification_log` với trạng thái `PENDING`.
   - Tiến trình `NotificationDispatcher` độc lập poll và gửi HTTP callback. Sự cố mạng hoặc đối tác phản hồi chậm không bao giờ làm nghẽn throughput của pipeline xử lý chính.

4. **Kiến trúc Ingestion chịu lỗi & Backpressure**:
   - Giao thức UDP RADIUS chỉ trả `Accounting-Response` sau khi Kafka đã xác nhận `acks=all`.
   - Nếu Kafka bị chậm hoặc Queue đầy, Ingestion chủ động giữ ACK để thiết bị NAS kích hoạt cơ chế retry tự nhiên của giao thức UDP.
   - Cache `_radius_ack_cache` trong RAM ngăn chặn việc đưa các gói tin retry đã xử lý vào Kafka lần thứ hai.

5. **Manual Offset Commit & Dead Letter Queue (DLQ)**:
   - Tắt hoàn toàn `enable_auto_commit`. Offset chỉ được commit sau khi batch đã ghi thành công vào DB.
   - Khi một bản ghi hoặc shard bị lỗi cấu trúc dữ liệu không thể xử lý, nó được đẩy vào topic `.dlq` kèm metadata lỗi chi tiết để phục vụ phân tích mà không làm dừng pipeline.

6. **Redis làm Read Cache & Fencing Version**:
   - Redis đóng vai trò Cache tốc độ cao phục vụ API truy vấn tức thời.
   - Các thao tác cập nhật Redis trong `ip_msisdn` sử dụng Lua script nguyên tử kết hợp kiểm tra fencing token `(event_epoch, partition, offset)` để loại bỏ triệt để vấn đề gói tin đến sai thứ tự (out-of-order delivery).

---

## 4. Cấu trúc thư mục dự án

```
.
├── api/                             # FastAPI - Triển khai các chuẩn CAMARA Network API
│   ├── routers/                     # Endpoint router: sim_swap, device_swap, number_verification...
│   ├── schemas/                     # Pydantic models theo chuẩn CAMARA
│   ├── dependencies/                # Dependency injection kết nối Database/Redis
│   └── main.py                      # FastAPI App entrypoint
├── pipeline/                        # Toàn bộ Core Data Pipeline
│   ├── ingestion/                   # Stage 1: UDP Listener & CSV Producer
│   │   ├── packet_reader.py         # Binary RADIUS RFC 2866 & 3GPP VSA parser
│   │   ├── producer.py              # Async Kafka Producer & ACK Manager
│   │   ├── csv_reader.py            # Local CSV Reader generator
│   │   └── radius_udp_sender.py     # UDP Traffic Generator / Load test simulator
│   ├── modules/                     # Stage 2: Parallel Consumer Modules
│   │   ├── shared/                  # Hạ tầng dùng chung (BaseConsumer, DatabasePool, Metrics...)
│   │   ├── ip_msisdn/               # Module 1: Ánh xạ IP↔MSISDN (Redis Lua + Session State)
│   │   ├── device_swap/             # Module 2: Phát hiện đổi thiết bị (IMEI Tracking)
│   │   └── sim_swap/                # Module 3: Phát hiện đổi SIM (IMSI Tracking)
│   ├── dispatcher/                  # Stage 3: Notification Outbox Dispatcher
│   │   └── notification_dispatcher.py # Worker gửi HTTP Callback độc lập
│   └── run_pipeline.py              # Orchestrator khởi chạy toàn bộ 3 consumers
├── storage/                         # Database Schemas & Migrations
│   ├── migrations/                  # Các file SQL khởi tạo cấu trúc bảng, indexes
│   └── seed/                        # Dữ liệu mẫu (Subscribers, Subscriptions)
├── simulator/                       # Trình sinh dữ liệu RADIUS giả lập (Synthetic Data Generator)
├── load_tests/                      # K6 Load testing scripts cho CAMARA APIs
├── infra/                           # Cấu hình Prometheus, Grafana dashboards
├── docker-compose.yml               # Môi trường Development cục bộ
├── docker-compose.prod.yml          # Môi trường Production (Multi-broker Kafka, Redis Sentinel)
└── requirements.txt                 # Python dependencies
```

---

## 5. Hướng dẫn khởi chạy nhanh (Quickstart)

### 5.1. Yêu cầu môi trường
- **Docker** & **Docker Compose** v2+
- **Python 3.11+** (nếu chạy script trực tiếp trên máy host)
- Tối thiểu 4GB RAM khả dụng cho Docker

### 5.2. Các bước triển khai

```bash
# 1. Clone repository và chuẩn bị biến môi trường
git clone <repo-url>
cd camara-pipeline
cp .env.example .env

# 2. Khởi động toàn bộ hạ tầng (Kafka, Zookeeper, PostgreSQL, Redis, Prometheus, Grafana)
docker compose up -d

# 3. Chạy migrations khởi tạo cơ sở dữ liệu PostgreSQL
# Script tự động thực thi các file SQL trong storage/migrations/
bash scripts/run_pipeline.sh --init-db

# 4. Sinh dữ liệu RADIUS mẫu (tuỳ chọn)
python -m simulator.simulator --output data/radius_sample.csv --records 50000

# 5. Khởi chạy Pipeline xử lý (Consumers)
python -m pipeline.run_pipeline

# 6. Khởi chạy Notification Dispatcher (Terminal riêng biệt)
python -m pipeline.dispatcher.notification_dispatcher

# 7. Khởi chạy CAMARA FastAPI Gateway (Terminal riêng biệt)
uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload

# 8. Bắn dữ liệu vào Ingestion (chọn 1 trong 2 cách):
# Cách A: Đọc trực tiếp từ file CSV đẩy vào Kafka
python -m pipeline.ingestion.producer --file data/radius_sample.csv

# Cách B: Giả lập thiết bị mạng gửi gói tin UDP thật qua cổng 1813
python -m pipeline.ingestion.radius_udp_sender --csv data/radius_sample.csv --rate 2000 --require-ack
```

---

## 6. Biến môi trường quan trọng (Configuration Reference)

| Tên Biến Môi Trường | Giá Trị Mặc Định | Ý Nghĩa / Mục Đích |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `camara-kafka:9092,camara-kafka-2:9092,camara-kafka-3:9092` | Danh sách địa chỉ Kafka Cluster Brokers |
| `KAFKA_TOPIC_RAW` | `radius.accounting.raw` | Tên topic Kafka chứa log thô |
| `KAFKA_TOPIC_PARTITIONS` | `16` | Số partition của topic (tối ưu xử lý song song 15k+ rec/s) |
| `CONSUMERS_PER_GROUP` | `4` | Số member/worker chạy song song trong mỗi Consumer Group |
| `PROCESSING_PARTITION_CONCURRENCY` | `3` | Số shard partition gom xử lý song song trong mỗi consumer |
| `BATCH_MAX_RECORDS` | `4000` | Số lượng bản ghi tối đa lấy trong một lần poll Kafka |
| `BATCH_TIMEOUT_MS` | `20` | Thời gian tối đa chờ gom đủ batch (ms) |
| `DATABASE_URL` | `postgresql://postgres:camara@camara-postgres:5432/camara_db` | Connection string PostgreSQL (`synchronous_commit=on` đảm bảo 100% ACID) |
| `DB_POOL_MIN` / `DB_POOL_MAX` | `6` / `24` | Kích thước Connection Pool `asyncpg` dùng chung (PostgreSQL `max_connections=200`) |
| `REDIS_HOST` / `REDIS_PORT` | `camara-redis` / `6379` | Thông tin kết nối Redis Standalone / Cluster |
| `RADIUS_SHARED_SECRET` | `camara-radius-dev-secret` | Secret key tính Authenticator RFC 2866 |
| `RADIUS_UDP_RECEIVE_BUFFER_BYTES` | `33554432` (32MB) | Kích thước socket buffer nhận UDP |
| `RADIUS_UDP_QUEUE_MAX_RECORDS` | `100000` | Dung lượng hàng đợi RAM đệm trước Kafka (khuyên dùng `300000` cho Prod) |
| `RADIUS_UDP_KAFKA_BATCH_RECORDS` | `250` | Kích thước batch Kafka của UDP Ingestion |
| `RADIUS_UDP_KAFKA_BATCH_WAIT_MS` | `5` | Thời gian gom batch Kafka của Ingestion (ms) |
| `RADIUS_UDP_KAFKA_MAX_INFLIGHT_BATCHES_PER_WORKER` | `32` | Số lượng batch Kafka produce song song cho mỗi worker |
| `RADIUS_UDP_KAFKA_TOTAL_MAX_INFLIGHT_BATCHES` | `64` | Giới hạn tổng số batch Kafka produce song song trên toàn bộ process |
| `RADIUS_UDP_PUBLISHER_WORKERS` | `4` | Số lượng worker coroutines publish song song (Key-sharded per MSISDN) |
| `INGESTION_METRICS_PORT` | `9201` | Cổng Exporter Prometheus Ingestion (tự động thử 9201-9210 nếu bận) |
| `DISPATCHER_BATCH_SIZE` | `50` | Số lượng notification claim mỗi vòng lặp của Dispatcher |
| `METRICS_PORT` | `9200` | Port expose `/metrics` cho Prometheus scraper |

---

## 7. Giám sát Telemetry, Bảo Mật & Phục hồi Thảm họa (Security & Disaster Recovery)

### 7.1. Giám sát Telemetry & Split Metrics
Mỗi `THROUGHPUT_LOG_INTERVAL_SECONDS` (mặc định 10 giây), hệ thống ghi log phân khối trực quan (`|`) cho cả hai chặng:
- **`[INGESTION]`**: Tốc độ nhận UDP (`udp_in`), tốc độ Kafka ACK (`kafka_ack`), dung lượng Queue per-worker (`queue`), chi tiết phản hồi RADIUS (`new_ack`, `dup_ack`, `withheld`), và **metrics rành mạch**: `invalid`, `dlq_published`, `publish_failed`, `queue_rejected_for_retry`.
- **`[PROCESSING]`**: Log chi tiết cho từng member: tốc độ nhận/xử lý (`recv`, `success`, `pg`, `rds`), độ trễ xử lý batch (`batch_avg`), độ trễ từng chặng `stage(state, pg, rds)`, **độ trễ bản tin toàn trình `e2e_lag(max)`** (tính qua float epoch `ingest_epoch_s` siêu tốc), và **định vị lỗi `data_loss` (`err`, `dlq`)**.

### 7.2. Tính năng Bảo mật An ninh Mạng (Production Security)
- **CAMARA OAuth2 OIDC Verification**: Xác thực JWT Bearer Token chuẩn (`exp`, `iss`, `aud`) kết hợp API Key fallback.
- **SSRF & DNS Rebinding Protection**: Kiểm tra URL webhook chặt chẽ (`ssrf_protection.py`), ngăn chặn các cuộc tấn công quét mạng nội bộ và DNS Rebinding.
- **HMAC SHA-256 Signature**: Đính kèm chữ ký `X-Signature-SHA256` trên mọi request webhook callback.
- **Container Hardening**: Toàn bộ Docker images (`pipeline/Dockerfile`, `api/Dockerfile`) thực thi dưới quyền user không có root (`USER appuser`).

### 7.3. Phục hồi Thảm họa (Disaster Recovery Runbook)
- **Kịch bản DR & Failover**: Xem quy trình xử lý sự cố chi tiết tại [`docs/DISASTER_RECOVERY_RUNBOOK.md`](docs/DISASTER_RECOVERY_RUNBOOK.md).
- **Sao lưu & Phục hồi PostgreSQL**:
  ```bash
  # Thực hiện sao lưu dữ liệu PostgreSQL
  bash scripts/backup_postgres.sh

  # Phục hồi dữ liệu từ bản sao lưu
  bash scripts/restore_postgres.sh storage/backups/camara_db_backup_latest.sql.gz
  ```

---

## 8. Tài liệu Báo cáo Kỹ thuật & Hướng dẫn Vận hành

Dự án đã được tài liệu hóa đầy đủ các giải pháp kiến trúc và thuật toán nâng cao:
- 🛠️ [**Hướng dẫn Cấu hình & Tối ưu hóa Phần cứng (Hardware Tuning Guide)**](docs/HARDWARE_TUNING_GUIDE.md): Giải thích chi tiết toàn bộ các biến môi trường trong `.env`, quy tắc sizing tài nguyên RAM/CPU, công thức tính toán độ đệm hàng đợi, connection budget PostgreSQL và bảng thông số cấu hình chuẩn cho các môi trường từ Dev/VPS đến Server Production 30k+ pkt/s.
- 📖 [**Báo cáo Kỹ thuật Chuyên sâu các File Trọng điểm**](docs/BAO_CAO_KY_THUAT_PIPELINE.md): Mô tả chi tiết kỹ thuật giải mã nhị phân RFC 2866, MD5 Authenticator, 3GPP VSA, Kernel `SO_REUSEPORT`, Key-Sharded Publisher Queue, Global Inflight Semaphore, Multi-Socket ACK Receiver (`select.select()`), Partition Sharding, Transaction 4 bảng nguyên tử qua `UNNEST`, Fencing Versioning Tuple, Lua Scripts nguyên tử, Fast Float Epoch E2E Lag, và Transactional Outbox Pattern (`FOR UPDATE SKIP LOCKED`).
- 📖 [**Disaster Recovery Runbook & HA Operational Guide**](docs/DISASTER_RECOVERY_RUNBOOK.md): Hướng dẫn vận hành sự cố, sao lưu Point-in-Time Recovery (PITR) và quy trình Failover Kafka/PostgreSQL.

