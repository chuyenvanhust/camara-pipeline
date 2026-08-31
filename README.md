# CAMARA Network API Data Pipeline

> **Production-grade Data Pipeline** phục vụ các chuẩn **CAMARA Network API** (SIM Swap · Device Swap · Number Verification) từ luồng dữ liệu **GGSN/PGW RADIUS Accounting** (RFC 2866 + 3GPP TS 29.061 VSA).

Dự án xử lý bản mirror RADIUS Accounting do một capture server bền vững bên ngoài chuyển tới UDP/1813 (hoặc dữ liệu CSV nạp trực tiếp). Ingestion ghi dữ liệu vào Apache Kafka theo `key=msisdn`; ba consumer module độc lập xử lý đổi SIM (IMSI), đổi thiết bị (IMEI) và ánh xạ IP↔MSISDN. Repo không sở hữu RADIUS protocol session: không trả `Accounting-Response`, không chờ ACK và không retry datagram.

---

## 1. Kiến trúc tổng thể hệ thống

```mermaid
flowchart TD
    subgraph SOURCELAYER["Nguồn Dữ Liệu"]
        direction TB
        GGSN["External RADIUS Capture Server<br/>(Durable Mirror Source)"]
        CSV["File Log Batch<br/>(CSV Radius Accounting)"]
    end

    subgraph INGESTIONLAYER["Stage 1: Ingestion Layer"]
        direction TB
        SENDER["radius_udp_sender.py<br/>(Traffic Generator / Test Tool)"]
        PKTREADER["PacketReader<br/>(UDP/1813 Listener, Binary Decoder)"]
        PRODUCER["RadiusLogProducer<br/>(Async Buffer + Batch Ingestion)"]
    end

    subgraph KAFKALAYER["Message Broker: Apache Kafka"]
        direction TB
        TOPIC["Topic: radius.accounting.raw<br/>(16 Partitions, Key = MSISDN)"]
        DLQ["Topic: radius.accounting.raw.dlq<br/>(Dead Letter Queue)"]
    end

    subgraph CONSUMERLAYER["Stage 2: Isolated Processing Services (3 Workers)"]
        direction TB
        subgraph SVC1["Service: pipeline-ip-msisdn (cg-ip-msisdn)"]
            IPM1["Member 1..4 (GIL Loop 1, Pool max 12)"]
        end
        subgraph SVC2["Service: pipeline-device-swap (cg-device-swap)"]
            DEV1["Member 1..4 (GIL Loop 2, Pool max 8)"]
        end
        subgraph SVC3["Service: pipeline-sim-swap (cg-sim-swap)"]
            SIM1["Member 1..4 (GIL Loop 3, Pool max 8)"]
        end
    end

    subgraph STORAGELAYER["Storage & State Layer"]
        direction TB
        REDIS[("Redis / Redis Sentinel<br/>- ip-ggsn:* / ggsn-ips:*<br/>- device:* / sim:* cache")]
        POSTGRES[("PostgreSQL Database (4 CPUs)<br/>- msisdn_device / msisdn_sim<br/>- device_swap_history / sim_swap_history<br/>- radius_session_state<br/>- audit_log<br/>- notification_log (Outbox)")]
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
    PRODUCER -->|Produce acks=1; capture durable| TOPIC
    PRODUCER -.->|Malformed Msg| DLQ

    TOPIC --> SVC1
    TOPIC --> SVC2
    TOPIC --> SVC3

    SVC1 -->|Atomic EVALSHA Lua| REDIS
    SVC1 -->|Session Batch Upsert| POSTGRES
    SVC2 -->|Batch MGET/MSET| REDIS
    SVC2 -->|Atomic Batch Tx| POSTGRES
    SVC3 -->|Batch MGET/MSET| REDIS
    SVC3 -->|Atomic Batch Tx| POSTGRES

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
    participant NAS as Capture Server / Test Sender
    participant ING as RadiusLogProducer
    participant KFK as Kafka Broker
    participant CSM as Consumer Modules
    participant RDS as Redis
    participant PGS as PostgreSQL
    participant DSP as NotificationDispatcher
    participant SUB as Open Gateway Subscriber

    Note over NAS,ING: Passive mirror ingestion
    NAS->>ING: UDP Accounting-Request (Code=4, Authenticator, AVPs)
    ING->>ING: Decode, validate và đưa vào bounded RAM queue
    ING->>KFK: Batch Produce (key=MSISDN, acks=1)
    KFK-->>ING: Xác nhận ghi nội bộ Kafka

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

4. **Passive Mirror Ingestion & Bounded Buffer**:
   - Capture server ngoài repo chịu trách nhiệm bền vững và RADIUS ACK/response; ingestion chỉ nhận mirror một chiều.
   - Queue RAM có giới hạn hấp thụ burst ngắn. Queue đầy hoặc Kafka publish lỗi được tính là `data_loss` và phải cảnh báo để replay từ capture server.
   - Profile mặc định dùng Kafka `acks=1`, không idempotence, vì capture ngoài repo chịu durability/replay. Có thể đổi sang `acks=all` + idempotence nếu hợp đồng nguồn thay đổi, nhưng phải hạ admission limit và đo lại p95.

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
│   │   ├── producer.py              # Passive UDP/CSV → Kafka ingestion
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

# Cách B: Giả lập capture server mirror gói tin UDP qua cổng 1813
python -m pipeline.ingestion.radius_udp_sender --csv data/radius_sample.csv --rate 15000
```

Sender là công cụ fire-and-forget có pacing; `--rate` là trần lưu lượng UDP mới.
Nó không nhận response, không retry và không dùng để chứng minh độ bền dữ liệu.

---

## 6. Biến môi trường quan trọng (Configuration Reference)

| Tên Biến Môi Trường | Giá Trị Mặc Định | Ý Nghĩa / Mục Đích |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `camara-kafka:9092,camara-kafka-2:9092,camara-kafka-3:9092` | Danh sách địa chỉ Kafka Cluster Brokers |
| `KAFKA_TOPIC_RAW` | `radius.accounting.raw` | Tên topic Kafka chứa log thô |
| `KAFKA_TOPIC_PARTITIONS` | `9` | Số partition của profile 12 vCPU/8 GiB; phải chia hết hợp lý cho số consumer |
| `PIPELINE_GROUPS` | `""` (All) | Chọn consumer group cho tiến trình (`ip-msisdn`, `device-swap`, `sim-swap`) |
| `*_CONSUMERS_PER_GROUP` | `3` | Số member của từng group; cấu hình riêng cho IP, device-swap và sim-swap |
| `*_PARTITION_CONCURRENCY` | `3` | Ba partition/member chạy đồng thời; cùng MSISDN vẫn giữ FIFO trong một partition |
| `*_PARTITION_QUEUE_RECORDS` | IP=`64`, Swap=`96` | Queue cục bộ tối đa bốn batch; Kafka giữ durable backlog |
| `*_BATCH_MAX_RECORDS` | IP=`16`, Swap=`24` | Micro-batch latency-first giữ transaction trong ngân sách p95 |
| `*_BATCH_TIMEOUT_MS` | `1` | Thời gian gom batch tối đa |
| `PROCESSING_PARTITION_QUEUE_*` | `75%/25%`, `12ms/4ms` | Backpressure theo cả depth và tuổi record; tránh pause/resume quá nhạy với jitter ngắn |
| `PROCESSING_COMMIT_INTERVAL_MS` / `MAX_RECORDS` | `25` / `512` | Coalesce commit offset ở coordinator nền, giảm request tới Kafka; không nằm trên critical path nghiệp vụ |
| `DATABASE_URL` | `postgresql://postgres:camara@camara-postgres:5432/camara_db` | Connection string PostgreSQL (`synchronous_commit=on` đảm bảo 100% ACID) |
| `IP_MSISDN_DB_POOL_MAX` / `DEVICE_SWAP...` / `SIM_SWAP...` | `12` / `8` / `8` | Connection Pool `asyncpg` tối đa phân chia theo từng service (PostgreSQL `max_connections=200`) |
| `POSTGRES_CPUS` | `2.5` | CPU PostgreSQL trong profile 12 vCPU/8 GiB; ưu tiên vì PG/state fallback là bottleneck chung của cả ba group |
| `REDIS_HOST` / `REDIS_PORT` | `camara-redis` / `6379` | Thông tin kết nối Redis Standalone / Cluster |
| `RADIUS_SHARED_SECRET` | `camara-radius-dev-secret` | Secret key tính Authenticator RFC 2866 |
| `RADIUS_UDP_RECEIVE_BUFFER_BYTES` | `33554432` (32MB) | Kích thước socket buffer nhận UDP |
| `RADIUS_UDP_QUEUE_MAX_RECORDS` | `20000` | Burst buffer profile 8 GiB; capture mới là nguồn bền vững |
| `RADIUS_UDP_KAFKA_BATCH_RECORDS` | `64` | Kích thước tối đa của micro-batch UDP Ingestion |
| `RADIUS_UDP_KAFKA_BATCH_WAIT_MS` | `1` | Thời gian gom batch Kafka của Ingestion (ms) |
| `RADIUS_UDP_KAFKA_MAX_INFLIGHT_BATCHES_PER_WORKER` | `6` | Số batch Kafka baseline cho mỗi worker; tổng vẫn bị chặn bởi global inflight limit |
| `RADIUS_UDP_KAFKA_PRESSURE_INFLIGHT_BATCHES_PER_WORKER` | `6` | Trần tạm thời khi queue shard vượt ngưỡng pressure |
| `RADIUS_UDP_KAFKA_PRESSURE_QUEUE_RATIO` | `0.25` | Tỷ lệ queue kích hoạt concurrency tăng cường |
| `RADIUS_UDP_KAFKA_PRODUCERS` | `1` | Producer dùng chung cho bốn publisher coroutine; Kafka giữ FIFO theo partition/key và gom batch tốt hơn |
| `RADIUS_UDP_KAFKA_TOTAL_MAX_INFLIGHT_BATCHES` | `24` | Trần tuyệt đối batch Kafka đang ghi trên toàn process |
| `INGESTION_KAFKA_ACKS` / `INGESTION_ENABLE_IDEMPOTENCE` | `1` / `false` | Latency-first vì capture ngoài repo chịu durability/replay |
| `PIPELINE_RECOMMENDED_SUSTAINED_PPS` | `800` | Baseline bảo thủ trước soak test cho E2E p95 < 100ms |
| `SWAP_CHECKPOINT_INTERVAL_MS` | `150` | Cửa sổ gom watermark không phát sinh SIM/Device swap |
| `SWAP_CHECKPOINT_MAX_RECORDS` | `256` | Số state kích hoạt một bulk PostgreSQL checkpoint |
| `SWAP_CHECKPOINT_QUEUE_RECORDS` | `4096` | Backpressure bound cho checkpoint mỗi consumer member |
| `PROCESSING_COMBINE_WAIT_MS` | `2` | Cửa sổ gom ghi giữa các partition trong cùng process |
| `PROCESSING_COMBINE_MAX_RECORDS` | `64` | Soft limit record cho một lần gọi persistence đã gom |
| `RADIUS_UDP_PUBLISHER_WORKERS` | `4` | Số lượng worker coroutines publish song song (Key-sharded per MSISDN) |
| `INGESTION_METRICS_PORT` | `9201` | Cổng Exporter Prometheus Ingestion (tự động thử 9201-9210 nếu bận) |
| `DISPATCHER_BATCH_SIZE` | `50` | Số lượng notification claim mỗi vòng lặp của Dispatcher |
| `METRICS_PORT` | `9200` | Port expose `/metrics` cho Prometheus scraper |

---

### Chọn profile phần cứng

`.env` và `.env.example` là profile 12 vCPU/8 GiB. Các profile lớn hơn chỉ
override tài nguyên và tham số song song, không chứa secret:

```bash
bash scripts/run_pipeline.sh config/env/16gb.env
bash scripts/run_pipeline.sh config/env/32gb.env
bash scripts/run_pipeline.sh config/env/64gb.env
```

Xem giả định CPU, ngân sách và lưu ý đổi partition tại
[`config/env/README.md`](config/env/README.md).

---

## 7. Giám sát Telemetry, Bảo Mật & Phục hồi Thảm họa (Security & Disaster Recovery)

### 7.1. Giám sát Telemetry & Split Metrics
Mỗi `THROUGHPUT_LOG_INTERVAL_SECONDS` (mặc định 10 giây), hệ thống ghi log phân khối trực quan (`|`) cho cả hai chặng:
- **`[INGESTION]`**: `udp_in`, `kafka_persisted`, throughput `gap`, queue/backlog, batch size, persistence p50/p95/p99, queue residence p95, `worker_slot_wait_p95`, `global_wait_p95` và `data_loss` (`queue_dropped`, `publish_failed`).
- **`[PROCESSING]`**: Log chi tiết cho từng member: throughput, độ trễ batch/chặng, **E2E `avg/p95/max`**, trạng thái `SLO_BREACH` khi p95 >=100 ms, swap/event và data loss.

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
- 📖 [**Báo cáo Kỹ thuật Chuyên sâu các File Trọng điểm**](docs/BAO_CAO_KY_THUAT_PIPELINE.md): Mô tả giải mã RFC 2866/3GPP VSA, passive mirror ingestion, `SO_REUSEPORT`, key-sharded queues, bounded Kafka inflight, per-partition temporal pipeline, transaction batch, fencing tuple, Lua scripts, E2E lag và Transactional Outbox.
- 🚀 [**Kế hoạch refactor hiệu năng ingestion**](docs/INGESTION_PERFORMANCE_REFACTOR_PLAN.md): Baseline 15k/s, nguyên nhân throughput/E2E/data loss, thay đổi đã triển khai và tiêu chí benchmark nghiệm thu.
- 📖 [**Disaster Recovery Runbook & HA Operational Guide**](docs/DISASTER_RECOVERY_RUNBOOK.md): Hướng dẫn vận hành sự cố, sao lưu Point-in-Time Recovery (PITR) và quy trình Failover Kafka/PostgreSQL.
