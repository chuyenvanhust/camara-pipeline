# HƯỚNG DẪN CẤU HÌNH & TỐI ƯU HÓA PHẦN CỨNG (HARDWARE TUNING GUIDE)

> **Tài liệu Hướng dẫn Khả chuyển (Portability & Hardware Sizing Guide)**: Giải thích các thông số trong `.env` và cách hiệu chỉnh trên phần cứng đích. Mọi throughput trong profile là candidate benchmark, không phải cam kết 30.000+ pkt/s hay hard admission limit.

---

## MỤC LỤC

1. [Tổng Quan Về Cơ Chế Cấu Hình Bằng Biến Môi Trường](#1-tổng-quan-về-cơ-chế-cấu-hình-bằng-biến-môi-trường)
2. [Danh Mục Giải Thích Chi Tiết Các Thông Số (.env Reference)](#2-danh-mục-giải-thích-chi-tiết-các-thông-số-env-reference)
   - 2.1. Hạ tầng Apache Kafka Cluster & ZooKeeper
   - 2.2. Cơ sở dữ liệu PostgreSQL (Storage & Session State)
   - 2.3. CSDL In-Memory Redis & Cache
   - 2.4. Tầng Ingestion Tiếp Nhận RADIUS UDP (Producer)
   - 2.5. Tầng Xử Lý Pipeline Consumers (Consumer Modules)
   - 2.6. Transactional Outbox Dispatcher (Webhook Notifications)
   - 2.7. API Gateway & Security
   - 2.8. Telemetry, Exporter & Monitoring
3. [Bảng Cấu Hình Mẫu Theo Phần Cứng (Hardware Sizing Matrix)](#3-bảng-cấu-hình-mẫu-theo-phần-cứng-hardware-sizing-matrix)
4. [Các Công Thức Tính Toán Kỹ Thuật (Sizing Formulas)](#4-các-công-thức-tính-toán-kỹ-thuật-sizing-formulas)
5. [Quy Trình Triển Khai Trên Máy Chủ Mới](#5-quy-trình-triển-khai-trên-máy-chủ-mới)

---

## 1. Tổng Quan Về Cơ Chế Cấu Hình Bằng Biến Môi Trường

Hệ thống **CAMARA Network API Data Pipeline** tuân thủ 100% nguyên lý **Twelve-Factor App**:
- Toàn bộ thông số môi trường, secret key, tài nguyên CPU/RAM, kích thước buffer, connection pool và tham số tuning của thuật toán được **tập trung hóa 100% tại file `.env`**.
- File `docker-compose.yml` và `docker-compose.prod.yml` chỉ đóng vai trò template tham chiếu các biến từ `.env`.
- Khi chuyển đổi sang hệ thống phần cứng mới (nhiều CPU hơn, RAM lớn hơn hoặc ổ đĩa NVMe tốc độ cao hơn), người vận hành **CHỈ CẦN THAY ĐỔI FILE `.env`** mà không cần sửa bất kỳ dòng mã nguồn hay file cấu hình Docker nào.

---

## 2. Danh Mục Giải Thích Chi Tiết Các Thông Số (.env Reference)

### 2.1. Hạ tầng Apache Kafka Cluster & ZooKeeper

| Tên Biến Môi Trường | Mặc Định | Đơn Vị | Ý Nghĩa Kỹ Thuật & Tác Động Khi Điều Chỉnh |
|---|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `camara-kafka:9092,...` | Host:Port | Danh sách địa chỉ kết nối Kafka Brokers. Khi đổi sang Kafka external/managed cluster (như MSK), thay đổi chuỗi này. |
| `KAFKA_TOPIC_RAW` | `radius.accounting.raw` | String | Tên topic Kafka chứa log thô tiếp nhận từ RADIUS Ingestion. |
| `KAFKA_TOPIC_PARTITIONS` | `9` | Integer | Số partition của profile 8 GiB. **Tác động**: quyết định trần scale ngang; nên chọn số chia đều cho consumer của từng group. |
| `KAFKA_REPLICATION_FACTOR` | `3` | Integer | Số lượng bản sao lưu của từng partition trên Kafka cluster. `3` nghĩa là mỗi partition có 1 Leader + 2 Followers. |
| `KAFKA_MIN_INSYNC_REPLICAS` | `2` | Integer | Số lượng broker replicas tối thiểu phải đồng bộ ISR khi Producer publish với `acks=all`. |
| `KAFKA_NUM_NETWORK_THREADS` | `8` | Integer | Số thread xử lý I/O mạng của Kafka Broker. Tăng lên 16-32 khi máy chủ có >16 vCPU. |
| `KAFKA_NUM_IO_THREADS` | `16` | Integer | Số thread xử lý truy xuất đĩa đĩa của Kafka Broker. Tăng khi sử dụng mảng ổ đĩa NVMe/SSD. |
| `ZOOKEEPER_MEM_LIMIT` | `192m` | RAM | Giới hạn RAM tối đa cấp cho Container ZooKeeper. |
| `KAFKA_MEM_LIMIT` | `900m` | RAM | Giới hạn RAM mỗi broker, gồm JVM heap và phần còn lại cho native/page cache. |
| `KAFKA_CPUS` | `1.5` | Cores | Giới hạn CPU **mỗi broker** trong profile 12 vCPU/8 GiB; ba broker có tổng quota 4,5 CPU. |
| `KAFKA_HEAP_OPTS` | `-Xms448M -Xmx448M` | JVM | Heap thực sự truyền vào cả ba broker; không đặt heap sát `mem_limit`. |

---

### 2.2. Cơ sở dữ liệu PostgreSQL (Storage & Session State)

| Tên Biến Môi Trường | Mặc Định | Đơn Vị | Ý Nghĩa Kỹ Thuật & Tác Động Khi Điều Chỉnh |
|---|---|---|---|
| `POSTGRES_LOCAL_USER` | `postgres` | String | Tên user quản trị PostgreSQL. |
| `POSTGRES_LOCAL_PASSWORD` | *(Mã hóa dev)* | Secret | Mật khẩu truy cập PostgreSQL. **Phải đổi khi lên Prod!** |
| `POSTGRES_LOCAL_DB` | `camara_db` | String | Tên cơ sở dữ liệu chính của dự án. |
| `DATABASE_URL` | `postgresql://...` | DSN | Connection string đầy đủ kết nối tới PostgreSQL (`synchronous_commit=on` bảo toàn ACID 100%). |
| `POSTGRES_MAX_CONNECTIONS` | `80` | Connections | Trần số kết nối của profile 8 GiB; cần cân đối với tổng pool và RAM. |
| `POSTGRES_SHARED_BUFFERS` | `256MB` | RAM | Cache dữ liệu/index của PostgreSQL trong profile cơ sở. |
| `POSTGRES_EFFECTIVE_CACHE_SIZE` | `768MB` | RAM | Ước tính cache khả dụng cho Query Planner; đây không phải vùng cấp phát trực tiếp. |
| `POSTGRES_WORK_MEM` | `4MB` | RAM | Bộ nhớ cho **mỗi phép toán** sort/hash; một query có thể cấp phát nhiều lần. |
| `POSTGRES_MAX_WAL_SIZE` | `1GB` | Storage | Trần WAL trước checkpoint trong profile cơ sở. |
| `IP_MSISDN_DB_POOL_MAX` | `4` | Connections/replica | Kích thước pool tối đa trên mỗi process IP của profile 8 GiB. |
| `DEVICE_SWAP_DB_POOL_MAX` | `4` | Connections/replica | Kích thước pool tối đa trên mỗi process Device Swap. |
| `SIM_SWAP_DB_POOL_MAX` | `4` | Connections/replica | Kích thước pool tối đa trên mỗi process SIM Swap. |
| `POSTGRES_MEM_LIMIT` | `1g` | RAM | Giới hạn RAM PostgreSQL của profile cơ sở. |
| `POSTGRES_CPUS` | `2.5` | Cores | CPU PostgreSQL của profile cơ sở; bottleneck dùng chung cho mapping và hai nhánh swap. |

---

### 2.3. CSDL In-Memory Redis & Cache

| Tên Biến Môi Trường | Mặc Định | Đơn Vị | Ý Nghĩa Kỹ Thuật & Tác Động Khi Điều Chỉnh |
|---|---|---|---|
| `REDIS_HOST` | `camara-redis` | Host | Địa chỉ host kết nối Redis Server. |
| `REDIS_PORT` | `6379` | Port | Cổng kết nối Redis TCP. |
| `REDIS_PASSWORD` | *(Mã hóa dev)* | Secret | Mật khẩu truy cập Redis Server. |
| `REDIS_SENTINELS` | `""` | Host:Port | Danh sách Sentinel nodes phục vụ chế độ High Availability (HA) trên Production. |
| `REDIS_MASTER_NAME` | `camara-master` | String | Tên master group trong Redis Sentinel cluster. |
| `REDIS_MAXMEMORY` | `256mb` | RAM | Bộ nhớ tối đa cho dataset Redis. |
| `REDIS_MEM_LIMIT` | `384m` | RAM | Container limit, cao hơn dataset để chừa overhead. |
| `REDIS_CPUS` | `0.5` | Cores | Quota Redis trong profile 8 GiB; theo dõi throttling và Redis p95 trước khi tăng throughput. |

---

### 2.4. Tầng Ingestion Tiếp Nhận RADIUS UDP (Producer)

| Tên Biến Môi Trường | Mặc Định | Đơn Vị | Ý Nghĩa Kỹ Thuật & Tác Động Khi Điều Chỉnh |
|---|---|---|---|
| `RADIUS_SHARED_SECRET` | *(Dev secret)* | Secret | Khóa bí mật dùng tính MD5 Request Authenticator theo chuẩn RFC 2866. |
| `RADIUS_UDP_RECEIVE_BUFFER_BYTES` | `16777216` (16MB) | Bytes | Mức ứng dụng yêu cầu; giá trị thực phụ thuộc `net.core.rmem_max` và phải đọc từ log `receive_buffer_actual`. |
| `RADIUS_UDP_QUEUE_MAX_RECORDS` | `20000` | Records | Burst buffer ngắn trước Kafka. Queue không phải durable storage; queue lớn che giấu overload và trực tiếp làm tăng E2E. |
| `RADIUS_UDP_KAFKA_BATCH_RECORDS` | `64` | Records | Micro-batch latency-first; tăng số lane để scale thay vì tăng batch. |
| `RADIUS_UDP_KAFKA_BATCH_WAIT_MS` | `1` | ms | Ngân sách chờ gom batch phía ingestion. |
| `RADIUS_UDP_KAFKA_MAX_INFLIGHT_BATCHES_PER_WORKER` | `6` | Batches | Baseline trên mỗi worker; global limit 24 ngăn tổng concurrency vượt ngân sách. |
| `RADIUS_UDP_KAFKA_PRESSURE_INFLIGHT_BATCHES_PER_WORKER` | `6` | Batches | Trần tạm thời khi queue shard vượt ngưỡng pressure. |
| `RADIUS_UDP_KAFKA_PRESSURE_QUEUE_RATIO` | `0.25` | Ratio | Tỷ lệ queue shard kích hoạt pressure concurrency sớm. |
| `RADIUS_UDP_KAFKA_PRODUCERS` | `1` | Producers | Producer dùng chung cho bốn shard coroutine. FIFO do key/partition bảo đảm; accumulator chung giảm batch fragmentation. |
| `RADIUS_UDP_KAFKA_TOTAL_MAX_INFLIGHT_BATCHES` | `24` | Batches | Trần tuyệt đối toàn process của profile 8 GiB. |
| `INGESTION_BATCH_SIZE_BYTES` | `262144` | Bytes | Buffer batch tối đa của Kafka producer. |
| `RADIUS_UDP_PUBLISHER_WORKERS` | `4` | Workers | Số lượng worker coroutines publish song song (đã được định tuyến theo MSISDN Key Sharding). |
| `INGESTION_KAFKA_ACKS` / `INGESTION_ENABLE_IDEMPOTENCE` | `1` / `false` | - | Chỉ chờ leader vì capture server ngoài repo là nguồn bền vững/replay. Dùng `all/true` nếu thay đổi hợp đồng durability. |
| `INGESTION_KAFKA_PERSIST_WARN_MS` | `18` | ms | Ngưỡng cảnh báo profile 8 GiB; profile lớn dùng 30ms trong ngân sách E2E 100ms. |
| `INGESTION_QUEUE_WARN_MS` | `4` | ms | Ngưỡng profile 8 GiB; profile lớn dùng 12ms. Đây là cảnh báo, không drop. |
| `RADIUS_INGESTION_MEM_LIMIT` | `1g` | RAM | Giới hạn RAM Docker cấp cho Container Ingestion. |
| `RADIUS_INGESTION_CPUS` | `1` | Cores | Hard quota của riêng ingestion profile 8 GiB; tăng khi CPU throttling đi kèm queue residence/kernel `RcvbufErrors`. |

---

### 2.5. Tầng Xử Lý Pipeline Consumers (Consumer Modules)

| Tên Biến Môi Trường | Mặc Định | Đơn Vị | Ý Nghĩa Kỹ Thuật & Tác Động Khi Điều Chỉnh |
|---|---|---|---|
| `PIPELINE_GROUPS` | `""` | String | Chọn group kích hoạt cho worker container (`ip-msisdn`, `device-swap`, `sim-swap`). Nếu rỗng sẽ khởi chạy cả 3. |
| `*_CONSUMERS_PER_GROUP` | `1` | Members/process | Bắt buộc bằng 1; scale process bằng `PIPELINE_*_REPLICAS`. |
| `*_PARTITION_CONCURRENCY` | `3` | Workers | Ba partition/member chạy đồng thời; FIFO theo partition vẫn được giữ. |
| `IP_MSISDN_PARTITION_QUEUE_RECORDS` / `*_SWAP_PARTITION_QUEUE_RECORDS` | `64` / `96` | Records/partition | Tối đa bốn batch cục bộ; Kafka mới là durable backlog. |
| `PROCESSING_PARTITION_QUEUE_HIGH_RATIO` / `LOW_RATIO` | `0.75` / `0.25` | Ratio | High/low watermark cho backpressure riêng partition. |
| `PROCESSING_PARTITION_QUEUE_MAX_AGE_MS` / `RESUME_AGE_MS` | `12` / `4` | ms | Backpressure theo tuổi record với hysteresis đủ rộng để tránh flapping do jitter ngắn. |
| `PROCESSING_COMMIT_INTERVAL_MS` / `MAX_RECORDS` | `25` / `512` | ms / records | Coalesce offset commit ở coordinator nền, giảm request Kafka; không đổi thời điểm xác nhận business write. |
| `IP_MSISDN_BATCH_MAX_RECORDS` / `*_SWAP_BATCH_MAX_RECORDS` | `48` / `64` | Records | Trần coalescing hiện hành; IP nhỏ hơn vì ghi cả PG và Redis cho mọi event. |
| `*_BATCH_TIMEOUT_MS` | `5` | ms | Poll/coalescing wait tối đa; worker không cố chờ đủ batch khi FIFO đã hết. |
| `THROUGHPUT_LOG_INTERVAL_SECONDS` | `10` | Giây | Chu kỳ in log thống kê thông lượng telemetry nội bộ. |
| `PIPELINE_IP_MEM_LIMIT` / `DEVICE...` / `SIM...` | `192m` / `160m` / `160m` | RAM/replica | Container limit trên mỗi replica profile 8 GiB. |
| `PIPELINE_*_CPU_SHARES` | `1024 / 768 / 768` | Weight | Trọng số tương đối khi CPU contention; pipeline không dùng hard `cpus` quota để tránh CFS throttling gây tail latency. |
| `PROCESSING_COMBINE_WAIT_MS` / `MAX_RECORDS` | `2 / 64` | ms / records | Gom batch từ các partition độc lập ở cấp process trước một lần ghi downstream. |
| `PIPELINE_SLA_E2E_P95_MS` | `100` | ms | Log chuyển thành `SLO_BREACH` nếu p95 cửa sổ vượt ngưỡng. |

---

### 2.6. Transactional Outbox Dispatcher (Webhook Notifications)

| Tên Biến Môi Trường | Mặc Định | Đơn Vị | Ý Nghĩa Kỹ Thuật & Tác Động Khi Điều Chỉnh |
|---|---|---|---|
| `DISPATCHER_BATCH_SIZE` | `50` | Items | Số lượng thông báo claim mỗi lần quét bằng câu lệnh `FOR UPDATE SKIP LOCKED`. |
| `DISPATCHER_POLL_INTERVAL` | `2.0` | Giây | Chu kỳ lặp lại việc claim thông báo mới trong cơ sở dữ liệu. |
| `DISPATCHER_MAX_ATTEMPTS` | `5` | Thử lại | Số lần thử lại tối đa khi gửi Webhook HTTP POST thất bại trước khi đánh dấu `DEAD`. |
| `WEBHOOK_SIGNING_SECRET` | *(Dev secret)* | Secret | Khóa bí mật dùng để ký chữ ký HMAC SHA-256 (`X-Signature-SHA256`) đính kèm Header HTTP. |
| `DISPATCHER_MEM_LIMIT` | `256m` | RAM | Giới hạn RAM Docker cấp cho Container Dispatcher. |

---

### 2.7. API Gateway & Security

| Tên Biến Môi Trường | Mặc Định | Đơn Vị | Ý Nghĩa Kỹ Thuật & Tác Động Khi Điều Chỉnh |
|---|---|---|---|
| `ENVIRONMENT` | `development` | String | Môi trường thực thi (`development` hoặc `production`). **Ở chế độ `production`, hệ thống fail-fast nếu dùng mật khẩu dev.** |
| `API_KEY` | *(Dev secret)* | Secret | API Key tĩnh dùng cho các ứng dụng legacy hoặc môi trường test. |
| `OAUTH_ISSUER_URL` | `https://auth...` | URL | Địa chỉ Keycloak / OIDC Issuer phục vụ xác thực JWT Bearer Token. |
| `OAUTH_CLIENT_ID` | `camara-gateway` | String | Client ID đăng ký trên hệ thống OAuth2 Server. |
| `FORWARDED_ALLOW_IPS` | `127.0.0.1` | IP List | Danh sách IP Proxy/Load Balancer được phép chuyển tiếp thông tin client IP. |
| `FASTAPI_MEM_LIMIT` | `512m` | RAM | Giới hạn RAM Docker cấp cho Container FastAPI Gateway. |

---

### 2.8. Telemetry, Exporter & Monitoring

| Tên Biến Môi Trường | Mặc Định | Đơn Vị | Ý Nghĩa Kỹ Thuật & Tác Động Khi Điều Chỉnh |
|---|---|---|---|
| `METRICS_PORT_IP_MSISDN` | `9200` | TCP Port | Cổng HTTP Exporter cho Prometheus scrape chỉ số của service `pipeline-ip-msisdn`. |
| `METRICS_PORT_DEVICE_SWAP` | `9202` | TCP Port | Cổng HTTP Exporter cho Prometheus scrape chỉ số của service `pipeline-device-swap`. |
| `METRICS_PORT_SIM_SWAP` | `9203` | TCP Port | Cổng HTTP Exporter cho Prometheus scrape chỉ số của service `pipeline-sim-swap`. |
| `INGESTION_METRICS_PORT` | `9201` | TCP Port | Cổng HTTP Exporter cho Prometheus scrape chỉ số của Ingestion Producer. |
| `GRAFANA_ADMIN_USER` | `admin` | String | Tài khoản quản trị Grafana Dashboard. |
| `GRAFANA_ADMIN_PASSWORD` | *(Dev secret)* | Secret | Mật khẩu quản trị Grafana Dashboard. |
| `PROMETHEUS_MEM_LIMIT` | `256m` | RAM | Giới hạn RAM Docker cấp cho Container Prometheus. |
| `GRAFANA_MEM_LIMIT` | `256m` | RAM | Giới hạn RAM Docker cấp cho Container Grafana. |

---

## 3. Bảng Cấu Hình Mẫu Theo Phần Cứng (Hardware Sizing Matrix)

> ⚠️ **Lưu ý**: Đây là ngân sách khởi điểm, không phải cam kết throughput. Mỗi profile phải qua soak test 30-60 phút trên phần cứng đích, đo CPU throttling, Kafka lag, UDP kernel drops và p95/p99 E2E. Các file thực thi nằm trong `.env` và `config/env/*.env`.

| Biến | 8 GiB / 12 CPU | 16 GiB / 16 CPU | 32 GiB / 24 CPU | 64 GiB / 32 CPU |
|---|---:|---:|---:|---:|
| Observe-only sustained target | **800/s baseline** | **3.9k/s candidate** | **7.8k/s candidate** | **15.5k/s candidate** |
| Observe-only burst target | 900/s | 3.9k/s | 7.8k/s | 15.5k/s |
| SLO bắt buộc | p95 <100ms | p95 <100ms | p95 <100ms | p95 <100ms |
| Kafka partitions | 9 | 12 | 24 | 48 |
| Kafka replication factor | 1 (benchmark) | 3 | 3 | 3 |
| Kafka RAM / broker | 1200m | 1536m | 3g | 6g |
| Kafka heap / broker | 448M | 768M | 1536M | 3G |
| Kafka CPU / broker | 1.5 | 1.5 | 2.5 | 2.5 |
| PostgreSQL RAM / CPU | 1500m / 2.5 | 2560m / 2.5 | 6g / 4.5 | 12g / 6 |
| Redis container / dataset / CPU | 384m / 256mb / 0.5 | 1g / 768mb / 1 | 2g / 1536mb / 1.5 | 4g / 3gb / 2 |
| Ingestion queue | 20k | 30k | 60k | 120k |
| Ingestion batch / wait / linger | 64 / 1ms / 0ms | 64 / 1ms / 0ms | 64 / 1ms / 0ms | 64 / 1ms / 0ms |
| Ingestion workers / producers | 4 / 1 | 4 / 1 | 8 / 2 | 8 / 4 |
| Ingestion RAM / CPU | 1g / 1 | 1g / 1.5 | 1536m / 2.5 | 3g / 3 |
| IP replicas x members / concurrency | 3x1 / 3 | 4x1 / 3 | 6x1 / 4 | 8x1 / 6 |
| IP partition batch / fetch wait / CPU | 48 / 5ms / no hard cap | 48 / 5ms / no hard cap | 48 / 5ms / no hard cap | 48 / 5ms / no hard cap |
| Swap replicas x members / concurrency | 3x1 / 3 | 4x1 / 3 | 6x1 / 4 | 8x1 / 6 |
| Swap partition batch / fetch wait / CPU | 64 / 5ms / no hard cap | 64 / 5ms / no hard cap | 64 / 5ms / no hard cap | 64 / 5ms / no hard cap |
| Process write combine wait / max | 2ms / 64 | 2ms / 64 | 2ms / 96 | 2ms / 128 |
| Swap checkpoint interval | 150ms | 150ms | 150ms | 150ms |

Mỗi member là một container/PID Python (`CONSUMERS_PER_GROUP=1`); không chạy ba
consumer coroutine trong cùng interpreter. Profile 8 GiB dùng 3 replica cho 9
partition để mỗi process nhận trung bình 3 partition. Không đặt hard CPU quota
cho worker; capture server chịu trách nhiệm điều tiết/replay, còn `cpu_shares` ưu
tiên IP khi host tranh chấp. Các biến `PIPELINE_RECOMMENDED_*_PPS` chỉ tạo cảnh
báo capacity và không giới hạn hay loại gói tại ingestion.

Mỗi process tự sở hữu PostgreSQL pool và Redis client. PostgreSQL gắn
`application_name` theo module; Redis dùng DB 0/1/2 tương ứng IP/Device/SIM.
PostgreSQL vật lý vẫn là một cluster để giữ transaction outbox, audit,
subscription và API nhất quán; tách cluster là một data-migration riêng.

Trong IP-MSISDN, hai store được ghi **đồng thời**. Sau khi cả hai thành công,
worker chỉ công bố offset đã bền cho commit coordinator; coordinator coalesce offset
mỗi 25ms hoặc 512 record rồi commit ngoài critical path. Vì vậy batch latency tiến
gần `max(pg, redis)` và không cộng thêm một Kafka commit round-trip cho mỗi batch.
Crash có thể replay cửa sổ chưa commit; version fence làm lần ghi lặp lại idempotent.

E2E được đo từ `ingest_epoch_ns` đóng dấu ngay sau khi application nhận UDP đến
sau khi business state đã ghi xong. `pre_process_p95` bao gồm ingestion/Kafka/queue,
`processing_p95` đo riêng `process_batch`, còn offset commit được báo cáo riêng.
Không cộng các p95 này để suy ra E2E p95 vì chúng không nhất thiết thuộc cùng record.

Khởi chạy profile override:

```bash
bash scripts/run_pipeline.sh config/env/16gb.env
```

Thay tên profile tương ứng. Nếu máy tăng RAM nhưng vẫn chỉ có 12 CPU, giữ các biến `*_CPUS` của `.env`; không áp nguyên profile CPU lớn hơn.

---

## 4. Các Công Thức Tính Toán Kỹ Thuật (Sizing Formulas)

Khi tự điều chỉnh thông số cho cấu hình phần cứng bất kỳ, áp dụng các công thức toán học sau:

### 1. Thời Gian Hấp Thụ Tràn Hàng Đợi RAM Ingestion (Hold Time)
$$T_{\text{hold (seconds)}} = \frac{\text{RADIUS\_UDP\_QUEUE\_MAX\_RECORDS}}{\text{Input Packet Rate (pkt/s)}}$$
*Ví dụ*: Queue 20.000 bản ghi ở 800 pkt/s là 25 giây đệm lỗi,
không phải ngân sách latency. Partition bị pause theo tuổi record 12ms; khi queue
residence tăng, capture phải giảm tốc/replay.

### 2. Dung Lượng Inflight Kafka Tối Đa Toàn Hệ Thống (Total Inflight Capacity)
$$\text{Required batches}=\left\lceil\frac{\text{target records/s}\times\text{persist latency s}}{\text{batch records}}\right\rceil$$
$$\text{Capacity}_{\text{inflight}} = \text{inflight batches} \times \text{batch records}$$
*Ví dụ*: candidate 3,9k/s, persist p95 30ms và batch 64 cần ít nhất 2 batch theo BDP.
Profile cấp inflight headroom cho tail ngắn. Capture server phải điều tiết tải
sustained vượt profile; UDP ingestion chỉ quan sát/cảnh báo và không được loại gói
theo ngưỡng cấu hình.

### 3. Ngân Sách Kết Nối PostgreSQL (Connection Budget)
$$\text{Total} = 10 + R_{ip}P_{ip} + R_{dev}P_{dev} + R_{sim}P_{sim} + 5 + 10$$
với $R$ là số replica và $P$ là pool max của mỗi replica. Ví dụ profile 16 GiB:
$10 + 4\times8 + 4\times4 + 4\times4 + 5 + 10 = \mathbf{89}$ connection,
nằm dưới `POSTGRES_MAX_CONNECTIONS=120` và còn 31 connection dự phòng.

> **Lưu ý**: Các consumer trong cùng replica dùng chung một pool. Khi scale process,
> phải nhân pool với replica như công thức trên; không nhân với số member coroutine.

### 4. Tổng Dung Lượng RAM Sử Dụng Cho PostgreSQL
$$\text{RAM}_{\text{Postgres}} \approx \text{POSTGRES\_SHARED\_BUFFERS} + (\text{POSTGRES\_MAX\_CONNECTIONS} \times \text{POSTGRES\_WORK\_MEM} \times N_{\text{sort\_ops}})$$
*Ví dụ (Staging defaults)*: $512\text{MB} + (200 \times 8\text{MB} \times 1) = 512\text{MB} + 1.6\text{GB} = \mathbf{2.1\text{GB RAM}}$ (đặt `POSTGRES_MEM_LIMIT=3g` để dư margin ~40%).

> ⚠️ **Lưu ý**: `work_mem` được cấp cho **mỗi phép toán sort/hash** chứ không phải mỗi connection. Một query phức tạp có thể dùng 2-4 lần `work_mem`. Giá trị thực tế tiêu thụ có thể cao hơn công thức lý thuyết. Khuyến nghị theo dõi RSS/shared memory thực tế bằng `docker stats` dưới tải peak.

---

## 5. Quy Trình Triển Khai Trên Máy Chủ Mới

Khi di chuyển dự án sang hệ thống máy chủ mới:

### Bước 1: Sao chép file cấu hình mẫu
```bash
# Local/dev
cp .env.example .env

# Production: vẫn bắt đầu từ base đầy đủ
cp .env.example .env
# Sau đó chép/thay các secret và endpoint được minh họa trong
# .env.production.example vào .env; file production example không chứa tuning.
```

### Bước 2: Thay đổi thông số Bảo mật & Secret Keys
Mở file `.env` và thay đổi các chuỗi mật khẩu mặc định:
```env
ENVIRONMENT=production
POSTGRES_LOCAL_PASSWORD=Doi_Mat_Khau_Postgres_Moi_Cho_Prod!
REDIS_PASSWORD=Doi_Mat_Khau_Redis_Moi_Cho_Prod!
RADIUS_SHARED_SECRET=Khau_Bi_Mat_Radius_3GPP_Prod!
API_KEY=Khau_Bi_Mat_Camara_API_Key_Prod!
WEBHOOK_SIGNING_SECRET=Khau_Bi_Mat_Ky_HMAC_Webhook_Prod!
GRAFANA_ADMIN_PASSWORD=Mat_Khau_Grafana_Prod!
```

### Bước 3: Áp dụng Profile Cấu Hình Phần Cứng
Dựa vào dung lượng vCPU và RAM của máy chủ mới, điều chỉnh các thông số tương ứng theo bảng [Hardware Sizing Matrix](#3-bảng-cấu-hình-mẫu-theo-phần-cứng-hardware-sizing-matrix).

### Bước 4: Khởi động hệ thống & Kiểm tra Log Telemetry
```bash
# Khởi động bằng hardware profile đã chọn (ví dụ host 32 GiB / 24 vCPU)
bash scripts/run_pipeline.sh config/env/32gb.env

# Kiểm tra log telemetry để xác nhận các thông số đã nhận đúng
docker compose logs -f radius-ingestion pipeline-ip-msisdn pipeline-device-swap pipeline-sim-swap
```

Nếu triển khai Linux với Redis Sentinel và host networking cho ingestion, dùng
thêm production override nhưng vẫn giữ profile 32 GiB làm nguồn tuning duy nhất:

```bash
docker compose --env-file .env --env-file config/env/32gb.env \
  -f docker-compose.yml -f docker-compose.prod.yml config --quiet

docker compose --env-file .env --env-file config/env/32gb.env \
  -f docker-compose.yml -f docker-compose.prod.yml up -d --build \
  --scale radius-ingestion=1 \
  --scale pipeline-ip-msisdn=6 \
  --scale pipeline-device-swap=6 \
  --scale pipeline-sim-swap=6
```

Không dùng `docker-compose.prod.yml` trên Docker Desktop Windows/macOS vì
`network_mode: host` ở đó không tương đương Linux host network.
