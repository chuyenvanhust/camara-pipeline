# HƯỚNG DẪN CẤU HÌNH & TỐI ƯU HÓA PHẦN CỨNG (HARDWARE TUNING GUIDE)

> **Tài liệu Hướng dẫn Khả chuyển (Portability & Hardware Sizing Guide)**: Giải thích chi tiết toàn bộ các thông số cài đặt trong file `.env` và công thức tính toán điều chỉnh tài nguyên khi chuyển đổi hạ tầng máy chủ (từ Dev/VPS đến Server Production viễn thông tải cao 30.000+ pkt/s).

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
| `KAFKA_TOPIC_PARTITIONS` | `16` | Integer | Số lượng partitions của topic thô. **Tác động**: Quyết định trần khả năng scale ngang song song của Consumer Groups (`CONSUMERS_PER_GROUP` tối đa bằng số partitions). |
| `KAFKA_REPLICATION_FACTOR` | `3` | Integer | Số lượng bản sao lưu của từng partition trên Kafka cluster. `3` nghĩa là mỗi partition có 1 Leader + 2 Followers. |
| `KAFKA_MIN_INSYNC_REPLICAS` | `2` | Integer | Số lượng broker replicas tối thiểu phải đồng bộ ISR khi Producer publish với `acks=all`. |
| `KAFKA_NUM_NETWORK_THREADS` | `8` | Integer | Số thread xử lý I/O mạng của Kafka Broker. Tăng lên 16-32 khi máy chủ có >16 vCPU. |
| `KAFKA_NUM_IO_THREADS` | `16` | Integer | Số thread xử lý truy xuất đĩa đĩa của Kafka Broker. Tăng khi sử dụng mảng ổ đĩa NVMe/SSD. |
| `ZOOKEEPER_MEM_LIMIT` | `256m` | RAM | Giới hạn RAM tối đa cấp cho Container ZooKeeper. |
| `KAFKA_MEM_LIMIT` | `3g` | RAM | Giới hạn RAM tối đa cấp cho mỗi Container Kafka Broker (chứa JVM Heap + Page Cache). |
| `KAFKA_CPUS` | `4` | Cores | Giới hạn nhân CPU tối đa cấp cho mỗi Container Kafka Broker trong `.env.example`. |

---

### 2.2. Cơ sở dữ liệu PostgreSQL (Storage & Session State)

| Tên Biến Môi Trường | Mặc Định | Đơn Vị | Ý Nghĩa Kỹ Thuật & Tác Động Khi Điều Chỉnh |
|---|---|---|---|
| `POSTGRES_LOCAL_USER` | `postgres` | String | Tên user quản trị PostgreSQL. |
| `POSTGRES_LOCAL_PASSWORD` | *(Mã hóa dev)* | Secret | Mật khẩu truy cập PostgreSQL. **Phải đổi khi lên Prod!** |
| `POSTGRES_LOCAL_DB` | `camara_db` | String | Tên cơ sở dữ liệu chính của dự án. |
| `DATABASE_URL` | `postgresql://...` | DSN | Connection string đầy đủ kết nối tới PostgreSQL (`synchronous_commit=on` bảo toàn ACID 100%). |
| `POSTGRES_MAX_CONNECTIONS` | `200` | Connections | Trần số kết nối tối đa PostgreSQL chấp nhận. **Lưu ý**: Cần cân đối với RAM theo công thức `RAM ≈ shared_buffers + (max_conns × work_mem × sort_ops_per_query)`. |
| `POSTGRES_SHARED_BUFFERS` | `512MB` | RAM | Vùng nhớ RAM đệm cache dữ liệu bảng và index của Postgres. Nên đặt bằng 25% tổng RAM máy chủ. |
| `POSTGRES_EFFECTIVE_CACHE_SIZE` | `1GB` | RAM | Ước tính dung lượng cache khả dụng (bao gồm Shared Buffers + OS Page Cache) cho Query Planner. Nên đặt bằng 50-75% RAM. |
| `POSTGRES_WORK_MEM` | `8MB` | RAM | Bộ nhớ RAM cấp cho **mỗi phép toán** sort/hashtable trong 1 query. ⚠️ Một kết nối có thể dùng nhiều lần `work_mem` nếu có nhiều phép sort/hash — giá trị thực tế tiêu thụ có thể gấp 2-4 lần `max_conns × work_mem`. |
| `POSTGRES_MAX_WAL_SIZE` | `2GB` | Storage | Dung lượng tối đa của tệp nhật ký ghi trước (Write-Ahead Log) trước khi tự động checkpoint. |
| `IP_MSISDN_DB_POOL_MAX` | `12` | Connections | Kích thước pool kết nối tối đa `asyncpg` cho service `pipeline-ip-msisdn`. |
| `DEVICE_SWAP_DB_POOL_MAX` | `8` | Connections | Kích thước pool kết nối tối đa `asyncpg` cho service `pipeline-device-swap`. |
| `SIM_SWAP_DB_POOL_MAX` | `8` | Connections | Kích thước pool kết nối tối đa `asyncpg` cho service `pipeline-sim-swap`. |
| `POSTGRES_MEM_LIMIT` | `3g` | RAM | Giới hạn RAM tối đa cấp cho Container PostgreSQL. Phải lớn hơn `shared_buffers + (max_conns × work_mem)`. |
| `POSTGRES_CPUS` | `4` | Cores | Giới hạn nhân CPU tối đa cấp cho Container PostgreSQL. |

---

### 2.3. CSDL In-Memory Redis & Cache

| Tên Biến Môi Trường | Mặc Định | Đơn Vị | Ý Nghĩa Kỹ Thuật & Tác Động Khi Điều Chỉnh |
|---|---|---|---|
| `REDIS_HOST` | `camara-redis` | Host | Địa chỉ host kết nối Redis Server. |
| `REDIS_PORT` | `6379` | Port | Cổng kết nối Redis TCP. |
| `REDIS_PASSWORD` | *(Mã hóa dev)* | Secret | Mật khẩu truy cập Redis Server. |
| `REDIS_SENTINELS` | `""` | Host:Port | Danh sách Sentinel nodes phục vụ chế độ High Availability (HA) trên Production. |
| `REDIS_MASTER_NAME` | `camara-master` | String | Tên master group trong Redis Sentinel cluster. |
| `REDIS_MAXMEMORY` | `512mb` | RAM | Bộ nhớ RAM tối đa cấp cho Redis lưu trữ key/cache. |
| `REDIS_MEM_LIMIT` | `640m` | RAM | Giới hạn RAM Docker cấp cho Container Redis. |

---

### 2.4. Tầng Ingestion Tiếp Nhận RADIUS UDP (Producer)

| Tên Biến Môi Trường | Mặc Định | Đơn Vị | Ý Nghĩa Kỹ Thuật & Tác Động Khi Điều Chỉnh |
|---|---|---|---|
| `RADIUS_SHARED_SECRET` | *(Dev secret)* | Secret | Khóa bí mật dùng tính MD5 Request Authenticator theo chuẩn RFC 2866. |
| `RADIUS_UDP_RECEIVE_BUFFER_BYTES` | `33554432` (32MB) | Bytes | Kích thước bộ nhớ đệm nhận UDP Socket của Hệ điều hành Linux (`SO_RCVBUF`). Tăng khi lưu lượng burst cao. |
| `RADIUS_UDP_QUEUE_MAX_RECORDS` | `300000` | Records | Burst buffer RAM trước Kafka; không tăng capacity dài hạn. |
| `RADIUS_UDP_KAFKA_BATCH_RECORDS` | `500` | Records | Số lượng bản ghi tối đa trong một micro-batch ứng dụng. |
| `RADIUS_UDP_KAFKA_BATCH_WAIT_MS` | `5` | ms | Thời gian tối đa chờ gom đủ batch (ms). |
| `RADIUS_UDP_KAFKA_MAX_INFLIGHT_BATCHES_PER_WORKER` | `4` | Batches | Số lượng batch Kafka produce song song cho mỗi worker (baseline 4 đã kiểm chứng qua A/B test ngăn stall Ingestion). |
| `RADIUS_UDP_KAFKA_PRESSURE_INFLIGHT_BATCHES_PER_WORKER` | `6` | Batches | Trần tạm thời khi queue shard vượt ngưỡng pressure. |
| `RADIUS_UDP_KAFKA_PRESSURE_QUEUE_RATIO` | `0.5` | Ratio | Tỷ lệ queue shard kích hoạt pressure concurrency. |
| `RADIUS_UDP_KAFKA_PRODUCERS` | `4` | Producers | Producer pool theo shard; cùng MSISDN luôn đi qua cùng worker/producer. |
| `RADIUS_UDP_KAFKA_TOTAL_MAX_INFLIGHT_BATCHES` | `24` | Batches | Trần tuyệt đối trên toàn process; không tăng vượt mức này nếu chưa benchmark. |
| `INGESTION_BATCH_SIZE_BYTES` | `524288` | Bytes | Buffer batch tối đa của Kafka producer. |
| `RADIUS_UDP_PUBLISHER_WORKERS` | `4` | Workers | Số lượng worker coroutines publish song song (đã được định tuyến theo MSISDN Key Sharding). |
| `INGESTION_KAFKA_PERSIST_WARN_MS` | `500` | ms | Ngưỡng cảnh báo p95 thời gian Kafka xác nhận ghi batch nội bộ. |
| `INGESTION_QUEUE_WARN_MS` | `1000` | ms | Ngưỡng cảnh báo p95 thời gian record nằm trong queue RAM. |
| `RADIUS_INGESTION_MEM_LIMIT` | `1g` | RAM | Giới hạn RAM Docker cấp cho Container Ingestion. |
| `RADIUS_INGESTION_CPUS` | `2` | Cores | Giới hạn nhân CPU cấp cho Container Ingestion. |

---

### 2.5. Tầng Xử Lý Pipeline Consumers (Consumer Modules)

| Tên Biến Môi Trường | Mặc Định | Đơn Vị | Ý Nghĩa Kỹ Thuật & Tác Động Khi Điều Chỉnh |
|---|---|---|---|
| `PIPELINE_GROUPS` | `""` | String | Chọn group kích hoạt cho worker container (`ip-msisdn`, `device-swap`, `sim-swap`). Nếu rỗng sẽ khởi chạy cả 3. |
| `CONSUMERS_PER_GROUP` | `4` | Members | Số lượng tiến trình/member chạy song song trong mỗi Consumer Group (`cg-ip-msisdn`, `cg-device-swap`, `cg-sim-swap`). |
| `PROCESSING_PARTITION_CONCURRENCY` | `2` | Shards | Số shard partition gom xử lý bất đồng bộ song song trong mỗi member (`asyncio.gather`). Giảm từ 3 xuống 2 để hạn chế tranh chấp connection pool. |
| `BATCH_MAX_RECORDS` | `4000` | Records | Số lượng bản ghi tối đa lấy trong một lần poll Kafka (`getmany()`). |
| `BATCH_TIMEOUT_MS` | `10` | ms | Thời gian tối đa chờ gom đủ batch poll (ms). |
| `THROUGHPUT_LOG_INTERVAL_SECONDS` | `10` | Giây | Chu kỳ in log thống kê thông lượng telemetry nội bộ. |
| `PIPELINE_IP_MEM_LIMIT` / `DEVICE...` / `SIM...` | `2g` | RAM | Giới hạn RAM Docker cấp cho mỗi Container Pipeline Worker. |
| `PIPELINE_IP_CPUS` / `DEVICE...` / `SIM...` | `2` | Cores | Giới hạn nhân CPU cấp cho mỗi Container Pipeline Worker. |

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

> ⚠️ **Lưu ý**: Các mốc tải trong bảng dưới đây (5k, 15k, 30k pkt/s) là **mục tiêu thiết kế (design targets)**, chưa được kiểm chứng qua soak test dài hạn (30-60 phút liên tục). Cần thực hiện benchmark thực tế trên phần cứng đích trước khi áp dụng vào production, đo đạc: CPU/RAM usage, Kafka lag, `radius_ingestion_queue_dropped_total`, UDP kernel drops, p95/p99 E2E latency.

| Biến Môi Trường | Profile 1: Dev / Small VPS<br/>**(4 vCPU, 8GB RAM)**<br/>*Mục tiêu: < 5.000 pkt/s* | Profile 2: Staging / Medium Server<br/>**(8 vCPU, 16GB RAM)**<br/>*Mục tiêu: 10.000 - 15.000 pkt/s* | Profile 3: Production High-Capacity<br/>**(16-32 vCPU, 32GB-64GB RAM)**<br/>*Mục tiêu: 30.000+ pkt/s* |
|---|---|---|---|
| **KAFKA_MEM_LIMIT** | `1.5g` | `3g` | `6g` |
| **KAFKA_CPUS** | `1` | `2` | `4` |
| **KAFKA_TOPIC_PARTITIONS** | `8` | `16` | `32` |
| **POSTGRES_MAX_CONNECTIONS** | `100` | `200` | `400` |
| **POSTGRES_SHARED_BUFFERS** | `512MB` | `1GB` | `4GB` |
| **POSTGRES_WORK_MEM** | `8MB` | `16MB` | `32MB` |
| **POSTGRES_MEM_LIMIT** | `1.5g` | `3g` | `8g` |
| **POSTGRES_CPUS** | `2` | `4` | `8` |
| **REDIS_MAXMEMORY** | `256mb` | `512mb` | `2gb` |
| **RADIUS_UDP_QUEUE_MAX_RECORDS** | `50000` | `100000` | `300000` |
| **RADIUS_UDP_PUBLISHER_WORKERS** | `2` | `4` | `8` |
| **RADIUS_UDP_KAFKA_TOTAL_MAX_INFLIGHT_BATCHES** | `12` | `24` | `24` |
| **RADIUS_INGESTION_MEM_LIMIT** | `512m` | `1g` | `2g` |
| **RADIUS_INGESTION_CPUS** | `1` | `2` | `4` |
| **CONSUMERS_PER_GROUP** | `2` | `4` | `8` |
| **PROCESSING_PARTITION_CONCURRENCY** | `2` | `2` | `2` |
| **IP_MSISDN_DB_POOL_MAX** | `6` | `12` | `16` |
| **DEVICE_SWAP_DB_POOL_MAX** | `4` | `8` | `12` |
| **SIM_SWAP_DB_POOL_MAX** | `4` | `8` | `12` |
| **PIPELINE_WORKER_CPUS** | `1` | `2` | `4` |

---

## 4. Các Công Thức Tính Toán Kỹ Thuật (Sizing Formulas)

Khi tự điều chỉnh thông số cho cấu hình phần cứng bất kỳ, áp dụng các công thức toán học sau:

### 1. Thời Gian Hấp Thụ Tràn Hàng Đợi RAM Ingestion (Hold Time)
$$T_{\text{hold (seconds)}} = \frac{\text{RADIUS\_UDP\_QUEUE\_MAX\_RECORDS}}{\text{Input Packet Rate (pkt/s)}}$$
*Ví dụ*: Với Queue $300.000$ bản ghi ở tốc độ $15.000 \text{ pkt/s} \implies T_{\text{hold}} = \frac{300.000}{15.000} = \mathbf{20 \text{ giây}}$ hấp thụ burst an toàn.

### 2. Dung Lượng Inflight Kafka Tối Đa Toàn Hệ Thống (Total Inflight Capacity)
$$\text{Capacity}_{\text{inflight}} = \text{RADIUS\_UDP\_KAFKA\_TOTAL\_MAX\_INFLIGHT\_BATCHES} \times \text{RADIUS\_UDP\_KAFKA\_BATCH\_RECORDS}$$
*Ví dụ*: Với $24 \text{ batches} \times 500 \text{ records} = \mathbf{12.000 \text{ records}}$ đang chờ xác nhận tối đa. Đây là concurrency thích ứng có trần, không phải queue thay thế durable storage.

### 3. Ngân Sách Kết Nối PostgreSQL (Connection Budget)
$$\text{Total} = \text{FastAPI (10)} + \text{IP-MSISDN Pool} + \text{Device Swap Pool} + \text{SIM Swap Pool} + \text{Dispatcher (5)} + \text{Admin (10)}$$
*Ví dụ (Staging)*: $10 + 12 + 8 + 8 + 5 + 10 = \mathbf{53 \text{ connections}}$. Đặt `POSTGRES_MAX_CONNECTIONS=200` bảo đảm dư >70% margin để scale.

> **Lưu ý**: Kiến trúc tách tiến trình (3 worker containers) giảm tổng connections từ ~131 (1 shared pool) xuống còn ~53, giảm 60% áp lực lên PostgreSQL và triệt tiêu tranh chấp pool giữa các consumer group.

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

# Production Linux dùng template production thay cho lệnh trên
cp .env.production.example .env
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
# Production Linux: áp dụng cả override HA/host-network
docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d

# Kiểm tra log telemetry để xác nhận các thông số đã nhận đúng
docker compose logs -f radius-ingestion pipeline-ip-msisdn pipeline-device-swap pipeline-sim-swap
```
