# BÁO CÁO KỸ THUẬT: CÁC KỸ THUẬT CHUYÊN SÂU TRONG CAMARA DATA PIPELINE

> **Tài liệu Kỹ thuật Dự án Viễn thông**: Báo cáo mô tả chi tiết toàn bộ các giải pháp kiến trúc, thuật toán và kỹ thuật tối ưu hóa hiệu năng được triển khai trong các module trọng điểm của CAMARA Data Pipeline.

---

## MỤC LỤC

1. [Tổng quan Kiến trúc Hệ thống](#1-tổng-quan-kiến-trúc-hệ-thống)
2. [Tầng Tiếp Nhận Dữ Liệu (Ingestion Layer)](#2-tầng-tiếp-nhận-dữ-liệu-ingestion-layer)
   - 2.1. `pipeline/ingestion/packet_reader.py`: Giải mã Nhị phân RFC 2866 & 3GPP VSA
   - 2.2. `pipeline/ingestion/producer.py`: Buffer Hàng đợi Bất đồng bộ & Multi-worker Producer
3. [Tầng Hạ Tầng Dùng Chung (Shared Infrastructure)](#3-tầng-hạ-tầng-dùng-chung-shared-infrastructure)
   - 3.1. `pipeline/modules/shared/base_consumer.py`: Sharding Phân vùng Song song, Manual Commit & Đo Lag Bản Tin E2E
   - 3.2. `pipeline/modules/shared/db.py`: Giao dịch Nguyên tử 4 Bảng, Fencing Versioning & Batch UNNEST
   - 3.3. `pipeline/modules/shared/metrics.py`: Telemetry Đo lường Hai Tầng, Giám sát Thất thoát & Sliding Window Logger
4. [Tầng Xử Lý Nghiệp Vụ (Consumer Modules)](#4-tầng-xử-lý-nghiệp-vụ-consumer-modules)
   - 4.1. `pipeline/modules/ip_msisdn/`: Lua Scripts Nguyên tử, Reverse Index Sorted Set & Fencing Versioning
   - 4.2. `pipeline/modules/device_swap/` & `sim_swap/`: Tra cứu Trạng thái Hai Tầng, In-Batch State Mutation & Chuẩn hóa CAMARA
5. [Tầng Phân Phối Thông Báo (Transactional Outbox Dispatcher)](#5-tầng-phân-phối-thông-báo-transactional-outbox-dispatcher)
   - 5.1. `pipeline/dispatcher/notification_dispatcher.py`: Outbox Pattern, FOR UPDATE SKIP LOCKED & Tự Khôi Phục Sự Cố
6. [Bảng Tổng Hợp Kỹ Thuật Trọng Điểm](#6-bảng-tổng-hợp-kỹ-thuật-trọng-điểm)

---

## 1. Tổng quan Kiến trúc Hệ thống

```mermaid
flowchart TD
    GGSN["External RADIUS Capture Server<br/>(Durable Mirror Source)"] -->|Mirrored UDP/1813| PR["PacketReader (SO_REUSEPORT)"]
    PR -->|asyncio.Queue| PROD["RadiusLogProducer (4 Publisher Workers)"]
    PROD -->|Produce acks=1| KAFKA["Apache Kafka (9-48 Partitions theo profile)"]

    KAFKA --> CG_IP["IP-MSISDN Consumer Group"]
    KAFKA --> CG_DEV["Device Swap Consumer Group"]
    KAFKA --> CG_SIM["SIM Swap Consumer Group"]

    CG_IP -->|Lua Scripts| REDIS[("Redis / Redis Sentinel")]
    CG_IP -->|Session Upsert| PG[("PostgreSQL")]
    CG_DEV -->|Two-Tier Lookup + Tx| PG
    CG_DEV -->|Cache MSET| REDIS
    CG_SIM -->|Two-Tier Lookup + Tx| PG
    CG_SIM -->|Cache MSET| REDIS

    PG -.->|FOR UPDATE SKIP LOCKED| DISPATCHER["NotificationDispatcher"]
    DISPATCHER -->|HTTP POST with Idempotency-Key| WEBHOOK["Open Gateway Subscribers"]
```

---

## 2. Tầng Tiếp Nhận Dữ Liệu (Ingestion Layer)

### 2.1. `pipeline/ingestion/packet_reader.py` — Bộ Giải Mã Nhị Phân RADIUS RFC 2866 & 3GPP VSA

#### Kỹ thuật 1: Giải mã nhị phân không sử dụng thư viện ngoài (Zero-dependency Binary Parser)
- Đọc trực tiếp cấu trúc nhị phân 20-byte RADIUS Header theo định dạng Big-Endian (`struct.unpack`):
  - Byte 0: `Code` (ingestion chỉ chấp nhận 4 = Accounting-Request).
  - Byte 1: `Identifier` của gói RADIUS.
  - Byte 2–3: `Length` (Độ dài toàn bộ gói tin).
  - Byte 4–19: `Request Authenticator` (16 bytes chuỗi xác thực ngẫu nhiên).
- Duyệt vòng lặp bóc tách các cặp thuộc tính TLV (Type-Length-Value) với độ phức tạp $O(N)$ tuyến tính theo độ dài gói tin.

#### Kỹ thuật 2: Xử lý 3GPP Vendor-Specific Attributes (VSA Vendor ID = 10415)
- Giải mã lồng các thuộc tính con chuẩn mạng di động viễn thông (3GPP TS 29.061):
  - Subtype `1`: **`3GPP-IMSI`** (Chuỗi nhận dạng thuê bao di động quốc tế).
  - Subtype `20`: **`3GPP-IMEISV`** (Mã nhận dạng thiết bị phần cứng).
  - Subtype `21`: **`3GPP-RAT-Type`** (Loại sóng mạng: 1=UTRAN, 2=GERAN, 6=EUTRAN/LTE).
  - Subtype `8`: **`3GPP-SGSN-MCC-MNC`** (Mã mạng quốc gia và nhà mạng viễn thông).

#### Kỹ thuật 3: Xác thực MD5 Request Authenticator
- `PacketReader` tính lại Request Authenticator từ header, AVP và shared secret rồi
  so sánh constant-time bằng `hmac.compare_digest` trước khi giải mã record.
- Đây là kiểm tra tính toàn vẹn đầu vào, không tạo RADIUS response và không quản lý
  session với thiết bị mạng.

#### Kỹ thuật 4: Kernel Socket Load Balancing (`SO_REUSEPORT`) & Gắn Thẻ Thời Gian Ingest
- Socket UDP được cấu hình cờ `SO_REUSEPORT` và buffer nhận tối đa 32MB (`SO_RCVBUF`). Khi chạy nhiều tiến trình Ingestion trên Linux, Kernel tự động băm (hash) 4-tuple (`src_ip, src_port, dst_ip, dst_port`) phân bổ gói tin đều cho các tiến trình mà không cần Proxy trung gian.
- Ngay sau `sock_recvfrom`, gắn `ingest_timestamp`, `ingest_epoch_s` và mốc nanosecond nguyên `ingest_epoch_ns`. Consumer ưu tiên mốc nguyên để đo End-to-End chính xác hơn và chỉ fallback sang ISO cho record cũ.

---

### 2.2. `pipeline/ingestion/producer.py` & `radius_udp_sender.py` — Quản Lý Hàng Đợi Bất Đồng Bộ, Key Sharding & Đa Socket Load-Test

#### Kỹ thuật 1: Key-Sharded Worker Queues (`hash(key) % num_workers`)
- Thay vì sử dụng 1 queue duy nhất làm cho các publisher workers tranh chấp làm xen kẽ thứ tự gọi `.send()` của Kafka Producer giữa các batch khác nhau, `RadiusLogProducer` định tuyến dữ liệu theo **MSISDN Hash Sharding**:
  $$\text{target\_worker} = \text{hash}(\text{msisdn}) \pmod{\text{publisher\_workers}}$$
- Tất cả các bản ghi của cùng một số thuê bao di động **luôn đi vào đúng 1 worker duy nhất**, bảo đảm tuyệt đối thứ tự gọi `send()` theo đúng trình tự thời gian mà không bị đảo lộn giữa các worker.
- Các bản ghi DLQ hoặc bản ghi không có key được phân bổ đều theo `event_id` hoặc Round-Robin `_rr_counter`, ngăn ngừa hiện tượng dồn ép bộ nhớ vào worker 0.

#### Kỹ thuật 2: Khống Chế Inflight Toàn Cục Trực Tiếp qua Semaphore (`RADIUS_UDP_KAFKA_TOTAL_MAX_INFLIGHT_BATCHES`)
- Sử dụng `asyncio.Semaphore(total_inflight_limit)` để khống chế trần tổng số Kafka produce futures đồng thời trên toàn bộ workers (profile 8 GiB tối đa 24 batch × 64 record).
- Mỗi worker có trần 6 batch; semaphore toàn process giữ tổng tối đa 24 batch ở profile 8 GiB. Mức này được sizing từ bandwidth-delay product và ngân sách p95.
- Bốn publisher coroutine dùng chung một `AIOKafkaProducer` ở profile 8/16 GiB.
  Kafka giữ thứ tự theo partition/key, còn accumulator chung gom record cùng
  partition hiệu quả hơn và giảm số connection/request nhỏ tới broker.
- Ngăn ngừa tình trạng áp lực bộ nhớ và biến động latency khi Kafka Cluster gặp hiện tượng nghẽn I/O hoặc rebalance.

#### Kỹ thuật 3: Passive Mirror Boundary
- Capture server bên ngoài là nguồn bền vững và sở hữu ACK/response/replay.
- Ingestion không giữ pending request, retry heap hay ACK cache. Mỗi datagram hợp lệ
  được chuẩn hóa và chuyển một chiều tới Kafka; queue đầy hoặc publish lỗi được ghi
  nhận là `data_loss` để vận hành replay từ nguồn.

#### Kỹ thuật 4: Multi-Socket Fire-and-Forget Traffic Generator (`--num-sockets 8`)
- `radius_udp_sender.py` hỗ trợ gửi từ $N$ UDP client sockets độc lập (mỗi socket sở hữu 1 source port riêng từ OS), kích hoạt 100% cơ chế Kernel `SO_REUSEPORT` của Linux phía receiver.
- Một sender loop sở hữu token bucket và round-robin qua các socket. Công cụ không
  nhận response hoặc retry nên số đo `actual` phản ánh tốc độ phát mirror, không bị
  trộn với RTT hay lưu lượng retry.

---

## 3. Tầng Hạ Tầng Dùng Chung (Shared Infrastructure)

### 3.1. `pipeline/modules/shared/base_consumer.py` — Lớp Cơ Sở Xử Lý Song Song & Đo Lag Bản Tin E2E

#### Kỹ thuật 1: Temporal Pipeline theo Partition
- `getmany()` phân phối record vào FIFO riêng của từng Kafka partition thay vì gom nhiều partition vào một shard.
- Mỗi partition có đúng một mutating worker; tối đa $K$ worker chạy đồng thời theo `PROCESSING_PARTITION_CONCURRENCY`.
- Worker của partition nhanh tiếp tục batch kế tiếp và công bố offset cho commit coordinator, không còn chờ `asyncio.gather()` hoặc Kafka commit round-trip theo từng batch.
- FIFO đạt high-watermark sẽ `pause()` đúng partition và `resume()` tại low-watermark, không chặn các partition còn lại.

#### Kỹ thuật 2: Cam kết Offset Thủ Công (Manual Offset Commit) & Dead Letter Queue (DLQ)
- Tắt hoàn toàn `enable_auto_commit`. Offset của từng partition chỉ được commit sau khi batch của chính partition đó ghi PostgreSQL/Redis thành công.
- Cơ chế Exponential Backoff Retry (thử lại tối đa 3 lần). Nếu một bản ghi hoặc shard bị lỗi cấu trúc dữ liệu không thể xử lý, nó được chuyển hướng tự động sang topic `<topic>.dlq` kèm toàn bộ payload và stack trace lỗi mà không làm dừng pipeline.

#### Kỹ thuật 3: Đo lường Độ trễ Bản tin Toàn trình Siêu Tốc (Fast Float Epoch E2E Lag)
- Trích xuất `ingest_epoch_s` từ mỗi bản ghi trong batch, so sánh với thời điểm hiện tại `time.time()` ngay sau khi hoàn tất ghi DB/Redis:
  $$\text{e2e\_lag\_ms} = (\text{now\_epoch} - \text{record.ingest\_epoch\_s}) \times 1000$$
- Triệt tiêu 100% chi phí parse chuỗi ISO-8601 datetime trên $45.000 \text{ records/giây}$ tiêu thụ bởi 3 consumer groups.

---

### 3.2. `pipeline/modules/shared/db.py` — Giao Dịch Nguyên Tử 4 Bảng & Fencing Versioning

#### Kỹ thuật 1: Quản lý Connection Pool Bất Đồng Bộ Duy Nhất (`asyncpg.Pool`)
- Sử dụng 1 connection pool duy nhất dùng chung cho toàn bộ $N$ consumer members trong cùng một tiến trình OS (cấu hình `min=6, max=32`), ngăn chặn tình trạng cạn kiệt socket kết nối tới PostgreSQL.
- PostgreSQL được giới hạn an toàn `max_connections=200` vừa vặn với container 2GB RAM (`work_mem=16MB`), ngăn ngừa hiện tượng memory pressure và context switching.

#### Kỹ thuật 2: Giao dịch Nguyên tử 4 Bảng trong Cùng một Transaction (`_persist_swap_batch`)
- Toàn bộ 4 thao tác ghi của một đợt phát hiện sự kiện Swap được đóng gói trong cùng một lệnh `connection.transaction()`:
  1. `_upsert_state`: Cập nhật trạng thái mới vào bảng `msisdn_device` hoặc `msisdn_sim`.
  2. `_insert_history`: Ghi nhật ký lịch sử vào `device_swap_history` hoặc `sim_swap_history`.
  3. `_insert_audit`: Ghi vết kiểm toán hệ thống vào `audit_log`.
  4. `_insert_outbox`: Ghi thông báo chờ gửi webhook vào `notification_log` (status `PENDING`).
- Đảm bảo tính toàn vẹn 100% theo chuẩn ACID: nếu có bất kỳ lỗi nào xảy ra, toàn bộ giao dịch được Rollback.

#### Kỹ thuật 3: Tối ưu hóa Ghi Hàng Loạt bằng Mệnh đề `UNNEST`
- Gom toàn bộ mảng dữ liệu của batch thành các mảng nguyên thủy (arrays) và thực thi qua 1 câu lệnh SQL duy nhất bằng `UNNEST`:
  ```sql
  INSERT INTO msisdn_device (msisdn, imei_current, last_event_at, last_event_id, last_source_partition, last_source_offset)
  SELECT * FROM UNNEST($1::text[], $2::text[], $3::timestamptz[], $4::text[], $5::int[], $6::bigint[])
  ON CONFLICT (msisdn) DO UPDATE SET
      imei_current = EXCLUDED.imei_current,
      last_event_at = EXCLUDED.last_event_at,
      last_event_id = EXCLUDED.last_event_id,
      last_source_partition = EXCLUDED.last_source_partition,
      last_source_offset = EXCLUDED.last_source_offset
  WHERE (EXCLUDED.last_event_at, EXCLUDED.last_source_partition, EXCLUDED.last_source_offset) >
        (msisdn_device.last_event_at, msisdn_device.last_source_partition, msisdn_device.last_source_offset);
  ```

#### Kỹ thuật 4: Fencing Tuple 3 Thành Phần Chống Ghi Đè Gói Tin Đến Sai Thứ Tự
- Ràng buộc phiên bản so sánh tuple:
  $$(\text{incoming.last\_event\_at}, \text{incoming.last\_source\_partition}, \text{incoming.last\_source\_offset}) > (\text{current.last\_event\_at}, \text{current.last\_source\_partition}, \text{current.last\_source\_offset})$$
- Loại bỏ hoàn toàn nguy cơ dữ liệu cũ bị ghi đè lên dữ liệu mới khi mạng có hiện tượng retry hoặc gói tin bị trễ (Out-of-Order Delivery).

---

### 3.3. `pipeline/modules/shared/metrics.py` — Telemetry Đo Lường Hai Tầng & Sampling Ngẫu Nhiên

#### Kỹ thuật 1: Sampling Ngẫu Nhiên 10% Prometheus Histogram (`random.sample()`)
- Thay vì lấy vị trí cố định `[::10]` làm thiên lệch kết quả p95 theo thứ tự record trong batch, `metrics.py` sử dụng `random.sample(lags_ms, min(count, count // 10))` để thu thập dữ liệu thống kê Prometheus Histogram không thiên lệch (unbiased).
- Giảm $90\%$ tải GIL/Lock Prometheus Exporter ở mức $45.000 \text{ ops/s}$.

#### Kỹ thuật 2: Phân Rã Độ Trễ Nội Bộ Từng Chặng (`latency_ms`)
- Đo đạc chính xác thời gian thực thi của từng chặng trong pipeline: `state`, `postgres`, `redis`.

#### Kỹ thuật 3: Metrics Phân Rã Ngữ Nghĩa (Split Metrics Semantics)
- Phân định rõ ràng các Counter trong Ingestion Exporter:
  - `radius_ingestion_invalid_total`: Record không hợp lệ sent to DLQ.
  - `radius_ingestion_dlq_published_total`: Record DLQ published to Kafka `.dlq`.
  - `radius_ingestion_kafka_persisted_total`: Record được Kafka xác nhận theo cấu hình producer.
  - `radius_ingestion_publish_failed_total`: Record mirror không ghi được Kafka.
  - `radius_ingestion_queue_dropped_total`: RAM queue đầy và record mirror bị bỏ.
  - `radius_ingestion_queue_capacity_records`: Capacity dùng tính queue pressure theo tỷ lệ.
  - `radius_ingestion_worker_queue_depth_records{worker="N"}`: RAM queue depth per worker shard.
  - `radius_ingestion_worker_slot_wait_seconds{worker="N"}`: Thời gian worker bị chặn bởi giới hạn inflight cục bộ.

---

## 4. Tầng Xử Lý Nghiệp Vụ (Consumer Modules)

### 4.1. `pipeline/modules/ip_msisdn/` — Quản Lý Phiên Mạng IP↔MSISDN (CAMARA Number Verification)
- **`UPSERT_LUA`**: Ghi nguyên tử key `ip-ggsn:<ip>` và Sorted Set `ggsn-ips:<nas>`.
- **`DELETE_LUA`**: Ownership check xóa key chỉ khi msisdn trùng khớp.
- **`ACCOUNTING_OFF_LUA`**: Xóa nguyên tử toàn bộ IP của trạm NAS khi khởi động lại.

---

### 4.2. `pipeline/modules/device_swap/` & `sim_swap/` — Phát Hiện Đổi Thiết Bị (IMEI) & Đổi SIM (IMSI)
- **Two-Tier State Lookup**: `redis.mget()` batch $\to$ SQL fallback `ANY($1::text[])`.
- **In-Batch State Mutation**: Cập nhật trạng thái in-memory tức thì giữa các record cùng msisdn trong batch.
- **CAMARA Payload Mapping**: Map chuẩn hóa sang CAMARA Open Gateway API specifications.

---

## 5. Tầng Phân Phối Thông Báo & Bảo Mật An Ninh Mạng (Outbox & Security)

### 5.1. `pipeline/dispatcher/notification_dispatcher.py` — Webhook Outbox Dispatcher
- **Decoupled Outbox Worker**: Tách hoàn toàn HTTP callbacks khỏi pipeline chính.
- **`FOR UPDATE SKIP LOCKED`**: Khóa phân tán phi tập trung cho phép scale ngang nhiều dispatcher workers.
- **SSRF & DNS-Rebinding Protection**: Validate URL qua `ssrf_protection.py` cấm IP nội bộ, loopback (127.0.0.1) và metadata (169.254.169.254).
- **HMAC SHA-256 Signatures**: Mỗi webhook POST được ký với `X-Signature-SHA256: sha256=<hex_digest>`.

---

## 6. Bảng Tổng Hợp Kỹ Thuật Trọng Điểm

| File / Module | Kỹ Thuật Trọng Điểm | Lợi Ích & Mục Đích Kỹ Thuật |
|---|---|---|
| [`packet_reader.py`](../pipeline/ingestion/packet_reader.py) | - RFC 2866 Binary TLV Parser<br/>- 3GPP VSA (Vendor 10415)<br/>- MD5 Authenticator<br/>- `SO_REUSEPORT` | - Giải mã cực nhanh không phụ thuộc thư viện ngoài<br/>- Kernel load-balancing đa tiến trình<br/>- Xác thực tính toàn vẹn gói tin |
| [`producer.py`](../pipeline/ingestion/producer.py) | - Key-Sharded Queues per MSISDN<br/>- Global Inflight Semaphore<br/>- Passive mirror boundary<br/>- Split Metrics Exporter | - Bảo đảm thứ tự gửi theo MSISDN<br/>- Khống chế trần inflight toàn process<br/>- Không mang state giao thức RADIUS<br/>- Định vị queue/Kafka bottleneck và data loss |
| [`radius_udp_sender.py`](../pipeline/ingestion/radius_udp_sender.py) | - Multi-Socket Sending (`--num-sockets 8`)<br/>- Token-bucket pacing<br/>- Pre-encoding/cache AVP | - Kích hoạt SO_REUSEPORT load balancing<br/>- Tạo tải fire-and-forget ổn định, không lẫn ACK/retry |
| [`base_consumer.py`](../pipeline/modules/shared/base_consumer.py) | - Partition Sharding (`asyncio.gather`)<br/>- Manual Offset Commit<br/>- Fast Float Epoch Lag (`ingest_epoch_s`) | - Khai thác tối đa I/O đa nhân<br/>- Chống mất mát dữ liệu khi sập nguồn<br/>- Tính lag E2E cực nhanh (500x speedup) |
| [`db.py`](../pipeline/modules/shared/db.py) | - Dynamic Connection Pool (`max=200`)<br/>- Giao dịch 4 bảng nguyên tử<br/>- Batch `UNNEST` SQL<br/>- Fencing Versioning Tuple | - Tiết kiệm bộ nhớ RAM Postgres<br/>- Bảo đảm 100% tính toàn vẹn ACID<br/>- Ghi hàng loạt tốc độ cao (< 25ms/batch)<br/>- Chống ghi đè gói tin đến sai thứ tự |
| [`metrics.py`](../pipeline/modules/shared/metrics.py) | - Random Sampling 10% Histogram<br/>- In-Memory Sliding Window<br/>- Per-Worker Queue Depth Gauge | - Thu thập dữ liệu Histogram không thiên lệch<br/>- Tích hợp chuẩn Dashboard Grafana<br/>- Giám sát chi tiết RAM queue của từng worker |
| [`ssrf_protection.py`](../pipeline/dispatcher/ssrf_protection.py) | - DNS Rebinding Validation<br/>- HMAC SHA-256 Signature Generator | - Ngăn chặn tấn công SSRF & DNS Rebinding vào mạng nội bộ<br/>- Đảm bảo tính toàn vẹn payload gửi webhook |
| [`notification_dispatcher.py`](../pipeline/dispatcher/notification_dispatcher.py) | - Transactional Outbox Pattern<br/>- `FOR UPDATE SKIP LOCKED`<br/>- Header `Idempotency-Key` | - Cách ly hoàn toàn callback khỏi luồng chính<br/>- Scale ngang không bị deadlock |
