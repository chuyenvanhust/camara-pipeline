# `pipeline/modules/shared/` — Hạ Tầng Dùng Chung (Shared Infrastructure)

Thư mục `pipeline/modules/shared/` cung cấp các lớp trừu tượng nền tảng, hệ thống kết nối cơ sở dữ liệu, quản lý phiên bản tin cậy, chuẩn hoá dữ liệu viễn thông và hệ thống telemetry giám sát hiệu năng cho toàn bộ các Consumer Modules.

---

## 1. Sơ đồ quan hệ lớp chi tiết (Class Diagram)

```mermaid
classDiagram
    class BaseKafkaConsumer {
        <<Abstract>>
        +str topic
        +str group_id
        +str bootstrap_servers
        +DatabasePool db
        +Redis redis
        +AIOKafkaConsumer consumer
        +AIOKafkaProducer dlq_producer
        +ModuleMetrics metrics
        +bool running
        +bool _owns_db
        +initialize()
        +send_to_dlq(record, error)
        +process_batch(records)*
        +run()
        +stop()
    }

    class DatabasePool {
        +str dsn
        +Pool pool
        +connect()
        +close()
        -_fetch_state(table, value_col, msisdns)
        +batch_get_device_state(msisdns)
        +batch_get_sim_state(msisdns)
        -_upsert_state(conn, table, col, records)
        -_insert_history(conn, table, old_c, new_c, records)
        -_insert_audit(conn, records)
        -_insert_outbox(conn, events)
        -_persist_swap_batch(...)
        +persist_sim_batch(states, history, audit, outbox)
        +persist_device_batch(states, history, audit, outbox)
        +persist_session_batch(records)
        +mark_nas_sessions_inactive(nas, time)
        +claim_notifications(limit)
        +mark_notification_sent(id)
        +mark_notification_failed(id, attempts, max, err)
        +recover_stale_notifications()
    }

    class ModuleMetrics {
        +str name
        +str member
        +dict counters
        +float processing_seconds
        +dict stage_seconds
        +dict stage_calls
        +int kafka_lag
        +increment(metric, amount)
        +observe_batch(seconds)
        +observe_stage(stage, seconds)
        +set_kafka_lag(records)
        +log_summary()
        +log_periodically(interval)
        -_stage_average_ms(stage, prev_sec, prev_calls)
    }

    class EventsModule {
        <<Static Helpers>>
        +canonical_msisdn(message) str
        +parse_event_time(message) datetime
        +normalize_status(value) str
        +event_id(record) str
        +required_text(message, *keys) str
    }

    class RedisClientModule {
        <<Factory>>
        +create_redis_client(**overrides) Redis
        -_sentinel_nodes(raw) list
    }

    BaseKafkaConsumer o-- DatabasePool
    BaseKafkaConsumer o-- ModuleMetrics
    BaseKafkaConsumer ..> EventsModule : uses
    BaseKafkaConsumer ..> RedisClientModule : uses
```

---

## 2. Chi tiết các thành phần trong `shared/`

### 2.1. `BaseKafkaConsumer` (`base_consumer.py`)

`process_batch()` có thể trả về một deferred-durability future. Business E2E được
ghi nhận ngay khi Redis và các transaction nghiệp vụ bắt buộc đã sẵn sàng; worker
partition tiếp tục xử lý batch kế tiếp. Một chuỗi completion FIFO riêng trên từng
partition chỉ chuyển offset sang commit coordinator sau khi future thành công, vì
vậy checkpoint nền không cho phép commit vượt qua một durability gap.

### 2.2. `StateCheckpointCoordinator` (`swap_checkpoint.py`)

Coordinator gom watermark SIM/Device không phát sinh swap theo MSISDN, chỉ giữ
version mới nhất và bulk UPSERT PostgreSQL theo cửa sổ/threshhold cấu hình. Queue
có giới hạn để tạo backpressure. Lỗi flush làm future thất bại, dừng consumer và
để Kafka replay các offset chưa commit; swap thật không đi qua đường checkpoint
mà vẫn dùng transaction state + history + audit + outbox.
Lớp cơ sở trừu tượng cho tất cả các consumer trong hệ thống:
- **Vòng lặp tiêu thụ (`run()`)**: Sử dụng `AIOKafkaConsumer.getmany()` gom tối đa `BATCH_MAX_RECORDS` hoặc chờ `BATCH_TIMEOUT_MS`.
- **Per-partition temporal pipeline**: `getmany()` đưa record vào FIFO riêng; mỗi partition có một mutating worker, còn các partition độc lập chạy song song tới `PROCESSING_PARTITION_CONCURRENCY`.
- **Batch coalescing**: Worker ghép fragment cùng partition tới `BATCH_MAX_RECORDS`; mặc định IP=16 và Swap=24 để giữ ngân sách p95.
- **Process-level write combiner**: Các partition worker gửi batch vào queue chung của đúng process. Combiner chờ tối đa 2ms/64 record rồi gọi `process_batch()` một lần; mỗi partition vẫn chờ future riêng nên FIFO và durability barrier không đổi.
- **Age-aware backpressure**: `pause()` khi FIFO đạt 75% hoặc record cũ nhất chờ 12ms; `resume()` tại 25% và 4ms. Partition nhanh không bị partition chậm chặn, còn hysteresis tránh flapping vì jitter ngắn.
- **Coalesced Manual Offset Commit**: Sau batch thành công (`enable_auto_commit=False`), worker công bố offset cho coordinator. Coordinator commit nhiều partition mỗi 25ms/512 record, tách Kafka RTT khỏi critical path và giảm request tới broker. Khi rebalance/crash, cửa sổ chưa commit được phát lại và store xử lý idempotent/version-fenced.
- **Dead Letter Queue (DLQ)**: Khi một record hoặc batch bị lỗi cấu trúc dữ liệu nghiêm trọng vượt quá `MAX_BATCH_RETRIES` (mặc định 3 lần), toàn bộ thông tin nguồn (topic, partition, offset, error_type, payload) được đẩy vào `<topic>.dlq`.

### 2.3. `DatabasePool` (`db.py`)
Lớp bọc `asyncpg.Pool` tối ưu hoá hiệu năng cho PostgreSQL:
- **Member-owned Connection Pool**: Mỗi member/PID sở hữu đúng một pool cho các partition worker của nó, cấu hình qua `DB_POOL_MIN` và `DB_POOL_MAX`; không chia pool qua process.
- **Giao dịch Nguyên Tử 4 Bảng (`_persist_swap_batch`)**:
  Thực thi trong cùng 1 `connection.transaction()`:
  1. `_upsert_state`: Upsert bảng state hiện tại (`msisdn_sim` hoặc `msisdn_device`) qua mệnh đề `UNNEST` kết hợp điều kiện so sánh phiên bản `(last_event_at, last_source_partition, last_source_offset)`.
  2. `_insert_history`: Ghi nhật ký lịch sử đổi SIM/thiết bị (`sim_swap_history`, `device_swap_history`).
  3. `_insert_audit`: Ghi vết kiểm toán hệ thống (`audit_log`).
  4. `_insert_outbox`: Ghi thông báo chờ gửi (`notification_log`) cho các subscription đang hoạt động.
- **Session State Tracking (`persist_session_batch`)**: Cập nhật `radius_session_state` với version fence; shared/exclusive row lock trên `radius_nas_off_watermark` ngăn Start cũ chạy khác partition hồi sinh state sau Accounting-Off mà không tuần tự hóa các Start thông thường.

### 2.4. `ModuleMetrics` (`metrics.py`)
Hệ thống thu thập và xuất dữ liệu đo lường (Telemetry):
- **Prometheus Exporter**: Tự động đăng ký các metrics chuẩn:
  - `pipeline_batch_processed_total` (Counter)
  - `pipeline_events_detected_total` (Counter)
  - `pipeline_batch_errors_total` (Counter)
  - `pipeline_partition_queue_records`, `pipeline_partition_queue_oldest_seconds`, `pipeline_partition_workers`, `pipeline_partitions_paused` (Gauge)
  - `pipeline_offset_commit_latency_seconds`, `pipeline_offset_commit_pending_records`, `pipeline_offset_commit_records_total`, `pipeline_offset_commit_errors_total`
  - `pipeline_batch_latency_seconds` (Histogram)
  - `pipeline_stage_latency_seconds` (Histogram theo stage: state, postgres, redis)
  - `pipeline_preprocess_message_lag_seconds` (Histogram từ lúc nhận UDP đến khi bắt đầu business processing)
  - `pipeline_kafka_lag_records` (Gauge theo từng member)
  - `pipeline_e2e_message_lag_seconds` (Histogram đo độ trễ từ lúc gói tin vào Ingestion đến khi hoàn tất ghi DB/Redis)
- **Sliding-Window Structured Logger**: Định kỳ in ra log phân khối trực quan (`|`), theo dõi riêng biệt:
  - Throughput (`recv`, `success`, `pg`, `rds`)
  - Latency (`batch_avg`, `stage(...)`, `pre_process_p95`, `processing_p95`, `e2e_avg/p95/max`)
  - Sự kiện Swap (`events_detected(+delta)`, `ignored`)
  - **Giám sát thất thoát (`data_loss = errors + dlq`)**
  - Số liệu tích lũy (`Totals`).

### 2.5. `redis_client.py`
Khởi tạo kết nối Redis hỗ trợ 2 chế độ:
- **Chế độ Standalone (Môi trường Dev)**: Kết nối trực tiếp qua `REDIS_HOST` và `REDIS_PORT`.
- **Chế độ Sentinel HA (Môi trường Production)**: Tự động khám phá Master Node thông qua danh sách Sentinel Nodes (`REDIS_SENTINELS`), hỗ trợ tự động failover mà không làm gián đoạn pipeline.

### 2.6. `events.py`
Bộ công cụ chuẩn hoá và kiểm tra ràng buộc dữ liệu:
- `canonical_msisdn()`: Chuẩn hoá số thuê bao theo định dạng quốc tế E.164 (bắt đầu bằng dấu `+`, từ 8 đến 15 chữ số).
- `parse_event_time()`: Phân giải timestamp từ chuỗi ISO-8601 hoặc Unix epoch thành đối tượng `datetime` có múi giờ chuẩn UTC.
- `normalize_status()`: Chuẩn hoá các trạng thái phiên RADIUS (`start`, `stop`, `interim-update`, `accounting-on`, `accounting-off`).
- `event_id()`: Tạo khóa định danh duy nhất cho sự kiện để đảm bảo tính Idempotency.

> 📖 **Báo cáo Kỹ thuật Chuyên sâu**: Đọc tài liệu phân tích kiến trúc chi tiết tại [`docs/BAO_CAO_KY_THUAT_PIPELINE.md`](../../docs/BAO_CAO_KY_THUAT_PIPELINE.md).

---

## 3. Ràng buộc toàn vẹn & Fencing Versioning

Để đảm bảo dữ liệu không bị sai lệch khi các sự kiện mạng đến sai thứ tự (Out-of-Order Delivery), hệ thống áp dụng cơ chế Fencing Tuple 3 thành phần:

$$\text{Version} = (\text{event\_timestamp}, \text{source\_partition}, \text{source\_offset})$$

Một bản ghi mới chỉ được phép cập nhật trạng thái nếu:

$$(\text{incoming.event\_timestamp}, \text{incoming.partition}, \text{incoming.offset}) > (\text{current.last\_event\_at}, \text{current.partition}, \text{current.offset})$$
