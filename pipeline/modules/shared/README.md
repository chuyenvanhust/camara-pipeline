# `pipeline/modules/shared/` — Hạ tầng dùng chung cho 3 consumer module

Không phải 1 module xử lý sự kiện — đây là code nền tảng mà `ip_msisdn`, `device_swap`,
`sim_swap` đều kế thừa/import. Sửa file trong này ảnh hưởng tới cả 3 module cùng lúc.

| File | Nội dung |
|---|---|
| `base_consumer.py` | `BaseKafkaConsumer` — vòng lặp đọc Kafka, retry, DLQ, manual commit |
| `db.py` | `DatabasePool` — pool Postgres dùng chung + toàn bộ query/transaction |
| `metrics.py` | `ModuleMetrics` — counter in-memory + export Prometheus |
| `notification.py` | Hàm `send_callback()` gửi HTTP với backoff — hiện chỉ còn dùng bởi `dispatcher/`, không còn được consumer gọi trực tiếp |

## `BaseKafkaConsumer` (`base_consumer.py`)

Lớp trừu tượng mọi consumer module đều kế thừa. Có 1 abstract method bắt buộc override
(`process_message`, xử lý từng message riêng lẻ — giữ lại để tương thích ngược) và 1 method
nên override để có hiệu năng tốt (`process_batch`, mặc định fallback gọi `process_message`
tuần tự nếu subclass không override).

**Vòng lặp chính (`run()`):**

1. `consumer.getmany(timeout_ms=BATCH_TIMEOUT_MS, max_records=BATCH_MAX_RECORDS)` — batch
   thích ứng: nếu throughput cao thì gom đủ `BATCH_MAX_RECORDS` (mặc định 500) rồi mới xử lý;
   nếu thưa thì tối đa `BATCH_TIMEOUT_MS` (mặc định 100ms) là flush, tránh latency cao khi ít
   dữ liệu.
2. Xử lý **theo từng partition riêng** (`for tp, tp_messages in data.items()`) — quan trọng
   vì offset phải commit đúng theo partition, gộp chung nhiều partition vào 1 lần commit có
   thể commit nhầm offset.
3. Gọi `process_batch()`. Nếu lỗi: retry tối đa `MAX_BATCH_RETRIES` lần (mặc định 3) với
   exponential backoff (`min(2**attempt, 10)` giây).
4. Nếu vẫn lỗi sau khi hết số lần retry: đẩy toàn bộ batch (kèm lỗi, offset, partition gốc)
   vào topic `<topic>.dlq` — **không** bỏ dữ liệu, không crash cả pipeline vì 1 batch lỗi.
5. **Chỉ commit offset sau khi bước 3 hoặc 4 hoàn tất** (`enable_auto_commit=False` khi khởi
   tạo `AIOKafkaConsumer`). Đây là điểm mấu chốt cho durability: nếu consumer crash giữa lúc
   xử lý, khi restart sẽ đọc lại đúng batch chưa commit thay vì mất dữ liệu.

**Vì sao dùng manual commit thay vì auto-commit:** auto-commit của Kafka chạy theo timer độc
lập với việc xử lý xong hay chưa — nếu consumer crash giữa chừng, offset có thể đã bị
auto-commit dù batch chưa ghi xong xuống DB, dẫn đến mất dữ liệu vĩnh viễn (message đó không
bao giờ được đọc lại). Manual commit sau khi xử lý xong loại bỏ hoàn toàn rủi ro này, đổi lại
phải tự viết logic retry/DLQ.

**Shutdown:** consumer con không tự bắt `SIGINT`/`SIGTERM` — chỉ set `self.running = False`
khi được orchestrator (`run_pipeline.py`) yêu cầu dừng, để tránh 1 module dừng lệch nhịp so
với 2 module còn lại.

## `DatabasePool` (`db.py`)

Bọc `asyncpg.Pool`, cấu hình qua env `DB_HOST`/`DB_PORT`/`DB_USER`/`DB_PASSWORD`/`DB_NAME`
(hoặc `DATABASE_URL` trực tiếp). Pool size mặc định `min=4, max=12` — **dùng chung cho cả 3
consumer trong 1 process**, không phải mỗi consumer 1 pool riêng, vì `run_pipeline.py` tạo
1 `DatabasePool` rồi truyền (`db=shared_db`) vào cả 3. `acquire(timeout=10)` — nếu pool cạn,
fail rõ ràng sau 10s thay vì treo vô hạn.

Có 2 nhóm method:

- **Single-record** (`get_current_imei`, `upsert_device_state`, `record_device_swap_history`,
  `insert_audit_log`, `insert_notification_log`, ...): mỗi lời gọi 1 round-trip DB riêng,
  giữ lại cho `process_message()` (đường xử lý từng message, ít dùng trong production).
- **Batch/atomic** (`batch_get_current_imei`, `batch_upsert_*`, và quan trọng nhất là
  `commit_sim_swap_batch` / `commit_device_swap_batch`): dùng cho `process_batch()`, tối ưu
  round-trip DB.

**`commit_sim_swap_batch()` / `commit_device_swap_batch()`** là phần quan trọng nhất của file
này. Toàn bộ 4 loại ghi cho 1 batch — upsert state hiện tại, insert lịch sử swap (qua
`copy_records_to_table`, nhanh hơn nhiều `executemany` cho insert thuần), insert audit log, và
insert notification log (status `PENDING`) — chạy trong **cùng một `conn.transaction()`**.
Nếu bất kỳ bước nào lỗi, toàn bộ rollback: không bao giờ tồn tại trạng thái "đã đổi SIM trong
bảng state nhưng thiếu bản ghi lịch sử tương ứng", và không bao giờ tạo notification cho 1
sự kiện mà cuối cùng không được ghi nhận.

## `ModuleMetrics` (`metrics.py`)

Counter in-memory đơn giản (`processed`, `success`, `ignored`, `events_detected`, `errors`,
`notifications_sent`, `notifications_failed`) dùng cho heartbeat log của orchestrator. Đồng
thời **tự động** đẩy song song sang Prometheus (`Counter`) cho 3 metric: `processed`,
`events_detected`, `errors` — có label `group_id` để phân biệt 3 consumer group trên cùng 1
dashboard Grafana. Nếu `prometheus_client` chưa cài, phần Prometheus tự tắt (log debug), phần
counter in-memory vẫn hoạt động bình thường — không phụ thuộc cứng vào Prometheus.

## Outbox pattern (F-03) — vì sao tách callback khỏi consumer

`notification.py` chứa `send_callback()` (HTTP POST + exponential backoff), nhưng **không
còn được gọi trực tiếp bởi bất kỳ consumer nào**. Lý do: nếu consumer gọi HTTP ngay trong lúc
xử lý batch, 1 subscriber Open Gateway chậm hoặc down sẽ làm nghẽn toàn bộ throughput Kafka
consumer (batch tiếp theo phải chờ HTTP timeout của batch trước).

Thay vào đó: consumer chỉ `INSERT INTO notification_log (..., status='PENDING')` trong cùng
transaction Postgres ở bước ghi DB (xem `commit_*_batch` ở trên). Một process hoàn toàn tách
biệt — `pipeline/dispatcher/notification_dispatcher.py` — poll bảng này định kỳ và mới thực
sự gọi `send_callback`-tương-đương. Xem chi tiết dispatcher ở
[`../../README.md`](../../README.md) (mục `dispatcher/`) vì code dispatcher nằm ngoài
`modules/`.

File `sim_swap/notifier.py` và `device_swap/notifier.py` chỉ còn là stub log cảnh báo
deprecated — giữ lại cho tương thích ngược, an toàn để xoá sau khi xác nhận dispatcher chạy
ổn định.
