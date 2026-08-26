# `pipeline/` — Xử lý dữ liệu RADIUS accounting

Thư mục này chứa toàn bộ logic đưa dữ liệu RADIUS accounting từ CSV vào Kafka, xử lý qua 3
consumer module song song, và dispatch notification callback. Đây là phần lõi của dự án —
`api/` chỉ đọc dữ liệu mà pipeline này ghi ra.

## Thành phần đang hoạt động

| Thư mục / file | Vai trò | Chạy như thế nào |
|---|---|---|
| `run_pipeline.py` | Orchestrator chính | `python -m pipeline.run_pipeline [--input file.csv] [--duration N]` |
| `ingestion/` | Stage 1 — đọc CSV, đẩy vào Kafka topic `radius.accounting.raw` | Được `run_pipeline.py` gọi tự động khi có `--input`; cũng có thể chạy độc lập qua `python -m pipeline.ingestion.producer --file <csv>` |
| `modules/shared/` | Code dùng chung: `BaseKafkaConsumer`, `DatabasePool`, `ModuleMetrics` | Không tự chạy — được 3 module bên dưới kế thừa/import |
| `modules/ip_msisdn/` | Module 1 — theo dõi ánh xạ IP↔MSISDN | Task con của `run_pipeline.py`, consumer group `cg-ip-msisdn` |
| `modules/device_swap/` | Module 2 — phát hiện đổi thiết bị (IMEI) | Task con của `run_pipeline.py`, consumer group `cg-device-swap` |
| `modules/sim_swap/` | Module 3 — phát hiện đổi SIM (IMSI) | Task con của `run_pipeline.py`, consumer group `cg-sim-swap` |
| `dispatcher/` | Gửi HTTP callback tới subscriber Open Gateway theo outbox pattern | **Chạy tách biệt**, KHÔNG nằm trong `run_pipeline.py`: `python -m pipeline.dispatcher.notification_dispatcher` |

README chi tiết theo từng module: [`modules/shared/README.md`](modules/shared/README.md),
[`modules/ip_msisdn/README.md`](modules/ip_msisdn/README.md),
[`modules/device_swap/README.md`](modules/device_swap/README.md),
[`modules/sim_swap/README.md`](modules/sim_swap/README.md).

## Thư mục skeleton — không được import bởi runtime

`validation/`, `deduplication/`, `conflict_resolution/`, `processing/`, `state/`, `storage/`
là tàn dư của kiến trúc Spark cũ (xem ADR-0001 ở root). Xác nhận bằng cách grep: không có
`import pipeline.<các_thư_mục_này>` ở bất kỳ đâu trong `run_pipeline.py` hay `modules/`.
An toàn để xoá, nhưng để lại làm tài liệu tham khảo lịch sử.

## Luồng chạy của `run_pipeline.py`

1. Load `.env`, đảm bảo Kafka topic `radius.accounting.raw` tồn tại (tạo nếu chưa có, 4
   partition).
2. Khởi động Prometheus metrics server (`METRICS_PORT`, mặc định `9200`) — nếu
   `prometheus_client` không được cài, tự động bỏ qua thay vì crash.
3. Tạo **1 `DatabasePool` dùng chung** rồi truyền vào cả 3 consumer — tránh mỗi consumer tự
   mở pool riêng (xem thêm ở `modules/shared/README.md`, mục F-09).
4. Khởi động 3 `asyncio.Task` chạy song song, mỗi task là `.run()` của 1 consumer.
5. Nếu có `--input <file.csv>`: dùng `RadiusLogProducer` (trong `ingestion/`) đẩy toàn bộ
   file vào Kafka topic, đợi flush xong rồi mới tiếp tục.
6. Đăng ký `SIGINT`/`SIGTERM` **duy nhất tại orchestrator** — các consumer con không tự bắt
   signal, tránh trường hợp 1 consumer dừng sớm trong khi 2 cái còn lại vẫn chạy dở batch.
7. Một `supervisor` task theo dõi: nếu 1 trong 3 consumer chết ngoài ý muốn (exception chưa
   bắt), toàn bộ pipeline sẽ dừng theo — fail-fast thay vì chạy thiếu 1 module mà không ai biết.
8. Mỗi 5s in heartbeat log: số message đã xử lý + throughput (rec/s) + số swap event phát
   hiện được, theo từng consumer group.
9. Khi dừng (hết `--duration`, nhận signal, hoặc 1 consumer chết): gọi `stop()` từng consumer,
   đóng `DatabasePool` dùng chung, thoát với exit code khác 0 nếu có lỗi.

## `ingestion/` — Stage 1 (CSV → Kafka)

`RadiusLogProducer.publish_csv()` đọc CSV qua `LocalCSVReader` (generator, không load hết
file vào RAM), gửi từng record lên Kafka với `key=msisdn` (đảm bảo tất cả sự kiện của cùng
1 thuê bao vào cùng 1 partition → giữ đúng thứ tự xử lý theo thời gian).

Điểm cần lưu ý khi đọc/sửa code:

- **Record thiếu `msisdn` bị loại bỏ hoàn toàn**, không gửi lên Kafka với key rỗng — nếu gửi
  với key rỗng, Kafka sẽ round-robin record đó sang partition ngẫu nhiên, phá vỡ đảm bảo thứ
  tự cho MSISDN đó.
- Producer mặc định `acks=all` + `enable_idempotence=true` để đảm bảo không mất và không
  duplicate record khi có retry mạng — có thể override qua `INGESTION_ACKS`/
  `INGESTION_ENABLE_IDEMPOTENCE` cho môi trường dev muốn ingest nhanh hơn, đánh đổi durability.
- Producer **kiểm tra kết quả thật của `asyncio.gather()`** sau mỗi cụm `FLUSH_EVERY_N_RECORDS`
  (mặc định 10.000) record — nếu có record gửi thất bại, log rõ số lượng và ví dụ lỗi đầu
  tiên, không âm thầm bỏ qua.

Biến môi trường riêng của `ingestion/`:

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `INGESTION_BATCH_SIZE_BYTES` | `262144` (256KB) | Kích thước batch gửi Kafka producer |
| `INGESTION_LINGER_MS` | `50` | Thời gian chờ gom batch trước khi gửi |
| `INGESTION_ACKS` | `all` | `0`/`1`/`all` — mức độ đảm bảo durability |
| `INGESTION_COMPRESSION_TYPE` | `lz4` | Nén dữ liệu gửi Kafka |
| `INGESTION_ENABLE_IDEMPOTENCE` | `true` | Tránh duplicate khi producer tự retry |
| `INGESTION_FLUSH_EVERY_N_RECORDS` | `10000` | Tần suất kiểm tra kết quả gửi + log throughput |

## `dispatcher/` — Notification Outbox Dispatcher

Đọc chi tiết đầy đủ ở [`modules/shared/README.md`](modules/shared/README.md) (mục Outbox
pattern), vì dispatcher dùng chung `DatabasePool` từ `shared/db.py`. Tóm tắt: dispatcher là
process **độc lập hoàn toàn** với 3 consumer, poll bảng `notification_log` (status `PENDING`
hoặc `FAILED` đã đến hạn retry), claim bằng `FOR UPDATE SKIP LOCKED` (an toàn khi chạy nhiều
instance dispatcher song song), gửi HTTP POST tới `callback_url` của subscription, và cập
nhật lại status (`SENT` / `FAILED` với backoff / `DEAD` sau `DISPATCHER_MAX_ATTEMPTS` lần).

Phải chạy dispatcher như 1 process riêng song song với `run_pipeline.py` nếu muốn subscriber
<<<<<<< ours
Open Gateway thực sự nhận được callback — pipeline chính **không** tự gửi HTTP.
=======
Open Gateway thực sự nhận được callback — pipeline chính **không** tự gửi HTTP.
>>>>>>> theirs
