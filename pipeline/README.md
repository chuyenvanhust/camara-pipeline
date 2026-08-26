# `pipeline/` — Xử lý dữ liệu RADIUS accounting

Thư mục này chứa toàn bộ logic đưa dữ liệu RADIUS accounting từ CSV vào Kafka, xử lý qua 3
consumer module song song, và dispatch notification callback. Đây là phần lõi của dự án —
`api/` chỉ đọc dữ liệu mà pipeline này ghi ra.

## Thành phần đang hoạt động

| Thư mục / file | Vai trò | Chạy như thế nào |
|---|---|---|
| `run_pipeline.py` | Orchestrator 3 consumer | `python -m pipeline.run_pipeline [--duration N]` |
| `ingestion/` | Stage 1 — CSV/UDP sang Kafka topic `radius.accounting.raw` | Chạy độc lập: `python -m pipeline.ingestion.producer --file <csv>` hoặc `--udp --port 1813` |
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

1. Đảm bảo Kafka topic `radius.accounting.raw` tồn tại (mặc định 8
   partition).
2. Khởi động Prometheus metrics server (`METRICS_PORT`, mặc định `9200`) — nếu
   `prometheus_client` không được cài, tự động bỏ qua thay vì crash.
3. Tạo **1 `DatabasePool` dùng chung** rồi truyền vào cả 3 consumer — tránh mỗi consumer tự
   mở pool riêng (xem thêm ở `modules/shared/README.md`, mục F-09).
4. Khởi động 3 `asyncio.Task` chạy song song, mỗi task là `.run()` của 1 consumer. Trong mỗi
   poll, các partition được gom thành `PROCESSING_PARTITION_CONCURRENCY` shard; shard chạy
   song song nhưng offset trong từng partition vẫn đúng thứ tự.
5. Đăng ký `SIGINT`/`SIGTERM` **duy nhất tại orchestrator** — các consumer con không tự bắt
   signal, tránh trường hợp 1 consumer dừng sớm trong khi 2 cái còn lại vẫn chạy dở batch.
6. Một `supervisor` task theo dõi: nếu 1 trong 3 consumer chết ngoài ý muốn (exception chưa
   bắt), toàn bộ pipeline sẽ dừng theo — fail-fast thay vì chạy thiếu 1 module mà không ai biết.
7. Mỗi `THROUGHPUT_LOG_INTERVAL_SECONDS` in log số message, throughput và số event
   hiện được, theo từng consumer group.
8. Khi dừng (hết `--duration`, nhận signal, hoặc 1 consumer chết): gọi `stop()` từng consumer,
   đóng `DatabasePool` dùng chung, thoát với exit code khác 0 nếu có lỗi.

## `ingestion/` — Stage 1 (CSV → Kafka)

`RadiusLogProducer.publish_csv()` đọc CSV qua `LocalCSVReader` (generator, không load hết
file vào RAM), gửi từng record lên Kafka với `key=msisdn` (đảm bảo tất cả sự kiện của cùng
1 thuê bao vào cùng 1 partition → giữ đúng thứ tự xử lý theo thời gian).

Điểm cần lưu ý khi đọc/sửa code:

- **Record thiếu `msisdn` bị loại bỏ hoàn toàn**, không gửi lên Kafka với key rỗng — nếu gửi
  với key rỗng, Kafka sẽ round-robin record đó sang partition ngẫu nhiên, phá vỡ đảm bảo thứ
  tự cho MSISDN đó.
- Producer cố định `acks=all` + `enable_idempotence=true`; ACK được kiểm tra trước khi tăng
  counter thành công.

Biến môi trường riêng của `ingestion/`:

| Biến | Mặc định | Ý nghĩa |
|---|---|---|
| `INGESTION_BATCH_SIZE_BYTES` | `262144` (256KB) | Kích thước batch gửi Kafka producer |
| `INGESTION_LINGER_MS` | `20` | Thời gian chờ gom batch trước khi gửi |
| `INGESTION_COMPRESSION_TYPE` | `lz4` | Nén dữ liệu gửi Kafka |
| `INGESTION_FLUSH_EVERY_N_RECORDS` | `1000` | Số future CSV chờ ACK cùng lúc |
| `RADIUS_UDP_QUEUE_MAX_RECORDS` | `20000` | Bounded queue giữa UDP và Kafka |
| `RADIUS_UDP_KAFKA_BATCH_RECORDS` | `500` | Số record tối đa mỗi batch Kafka |
| `RADIUS_UDP_KAFKA_BATCH_WAIT_MS` | `2` (Compose) | Thời gian gom một batch UDP |
| `RADIUS_UDP_KAFKA_MAX_INFLIGHT_BATCHES` | `8` | Số batch chờ Kafka ACK song song; thứ tự enqueue vẫn được giữ |

## `dispatcher/` — Notification Outbox Dispatcher

Đọc chi tiết đầy đủ ở [`modules/shared/README.md`](modules/shared/README.md) (mục Outbox
pattern), vì dispatcher dùng chung `DatabasePool` từ `shared/db.py`. Tóm tắt: dispatcher là
process **độc lập hoàn toàn** với 3 consumer, poll bảng `notification_log` (status `PENDING`
hoặc `FAILED` đã đến hạn retry), claim bằng `FOR UPDATE SKIP LOCKED` (an toàn khi chạy nhiều
instance dispatcher song song), gửi HTTP POST tới `callback_url` của subscription, và cập
nhật lại status (`SENT` / `FAILED` với backoff / `DEAD` sau `DISPATCHER_MAX_ATTEMPTS` lần).

Phải chạy dispatcher như 1 process riêng song song với `run_pipeline.py` nếu muốn subscriber
Open Gateway thực sự nhận được callback — pipeline chính **không** tự gửi HTTP.

## Log throughput

Mỗi `THROUGHPUT_LOG_INTERVAL_SECONDS` (mặc định 10 giây), UDP/CSV producer ghi
`stage=producer`; từng consumer ghi `stage=processing`. Log chứa tổng tích lũy và tốc độ
cửa sổ cho Kafka receive/ack, xử lý thành công, DLQ, PostgreSQL records và Redis mutations.
`rate=0` nghĩa là tiến trình đang chạy nhưng không nhận dữ liệu trong cửa sổ đó.
`postgres_records` chỉ tăng sau commit thành công; `redis_records` chỉ tăng khi Redis thực
sự thay đổi state.

UDP ingestion dùng đúng một socket và một `AIOKafkaProducer`. Receiver giải mã liên tục rồi
đưa record vào bounded queue; publisher lấy tối đa `RADIUS_UDP_KAFKA_BATCH_RECORDS` hoặc chờ
`RADIUS_UDP_KAFKA_BATCH_WAIT_MS`. Tối đa `RADIUS_UDP_KAFKA_MAX_INFLIGHT_BATCHES` batch được
chờ ACK đồng thời, loại bỏ thời gian chết một round-trip giữa hai batch. Các biến
`queue_depth`, `queue_high_watermark` và `queue_dropped_total` trong log cho biết buffer có
đang quá tải hay không. Tăng `RADIUS_UDP_RECEIVE_BUFFER_BYTES` chỉ giúp hấp thụ burst ngắn;
`queue_dropped_total > 0` nghĩa là tốc độ vào duy trì cao hơn khả năng Kafka trong thời gian
queue đã đầy.

Sender giả lập dùng pipeline hai tầng: thread encoder đọc CSV và prefetch packet vào bounded
queue; thread chính chỉ gửi UDP theo deadline tuyệt đối. AVP lặp lại được cache bằng LRU có
giới hạn, còn session id cardinality cao không cache. `--pacing-window-ms` tạo micro-burst và
`--max-catchup-ms` giới hạn phần pacing debt được bù, tránh burst vô hạn sau một lần host bị
pause. Benchmark cục bộ ngày 2026-08-26 đạt 300.000 packet/20,00s (14.999,5 pkt/s).

Giới hạn đã đo trên Compose hiện tại: ingestion nhận và Kafka ACK đủ 15k pkt/s, không drop;
một container pipeline 2 CPU chạy cả ba consumer xử lý khoảng 9–10k record/s và cần drain
Kafka sau burst. Muốn **sustained** 15k end-to-end phải scale consumer thành nhiều process/
container theo consumer group hoặc cấp thêm CPU; không tăng sender tiếp nếu consumer lag tăng
liên tục.
