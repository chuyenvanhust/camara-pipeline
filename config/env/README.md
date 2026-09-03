# Hướng dẫn hardware profiles `config/env/*.env`

Các file trong thư mục này được nạp **sau** `.env`. Chúng chỉ override topology,
batching và resource limit; secret, DSN và địa chỉ dịch vụ vẫn lấy từ `.env`.

## 1. Ma trận profile hiện tại

| Profile | Phần cứng đích | Kafka partitions | Replica mỗi group | Partition/member | Concurrency | Observe-only target | SLO cần chứng nhận |
|---|---:|---:|---:|---:|---:|---:|---:|
| `8gb.env` | 12 CPU / 8 GiB | 9 | 3 | 3 | 3 | 800/s, burst 900/s | E2E p95 <100ms |
| `16gb.env` | 16 CPU / 16 GiB | 12 | 4 | 3 | 3 | 3.9k/s candidate | E2E p95 <100ms |
| `32gb.env` | 24 CPU / 32 GiB | 24 | 6 | 4 | 4 | 7.8k/s candidate | E2E p95 <100ms |
| `64gb.env` | 32 CPU / 64 GiB | 48 | 8 | 6 | 6 | 15.5k/s candidate | E2E p95 <100ms |

`PIPELINE_RECOMMENDED_SUSTAINED_PPS` và
`PIPELINE_RECOMMENDED_BURST_PPS` là nhãn capacity cho telemetry/benchmark. Chúng
không throttle và không drop ở ingestion. Capture server bên ngoài phải điều tiết
và replay khi workload vượt khả năng đã chứng nhận.

Profile 8 GiB là baseline bảo thủ. Log ngắn đã cho thấy 2.5k/s có thể chạy với
loss=0 và phần lớn cửa sổ p95 dưới 100ms, nhưng IP-MSISDN còn một cửa sổ sát/qua
100ms; vì vậy tài liệu không nâng target chính thức nếu chưa có soak test dài.
Target 16/32/64 GiB cũng là điểm bắt đầu test, không phải phép nội suy được bảo đảm.

## 2. Cách nạp profile

```bash
# Reset benchmark và chạy phải dùng cùng một profile
bash scripts/reset.sh config/env/16gb.env
bash scripts/run_pipeline.sh config/env/16gb.env

# Tương đương khi dùng Compose trực tiếp
docker compose --env-file .env --env-file config/env/16gb.env config --quiet
docker compose --env-file .env --env-file config/env/16gb.env ps
```

Có thể đặt `CAMARA_ENV_PROFILE=config/env/16gb.env` nếu script được gọi nhiều lần.
Không truyền một file profile làm đối số `--init-db`; `run_pipeline.sh` tự chạy
migration qua service `migrate`.

`run_ingest_udp.sh [profile.env]` và `run_ingest_csv.sh FILE [profile.env]` cũng
nạp profile theo cùng thứ tự. Điều này tránh việc recreate riêng ingestion bằng
default của `.env` trong khi phần còn lại của stack đang chạy profile khác.

## 3. Topology và thứ tự dữ liệu

- `CONSUMERS_PER_GROUP=1` là invariant. Một replica = một container/PID Python =
  một Kafka member = một DB pool = một Redis client.
- Scale bằng `PIPELINE_IP_REPLICAS`, `PIPELINE_DEVICE_REPLICAS` và
  `PIPELINE_SIM_REPLICAS`, không tăng `CONSUMERS_PER_GROUP`.
- Kafka phân phối partition trong từng group. Mỗi profile đặt số partition chia
  gần đều cho replica và concurrency bằng số partition trung bình mỗi replica.
- Cùng MSISDN luôn dùng cùng Kafka key/partition. Một worker duy nhất thay đổi
  partition đó; tăng concurrency chỉ song song hóa các partition độc lập.
- Pipeline container không có CFS `cpus` hard quota; `cpu_shares` chỉ là trọng số
  IP=1024, Device/SIM=768 khi host thực sự tranh chấp.

## 4. Batching latency-first

| Tham số | 8/16 GiB | 32 GiB | 64 GiB | Ý nghĩa |
|---|---:|---:|---:|---|
| Ingestion batch/wait | 64 / 1ms | 64 / 1ms | 64 / 1ms | Publish group trước Kafka accumulator |
| IP partition batch/wait | 48 / 5ms | 48 / 5ms | 48 / 5ms | IP ghi PG và Redis cho mọi event |
| Swap partition batch/wait | 64 / 5ms | 64 / 5ms | 64 / 5ms | No-change đi Redis + checkpoint nền |
| Process combiner wait/max | 2ms / 64 | 2ms / 96 | 2ms / 128 | Gom partition độc lập trong một process |
| Offset commit interval/max | 25ms / 512 | 25ms / 512 | 25ms / 512 | Ngoài business E2E |
| Swap checkpoint interval | 150ms | 150ms | 150ms | Deferred PG watermark cho no-change |

`BATCH_MAX_RECORDS` là trần, không phải batch bắt buộc. Partition worker dừng gom
khi FIFO hiện tại hết, nên tải thấp không phải chờ đủ 48/64 record. Timeout 5ms là
poll/coalescing budget; write combiner có budget riêng 2ms.

## 5. Kafka và ingestion

- Profile 8 GiB benchmark dùng RF=1 để đo latency trên một host; không được xem là
  topology HA production. 16/32/64 GiB dùng RF=3 và `min.insync.replicas=2`.
  Producer raw mặc định
  `acks=1` vì capture là nguồn bền/replay; DLQ producer vẫn `acks=all` + idempotent.
- Ingestion key-shard vào worker queues, micro-batch rồi gửi Kafka. Nhiều Kafka
  producer ở profile lớn chỉ tăng lane; cùng key vẫn đi một worker/producer trong
  vòng đời process nên không đảo thứ tự.
- Queue RAM 30k/60k/120k chỉ hấp thụ burst ngắn, không phải durable spool và không
  sửa được sustained throughput thấp hơn input.
- Warn budget profile lớn là Kafka persist p95 30ms và queue residence p95 12ms.
  Cảnh báo không làm thay đổi đường dữ liệu.
- `RADIUS_UDP_RECEIVE_BUFFER_BYTES` là giá trị application yêu cầu. Phải kiểm tra
  `receive_buffer_actual` và Linux `net.core.rmem_max`; tăng env mà kernel không
  cho phép không có tác dụng.

## 6. PostgreSQL connection budget

Mỗi replica có pool riêng. Tổng connection pipeline phải tính:

```text
pipeline_pool = R_ip*P_ip + R_device*P_device + R_sim*P_sim
total_budget  = pipeline_pool + API + dispatcher + migrate/admin + reserve
```

| Profile | Pool IP | Pool Device | Pool SIM | Pipeline tối đa | PG max_connections |
|---|---:|---:|---:|---:|---:|
| 8 GiB | 3×4 | 3×4 | 3×4 | 36 | lấy từ `.env` (80) |
| 16 GiB | 4×8 | 4×4 | 4×4 | 64 | 120 |
| 32 GiB | 6×12 | 6×5 | 6×5 | 132 | 200 |
| 64 GiB | 8×16 | 8×7 | 8×7 | 240 | 300 |

Phần chênh dành cho API, dispatcher, migration, admin và failover. Không tăng pool
chỉ vì còn `max_connections`: `pool_acq` gần 0 nhưng PG stage cao nghĩa là bottleneck
nằm ở query/WAL/I/O, không phải thiếu connection.

Các profile 16/32/64 ghi rõ `POSTGRES_SYNCHRONOUS_COMMIT=on`. Thay đổi sang `off`
là thay đổi durability, không phải tuning vô hại và không liên quan tới UDP loss.

## 7. Memory và CPU

- `KAFKA_MEM_LIMIT` và heap là **trên mỗi broker**; Compose chạy ba broker.
- `PIPELINE_*_MEM_LIMIT` là **trên mỗi replica**; phải nhân với replica khi tính
  tổng RAM host.
- Kafka/PostgreSQL/Redis/ingestion có hard CPU quota theo profile. Business
  pipeline dùng `cpu_shares`, không dùng hard quota để tránh CFS throttling làm
  tăng tail latency.
- Không áp profile CPU 24/32 core lên host 12 core chỉ vì host có đủ RAM. Nếu chỉ
  nâng RAM, giữ lại các biến `*_CPUS`, replica và partition của profile CPU thấp.

## 8. Quy trình chứng nhận target

1. Reset và run bằng cùng profile.
2. Warm-up đến khi cache hit ổn định; không trộn Kafka state của lần chạy trước.
3. Chạy sustained ít nhất 30–60 phút, sau đó chạy burst candidate riêng.
4. Chỉ pass khi đồng thời:
   - event-level E2E p95 <100ms trên mọi module, không chỉ trung bình cửa sổ;
   - ingestion `queue_dropped=0`, `publish_failed=0`;
   - processing `err=0`, `dlq=0` ngoài dữ liệu invalid chủ ý;
   - Kafka lag và partition queue không tăng theo thời gian;
   - kernel UDP `RcvbufErrors/InErrors` không tăng;
   - không có CPU throttling kéo dài hoặc PG/Redis/Kafka saturation.
5. Nếu fail, hạ tốc độ capture hoặc tăng đúng stage có p95/queue/CPU saturation;
   không tăng queue để che backlog và không dùng hard admission tại UDP receiver.

Luồng dữ liệu chi tiết nằm tại
[`docs/PIPELINE_ARCHITECTURE.md`](../../docs/PIPELINE_ARCHITECTURE.md); diễn giải
từng biến và công thức sizing nằm tại
[`docs/HARDWARE_TUNING_GUIDE.md`](../../docs/HARDWARE_TUNING_GUIDE.md).

`.env.production.example` chỉ là overlay mẫu cho secret/endpoint production; nó
không nhân bản các tham số tuning. Hãy đưa secret vào `.env`, rồi luôn chọn đúng
profile hardware khi gọi `reset.sh` và `run_pipeline.sh`.

Trên Linux cần Redis Sentinel/host networking, thêm `docker-compose.prod.yml` sau
file gốc nhưng vẫn nạp hardware profile. Override production không định nghĩa lại
CPU/RAM/batch/pool:

```bash
docker compose --env-file .env --env-file config/env/32gb.env \
  -f docker-compose.yml -f docker-compose.prod.yml config --quiet
```
