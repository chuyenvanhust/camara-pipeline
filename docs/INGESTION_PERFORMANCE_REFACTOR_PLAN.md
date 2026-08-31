# Kế hoạch khắc phục throughput, E2E lag và mất bản mirror

> **Trạng thái: tài liệu lịch sử/superseded.** Baseline 15k dưới đây giải thích sự cố cũ.
> Cấu hình vận hành hiện hành nằm trong `config/env/*.env`: ingestion batch 64/1ms,
> Kafka `acks=1`, queue theo profile; 8 GiB dùng baseline 800/s, còn target
> 3.9k/7.8k/15.5k của profile lớn phải được soak-test để bảo vệ
> E2E p95 <100ms. Profile 8 GiB dùng 4 worker/1 producer dùng chung và inflight
> 6/worker, tối đa 24 toàn process. Không áp các con số batch/queue trong phần lịch sử này.

## 1. Kết luận từ baseline 15.000 pkt/s

| Chặng | Quan sát | Kết luận |
|---|---:|---|
| Sender | ~15.000 pkt/s | Đạt mục tiêu, không phải bottleneck. |
| UDP receiver | ~15.000 record/s | Decode/socket theo kịp nguồn. |
| Ingestion → Kafka | ~10.300–12.000 record/s | Bottleneck của baseline một producer. |
| Processing | ~12.000 record/s/group, Kafka lag gần 0 | Theo kịp dữ liệu Kafka cung cấp. |
| Queue residence | p95 8–10,7 giây | Nguồn chính của E2E lag. |
| Queue dropped | 251.845/1.996.405 record nhận được | Mất bản mirror thật sau khi queue đầy. |

Ba triệu chứng có cùng một nguyên nhân dây chuyền: Kafka persistence thấp hơn đầu
vào, queue tăng liên tục, record nằm chờ nhiều giây rồi queue đầy và bắt đầu drop.

## 2. Refactor đã triển khai

1. Tăng micro-batch tối đa từ 250 lên 500 record.
2. Tăng Kafka producer batch buffer từ 256 KiB lên 512 KiB.
3. Áp dụng concurrency thích ứng:
   - bình thường: 4 inflight batch/worker;
   - queue shard đạt 25%: tối đa 10 inflight batch/worker;
   - trần toàn process: 24 batch.
4. Tách một producer dùng chung thành producer pool (mặc định 4). Mỗi worker
   được ánh xạ cố định tới producer và mỗi MSISDN luôn vào cùng worker, vì vậy
   tăng số Kafka sender loop mà không phá thứ tự sự kiện của thuê bao.
5. Tăng queue lên 300.000 để hấp thụ burst khoảng 20 giây tại 15k/s. Queue chỉ
   là burst buffer; nếu Kafka dài hạn thấp hơn nguồn, queue vẫn đầy và E2E lag
   vẫn tăng.
6. Bổ sung telemetry:
   - `gap = udp_in - kafka_persisted`;
   - backlog ước tính theo giây;
   - `worker_slot_wait_p95` để thấy giới hạn per-worker;
   - `global_wait_p95` cho semaphore toàn process;
   - percentile log được reset đúng theo từng cửa sổ 10 giây.
7. Bổ sung Prometheus alerts cho throughput deficit, queue >70%, Kafka persist
   p95 >200ms và mọi queue drop/publish failure.
8. Sửa `scripts/reset.sh`: Redis có `requirepass` nhưng lệnh cũ gọi `FLUSHALL`
   không xác thực, bỏ mất thông báo `NOAUTH` rồi vẫn báo thành công. Script mới
   dùng `REDISCLI_AUTH` và chỉ hoàn tất khi `DBSIZE=0`. Nếu không reset sạch,
   state mới hơn trong Redis sẽ khiến fencing chống replay bỏ toàn bộ dataset và
   số SIM/device swap hợp lệ vẫn bằng 0.

## 3. Giới hạn bảo đảm dữ liệu

UDP mirror không có ACK/backpressure nên repo không thể bảo đảm zero-loss tuyệt
đối. `queue_dropped=0` chỉ có thể duy trì khi capacity dài hạn lớn hơn input và
burst nằm trong khả năng buffer. Production cần một trong hai hợp đồng:

- capture server giữ durable log và hỗ trợ replay theo khoảng thời gian; hoặc
- capture server ghi Kafka/durable transport trực tiếp thay vì UDP nếu yêu cầu
  zero-loss end-to-end.

Khi `queue_dropped_total` tăng, dừng/giảm mirror, khắc phục bottleneck và replay
từ capture source. Không tăng RAM queue để che tải kéo dài.

## 4. Benchmark nghiệm thu

Chạy từ trạng thái sạch, warm-up 60 giây rồi đo liên tục ít nhất 15 phút ở 15k/s.

```bash
bash scripts/reset.sh
bash scripts/run_pipeline.sh

# Terminal 1
docker compose logs -f radius-ingestion pipeline-ip-msisdn pipeline-device-swap pipeline-sim-swap

# Terminal 2 (Linux/Git Bash): 13,5 triệu packet = 15 phút ở 15k/s
RADIUS_SENDER_MAX_PACKETS=13500000 \
  bash scripts/simulate_radius_device.sh data/radius_log.csv 15000 --loop
```

Windows CMD tương đương:

```bat
set RADIUS_SENDER_MAX_PACKETS=13500000 && bash scripts/simulate_radius_device.sh data/radius_log.csv 15000 --loop
```

Sau mỗi lần đổi cấu hình phải restart/recreate `radius-ingestion`; biến trong
`.env` không tự cập nhật vào container đang chạy.

Sau `reset.sh`, phải thấy `Redis ... dbsize=0`. Dataset hiện tại có 1.807 lần
chuyển IMEI và 1.754 lần chuyển IMSI theo thứ tự file; kết quả nghiệm thu có thể
thấp hơn nếu ingestion còn drop, nhưng không được bằng 0 khi state đã sạch.

### Điều kiện đạt

- `kafka_persisted >= 15.000 record/s` trung bình 5 phút;
- `gap` không dương liên tục quá 3 cửa sổ;
- queue không tăng đơn điệu và ổn định dưới 20%;
- `queue_dropped=0`, `publish_failed=0`;
- Kafka consumer lag không tăng liên tục;
- ingestion `queue_p95 < 50ms`;
- processing E2E p95 mục tiêu ban đầu `< 150ms`, sau soak test mới siết `< 100ms`.

### Nếu chưa đạt

1. Nếu `worker_slot_wait_p95` cao nhưng `global_wait_p95` thấp: kiểm tra persist
   p95 và broker trước. Chỉ A/B tăng producer từ 1 lên 2 nếu producer dùng chung
   thật sự bão hòa; không mặc định quay lại một producer cho mỗi worker.
2. Nếu `global_wait_p95` cao và Kafka persist p95 ổn định: thử total 32 trong
   một A/B test riêng; rollback nếu p99 hoặc broker CPU tăng mạnh.
3. Nếu Kafka persist p95 vẫn >200ms: không tăng inflight tiếp. Kiểm tra broker
   CPU, leader distribution, disk latency, replication và Docker host limits.
4. Nếu một process đã bão hòa CPU: scale ingestion trên Linux host networking
   bằng `SO_REUSEPORT`; capture source phải dùng nhiều source flows để kernel
   phân phối được tải.

Không thay đồng thời batch, inflight và tài nguyên broker trong các vòng A/B tiếp
theo; mỗi lần chỉ đổi một biến để xác định đúng giới hạn.
