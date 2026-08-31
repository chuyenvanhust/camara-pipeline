# Hardware profiles

`.env` la profile mac dinh cho may **12 vCPU / 8 GiB RAM**. Cac file trong thu muc
nay chi ghi de cac bien tai nguyen/hieu nang, khong lap lai secret hay dia chi dich vu.

| Profile | CPU | Partitions | IP replicas x members | Swap replicas x members | Admission ceiling | SLO |
|---|---:|---:|---:|---:|---:|---:|
| `8gb.env` / `.env` | 12 | 9 | 3 x 1 | 3 x 1 | 800 pkt/s sustained, 900 burst candidate | E2E p95 < 100ms |
| `16gb.env` | 16 | 12 | 4 x 1 | 4 x 1 | 3.9k pkt/s | E2E p95 < 100ms |
| `32gb.env` | 24 | 24 | 6 x 1 | 6 x 1 | 7.8k pkt/s | E2E p95 < 100ms |
| `64gb.env` | 32 | 48 | 8 x 1 | 8 x 1 | 15.5k pkt/s | E2E p95 < 100ms |

CPU quota là trần burst của từng container, không phải phần core được giữ chỗ.
Tổng trần của các đường nóng được đặt gần số core profile để broker và database
có thể hấp thụ tail ngắn; admission ceiling vẫn phải bảo đảm CPU host không bão
hòa kéo dài và còn thời gian chạy cho Redis, networking, kernel UDP, monitoring.

Profile 8 GiB không đặt CFS hard quota cho chín process pipeline. Kafka,
PostgreSQL, Redis và ingestion có tổng ceiling 8.5 CPU; các process business được
scheduler cho burst trên toàn bộ 12 CPU còn khả dụng. `cpu_shares` 1024/768/768
chỉ là trọng số tương đối IP/Device/SIM khi host thực sự tranh chấp CPU, không
giới hạn một replica ở 0.3-0.56 core như cấu hình cũ.

Moi member la mot container/PID va tu so huu DB pool, Redis client, Kafka client.
`CONSUMERS_PER_GROUP` bat buoc bang 1; scale bang `PIPELINE_*_REPLICAS`. Redis state
duoc tach DB 0/1/2 theo IP/Device/SIM, con PostgreSQL pool duoc nhan dien bang
`application_name` theo module. PostgreSQL vat ly van dung chung de giu atomic
outbox/audit/subscription; tach cluster can migration du lieu va API rieng.

Moi member co FIFO rieng cho tung partition. Cung mot MSISDN luon vao cung Kafka
partition, nen ba partition cua mot member co the xu ly dong thoi ma van giu dung
thu tu tren tung MSISDN. Profile 8 GiB dung concurrency=2/process; Kafka fetch
wait=2ms. Write combiner cap process gom cac partition doc lap toi da 2ms/64
record truoc mot lan goi PostgreSQL/Redis. Profile lon tang so process va
partition lanes. FIFO cuc bo chi giu toi da bon batch; partition pause tai 75% hoac khi
record cu nhat cho 12ms, va resume tai 25%/4ms.

Capture la nguon ben vung va phai admission-control theo sustained rate trong
profile. Ingestion dung `acks=1`; PostgreSQL van `synchronous_commit=on` va Kafka
offset chi duoc danh dau sau khi PostgreSQL + Redis hoan tat. Commit coordinator
gom offset da xu ly moi 25ms/512 records ngoai critical path. Crash co the replay
mot cua so nho; version fence/idempotency bao ve state va event.

Ingestion inflight duoc sizing theo bandwidth-delay product:
`ceil(target_records_per_second * kafka_persist_seconds / batch_records)`, sau do
them headroom cho p95. Queue RAM chi hap thu burst; no khong sua duoc sustained
throughput thap hon input.

Khoi dong profile 16 GiB qua script bootstrap co health-check:

```bash
bash scripts/run_pipeline.sh config/env/16gb.env
```

Khi can reset benchmark, bat buoc dung cung profile de topic duoc tao lai dung so
partition:

```bash
bash scripts/reset.sh config/env/16gb.env
bash scripts/run_pipeline.sh config/env/16gb.env
```

Thay `16gb.env` bang `8gb.env`, `32gb.env` hoac `64gb.env` khi can. File profile
duoc nap sau `.env` va chi override cac gia tri lien quan. Co the
dung Compose truc tiep cho lenh quan tri:

```bash
docker compose --env-file .env --env-file config/env/16gb.env ps
```

Neu may chi co 12 vCPU nhung co nhieu RAM hon, khong dung nguyen CPU override cua
profile lon. Hay giu cac bien `*_CPUS` cua `.env`, chi sao chep cac override RAM.

`RADIUS_UDP_RECEIVE_BUFFER_BYTES` la muc ung dung yeu cau. Linux phai co
`net.core.rmem_max` du lon; gia tri log `receive_buffer_actual` moi la gia tri thuc.

Sau khi doi `KAFKA_TOPIC_PARTITIONS`, phai chay quy trinh cap nhat topic; Compose
khong tu giam partition va consumer chi can scale den so partition huu dung.

Muc throughput la admission ceiling latency-first, khong phai throughput toi da
de queue day. Ceiling 8 GiB hien la baseline bao thu 800/s; cac ceiling profile
lon hon chi la diem khoi dau benchmark, chua phai cam ket tren phan cung dich.
Khong tang ceiling neu p95 <100ms chua on dinh.
Các ceiling 16/32/64 GiB là target benchmark, chưa phải cam kết SLO. Chỉ công
nhận bất kỳ profile nào trên phần cứng đích khi soak test cho thấy
event-level E2E p95 < 100ms, loss=0 va Kafka lag khong tang lien tuc.
