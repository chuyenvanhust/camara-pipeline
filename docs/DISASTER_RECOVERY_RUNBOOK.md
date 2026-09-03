# QUY TRÌNH PHỤC HỒI THẢM HỌA (DISASTER RECOVERY RUNBOOK) & BẢO TRÌ NÂNG CẤP

> **Tài liệu Vận hành Chuẩn (SOP)**: Quy trình phục hồi thảm họa, mục tiêu RPO/RTO, sao lưu dữ liệu và khôi phục sự cố cho CAMARA Data Pipeline.

---

## 1. Mục Tiêu Phục Hồi (Target Service Level Objectives)

> ⚠️ **Lưu ý Kiến trúc**: Các thông số RPO/RTO dưới đây đại diện cho **Mục tiêu Thiết kế (Target SLAs)** khi hệ thống được triển khai trên cụm máy chủ phân tán (Kubernetes HA / Multi-node). Môi trường Docker Compose đơn host hiện tại không tự động bảo đảm dự phòng sự cố cấp hạ tầng phần cứng.

- **RPO (Target Recovery Point Objective)**: **< 1 Phút** (Yêu cầu bật WAL
  Archiving/PITR trên PostgreSQL và phải có hợp đồng replay từ capture server.
  Nếu Kafka được chọn làm ranh giới durability thay cho capture, raw producer
  phải đổi từ cấu hình hiện hành `acks=1` sang `acks=all`, RF=3,
  `min.insync.replicas=2` và benchmark lại p95).
- **RTO (Target Recovery Time Objective)**: **< 15 Phút** (Yêu cầu Kubernetes StatefulSet / Multi-node Auto-Failover Orchestration).

---

## 2. Kế Hoạch Sao Lưu (Backup Strategy)

### 2.1. Sao Lưu Cơ Sở Dữ Liệu PostgreSQL
Thực thi định kỳ qua Cron job hàng ngày vào lúc 02:00 AM:

```bash
# Đặt quyền thực thi cho script
chmod +x scripts/backup_postgres.sh

# Chạy backup thủ công hoặc thêm vào crontab
./scripts/backup_postgres.sh
```

### 2.2. Điểm Khôi Phục Theo Thời Gian (PITR - Point-In-Time Recovery)
Đối với môi trường Production chính thức, bật WAL Archiving trong PostgreSQL configuration (`postgresql.conf`):
```ini
wal_level = replica
archive_mode = on
archive_command = 'test ! -f /var/lib/postgresql/wal_archive/%f && cp %p /var/lib/postgresql/wal_archive/%f'
```

---

## 3. Quy Trình Khôi Phục Dữ Liệu (Restore Rehearsal)

Khi xảy ra sự cố mất mát dữ liệu hoặc thảm họa hạ tầng:

```bash
# 1. Tạm dừng các dịch vụ ghi dữ liệu (Pipeline Consumers, Ingestion & Notification Dispatcher)
docker compose stop pipeline-ip-msisdn pipeline-device-swap pipeline-sim-swap radius-ingestion notification-dispatcher

# 2. Đảm bảo container PostgreSQL đang hoạt động
docker compose up -d postgres

# 3. Thực thi script restore từ bản backup gần nhất
./scripts/restore_postgres.sh /var/backups/camara_postgres/camara_db_YYYYMMDD_HHMMSS.sql.gz

# 4. Khởi động lại toàn bộ pipeline services
docker compose up -d
```

---

## 4. Xử Lý Sự Cố Thường Gặp (Incident Playbook)

### Sự cố 1: 1 Broker Kafka bị hỏng / ngắt kết nối
- **Hiện tượng**: Log Kafka báo `BrokerDisconnected`, lag tăng nhẹ.
- **Xử lý**: Hệ thống tiếp tục chạy nhờ `min.insync.replicas=2`. Khởi động lại container broker lỗi:
  ```bash
  docker compose restart kafka-2
  ```

### Sự cố 2: Kafka Consumer Lag dồn ứ (Lag High Alert)
- **Hiện tượng**: Alert `HighKafkaLag` kích hoạt trên Prometheus.
- **Xử lý**: 
  1. Kiểm tra tài nguyên CPU/RAM của PostgreSQL & Redis: `docker stats pipeline-ip-msisdn pipeline-device-swap pipeline-sim-swap`.
  2. Giữ `IP_MSISDN_CONSUMERS_PER_GROUP=1`; scale số process bằng
     `PIPELINE_IP_REPLICAS` hoặc `docker compose up -d --scale
     pipeline-ip-msisdn=N`. Không tăng đồng loạt ba group nếu bottleneck chỉ nằm
     ở IP-MSISDN, và không scale vượt số Kafka partition hữu ích.
  3. Restart service có lag cao nhất (ví dụ `pipeline-ip-msisdn`):
     ```bash
     docker compose restart pipeline-ip-msisdn
     ```
  4. Nếu lag vẫn tăng, scale hết cả 3 worker services:
     ```bash
     docker compose up -d --scale pipeline-ip-msisdn=2 --scale pipeline-device-swap=2 --scale pipeline-sim-swap=2
     ```

### Sự cố 3: Đăng ký Webhook bị khóa do lỗi SSRF
- **Hiện tượng**: Log báo `SSRF Blocked: Forbidden private/internal IP address`.
- **Xử lý**: Yêu cầu phía đối tác cung cấp Webhook URL dùng tên miền public và giao thức HTTPS theo quy chuẩn bảo mật CAMARA.

### Sự cố 4: Một worker pipeline bị crash / exit
- **Hiện tượng**: Container `pipeline-ip-msisdn`, `pipeline-device-swap` hoặc `pipeline-sim-swap` dừng đột ngột. Kafka lag chỉ tăng cho group tương ứng.
- **Xử lý**: Các worker khác **không bị ảnh hưởng** (process isolation). Restart dịch vụ bị lỗi:
  ```bash
  # Ví dụ restart dịch vụ device-swap
  docker compose restart pipeline-device-swap

  # Kiểm tra log để xác định nguyên nhân
  docker compose logs --tail=100 pipeline-device-swap
  ```
- **Phòng ngừa**: Đặt `restart: unless-stopped` trong `docker-compose.yml` để Docker tự động khởi động lại.

### Sự cố 5: Mất bản mirror tại ingestion
- **Hiện tượng**: `radius_ingestion_queue_dropped_total` hoặc
  `radius_ingestion_publish_failed_total` tăng; `udp_in` cao hơn
  `kafka_persisted` trong khi nguồn vẫn phát.
- **Giới hạn trách nhiệm**: Ingestion là receiver một chiều, không gửi
  `Accounting-Response` và không yêu cầu retry. Queue RAM không phải durable log.
- **Xử lý**:
  1. Dừng hoặc giảm tốc độ mirror tại capture server ngoài repo.
  2. Khắc phục Kafka/CPU/queue pressure và xác nhận hai counter không tăng thêm.
  3. Replay khoảng thời gian bị ảnh hưởng từ durable capture source vào UDP/1813
     hoặc qua đường CSV ingest.
  4. Theo dõi `kafka_persisted`, consumer lag và `data_loss` cho tới khi hệ thống
     bắt kịp.
