# QUY TRÌNH PHỤC HỒI THẢM HỌA (DISASTER RECOVERY RUNBOOK) & BẢO TRÌ NÂNG CẤP

> **Tài liệu Vận hành Chuẩn (SOP)**: Quy trình phục hồi thảm họa, mục tiêu RPO/RTO, sao lưu dữ liệu và khôi phục sự cố cho CAMARA Data Pipeline.

---

## 1. Mục Tiêu Phục Hồi (Target Service Level Objectives)

> ⚠️ **Lưu ý Kiến trúc**: Các thông số RPO/RTO dưới đây đại diện cho **Mục tiêu Thiết kế (Target SLAs)** khi hệ thống được triển khai trên cụm máy chủ phân tán (Kubernetes HA / Multi-node). Môi trường Docker Compose đơn host hiện tại không tự động bảo đảm dự phòng sự cố cấp hạ tầng phần cứng.

- **RPO (Target Recovery Point Objective)**: **< 1 Phút** (Yêu cầu bật WAL Archiving / PITR trên PostgreSQL & 3-broker Kafka cluster `acks=all`, `min.insync.replicas=2`).
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
docker compose stop pipeline radius-ingestion notification-dispatcher

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
  1. Kiểm tra tài nguyên CPU/RAM của PostgreSQL & Redis.
  2. Tăng số lượng Consumer Workers per group: `CONSUMERS_PER_GROUP=8`.
  3. Khởi chạy thêm replica container:
     ```bash
     docker compose up -d --scale pipeline=2
     ```

### Sự cố 3: Đăng ký Webhook bị khóa do lỗi SSRF
- **Hiện tượng**: Log báo `SSRF Blocked: Forbidden private/internal IP address`.
- **Xử lý**: Yêu cầu phía đối tác cung cấp Webhook URL dùng tên miền public và giao thức HTTPS theo quy chuẩn bảo mật CAMARA.
