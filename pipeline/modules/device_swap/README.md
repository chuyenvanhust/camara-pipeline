# `pipeline/modules/device_swap/` — Module 2: Phát Hiện Đổi Thiết Bị (IMEI Tracking)

Consumer Group: `cg-device-swap`. Module này theo dõi mã định danh thiết bị phần cứng di động (**IMEI / IMEISV**) gắn với mỗi số thuê bao (**MSISDN**). Khi phát hiện thuê bao chuyển thẻ SIM sang một thiết bị khác (IMEI thay đổi so với bản ghi gần nhất), hệ thống ghi nhận sự kiện **Device Swap**, phục vụ trực tiếp cho chuẩn API **CAMARA Device Swap** (chống gian lận tài chính, xác thực bảo mật 2 lớp).

---

## 1. Sơ đồ luồng phát hiện Đổi Thiết Bị (Device Swap Flow)

```mermaid
flowchart TD
    MSG["Kafka Record (radius.accounting.raw)"] --> PARSE["Parse & Validate:<br/>- msisdn (E.164)<br/>- imei (3GPP VSA 20)<br/>- event_timestamp (UTC)"]

    PARSE --> CACHE_LOOKUP["Tra cứu Trạng Thái Hiện Tại (State Lookup):<br/>1. Redis: MGET device:MSISDN<br/>2. Cache Miss: PostgreSQL batch_get_device_state()"]

    CACHE_LOOKUP --> DECISION{So sánh IMEI Mới vs IMEI Cũ}

    DECISION -->|Chưa có dữ liệu cũ<br/>(Lần đầu thấy MSISDN)| INIT["Khởi tạo trạng thái ban đầu:<br/>- Upsert msisdn_device<br/>- KHÔNG tính là Swap<br/>- KHÔNG ghi History/Outbox"]

    DECISION -->|IMEI Mới == IMEI Cũ| SAME["Không đổi máy:<br/>- Bỏ qua (ignored counter +1)"]

    DECISION -->|Bản ghi cũ hơn<br/>(Out-of-Order / Duplicate)| OLD["Sự kiện đến trễ / trùng event_id:<br/>- Bỏ qua (ignored counter +1)"]

    DECISION -->|IMEI Mới != IMEI Cũ<br/>& Phiên bản mới hơn| SWAP_EVENT["PHÁT HIỆN SỰ KIỆN DEVICE SWAP:<br/>1. Cập nhật In-memory State<br/>2. Chuẩn bị Atomic DB Transaction"]

    SWAP_EVENT --> ATOMIC_TX["Thực Thi Giao Dịch PostgreSQL (Atomic Transaction):<br/>- Upsert msisdn_device (State mới)<br/>- Insert device_swap_history (Lịch sử)<br/>- Insert audit_log (Kiểm toán)<br/>- Insert notification_log (Outbox status=PENDING)"]

    INIT --> ATOMIC_TX
    ATOMIC_TX --> REDIS_SYNC["Cập nhật Redis Cache:<br/>MSET device:MSISDN"]
    SAME --> COMMIT_OFFSET["Commit Kafka Offset"]
    OLD --> COMMIT_OFFSET
    REDIS_SYNC --> COMMIT_OFFSET
```

---

## 2. Chiến lược Tối ưu hóa Batching (`process_batch`)

Để đạt thông lượng hàng chục nghìn message/giây, module thực hiện xử lý batch theo quy trình 5 bước nghiêm ngặt:

1. **Trích xuất & Gom nhóm MSISDN**:
   - Lọc các bản ghi hợp lệ trong batch, gom tập hợp các `msisdn` duy nhất.
2. **Đọc State Đa Tầng 2 Bước (Two-Tier State Read)**:
   - **Bước 1**: Đọc song song toàn bộ danh sách MSISDN từ Redis bằng 1 lệnh `MGET` duy nhất (`device:<msisdn>`).
   - **Bước 2**: Với các MSISDN bị cache miss, thực hiện đúng 1 truy vấn PostgreSQL:
     ```sql
     SELECT msisdn, imei_current AS value, last_event_at, last_event_id,
            last_source_partition, last_source_offset
     FROM msisdn_device
     WHERE msisdn = ANY($1::text[])
     ```
3. **Phát Hiện & Cập Nhật State Nội Bộ (In-batch State Mutation)**:
   - Khi duyệt qua từng bản ghi trong batch, state in-memory của MSISDN được cập nhật ngay lập tức.
   - Điều này đảm bảo tính đúng đắn khi một thuê bao đổi máy nhiều lần liên tiếp trong cùng một batch (bản ghi thứ 2 sẽ so sánh với kết quả của bản ghi thứ 1).
   - Sử dụng `dict` theo `msisdn` để giữ lại bản ghi mới nhất cho mỗi thuê bao, tránh xung đột `CardinalityViolationError` khi thực thi câu lệnh SQL `ON CONFLICT DO UPDATE`.
4. **Giao dịch Cơ sở dữ liệu Nguyên tử (`db.persist_device_batch`)**:
   - Mở 1 `connection.transaction()` duy nhất để thực thi đồng thời cả 4 thao tác ghi.
5. **Đồng bộ Read Cache (`redis.mset`)**:
   - Chỉ cập nhật cache Redis **sau khi** PostgreSQL commit thành công, đảm bảo Redis luôn là hình chiếu trung thực của dữ liệu nguồn.

---

## 3. Cấu trúc Bảng Cơ Sở Dữ Liệu

### Bảng `msisdn_device` (Trạng thái hiện tại)
```sql
CREATE TABLE msisdn_device (
    msisdn VARCHAR(16) PRIMARY KEY,
    imei_current VARCHAR(32) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_event_at TIMESTAMPTZ NOT NULL,
    last_event_id VARCHAR(128) NOT NULL,
    last_source_partition INT NOT NULL,
    last_source_offset BIGINT NOT NULL
);
```

### Bảng `device_swap_history` (Lịch sử đổi thiết bị)
```sql
CREATE TABLE device_swap_history (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(128) NOT NULL UNIQUE,
    source_topic VARCHAR(128) NOT NULL,
    source_partition INT NOT NULL,
    source_offset BIGINT NOT NULL,
    msisdn VARCHAR(16) NOT NULL,
    imei_old VARCHAR(32),
    imei_new VARCHAR(32) NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

---

## 4. Định dạng Payload Thông báo Outbox (CAMARA Schema)

Khi phát hiện sự kiện Device Swap, hệ thống sinh payload JSON ghi vào `notification_log`:

```json
{
  "event_id": "radius:9f8a7b6c5d4e3f2a1b0c...",
  "event_type": "DEVICE_SWAP",
  "msisdn": "+84981234567",
  "details": {
    "imei_old": "860123045678901",
    "imei_new": "860987065432109",
    "event_time": "2026-08-27T08:31:00.000000+00:00"
  }
}
```
