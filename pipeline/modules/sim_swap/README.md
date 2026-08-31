# `pipeline/modules/sim_swap/` — Module 3: Phát Hiện Đổi SIM (IMSI Tracking)

Consumer Group: `cg-sim-swap`. Module này theo dõi mã định danh thuê bao quốc tế (**IMSI**) gắn với mỗi số điện thoại (**MSISDN**). Khi phát hiện thẻ SIM vật lý hoặc eSIM bị thay đổi (IMSI thay đổi so với bản ghi gần nhất), hệ thống ghi nhận sự kiện **SIM Swap**, phục vụ trực tiếp cho chuẩn API **CAMARA SIM Swap** — giải pháp bảo mật cốt lõi ngăn chặn tấn công chiếm đoạt OTP ngân hàng và gian lận định danh số.

---

## 1. Sơ đồ luồng phát hiện Đổi SIM (SIM Swap Flow)

```mermaid
flowchart TD
    MSG["Kafka Record (radius.accounting.raw)"] --> PARSE["Parse & Validate:<br/>- msisdn (E.164)<br/>- imsi (3GPP VSA 1)<br/>- event_timestamp (UTC)"]

    PARSE --> CACHE_LOOKUP["Tra cứu Trạng Thái Hiện Tại (State Lookup):<br/>1. Redis: MGET sim:MSISDN<br/>2. Cache Miss: PostgreSQL batch_get_sim_state()"]

    CACHE_LOOKUP --> DECISION{So sánh IMSI Mới vs IMSI Cũ}

    DECISION -->|Chưa có dữ liệu cũ<br/>(Lần đầu thấy MSISDN)| INIT["Khởi tạo trạng thái ban đầu:<br/>- Redis business-ready<br/>- Queue PostgreSQL checkpoint<br/>- KHÔNG tính là Swap"]

    DECISION -->|IMSI Mới == IMSI Cũ| SAME["Không đổi SIM:<br/>- Cập nhật Redis watermark<br/>- Queue PostgreSQL checkpoint"]

    DECISION -->|Bản ghi cũ hơn<br/>(Out-of-Order / Duplicate)| OLD["Sự kiện đến trễ / trùng event_id:<br/>- Bỏ qua (ignored counter +1)"]

    DECISION -->|IMSI Mới != IMSI Cũ<br/>& Phiên bản mới hơn| SWAP_EVENT["PHÁT HIỆN SỰ KIỆN SIM SWAP:<br/>1. Cập nhật In-memory State<br/>2. Chuẩn bị Atomic DB Transaction"]

    SWAP_EVENT --> ATOMIC_TX["Thực Thi Giao Dịch PostgreSQL (Atomic Transaction):<br/>- Upsert msisdn_sim (State mới)<br/>- Insert sim_swap_history (Lịch sử)<br/>- Insert audit_log (Kiểm toán)<br/>- Insert notification_log (Outbox status=PENDING)"]

    INIT --> CHECKPOINT["Checkpoint coordinator:<br/>coalesce theo MSISDN<br/>bulk UPSERT nền"]
    SAME --> CHECKPOINT
    CHECKPOINT --> COMMIT_OFFSET["Chỉ commit offset sau checkpoint durable"]
    ATOMIC_TX --> REDIS_SYNC["Cập nhật Redis Cache:<br/>MSET sim:MSISDN<br/>(Lưu kèm last_time_sim_change)"]
    OLD --> COMMIT_OFFSET
    REDIS_SYNC --> COMMIT_OFFSET
```

---

## 2. Đặc điểm kỹ thuật & Điểm khác biệt với Device Swap

Mặc dù có mô hình kiến trúc tương đồng với `device_swap`, module `sim_swap` có các yêu cầu kỹ thuật đặc thù để tuân thủ chuẩn CAMARA:

1. **Lưu trữ `last_time_sim_change` trong Cache**:
   - Cache Redis `sim:<msisdn>` lưu trữ cấu trúc JSON mở rộng:
     ```json
     {
       "imsi_current": "452041234567890",
       "last_event_at": "2026-08-27T08:32:00+00:00",
       "last_event_id": "radius:e5f6a1b2...",
       "last_source_partition": 1,
       "last_source_offset": 89450
     }
     ```
   - Trường thời gian này phục vụ trực tiếp cho endpoint `GET /sim-swap/v0/retrieve-date` của CAMARA API trả về ngày đổi SIM gần nhất trong vòng $N$ giờ.

2. **Cấu trúc Payload Notification (CAMARA Schema)**:
   - Khi phát hiện sự kiện SIM Swap, payload JSON được định dạng theo đúng quy chuẩn CAMARA Network API:
     ```json
     {
       "event_id": "radius:e5f6a1b2c3d4...",
       "event_type": "SIM_SWAP",
       "msisdn": "+84981234567",
       "details": {
         "imsi_old": "452040111222333",
         "imsi_new": "452041234567890",
         "last_time_sim_change": "2026-08-27T08:32:00.000000+00:00"
       }
     }
     ```

---

## 3. Fast path và durability checkpoint

- Record trùng hoặc cũ không ghi Redis/PostgreSQL.
- Record mới nhưng IMSI không đổi cập nhật Redis ngay; state watermark được
  coalesce theo MSISDN trong `StateCheckpointCoordinator`.
- Coordinator bulk UPSERT PostgreSQL theo
  `SWAP_CHECKPOINT_INTERVAL_MS`/`SWAP_CHECKPOINT_MAX_RECORDS`. Kafka offset chỉ
  được commit sau khi checkpoint future thành công; crash trước checkpoint sẽ
  replay an toàn.
- Swap thật vẫn dùng transaction đồng bộ gồm current state, history, audit và
  outbox, nên checkpoint nền không làm yếu tính nguyên tử của sự kiện CAMARA.

## 4. Cấu trúc Bảng Cơ Sở Dữ Liệu

### Bảng `msisdn_sim` (Trạng thái hiện tại)
```sql
CREATE TABLE msisdn_sim (
    msisdn VARCHAR(16) PRIMARY KEY,
    imsi_current VARCHAR(32) NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_event_at TIMESTAMPTZ NOT NULL,
    last_event_id VARCHAR(128) NOT NULL,
    last_source_partition INT NOT NULL,
    last_source_offset BIGINT NOT NULL
);
```

### Bảng `sim_swap_history` (Lịch sử đổi SIM)
```sql
CREATE TABLE sim_swap_history (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(128) NOT NULL UNIQUE,
    source_topic VARCHAR(128) NOT NULL,
    source_partition INT NOT NULL,
    source_offset BIGINT NOT NULL,
    msisdn VARCHAR(16) NOT NULL,
    imsi_old VARCHAR(32),
    imsi_new VARCHAR(32) NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

---

## 5. Xử lý Trùng Lặp & Thứ Tự trong Batch

- **Khử trùng lặp theo MSISDN**: Trong một batch lớn, nếu một số thuê bao đổi SIM nhiều lần (ví dụ qua lại giữa các mạng), danh sách `states_by_msisdn` (kiểu `dict`) chỉ giữ lại bản ghi cuối cùng của batch để gửi lệnh `UPSERT` xuống PostgreSQL, loại bỏ hoàn toàn lỗi tranh chấp `ON CONFLICT DO UPDATE` khi cập nhật cùng 1 khóa chính trong 1 câu truy vấn SQL duy nhất.
- **Tính toàn vẹn lịch sử**: Tất cả các bước chuyển SIM trung gian vẫn được lưu đầy đủ vào bảng `sim_swap_history` để phục vụ công tác điều tra viễn thông và kiểm toán.
