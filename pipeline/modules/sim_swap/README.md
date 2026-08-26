# `pipeline/modules/sim_swap/` — Module 3: Phát hiện đổi SIM (IMSI)

Consumer group: `cg-sim-swap`. Theo dõi IMSI hiện tại gắn với mỗi MSISDN; khi IMSI thay đổi,
ghi nhận sự kiện **SIM Swap**, phục vụ trực tiếp API **SIM Swap** của CAMARA — use case phổ
biến nhất trong bộ CAMARA Network API (chống gian lận SIM swap khi thực hiện giao dịch nhạy
cảm như OTP ngân hàng).

Cấu trúc gần như song song hoàn toàn với `device_swap/` (cùng pattern, khác entity theo dõi:
IMSI thay vì IMEI) — nếu đã hiểu `device_swap`, phần khác biệt đáng chú ý duy nhất nằm ở cache
value và payload notification, nêu ở dưới.

| File | Vai trò |
|---|---|
| `consumer.py` | `SimSwapConsumer` — logic phát hiện swap, ghi state/history/audit/notification |
| `notifier.py` | Stub deprecated — xem giải thích outbox pattern ở `../shared/README.md` |

## Luồng xử lý (giống pattern `device_swap`, entity là IMSI)

1. Lấy `msisdn` + `imsi` mới. Thiếu 1 trong 2 → bỏ qua.
2. Tra IMSI hiện tại: Redis (`sim:<msisdn>`) trước, `msisdn_sim` (Postgres) sau nếu cache miss.
3. Lần đầu gặp MSISDN → chỉ upsert state, không tính swap.
4. IMSI mới == cũ → bỏ qua.
5. Khác → validate `event_time` (xem `../device_swap/README.md`, cùng cơ chế `_parse_event_time`
   trả `None` thay vì fallback `now()` khi parse lỗi — áp dụng y hệt ở đây).
6. Ghi nhận SIM Swap: cập nhật `msisdn_sim`, insert `sim_swap_history`, insert `audit_log`
   (`event_type='SIM_SWAP'`), insert `notification_log` (status `PENDING`) cho mỗi subscription
   active loại `SIM_SWAP`.

## Khác biệt so với `device_swap`

- **Cache value có thêm field `last_time_sim_change`**: `sim:<msisdn>` lưu
  `{"imsi_current": ..., "last_time_sim_change": ...}` thay vì chỉ IMEI hiện tại — vì API
  CAMARA SIM Swap chuẩn có field `lastSimChangeDate`/tương đương cần trả về thời điểm đổi SIM
  gần nhất, không chỉ trạng thái hiện tại.
- **Payload notification dùng key `PascalCase`** (`"MSISDN"`, `"LastTimeSIMChange"`) thay vì
  `snake_case` như `device_swap` (`"msisdn"`, `"imei_old"`, `"imei_new"`) — phản ánh đúng
  format response mà CAMARA SIM Swap API expose ra ngoài (subscriber Open Gateway mong đợi
  đúng schema này).

## `process_batch()` — cùng chiến lược tối ưu với `device_swap`

1. Lọc message hợp lệ.
2. `MGET` batch `sim:<msisdn>` — 1 round-trip Redis.
3. `batch_get_current_imsi()` cho cache-miss — 1 round-trip Postgres.
4. Phân loại theo state in-memory, cập nhật state ngay trong vòng lặp để xử lý đúng trường
   hợp nhiều swap liên tiếp trong cùng 1 batch cho cùng 1 MSISDN.
5. Gọi `db.commit_sim_swap_batch()` — atomic transaction cho toàn bộ upsert + history + audit
   + notification của cả batch (chi tiết ở [`../shared/README.md`](../shared/README.md)).
6. Cập nhật Redis (`MSET`) chỉ sau khi Postgres commit thành công.

## Bảng Postgres liên quan

- `msisdn_sim` — state hiện tại (1 dòng / MSISDN).
- `sim_swap_history` — lịch sử mọi lần đổi SIM, insert-only qua `copy_records_to_table`.
- `audit_log`, `notification_log` — dùng chung schema với `device_swap` (phân biệt qua cột
<<<<<<< ours
  `event_type`).
=======
  `event_type`).
>>>>>>> theirs
