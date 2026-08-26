# `pipeline/modules/device_swap/` — Module 2: Phát hiện đổi thiết bị (IMEI)

Consumer group: `cg-device-swap`. Theo dõi IMEI hiện tại gắn với mỗi MSISDN; khi IMEI thay
đổi so với lần gần nhất, ghi nhận đây là sự kiện **Device Swap**, phục vụ API
**Device Swap** của CAMARA (callback cho subscriber khi thuê bao đổi máy — hữu ích cho các
use case chống gian lận, xác thực 2 lớp).

| File | Vai trò |
|---|---|
| `consumer.py` | `DeviceSwapConsumer` — logic phát hiện swap, ghi state/history/audit/notification |
| `notifier.py` | Stub deprecated — xem giải thích outbox pattern ở `../shared/README.md` |

## Luồng xử lý 1 message (khái niệm, xem thực thi tối ưu ở batch bên dưới)

1. Lấy `msisdn` + `imei` mới từ message. Thiếu 1 trong 2 → bỏ qua (`ignored`).
2. Tra IMEI hiện tại: **Redis trước** (`device:<msisdn>`), **Postgres sau** nếu cache miss
   (`msisdn_device` table).
3. Nếu chưa từng thấy MSISDN này (không có ở cả Redis lẫn Postgres) → coi là lần đầu ghi
   nhận thiết bị, upsert state, **không** tính là swap, **không** ghi history/audit.
4. Nếu IMEI mới == IMEI cũ → không có gì thay đổi, bỏ qua.
5. Nếu khác → validate `event_time` từ message (xem mục timestamp bên dưới). Nếu hợp lệ:
   ghi nhận Device Swap — cập nhật state, insert `device_swap_history`, insert `audit_log`
   (`event_type='DEVICE_SWAP'`), và với mỗi subscription đang active của MSISDN này cho event
   type `DEVICE_SWAP`, insert `notification_log` (status `PENDING`).

## Xử lý timestamp — vì sao không fallback về `now()`

`_parse_event_time()` đọc `timestamp`/`event_timestamp` từ message, parse ISO format. **Nếu
parse thất bại, trả về `None` thay vì âm thầm dùng `datetime.now()`.** Caller (cả
`process_message` và `process_batch`) khi nhận `None` sẽ tăng counter `errors`, log warning,
và **bỏ qua message đó hoàn toàn** — không ghi swap event với timestamp sai.

Lý do: nếu fallback về `now()`, một message bị trễ hàng giờ do retry/network delay sẽ bị ghi
nhận với timestamp xử lý thay vì timestamp thật sự xảy ra ở GGSN, làm sai lệch toàn bộ dữ
liệu lịch sử swap (order sai, khoảng cách giữa 2 lần swap sai) — hậu quả xa hơn ảnh hưởng cả
tới các API dùng field như `LastTimeSIMChange`/tương đương cho device swap.

## `process_batch()` — chiến lược tối ưu round-trip DB/Redis

Vì phải tra cứu state hiện tại cho **nhiều MSISDN cùng lúc** trước khi biết ai swap ai không,
batch được xử lý theo từng giai đoạn thay vì lặp tuần tự gọi DB cho từng message:

1. Lọc message hợp lệ (có đủ `msisdn` + `imei`).
2. `MGET` toàn bộ `device:<msisdn>` unique trong batch — 1 round-trip Redis.
3. Với các MSISDN cache-miss, `batch_get_current_imei()` — 1 round-trip Postgres
   (`WHERE msisdn = ANY($1::text[])`) thay vì N query riêng.
4. Duyệt từng message theo state đã có trong bộ nhớ (`redis_state` dict, được cập nhật ngay
   sau mỗi message xử lý trong batch để message thứ 2 của cùng 1 MSISDN trong cùng batch thấy
   đúng state mới nhất — quan trọng nếu 1 thuê bao đổi máy 2 lần liên tiếp trong cùng 1 batch).
5. Gom toàn bộ thao tác ghi (`init_records`, `swap_upserts`, `swap_records`, `swap_audit`,
   `notification_records`) vào list, rồi gọi **1 lần duy nhất**
   `db.commit_device_swap_batch()` — atomic transaction, xem chi tiết ở
   [`../shared/README.md`](../shared/README.md).
6. Redis chỉ được cập nhật (`MSET`) **sau khi** Postgres commit thành công — Redis không nằm
   trong transaction Postgres (khác engine), nên coi Redis là projection có thể rebuild từ
   Postgres, cập nhật sau để tránh trạng thái Redis "nói đã swap" nhưng Postgres rollback.

## Bảng Postgres liên quan

- `msisdn_device` — state hiện tại (1 dòng / MSISDN), upsert qua `ON CONFLICT`.
- `device_swap_history` — lịch sử mọi lần swap, insert-only, dùng `copy_records_to_table`
  cho hiệu năng cao khi batch lớn.
- `audit_log` — audit chung toàn hệ thống, chỉ ghi khi thực sự có swap (không ghi cho lần đầu
  gặp MSISDN).
- `notification_log` — outbox, xem `../shared/README.md`.
