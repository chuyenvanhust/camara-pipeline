# `pipeline/modules/ip_msisdn/` — Module 1: Ánh xạ IP ↔ MSISDN

Consumer group: `cg-ip-msisdn`. Duy trì ánh xạ **Framed-IP-Address ↔ MSISDN** hiện đang hoạt
động, phục vụ tra cứu ngược "IP này là của thuê bao nào" — nền tảng cho API
**Number Verification** của CAMARA (xác thực số điện thoại dựa trên IP nguồn kết nối).

Đây là module **duy nhất trong 3 module không đụng tới Postgres** — toàn bộ state chỉ nằm
trong Redis, coi RADIUS accounting session là dữ liệu tồn tại ngắn hạn, không cần lịch sử lâu
dài như đổi SIM/đổi máy.

| File | Vai trò |
|---|---|
| `consumer.py` | `IPMsisdnConsumer` — nhận diện Start/Interim-Update/Stop/Accounting-Off, gọi `redis_store` |
| `redis_store.py` | `IPMappingStore` — thao tác Redis trực tiếp (single-record, dùng cho `process_message`) |

## Cấu trúc dữ liệu Redis

- `ip-ggsn:<framed_ip>` → JSON `{"msisdn": "...", "timestamp": "..."}`, TTL 24h
  (`SESSION_TTL_SECONDS`), refresh TTL mỗi lần có Interim-Update.
- `ggsn-ips:<nas_identifier>` → Redis SET chứa tất cả `framed_ip` đang active trên 1 GGSN/NAS
  cụ thể — key phụ để hỗ trợ xoá hàng loạt khi có sự kiện Accounting-Off (GGSN restart, mất
  toàn bộ session đang track).

## Logic xử lý theo loại sự kiện RADIUS

| `acct_status_type` | Hành động |
|---|---|
| `Start` / `Interim-Update` | Upsert `ip-ggsn:<ip>` với MSISDN + timestamp mới, thêm `ip` vào set `ggsn-ips:<nas_id>`, refresh TTL cả hai key |
| `Stop` | Đọc giá trị hiện tại của `ip-ggsn:<ip>`, **chỉ xoá nếu MSISDN trong Redis khớp với MSISDN trong message Stop** — tránh xoá nhầm session mới nếu IP đã được cấp lại cho thuê bao khác trước khi Stop cũ tới nơi (out-of-order delivery) |
| `Accounting-Off` | GGSN báo mất toàn bộ session — đọc set `ggsn-ips:<nas_id>`, xoá toàn bộ `ip-ggsn:*` tương ứng, xoá luôn set |
| Khác / thiếu field bắt buộc | Tăng counter `ignored`, không làm gì thêm |

## `process_batch()` — batch hoá thao tác Redis

Không xử lý từng message riêng lẻ (`process_message` chỉ giữ để tương thích ngược) mà gom cả
batch rồi thực hiện theo 3 nhóm, mỗi nhóm dùng Redis pipeline (`transaction=False`, vì các
lệnh trong 1 batch độc lập nhau, không cần atomic chéo):

1. **Upsert** (Start/Interim-Update): gom hết vào 1 `pipeline`, mỗi upsert là 2-3 lệnh
   (`SET` + `SADD` + `EXPIRE`), chỉ 1 round-trip network cho cả batch.
2. **Delete** (Stop): trước tiên `MGET` toàn bộ key cần kiểm tra ownership trong 1 round-trip,
   sau đó mới `pipeline` các lệnh xoá cho những entry thực sự khớp MSISDN.
3. **Accounting-Off**: xử lý tuần tự từng NAS (sự kiện hiếm, số lượng nhỏ trong 1 batch, và
   bản thân nó đã là thao tác gộp xoá nhiều key).

Cách này giảm số round-trip Redis từ O(số message) xuống O(số nhóm thao tác) — quan trọng vì
Redis là single-threaded, latency network dồn lại nhanh nếu gọi tuần tự từng lệnh.

## Khi đọc/sửa code này cần lưu ý

- Field tên message hỗ trợ cả 2 kiểu (RADIUS attribute gốc và tên đã chuẩn hoá):
  `Framed_IP_Address`/`framed_ip`, `Calling-StationId`/`Calling_Station_Id`/`msisdn`,
  `NAS-Identifier`/`NAS_Identifier`/`nas_identifier` — do dữ liệu simulator và dữ liệu CSV
  thật có thể khác convention đặt tên.
- Không có audit log hay history table cho module này — nếu sau này cần truy vết lịch sử IP
<<<<<<< ours
  (ví dụ phục vụ điều tra), cần bổ sung bảng Postgres tương tự `device_swap_history`.
=======
  (ví dụ phục vụ điều tra), cần bổ sung bảng Postgres tương tự `device_swap_history`.
>>>>>>> theirs
