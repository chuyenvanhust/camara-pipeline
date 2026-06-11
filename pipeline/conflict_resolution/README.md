# pipeline/conflict_resolution/

Stage S4 — phân loại và xử lý 3 loại conflict trong dữ liệu RADIUS.
Conflict loại C (MSISDN remap) được emit thành `swap_event` — input cho CAMARA API.

## Files

| File | Vai trò |
|------|---------|
| `resolver.py` | Spark job: nhận `radius.dedup`, phân loại conflict A/B/C, route output |
| `swap_detector.py` | Xử lý riêng conflict C: gọi HLR/HSS mock xác nhận, emit `swap_event` |

## 3 loại conflict

### Loại A — Session Inconsistency
Cùng `acct_session_id` nhưng `imsi` hoặc `msisdn` thay đổi giữa Start và Stop/Interim.

```
Xử lý: giữ Start record, đánh dấu Stop/Interim là CONFLICT_A
Ghi:   conflict_log với conflict_type='A'
```

### Loại B — Double Active Session
Cùng `imsi` có 2 Start chưa có Stop tương ứng tại cùng thời điểm.

```
Xử lý: giữ session có event_timestamp nhỏ hơn, đánh dấu session sau là CONFLICT_B
Ghi:   conflict_log với conflict_type='B'
```

### Loại C — MSISDN↔IMSI Remap (SIM Swap signal)
Cùng `msisdn` mapping sang `imsi` mới khác `imsi` cũ.

```
Xử lý: giữ cả 2 record (đây là business event hợp lệ, không phải lỗi dữ liệu)
Gọi:   HLR/HSS Mock GET /subscribers/{msisdn}/imsi-history để xác nhận + lấy timestamp chính xác
Emit:  swap_event vào Kafka với swap_type='SIM_SWAP'
Ghi:   conflict_log với conflict_type='C', swap_event table
```

## swap_detector.py — luồng xử lý conflict C

```
Phát hiện msisdn → imsi mới
    │
    ▼ GET /subscribers/{msisdn}/imsi-history  (HLR/HSS Mock :8200)
    │   → xác nhận imsi cũ, lấy assigned_at chính xác
    │
    ▼ So sánh với radius_sessions trong PostgreSQL
    │   → tính thời gian kể từ swap
    │
    ▼ Emit swap_event:
        {
          msisdn, old_imsi, new_imsi,
          swap_type: "SIM_SWAP",
          detected_at,   ← từ RADIUS record
          confirmed_at,  ← từ HLR/HSS mock
          source: "RADIUS_CONFLICT_C"
        }
```

## Ưu tiên xử lý

A → B → C (A được check trước). Một record chỉ thuộc 1 loại conflict.
