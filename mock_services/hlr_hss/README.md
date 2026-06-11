# HLR/HSS Mock API

Mô phỏng **Home Location Register (HLR)** / **Home Subscriber Server (HSS)**
— thành phần lõi của mạng di động lưu trữ hồ sơ thuê bao và ánh xạ
MSISDN ↔ IMSI.

**Spec thật:** 3GPP TS 29.002 (HLR MAP), 3GPP TS 29.272 (HSS Diameter S6a)  
**Port:** `8200`  
**Swagger UI:** http://localhost:8200/docs

---

## Tại sao pipeline cần API này

HLR/HSS là nguồn sự thật duy nhất về:

1. **IMSI có tồn tại không** — validation rule R3
2. **MSISDN ↔ IMSI mapping hiện tại** — xác nhận SIM Swap
3. **Lịch sử thay đổi IMSI của 1 MSISDN** — phát hiện SIM Swap (conflict C)

Khi `pipeline/conflict_resolution/swap_detector.py` phát hiện
cùng MSISDN ánh xạ sang IMSI mới (conflict C), nó gọi API này
để lấy lịch sử đầy đủ và xác nhận thời điểm swap.

```
conflict_resolution/swap_detector.py
    │
    ▼ GET /subscribers/{msisdn}/imsi-history
HLR/HSS Mock API
    │
    ▼ trả lịch sử: IMSI cũ → IMSI mới, với timestamp
swap_detector.py emit swap_event vào Kafka
```

---

## Endpoints

### `GET /subscribers/by-imsi/{imsi}`

Lấy profile đầy đủ của subscriber theo IMSI.

**Response 200:**
```json
{
  "imsi": "452010123456789",
  "msisdn": "+84971234567",
  "status": "active",
  "mcc": "452",
  "mnc": "01",
  "operator": "Viettel",
  "service_profile": {
    "data_enabled": true,
    "roaming_enabled": false,
    "volte_enabled": true
  },
  "registered_at": "2022-03-15T08:00:00Z",
  "last_updated": "2024-11-01T14:23:00Z"
}
```

**Response 404:** IMSI không tồn tại trong HLR.

---

### `GET /subscribers/by-msisdn/{msisdn}`

Lấy profile theo MSISDN (E.164, encode URL: `%2B84971234567`).

**Response 200:** Cùng format như by-imsi.  
**Response 404:** MSISDN không tồn tại.

---

### `GET /subscribers/{imsi}/msisdn-history`

Lịch sử thay đổi MSISDN của 1 IMSI (number portability / reassignment).

**Query params:**
- `from_date` (ISO 8601, optional)
- `to_date` (ISO 8601, optional)
- `limit` (int, default=20)

**Response 200:**
```json
{
  "imsi": "452010123456789",
  "history": [
    {
      "msisdn": "+84971234567",
      "assigned_at": "2022-03-15T08:00:00Z",
      "unassigned_at": null,
      "is_current": true
    },
    {
      "msisdn": "+84909876543",
      "assigned_at": "2020-01-10T00:00:00Z",
      "unassigned_at": "2022-03-15T08:00:00Z",
      "is_current": false
    }
  ],
  "total": 2
}
```

---

### `GET /subscribers/{msisdn}/imsi-history`

Lịch sử thay đổi IMSI của 1 MSISDN — **endpoint chính cho SIM Swap detection**.

Mỗi lần SIM được thay (người dùng mua SIM mới, cắm vào điện thoại),
MSISDN được gán sang IMSI mới → HLR ghi lại.

**Response 200:**
```json
{
  "msisdn": "+84971234567",
  "history": [
    {
      "imsi": "452010123456789",
      "assigned_at": "2024-10-20T09:15:00Z",
      "unassigned_at": null,
      "is_current": true,
      "swap_reason": "customer_request"
    },
    {
      "imsi": "452010987654321",
      "assigned_at": "2022-03-15T08:00:00Z",
      "unassigned_at": "2024-10-20T09:15:00Z",
      "is_current": false,
      "swap_reason": "initial_activation"
    }
  ],
  "total": 2,
  "sim_swap_count": 1,
  "latest_swap_at": "2024-10-20T09:15:00Z"
}
```

---

### `POST /subscribers/batch-lookup`

Lookup nhiều subscriber cùng lúc. Tối đa 200 records mỗi request.

**Request body:**
```json
{
  "lookups": [
    {"type": "imsi", "value": "452010123456789"},
    {"type": "msisdn", "value": "+84971234567"},
    {"type": "imsi", "value": "000000000000000"}
  ]
}
```

**Response 200:**
```json
{
  "results": [
    {"query": {"type": "imsi", "value": "452010123456789"}, "found": true, "subscriber": {...}},
    {"query": {"type": "msisdn", "value": "+84971234567"},   "found": true, "subscriber": {...}},
    {"query": {"type": "imsi", "value": "000000000000000"},  "found": false}
  ],
  "total": 3,
  "found": 2,
  "not_found": 1
}
```

---

### `GET /health`

```json
{
  "status": "ok",
  "service": "hlr-hss-mock",
  "subscribers": 100000,
  "uptime_seconds": 3600
}
```

---

## Dữ liệu mock (`data/subscribers.csv`)

100.000 subscriber, đồng bộ với `simulator/generators.py --seed 42`.

Schema CSV:
```
imsi, msisdn, status, mcc, mnc, operator,
data_enabled, roaming_enabled, volte_enabled,
registered_at, last_updated
```

Lưu ý: một số subscriber (khoảng 2%) có **2 dòng** trong CSV
với cùng MSISDN nhưng IMSI khác nhau — đây là dữ liệu SIM Swap
đã xảy ra, phục vụ test case TC05–TC07 và TC28.

---

## Seed

```bash
python mock_services/hlr_hss/seed.py --count 100000 --seed 42
# Output: mock_services/hlr_hss/data/subscribers.csv
# Phải chạy CÙNG seed với simulator
```

---

## Cấu trúc code

```
hlr_hss/
├── app.py          # FastAPI app, load CSV vào 2 dict: by_imsi, by_msisdn
├── router.py       # 5 endpoints trên
├── models.py       # SubscriberProfile, ImsiHistoryEntry, BatchLookupRequest…
├── seed.py         # Sinh subscribers.csv đồng bộ với simulator seed
├── data/
│   └── subscribers.csv
├── Dockerfile
└── README.md
```
