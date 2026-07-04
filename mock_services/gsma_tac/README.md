# GSMA TAC Mock API

Mô phỏng **GSMA TAC Allocation Database** — cơ sở dữ liệu chính thức
ánh xạ 6 chữ số đầu IMEI (Type Allocation Code) tới thông tin
nhà sản xuất và model thiết bị.

**API thực:** https://www.gsma.com/services/tac-allocation/  
**Port:** `8100`  
**Swagger UI:** http://localhost:8100/docs

---

## Tại sao pipeline cần API này

IMEI = TAC (6 chữ số) + Serial Number (6 chữ số) + Check Digit (1 chữ số).

Thuật toán Luhn chỉ kiểm tra tính toàn vẹn toán học của IMEI —
không biết TAC đó có được GSMA cấp phép hay không.
Một IMEI có thể pass Luhn nhưng vẫn là giả nếu TAC không tồn tại
trong database GSMA.

```
IMEI:  3 5 2 0 9 9 | 0 0 1 7 6 1 | 4 8 1
       └─────┬─────┘ └─────┬─────┘ └──┬──┘
             TAC          Serial    Check
         (lookup)         Number    (Luhn)
```

Pipeline gọi API này tại **Stage S2 – Validation, Rule R4**.

---

## Endpoints

### `GET /tac/{tac_code}`

Tra cứu thông tin 1 TAC.

**Path param:** `tac_code` — đúng 6 chữ số

**Response 200:**
```json
{
  "tac": "352099",
  "manufacturer": "Samsung",
  "model": "Galaxy S23",
  "device_type": "smartphone",
  "operating_system": "Android",
  "band_support": ["LTE", "NR", "WCDMA", "GSM"],
  "approved_date": "2023-01-15",
  "status": "active"
}
```

**Response 404:**
```json
{
  "error": "NOT_FOUND",
  "message": "TAC '999999' not found in database",
  "request_id": "uuid"
}
```

**Response 422:**
```json
{
  "error": "INVALID_INPUT",
  "message": "TAC must be exactly 6 digits",
  "field": "tac_code"
}
```

---

### `POST /tac/batch`

Tra cứu nhiều TAC cùng lúc. Tối đa 100 TAC mỗi request.

**Request body:**
```json
{
  "tac_codes": ["352099", "490154", "013030"]
}
```

**Response 200:**
```json
{
  "results": {
    "352099": {
      "found": true,
      "manufacturer": "Samsung",
      "model": "Galaxy S23",
      "device_type": "smartphone",
      "status": "active"
    },
    "490154": {
      "found": true,
      "manufacturer": "Apple",
      "model": "iPhone 14",
      "device_type": "smartphone",
      "status": "active"
    },
    "999999": {
      "found": false
    }
  },
  "total": 3,
  "found": 2,
  "not_found": 1
}
```

---

### `GET /tac`

Liệt kê TAC với phân trang.

**Query params:**
- `page` (int, default=1)
- `page_size` (int, default=50, max=200)
- `manufacturer` (string, optional) — filter theo nhà sản xuất
- `device_type` (string, optional) — filter: smartphone / tablet / router / iot

**Response 200:**
```json
{
  "items": [ ... ],
  "total": 2000,
  "page": 1,
  "page_size": 50,
  "pages": 40
}
```

---

### `GET /health`

```json
{
  "status": "ok",
  "service": "gsma-tac-mock",
  "records": 2000,
  "uptime_seconds": 3600
}
```

---

## Dữ liệu mock (`data/tac_records.csv`)

2.000 TAC records, generated bởi `seed.py --count 2000 --seed 42`.

Schema CSV:
```
tac_code, manufacturer, model, device_type, operating_system,
band_support, approved_date, status
```

Phân bố:
- 60% smartphone (Samsung, Apple, Xiaomi, Oppo, Vivo, Realme)
- 20% tablet
- 10% mobile router / MiFi
- 10% IoT / M2M

**Quan trọng:** simulator `generators.py` đọc file này khi sinh IMEI,
đảm bảo 100% IMEI hợp lệ do simulator sinh ra có TAC trong mock database.
`error_injectors.py::InvalidImeiInjector` sinh TAC nằm ngoài bộ này.

---

## Seed

```bash
python mock_services/gsma_tac/seed.py --count 2000 --seed 42
# Output: mock_services/gsma_tac/data/tac_records.csv
```

---

## Cấu trúc code

```
gsma_tac/
├── app.py          # FastAPI app factory, lifespan (load CSV vào memory)
├── router.py       # 4 endpoints trên
├── models.py       # Pydantic models: TacRecord, TacLookupResponse, BatchRequest…
├── seed.py         # Script sinh tac_records.csv
├── data/
│   └── tac_records.csv
├── Dockerfile
└── README.md       # File này
```

`app.py` load toàn bộ CSV vào dict `{tac_code: TacRecord}` khi startup
→ mọi lookup là O(1), không cần database riêng.
