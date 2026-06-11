# Mock External Services

Ba service này mô phỏng các API bên ngoài mà pipeline và simulator
phụ thuộc. Mỗi service là một FastAPI app độc lập, chạy trong Docker,
phục hồi **đúng contract** của API thực tế tương ứng.

---

## Tổng quan 3 mock services

```
┌─────────────────────────────────────────────────────────────────────┐
│                        camara-pipeline                              │
│                                                                     │
│  simulator/         pipeline/validation/      api/                  │
│  generators.py      rules.py (R3, R4)         routers/              │
│       │                  │      │                  │                │
│       │     ┌────────────┘      │                  │                │
│       ▼     ▼                   ▼                  ▼                │
│  ┌─────────────┐  ┌──────────────────┐  ┌──────────────────────┐   │
│  │  GSMA TAC   │  │    HLR / HSS     │  │   ITU E.164          │   │
│  │  Mock API   │  │   Mock API       │  │   Mock API           │   │
│  │  :8100      │  │   :8200          │  │   :8300              │   │
│  │             │  │                  │  │                      │   │
│  │ TAC lookup  │  │ IMSI↔MSISDN map  │  │ Country code /       │   │
│  │ IMEI valid. │  │ subscriber exist │  │ operator prefix      │   │
│  │ device info │  │ profile query    │  │ MSISDN format valid  │   │
│  └─────────────┘  └──────────────────┘  └──────────────────────┘   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 1. GSMA TAC Mock API (`:8100`)

**Thực thể thật:** GSMA TAC Allocation database  
**Spec thật:** https://www.gsma.com/services/tac-allocation/  
**Dùng bởi:** `pipeline/validation/rules.py` rule R4, `simulator/generators.py`

| Endpoint | Method | Mô tả |
|----------|--------|-------|
| `GET /tac/{tac_code}` | GET | Tra cứu thông tin 1 TAC (6 chữ số đầu IMEI) |
| `POST /tac/batch` | POST | Tra cứu nhiều TAC cùng lúc (tối đa 100) |
| `GET /tac` | GET | Liệt kê TAC (có phân trang) |
| `GET /health` | GET | Health check |

**Dữ liệu:** `data/tac_records.csv` — 2.000 TAC giả, mỗi record gồm:
`tac_code, manufacturer, model, device_type, band_support, approved_date`

Chi tiết: [`gsma_tac/README.md`](gsma_tac/README.md)

---

## 2. HLR/HSS Mock API (`:8200`)

**Thực thể thật:** Home Location Register / Home Subscriber Server (3GPP TS 29.002 / TS 29.272)  
**Dùng bởi:** `pipeline/validation/rules.py` rule R3 (IMSI tồn tại), `pipeline/conflict_resolution/swap_detector.py`

| Endpoint | Method | Mô tả |
|----------|--------|-------|
| `GET /subscribers/by-imsi/{imsi}` | GET | Lấy profile theo IMSI |
| `GET /subscribers/by-msisdn/{msisdn}` | GET | Lấy profile theo MSISDN (E.164) |
| `GET /subscribers/{imsi}/msisdn-history` | GET | Lịch sử MSISDN của 1 IMSI |
| `GET /subscribers/{msisdn}/imsi-history` | GET | Lịch sử IMSI của 1 MSISDN (SIM Swap trace) |
| `POST /subscribers/batch-lookup` | POST | Lookup nhiều IMSI/MSISDN cùng lúc |
| `GET /health` | GET | Health check |

**Dữ liệu:** `data/subscribers.csv` — 100.000 subscriber khớp với simulator seed=42.

Chi tiết: [`hlr_hss/README.md`](hlr_hss/README.md)

---

## 3. ITU E.164 Number Plan Mock API (`:8300`)

**Thực thể thật:** ITU-T E.164 National Number Plan / GSMA IMSI database  
**Spec thật:** https://www.itu.int/rec/T-REC-E.164/en  
**Dùng bởi:** `pipeline/validation/rules.py` rule R2 (MSISDN format)

| Endpoint | Method | Mô tả |
|----------|--------|-------|
| `GET /country-codes` | GET | Liệt kê tất cả country code E.164 |
| `GET /country-codes/{cc}` | GET | Thông tin 1 country code (VD: `84` → Vietnam) |
| `GET /country-codes/{cc}/operators` | GET | Operator prefixes trong 1 country |
| `POST /validate` | POST | Validate 1 MSISDN: format + country + operator |
| `POST /validate/batch` | POST | Validate nhiều MSISDN (tối đa 500) |
| `GET /health` | GET | Health check |

**Dữ liệu:** `data/country_codes.csv` + `data/operator_prefixes.csv`  
Bao gồm: Việt Nam (MCC 452) đầy đủ 6 operator, và ~50 country codes phổ biến.

Chi tiết: [`itu_e164/README.md`](itu_e164/README.md)

---

## Khởi động

```bash
# Chạy cả 3 mock service
docker compose -f mock_services/docker-compose.mock.yml up -d

# Hoặc từ root project (mock được include trong docker-compose.yml chính)
make up
```

### Seed dữ liệu đồng bộ với simulator

Mock services và simulator dùng **cùng seed=42**, đảm bảo:
- IMEI do simulator sinh ra có TAC tồn tại trong GSMA TAC mock
- MSISDN/IMSI do simulator sinh ra có trong HLR/HSS mock
- MSISDN format hợp lệ theo ITU E.164 mock

```bash
# Sinh dữ liệu cho cả 3 mock service (chạy 1 lần trước khi start)
python mock_services/gsma_tac/seed.py   --count 2000 --seed 42
python mock_services/hlr_hss/seed.py    --count 100000 --seed 42
python mock_services/itu_e164/seed.py
```

---

## Cách pipeline gọi mock services

Pipeline gọi mock service qua HTTP, giống hệt cách gọi API thực:

```python
# pipeline/validation/rules.py — Rule R4: IMEI validation
async def validate_imei(imei: str) -> ValidationResult:
    # 1. Luhn check (local, không cần network)
    if not luhn_check(imei):
        return ValidationResult(valid=False, error="ERR_IMEI_LUHN_FAIL")
    # 2. TAC lookup qua GSMA TAC Mock API
    tac = imei[:6]
    resp = await http_client.get(f"{TAC_API_URL}/tac/{tac}")
    if resp.status_code == 404:
        return ValidationResult(valid=False, error="ERR_IMEI_TAC_UNKNOWN")
    return ValidationResult(valid=True, device_info=resp.json())
```

```python
# pipeline/validation/rules.py — Rule R3: IMSI validation
async def validate_imsi(imsi: str, msisdn: str) -> ValidationResult:
    resp = await http_client.get(f"{HLR_API_URL}/subscribers/by-imsi/{imsi}")
    if resp.status_code == 404:
        return ValidationResult(valid=False, error="ERR_IMSI_NOT_IN_HLR")
    return ValidationResult(valid=True)
```

```python
# pipeline/validation/rules.py — Rule R2: MSISDN validation
async def validate_msisdn(msisdn: str) -> ValidationResult:
    resp = await http_client.post(
        f"{ITU_API_URL}/validate",
        json={"phoneNumber": msisdn}
    )
    data = resp.json()
    if not data["valid"]:
        return ValidationResult(valid=False, error="ERR_MSISDN_INVALID_FORMAT")
    return ValidationResult(valid=True, country=data["country"], operator=data["operator"])
```

---

## Response format chuẩn

Tất cả 3 mock service dùng chung error format từ `shared/errors.py`:

```json
// 404 Not Found
{
  "error": "NOT_FOUND",
  "message": "TAC '999999' not found in database",
  "request_id": "uuid-..."
}

// 422 Validation Error
{
  "error": "INVALID_INPUT",
  "message": "TAC must be exactly 6 digits",
  "field": "tac_code"
}

// 503 Service Unavailable (chỉ khi mock tự test fault tolerance)
{
  "error": "SERVICE_UNAVAILABLE",
  "message": "Database temporarily unavailable",
  "retry_after": 5
}
```

---

## Fault injection (test pipeline resilience)

Mỗi mock service hỗ trợ header `X-Inject-Fault` để test pipeline
xử lý lỗi external service:

```bash
# Giả lập TAC service chậm (500ms delay)
curl -H "X-Inject-Fault: delay=500" http://localhost:8100/tac/352099

# Giả lập TAC service trả 503
curl -H "X-Inject-Fault: status=503" http://localhost:8100/tac/352099

# Giả lập 20% request bị lỗi ngẫu nhiên
curl -H "X-Inject-Fault: error_rate=0.2" http://localhost:8100/tac/352099
```
