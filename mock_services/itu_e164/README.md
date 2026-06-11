# ITU E.164 Number Plan Mock API

Mô phỏng **ITU-T E.164 National Numbering Plan** — tiêu chuẩn quốc tế
định nghĩa cấu trúc và độ dài hợp lệ của số điện thoại theo từng quốc gia
và từng nhà mạng.

**Spec thật:** ITU-T E.164, ITU-T E.212 (IMSI/MCC)  
**Port:** `8300`  
**Swagger UI:** http://localhost:8300/docs

---

## Tại sao pipeline cần API này

Validation Rule R2 không chỉ kiểm tra "có đúng E.164 format không"
(bắt đầu `+`, đủ độ dài) — mà cần kiểm tra sâu hơn:

- Country code (`+84`) có tồn tại không?
- Prefix sau country code có thuộc về operator nào không?
- Độ dài tổng có đúng với quy định của quốc gia đó không?

Ví dụ: `+84971234567` — CC=84 (Việt Nam), prefix=97 (Viettel)
→ đây là số hợp lệ, độ dài 11 chữ số (đúng với Việt Nam).

Ví dụ: `+849712345` — CC=84, prefix=97, nhưng chỉ có 9 chữ số
→ quá ngắn theo quy định Việt Nam → invalid.

---

## Endpoints

### `GET /country-codes`

Liệt kê tất cả country code có trong mock database.

**Query params:**
- `page`, `page_size`
- `region` (optional): `APAC` / `EMEA` / `AMER`

**Response 200:**
```json
{
  "items": [
    {
      "country_code": "84",
      "country_name": "Vietnam",
      "iso_alpha2": "VN",
      "region": "APAC",
      "min_subscriber_length": 9,
      "max_subscriber_length": 9,
      "trunk_prefix": "0"
    },
    ...
  ],
  "total": 52,
  "page": 1,
  "page_size": 50
}
```

---

### `GET /country-codes/{cc}`

Thông tin chi tiết 1 country code.

**Response 200:**
```json
{
  "country_code": "84",
  "country_name": "Vietnam",
  "iso_alpha2": "VN",
  "iso_alpha3": "VNM",
  "region": "APAC",
  "min_subscriber_length": 9,
  "max_subscriber_length": 9,
  "trunk_prefix": "0",
  "international_prefix": "00",
  "note": "9-digit subscriber numbers since 2018 migration from 10-digit"
}
```

**Response 404:** Country code không tồn tại.

---

### `GET /country-codes/{cc}/operators`

Danh sách operator prefix trong 1 quốc gia.

**Response 200 (ví dụ Việt Nam CC=84):**
```json
{
  "country_code": "84",
  "country_name": "Vietnam",
  "operators": [
    {
      "prefix": "32", "operator": "Viettel",  "mnc": "04", "type": "mobile"
    },
    {
      "prefix": "33", "operator": "Viettel",  "mnc": "04", "type": "mobile"
    },
    {
      "prefix": "34", "operator": "Viettel",  "mnc": "04", "type": "mobile"
    },
    {
      "prefix": "35", "operator": "Viettel",  "mnc": "04", "type": "mobile"
    },
    {
      "prefix": "36", "operator": "Viettel",  "mnc": "04", "type": "mobile"
    },
    {
      "prefix": "37", "operator": "Viettel",  "mnc": "04", "type": "mobile"
    },
    {
      "prefix": "38", "operator": "Viettel",  "mnc": "04", "type": "mobile"
    },
    {
      "prefix": "39", "operator": "Viettel",  "mnc": "04", "type": "mobile"
    },
    {
      "prefix": "56", "operator": "Vietnamobile", "mnc": "05", "type": "mobile"
    },
    {
      "prefix": "58", "operator": "Vietnamobile", "mnc": "05", "type": "mobile"
    },
    {
      "prefix": "70", "operator": "Mobifone",  "mnc": "01", "type": "mobile"
    },
    {
      "prefix": "76", "operator": "Mobifone",  "mnc": "01", "type": "mobile"
    },
    {
      "prefix": "77", "operator": "Mobifone",  "mnc": "01", "type": "mobile"
    },
    {
      "prefix": "78", "operator": "Mobifone",  "mnc": "01", "type": "mobile"
    },
    {
      "prefix": "79", "operator": "Mobifone",  "mnc": "01", "type": "mobile"
    },
    {
      "prefix": "81", "operator": "Vinaphone", "mnc": "02", "type": "mobile"
    },
    {
      "prefix": "82", "operator": "Vinaphone", "mnc": "02", "type": "mobile"
    },
    {
      "prefix": "83", "operator": "Vinaphone", "mnc": "02", "type": "mobile"
    },
    {
      "prefix": "84", "operator": "Vinaphone", "mnc": "02", "type": "mobile"
    },
    {
      "prefix": "85", "operator": "Vinaphone", "mnc": "02", "type": "mobile"
    },
    {
      "prefix": "86", "operator": "Vinaphone", "mnc": "02", "type": "mobile"
    },
    {
      "prefix": "88", "operator": "Vinaphone", "mnc": "02", "type": "mobile"
    },
    {
      "prefix": "89", "operator": "Vinaphone", "mnc": "02", "type": "mobile"
    },
    {
      "prefix": "92", "operator": "Vietnamobile", "mnc": "05", "type": "mobile"
    },
    {
      "prefix": "93", "operator": "Mobifone",  "mnc": "01", "type": "mobile"
    },
    {
      "prefix": "94", "operator": "Vinaphone", "mnc": "02", "type": "mobile"
    },
    {
      "prefix": "96", "operator": "Viettel",   "mnc": "04", "type": "mobile"
    },
    {
      "prefix": "97", "operator": "Viettel",   "mnc": "04", "type": "mobile"
    },
    {
      "prefix": "98", "operator": "Viettel",   "mnc": "04", "type": "mobile"
    }
  ],
  "total": 29
}
```

---

### `POST /validate`

Validate 1 MSISDN đầy đủ: format + country code + operator prefix + độ dài.

**Request body:**
```json
{
  "phone_number": "+84971234567"
}
```

**Response 200 — valid:**
```json
{
  "phone_number": "+84971234567",
  "valid": true,
  "country_code": "84",
  "country_name": "Vietnam",
  "subscriber_number": "971234567",
  "operator": "Viettel",
  "mnc": "04",
  "number_type": "mobile",
  "e164_format": "+84971234567",
  "national_format": "097 123 4567"
}
```

**Response 200 — invalid:**
```json
{
  "phone_number": "+849712345",
  "valid": false,
  "error_code": "ERR_SUBSCRIBER_TOO_SHORT",
  "error_detail": "Vietnam subscriber number must be 9 digits, got 7",
  "country_code": "84",
  "country_name": "Vietnam"
}
```

Các `error_code` có thể trả về:
- `ERR_NOT_E164` — không bắt đầu bằng `+`
- `ERR_COUNTRY_CODE_UNKNOWN` — CC không có trong database
- `ERR_PREFIX_UNKNOWN` — prefix không thuộc operator nào
- `ERR_SUBSCRIBER_TOO_SHORT` / `ERR_SUBSCRIBER_TOO_LONG`
- `ERR_NON_NUMERIC` — có ký tự không phải số

---

### `POST /validate/batch`

Validate nhiều MSISDN cùng lúc. Tối đa 500 mỗi request.

**Request body:**
```json
{
  "phone_numbers": ["+84971234567", "+849712345", "invalid"]
}
```

**Response 200:**
```json
{
  "results": [
    {"phone_number": "+84971234567", "valid": true,  "operator": "Viettel", ...},
    {"phone_number": "+849712345",   "valid": false, "error_code": "ERR_SUBSCRIBER_TOO_SHORT"},
    {"phone_number": "invalid",      "valid": false, "error_code": "ERR_NOT_E164"}
  ],
  "total": 3,
  "valid": 1,
  "invalid": 2
}
```

---

### `GET /health`

```json
{
  "status": "ok",
  "service": "itu-e164-mock",
  "country_codes": 52,
  "operator_prefixes": 380,
  "uptime_seconds": 3600
}
```

---

## Dữ liệu mock

### `data/country_codes.csv`
52 quốc gia phổ biến nhất. Schema:
```
country_code, country_name, iso_alpha2, iso_alpha3, region,
min_subscriber_length, max_subscriber_length, trunk_prefix, international_prefix
```

### `data/operator_prefixes.csv`
~380 operator prefix. Việt Nam đầy đủ 29 prefix (6 operator).
Schema:
```
country_code, prefix, operator, mnc, type
```

---

## Seed

Dữ liệu E.164 là static (không phụ thuộc seed), chạy 1 lần:

```bash
python mock_services/itu_e164/seed.py
# Ghi data/country_codes.csv và data/operator_prefixes.csv
```

---

## Cấu trúc code

```
itu_e164/
├── app.py          # FastAPI app, load 2 CSV vào dict khi startup
├── router.py       # 5 endpoints trên
├── models.py       # PhoneNumber, ValidationResult, CountryCode, OperatorPrefix…
├── seed.py         # Sinh country_codes.csv + operator_prefixes.csv (static data)
├── data/
│   ├── country_codes.csv
│   └── operator_prefixes.csv
├── Dockerfile
└── README.md
```
