# api/

FastAPI cung cấp các API CAMARA, tra cứu mapping Redis và quản lý subscription outbox.

## Files

```
api/
├── main.py                      # App factory, health, metrics mount
├── routers/
│   ├── sim_swap.py              # POST /sim-swap/v0/check & retrieve-date
│   ├── device_swap.py           # POST /device-swap/v0/check & retrieve-date
│   ├── number_verification.py   # POST /number-verification/v0/verify
│   ├── ip_msisdn.py             # GET /ip-msisdn
│   ├── subscriptions.py         # CRUD /subscriptions
│   └── health.py                # GET /health (legacy)
├── schemas/
└── dependencies/
    ├── auth.py                  # X-API-Key validation
    ├── database.py              # asyncpg connection pool
    └── redis.py                 # Redis/Sentinel dependency
```

## Endpoints

| Method | Path | SLA p95 | CAMARA Spec |
|--------|------|---------|------------|
| POST | `/sim-swap/v0/check` | ≤ 200ms | Chính thức |
| POST | `/sim-swap/v0/retrieve-date` | ≤ 200ms | Chính thức |
| POST | `/device-swap/v0/check` | ≤ 200ms | Custom (ADR-005) |
| POST | `/device-swap/v0/retrieve-date` | ≤ 200ms | Custom (ADR-005) |
| POST | `/number-verification/v0/verify` | ≤ 100ms | Chính thức |
| GET | `/ip-msisdn?ipAddress=...` | Chưa chốt | Tra mapping phiên đang active từ Redis |
| POST/GET | `/subscriptions` | Chưa chốt | Tạo/liệt kê subscription |
| GET/PATCH/DELETE | `/subscriptions/{id}` | Chưa chốt | Đọc/sửa/hủy subscription |
| GET | `/health/live` | — | Liveness |
| GET | `/health/ready` | — | Readiness (DB ping) |
| GET | `/metrics` | — | Prometheus |

Swagger UI: http://localhost:8000/docs

## Authentication

Tất cả endpoint nghiệp vụ yêu cầu header `X-API-Key` khớp biến môi trường `API_KEY`.

## Query logic

### SIM Swap

```sql
SELECT changed_at AS detected_at
FROM sim_swap_history
WHERE msisdn = $1
  AND changed_at >= NOW() - ($2 * INTERVAL '1 day')
ORDER BY changed_at DESC
LIMIT 1
```

### Device Swap

Cùng pattern trên `device_swap_history`.

### Number Verification

```sql
SELECT EXISTS (
    SELECT 1
    FROM radius_session_state
    WHERE msisdn = $1
      AND active
      AND last_event_at >= NOW() - INTERVAL '24 hours'
) AS has_active_session
```

Session state được cập nhật bởi consumer `cg-ip-msisdn` từ RADIUS Start/Stop/Accounting-Off.

`changed_at` là thời điểm pipeline **phát hiện** thay đổi IMSI/IMEI từ RADIUS accounting.
Hệ thống hiện chưa tích hợp HLR/HSS/EIR, vì vậy không được diễn giải trường trả về
`latestSimChange`/`latestDeviceChange` là tín hiệu đã được nguồn sự thật thứ hai xác nhận.

Subscription có `phoneNumber=null` áp dụng cho mọi UE. Outbox chọn cả subscription đúng
MSISDN và any-UE; `DELETE` là soft-cancel để giữ audit trail.

## Chạy API server

```bash
# Trong Docker (khuyến nghị — có --build)
bash scripts/run.sh up

# Local development
uvicorn api.main:app --reload --port 8000
```
