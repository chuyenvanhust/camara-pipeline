# api/

Ba CAMARA Network API endpoint được xây dựng bằng FastAPI,
query trực tiếp từ PostgreSQL storage layer.

## Files

```
api/
├── main.py                      # App factory, mount routers, exception handlers
├── config.py                    # Settings từ env vars
├── routers/
│   ├── sim_swap.py              # POST /sim-swap/v0/check & retrieve-date
│   ├── device_swap.py           # POST /device-swap/v0/check & retrieve-date
│   ├── number_verification.py   # POST /number-verification/v0/verify
│   └── health.py                # GET /health
├── schemas/
│   ├── sim_swap.py              # Pydantic request/response cho SIM Swap
│   ├── device_swap.py           # Pydantic request/response cho Device Swap
│   ├── number_verification.py   # Pydantic request/response cho Number Verification
│   └── common.py                # PhoneNumber (E.164), ErrorResponse, shared types
└── dependencies/
    ├── auth.py                  # API Key validation: header X-API-Key
    └── database.py              # asyncpg connection pool
```

## Endpoints

| Method | Path | SLA p95 | CAMARA Spec |
|--------|------|---------|------------|
| POST | `/sim-swap/v0/check` | ≤ 200ms | Chính thức |
| POST | `/sim-swap/v0/retrieve-date` | ≤ 200ms | Chính thức |
| POST | `/device-swap/v0/check` | ≤ 200ms | Custom (xem ADR-005) |
| POST | `/device-swap/v0/retrieve-date` | ≤ 200ms | Custom (xem ADR-005) |
| POST | `/number-verification/v0/verify` | ≤ 100ms | Chính thức |
| GET | `/health` | — | — |

Swagger UI: http://localhost:8000/docs  
ReDoc: http://localhost:8000/redoc

## Authentication

Tất cả endpoint (trừ `/health`) yêu cầu:
```
X-API-Key: <value của API_KEY trong .env>
```

Thiếu hoặc sai key → `401 Unauthorized`.

## Query logic

### SIM Swap
```sql
SELECT detected_at FROM swap_event
WHERE msisdn = $1
  AND swap_type = 'SIM_SWAP'
  AND detected_at >= NOW() - INTERVAL '$2 days'
ORDER BY detected_at DESC
LIMIT 1
```
`check` trả `swapped: true` nếu có row. `retrieve-date` trả `latestSimChange`.

### Device Swap
Cùng logic, `swap_type = 'DEVICE_SWAP'`, trả `latestDeviceChange`.

### Number Verification
```sql
SELECT EXISTS (
  SELECT 1 FROM radius_sessions
  WHERE msisdn = $1
    AND acct_status_type = 'Start'
    AND event_timestamp >= NOW() - INTERVAL '24 hours'
    AND NOT EXISTS (
      SELECT 1 FROM radius_sessions s2
      WHERE s2.acct_session_id = radius_sessions.acct_session_id
        AND s2.acct_status_type = 'Stop'
    )
)
```

## Chạy API server

```bash
# Trong Docker (khuyến nghị)
docker compose up api

# Local development
uvicorn api.main:app --reload --port 8000
```
