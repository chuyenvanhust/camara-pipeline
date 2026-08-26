# api/

Ba CAMARA Network API endpoint được xây dựng bằng FastAPI, query trực tiếp từ PostgreSQL storage layer.

## Files

```
api/
├── main.py                      # App factory, health, metrics mount
├── routers/
│   ├── sim_swap.py              # POST /sim-swap/v0/check & retrieve-date
│   ├── device_swap.py           # POST /device-swap/v0/check & retrieve-date
│   ├── number_verification.py   # POST /number-verification/v0/verify
│   └── health.py                # GET /health (legacy)
├── schemas/
└── dependencies/
    ├── auth.py                  # X-API-Key validation
    └── database.py              # asyncpg connection pool
```

## Endpoints

| Method | Path | SLA p95 | CAMARA Spec |
|--------|------|---------|------------|
| POST | `/sim-swap/v0/check` | ≤ 200ms | Chính thức |
| POST | `/sim-swap/v0/retrieve-date` | ≤ 200ms | Chính thức |
| POST | `/device-swap/v0/check` | ≤ 200ms | Custom (ADR-005) |
| POST | `/device-swap/v0/retrieve-date` | ≤ 200ms | Custom (ADR-005) |
| POST | `/number-verification/v0/verify` | ≤ 100ms | Chính thức |
| GET | `/health/live` | — | Liveness |
| GET | `/health/ready` | — | Readiness (DB ping) |
| GET | `/metrics` | — | Prometheus |

Swagger UI: http://localhost:8000/docs

## Authentication

Tất cả endpoint nghiệp vụ yêu cầu header `X-API-Key` khớp biến môi trường `API_KEY`.

## Query logic

### SIM Swap

```sql
SELECT changed_at AS confirmed_at
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

## Chạy API server

```bash
# Trong Docker (khuyến nghị — có --build)
bash scripts/run.sh up

# Local development
uvicorn api.main:app --reload --port 8000
```
