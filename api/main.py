#api\main.py

"""
FastAPI application factory cho CAMARA Network API.

Vòng đời app (lifespan):
  startup  → tạo asyncpg connection pool (create_pool)
  shutdown → đóng pool (close_pool)

Routers được mount với prefix rõ ràng.
Exception handlers chuẩn hóa response format theo ErrorResponse schema.

Chạy local:
  uvicorn api.main:app --reload --port 8000

Swagger UI: http://localhost:8000/docs
ReDoc:      http://localhost:8000/redoc
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

from api.routers import health, sim_swap, device_swap, number_verification
from api.dependencies.database import create_pool, close_pool, _pool
import asyncpg


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Quản lý vòng đời app:
    - Trước yield: startup — tạo DB pool.
    - Sau yield:   shutdown — đóng DB pool.

    FastAPI gọi lifespan tự động khi start/stop.
    Không dùng @app.on_event("startup") vì deprecated từ FastAPI 0.93.
    """
    await create_pool()
    yield
    await close_pool()


app = FastAPI(
    title="CAMARA Network API",
    description=(
        "Data Pipeline phục vụ CAMARA Network API: SIM Swap, "
        "Device Swap, Number Verification. "
        "Nguồn dữ liệu: GGSN RADIUS Accounting (RFC 2866 + 3GPP TS 29.061)."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# F-12: Tách liveness (process sống) và readiness (DB truy vấn được)
@app.get("/health")
async def health_legacy():
    """Legacy health endpoint — redirect về /health/live."""
    return {"status": "ok"}


@app.get("/health/live")
async def liveness():
    """Liveness: process còn sống — dùng cho container restart policy."""
    return {"status": "ok"}


@app.get("/health/ready")
async def readiness():
    """Readiness: DB thực sự truy vấn được — dùng cho load balancer routing."""
    from api.dependencies.database import _pool as pool_ref
    try:
        if pool_ref is None:
            return JSONResponse(
                status_code=503,
                content={"status": "not_ready", "error": "Database pool not initialized"},
            )
        async with pool_ref.acquire() as conn:
            await conn.fetchval("SELECT 1")
        return {"status": "ready"}
    except Exception as exc:
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "error": str(exc)},
        )


# F-08: Mount Prometheus metrics ASGI app
try:
    from prometheus_client import make_asgi_app
    metrics_app = make_asgi_app()
    app.mount("/metrics", metrics_app)
except ImportError:
    pass  # prometheus_client not installed — metrics endpoint disabled


# ── Exception Handlers ────────────────────────────────────────────────────────

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """
    Override FastAPI default 422 handler để chuẩn hóa format lỗi
    theo ErrorResponse schema (error, message, request_id).

    FastAPI mặc định trả lỗi 422 với format khác — nếu để vậy,
    client CAMARA API sẽ nhận format không nhất quán với 401/503.
    """
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "INVALID_ARGUMENT",
            "message": str(exc.errors()),
            "request_id": request.headers.get("x-request-id", "unknown"),
        },
    )


@app.exception_handler(Exception)
async def generic_exception_handler(
    request: Request, exc: Exception
) -> JSONResponse:
    """
    Catch-all handler cho exception không lường trước.
    Trả 500 Internal Server Error với format chuẩn.
    Không expose stack trace ra client (security).
    """
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "error": "INTERNAL_ERROR",
            "message": "An unexpected error occurred.",
            "request_id": request.headers.get("x-request-id", "unknown"),
        },
    )

@app.exception_handler(asyncpg.PostgresError)
async def db_exception_handler(request: Request, exc: asyncpg.PostgresError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={
            "error": "SERVICE_UNAVAILABLE",
            "message": "Database temporarily unavailable.",
            "request_id": request.headers.get("x-request-id", "unknown"),
        },
    )


@app.exception_handler(asyncpg.InterfaceError)
async def db_interface_error_handler(request: Request, exc: asyncpg.InterfaceError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={
            "error": "SERVICE_UNAVAILABLE",
            "message": str(exc) or "Database connection timeout",
            "request_id": request.headers.get("x-request-id", "unknown"),
        },
    )

# ── Mount Routers ─────────────────────────────────────────────────────────────


app.include_router(sim_swap.router)
app.include_router(device_swap.router)
app.include_router(number_verification.router)