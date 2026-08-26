#api\dependencies\database.py
"""
asyncpg connection pool dependency cho FastAPI.

Vòng đời pool:
  - Tạo khi app startup (lifespan context trong main.py).
  - Đóng khi app shutdown.
  - Mỗi request nhận 1 connection từ pool qua Depends(get_db).

Cách dùng trong router:
    from api.dependencies.database import get_db
    @router.post("/endpoint")
    async def endpoint(db: asyncpg.Connection = Depends(get_db)):
        row = await db.fetchrow("SELECT ...")
"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator
import asyncpg
from api.config import settings

# Pool singleton — được khởi tạo trong lifespan của main.py
_pool: asyncpg.Pool | None = None


async def create_pool() -> None:
    """
    Khởi tạo connection pool khi app startup.
    Gọi từ lifespan() trong main.py, không gọi trực tiếp từ request.

    Pool config:
        min_size=2: giữ ít nhất 2 connection sẵn sàng.
        max_size=10: tối đa 10 connection đồng thời (đọc từ DB_POOL_SIZE).
        command_timeout=30: query timeout 30s, tránh long-running query
            chặn connection và làm trễ p95 latency.
    """
    global _pool
    _pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=2,
        max_size=settings.db_pool_size,
        command_timeout=30,
        timeout=10,
    )


async def close_pool() -> None:
    """
    Đóng pool khi app shutdown.
    Gọi từ lifespan() trong main.py.
    """
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def get_db() -> AsyncGenerator[asyncpg.Connection, None]:
    """
    FastAPI dependency: cấp 1 connection từ pool cho mỗi request.

    Dùng context manager để đảm bảo connection luôn được trả về pool
    sau khi request hoàn thành — kể cả khi có exception.

    Yields:
        asyncpg.Connection: connection đang active, sẵn sàng query.

    Raises:
        HTTPException 503: nếu pool chưa được khởi tạo (app chưa startup)
            hoặc pool exhausted (tất cả connection đang bận).
    """
    from fastapi import HTTPException, status

    if _pool is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "SERVICE_UNAVAILABLE",
                "message": "Database connection pool not initialized.",
            },
        )

    try:
        async with _pool.acquire(timeout=3) as connection:
            yield connection
    except TimeoutError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "SERVICE_UNAVAILABLE", "message": "Database pool is busy."},
        ) from exc
