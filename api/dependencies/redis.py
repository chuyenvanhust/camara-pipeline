from __future__ import annotations

from typing import AsyncGenerator

from fastapi import HTTPException, status
from redis.asyncio import Redis

from pipeline.modules.shared.redis_client import create_redis_client


_redis: Redis | None = None


async def create_redis_pool() -> None:
    global _redis
    client = create_redis_client()
    await client.ping()
    _redis = client


async def close_redis_pool() -> None:
    global _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None


async def get_redis() -> AsyncGenerator[Redis, None]:
    if _redis is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "SERVICE_UNAVAILABLE", "message": "Redis is not ready."},
        )
    yield _redis
