from __future__ import annotations

import os
from typing import Any

import redis.asyncio as aioredis
from redis.asyncio.sentinel import Sentinel


def _sentinel_nodes(raw: str) -> list[tuple[str, int]]:
    nodes: list[tuple[str, int]] = []
    for item in raw.split(","):
        host, separator, port = item.strip().rpartition(":")
        if not separator or not host:
            raise ValueError("REDIS_SENTINELS must use host:port[,host:port]")
        nodes.append((host, int(port)))
    return nodes


def create_redis_client(**overrides: Any) -> aioredis.Redis:
    """Create a direct Redis client or a Sentinel-discovered master client."""
    options: dict[str, Any] = {
        "db": int(os.getenv("REDIS_DB", "0")),
        "decode_responses": True,
        "socket_connect_timeout": 5,
        "socket_timeout": 5,
        "health_check_interval": 15,
    }
    options.update(overrides)
    sentinels = os.getenv("REDIS_SENTINELS", "").strip()
    if sentinels:
        sentinel = Sentinel(
            _sentinel_nodes(sentinels),
            socket_connect_timeout=options["socket_connect_timeout"],
            socket_timeout=options["socket_timeout"],
        )
        return sentinel.master_for(
            os.getenv("REDIS_MASTER_NAME", "camara-master"),
            **options,
        )
    return aioredis.Redis(
        host=os.getenv("REDIS_HOST", "camara-redis"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        **options,
    )
