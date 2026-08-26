from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from pathlib import Path

import asyncpg


logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
logger = logging.getLogger(__name__)
MIGRATIONS = Path(__file__).resolve().parent / "migrations"
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:camara@camara-postgres:5432/camara_db",
)


async def apply_migrations() -> None:
    connection = await asyncpg.connect(DATABASE_URL, timeout=10)
    locked = False
    try:
        await connection.execute(
            "SELECT pg_advisory_lock(hashtext('camara_pipeline_migrations'))"
        )
        locked = True
        await connection.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version TEXT PRIMARY KEY,
                checksum TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        for path in sorted(MIGRATIONS.glob("*.sql")):
            sql = path.read_text(encoding="utf-8")
            checksum = hashlib.sha256(sql.encode("utf-8")).hexdigest()
            current = await connection.fetchval(
                "SELECT checksum FROM schema_migrations WHERE version=$1", path.name
            )
            if current:
                if current != checksum:
                    raise RuntimeError(f"applied migration was modified: {path.name}")
                continue
            async with connection.transaction():
                await connection.execute(sql)
                await connection.execute(
                    "INSERT INTO schema_migrations(version, checksum) VALUES($1, $2)",
                    path.name,
                    checksum,
                )
            logger.info("migration applied: %s", path.name)
    finally:
        if locked:
            await connection.execute(
                "SELECT pg_advisory_unlock(hashtext('camara_pipeline_migrations'))"
            )
        await connection.close()


if __name__ == "__main__":
    asyncio.run(apply_migrations())
