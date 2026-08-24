# pipeline/modules/shared/db.py
import os
import json
import logging
from typing import Optional, List, Dict, Any, Tuple
import asyncpg

logger = logging.getLogger(__name__)

DB_HOST = os.getenv("DB_HOST", "camara-postgres")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "camara")
DB_NAME = os.getenv("DB_NAME", "camara_db")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


class DatabasePool:
    def __init__(self, dsn: str = DATABASE_URL):
        self.dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        if self.pool is None:
            # F-09: Reduced pool size — now shared across all consumers in 1 process
            self.pool = await asyncpg.create_pool(
                dsn=self.dsn,
                min_size=int(os.getenv("DB_POOL_MIN", "4")),
                max_size=int(os.getenv("DB_POOL_MAX", "12")),
                command_timeout=30,
                timeout=10,  # acquire timeout — fail rõ ràng thay vì treo vô hạn khi pool cạn
            )
            logger.info("Database connection pool initialized.")

    async def close(self):
        if self.pool is not None:
            await self.pool.close()
            self.pool = None
            logger.info("Database connection pool closed.")

    # =========================================================================
    # Single-record methods (giữ lại cho backward compatibility)
    # =========================================================================

    async def get_current_imei(self, msisdn: str) -> Optional[str]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            val = await conn.fetchval(
                "SELECT imei_current FROM msisdn_device WHERE msisdn = $1",
                msisdn
            )
            return val

    async def upsert_device_state(self, msisdn: str, imei_new: str) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO msisdn_device (msisdn, imei_current, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (msisdn)
                DO UPDATE SET imei_current = EXCLUDED.imei_current, updated_at = NOW()
                """,
                msisdn, imei_new
            )

    async def record_device_swap_history(
        self, msisdn: str, imei_old: Optional[str], imei_new: str, changed_at
    ) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO device_swap_history (msisdn, imei_old, imei_new, changed_at)
                VALUES ($1, $2, $3, $4)
                """,
                msisdn, imei_old, imei_new, changed_at
            )

    async def get_current_imsi(self, msisdn: str) -> Optional[str]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            val = await conn.fetchval(
                "SELECT imsi_current FROM msisdn_sim WHERE msisdn = $1",
                msisdn
            )
            return val

    async def upsert_sim_state(self, msisdn: str, imsi_new: str) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO msisdn_sim (msisdn, imsi_current, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (msisdn)
                DO UPDATE SET imsi_current = EXCLUDED.imsi_current, updated_at = NOW()
                """,
                msisdn, imsi_new
            )

    async def record_sim_swap_history(
        self, msisdn: str, imsi_old: Optional[str], imsi_new: str, changed_at
    ) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO sim_swap_history (msisdn, imsi_old, imsi_new, changed_at)
                VALUES ($1, $2, $3, $4)
                """,
                msisdn, imsi_old, imsi_new, changed_at
            )

    async def get_active_subscriptions(self, msisdn: str, event_type: str) -> List[Dict[str, Any]]:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT subscription_id, msisdn, event_type, callback_url, expires_at
                FROM subscription
                WHERE msisdn = $1 AND event_type = $2 AND status = 'ACTIVE'
                  AND (expires_at IS NULL OR expires_at > NOW())
                """,
                msisdn, event_type
            )
            return [dict(r) for r in rows]

    async def insert_audit_log(self, event_type: str, msisdn: Optional[str], details: Dict[str, Any]) -> None:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO audit_log (event_type, msisdn, details)
                VALUES ($1, $2, $3::jsonb)
                """,
                event_type, msisdn, json.dumps(details)
            )

    async def insert_notification_log(
        self, subscription_id: Any, event_type: str, payload: Dict[str, Any], status: str = "PENDING"
    ) -> int:
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            log_id = await conn.fetchval(
                """
                INSERT INTO notification_log (subscription_id, event_type, payload, status, attempts, last_attempt_at)
                VALUES ($1, $2, $3::jsonb, $4, 1, NOW())
                RETURNING id
                """,
                subscription_id, event_type, json.dumps(payload), status
            )
            return log_id

    # =========================================================================
    # BATCH methods — Tầng 1: Batch Processing (high throughput)
    # =========================================================================

    async def batch_get_current_imei(self, msisdns: List[str]) -> Dict[str, Optional[str]]:
        """Batch lookup IMEI hiện tại cho danh sách MSISDN. Returns {msisdn: imei_or_None}."""
        assert self.pool is not None
        result: Dict[str, Optional[str]] = {m: None for m in msisdns}
        if not msisdns:
            return result
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT msisdn, imei_current FROM msisdn_device WHERE msisdn = ANY($1::text[])",
                msisdns
            )
            for r in rows:
                result[r["msisdn"]] = r["imei_current"]
        return result

    async def batch_upsert_device_state(self, records: List[Tuple[str, str]]) -> None:
        """Batch UPSERT (msisdn, imei_new) vào msisdn_device."""
        if not records:
            return
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO msisdn_device (msisdn, imei_current, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (msisdn)
                DO UPDATE SET imei_current = EXCLUDED.imei_current, updated_at = NOW()
                """,
                records
            )

    async def batch_insert_device_swap_history(self, records: List[tuple]) -> None:
        """Batch INSERT vào device_swap_history. records: [(msisdn, imei_old, imei_new, changed_at), ...]"""
        if not records:
            return
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.copy_records_to_table(
                "device_swap_history",
                records=records,
                columns=["msisdn", "imei_old", "imei_new", "changed_at"],
            )

    async def batch_get_current_imsi(self, msisdns: List[str]) -> Dict[str, Optional[str]]:
        """Batch lookup IMSI hiện tại cho danh sách MSISDN. Returns {msisdn: imsi_or_None}."""
        assert self.pool is not None
        result: Dict[str, Optional[str]] = {m: None for m in msisdns}
        if not msisdns:
            return result
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT msisdn, imsi_current FROM msisdn_sim WHERE msisdn = ANY($1::text[])",
                msisdns
            )
            for r in rows:
                result[r["msisdn"]] = r["imsi_current"]
        return result

    async def batch_upsert_sim_state(self, records: List[Tuple[str, str]]) -> None:
        """Batch UPSERT (msisdn, imsi_new) vào msisdn_sim."""
        if not records:
            return
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO msisdn_sim (msisdn, imsi_current, updated_at)
                VALUES ($1, $2, NOW())
                ON CONFLICT (msisdn)
                DO UPDATE SET imsi_current = EXCLUDED.imsi_current, updated_at = NOW()
                """,
                records
            )

    async def batch_insert_sim_swap_history(self, records: List[tuple]) -> None:
        """Batch INSERT vào sim_swap_history. records: [(msisdn, imsi_old, imsi_new, changed_at), ...]"""
        if not records:
            return
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.copy_records_to_table(
                "sim_swap_history",
                records=records,
                columns=["msisdn", "imsi_old", "imsi_new", "changed_at"],
            )

    async def batch_insert_audit_logs(self, records: List[Tuple[str, Optional[str], str]]) -> None:
        """Batch INSERT vào audit_log. records: [(event_type, msisdn, details_json_str), ...]"""
        if not records:
            return
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            await conn.executemany(
                """
                INSERT INTO audit_log (event_type, msisdn, details)
                VALUES ($1, $2, $3::jsonb)
                """,
                records
            )

    # =========================================================================
    # F-02: ATOMIC BATCH methods — single transaction for consistency
    # =========================================================================

    async def commit_sim_swap_batch(
        self,
        upserts: List[Tuple[str, str]],
        history: List[tuple],
        audit: List[Tuple[str, Optional[str], str]],
        notification_records: Optional[List[tuple]] = None,
    ) -> None:
        """
        F-02: Ghi atomic: state (msisdn_sim) + history (sim_swap_history) + audit_log
        + notification_log trong CÙNG một transaction. Nếu bất kỳ bước nào lỗi,
        toàn bộ rollback — không bao giờ để state mới tồn tại mà thiếu history tương ứng.
        """
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                if upserts:
                    await conn.executemany(
                        """
                        INSERT INTO msisdn_sim (msisdn, imsi_current, updated_at)
                        VALUES ($1, $2, NOW())
                        ON CONFLICT (msisdn)
                        DO UPDATE SET imsi_current = EXCLUDED.imsi_current, updated_at = NOW()
                        """,
                        upserts
                    )
                if history:
                    await conn.copy_records_to_table(
                        "sim_swap_history",
                        records=history,
                        columns=["msisdn", "imsi_old", "imsi_new", "changed_at"],
                    )
                if audit:
                    await conn.executemany(
                        """
                        INSERT INTO audit_log (event_type, msisdn, details)
                        VALUES ($1, $2, $3::jsonb)
                        """,
                        audit
                    )
                # F-03: Ghi notification_log PENDING trong cùng transaction
                if notification_records:
                    for sub_id, event_type, payload_json in notification_records:
                        await conn.execute(
                            """
                            INSERT INTO notification_log
                                (subscription_id, event_type, payload, status, attempts)
                            VALUES ($1, $2, $3::jsonb, 'PENDING', 0)
                            """,
                            sub_id, event_type, payload_json,
                        )

    async def commit_device_swap_batch(
        self,
        upserts: List[Tuple[str, str]],
        history: List[tuple],
        audit: List[Tuple[str, Optional[str], str]],
        notification_records: Optional[List[tuple]] = None,
    ) -> None:
        """
        F-02: Ghi atomic: state (msisdn_device) + history (device_swap_history) + audit_log
        + notification_log trong CÙNG một transaction.
        """
        assert self.pool is not None
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                if upserts:
                    await conn.executemany(
                        """
                        INSERT INTO msisdn_device (msisdn, imei_current, updated_at)
                        VALUES ($1, $2, NOW())
                        ON CONFLICT (msisdn)
                        DO UPDATE SET imei_current = EXCLUDED.imei_current, updated_at = NOW()
                        """,
                        upserts
                    )
                if history:
                    await conn.copy_records_to_table(
                        "device_swap_history",
                        records=history,
                        columns=["msisdn", "imei_old", "imei_new", "changed_at"],
                    )
                if audit:
                    await conn.executemany(
                        """
                        INSERT INTO audit_log (event_type, msisdn, details)
                        VALUES ($1, $2, $3::jsonb)
                        """,
                        audit
                    )
                # F-03: Ghi notification_log PENDING trong cùng transaction
                if notification_records:
                    for sub_id, event_type, payload_json in notification_records:
                        await conn.execute(
                            """
                            INSERT INTO notification_log
                                (subscription_id, event_type, payload, status, attempts)
                            VALUES ($1, $2, $3::jsonb, 'PENDING', 0)
                            """,
                            sub_id, event_type, payload_json,
                        )
