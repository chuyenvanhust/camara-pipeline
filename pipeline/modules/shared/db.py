from __future__ import annotations

import logging
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

import asyncpg


logger = logging.getLogger(__name__)
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:camara@camara-postgres:5432/camara_db",
)

StateRecord = Tuple[str, str, datetime, str, int, int]
HistoryRecord = Tuple[str, str, int, int, str, Optional[str], str, datetime]
AuditRecord = Tuple[str, str, str, str, datetime]
OutboxEvent = Tuple[str, str, str, str]
SessionRecord = Tuple[str, str, Optional[str], bool, datetime, str, int, int]


class DatabasePool:
    def __init__(self, dsn: str = DATABASE_URL):
        self.dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None
        self.metrics: Optional[Any] = None

    def set_metrics(self, metrics: Any) -> None:
        self.metrics = metrics

    @asynccontextmanager
    async def acquire(self, timeout: float = 3.0, metrics: Optional[Any] = None):
        assert self.pool is not None
        start = time.monotonic()
        async with self.pool.acquire(timeout=timeout) as connection:
            acquired_in = time.monotonic() - start
            m = metrics or getattr(self, "metrics", None)
            if m is not None and hasattr(m, "observe_db_pool_acquire"):
                m.observe_db_pool_acquire(acquired_in)
            yield connection

    async def connect(self) -> None:
        if self.pool is None:
            minimum = int(os.getenv("DB_POOL_MIN", "6"))
            maximum = int(os.getenv("DB_POOL_MAX", "32"))
            if minimum < 1 or maximum < minimum:
                raise ValueError("invalid DB_POOL_MIN/DB_POOL_MAX")
            self.pool = await asyncpg.create_pool(
                dsn=self.dsn,
                min_size=minimum,
                max_size=maximum,
                command_timeout=30,
                timeout=10,
            )
            logger.info("Database pool initialized min=%d max=%d", minimum, maximum)

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()
            self.pool = None

    async def _fetch_state(
        self, table: str, value_column: str, msisdns: Sequence[str], metrics: Optional[Any] = None
    ) -> Dict[str, Dict[str, Any]]:
        if not msisdns:
            return {}
        query = f"""
            SELECT msisdn, {value_column} AS value, last_event_at, last_event_id,
                   last_source_partition, last_source_offset
            FROM {table}
            WHERE msisdn = ANY($1::text[])
        """
        async with self.acquire(timeout=3, metrics=metrics) as connection:
            rows = await connection.fetch(query, list(msisdns))
        return {row["msisdn"]: dict(row) for row in rows}

    async def batch_get_device_state(self, msisdns: Sequence[str], metrics: Optional[Any] = None):
        return await self._fetch_state("msisdn_device", "imei_current", msisdns, metrics=metrics)

    async def batch_get_sim_state(self, msisdns: Sequence[str], metrics: Optional[Any] = None):
        return await self._fetch_state("msisdn_sim", "imsi_current", msisdns, metrics=metrics)

    async def _upsert_state(
        self,
        connection: asyncpg.Connection,
        table: str,
        value_column: str,
        records: Sequence[StateRecord],
    ) -> None:
        if not records:
            return
        query = f"""
            INSERT INTO {table}(
                msisdn, {value_column}, updated_at, last_event_at, last_event_id,
                last_source_partition, last_source_offset
            )
            SELECT msisdn, value, NOW(), event_time, event_id, partition, offset_value
            FROM UNNEST(
                $1::text[], $2::text[], $3::timestamptz[], $4::text[],
                $5::int[], $6::bigint[]
            ) incoming(msisdn, value, event_time, event_id, partition, offset_value)
            ON CONFLICT(msisdn) DO UPDATE SET
                {value_column}=EXCLUDED.{value_column}, updated_at=NOW(),
                last_event_at=EXCLUDED.last_event_at,
                last_event_id=EXCLUDED.last_event_id,
                last_source_partition=EXCLUDED.last_source_partition,
                last_source_offset=EXCLUDED.last_source_offset
            WHERE (EXCLUDED.last_event_at, EXCLUDED.last_source_partition,
                   EXCLUDED.last_source_offset)
                > ({table}.last_event_at, {table}.last_source_partition,
                   {table}.last_source_offset)
        """
        columns = list(zip(*records))
        await connection.execute(query, *(list(column) for column in columns))

    @staticmethod
    async def _insert_history(
        connection: asyncpg.Connection,
        table: str,
        old_column: str,
        new_column: str,
        records: Sequence[HistoryRecord],
    ) -> None:
        if not records:
            return
        columns = list(zip(*records))
        query = f"""
            INSERT INTO {table}(
                event_id, source_topic, source_partition, source_offset,
                msisdn, {old_column}, {new_column}, changed_at
            )
            SELECT event_id, topic, partition, offset_val, msisdn, old_val, new_val, changed_at
            FROM UNNEST(
                $1::text[], $2::text[], $3::int[], $4::bigint[],
                $5::text[], $6::text[], $7::text[], $8::timestamptz[]
            ) incoming(event_id, topic, partition, offset_val, msisdn, old_val, new_val, changed_at)
            ON CONFLICT(event_id) DO NOTHING
        """
        await connection.execute(query, *(list(c) for c in columns))

    @staticmethod
    async def _insert_audit(
        connection: asyncpg.Connection, records: Sequence[AuditRecord]
    ) -> None:
        if not records:
            return
        columns = list(zip(*records))
        query = """
            INSERT INTO audit_log(event_id, event_type, msisdn, details, event_time)
            SELECT event_id, event_type, msisdn, details::jsonb, event_time
            FROM UNNEST(
                $1::text[], $2::varchar(32)[], $3::varchar(16)[],
                $4::text[], $5::timestamptz[]
            ) incoming(event_id, event_type, msisdn, details, event_time)
            ON CONFLICT(event_id, event_type) DO NOTHING
        """
        await connection.execute(query, *(list(c) for c in columns))

    @staticmethod
    async def _insert_outbox(
        connection: asyncpg.Connection, events: Sequence[OutboxEvent]
    ) -> None:
        if not events:
            return
        columns = list(zip(*events))
        query = """
            INSERT INTO notification_log(
                event_id, subscription_id, event_type, payload, status,
                attempts, next_retry_at
            )
            SELECT incoming.event_id, sub.subscription_id, incoming.event_type,
                   incoming.payload::jsonb, 'PENDING', 0, NOW()
            FROM UNNEST(
                $1::text[], $2::varchar(32)[], $3::varchar(16)[], $4::text[]
            ) incoming(event_id, event_type, msisdn, payload)
            JOIN subscription sub
              ON (sub.msisdn = incoming.msisdn OR sub.msisdn IS NULL)
             AND sub.event_type = incoming.event_type
             AND sub.status = 'ACTIVE'
             AND (sub.expires_at IS NULL OR sub.expires_at > NOW())
            ON CONFLICT(event_id, subscription_id) DO NOTHING
        """
        await connection.execute(query, *(list(c) for c in columns))

    async def _persist_swap_batch(
        self,
        table: str,
        value_column: str,
        history_table: str,
        old_column: str,
        new_column: str,
        states: Sequence[StateRecord],
        history: Sequence[HistoryRecord],
        audit: Sequence[AuditRecord],
        outbox: Sequence[OutboxEvent],
    ) -> None:
        if not (states or history or audit or outbox):
            return
        assert self.pool is not None
        async with self.acquire(timeout=3) as connection:
            async with connection.transaction():
                await self._upsert_state(connection, table, value_column, states)
                await self._insert_history(
                    connection, history_table, old_column, new_column, history
                )
                await self._insert_audit(connection, audit)
                await self._insert_outbox(connection, outbox)

    async def persist_sim_batch(self, states, history, audit, outbox) -> None:
        await self._persist_swap_batch(
            "msisdn_sim", "imsi_current", "sim_swap_history", "imsi_old",
            "imsi_new", states, history, audit, outbox
        )

    async def persist_device_batch(self, states, history, audit, outbox) -> None:
        await self._persist_swap_batch(
            "msisdn_device", "imei_current", "device_swap_history", "imei_old",
            "imei_new", states, history, audit, outbox
        )

    async def persist_session_batch(self, records: Sequence[SessionRecord]) -> None:
        if not records:
            return
        assert self.pool is not None
        columns = list(zip(*records))
        async with self.acquire(timeout=3) as connection:
            await connection.execute(
                """
                INSERT INTO radius_session_state(
                    acct_session_id,msisdn,nas_identifier,active,last_event_at,
                    last_event_id,source_partition,source_offset,updated_at
                )
                SELECT session_id,msisdn,nas,active,event_time,event_id,
                       partition,offset_value,NOW()
                FROM UNNEST(
                    $1::text[],$2::text[],$3::text[],$4::boolean[],
                    $5::timestamptz[],$6::text[],$7::int[],$8::bigint[]
                ) incoming(session_id,msisdn,nas,active,event_time,event_id,
                           partition,offset_value)
                ON CONFLICT(acct_session_id) DO UPDATE SET
                    msisdn=EXCLUDED.msisdn,nas_identifier=EXCLUDED.nas_identifier,
                    active=EXCLUDED.active,last_event_at=EXCLUDED.last_event_at,
                    last_event_id=EXCLUDED.last_event_id,
                    source_partition=EXCLUDED.source_partition,
                    source_offset=EXCLUDED.source_offset,updated_at=NOW()
                WHERE (EXCLUDED.last_event_at,EXCLUDED.source_partition,
                       EXCLUDED.source_offset)
                    > (radius_session_state.last_event_at,
                       radius_session_state.source_partition,
                       radius_session_state.source_offset)
                  AND EXCLUDED.last_event_id IS DISTINCT FROM radius_session_state.last_event_id
                """,
                *(list(column) for column in columns),
            )

    async def mark_nas_sessions_inactive(
        self, nas_identifier: str, event_time: datetime
    ) -> None:
        assert self.pool is not None
        async with self.acquire(timeout=3) as connection:
            await connection.execute(
                """
                UPDATE radius_session_state
                SET active=FALSE,last_event_at=$2,updated_at=NOW()
                WHERE nas_identifier=$1 AND active AND last_event_at <= $2
                """,
                nas_identifier,
                event_time,
            )

    async def recover_stale_notifications(self) -> None:
        assert self.pool is not None
        async with self.acquire(timeout=3) as connection:
            await connection.execute(
                """
                UPDATE notification_log
                SET status='FAILED',locked_at=NULL,next_retry_at=NOW(),
                    error_detail='recovered stale claim'
                WHERE status='IN_PROGRESS'
                  AND locked_at < NOW() - INTERVAL '5 minutes'
                """
            )

    async def claim_notifications(self, limit: int) -> List[Dict[str, Any]]:
        assert self.pool is not None
        async with self.acquire(timeout=3) as connection:
            async with connection.transaction():
                rows = await connection.fetch(
                    """
                    WITH picked AS (
                        SELECT nl.id
                        FROM notification_log nl
                        WHERE nl.status IN ('PENDING','FAILED')
                          AND (nl.next_retry_at IS NULL OR nl.next_retry_at <= NOW())
                        ORDER BY nl.created_at
                        FOR UPDATE SKIP LOCKED LIMIT $1
                    )
                    UPDATE notification_log nl
                    SET status='IN_PROGRESS',locked_at=NOW(),attempts=attempts+1
                    FROM picked, subscription s
                    WHERE nl.id=picked.id AND s.subscription_id=nl.subscription_id
                    RETURNING nl.id,nl.event_id,nl.payload,nl.attempts,s.callback_url
                    """,
                    limit,
                )
        return [dict(row) for row in rows]

    async def mark_notification_sent(self, notification_id: int) -> None:
        await self._update_notification(
            notification_id,
            "status='SENT',last_attempt_at=NOW(),locked_at=NULL,error_detail=NULL",
        )

    async def mark_notification_failed(
        self, notification_id: int, attempts: int, max_attempts: int, error: str
    ) -> None:
        assert self.pool is not None
        status = "DEAD" if attempts >= max_attempts else "FAILED"
        delay = min(2 ** max(attempts, 1), 300)
        async with self.acquire(timeout=3) as connection:
            await connection.execute(
                """
                UPDATE notification_log
                SET status=$2,last_attempt_at=NOW(),locked_at=NULL,
                    next_retry_at=NOW()+($3*INTERVAL '1 second'),error_detail=$4
                WHERE id=$1
                """,
                notification_id,
                status,
                delay,
                error[:2000],
            )

    async def _update_notification(self, notification_id: int, assignment: str) -> None:
        assert self.pool is not None
        async with self.acquire(timeout=3) as connection:
            await connection.execute(
                f"UPDATE notification_log SET {assignment} WHERE id=$1",
                notification_id,
            )
