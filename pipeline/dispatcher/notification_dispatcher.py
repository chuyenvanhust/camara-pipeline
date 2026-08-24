# pipeline/dispatcher/notification_dispatcher.py
"""
F-03: Notification Dispatcher — Outbox Pattern

Dispatcher độc lập: poll notification_log cho status IN ('PENDING', 'FAILED'),
claim batch bằng FOR UPDATE SKIP LOCKED (nhiều instance chạy song song an toàn),
gửi HTTP callback, retry với exponential backoff, đánh status DEAD sau max attempts.

Chạy như process/task riêng, KHÔNG chạy chung với consumer.

Usage:
    python -m pipeline.dispatcher.notification_dispatcher

Environment:
    DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME — kết nối Postgres
    DISPATCHER_BATCH_SIZE (default: 50)
    DISPATCHER_POLL_INTERVAL (default: 2.0s)
    DISPATCHER_MAX_ATTEMPTS (default: 5)
"""

import asyncio
import logging
import os
import sys
import signal

import httpx

# Bootstrap path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from pipeline.modules.shared.db import DatabasePool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

BATCH_SIZE = int(os.getenv("DISPATCHER_BATCH_SIZE", "50"))
POLL_INTERVAL = float(os.getenv("DISPATCHER_POLL_INTERVAL", "2.0"))
MAX_ATTEMPTS = int(os.getenv("DISPATCHER_MAX_ATTEMPTS", "5"))


class NotificationDispatcher:
    def __init__(
        self,
        db: DatabasePool,
        batch_size: int = BATCH_SIZE,
        poll_interval: float = POLL_INTERVAL,
        max_attempts: int = MAX_ATTEMPTS,
    ):
        self.db = db
        self.batch_size = batch_size
        self.poll_interval = poll_interval
        self.max_attempts = max_attempts
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=3.0, read=10.0, write=5.0, pool=5.0)
        )
        self.running = False

    async def _claim_batch(self):
        """
        Claim a batch of pending/failed notifications using FOR UPDATE SKIP LOCKED.
        This allows multiple dispatcher instances to run in parallel safely.
        """
        assert self.db.pool is not None
        async with self.db.pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    """
                    SELECT nl.id, nl.subscription_id, nl.event_type, nl.payload,
                           nl.attempts, s.callback_url
                    FROM notification_log nl
                    JOIN subscription s ON s.subscription_id = nl.subscription_id
                    WHERE nl.status IN ('PENDING', 'FAILED')
                      AND (nl.next_retry_at IS NULL OR nl.next_retry_at <= NOW())
                    ORDER BY nl.created_at
                    FOR UPDATE OF nl SKIP LOCKED
                    LIMIT $1
                    """,
                    self.batch_size,
                )
                if rows:
                    await conn.executemany(
                        "UPDATE notification_log SET status = 'IN_PROGRESS' WHERE id = $1",
                        [(r["id"],) for r in rows],
                    )
                return rows

    async def _dispatch_one(self, row):
        """Send HTTP callback for a single notification row."""
        callback_url = row["callback_url"]
        log_id = row["id"]
        payload = row["payload"]

        ok = False
        try:
            # payload is already a dict from JSONB
            resp = await self._client.post(callback_url, json=payload)
            ok = resp.status_code in (200, 201, 202, 204)
            if not ok:
                logger.warning(
                    f"Callback HTTP {resp.status_code} cho log_id={log_id} -> {callback_url}"
                )
        except Exception as exc:
            logger.warning(f"Callback lỗi cho log_id={log_id}: {exc}")

        assert self.db.pool is not None
        async with self.db.pool.acquire() as conn:
            if ok:
                await conn.execute(
                    """
                    UPDATE notification_log
                    SET status='SENT', attempts=attempts+1, last_attempt_at=NOW()
                    WHERE id=$1
                    """,
                    log_id,
                )
                logger.info(f"Notification log_id={log_id} sent successfully to {callback_url}")
            else:
                attempts = row["attempts"] + 1
                if attempts >= self.max_attempts:
                    await conn.execute(
                        """
                        UPDATE notification_log
                        SET status='DEAD', attempts=$2, last_attempt_at=NOW()
                        WHERE id=$1
                        """,
                        log_id, attempts,
                    )
                    logger.error(
                        f"Notification log_id={log_id} marked DEAD after {attempts} attempts"
                    )
                else:
                    backoff_s = min(2 ** attempts, 60)
                    await conn.execute(
                        """
                        UPDATE notification_log
                        SET status='FAILED', attempts=$2, last_attempt_at=NOW(),
                            next_retry_at=NOW() + ($3 || ' seconds')::interval
                        WHERE id=$1
                        """,
                        log_id, attempts, str(backoff_s),
                    )
                    logger.info(
                        f"Notification log_id={log_id} retry scheduled "
                        f"(attempt {attempts}, backoff {backoff_s}s)"
                    )

    async def start(self):
        """Main loop: poll, claim, dispatch."""
        self.running = True
        logger.info(
            f"NotificationDispatcher started | batch_size={self.batch_size} "
            f"poll_interval={self.poll_interval}s max_attempts={self.max_attempts}"
        )
        while self.running:
            try:
                rows = await self._claim_batch()
                if not rows:
                    await asyncio.sleep(self.poll_interval)
                    continue

                logger.info(f"Claimed {len(rows)} notifications to dispatch")
                await asyncio.gather(*[
                    self._dispatch_one(r) for r in rows
                ])
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"Dispatcher loop error: {exc}", exc_info=True)
                await asyncio.sleep(self.poll_interval)

    def stop(self):
        self.running = False

    async def cleanup(self):
        """Close HTTP client."""
        await self._client.aclose()


async def main():
    db = DatabasePool()
    await db.connect()

    dispatcher = NotificationDispatcher(db=db)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, dispatcher.stop)
        except NotImplementedError:
            pass

    try:
        await dispatcher.start()
    finally:
        await dispatcher.cleanup()
        await db.close()
        logger.info("NotificationDispatcher stopped.")


if __name__ == "__main__":
    asyncio.run(main())
