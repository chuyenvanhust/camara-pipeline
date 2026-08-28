from __future__ import annotations

import asyncio
import logging
import os
import signal

import httpx

import json

from pipeline.modules.shared.db import DatabasePool

from pipeline.dispatcher.ssrf_protection import (
    SSRFValidationError,
    sign_webhook_payload,
    validate_webhook_url,
)

logger = logging.getLogger(__name__)


class NotificationDispatcher:
    def __init__(self, db: DatabasePool):
        self.db = db
        self.batch_size = int(os.getenv("DISPATCHER_BATCH_SIZE", "50"))
        self.poll_interval = float(os.getenv("DISPATCHER_POLL_INTERVAL", "2"))
        self.max_attempts = int(os.getenv("DISPATCHER_MAX_ATTEMPTS", "5"))
        self.running = True
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=3, read=10, write=5, pool=5),
            limits=httpx.Limits(max_connections=self.batch_size, max_keepalive_connections=20),
            follow_redirects=False,
        )

    def stop(self) -> None:
        self.running = False

    async def dispatch_one(self, row) -> None:
        error = "callback failed"
        callback_url = str(row["callback_url"])
        try:
            # P1 Security: SSRF & DNS Rebinding validation prior to HTTP POST dispatch
            validated_url, _resolved_ip = validate_webhook_url(callback_url)
            payload_bytes = json.dumps(row["payload"]).encode("utf-8")
            signature = sign_webhook_payload(payload_bytes)
            headers = {
                "Content-Type": "application/json",
                "Idempotency-Key": str(row["event_id"]),
                "X-Signature-SHA256": signature,
                "User-Agent": "CAMARA-NotificationDispatcher/1.0",
            }
            response = await self.client.post(
                validated_url,
                content=payload_bytes,
                headers=headers,
            )
            if response.status_code in {200, 201, 202, 204}:
                await self.db.mark_notification_sent(row["id"])
                return
            error = f"callback returned HTTP {response.status_code}"
        except SSRFValidationError as exc:
            error = f"SSRF Blocked: {exc}"
            logger.warning("SSRF security policy blocked webhook dispatch notification=%s url=%s: %s", row["id"], callback_url, exc)
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            error = f"{type(exc).__name__}: {exc}"
        except Exception as exc:
            logger.exception("Unexpected callback failure notification=%s", row["id"])
            error = f"{type(exc).__name__}: {exc}"
        await self.db.mark_notification_failed(row["id"], row["attempts"], self.max_attempts, error)

    async def run(self) -> None:
        await self.db.recover_stale_notifications()
        recovery_counter = 0
        while self.running:
            rows = await self.db.claim_notifications(self.batch_size)
            if rows:
                await asyncio.gather(*(self.dispatch_one(row) for row in rows))
            else:
                await asyncio.sleep(self.poll_interval)
            recovery_counter += 1
            if recovery_counter >= 150:
                await self.db.recover_stale_notifications()
                recovery_counter = 0

    async def close(self) -> None:
        await self.client.aclose()


async def main() -> None:
    db = DatabasePool()
    dispatcher = NotificationDispatcher(db)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, dispatcher.stop)
        except NotImplementedError:
            pass
    await db.connect()
    try:
        await dispatcher.run()
    finally:
        await dispatcher.close()
        await db.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    asyncio.run(main())
