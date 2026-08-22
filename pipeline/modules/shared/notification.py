# pipeline/modules/shared/notification.py
import asyncio
import json
import logging
from typing import Dict, Any, Optional
import httpx
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)


async def send_callback(
    callback_url: str,
    payload: Dict[str, Any],
    max_retries: int = 5,
    base_backoff: float = 1.0,
    max_backoff: float = 60.0,
) -> bool:
    """
    Gửi HTTP POST callback với exponential backoff.
    """
    async with httpx.AsyncClient(timeout=10.0) as client:
        for attempt in range(1, max_retries + 1):
            try:
                response = await client.post(callback_url, json=payload)
                if response.status_code in (200, 201, 202, 204):
                    logger.info(f"Callback successful to {callback_url} on attempt {attempt}")
                    return True
                else:
                    logger.warning(
                        f"Callback to {callback_url} returned HTTP {response.status_code} "
                        f"(attempt {attempt}/{max_retries})"
                    )
            except Exception as exc:
                logger.warning(
                    f"Callback error to {callback_url}: {exc} (attempt {attempt}/{max_retries})"
                )

            if attempt < max_retries:
                backoff = min(base_backoff * (2 ** (attempt - 1)), max_backoff)
                await asyncio.sleep(backoff)

    return False


class NotificationRetryWorker:
    """
    Worker xử lý hàng đợi retry notification trong Redis.
    """
    def __init__(self, redis_client: aioredis.Redis, queue_name: str = "notification_retry_queue"):
        self.redis = redis_client
        self.queue_name = queue_name
        self._running = False

    async def enqueue(self, callback_url: str, payload: Dict[str, Any], event_type: str):
        item = {
            "callback_url": callback_url,
            "payload": payload,
            "event_type": event_type,
            "attempt": 1,
        }
        await self.redis.rpush(self.queue_name, json.dumps(item))

    async def start(self):
        self._running = True
        logger.info(f"Notification retry worker started for queue '{self.queue_name}'")
        while self._running:
            try:
                raw_item = await self.redis.blpop(self.queue_name, timeout=5)
                if not raw_item:
                    continue

                _, data = raw_item
                item = json.loads(data)
                url = item["callback_url"]
                payload = item["payload"]
                attempt = item.get("attempt", 1)

                success = await send_callback(url, payload, max_retries=1)
                if not success and attempt < 5:
                    item["attempt"] = attempt + 1
                    # Sleep trước khi re-queue
                    await asyncio.sleep(min(2 ** attempt, 30))
                    await self.redis.rpush(self.queue_name, json.dumps(item))
                elif not success:
                    logger.error(f"Callback failed permanently for {url} after {attempt} attempts")

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"Error in NotificationRetryWorker: {exc}")
                await asyncio.sleep(1)

    def stop(self):
        self._running = False
