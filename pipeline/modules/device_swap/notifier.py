# pipeline/modules/device_swap/notifier.py
import logging
from typing import Dict, Any, List
import redis.asyncio as aioredis
from pipeline.modules.shared.notification import send_callback
from pipeline.modules.shared.db import DatabasePool

logger = logging.getLogger(__name__)


class DeviceSwapNotifier:
    def __init__(self, db: DatabasePool, redis_client: aioredis.Redis):
        self.db = db
        self.redis = redis_client

    async def notify_subscriptions(
        self,
        msisdn: str,
        imei_old: str,
        imei_new: str,
        event_time: str,
    ):
        subscriptions = await self.db.get_active_subscriptions(msisdn, "DEVICE_SWAP")
        if not subscriptions:
            logger.debug(f"No active DEVICE_SWAP subscriptions found for msisdn {msisdn}")
            return

        payload = {
            "msisdn": msisdn,
            "imei_old": imei_old,
            "imei_new": imei_new,
            "event_time": event_time,
        }

        for sub in subscriptions:
            callback_url = sub["callback_url"]
            sub_id = sub["subscription_id"]

            logger.info(f"Sending DEVICE_SWAP notification for {msisdn} to {callback_url}")

            # Ghi notification_log
            log_id = await self.db.insert_notification_log(
                subscription_id=sub_id,
                event_type="DEVICE_SWAP",
                payload=payload,
                status="PENDING",
            )

            success = await send_callback(callback_url, payload, max_retries=3)
            if success:
                logger.info(f"Successfully notified {callback_url}")
            else:
                logger.warning(f"Failed to notify {callback_url}, adding to retry queue")
                # Đưa vào Redis retry queue
                queue_item = {
                    "log_id": log_id,
                    "callback_url": callback_url,
                    "payload": payload,
                    "attempt": 1,
                }
                await self.redis.rpush("retry:device_swap", str(queue_item))
