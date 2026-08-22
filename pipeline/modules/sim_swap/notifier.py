# pipeline/modules/sim_swap/notifier.py
import logging
from typing import Dict, Any
import redis.asyncio as aioredis
from pipeline.modules.shared.notification import send_callback
from pipeline.modules.shared.db import DatabasePool

logger = logging.getLogger(__name__)


class SimSwapNotifier:
    def __init__(self, db: DatabasePool, redis_client: aioredis.Redis):
        self.db = db
        self.redis = redis_client

    async def notify_subscriptions(
        self,
        msisdn: str,
        last_time_sim_change: str,
    ):
        subscriptions = await self.db.get_active_subscriptions(msisdn, "SIM_SWAP")
        if not subscriptions:
            logger.debug(f"No active SIM_SWAP subscriptions found for msisdn {msisdn}")
            return

        payload = {
            "MSISDN": msisdn,
            "LastTimeSIMChange": last_time_sim_change,
        }

        for sub in subscriptions:
            callback_url = sub["callback_url"]
            sub_id = sub["subscription_id"]

            logger.info(f"Sending SIM_SWAP notification for {msisdn} to {callback_url}")

            log_id = await self.db.insert_notification_log(
                subscription_id=sub_id,
                event_type="SIM_SWAP",
                payload=payload,
                status="PENDING",
            )

            success = await send_callback(callback_url, payload, max_retries=3)
            if success:
                logger.info(f"Successfully notified {callback_url}")
            else:
                logger.warning(f"Failed to notify {callback_url}, adding to retry queue")
                queue_item = {
                    "log_id": log_id,
                    "callback_url": callback_url,
                    "payload": payload,
                    "attempt": 1,
                }
                await self.redis.rpush("retry:sim_swap", str(queue_item))
