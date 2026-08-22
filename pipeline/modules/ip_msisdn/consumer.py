# pipeline/modules/ip_msisdn/consumer.py
import json
import logging
import os
from typing import Dict, Any, List

from pipeline.modules.shared.base_consumer import BaseKafkaConsumer
from pipeline.modules.ip_msisdn.redis_store import IPMappingStore

logger = logging.getLogger(__name__)

SESSION_TTL_SECONDS = 86400  # 24h


class IPMsisdnConsumer(BaseKafkaConsumer):
    def __init__(
        self,
        topic: str = os.getenv("KAFKA_TOPIC_RAW", "radius.accounting.raw"),
        group_id: str = "cg-ip-msisdn",
    ):
        super().__init__(topic=topic, group_id=group_id)
        self.store: IPMappingStore = None

    async def initialize(self):
        await super().initialize()
        self.store = IPMappingStore(self.redis)

    async def process_message(self, message: Dict[str, Any]):
        """Fallback single-message processing (backward compatible)."""
        framed_ip = message.get("Framed_IP_Address") or message.get("framed_ip")
        msisdn = message.get("Calling-StationId") or message.get("Calling_Station_Id") or message.get("msisdn")
        nas_id = message.get("NAS-Identifier") or message.get("NAS_Identifier") or message.get("nas_identifier")
        status_type = message.get("acct_status_type", "")
        timestamp = message.get("timestamp") or message.get("event_timestamp", "")

        if not status_type:
            self.metrics.increment("ignored")
            return

        status_norm = status_type.strip().lower()

        if status_norm in ("start", "interim-update", "interim_update"):
            if not framed_ip or not msisdn:
                self.metrics.increment("ignored")
                return
            await self.store.upsert_mapping(
                framed_ip=framed_ip, msisdn=msisdn, timestamp=timestamp, nas_identifier=nas_id,
            )
            self.metrics.increment("events_detected")

        elif status_norm == "stop":
            if not framed_ip or not msisdn:
                self.metrics.increment("ignored")
                return
            await self.store.delete_mapping(
                framed_ip=framed_ip, msisdn=msisdn, nas_identifier=nas_id,
            )
            self.metrics.increment("events_detected")

        elif status_norm in ("accounting-off", "accounting_off"):
            if not nas_id:
                self.metrics.increment("ignored")
                return
            await self.store.accounting_off(nas_identifier=nas_id)
            self.metrics.increment("events_detected")

        else:
            self.metrics.increment("ignored")

    async def process_batch(self, messages: List[Dict[str, Any]]):
        """
        Batch processing — Tầng 1 Optimization.
        Gom tất cả Start/Interim/Stop thành batch Redis pipeline operations.
        """
        upsert_ops = []   # (framed_ip, msisdn, timestamp, nas_id)
        delete_ops = []   # (framed_ip, msisdn, nas_id)
        acct_off_ops = [] # (nas_id,)

        for msg in messages:
            framed_ip = msg.get("Framed_IP_Address") or msg.get("framed_ip")
            msisdn = msg.get("Calling-StationId") or msg.get("Calling_Station_Id") or msg.get("msisdn")
            nas_id = msg.get("NAS-Identifier") or msg.get("NAS_Identifier") or msg.get("nas_identifier")
            status_type = msg.get("acct_status_type", "")
            timestamp = msg.get("timestamp") or msg.get("event_timestamp", "")

            if not status_type:
                self.metrics.increment("ignored")
                continue

            status_norm = status_type.strip().lower()

            if status_norm in ("start", "interim-update", "interim_update"):
                if not framed_ip or not msisdn:
                    self.metrics.increment("ignored")
                    continue
                upsert_ops.append((framed_ip, msisdn, timestamp, nas_id))
                self.metrics.increment("events_detected")

            elif status_norm == "stop":
                if not framed_ip or not msisdn:
                    self.metrics.increment("ignored")
                    continue
                delete_ops.append((framed_ip, msisdn, nas_id))
                self.metrics.increment("events_detected")

            elif status_norm in ("accounting-off", "accounting_off"):
                if not nas_id:
                    self.metrics.increment("ignored")
                    continue
                acct_off_ops.append(nas_id)
                self.metrics.increment("events_detected")

            else:
                self.metrics.increment("ignored")

        # ── Batch Redis operations ──

        # 1. Batch UPSERT (Start / Interim-Update) — 1 Redis pipeline
        if upsert_ops:
            async with self.redis.pipeline(transaction=False) as pipe:
                for framed_ip, msisdn, timestamp, nas_id in upsert_ops:
                    ip_key = IPMappingStore._ip_key(framed_ip)
                    payload = json.dumps({"msisdn": msisdn, "timestamp": timestamp})
                    pipe.set(ip_key, payload, ex=SESSION_TTL_SECONDS)
                    if nas_id:
                        ggsn_key = IPMappingStore._ggsn_key(nas_id)
                        pipe.sadd(ggsn_key, framed_ip)
                        pipe.expire(ggsn_key, SESSION_TTL_SECONDS)
                await pipe.execute()

        # 2. Batch DELETE (Stop) — need to check ownership first
        if delete_ops:
            # 2a. Batch MGET to check ownership
            ip_keys_to_check = [IPMappingStore._ip_key(framed_ip) for framed_ip, _, _ in delete_ops]
            existing_values = await self.redis.mget(ip_keys_to_check)

            # 2b. Filter: only delete if MSISDN matches
            async with self.redis.pipeline(transaction=False) as pipe:
                for (framed_ip, msisdn, nas_id), existing_val in zip(delete_ops, existing_values):
                    if existing_val:
                        try:
                            data = json.loads(existing_val)
                            if data.get("msisdn") == msisdn:
                                pipe.delete(IPMappingStore._ip_key(framed_ip))
                                if nas_id:
                                    pipe.srem(IPMappingStore._ggsn_key(nas_id), framed_ip)
                        except Exception:
                            pass
                await pipe.execute()

        # 3. Accounting-Off — process sequentially (rare event, bulk delete)
        for nas_id in acct_off_ops:
            await self.store.accounting_off(nas_identifier=nas_id)

        self.metrics.increment("success", len(messages))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    consumer = IPMsisdnConsumer()
    import asyncio
    asyncio.run(consumer.run())
