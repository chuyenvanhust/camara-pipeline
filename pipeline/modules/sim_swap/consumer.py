# pipeline/modules/sim_swap/consumer.py
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from pipeline.modules.shared.base_consumer import BaseKafkaConsumer
from pipeline.modules.sim_swap.notifier import SimSwapNotifier

logger = logging.getLogger(__name__)


class SimSwapConsumer(BaseKafkaConsumer):
    def __init__(
        self,
        topic: str = os.getenv("KAFKA_TOPIC_RAW", "radius.accounting.raw"),
        group_id: str = "cg-sim-swap",
    ):
        super().__init__(topic=topic, group_id=group_id)
        self.notifier: Optional[SimSwapNotifier] = None

    async def initialize(self):
        await super().initialize()
        self.notifier = SimSwapNotifier(self.db, self.redis)

    @staticmethod
    def _sim_cache_key(msisdn: str) -> str:
        return f"sim:{msisdn}"

    @staticmethod
    def _parse_event_time(message: Dict[str, Any]) -> datetime:
        event_time = message.get("timestamp") or message.get("event_timestamp")
        dt = datetime.now(timezone.utc)
        try:
            if event_time and "T" in str(event_time):
                dt = datetime.fromisoformat(str(event_time).replace("Z", "+00:00"))
        except Exception:
            pass
        return dt

    async def process_message(self, message: Dict[str, Any]):
        """Fallback single-message processing (backward compatible)."""
        msisdn = message.get("Calling-StationId") or message.get("Calling_Station_Id") or message.get("msisdn")
        imsi_new = message.get("imsi")
        if not msisdn or not imsi_new:
            self.metrics.increment("ignored")
            return

        cache_key = self._sim_cache_key(msisdn)
        cached_val = await self.redis.get(cache_key)
        imsi_old = None
        if cached_val:
            try:
                data = json.loads(cached_val)
                imsi_old = data.get("imsi_current")
            except Exception:
                pass
        if imsi_old is None:
            imsi_old = await self.db.get_current_imsi(msisdn)

        if imsi_old is None:
            await self.db.upsert_sim_state(msisdn, imsi_new)
            await self.redis.set(cache_key, json.dumps({"imsi_current": imsi_new}))
            self.metrics.increment("ignored")
            return

        if imsi_old == imsi_new:
            self.metrics.increment("ignored")
            return

        logger.info(f"[SIM Swap Detected] MSISDN: {msisdn} | IMSI Old: {imsi_old} -> New: {imsi_new}")
        self.metrics.increment("events_detected")
        dt = self._parse_event_time(message)
        await self.db.upsert_sim_state(msisdn, imsi_new)
        await self.db.record_sim_swap_history(msisdn, imsi_old, imsi_new, dt)
        await self.redis.set(cache_key, json.dumps({"imsi_current": imsi_new, "last_time_sim_change": dt.isoformat()}))

        # Chỉ ghi audit log cho sự kiện Swap
        await self.db.insert_audit_log(
            event_type="SIM_SWAP", msisdn=msisdn,
            details={"imsi_old": imsi_old, "imsi_new": imsi_new, "last_time_sim_change": dt.isoformat()},
        )

        if self.notifier:
            await self.notifier.notify_subscriptions(
                msisdn=msisdn, last_time_sim_change=dt.isoformat(),
            )

    async def process_batch(self, messages: List[Dict[str, Any]]):
        """
        Batch processing — Tầng 1 Optimization.
        1. Redis MGET tất cả MSISDN cùng lúc
        2. Postgres batch SELECT cho cache miss
        3. Phân loại: init / unchanged / swap
        4. Batch UPSERT + COPY INSERT + Redis MSET
        5. Audit log chỉ cho sự kiện Swap
        """
        # ── Step 1: Extract & filter valid messages ──
        valid_msgs = []
        for msg in messages:
            msisdn = msg.get("Calling-StationId") or msg.get("Calling_Station_Id") or msg.get("msisdn")
            imsi_new = msg.get("imsi")
            if not msisdn or not imsi_new:
                self.metrics.increment("ignored")
                continue
            valid_msgs.append((msisdn, imsi_new, msg))

        if not valid_msgs:
            return

        # ── Step 2: Redis MGET — 1 round-trip cho tất cả MSISDN ──
        unique_msisdns = list({m[0] for m in valid_msgs})
        cache_keys = [self._sim_cache_key(m) for m in unique_msisdns]
        cached_values = await self.redis.mget(cache_keys)

        redis_state: Dict[str, Optional[str]] = {}
        db_miss_msisdns: List[str] = []

        for msisdn, cached_val in zip(unique_msisdns, cached_values):
            if cached_val:
                try:
                    data = json.loads(cached_val)
                    redis_state[msisdn] = data.get("imsi_current")
                    continue
                except Exception:
                    pass
            db_miss_msisdns.append(msisdn)

        # ── Step 3: Postgres batch SELECT cho cache miss — 1 round-trip ──
        if db_miss_msisdns:
            db_results = await self.db.batch_get_current_imsi(db_miss_msisdns)
            redis_state.update(db_results)

        # ── Step 4: Phân loại messages ──
        init_records: List[tuple] = []
        swap_records: List[tuple] = []
        swap_upserts: List[tuple] = []
        swap_audit: List[tuple] = []
        swap_notify: List[tuple] = []
        cache_updates: Dict[str, str] = {}

        for msisdn, imsi_new, msg in valid_msgs:
            imsi_old = redis_state.get(msisdn)

            if imsi_old is None:
                init_records.append((msisdn, imsi_new))
                cache_updates[self._sim_cache_key(msisdn)] = json.dumps({"imsi_current": imsi_new})
                redis_state[msisdn] = imsi_new
                self.metrics.increment("ignored")
                continue

            if imsi_old == imsi_new:
                self.metrics.increment("ignored")
                continue

            # ── SIM Swap Detected ──
            dt = self._parse_event_time(msg)
            logger.info(f"[SIM Swap Detected] MSISDN: {msisdn} | IMSI Old: {imsi_old} -> New: {imsi_new}")
            self.metrics.increment("events_detected")

            swap_upserts.append((msisdn, imsi_new))
            swap_records.append((msisdn, imsi_old, imsi_new, dt))
            swap_audit.append(("SIM_SWAP", msisdn, json.dumps({
                "imsi_old": imsi_old, "imsi_new": imsi_new, "last_time_sim_change": dt.isoformat(),
            })))
            swap_notify.append((msisdn, dt.isoformat()))
            cache_updates[self._sim_cache_key(msisdn)] = json.dumps({
                "imsi_current": imsi_new, "last_time_sim_change": dt.isoformat(),
            })
            redis_state[msisdn] = imsi_new

        # ── Step 5: Batch writes ──
        all_upserts = init_records + swap_upserts
        if all_upserts:
            await self.db.batch_upsert_sim_state(all_upserts)

        if swap_records:
            await self.db.batch_insert_sim_swap_history(swap_records)

        # Audit log — CHỈ cho sự kiện Swap
        if swap_audit:
            await self.db.batch_insert_audit_logs(swap_audit)

        if cache_updates:
            await self.redis.mset(cache_updates)

        # Notifications
        if self.notifier and swap_notify:
            for msisdn, last_time in swap_notify:
                try:
                    await self.notifier.notify_subscriptions(
                        msisdn=msisdn, last_time_sim_change=last_time,
                    )
                except Exception as exc:
                    logger.error(f"[{self.group_id}] Notification error for {msisdn}: {exc}")

        self.metrics.increment("success", len(valid_msgs))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    consumer = SimSwapConsumer()
    import asyncio
    asyncio.run(consumer.run())
