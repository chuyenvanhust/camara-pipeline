# pipeline/modules/device_swap/consumer.py
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from pipeline.modules.shared.base_consumer import BaseKafkaConsumer
from pipeline.modules.device_swap.notifier import DeviceSwapNotifier

logger = logging.getLogger(__name__)


class DeviceSwapConsumer(BaseKafkaConsumer):
    def __init__(
        self,
        topic: str = os.getenv("KAFKA_TOPIC_RAW", "radius.accounting.raw"),
        group_id: str = "cg-device-swap",
    ):
        super().__init__(topic=topic, group_id=group_id)
        self.notifier: Optional[DeviceSwapNotifier] = None

    async def initialize(self):
        await super().initialize()
        self.notifier = DeviceSwapNotifier(self.db, self.redis)

    @staticmethod
    def _device_cache_key(msisdn: str) -> str:
        return f"device:{msisdn}"

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
        imei_new = message.get("imei")
        if not msisdn or not imei_new:
            self.metrics.increment("ignored")
            return

        cache_key = self._device_cache_key(msisdn)
        cached_val = await self.redis.get(cache_key)
        imei_old = None
        if cached_val:
            try:
                data = json.loads(cached_val)
                imei_old = data.get("imei_current")
            except Exception:
                pass
        if imei_old is None:
            imei_old = await self.db.get_current_imei(msisdn)

        if imei_old is None:
            await self.db.upsert_device_state(msisdn, imei_new)
            await self.redis.set(cache_key, json.dumps({"imei_current": imei_new}))
            self.metrics.increment("ignored")
            return

        if imei_old == imei_new:
            self.metrics.increment("ignored")
            return

        logger.info(f"[Device Swap Detected] MSISDN: {msisdn} | IMEI Old: {imei_old} -> New: {imei_new}")
        self.metrics.increment("events_detected")
        dt = self._parse_event_time(message)
        await self.db.upsert_device_state(msisdn, imei_new)
        await self.db.record_device_swap_history(msisdn, imei_old, imei_new, dt)
        await self.redis.set(cache_key, json.dumps({"imei_current": imei_new}))

        # Chỉ ghi audit log cho sự kiện Swap
        await self.db.insert_audit_log(
            event_type="DEVICE_SWAP", msisdn=msisdn,
            details={"imei_old": imei_old, "imei_new": imei_new, "event_time": dt.isoformat()},
        )

        if self.notifier:
            await self.notifier.notify_subscriptions(
                msisdn=msisdn, imei_old=imei_old, imei_new=imei_new, event_time=dt.isoformat(),
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
            imei_new = msg.get("imei")
            if not msisdn or not imei_new:
                self.metrics.increment("ignored")
                continue
            valid_msgs.append((msisdn, imei_new, msg))

        if not valid_msgs:
            return

        # ── Step 2: Redis MGET — 1 round-trip cho tất cả MSISDN ──
        unique_msisdns = list({m[0] for m in valid_msgs})
        cache_keys = [self._device_cache_key(m) for m in unique_msisdns]
        cached_values = await self.redis.mget(cache_keys)

        redis_state: Dict[str, Optional[str]] = {}
        db_miss_msisdns: List[str] = []

        for msisdn, cached_val in zip(unique_msisdns, cached_values):
            if cached_val:
                try:
                    data = json.loads(cached_val)
                    redis_state[msisdn] = data.get("imei_current")
                    continue
                except Exception:
                    pass
            db_miss_msisdns.append(msisdn)

        # ── Step 3: Postgres batch SELECT cho cache miss — 1 round-trip ──
        if db_miss_msisdns:
            db_results = await self.db.batch_get_current_imei(db_miss_msisdns)
            redis_state.update(db_results)

        # ── Step 4: Phân loại messages ──
        init_records: List[tuple] = []         # (msisdn, imei) — lần đầu gặp
        swap_records: List[tuple] = []         # (msisdn, imei_old, imei_new, dt)
        swap_upserts: List[tuple] = []         # (msisdn, imei_new)
        swap_audit: List[tuple] = []           # (event_type, msisdn, details_json)
        swap_notify: List[tuple] = []          # (msisdn, imei_old, imei_new, event_time)
        cache_updates: Dict[str, str] = {}     # cache_key -> json_value

        for msisdn, imei_new, msg in valid_msgs:
            imei_old = redis_state.get(msisdn)

            if imei_old is None:
                # Khởi tạo bản ghi ban đầu
                init_records.append((msisdn, imei_new))
                cache_updates[self._device_cache_key(msisdn)] = json.dumps({"imei_current": imei_new})
                redis_state[msisdn] = imei_new  # cập nhật state cho msg tiếp theo trong batch
                self.metrics.increment("ignored")
                continue

            if imei_old == imei_new:
                self.metrics.increment("ignored")
                continue

            # ── Device Swap Detected ──
            dt = self._parse_event_time(msg)
            logger.info(f"[Device Swap Detected] MSISDN: {msisdn} | IMEI Old: {imei_old} -> New: {imei_new}")
            self.metrics.increment("events_detected")

            swap_upserts.append((msisdn, imei_new))
            swap_records.append((msisdn, imei_old, imei_new, dt))
            swap_audit.append(("DEVICE_SWAP", msisdn, json.dumps({
                "imei_old": imei_old, "imei_new": imei_new, "event_time": dt.isoformat(),
            })))
            swap_notify.append((msisdn, imei_old, imei_new, dt.isoformat()))
            cache_updates[self._device_cache_key(msisdn)] = json.dumps({"imei_current": imei_new})
            redis_state[msisdn] = imei_new  # cập nhật state cho msg tiếp theo trong batch

        # ── Step 5: Batch writes — tối thiểu round-trips ──
        # 5a. Batch UPSERT init + swap vào msisdn_device
        all_upserts = init_records + swap_upserts
        if all_upserts:
            await self.db.batch_upsert_device_state(all_upserts)

        # 5b. Batch INSERT swap history (COPY protocol)
        if swap_records:
            await self.db.batch_insert_device_swap_history(swap_records)

        # 5c. Batch INSERT audit log — CHỈ cho sự kiện Swap
        if swap_audit:
            await self.db.batch_insert_audit_logs(swap_audit)

        # 5d. Redis MSET — 1 round-trip cập nhật tất cả cache
        if cache_updates:
            await self.redis.mset(cache_updates)

        # 5e. Notifications (chạy tuần tự vì có HTTP callback)
        if self.notifier and swap_notify:
            for msisdn, imei_old, imei_new, event_time in swap_notify:
                try:
                    await self.notifier.notify_subscriptions(
                        msisdn=msisdn, imei_old=imei_old, imei_new=imei_new, event_time=event_time,
                    )
                except Exception as exc:
                    logger.error(f"[{self.group_id}] Notification error for {msisdn}: {exc}")

        self.metrics.increment("success", len(valid_msgs))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    consumer = DeviceSwapConsumer()
    import asyncio
    asyncio.run(consumer.run())
