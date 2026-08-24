# pipeline/modules/device_swap/consumer.py
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from pipeline.modules.shared.base_consumer import BaseKafkaConsumer
from pipeline.modules.shared.db import DatabasePool

logger = logging.getLogger(__name__)


class DeviceSwapConsumer(BaseKafkaConsumer):
    def __init__(
        self,
        topic: str = os.getenv("KAFKA_TOPIC_RAW", "radius.accounting.raw"),
        group_id: str = "cg-device-swap",
        db: Optional[DatabasePool] = None,
    ):
        super().__init__(topic=topic, group_id=group_id, db=db)

    async def initialize(self):
        await super().initialize()

    @staticmethod
    def _device_cache_key(msisdn: str) -> str:
        return f"device:{msisdn}"

    @staticmethod
    def _parse_event_time(message: Dict[str, Any]) -> Optional[datetime]:
        """
        F-14: Trả None nếu không parse được — KHÔNG fallback về 'now()',
        để caller quyết định (skip + log warning thay vì âm thầm tạo swap event
        với timestamp sai).
        """
        event_time = message.get("timestamp") or message.get("event_timestamp")
        if not event_time:
            return None
        try:
            return datetime.fromisoformat(str(event_time).replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None

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

        # F-14: Validate event_time — skip message nếu không parse được
        dt = self._parse_event_time(message)
        if dt is None:
            self.metrics.increment("errors")
            logger.warning(
                f"[{self.group_id}] Bỏ qua message có event_time không hợp lệ: msisdn={msisdn}"
            )
            return

        logger.info(f"[Device Swap Detected] MSISDN: {msisdn} | IMEI Old: {imei_old} -> New: {imei_new}")
        self.metrics.increment("events_detected")
        await self.db.upsert_device_state(msisdn, imei_new)
        await self.db.record_device_swap_history(msisdn, imei_old, imei_new, dt)
        await self.redis.set(cache_key, json.dumps({"imei_current": imei_new}))

        # Chỉ ghi audit log cho sự kiện Swap
        await self.db.insert_audit_log(
            event_type="DEVICE_SWAP", msisdn=msisdn,
            details={"imei_old": imei_old, "imei_new": imei_new, "event_time": dt.isoformat()},
        )

        # F-03: Ghi notification_log PENDING — KHÔNG gọi HTTP trong consumer
        subs = await self.db.get_active_subscriptions(msisdn, "DEVICE_SWAP")
        if subs:
            payload = {"msisdn": msisdn, "imei_old": imei_old, "imei_new": imei_new, "event_time": dt.isoformat()}
            for sub in subs:
                await self.db.insert_notification_log(
                    subscription_id=sub["subscription_id"],
                    event_type="DEVICE_SWAP",
                    payload=payload,
                    status="PENDING",
                )

    async def process_batch(self, messages: List[Dict[str, Any]]):
        """
        Batch processing — Tầng 1 Optimization.
        F-02: Atomic transaction cho tất cả DB writes.
        F-03: Notification chỉ ghi vào notification_log, không gọi HTTP.
        F-14: Validate event_time, skip message không hợp lệ.
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
        notification_records: List[tuple] = [] # F-03: (sub_id, event_type, payload_json)
        cache_updates: Dict[str, str] = {}     # cache_key -> json_value

        swap_msisdns_for_notify: List[str] = []

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

            # F-14: Validate event_time
            dt = self._parse_event_time(msg)
            if dt is None:
                self.metrics.increment("errors")
                logger.warning(
                    f"[{self.group_id}] Bỏ qua message có event_time không hợp lệ: msisdn={msisdn}"
                )
                continue

            # ── Device Swap Detected ──
            logger.info(f"[Device Swap Detected] MSISDN: {msisdn} | IMEI Old: {imei_old} -> New: {imei_new}")
            self.metrics.increment("events_detected")

            swap_upserts.append((msisdn, imei_new))
            swap_records.append((msisdn, imei_old, imei_new, dt))
            swap_audit.append(("DEVICE_SWAP", msisdn, json.dumps({
                "imei_old": imei_old, "imei_new": imei_new, "event_time": dt.isoformat(),
            })))
            swap_msisdns_for_notify.append(msisdn)
            cache_updates[self._device_cache_key(msisdn)] = json.dumps({"imei_current": imei_new})
            redis_state[msisdn] = imei_new  # cập nhật state cho msg tiếp theo trong batch

        # ── Step 4b: Fetch subscriptions for notification ──
        if swap_msisdns_for_notify:
            for i, msisdn in enumerate(swap_msisdns_for_notify):
                subs = await self.db.get_active_subscriptions(msisdn, "DEVICE_SWAP")
                if subs:
                    rec = swap_records[i]
                    dt_iso = rec[3].isoformat()
                    payload_json = json.dumps({
                        "msisdn": msisdn,
                        "imei_old": rec[1],
                        "imei_new": rec[2],
                        "event_time": dt_iso,
                    })
                    for sub in subs:
                        notification_records.append((sub["subscription_id"], "DEVICE_SWAP", payload_json))

        # ── Step 5: F-02 Atomic batch writes ──
        all_upserts = init_records + swap_upserts
        if all_upserts or swap_records or swap_audit or notification_records:
            await self.db.commit_device_swap_batch(
                upserts=all_upserts,
                history=swap_records,
                audit=swap_audit,
                notification_records=notification_records if notification_records else None,
            )

        # Redis KHÔNG nằm trong transaction Postgres (khác engine).
        # Coi Redis là projection có thể rebuild: chỉ update SAU KHI Postgres commit.
        if cache_updates:
            await self.redis.mset(cache_updates)

        self.metrics.increment("success", len(valid_msgs))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    consumer = DeviceSwapConsumer()
    import asyncio
    asyncio.run(consumer.run())
