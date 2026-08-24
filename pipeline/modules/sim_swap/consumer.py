# pipeline/modules/sim_swap/consumer.py
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

from pipeline.modules.shared.base_consumer import BaseKafkaConsumer
from pipeline.modules.shared.db import DatabasePool

logger = logging.getLogger(__name__)


class SimSwapConsumer(BaseKafkaConsumer):
    def __init__(
        self,
        topic: str = os.getenv("KAFKA_TOPIC_RAW", "radius.accounting.raw"),
        group_id: str = "cg-sim-swap",
        db: Optional[DatabasePool] = None,
    ):
        super().__init__(topic=topic, group_id=group_id, db=db)

    async def initialize(self):
        await super().initialize()

    @staticmethod
    def _sim_cache_key(msisdn: str) -> str:
        return f"sim:{msisdn}"

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

        # F-14: Validate event_time — skip message nếu không parse được
        dt = self._parse_event_time(message)
        if dt is None:
            self.metrics.increment("errors")
            logger.warning(
                f"[{self.group_id}] Bỏ qua message có event_time không hợp lệ: msisdn={msisdn}"
            )
            return

        logger.info(f"[SIM Swap Detected] MSISDN: {msisdn} | IMSI Old: {imsi_old} -> New: {imsi_new}")
        self.metrics.increment("events_detected")
        await self.db.upsert_sim_state(msisdn, imsi_new)
        await self.db.record_sim_swap_history(msisdn, imsi_old, imsi_new, dt)
        await self.redis.set(cache_key, json.dumps({"imsi_current": imsi_new, "last_time_sim_change": dt.isoformat()}))

        # Chỉ ghi audit log cho sự kiện Swap
        await self.db.insert_audit_log(
            event_type="SIM_SWAP", msisdn=msisdn,
            details={"imsi_old": imsi_old, "imsi_new": imsi_new, "last_time_sim_change": dt.isoformat()},
        )

        # F-03: Ghi notification_log PENDING — KHÔNG gọi HTTP trong consumer
        subs = await self.db.get_active_subscriptions(msisdn, "SIM_SWAP")
        if subs:
            payload = {"MSISDN": msisdn, "LastTimeSIMChange": dt.isoformat()}
            for sub in subs:
                await self.db.insert_notification_log(
                    subscription_id=sub["subscription_id"],
                    event_type="SIM_SWAP",
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
        notification_records: List[tuple] = []  # F-03: (sub_id, event_type, payload_json)
        cache_updates: Dict[str, str] = {}

        # Pre-fetch subscriptions for all swap msisdns in batch
        swap_msisdns_for_notify: List[str] = []

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

            # F-14: Validate event_time
            dt = self._parse_event_time(msg)
            if dt is None:
                self.metrics.increment("errors")
                logger.warning(
                    f"[{self.group_id}] Bỏ qua message có event_time không hợp lệ: msisdn={msisdn}"
                )
                continue

            # ── SIM Swap Detected ──
            logger.info(f"[SIM Swap Detected] MSISDN: {msisdn} | IMSI Old: {imsi_old} -> New: {imsi_new}")
            self.metrics.increment("events_detected")

            swap_upserts.append((msisdn, imsi_new))
            swap_records.append((msisdn, imsi_old, imsi_new, dt))
            swap_audit.append(("SIM_SWAP", msisdn, json.dumps({
                "imsi_old": imsi_old, "imsi_new": imsi_new, "last_time_sim_change": dt.isoformat(),
            })))
            swap_msisdns_for_notify.append(msisdn)
            cache_updates[self._sim_cache_key(msisdn)] = json.dumps({
                "imsi_current": imsi_new, "last_time_sim_change": dt.isoformat(),
            })
            redis_state[msisdn] = imsi_new

        # ── Step 4b: Fetch subscriptions for notification ──
        if swap_msisdns_for_notify:
            for i, msisdn in enumerate(swap_msisdns_for_notify):
                subs = await self.db.get_active_subscriptions(msisdn, "SIM_SWAP")
                if subs:
                    # Tìm dt tương ứng từ swap_records
                    dt_iso = swap_records[i][3].isoformat()
                    payload_json = json.dumps({"MSISDN": msisdn, "LastTimeSIMChange": dt_iso})
                    for sub in subs:
                        notification_records.append((sub["subscription_id"], "SIM_SWAP", payload_json))

        # ── Step 5: F-02 Atomic batch writes ──
        all_upserts = init_records + swap_upserts
        if all_upserts or swap_records or swap_audit or notification_records:
            await self.db.commit_sim_swap_batch(
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
    consumer = SimSwapConsumer()
    import asyncio
    asyncio.run(consumer.run())
