from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

from pipeline.modules.shared.base_consumer import BaseKafkaConsumer
from pipeline.modules.shared.db import DatabasePool
from pipeline.modules.shared.events import InvalidMessageError, canonical_msisdn, event_id, parse_event_time, required_text


class DeviceSwapConsumer(BaseKafkaConsumer):
    def __init__(self, topic: str = os.getenv("KAFKA_TOPIC_RAW", "radius.accounting.raw"), group_id: str = "cg-device-swap", db: Optional[DatabasePool] = None):
        super().__init__(topic=topic, group_id=group_id, db=db)

    @staticmethod
    def _cache_key(msisdn: str) -> str:
        return f"device:{msisdn}"

    async def _load_state(self, msisdns: List[str]) -> Dict[str, Dict[str, Any]]:
        assert self.redis is not None
        mget_started = time.monotonic()
        cached = await self.redis.mget([self._cache_key(value) for value in msisdns])
        self.metrics.observe_redis_mget(time.monotonic() - mget_started)
        state: Dict[str, Dict[str, Any]] = {}
        missing: List[str] = []
        for msisdn, raw in zip(msisdns, cached):
            try:
                item = json.loads(raw) if raw else None
                required = ("imei_current", "last_event_at", "last_source_partition", "last_source_offset")
                if item and all(key in item for key in required):
                    item["last_event_at"] = parse_event_time({"event_timestamp": item["last_event_at"]})
                    state[msisdn] = item
                    continue
            except (TypeError, ValueError, InvalidMessageError):
                pass
            missing.append(msisdn)
        hits = len(msisdns) - len(missing)
        self.metrics.set_cache_hit_ratio(hits, len(msisdns))
        if missing:
            fb_started = time.monotonic()
            state.update(await self.db.batch_get_device_state(missing))
            self.metrics.observe_postgres_fallback(time.monotonic() - fb_started)
        return state

    async def process_batch(self, records: List[Any]) -> None:
        parsed = []
        for record in records:
            try:
                message = record.value
                if message.get("_decode_error"):
                    raise InvalidMessageError(message["_decode_error"])
                parsed.append((record, canonical_msisdn(message), required_text(message, "imei"), parse_event_time(message)))
            except InvalidMessageError as exc:
                await self.send_to_dlq(record, exc)
        if not parsed:
            return

        stage_started = time.monotonic()
        state = await self._load_state(list({item[1] for item in parsed}))
        self.metrics.observe_stage("state", time.monotonic() - stage_started)
        # F-BATCH-DUP-FIX: dict thay vì list — nếu CÙNG msisdn xuất hiện nhiều lần
        # trong 1 batch (nhiều bản ghi RADIUS liên tiếp của cùng thuê bao), UPSERT
        # nhiều dòng CÙNG msisdn trong 1 lệnh SQL sẽ bị Postgres từ chối
        # ("ON CONFLICT DO UPDATE command cannot affect row a second time").
        # records của process_batch luôn thuộc 1 partition, đã sắp theo offset,
        # nên ghi đè theo thứ tự lặp = giữ lại đúng bản ghi MỚI NHẤT.
        states_by_msisdn: Dict[str, tuple] = {}
        history, audit, outbox = [], [], []
        cache_updates: Dict[str, str] = {}
        for record, msisdn, imei_new, occurred_at in parsed:
            eid = event_id(record)
            previous = state.get(msisdn)
            version = (occurred_at, record.partition, record.offset)
            if previous and previous.get("last_event_id") == eid:
                self.metrics.increment("ignored")
                continue
            if previous and version <= (previous["last_event_at"], previous["last_source_partition"], previous["last_source_offset"]):
                self.metrics.increment("ignored")
                continue
            imei_old = previous.get("value", previous.get("imei_current")) if previous else None
            states_by_msisdn[msisdn] = (msisdn, imei_new, occurred_at, eid, record.partition, record.offset)
            current = {"imei_current": imei_new, "last_event_at": occurred_at, "last_event_id": eid,
                       "last_source_partition": record.partition, "last_source_offset": record.offset}
            state[msisdn] = current
            cache_updates[self._cache_key(msisdn)] = json.dumps({**current, "last_event_at": occurred_at.isoformat()})
            if imei_old is None or imei_old == imei_new:
                self.metrics.increment("ignored")
                continue
            details = json.dumps({"imei_old": imei_old, "imei_new": imei_new, "event_time": occurred_at.isoformat()})
            history.append((eid, record.topic, record.partition, record.offset, msisdn, imei_old, imei_new, occurred_at))
            audit.append((eid, "DEVICE_SWAP", msisdn, details, occurred_at))
            outbox.append((eid, "DEVICE_SWAP", msisdn, details))
            self.metrics.increment("events_detected")

        stage_started = time.monotonic()
        await self.db.persist_device_batch(list(states_by_msisdn.values()), history, audit, outbox)
        self.metrics.observe_stage("postgres", time.monotonic() - stage_started)
        self.metrics.increment("postgres_records", len(states_by_msisdn))
        if cache_updates:
            assert self.redis is not None
            stage_started = time.monotonic()
            await self.redis.mset(cache_updates)
            self.metrics.observe_stage("redis", time.monotonic() - stage_started)
            self.metrics.increment("redis_records", len(cache_updates))
        self.metrics.increment("success", len(parsed))


if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)
    asyncio.run(DeviceSwapConsumer().run())
