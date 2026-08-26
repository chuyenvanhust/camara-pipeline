from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

from pipeline.modules.shared.base_consumer import BaseKafkaConsumer
from pipeline.modules.shared.db import DatabasePool
from pipeline.modules.shared.events import InvalidMessageError, canonical_msisdn, event_id, parse_event_time, required_text


class SimSwapConsumer(BaseKafkaConsumer):
    def __init__(self, topic: str = os.getenv("KAFKA_TOPIC_RAW", "radius.accounting.raw"), group_id: str = "cg-sim-swap", db: Optional[DatabasePool] = None):
        super().__init__(topic=topic, group_id=group_id, db=db)

    @staticmethod
    def _cache_key(msisdn: str) -> str:
        return f"sim:{msisdn}"

    async def _load_state(self, msisdns: List[str]) -> Dict[str, Dict[str, Any]]:
        assert self.redis is not None
        cached = await self.redis.mget([self._cache_key(value) for value in msisdns])
        state: Dict[str, Dict[str, Any]] = {}
        missing: List[str] = []
        for msisdn, raw in zip(msisdns, cached):
            try:
                item = json.loads(raw) if raw else None
                required = ("imsi_current", "last_event_at", "last_source_partition", "last_source_offset")
                if item and all(key in item for key in required):
                    item["last_event_at"] = parse_event_time({"event_timestamp": item["last_event_at"]})
                    state[msisdn] = item
                    continue
            except (TypeError, ValueError, InvalidMessageError):
                pass
            missing.append(msisdn)
        if missing:
            state.update(await self.db.batch_get_sim_state(missing))
        return state

    async def process_batch(self, records: List[Any]) -> None:
        parsed = []
        for record in records:
            try:
                message = record.value
                if message.get("_decode_error"):
                    raise InvalidMessageError(message["_decode_error"])
                parsed.append((record, canonical_msisdn(message), required_text(message, "imsi"), parse_event_time(message)))
            except InvalidMessageError as exc:
                await self.send_to_dlq(record, exc)
        if not parsed:
            return

        state = await self._load_state(list({item[1] for item in parsed}))
        states, history, audit, outbox = [], [], [], []
        cache_updates: Dict[str, str] = {}
        for record, msisdn, imsi_new, occurred_at in parsed:
            eid = event_id(record)
            previous = state.get(msisdn)
            version = (occurred_at, record.partition, record.offset)
            if previous and version <= (previous["last_event_at"], previous["last_source_partition"], previous["last_source_offset"]):
                self.metrics.increment("ignored")
                continue
            imsi_old = previous.get("value", previous.get("imsi_current")) if previous else None
            states.append((msisdn, imsi_new, occurred_at, eid, record.partition, record.offset))
            current = {"imsi_current": imsi_new, "last_event_at": occurred_at, "last_event_id": eid,
                       "last_source_partition": record.partition, "last_source_offset": record.offset}
            state[msisdn] = current
            cache_updates[self._cache_key(msisdn)] = json.dumps({**current, "last_event_at": occurred_at.isoformat()})
            if imsi_old is None or imsi_old == imsi_new:
                self.metrics.increment("ignored")
                continue
            details = json.dumps({"imsi_old": imsi_old, "imsi_new": imsi_new, "last_time_sim_change": occurred_at.isoformat()})
            history.append((eid, record.topic, record.partition, record.offset, msisdn, imsi_old, imsi_new, occurred_at))
            audit.append((eid, "SIM_SWAP", msisdn, details, occurred_at))
            outbox.append((eid, "SIM_SWAP", msisdn, details, occurred_at))
            self.metrics.increment("events_detected")

        await self.db.persist_sim_batch(states, history, audit, outbox)
        if cache_updates:
            assert self.redis is not None
            await self.redis.mset(cache_updates)
        self.metrics.increment("success", len(parsed))


if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)
    asyncio.run(SimSwapConsumer().run())
