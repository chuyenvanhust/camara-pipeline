from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections.abc import Awaitable
from typing import Any, Dict, List, Optional

from pipeline.modules.shared.base_consumer import BaseKafkaConsumer
from pipeline.modules.shared.db import DatabasePool
from pipeline.modules.shared.events import InvalidMessageError, canonical_msisdn, event_id, parse_event_time, required_text
from pipeline.modules.shared.swap_checkpoint import StateCheckpointCoordinator


class SimSwapConsumer(BaseKafkaConsumer):
    def __init__(self, topic: str = os.getenv("KAFKA_TOPIC_RAW", "radius.accounting.raw"), group_id: str = "cg-sim-swap", db: Optional[DatabasePool] = None):
        super().__init__(topic=topic, group_id=group_id, db=db)
        self.checkpoints: Optional[StateCheckpointCoordinator] = None

    async def initialize(self) -> None:
        await super().initialize()
        self.checkpoints = StateCheckpointCoordinator(
            self.group_id, self.db.checkpoint_sim_states, self.metrics
        )
        self.checkpoints.start()

    async def stop(self) -> None:
        if self.checkpoints is not None:
            await self.checkpoints.close()
            self.checkpoints = None
        await super().stop()

    @staticmethod
    def _cache_key(msisdn: str) -> str:
        return f"sim:{msisdn}"

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
                required = (
                    "imsi_current", "last_event_at", "last_event_id",
                    "last_source_partition", "last_source_offset",
                )
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
            state.update(await self.db.batch_get_sim_state(missing))
            self.metrics.observe_postgres_fallback(time.monotonic() - fb_started)
        return state

    async def process_batch(
        self, records: List[Any]
    ) -> Optional[Awaitable[None]]:
        assert self.checkpoints is not None
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

        stage_started = time.monotonic()
        state = await self._load_state(list({item[1] for item in parsed}))
        self.metrics.observe_stage("state", time.monotonic() - stage_started)
        # F-BATCH-DUP-FIX: xem giải thích trong device_swap/consumer.py
        states_by_msisdn: Dict[str, tuple] = {}
        history, audit, outbox = [], [], []
        cache_updates: Dict[str, str] = {}
        changed_msisdns: set[str] = set()
        replay_checkpoints: Dict[str, tuple] = {}
        ignored_count = 0
        same_value_count = 0
        stale_count = 0
        events_count = 0
        for record, msisdn, imsi_new, occurred_at in parsed:
            eid = event_id(record)
            previous = state.get(msisdn)
            version = (occurred_at, record.partition, record.offset)
            if previous and previous.get("last_event_id") == eid:
                ignored_count += 1
                stale_count += 1
                replay_checkpoints[msisdn] = (
                    msisdn, previous.get("value", previous.get("imsi_current")),
                    previous["last_event_at"], previous["last_event_id"],
                    previous["last_source_partition"], previous["last_source_offset"],
                )
                continue
            if previous and version <= (previous["last_event_at"], previous["last_source_partition"], previous["last_source_offset"]):
                ignored_count += 1
                stale_count += 1
                replay_checkpoints[msisdn] = (
                    msisdn, previous.get("value", previous.get("imsi_current")),
                    previous["last_event_at"], previous["last_event_id"],
                    previous["last_source_partition"], previous["last_source_offset"],
                )
                continue
            imsi_old = previous.get("value", previous.get("imsi_current")) if previous else None
            states_by_msisdn[msisdn] = (msisdn, imsi_new, occurred_at, eid, record.partition, record.offset)
            current = {"imsi_current": imsi_new, "last_event_at": occurred_at, "last_event_id": eid,
                       "last_source_partition": record.partition, "last_source_offset": record.offset}
            state[msisdn] = current
            cache_updates[self._cache_key(msisdn)] = json.dumps({**current, "last_event_at": occurred_at.isoformat()})
            if imsi_old is None or imsi_old == imsi_new:
                ignored_count += 1
                same_value_count += 1
                continue
            changed_msisdns.add(msisdn)
            details = json.dumps({"imsi_old": imsi_old, "imsi_new": imsi_new, "last_time_sim_change": occurred_at.isoformat()})
            history.append((eid, record.topic, record.partition, record.offset, msisdn, imsi_old, imsi_new, occurred_at))
            audit.append((eid, "SIM_SWAP", msisdn, details, occurred_at))
            outbox.append((eid, "SIM_SWAP", msisdn, details))
            events_count += 1

        swap_states = [
            value for msisdn, value in states_by_msisdn.items()
            if msisdn in changed_msisdns
        ]
        checkpoint_by_msisdn = {
            msisdn: value for msisdn, value in replay_checkpoints.items()
            if msisdn not in changed_msisdns
        }
        checkpoint_by_msisdn.update({
            msisdn: value for msisdn, value in states_by_msisdn.items()
            if msisdn not in changed_msisdns
        })
        checkpoint_states = list(checkpoint_by_msisdn.values())

        async def persist_postgres() -> None:
            stage_started = time.monotonic()
            await self.db.persist_sim_batch(
                swap_states, history, audit, outbox
            )
            self.metrics.observe_stage("postgres", time.monotonic() - stage_started)

        async def persist_redis() -> None:
            if not cache_updates:
                return
            assert self.redis is not None
            stage_started = time.monotonic()
            await self.redis.mset(cache_updates)
            self.metrics.observe_stage("redis", time.monotonic() - stage_started)

        persistence_started = time.monotonic()
        # A real swap is rare but correctness-critical: PostgreSQL must become
        # durable before Redis publishes the new state. If Redis advanced first
        # and PostgreSQL failed, a retry would see the event_id in cache and could
        # incorrectly skip history/outbox persistence. Non-swap batches avoid the
        # PostgreSQL critical path entirely and go straight to Redis + checkpoint.
        if swap_states or history or audit or outbox:
            await persist_postgres()
        await persist_redis()
        self.metrics.observe_stage("persistence", time.monotonic() - persistence_started)
        if cache_updates:
            self.metrics.increment("redis_records", len(cache_updates))
        self.metrics.increment("postgres_records", len(swap_states))
        self.metrics.increment("ignored", ignored_count)
        self.metrics.increment("same_value", same_value_count)
        self.metrics.increment("stale", stale_count)
        self.metrics.increment("events_detected", events_count)
        self.metrics.increment("success", len(parsed))
        if checkpoint_states:
            return await self.checkpoints.submit(checkpoint_states)
        return None


if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)
    asyncio.run(SimSwapConsumer().run())
