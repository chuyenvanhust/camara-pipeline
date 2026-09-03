from __future__ import annotations

import asyncio
import ipaddress
import logging
import os
import time
from typing import Any, Dict, List, Optional

from pipeline.modules.ip_msisdn.redis_store import IPMappingStore
from pipeline.modules.shared.base_consumer import BaseKafkaConsumer
from pipeline.modules.shared.db import DatabasePool
from pipeline.modules.shared.events import InvalidMessageError, canonical_msisdn, event_id, normalize_status, parse_event_time, required_text


class IPMsisdnConsumer(BaseKafkaConsumer):
    def __init__(self, topic: str = os.getenv("KAFKA_TOPIC_RAW", "radius.accounting.raw"), group_id: str = "cg-ip-msisdn", db: Optional[DatabasePool] = None):
        super().__init__(topic=topic, group_id=group_id, db=db)
        self.store: Optional[IPMappingStore] = None

    async def initialize(self) -> None:
        await super().initialize()
        assert self.redis is not None
        self.store = IPMappingStore(self.redis)

    @staticmethod
    def _optional_text(message, *keys):
        for key in keys:
            value = message.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
        return None

    async def process_batch(self, records: List[Any]) -> None:
        assert self.store is not None
        operations = []
        
        session_by_id: Dict[str, tuple] = {}
        for record in records:
            try:
                message = record.value
                if message.get("_decode_error"):
                    raise InvalidMessageError(message["_decode_error"])
                status = normalize_status(message.get("acct_status_type"))
                occurred_at = parse_event_time(message)
                nas = self._optional_text(message, "nas_identifier", "NAS_Identifier", "NAS-Identifier")
                if status == "accounting-off":
                    if not nas:
                        raise InvalidMessageError("accounting-off requires nas_identifier")
                    operations.append((record, status, occurred_at, nas, None, None))
                    continue
                if status == "accounting-on":
                    continue
                msisdn = canonical_msisdn(message)
                framed_ip = required_text(message, "framed_ip", "Framed_IP_Address", "Framed-IP-Address")
                try:
                    ipaddress.ip_address(framed_ip)
                except ValueError as exc:
                    raise InvalidMessageError("invalid framed_ip") from exc
                raw_session_id = required_text(message, "acct_session_id", "Acct_Session_Id", "Acct-Session-Id")
                session_id = f"{nas or '-'}:{raw_session_id}"
                eid = event_id(record)
                session_by_id[session_id] = (session_id, msisdn, nas, status != "stop", occurred_at,
                                              eid, record.partition, record.offset)
                operations.append((record, status, occurred_at, nas, framed_ip, msisdn))
            except InvalidMessageError as exc:
                await self.send_to_dlq(record, exc)

        async def persist_postgres() -> int:
            """Preserve PostgreSQL ordering while running independently of Redis."""
            stage_started = time.monotonic()
            written = len(session_by_id)
            await self.db.persist_session_batch(list(session_by_id.values()))
            for _record, status, occurred_at, nas, _framed_ip, _msisdn in operations:
                if status == "accounting-off":
                    await self.db.mark_nas_sessions_inactive(nas, occurred_at)
                    written += 1
            self.metrics.observe_stage("postgres", time.monotonic() - stage_started)
            return written

        async def persist_redis() -> int:
            """Preserve Kafka offset order inside Redis while batching round trips."""
            stage_started = time.monotonic()
            changed = 0
            redis_batch: List[Dict[str, Any]] = []

            async def flush() -> None:
                nonlocal changed, redis_batch
                if redis_batch:
                    changed += await self.store.apply_batch(redis_batch)
                    redis_batch = []

            for record, status, occurred_at, nas, framed_ip, msisdn in operations:
                if status in {"start", "interim-update"}:
                    redis_batch.append({
                        "kind": "upsert", "framed_ip": framed_ip, "msisdn": msisdn,
                        "event_time": occurred_at, "event_id": event_id(record),
                        "partition": record.partition, "offset": record.offset,
                        "nas_identifier": nas,
                    })
                elif status == "stop":
                    redis_batch.append({
                        "kind": "delete", "framed_ip": framed_ip, "msisdn": msisdn,
                        "event_time": occurred_at, "partition": record.partition,
                        "offset": record.offset,
                    })
                elif status == "accounting-off":
                    await flush()
                    changed += await self.store.accounting_off(nas, occurred_at)
            await flush()
            self.metrics.observe_stage("redis", time.monotonic() - stage_started)
            return changed

        # PostgreSQL and Redis are independently version-fenced/idempotent. Run both
        # branches concurrently, but wait for both even when one fails. This prevents
        # an orphaned write from overlapping the retry. BaseKafkaConsumer commits the
        # Kafka offsets only after this method has completed successfully.
        persistence_started = time.monotonic()
        postgres_result, redis_result = await asyncio.gather(
            persist_postgres(), persist_redis(), return_exceptions=True
        )
        self.metrics.observe_stage("persistence", time.monotonic() - persistence_started)
        failures = [
            result for result in (postgres_result, redis_result)
            if isinstance(result, BaseException)
        ]
        if failures:
            raise failures[0]

        assert isinstance(postgres_result, int)
        assert isinstance(redis_result, int)
        self.metrics.increment("postgres_records", postgres_result)
        self.metrics.increment("redis_records", redis_result)
        self.metrics.increment("events_detected", len(operations))
        self.metrics.increment("success", len(operations))


if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)
    asyncio.run(IPMsisdnConsumer().run())
