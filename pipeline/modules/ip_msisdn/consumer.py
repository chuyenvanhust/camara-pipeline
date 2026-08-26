from __future__ import annotations

import ipaddress
import logging
import os
from typing import Any, List, Optional

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
        session_records = []
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
                session_records.append((session_id, msisdn, nas, status != "stop", occurred_at,
                                        eid, record.partition, record.offset))
                operations.append((record, status, occurred_at, nas, framed_ip, msisdn))
            except InvalidMessageError as exc:
                await self.send_to_dlq(record, exc)

        await self.db.persist_session_batch(session_records)
        for record, status, occurred_at, nas, framed_ip, msisdn in operations:
            if status in {"start", "interim-update"}:
                await self.store.upsert_mapping(framed_ip, msisdn, occurred_at, event_id(record),
                                                record.partition, record.offset, nas)
            elif status == "stop":
                await self.store.delete_mapping(framed_ip, msisdn, occurred_at,
                                                record.partition, record.offset)
            elif status == "accounting-off":
                await self.db.mark_nas_sessions_inactive(nas, occurred_at)
                await self.store.accounting_off(nas, occurred_at)
            self.metrics.increment("events_detected")
        self.metrics.increment("success", len(operations))


if __name__ == "__main__":
    import asyncio
    logging.basicConfig(level=logging.INFO)
    asyncio.run(IPMsisdnConsumer().run())
