from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import redis.asyncio as aioredis

logger = logging.getLogger(__name__)
SESSION_TTL_SECONDS = 86400

UPSERT_LUA = """
local old = redis.call('GET', KEYS[1])
if old then
  local decoded = cjson.decode(old)
  local newer = tonumber(ARGV[3]) > tonumber(decoded.event_epoch)
    or (tonumber(ARGV[3]) == tonumber(decoded.event_epoch)
      and (tonumber(ARGV[4]) > tonumber(decoded.source_partition)
        or (tonumber(ARGV[4]) == tonumber(decoded.source_partition)
          and tonumber(ARGV[5]) > tonumber(decoded.source_offset))))
  if not newer then return 0 end
  if decoded.nas_identifier and decoded.nas_identifier ~= '' and decoded.nas_identifier ~= ARGV[2] then
    redis.call('ZREM', 'ggsn-ips:' .. decoded.nas_identifier, ARGV[1])
  end
end
redis.call('SET', KEYS[1], ARGV[6], 'EX', ARGV[7])
if ARGV[2] ~= '' then
  redis.call('ZADD', 'ggsn-ips:' .. ARGV[2], ARGV[3], ARGV[1])
  redis.call('EXPIRE', 'ggsn-ips:' .. ARGV[2], ARGV[7])
end
return 1
"""

DELETE_LUA = """
local old = redis.call('GET', KEYS[1])
if not old then return 0 end
local decoded = cjson.decode(old)
if decoded.msisdn ~= ARGV[2] then return 0 end
local allowed = tonumber(ARGV[3]) > tonumber(decoded.event_epoch)
  or (tonumber(ARGV[3]) == tonumber(decoded.event_epoch)
    and (tonumber(ARGV[4]) > tonumber(decoded.source_partition)
      or (tonumber(ARGV[4]) == tonumber(decoded.source_partition)
        and tonumber(ARGV[5]) >= tonumber(decoded.source_offset))))
if not allowed then return 0 end
redis.call('DEL', KEYS[1])
if decoded.nas_identifier and decoded.nas_identifier ~= '' then
  redis.call('ZREM', 'ggsn-ips:' .. decoded.nas_identifier, ARGV[1])
end
return 1
"""

ACCOUNTING_OFF_LUA = """
local old = redis.call('GET', KEYS[1])
if old then
  local decoded = cjson.decode(old)
  if decoded.nas_identifier == ARGV[2] and tonumber(decoded.event_epoch) <= tonumber(ARGV[3]) then
    redis.call('DEL', KEYS[1])
  end
end
redis.call('ZREM', 'ggsn-ips:' .. ARGV[2], ARGV[1])
return 1
"""


class IPMappingStore:
    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client
        self._upsert_script = self.redis.register_script(UPSERT_LUA)
        self._delete_script = self.redis.register_script(DELETE_LUA)
        self._acct_off_script = self.redis.register_script(ACCOUNTING_OFF_LUA)

    @staticmethod
    def _ip_key(framed_ip: str) -> str:
        return f"ip-ggsn:{framed_ip}"

    @staticmethod
    def _ggsn_key(nas_identifier: str) -> str:
        return f"ggsn-ips:{nas_identifier}"

    async def upsert_mapping(self, framed_ip: str, msisdn: str, event_time: datetime,
                             event_id: str, partition: int, offset: int,
                             nas_identifier: Optional[str] = None,
                             ttl: int = SESSION_TTL_SECONDS) -> bool:
        epoch = event_time.timestamp()
        nas = nas_identifier or ""
        payload = json.dumps({"msisdn": msisdn, "nas_identifier": nas,
                              "event_timestamp": event_time.isoformat(), "event_epoch": epoch,
                              "event_id": event_id, "source_partition": partition,
                              "source_offset": offset})
        result = await self._upsert_script(
            keys=[self._ip_key(framed_ip)],
            args=[framed_ip, nas, epoch, partition, offset, payload, ttl]
        )
        return bool(result)

    async def delete_mapping(self, framed_ip: str, msisdn: str, event_time: datetime,
                             partition: int, offset: int) -> bool:
        result = await self._delete_script(
            keys=[self._ip_key(framed_ip)],
            args=[framed_ip, msisdn, event_time.timestamp(), partition, offset]
        )
        return bool(result)

    async def apply_batch(self, operations: List[Dict[str, Any]],
                          ttl: int = SESSION_TTL_SECONDS) -> int:
        """Apply ordered Start/Interim/Stop operations in one Redis round trip.

        Each Lua invocation remains atomic. A non-transactional pipeline only batches
        network I/O; Redis still executes commands in the original Kafka offset order.
        Script execution uses EVALSHA via registered Script instances to minimize network size.
        """
        if not operations:
            return 0
        async with self.redis.pipeline(transaction=False) as pipe:
            for operation in operations:
                framed_ip = operation["framed_ip"]
                event_time = operation["event_time"]
                if operation["kind"] == "upsert":
                    nas = operation.get("nas_identifier") or ""
                    epoch = event_time.timestamp()
                    payload = json.dumps({
                        "msisdn": operation["msisdn"], "nas_identifier": nas,
                        "event_timestamp": event_time.isoformat(), "event_epoch": epoch,
                        "event_id": operation["event_id"],
                        "source_partition": operation["partition"],
                        "source_offset": operation["offset"],
                    })
                    self._upsert_script(
                        keys=[self._ip_key(framed_ip)],
                        args=[framed_ip, nas, epoch, operation["partition"], operation["offset"], payload, ttl],
                        client=pipe
                    )
                else:
                    self._delete_script(
                        keys=[self._ip_key(framed_ip)],
                        args=[framed_ip, operation["msisdn"], event_time.timestamp(), operation["partition"], operation["offset"]],
                        client=pipe
                    )
            results = await pipe.execute()
        return sum(bool(result) for result in results)

    async def accounting_off(self, nas_identifier: str, event_time: datetime,
                             chunk_size: int = 500) -> int:
        reverse_key = self._ggsn_key(nas_identifier)
        removed = 0
        cutoff = event_time.timestamp()
        while True:
            addresses = await self.redis.zrangebyscore(reverse_key, "-inf", cutoff, start=0, num=chunk_size)
            if not addresses:
                break
            async with self.redis.pipeline(transaction=False) as pipe:
                for address in addresses:
                    self._acct_off_script(
                        keys=[self._ip_key(address)],
                        args=[address, nas_identifier, cutoff],
                        client=pipe
                    )
                await pipe.execute()
            removed += len(addresses)
        if await self.redis.zcard(reverse_key) == 0:
            await self.redis.delete(reverse_key)
        logger.info("Accounting-Off removed %d mappings for NAS %s", removed, nas_identifier)
        return removed
