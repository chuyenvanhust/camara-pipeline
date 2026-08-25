# pipeline/modules/ip_msisdn/redis_store.py
import json
import logging
from typing import Optional, List
import redis.asyncio as aioredis

logger = logging.getLogger(__name__)

SESSION_TTL_SECONDS = 86400  # 24 giờ


class IPMappingStore:
    def __init__(self, redis_client: aioredis.Redis):
        self.redis = redis_client

    @staticmethod
    def _ip_key(framed_ip: str) -> str:
        return f"ip-ggsn:{framed_ip}"

    @staticmethod
    def _ggsn_key(nas_identifier: str) -> str:
        return f"ggsn-ips:{nas_identifier}"

    async def upsert_mapping(
        self,
        framed_ip: str,
        msisdn: str,
        timestamp: str,
        nas_identifier: Optional[str] = None,
        ttl: int = SESSION_TTL_SECONDS,
    ):
        """
        Start / Interim-Update:
        - Upsert (Key: ip-ggsn:<framed_ip>; Value: {"msisdn": "...", "timestamp": "..."}) vào Redis.
        - Set TTL 24h, refreshed on Interim-Update.
        - Bổ sung key phụ ggsn-ips:<nas_identifier> (SET) phục vụ xóa hàng loạt.
        """
        ip_key = self._ip_key(framed_ip)
        payload = json.dumps({"msisdn": msisdn, "timestamp": timestamp})

        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.set(ip_key, payload, ex=ttl)
            if nas_identifier:
                ggsn_key = self._ggsn_key(nas_identifier)
                pipe.sadd(ggsn_key, framed_ip)
                pipe.expire(ggsn_key, ttl)
            await pipe.execute()

        logger.debug(f"Upserted IP mapping {ip_key} -> {msisdn} (nas={nas_identifier})")

    async def delete_mapping(self, framed_ip: str, msisdn: str, nas_identifier: Optional[str] = None):
        """
        Stop: Xóa hoặc đánh dấu hết hiệu lực bản ghi tương ứng trong Redis (xóa theo key và check MSISDN)
        """
        ip_key = self._ip_key(framed_ip)
        existing_val = await self.redis.get(ip_key)

        if existing_val:
            try:
                data = json.loads(existing_val)
                # Chỉ xóa nếu MSISDN khớp (tránh xóa nhầm session mới nếu IP đã bị cấp lại)
                if data.get("msisdn") == msisdn:
                    async with self.redis.pipeline(transaction=True) as pipe:
                        pipe.delete(ip_key)
                        if nas_identifier:
                            pipe.srem(self._ggsn_key(nas_identifier), framed_ip)
                        await pipe.execute()
                    logger.debug(f"Deleted IP mapping {ip_key} for msisdn {msisdn}")
            except Exception as e:
                logger.error(f"Error deleting IP mapping {ip_key}: {e}")

    async def accounting_off(self, nas_identifier: str):
        """
        Accounting-Off: Xóa hoặc đánh dấu hết hiệu lực tất cả bản ghi có key phụ nas_identifier tương ứng.
        """
        if not nas_identifier:
            return

        ggsn_key = self._ggsn_key(nas_identifier)
        
        framed_ips = set()
        cursor = 0
        while True:
            cursor, batch = await self.redis.sscan(ggsn_key, cursor=cursor, count=500)
            framed_ips.update(batch)
            if cursor == 0:
                break

        if framed_ips:
            async with self.redis.pipeline(transaction=True) as pipe:
                for ip in framed_ips:
                    pipe.delete(self._ip_key(ip))
                pipe.delete(ggsn_key)
                await pipe.execute()
            logger.info(f"Accounting-Off: Cleared {len(framed_ips)} IP mappings for NAS {nas_identifier}")
        else:
            await self.redis.delete(ggsn_key)
