from __future__ import annotations

import ipaddress
import json

from fastapi import APIRouter, Depends, HTTPException, Query, status
from redis.asyncio import Redis

from api.dependencies.auth import verify_api_key
from api.dependencies.redis import get_redis
from api.schemas.ip_msisdn import IPMsisdnResponse


router = APIRouter(
    prefix="/ip-msisdn",
    tags=["IP-MSISDN"],
    dependencies=[Depends(verify_api_key)],
)


@router.get("", response_model=IPMsisdnResponse, summary="Tra cứu MSISDN đang dùng địa chỉ IP")
async def resolve_ip_msisdn(
    ipAddress: str = Query(..., description="Địa chỉ IPv4/IPv6 cần tra cứu"),
    redis: Redis = Depends(get_redis),
) -> IPMsisdnResponse:
    try:
        normalized_ip = str(ipaddress.ip_address(ipAddress))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="ipAddress is invalid") from exc
    raw = await redis.get(f"ip-ggsn:{normalized_ip}")
    if raw is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "NOT_FOUND", "message": "No active mapping for ipAddress."},
        )
    try:
        mapping = json.loads(raw)
        return IPMsisdnResponse(
            ipAddress=normalized_ip,
            phoneNumber=mapping["msisdn"],
            nasIdentifier=mapping.get("nas_identifier") or None,
            eventTimestamp=mapping["event_timestamp"],
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error": "INVALID_STATE", "message": "Stored IP mapping is invalid."},
        ) from exc
