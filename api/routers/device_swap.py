#api\routers\device_swap.py
"""
Device Swap API — custom extension theo CAMARA SIM Swap pattern.
Xem ADR-005: https://... (docs/adr/ADR-005-device-swap-api-design.md)

CAMARA chưa có spec chính thức cho Device Swap. Thiết kế này
dùng cùng pattern SIM Swap nhưng track IMEI thay vì IMSI.
OpenAPI description ghi rõ đây là custom extension.

Endpoints:
  POST /device-swap/v0/check          → deviceSwapped: bool
  POST /device-swap/v0/retrieve-date  → latestDeviceChange: datetime | null

Query logic:
  SELECT detected_at FROM swap_event
  WHERE msisdn = $1
    AND swap_type = 'DEVICE_SWAP'
    AND detected_at >= NOW() - $2 * INTERVAL '1 day'
  ORDER BY detected_at DESC
  LIMIT 1

SLA: p95 ≤ 200ms — index idx_swap_imei (imei, detected_at DESC).
"""

from fastapi import APIRouter, Depends
import asyncpg

from api.schemas.device_swap import (
    DeviceSwapCheckRequest,
    DeviceSwapCheckResponse,
    DeviceSwapRetrieveDateResponse,
)
from api.dependencies.auth import verify_api_key
from api.dependencies.database import get_db

router = APIRouter(
    prefix="/device-swap/v0",
    tags=["Device Swap"],
    dependencies=[Depends(verify_api_key)],
)

_QUERY_LATEST_DEVICE_SWAP = """
    SELECT detected_at
    FROM swap_event
    WHERE msisdn = $1
      AND swap_type = 'DEVICE_SWAP'
      AND detected_at >= NOW() - ($2 * INTERVAL '1 day')
    ORDER BY detected_at DESC
    LIMIT 1
"""


@router.post(
    "/check",
    response_model=DeviceSwapCheckResponse,
    summary="Kiểm tra thiết bị (IMEI) đã thay đổi trong N ngày qua",
    description=(
        "**Custom extension** — CAMARA chưa có spec chính thức cho Device Swap. "
        "Thiết kế theo SIM Swap pattern, track IMEI thay vì IMSI. "
        "Xem ADR-005 để biết lý do thiết kế."
    ),
)
async def check_device_swap(
    body: DeviceSwapCheckRequest,
    db: asyncpg.Connection = Depends(get_db),
) -> DeviceSwapCheckResponse:
    """
    Trả về deviceSwapped=True nếu IMEI của thuê bao thay đổi
    trong khoảng maxAge ngày gần nhất.

    Args:
        body: phoneNumber (E.164) + maxAge (ngày, mặc định 30).
        db: asyncpg connection từ pool.

    Returns:
        DeviceSwapCheckResponse: { deviceSwapped: bool }
    """
    row = await db.fetchrow(
        _QUERY_LATEST_DEVICE_SWAP, str(body.phoneNumber), body.maxAge
    )
    return DeviceSwapCheckResponse(deviceSwapped=row is not None)


@router.post(
    "/retrieve-date",
    response_model=DeviceSwapRetrieveDateResponse,
    summary="Lấy thời điểm đổi thiết bị gần nhất",
    description=(
        "**Custom extension** — trả về thời điểm IMEI thay đổi gần nhất. "
        "null nếu không có thay đổi trong khoảng maxAge ngày."
    ),
)
async def retrieve_device_swap_date(
    body: DeviceSwapCheckRequest,
    db: asyncpg.Connection = Depends(get_db),
) -> DeviceSwapRetrieveDateResponse:
    """
    Trả về thời điểm đổi thiết bị gần nhất trong maxAge ngày.

    Args:
        body: phoneNumber (E.164) + maxAge (ngày).
        db: asyncpg connection từ pool.

    Returns:
        DeviceSwapRetrieveDateResponse: { latestDeviceChange: datetime | null }
    """
    row = await db.fetchrow(
        _QUERY_LATEST_DEVICE_SWAP, str(body.phoneNumber), body.maxAge
    )
    return DeviceSwapRetrieveDateResponse(
        latestDeviceChange=row["detected_at"] if row else None
    )