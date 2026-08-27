#api\routers\device_swap.py
"""Device Swap custom API backed by IMEI changes detected from RADIUS.

No HLR/EIR confirmation source is integrated, therefore ``changed_at`` is a
detection timestamp and must not be presented as a confirmed timestamp.
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
    SELECT changed_at AS detected_at
    FROM device_swap_history
    WHERE msisdn = $1
      AND changed_at >= NOW() - ($2 * INTERVAL '1 day')
    ORDER BY changed_at DESC
    LIMIT 1
"""



@router.post(
    "/check",
    response_model=DeviceSwapCheckResponse,
    summary="Kiểm tra thiết bị (IMEI) đã thay đổi trong N ngày qua",
    description=(
        "**Custom extension** — CAMARA chưa có spec chính thức cho Device Swap. "
        "Thiết kế theo SIM Swap pattern, track IMEI thay vì IMSI. "
        "Tín hiệu lấy từ RADIUS và chưa được xác nhận qua HLR/EIR."
    ),
)
async def check_device_swap(
    body: DeviceSwapCheckRequest,
    db: asyncpg.Connection = Depends(get_db),
) -> DeviceSwapCheckResponse:
    """
    Trả về deviceSwapped=True nếu pipeline phát hiện IMEI thay đổi từ
    RADIUS trong khoảng maxAge. Đây chưa phải xác nhận từ HLR/EIR.

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
        "Đây là thời điểm phát hiện từ RADIUS; null nếu không có tín hiệu."
    ),
)
async def retrieve_device_swap_date(
    body: DeviceSwapCheckRequest,
    db: asyncpg.Connection = Depends(get_db),
) -> DeviceSwapRetrieveDateResponse:
    """
    Trả về thời điểm pipeline phát hiện đổi thiết bị gần nhất từ RADIUS.

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
