#api\routers\sim_swap.py
"""
SIM Swap API — tuân thủ CAMARA SimSwap spec chính thức.
https://github.com/camaraproject/SimSwap

Endpoints:
  POST /sim-swap/v0/check          → swapped: bool
  POST /sim-swap/v0/retrieve-date  → latestSimChange: datetime | null

Query logic:
  SELECT detected_at FROM swap_event
  WHERE msisdn = $1
    AND swap_type = 'SIM_SWAP'
    AND detected_at >= NOW() - $2 * INTERVAL '1 day'
  ORDER BY detected_at DESC
  LIMIT 1

SLA: p95 ≤ 200ms — đảm bảo bởi index idx_swap_msisdn
(msisdn, detected_at DESC) trên bảng swap_event.
"""

from fastapi import APIRouter, Depends
import asyncpg

from api.schemas.sim_swap import (
    SimSwapCheckRequest,
    SimSwapCheckResponse,
    SimSwapRetrieveDateResponse,
)
from api.dependencies.auth import verify_api_key
from api.dependencies.database import get_db

router = APIRouter(
    prefix="/sim-swap/v0",
    tags=["SIM Swap"],
    dependencies=[Depends(verify_api_key)],
)

# SQL dùng chung cho cả 2 endpoint — khác nhau ở cách dùng kết quả
_QUERY_LATEST_SWAP = """
    SELECT detected_at
    FROM swap_event
    WHERE msisdn = $1
      AND swap_type = 'SIM_SWAP'
      AND detected_at >= NOW() - ($2 * INTERVAL '1 day')
    ORDER BY detected_at DESC
    LIMIT 1
"""


@router.post(
    "/check",
    response_model=SimSwapCheckResponse,
    summary="Kiểm tra SIM Swap đã xảy ra trong N ngày qua",
)
async def check_sim_swap(
    body: SimSwapCheckRequest,
    db: asyncpg.Connection = Depends(get_db),
) -> SimSwapCheckResponse:
    """
    Trả về swapped=True nếu số điện thoại đã được gán SIM mới
    trong khoảng maxAge ngày gần nhất.

    Args:
        body: phoneNumber (E.164) + maxAge (ngày, mặc định 30).
        db: asyncpg connection từ pool (inject bởi Depends).

    Returns:
        SimSwapCheckResponse: { swapped: bool }
    """
    row = await db.fetchrow(_QUERY_LATEST_SWAP, str(body.phoneNumber), body.maxAge)
    return SimSwapCheckResponse(swapped=row is not None)


@router.post(
    "/retrieve-date",
    response_model=SimSwapRetrieveDateResponse,
    summary="Lấy thời điểm SIM Swap gần nhất",
)
async def retrieve_sim_swap_date(
    body: SimSwapCheckRequest,
    db: asyncpg.Connection = Depends(get_db),
) -> SimSwapRetrieveDateResponse:
    """
    Trả về thời điểm SIM Swap gần nhất trong khoảng maxAge ngày.
    Nếu không có SIM Swap nào, latestSimChange = null.

    Args:
        body: phoneNumber (E.164) + maxAge (ngày, mặc định 30).
        db: asyncpg connection từ pool.

    Returns:
        SimSwapRetrieveDateResponse: { latestSimChange: datetime | null }
    """
    row = await db.fetchrow(_QUERY_LATEST_SWAP, str(body.phoneNumber), body.maxAge)
    return SimSwapRetrieveDateResponse(
        latestSimChange=row["detected_at"] if row else None
    )