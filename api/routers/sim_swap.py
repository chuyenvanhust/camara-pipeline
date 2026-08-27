#api\routers\sim_swap.py
"""SIM Swap API backed by swap signals detected from RADIUS accounting.

The current pipeline does not integrate with HLR/HSS, so ``changed_at`` is a
detection timestamp, not an externally confirmed timestamp.
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

_QUERY_LATEST_SWAP = """
    SELECT changed_at AS detected_at
    FROM sim_swap_history
    WHERE msisdn = $1
      AND changed_at >= NOW() - ($2 * INTERVAL '1 day')
    ORDER BY changed_at DESC
    LIMIT 1
"""



@router.post(
    "/check",
    response_model=SimSwapCheckResponse,
    summary="Kiểm tra SIM Swap đã xảy ra trong N ngày qua",
    description="Tín hiệu phát hiện từ RADIUS accounting; chưa được đối chiếu HLR/HSS.",
)
async def check_sim_swap(
    body: SimSwapCheckRequest,
    db: asyncpg.Connection = Depends(get_db),
) -> SimSwapCheckResponse:
    """
    Trả về swapped=True khi pipeline phát hiện IMSI thay đổi từ dữ liệu RADIUS
    trong khoảng maxAge. Đây chưa phải xác nhận từ HLR/HSS.

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
    description="Thời điểm pipeline phát hiện thay đổi IMSI từ RADIUS, chưa xác nhận HLR/HSS.",
)
async def retrieve_sim_swap_date(
    body: SimSwapCheckRequest,
    db: asyncpg.Connection = Depends(get_db),
) -> SimSwapRetrieveDateResponse:
    """
    Trả về thời điểm phát hiện SIM Swap gần nhất từ RADIUS trong khoảng
    maxAge ngày; latestSimChange=null nếu không có tín hiệu.

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
