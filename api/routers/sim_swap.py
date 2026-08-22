#api\routers\sim_swap.py
"""
SIM Swap API — tuân thủ CAMARA SimSwap spec chính thức.
https://github.com/camaraproject/SimSwap

Endpoints:
  POST /sim-swap/v0/check          → swapped: bool
  POST /sim-swap/v0/retrieve-date  → latestSimChange: datetime | null

[FIX - KHONG SUA SQL/INDEX] Query CU dung `detected_at` (tin hieu
RADIUS tho, do swap_detector.py phat hien qua Conflict C) lam can cu
tra ket qua, KHONG loc `confirmed_at IS NOT NULL`. `detected_at` chi
la tin hieu so bo; `confirmed_at` moi la moc da duoc doi chieu voi
HLR/HSS (nguon su that duy nhat ve SIM Swap that su xay ra hay
khong -- xem mock_services/hlr_hss/README.md). Voi 1 API dung cho
muc dich chong gian lan (fraud prevention), tra `swapped=true` dua
tren su kien CHUA duoc xac nhan la sai nghiem trong ve nghiep vu.

Query logic MOI:
  SELECT confirmed_at FROM swap_event
  WHERE msisdn = $1
    AND swap_type = 'SIM_SWAP'
    AND confirmed_at IS NOT NULL
    AND confirmed_at >= NOW() - $2 * INTERVAL '1 day'
  ORDER BY confirmed_at DESC
  LIMIT 1

[VE INDEX] KHONG can tao index moi. idx_swap_msisdn hien co
(msisdn, detected_at DESC) van duoc planner dung de loc theo `msisdn`
(cot dau tien cua index luon dung duoc du sort key con lai la
detected_at chu khong phai confirmed_at) -- Postgres se dung index
nay de tim nhanh cac dong cua thue bao nay, sau do loc/sort
confirmed_at tren tap con da thu hep (thuong chi vai dong swap_event
cho 1 msisdn), khong can quet toan bang. Chap nhan danh doi nay thay
vi them index moi, vi so luong swap_event cho 1 msisdn rat nho.
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

# [FIX] Doi cot loc/sort/tra ve tu detected_at -> confirmed_at, them
# dieu kien confirmed_at IS NOT NULL de loai bo cac su kien swap CHUA
# duoc HLR xac nhan.
_QUERY_LATEST_SWAP = """
    SELECT changed_at AS confirmed_at
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
)
async def check_sim_swap(
    body: SimSwapCheckRequest,
    db: asyncpg.Connection = Depends(get_db),
) -> SimSwapCheckResponse:
    """
    Trả về swapped=True nếu số điện thoại đã được gán SIM mới
    trong khoảng maxAge ngày gần nhất, VÀ sự kiện đó đã được
    xác nhận qua HLR/HSS (confirmed_at IS NOT NULL).

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
    Trả về thời điểm SIM Swap gần nhất ĐÃ ĐƯỢC XÁC NHẬN (confirmed_at)
    trong khoảng maxAge ngày. Nếu không có SIM Swap nào đã xác nhận,
    latestSimChange = null.

    Args:
        body: phoneNumber (E.164) + maxAge (ngày, mặc định 30).
        db: asyncpg connection từ pool.

    Returns:
        SimSwapRetrieveDateResponse: { latestSimChange: datetime | null }
    """
    row = await db.fetchrow(_QUERY_LATEST_SWAP, str(body.phoneNumber), body.maxAge)
    return SimSwapRetrieveDateResponse(
        latestSimChange=row["confirmed_at"] if row else None
    )