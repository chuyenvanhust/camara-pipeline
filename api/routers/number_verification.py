"""
Number Verification API — tuân thủ CAMARA NumberVerification spec.
https://github.com/camaraproject/NumberVerification

Endpoint:
  POST /number-verification/v0/verify → devicePhoneNumberVerified: bool

Query logic (simplified lab — xem Quyết định Lab #9):
  SELECT EXISTS (
    SELECT 1 FROM radius_sessions r1
    WHERE r1.msisdn = $1
      AND r1.acct_status_type = 'Start'
      AND r1.event_timestamp >= NOW() - INTERVAL '24 hours'
      AND NOT EXISTS (
        SELECT 1 FROM radius_sessions r2
        WHERE r2.msisdn = $1
          AND r2.acct_session_id = r1.acct_session_id
          AND r2.acct_status_type = 'Stop'
          AND r2.event_timestamp >= r1.event_timestamp
      )
  )

[FIX - KHONG THEM INDEX MOI] Ban truoc subquery r2 chi loc theo
`acct_session_id` -- cot nay KHONG nam trong bat ky index nao (3
index hien co idx_msisdn_ts/idx_imsi_ts/idx_imei_ts deu khong phu
acct_session_id) -> Postgres phai Seq Scan TOAN BO cac partition cho
MOI row cua r1 de tim Stop tuong ung -> voi bang trieu record, vi
pham SLA p95<=100ms nghiem trong.

Thay vi them index moi cho acct_session_id, sua truc tiep cau SQL:
them dieu kien `r2.msisdn = $1` (hang so da biet tu tham so, khong
phai gia tri tuong quan) vao subquery r2. Vi moi ban ghi Start/Stop
cua CUNG 1 session luon co CUNG msisdn (session khong the "doi chu"
giua Start va Stop trong du lieu hop le), dieu kien nay khong lam
sai lech ket qua, nhung cho phep planner dung idx_msisdn_ts
(msisdn, event_timestamp DESC) DA CO SAN de gioi han subquery r2 chi
quet trong tap ban ghi CUA RIENG thue bao nay (thuong vai chuc/vai
tram dong trong toan bo lich su), thay vi toan bang.

Them dieu kien `r2.event_timestamp >= r1.event_timestamp` de:
  1. Dung dinh dung nghiep vu: 1 ban ghi Stop luon xay ra SAU (hoac
     cung luc) ban ghi Start tuong ung cua no, khong bao gio truoc.
  2. Giup planner uu tien quet theo thu tu DESC cua index, dung lai
     som hon khi tim thay Stop gan nhat.

SLA: p95 ≤ 100ms — nghiêm hơn SIM/Device Swap.
Đảm bảo bởi: idx_msisdn_ts (msisdn, event_timestamp DESC) — DÙNG
CHUNG cho cả r1 lẫn r2 (không cần index mới nào khác) + partition
pruning theo event_timestamp cho r1 (RANGE by month).
"""

from fastapi import APIRouter, Depends
import asyncpg

from api.schemas.number_verification import (
    NumberVerifyRequest,
    NumberVerifyResponse,
)
from api.dependencies.auth import verify_api_key
from api.dependencies.database import get_db

router = APIRouter(
    prefix="/number-verification/v0",
    tags=["Number Verification"],
    dependencies=[Depends(verify_api_key)],
)


_QUERY_ACTIVE_SESSION = """
    SELECT EXISTS (
        SELECT 1
        FROM radius_sessions r1
        WHERE r1.msisdn = $1
          AND r1.acct_status_type = 'Start'
          AND r1.event_timestamp >= NOW() - INTERVAL '24 hours'
          AND NOT EXISTS (
              SELECT 1
              FROM radius_sessions r2
              WHERE r2.msisdn = $1
                AND r2.acct_session_id = r1.acct_session_id
                AND r2.acct_status_type = 'Stop'
                AND r2.event_timestamp >= r1.event_timestamp
          )
    ) AS has_active_session
"""


@router.post(
    "/verify",
    response_model=NumberVerifyResponse,
    summary="Xác minh số điện thoại đang active trên mạng",
    description=(
        "Kiểm tra MSISDN có session đang active (Start chưa có Stop) "
        "trong 24h gần nhất trong bảng radius_sessions. "
        "**Lab simplification** (Quyết định #9): thay thế cho "
        "network-based authentication của CAMARA spec đầy đủ."
    ),
)
async def verify_number(
    body: NumberVerifyRequest,
    db: asyncpg.Connection = Depends(get_db),
) -> NumberVerifyResponse:
    """
    Xác minh MSISDN có session active trong 24h gần nhất.

    Lưu ý: MSISDN không tồn tại trong DB → trả verified=False
    (không phải 404) — theo CAMARA spec, số không tồn tại vẫn
    được coi là "not verified", không phải error.

    Args:
        body: phoneNumber (E.164) cần xác minh.
        db: asyncpg connection từ pool.

    Returns:
        NumberVerifyResponse: { devicePhoneNumberVerified: bool }
    """
    msisdn = str(body.phoneNumber)
    row = await db.fetchrow(_QUERY_ACTIVE_SESSION, msisdn)
    verified = row["has_active_session"] if row else False
    return NumberVerifyResponse(devicePhoneNumberVerified=verified)