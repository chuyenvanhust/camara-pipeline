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
        WHERE r2.acct_session_id = r1.acct_session_id
          AND r2.acct_status_type = 'Stop'
      )
  )

SLA: p95 ≤ 100ms — nghiêm hơn SIM/Device Swap.
Đảm bảo bởi: index idx_msisdn_ts (msisdn, event_timestamp DESC)
+ query chỉ scan partition tháng hiện tại (RANGE partition by month).
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

# Dùng EXISTS thay vì COUNT(*) — dừng scan ngay khi tìm thấy 1 row,
# hiệu quả hơn khi index đã có, đảm bảo SLA 100ms p95.
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
              WHERE r2.acct_session_id = r1.acct_session_id
                AND r2.acct_status_type = 'Stop'
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
    row = await db.fetchrow(_QUERY_ACTIVE_SESSION, str(body.phoneNumber))
    verified = row["has_active_session"] if row else False
    return NumberVerifyResponse(devicePhoneNumberVerified=verified)