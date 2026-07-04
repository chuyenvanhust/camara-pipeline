"""
Pydantic schemas cho Number Verification API endpoint.

Tuân thủ CAMARA NumberVerification spec:
https://github.com/camaraproject/NumberVerification

1 endpoint:
  POST /number-verification/v0/verify → NumberVerifyResponse

Lưu ý scope đơn giản hóa (xem Quyết định Lab #9):
  Thay vì network-based auth (carrier xác nhận device đang dùng
  connection đó), logic lab kiểm tra MSISDN có active session
  trong 24h gần nhất trong radius_sessions.
"""

from pydantic import BaseModel
from api.schemas.common import PhoneNumber


class NumberVerifyRequest(BaseModel):
    """
    Request body cho POST /number-verification/v0/verify.

    Attributes:
        phoneNumber: MSISDN E.164 cần xác minh.
    """
    phoneNumber: PhoneNumber


class NumberVerifyResponse(BaseModel):
    """
    Response cho POST /number-verification/v0/verify.

    Attributes:
        devicePhoneNumberVerified: True nếu MSISDN có active session
            trong 24h gần nhất (có Start, chưa có Stop tương ứng).
    """
    devicePhoneNumberVerified: bool