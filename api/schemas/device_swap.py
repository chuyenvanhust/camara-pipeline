#api\schemas\device_swap.py
"""
Pydantic schemas cho Device Swap API endpoints.

Device Swap là custom extension — CAMARA chưa có spec chính thức.
Thiết kế theo đúng pattern của SIM Swap (xem ADR-005):
  - Thay IMSI → IMEI
  - Thay latestSimChange → latestDeviceChange
  - Thay swapped → deviceSwapped

2 endpoint:
  POST /device-swap/v0/check           → DeviceSwapCheckResponse
  POST /device-swap/v0/retrieve-date   → DeviceSwapRetrieveDateResponse
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field
from api.schemas.common import PhoneNumber


class DeviceSwapCheckRequest(BaseModel):
    """
    Request body chung cho cả check và retrieve-date.

    Attributes:
        phoneNumber: MSISDN E.164 của thuê bao.
        maxAge: Số ngày nhìn lại (mặc định 30, tối thiểu 0).
    """
    phoneNumber: PhoneNumber
    maxAge: Optional[int] = Field(
        default=30,
        ge=0,
        description="Số ngày nhìn lại để detect device swap.",
    )


class DeviceSwapCheckResponse(BaseModel):
    """
    Response cho POST /device-swap/v0/check.

    Attributes:
        deviceSwapped: True nếu IMEI thay đổi trong khoảng maxAge ngày.
    """
    deviceSwapped: bool


class DeviceSwapRetrieveDateResponse(BaseModel):
    """
    Response cho POST /device-swap/v0/retrieve-date.

    Attributes:
        latestDeviceChange: Thời điểm đổi thiết bị gần nhất (ISO 8601).
                            None nếu không có thay đổi trong lịch sử.
    """
    latestDeviceChange: Optional[datetime] = None