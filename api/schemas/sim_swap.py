#api\schemas\sim_swap.py
"""
Pydantic schemas cho SIM Swap API endpoints.

Tuân thủ CAMARA SimSwap spec:
https://github.com/camaraproject/SimSwap

2 endpoint:
  POST /sim-swap/v0/check           → SimSwapCheckRequest  → SimSwapCheckResponse
  POST /sim-swap/v0/retrieve-date   → SimSwapCheckRequest  → SimSwapRetrieveDateResponse

Cùng Request schema cho cả 2 endpoint (maxAge optional).
Response schema khác nhau:
  - check:         { swapped: bool }
  - retrieve-date: { latestSimChange: datetime | null }
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field
from api.schemas.common import PhoneNumber


class SimSwapCheckRequest(BaseModel):
    """
    Request body chung cho cả check và retrieve-date.

    Attributes:
        phoneNumber: MSISDN định dạng E.164 của thuê bao cần kiểm tra.
        maxAge: Khoảng thời gian nhìn lại (ngày). Nếu không truyền,
                mặc định 30 ngày. API trả swapped=True nếu có SIM Swap
                xảy ra trong khoảng [now - maxAge days, now].
    """
    phoneNumber: PhoneNumber
    maxAge: Optional[int] = Field(
        default=30,
        ge=0,
        description="Số ngày nhìn lại. 0 = chỉ kiểm tra trong ngày hiện tại.",
    )


class SimSwapCheckResponse(BaseModel):
    """
    Response cho POST /sim-swap/v0/check.

    Attributes:
        swapped: True nếu SIM Swap xảy ra trong khoảng maxAge ngày qua.
    """
    swapped: bool


class SimSwapRetrieveDateResponse(BaseModel):
    """
    Response cho POST /sim-swap/v0/retrieve-date.

    Attributes:
        latestSimChange: Thời điểm SIM Swap gần nhất (ISO 8601, UTC).
                         None nếu không có SIM Swap nào trong lịch sử.
    """
    latestSimChange: Optional[datetime] = None