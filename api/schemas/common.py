#common.py
import re
from typing import Any, Generator
from altair import value
from pydantic import BaseModel, Field
from pydantic_core import core_schema

# -----------------------------------------------------------------
# 1. Custom Type: PhoneNumber (Định dạng chuẩn ITU-T E.164)
# -----------------------------------------------------------------
class PhoneNumber(str):
    """
    Kiểu dữ liệu chuỗi số điện thoại tự động validate theo định dạng E.164.
    Quy tắc E.164: Bắt đầu bằng dấu '+', theo sau là mã quốc gia và số thuê bao (tổng từ 7 đến 15 ký tự số).
    Ví dụ hợp lệ: +84912345678, +14155552671
    """
    E164_REGEX = re.compile(r"^\+[1-9]\d{6,14}$")

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: Any
    ) -> core_schema.CoreSchema:
        """ Định nghĩa cách Pydantic parse và validate kiểu dữ liệu này """
        return core_schema.no_info_after_validator_function(
            cls.validate,
            core_schema.str_schema(),
        )

    @classmethod
    def validate(cls, value: str) -> "PhoneNumber":
        """ Hàm kiểm tra chuỗi đầu vào có khớp regex E.164 hay không """
        value = value.strip()
        if not cls.E164_REGEX.match(value):
            raise ValueError(
                f"Invalid E.164 phone number format: '{value}'. "
                "Expected '+' followed by 7-15 digits, e.g. +84971234567"
            )
        return cls(value)                            
        

# -----------------------------------------------------------------
# 2. Standard Error Response (Khớp contract của errors.py)
# -----------------------------------------------------------------
class ErrorResponse(BaseModel):
    """ Schema phản hồi lỗi chuẩn của API tầng trên, đồng bộ với mock services """
    error: str = Field(..., description="Mã lỗi viết hoa, VD: INVALID_ARGUMENT, NOT_FOUND")
    message: str = Field(..., description="Mô tả chi tiết nguyên nhân lỗi")
    request_id: str = Field(..., description="UUID định danh duy nhất cho request sự cố")