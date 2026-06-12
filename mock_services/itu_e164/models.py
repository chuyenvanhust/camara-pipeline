from pydantic import BaseModel, Field
from typing import List, Optional, Dict

class ValidationResult(BaseModel):
    is_valid: bool
    country_code: Optional[str] = None
    country_name: Optional[str] = None
    operator: Optional[str] = None
    error_code: Optional[str] = None  # INVALID_FORMAT, UNKNOWN_COUNTRY, UNKNOWN_OPERATOR
    message: Optional[str] = None

class SingleValidationRequest(BaseModel):
    phone_number: str = Field(..., description="Số điện thoại cần kiểm tra, ví dụ: +84912345678")

class BatchValidationRequest(BaseModel):
    phone_numbers: List[str] = Field(..., max_length=100)

class BatchValidationResponse(BaseModel):
    results: Dict[str, ValidationResult]
    total: int
    valid_count: int
    invalid_count: int