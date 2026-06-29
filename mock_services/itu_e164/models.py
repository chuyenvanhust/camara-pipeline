from pydantic import BaseModel, Field
from typing import List, Optional, Dict
class OperatorInfo(BaseModel):
    prefix: str
    operator: str
    mnc: str
    type: str
class CountryCodeInfo(BaseModel):
    country_code: str
    country_name: str
    iso_alpha2: str
    iso_alpha3: str
    region: str
    min_subscriber_length: int
    max_subscriber_length: int
    trunk_prefix: str
    international_prefix: str
class SingleValidationRequest(BaseModel):
    phone_number: str = Field(..., description="Số điện thoại cần kiểm tra, ví dụ: +84912345678")
class BatchValidationRequest(BaseModel):
    phone_numbers: List[str] = Field(..., max_length=100)
class ValidationResult(BaseModel):
    phone_number: str
    valid: bool
    country_code: Optional[str] = None
    country_name: Optional[str] = None
    subscriber_number: Optional[str] = None
    operator: Optional[str] = None
    mnc: Optional[str] = None
    number_type: Optional[str] = None
    e164_format: Optional[str] = None

    national_format: Optional[str] = None
    error_code: Optional[str] = None
    error_detail: Optional[str] = None
class BatchValidationResponse(BaseModel):
    results: Dict[str, ValidationResult]
    total: int
    valid_count: int
    invalid_count: int