#mock_services\gsma_tac\models.py
from pydantic import BaseModel, Field
from typing import List, Optional, Dict

class TacRecord(BaseModel):
    tac: str = Field(..., description="Mã TAC - đúng 6 chữ số")
    manufacturer: str
    model: str
    device_type: str
    operating_system: str
    band_support: List[str]
    approved_date: str
    status: str

class TacLookupResponse(BaseModel):
    tac: str
    manufacturer: str
    model: str
    device_type: str
    operating_system: str
    band_support: List[str]
    approved_date: str
    status: str

class BatchRequest(BaseModel):
    tac_codes: List[str] = Field(..., max_length=100) # Đổi max_items thành max_length

class BatchResultItem(BaseModel):
    found: bool
    manufacturer: Optional[str] = None
    model: Optional[str] = None
    device_type: Optional[str] = None
    status: Optional[str] = None

class BatchResponse(BaseModel):
    results: Dict[str, BatchResultItem]
    total: int
    found: int
    not_found: int

class PaginatedTacResponse(BaseModel):
    items: List[TacRecord]
    total: int
    page: int
    page_size: int
    pages: int

class HealthResponse(BaseModel):
    status: str
    service: str
    records: int
    uptime_seconds: float