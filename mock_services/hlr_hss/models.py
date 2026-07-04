#mock_services\hlr_hss\models.py
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Literal
from datetime import datetime

class ServiceProfile(BaseModel):
    data_enabled: bool = True
    roaming_enabled: bool = False
    volte_enabled: bool = True

class SubscriberProfile(BaseModel):
    imsi: str
    msisdn: str
    status: str = "active"
    mcc: str
    mnc: str
    operator: str
    service_profile: ServiceProfile
    registered_at: datetime
    last_updated: datetime

class MsisdnHistoryEntry(BaseModel):
    msisdn: str
    assigned_at: datetime
    unassigned_at: Optional[datetime] = None
    is_current: bool

class MsisdnHistoryResponse(BaseModel):
    imsi: str
    history: List[MsisdnHistoryEntry]
    total: int

class ImsiHistoryEntry(BaseModel):
    imsi: str
    assigned_at: datetime
    unassigned_at: Optional[datetime] = None
    is_current: bool
    swap_reason: str = "initial_activation"

class ImsiHistoryResponse(BaseModel):
    msisdn: str
    history: List[ImsiHistoryEntry]
    total: int
    sim_swap_count: int
    latest_swap_at: Optional[datetime] = None

class LookupQuery(BaseModel):
    type: Literal["imsi", "msisdn"]
    value: str

class BatchLookupItemResult(BaseModel):
    query: LookupQuery
    found: bool
    subscriber: Optional[SubscriberProfile] = None

class BatchLookupRequest(BaseModel):
    lookups: List[LookupQuery] = Field(..., max_length=500)

class BatchLookupResponse(BaseModel):
    results: List[BatchLookupItemResult]
    total: int
    found: int
    not_found: int

class IdentityHistoryEntry(BaseModel):
    value: str  # imsi hoặc imei tùy identity_type
    assigned_at: datetime
    unassigned_at: Optional[datetime] = None
    is_current: bool
    swap_reason: str = "initial_activation"

class IdentityHistoryResponse(BaseModel):
    msisdn: str
    identity_type: Literal["imsi", "imei"]
    history: List[IdentityHistoryEntry]
    total: int
    swap_count: int
    latest_swap_at: Optional[datetime] = None

class BatchHistoryRequest(BaseModel):
    msisdns: List[str] = Field(..., max_length=1000)
    identity_type: Literal["imsi", "imei"]

class BatchHistoryItemResult(BaseModel):
    msisdn: str
    found: bool
    history: Optional[IdentityHistoryResponse] = None

class BatchHistoryResponse(BaseModel):
    results: List[BatchHistoryItemResult]
    total: int
    found: int
    not_found: int