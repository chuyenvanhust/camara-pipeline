from __future__ import annotations

from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import AnyHttpUrl, BaseModel, Field, model_validator

from api.schemas.common import PhoneNumber


class SubscriptionEventType(str, Enum):
    SIM_SWAP = "SIM_SWAP"
    DEVICE_SWAP = "DEVICE_SWAP"


class SubscriptionStatus(str, Enum):
    ACTIVE = "ACTIVE"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"


class SubscriptionCreate(BaseModel):
    phoneNumber: PhoneNumber | None = Field(
        default=None,
        description="Bỏ trống để đăng ký mọi thuê bao (any UE).",
    )
    eventType: SubscriptionEventType
    callbackUrl: AnyHttpUrl
    expiresAt: datetime | None = None

    @model_validator(mode="after")
    def require_timezone(self) -> "SubscriptionCreate":
        if self.expiresAt is not None and self.expiresAt.tzinfo is None:
            raise ValueError("expiresAt must include a timezone")
        return self


class SubscriptionUpdate(BaseModel):
    callbackUrl: AnyHttpUrl | None = None
    status: SubscriptionStatus | None = None
    expiresAt: datetime | None = None

    @model_validator(mode="after")
    def require_change(self) -> "SubscriptionUpdate":
        if not self.model_fields_set:
            raise ValueError("at least one subscription field must be supplied")
        if self.expiresAt is not None and self.expiresAt.tzinfo is None:
            raise ValueError("expiresAt must include a timezone")
        return self


class SubscriptionResponse(BaseModel):
    subscriptionId: UUID
    phoneNumber: PhoneNumber | None
    eventType: SubscriptionEventType
    callbackUrl: AnyHttpUrl
    status: SubscriptionStatus
    createdAt: datetime
    expiresAt: datetime | None
