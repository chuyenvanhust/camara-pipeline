from datetime import datetime

from pydantic import BaseModel

from api.schemas.common import PhoneNumber


class IPMsisdnResponse(BaseModel):
    ipAddress: str
    phoneNumber: PhoneNumber
    nasIdentifier: str | None = None
    eventTimestamp: datetime
