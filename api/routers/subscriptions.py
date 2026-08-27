from __future__ import annotations

from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query, Response, status

from api.dependencies.auth import verify_api_key
from api.dependencies.database import get_db
from api.schemas.subscription import (
    SubscriptionCreate,
    SubscriptionEventType,
    SubscriptionResponse,
    SubscriptionStatus,
    SubscriptionUpdate,
)


router = APIRouter(
    prefix="/subscriptions",
    tags=["Subscriptions"],
    dependencies=[Depends(verify_api_key)],
)


def _response(row: asyncpg.Record) -> SubscriptionResponse:
    return SubscriptionResponse(
        subscriptionId=row["subscription_id"],
        phoneNumber=row["msisdn"],
        eventType=row["event_type"],
        callbackUrl=row["callback_url"],
        status=row["status"],
        createdAt=row["created_at"],
        expiresAt=row["expires_at"],
    )


@router.post("", response_model=SubscriptionResponse, status_code=status.HTTP_201_CREATED)
async def create_subscription(
    body: SubscriptionCreate,
    db: asyncpg.Connection = Depends(get_db),
) -> SubscriptionResponse:
    row = await db.fetchrow(
        """
        INSERT INTO subscription(msisdn,event_type,callback_url,status,expires_at)
        VALUES($1,$2,$3,'ACTIVE',$4)
        RETURNING *
        """,
        str(body.phoneNumber) if body.phoneNumber else None,
        body.eventType.value,
        str(body.callbackUrl),
        body.expiresAt,
    )
    return _response(row)


@router.get("", response_model=list[SubscriptionResponse])
async def list_subscriptions(
    eventType: SubscriptionEventType | None = None,
    subscriptionStatus: SubscriptionStatus | None = Query(default=None, alias="status"),
    limit: int = Query(default=100, ge=1, le=1000),
    db: asyncpg.Connection = Depends(get_db),
) -> list[SubscriptionResponse]:
    rows = await db.fetch(
        """
        SELECT * FROM subscription
        WHERE ($1::varchar IS NULL OR event_type=$1)
          AND ($2::varchar IS NULL OR status=$2)
        ORDER BY created_at DESC
        LIMIT $3
        """,
        eventType.value if eventType else None,
        subscriptionStatus.value if subscriptionStatus else None,
        limit,
    )
    return [_response(row) for row in rows]


@router.get("/{subscription_id}", response_model=SubscriptionResponse)
async def get_subscription(
    subscription_id: UUID,
    db: asyncpg.Connection = Depends(get_db),
) -> SubscriptionResponse:
    row = await db.fetchrow(
        "SELECT * FROM subscription WHERE subscription_id=$1", subscription_id
    )
    if row is None:
        raise HTTPException(status_code=404, detail="subscription not found")
    return _response(row)


@router.patch("/{subscription_id}", response_model=SubscriptionResponse)
async def update_subscription(
    subscription_id: UUID,
    body: SubscriptionUpdate,
    db: asyncpg.Connection = Depends(get_db),
) -> SubscriptionResponse:
    row = await db.fetchrow(
        """
        UPDATE subscription
        SET callback_url=COALESCE($2,callback_url),
            status=COALESCE($3,status),
            expires_at=CASE WHEN $4 THEN $5 ELSE expires_at END
        WHERE subscription_id=$1
        RETURNING *
        """,
        subscription_id,
        str(body.callbackUrl) if body.callbackUrl else None,
        body.status.value if body.status else None,
        "expiresAt" in body.model_fields_set,
        body.expiresAt,
    )
    if row is None:
        raise HTTPException(status_code=404, detail="subscription not found")
    return _response(row)


@router.delete("/{subscription_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_subscription(
    subscription_id: UUID,
    db: asyncpg.Connection = Depends(get_db),
) -> Response:
    result = await db.execute(
        "UPDATE subscription SET status='CANCELLED' WHERE subscription_id=$1",
        subscription_id,
    )
    if result == "UPDATE 0":
        raise HTTPException(status_code=404, detail="subscription not found")
    return Response(status_code=status.HTTP_204_NO_CONTENT)
