import asyncio
import uuid
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.consumers.projection_model import AccountBalanceProjection
from app.consumers.projection_service import apply_balance_projection


@pytest.mark.asyncio
async def test_concurrent_projection_updates(db):
    account_id = uuid.uuid4()

    events = [
        {
            "event_id": str(uuid.uuid4()),
            "aggregate_id": str(uuid.uuid4()),
            "event_type": "DEPOSIT_POSTED",
            "event_version": 1,
            "payload": {
                "account_id": str(account_id),
                "amount": "10.00",
            },
            "created_at": "2026-01-01T00:00:00+00:00",
        }
        for _ in range(100)
    ]

    async def apply_event(event):
        async with AsyncSession(
            bind=db.bind,
            expire_on_commit=False,
        ) as session:
            await apply_balance_projection(session, event)
            await session.commit()

    await asyncio.gather(
        *(apply_event(event) for event in events)
    )

    result = await db.execute(
        select(AccountBalanceProjection).where(
            AccountBalanceProjection.account_id == account_id
        )
    )

    projection = result.scalar_one()

    assert projection.balance == Decimal("1000.00")