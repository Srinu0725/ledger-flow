import asyncio
import uuid
from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumers.model import ProcessedEvent
from app.consumers.projection_model import AccountBalanceProjection
from app.consumers.projection_service import (
    apply_balance_projection,
)
from app.consumers.service import (
    mark_event_processed,
)


@pytest.mark.asyncio
async def test_concurrent_duplicate_events_are_processed_once(db):
    account_id = uuid.uuid4()
    event_id = uuid.uuid4()

    event = {
        "event_id": str(event_id),
        "event_type": "DEPOSIT_POSTED",
        "payload": {
            "account_id": str(account_id),
            "amount": "1000.00",
        },
    }

    engine = db.bind

    SessionLocal = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async def process_event():
        async with SessionLocal() as session:
            registered = await mark_event_processed(
                session,
                event_id,
                event["event_type"],
            )

            if not registered:
                await session.rollback()
                return False

            await apply_balance_projection(
                session,
                event,
            )

            await session.commit()

            return True

    results = await asyncio.gather(
        process_event(),
        process_event(),
    )

    # Exactly one consumer should win.
    assert sorted(results) == [False, True]

    # ---------------------------------------------------------
    # Verify only one processed-event record exists.
    # ---------------------------------------------------------
    async with SessionLocal() as session:
        result = await session.execute(
            select(ProcessedEvent)
            .where(
                ProcessedEvent.event_id == event_id
            )
        )

        processed_events = list(
            result.scalars().all()
        )

        assert len(processed_events) == 1

    # ---------------------------------------------------------
    # Verify balance was applied exactly once.
    # ---------------------------------------------------------
    async with SessionLocal() as session:
        result = await session.execute(
            select(AccountBalanceProjection)
            .where(
                AccountBalanceProjection.account_id
                == account_id
            )
        )

        projection = result.scalar_one()

        assert projection.balance == Decimal("1000.00")