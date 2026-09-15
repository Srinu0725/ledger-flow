import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.outbox.model import OutboxEvent, OutboxStatus
from app.outbox.service import (
    create_outbox_event,
    claim_pending_events,
    recover_stale_events,
)


@pytest.mark.asyncio
async def test_stale_processing_event_is_recovered(db):
    engine = db.bind

    SessionLocal = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    # ---------------------------------------------------------
    # Create a pending event
    # ---------------------------------------------------------
    async with SessionLocal() as session:
        event = await create_outbox_event(
            db=session,
            aggregate_id=uuid.uuid4(),
            event_type="TEST_EVENT",
            payload={"value": "test"},
        )

        await session.commit()

    # ---------------------------------------------------------
    # Claim it
    #
    # PENDING -> PROCESSING
    # ---------------------------------------------------------
    async with SessionLocal() as session:
        claimed = await claim_pending_events(
            session,
            limit=1,
        )

        assert len(claimed) == 1
        assert claimed[0].status == OutboxStatus.PROCESSING
        assert claimed[0].claimed_at is not None

    # ---------------------------------------------------------
    # Simulate worker crash by making claimed_at old
    # ---------------------------------------------------------
    async with SessionLocal() as session:
        result = await session.execute(
            select(OutboxEvent)
        )

        event = result.scalars().first()

        event.claimed_at = (
            datetime.now(timezone.utc)
            - timedelta(seconds=60)
        )

        await session.commit()

    # ---------------------------------------------------------
    # Recover stale event
    #
    # PROCESSING -> PENDING
    # ---------------------------------------------------------
    async with SessionLocal() as session:
        recovered = await recover_stale_events(
            session,
            limit=100,
        )

        assert recovered == 1

    # ---------------------------------------------------------
    # Verify final state
    # ---------------------------------------------------------
    async with SessionLocal() as session:
        result = await session.execute(
            select(OutboxEvent)
        )

        event = result.scalars().first()

        assert event.status == OutboxStatus.PENDING
        assert event.claimed_at is None