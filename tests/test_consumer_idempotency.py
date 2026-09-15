import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumers.model import ProcessedEvent
from app.consumers.service import (
    is_event_processed,
    mark_event_processed,
)


@pytest.mark.asyncio
async def test_event_is_not_processed_initially(db):
    event_id = uuid.uuid4()

    assert await is_event_processed(
        db,
        event_id,
    ) is False


@pytest.mark.asyncio
async def test_event_can_be_marked_processed(db):
    event_id = uuid.uuid4()

    created = await mark_event_processed(
        db,
        event_id,
        "TEST_EVENT",
    )

    await db.commit()

    assert created is True

    assert await is_event_processed(
        db,
        event_id,
    ) is True


@pytest.mark.asyncio
async def test_duplicate_event_is_ignored(db):
    event_id = uuid.uuid4()

    first = await mark_event_processed(
        db,
        event_id,
        "TEST_EVENT",
    )

    await db.commit()

    second = await mark_event_processed(
        db,
        event_id,
        "TEST_EVENT",
    )

    assert first is True
    assert second is False


@pytest.mark.asyncio
async def test_duplicate_event_creates_only_one_record(db):
    event_id = uuid.uuid4()

    await mark_event_processed(
        db,
        event_id,
        "TEST_EVENT",
    )

    await db.commit()

    async_session = async_sessionmaker(
        bind=db.bind,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with async_session() as session:
        result = await session.execute(
            select(ProcessedEvent).where(
                ProcessedEvent.event_id == event_id
            )
        )

        events = list(result.scalars().all())

        assert len(events) == 1