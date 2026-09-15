import uuid
from datetime import datetime, timedelta, timezone

import pytest

from app.outbox.model import OutboxStatus
from app.outbox.service import (
    create_outbox_event,
    claim_pending_events,
    mark_failed,
    mark_published,
)


@pytest.mark.asyncio
async def test_failed_event_is_scheduled_for_retry(db):
    event = await create_outbox_event(
        db=db,
        aggregate_id=uuid.uuid4(),
        event_type="TEST_EVENT",
        payload={"value": "test"},
    )

    await db.commit()

    await mark_failed(
        db,
        event,
        "Kafka unavailable",
    )

    await db.commit()

    assert event.status == OutboxStatus.PENDING
    assert event.retry_count == 1
    assert event.next_retry_at is not None
    assert event.last_error == "Kafka unavailable"

    assert event.next_retry_at > datetime.now(timezone.utc)


@pytest.mark.asyncio
async def test_event_is_not_retried_before_next_retry_at(db):
    event = await create_outbox_event(
        db=db,
        aggregate_id=uuid.uuid4(),
        event_type="TEST_EVENT",
        payload={"value": "test"},
    )

    await db.commit()

    event.next_retry_at = (
        datetime.now(timezone.utc)
        + timedelta(minutes=10)
    )

    await db.commit()

    events = await claim_pending_events(db)

    assert event not in events


@pytest.mark.asyncio
async def test_event_is_retried_after_next_retry_at(db):
    event = await create_outbox_event(
        db=db,
        aggregate_id=uuid.uuid4(),
        event_type="TEST_EVENT",
        payload={"value": "test"},
    )

    await db.commit()

    event.next_retry_at = (
        datetime.now(timezone.utc)
        - timedelta(seconds=1)
    )

    await db.commit()

    events = await claim_pending_events(db)

    assert event in events


@pytest.mark.asyncio
async def test_max_retries_marks_event_failed(db):
    event = await create_outbox_event(
        db=db,
        aggregate_id=uuid.uuid4(),
        event_type="TEST_EVENT",
        payload={"value": "test"},
    )

    await db.commit()

    for _ in range(5):
        await mark_failed(
            db,
            event,
            "Kafka unavailable",
        )

    await db.commit()

    assert event.status == OutboxStatus.FAILED
    assert event.retry_count == 5
    assert event.next_retry_at is None


@pytest.mark.asyncio
async def test_published_event_clears_retry_state(db):
    event = await create_outbox_event(
        db=db,
        aggregate_id=uuid.uuid4(),
        event_type="TEST_EVENT",
        payload={"value": "test"},
    )

    await db.commit()

    await mark_failed(
        db,
        event,
        "Kafka unavailable",
    )

    await mark_published(
        db,
        event,
    )

    await db.commit()

    assert event.status == OutboxStatus.PUBLISHED
    assert event.retry_count == 1
    assert event.next_retry_at is None
    assert event.last_error is None
    assert event.published_at is not None