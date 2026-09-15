import uuid
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.outbox.model import OutboxEvent, OutboxStatus
from app.outbox.service import create_outbox_event
from app.outbox.worker import OutboxWorker


class FailingKafkaProducer:
    async def start(self):
        pass

    async def stop(self):
        pass

    async def send(self, topic: str, event: dict):
        raise RuntimeError("Kafka unavailable")


class SuccessfulKafkaProducer:
    def __init__(self):
        self.published_events = []

    async def start(self):
        pass

    async def stop(self):
        pass

    async def send(self, topic: str, event: dict):
        self.published_events.append(event)


async def create_test_event(db):
    event = await create_outbox_event(
        db=db,
        aggregate_id=uuid.uuid4(),
        event_type="TEST_EVENT",
        payload={
            "value": "test",
        },
    )

    await db.commit()

    return event


@pytest.mark.asyncio
async def test_worker_schedules_retry_after_kafka_failure(db):
    engine = db.bind

    SessionLocal = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    await create_test_event(db)

    producer = FailingKafkaProducer()

    worker = OutboxWorker(
        session_factory=SessionLocal,
        producer=producer,
        poll_interval=0.01,
        batch_size=10,
    )

    processed = await worker.process_batch()

    assert processed == 0

    async with SessionLocal() as session:
        result = await session.execute(
            select(OutboxEvent)
        )

        event = result.scalars().first()

    assert event is not None
    assert event.status == OutboxStatus.PENDING
    assert event.retry_count == 1
    assert event.next_retry_at is not None
    assert event.last_error == "Kafka unavailable"

    assert event.next_retry_at > datetime.now(timezone.utc)


@pytest.mark.asyncio
async def test_worker_skips_event_before_retry_time(db):
    engine = db.bind

    SessionLocal = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    event = await create_test_event(db)

    event.next_retry_at = (
        datetime.now(timezone.utc)
        + timedelta(minutes=10)
    )

    await db.commit()

    producer = SuccessfulKafkaProducer()

    worker = OutboxWorker(
        session_factory=SessionLocal,
        producer=producer,
        poll_interval=0.01,
        batch_size=10,
    )

    processed = await worker.process_batch()

    assert processed == 0
    assert producer.published_events == []


@pytest.mark.asyncio
async def test_worker_retries_and_publishes_after_retry_time(db):
    engine = db.bind

    SessionLocal = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    event = await create_test_event(db)

    event.retry_count = 1
    event.next_retry_at = (
        datetime.now(timezone.utc)
        - timedelta(seconds=1)
    )
    event.last_error = "Previous Kafka failure"

    await db.commit()

    producer = SuccessfulKafkaProducer()

    worker = OutboxWorker(
        session_factory=SessionLocal,
        producer=producer,
        poll_interval=0.01,
        batch_size=10,
    )

    processed = await worker.process_batch()

    assert processed == 1
    assert len(producer.published_events) == 1

    published_event = producer.published_events[0]

    assert published_event["event_id"] == str(event.event_id)

    async with SessionLocal() as session:
        result = await session.execute(
            select(OutboxEvent)
            .where(
                OutboxEvent.event_id == event.event_id
            )
        )

        updated_event = result.scalar_one()

    assert updated_event.status == OutboxStatus.PUBLISHED
    assert updated_event.retry_count == 1
    assert updated_event.next_retry_at is None
    assert updated_event.last_error is None
    assert updated_event.published_at is not None


@pytest.mark.asyncio
async def test_worker_event_becomes_failed_after_max_retries(db):
    engine = db.bind

    SessionLocal = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    event = await create_test_event(db)

    event.retry_count = 4
    event.next_retry_at = (
        datetime.now(timezone.utc)
        - timedelta(seconds=1)
    )

    await db.commit()

    producer = FailingKafkaProducer()

    worker = OutboxWorker(
        session_factory=SessionLocal,
        producer=producer,
        poll_interval=0.01,
        batch_size=10,
    )

    processed = await worker.process_batch()

    assert processed == 0

    async with SessionLocal() as session:
        result = await session.execute(
            select(OutboxEvent)
            .where(
                OutboxEvent.event_id == event.event_id
            )
        )

        updated_event = result.scalar_one()

    assert updated_event.status == OutboxStatus.FAILED
    assert updated_event.retry_count == 5
    assert updated_event.next_retry_at is None
    assert updated_event.last_error == "Kafka unavailable"