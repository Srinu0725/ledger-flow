import asyncio
import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from app.outbox.model import OutboxEvent, OutboxStatus
from app.outbox.service import create_outbox_event
from app.outbox.worker import OutboxWorker


class FakeKafkaProducer:
    def __init__(self):
        self.published_events = []
        self._lock = asyncio.Lock()

    async def start(self):
        pass

    async def stop(self):
        pass

    async def send(self, topic: str, event: dict):
        await asyncio.sleep(0.01)

        async with self._lock:
            self.published_events.append(event)

        return None


@pytest.mark.asyncio
async def test_multiple_workers_do_not_process_same_event(db):

    event_count = 20

    engine = db.bind

    SessionLocal = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with SessionLocal() as session:
        for _ in range(event_count):
            await create_outbox_event(
                db=session,
                aggregate_id=uuid.uuid4(),
                event_type="TEST_EVENT",
                payload={
                    "value": "test",
                },
            )

        await session.commit()

    producer = FakeKafkaProducer()

    worker_1 = OutboxWorker(
        session_factory=SessionLocal,
        producer=producer,
        poll_interval=0.01,
        batch_size=20,
    )

    worker_2 = OutboxWorker(
        session_factory=SessionLocal,
        producer=producer,
        poll_interval=0.01,
        batch_size=20,
    )

    await asyncio.gather(
        worker_1.process_batch(),
        worker_2.process_batch(),
    )

    assert len(producer.published_events) == event_count

    event_ids = [
        event["event_id"]
        for event in producer.published_events
    ]

    assert len(event_ids) == len(set(event_ids))

    async with SessionLocal() as session:
        result = await session.execute(
            select(OutboxEvent)
        )

        events = list(result.scalars().all())

    assert len(events) == event_count

    assert all(
        event.status == OutboxStatus.PUBLISHED
        for event in events
    )