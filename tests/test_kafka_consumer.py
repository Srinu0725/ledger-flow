import uuid
from decimal import Decimal

import pytest

from app.consumers.consumer import LedgerEventConsumer
from app.consumers.projection_model import AccountBalanceProjection


class FakeKafkaConsumer:
    def __init__(self, *args, **kwargs):
        self.committed = False
        self.committed_offsets = None
        self.started = False
        self.stopped = False

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def commit(self, offsets=None):
        self.committed = True
        self.committed_offsets = offsets


@pytest.fixture
def fake_kafka_consumer(monkeypatch):
    monkeypatch.setattr(
        "app.consumers.consumer.AIOKafkaConsumer",
        FakeKafkaConsumer,
    )


@pytest.mark.asyncio
async def test_consumer_processes_new_event(
    db_session_factory,
    fake_kafka_consumer,
):
    account_id = uuid.uuid4()
    event_id = uuid.uuid4()

    event = {
        "event_id": str(event_id),
        "aggregate_id": str(account_id),
        "event_type": "DEPOSIT_POSTED",
        "event_version": 1,
        "payload": {
            "account_id": str(account_id),
            "amount": "100.00",
        },
        "created_at": "2026-09-15T00:00:00+00:00",
    }

    message = type(
        "Message",
        (),
        {
            "value": event,
            "topic": "ledger-events",
            "partition": 0,
            "offset": 0,
        },
    )()

    consumer = LedgerEventConsumer(
        session_factory=db_session_factory,
    )

    await consumer.process_message(message)

    async with db_session_factory() as db:
        projection = await db.get(
            AccountBalanceProjection,
            account_id,
        )

        assert projection is not None
        assert projection.balance == Decimal("100.00")

    assert consumer.consumer.committed is True
    assert consumer.consumer.committed_offsets is not None


@pytest.mark.asyncio
async def test_consumer_duplicate_does_not_update_projection_again(
    db_session_factory,
    fake_kafka_consumer,
):
    account_id = uuid.uuid4()
    event_id = uuid.uuid4()

    event = {
        "event_id": str(event_id),
        "aggregate_id": str(account_id),
        "event_type": "DEPOSIT_POSTED",
        "event_version": 1,
        "payload": {
            "account_id": str(account_id),
            "amount": "100.00",
        },
        "created_at": "2026-09-15T00:00:00+00:00",
    }

    message = type(
        "Message",
        (),
        {
            "value": event,
            "topic": "ledger-events",
            "partition": 0,
            "offset": 0,
        },
    )()

    consumer = LedgerEventConsumer(
        session_factory=db_session_factory,
    )

    # First processing
    await consumer.process_message(message)

    # Same event delivered again
    await consumer.process_message(message)

    async with db_session_factory() as db:
        projection = await db.get(
            AccountBalanceProjection,
            account_id,
        )

        assert projection is not None

        # Must remain 100, not become 200
        assert projection.balance == Decimal("100.00")

    assert consumer.consumer.committed is True
    assert consumer.consumer.committed_offsets is not None


@pytest.mark.asyncio
async def test_consumer_commits_next_offset(
    db_session_factory,
    fake_kafka_consumer,
):
    account_id = uuid.uuid4()
    event_id = uuid.uuid4()

    event = {
        "event_id": str(event_id),
        "aggregate_id": str(account_id),
        "event_type": "DEPOSIT_POSTED",
        "event_version": 1,
        "payload": {
            "account_id": str(account_id),
            "amount": "50.00",
        },
        "created_at": "2026-09-15T00:00:00+00:00",
    }

    message = type(
        "Message",
        (),
        {
            "value": event,
            "topic": "ledger-events",
            "partition": 2,
            "offset": 41,
        },
    )()

    consumer = LedgerEventConsumer(
        session_factory=db_session_factory,
    )

    await consumer.process_message(message)

    assert consumer.consumer.committed is True

    committed_offsets = consumer.consumer.committed_offsets

    assert committed_offsets is not None
    assert len(committed_offsets) == 1

    topic_partition, offset_metadata = next(
        iter(committed_offsets.items())
    )

    assert topic_partition.topic == "ledger-events"
    assert topic_partition.partition == 2

    # Kafka commits the NEXT offset
    assert offset_metadata.offset == 42