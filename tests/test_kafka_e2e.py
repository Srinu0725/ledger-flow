import asyncio
import json
import uuid
from decimal import Decimal

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from sqlalchemy import select

from app.consumers.model import ProcessedEvent
from app.consumers.projection_model import AccountBalanceProjection
from app.db.database import AsyncSessionLocal
from app.kafka.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_LEDGER_TOPIC,
)


async def main():
    event_id = uuid.uuid4()
    account_id = uuid.uuid4()

    event = {
        "event_id": str(event_id),
        "aggregate_id": str(account_id),
        "event_type": "DEPOSIT_POSTED",
        "event_version": 1,
        "payload": {
            "account_id": str(account_id),
            "amount": "1000.00",
            "currency": "INR",
        },
    }

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode(
            "utf-8"
        ),
    )

    consumer = AIOKafkaConsumer(
        KAFKA_LEDGER_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=f"e2e-test-{uuid.uuid4()}",
        auto_offset_reset="latest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )

    producer_started = False
    consumer_started = False

    try:
        await producer.start()
        producer_started = True

        await consumer.start()
        consumer_started = True

        print(f"Sending event: {event_id}")

        await producer.send_and_wait(
            KAFKA_LEDGER_TOPIC,
            event,
        )

        message = await asyncio.wait_for(
            consumer.getone(),
            timeout=10,
        )

        received_event = message.value

        print(
            f"Received event: "
            f"{received_event['event_id']}"
        )

        assert (
            received_event["event_id"]
            == str(event_id)
        )

        # -----------------------------------------------------
        # Apply the same logic used by the real consumer.
        # -----------------------------------------------------
        async with AsyncSessionLocal() as db:

            from app.consumers.service import (
                mark_event_processed,
            )

            from app.consumers.projection_service import (
                apply_balance_projection,
            )

            registered = await mark_event_processed(
                db,
                event_id,
                received_event["event_type"],
            )

            assert registered is True

            await apply_balance_projection(
                db,
                received_event,
            )

            await db.commit()

        await consumer.commit()

        # -----------------------------------------------------
        # Verify PostgreSQL state.
        # -----------------------------------------------------
        async with AsyncSessionLocal() as db:

            result = await db.execute(
                select(ProcessedEvent)
                .where(
                    ProcessedEvent.event_id
                    == event_id
                )
            )

            processed = result.scalar_one_or_none()

            assert processed is not None

            result = await db.execute(
                select(AccountBalanceProjection)
                .where(
                    AccountBalanceProjection.account_id
                    == account_id
                )
            )

            projection = result.scalar_one()

            assert (
                projection.balance
                == Decimal("1000.00")
            )

        print("E2E Kafka test passed")
        print(
            f"Projection balance: "
            f"{projection.balance}"
        )

    finally:
        # Stop Kafka clients before the event loop closes.
        if consumer_started:
            await consumer.stop()

        if producer_started:
            await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
