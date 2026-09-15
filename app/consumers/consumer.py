import json
import logging
import uuid

from aiokafka import AIOKafkaConsumer
from aiokafka.structs import OffsetAndMetadata, TopicPartition
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumers.projection_service import apply_balance_projection
from app.consumers.service import mark_event_processed
from app.kafka.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_LEDGER_TOPIC,
)


logger = logging.getLogger(__name__)


class LedgerEventConsumer:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        group_id: str = "ledgerflow-consumer",
    ):
        self.session_factory = session_factory

        self.consumer = AIOKafkaConsumer(
            KAFKA_LEDGER_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda value: json.loads(
                value.decode("utf-8")
            ),
        )

        self._running = False

    async def start(self):
        await self.consumer.start()
        self._running = True

    async def stop(self):
        self._running = False
        await self.consumer.stop()

    async def run(self):
        await self.start()

        try:
            async for message in self.consumer:

                if not self._running:
                    break

                await self.process_message(message)

        finally:
            await self.stop()

    async def _commit_message_offset(self, message):
        """
        Commit only the offset immediately after this message.

        Kafka offsets represent the next message to consume, so
        we commit message.offset + 1.
        """
        topic_partition = TopicPartition(
            message.topic,
            message.partition,
        )

        await self.consumer.commit(
            {
                topic_partition: OffsetAndMetadata(
                    message.offset + 1,
                    "",
                )
            }
        )

    async def process_message(self, message):
        event = message.value

        event_id = uuid.UUID(
            event["event_id"]
        )

        event_type = event["event_type"]

        async with self.session_factory() as db:

            try:
                # -------------------------------------------------
                # Register the event and apply its side effect in
                # the same PostgreSQL transaction.
                # -------------------------------------------------
                registered = await mark_event_processed(
                    db,
                    event_id,
                    event_type,
                )

                if not registered:
                    logger.info(
                        "Skipping duplicate event %s",
                        event_id,
                    )

                    await db.rollback()

                    # The event has already been successfully
                    # processed, so acknowledge THIS Kafka message.
                    await self._commit_message_offset(message)

                    return

                await apply_balance_projection(
                    db,
                    event,
                )

                await db.commit()

            except Exception:
                await db.rollback()

                logger.exception(
                    "Failed to process event %s",
                    event_id,
                )

                # IMPORTANT:
                # Do not commit the Kafka offset.
                #
                # PostgreSQL rolled back, therefore Kafka will
                # redeliver the event.
                raise

        # ---------------------------------------------------------
        # PostgreSQL committed successfully.
        #
        # Only now acknowledge THIS exact Kafka message.
        # ---------------------------------------------------------
        await self._commit_message_offset(message)

        logger.info(
            "Successfully processed event %s",
            event_id,
        )