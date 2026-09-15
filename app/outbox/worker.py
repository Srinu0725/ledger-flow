import asyncio
import logging

from sqlalchemy.ext.asyncio import async_sessionmaker, AsyncSession

from app.outbox.service import (
    claim_pending_events,
    recover_stale_events,
    mark_published,
    mark_failed,
)
from app.kafka.config import KAFKA_LEDGER_TOPIC
from app.kafka.producer import KafkaProducer


logger = logging.getLogger(__name__)


class OutboxWorker:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        producer: KafkaProducer,
        poll_interval: float = 1.0,
        batch_size: int = 100,
    ):
        self.session_factory = session_factory
        self.producer = producer
        self.poll_interval = poll_interval
        self.batch_size = batch_size

        self._running = False


    async def run(self):
        self._running = True

        logger.info("Outbox worker starting")

        try:
            logger.info("Starting Kafka producer")

            await self.producer.start()

            logger.info("Kafka producer started")

            while self._running:
                logger.info("Polling outbox")

                processed = await self.process_batch()

                logger.info(
                    "Outbox batch processed: %s events",
                    processed,
                )

                if processed == 0:
                    await asyncio.sleep(
                        self.poll_interval
                    )

        except Exception:
            logger.exception(
                "Outbox worker crashed"
            )
            raise

        finally:
            logger.info(
                "Stopping Kafka producer"
            )

            await self.producer.stop()

            logger.info(
                "Outbox worker stopped"
            )



    async def process_batch(self) -> int:
    # ---------------------------------------------------------
    # STEP 0:
    # Recover events abandoned by crashed workers.
    # ---------------------------------------------------------
        async with self.session_factory() as db:
            await recover_stale_events(
                db,
                limit=self.batch_size,
            )

        # ---------------------------------------------------------
        # STEP 1:
        # Claim new events.
        #
        # PENDING -> PROCESSING
        # ---------------------------------------------------------
        async with self.session_factory() as db:
            events = await claim_pending_events(
                db,
                limit=self.batch_size,
            )

        if not events:
            return 0

        processed = 0

        # ---------------------------------------------------------
        # STEP 2:
        # Publish to Kafka.
        # ---------------------------------------------------------
        for event in events:

            payload = {
                "event_id": str(event.event_id),
                "aggregate_id": str(event.aggregate_id),
                "event_type": event.event_type,
                "event_version": event.event_version,
                "payload": event.payload,
                "created_at": event.created_at.isoformat(),
            }

            try:
                await self.producer.send(
                    KAFKA_LEDGER_TOPIC,
                    payload,
                )

            except Exception as exc:
                logger.exception(
                    "Failed to publish outbox event %s",
                    event.event_id,
                )

                async with self.session_factory() as db:
                    result = await db.get(
                        type(event),
                        event.event_id,
                    )

                    if result is not None:
                        await mark_failed(
                            db,
                            result,
                            str(exc),
                        )

                        await db.commit()

                continue

            # -----------------------------------------------------
            # Kafka succeeded.
            #
            # PROCESSING -> PUBLISHED
            # -----------------------------------------------------
            async with self.session_factory() as db:
                result = await db.get(
                    type(event),
                    event.event_id,
                )

                if result is not None:
                    await mark_published(
                        db,
                        result,
                    )

                    await db.commit()

            processed += 1

        return processed

    def stop(self):
        self._running = False