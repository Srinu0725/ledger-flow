import asyncio
import logging

from app.consumers.consumer import LedgerEventConsumer
from app.db.database import AsyncSessionLocal


logging.basicConfig(
    level=logging.INFO,
)


async def main():
    consumer = LedgerEventConsumer(
        session_factory=AsyncSessionLocal,
        group_id="ledgerflow-e2e",
    )

    try:
        await consumer.run()
    except KeyboardInterrupt:
        consumer._running = False

if __name__ == "__main__":
    asyncio.run(main())