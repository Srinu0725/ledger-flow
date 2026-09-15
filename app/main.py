import asyncio
import logging

from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

from app.accounts.router import router as accounts_router
from app.ledger.router import router as ledger_router
from app.db.redis import redis_client
from app.db.database import AsyncSessionLocal
from app.kafka.producer import KafkaProducer
from app.outbox.worker import OutboxWorker


logger = logging.getLogger(__name__)


app = FastAPI(
    title="LedgerFlow",
    version="1.0.0",
)


# ---------------------------------------------------------
# Prometheus Metrics
# ---------------------------------------------------------

instrumentator = Instrumentator()

instrumentator.instrument(app)

instrumentator.expose(
    app,
    endpoint="/metrics",
)


# ---------------------------------------------------------
# Routers
# ---------------------------------------------------------

app.include_router(accounts_router)
app.include_router(ledger_router)


# ---------------------------------------------------------
# Kafka / Outbox Worker
# ---------------------------------------------------------

kafka_producer = KafkaProducer()

outbox_worker = OutboxWorker(
    session_factory=AsyncSessionLocal,
    producer=kafka_producer,
)


def handle_outbox_task_done(task: asyncio.Task):
    """
    Detect unexpected termination of the background
    OutboxWorker task and log the exception.
    """

    try:
        task.result()

    except asyncio.CancelledError:
        logger.info(
            "Outbox worker task was cancelled"
        )

    except Exception:
        logger.exception(
            "Outbox worker task terminated unexpectedly"
        )


@app.on_event("startup")
async def startup():
    logger.info(
        "Starting LedgerFlow background services"
    )

    app.state.outbox_task = asyncio.create_task(
        outbox_worker.run()
    )

    app.state.outbox_task.add_done_callback(
        handle_outbox_task_done
    )


@app.on_event("shutdown")
async def shutdown():
    logger.info(
        "Shutting down LedgerFlow"
    )

    outbox_worker.stop()

    if hasattr(app.state, "outbox_task"):
        await app.state.outbox_task

    logger.info(
        "LedgerFlow shutdown complete"
    )


# ---------------------------------------------------------
# Health
# ---------------------------------------------------------

@app.get("/health")
async def health():
    return {
        "status": "healthy"
    }


@app.get("/health/redis")
async def redis_health():
    try:
        await redis_client.ping()

        return {
            "status": "healthy",
            "redis": "connected",
        }

    except Exception:
        return {
            "status": "unhealthy",
            "redis": "disconnected",
        }
