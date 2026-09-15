import uuid
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from app.consumers.model import ProcessedEvent


async def is_event_processed(
    db,
    event_id: uuid.UUID,
) -> bool:
    """
    Check whether an event has already been processed.
    """

    result = await db.execute(
        select(ProcessedEvent.event_id)
        .where(
            ProcessedEvent.event_id == event_id
        )
    )

    return result.scalar_one_or_none() is not None


async def mark_event_processed(
    db,
    event_id: uuid.UUID,
    event_type: str,
) -> bool:
    """
    Atomically register an event as processed.

    Returns:
        True  -> event was newly registered
        False -> event was already registered
    """

    event = ProcessedEvent(
        event_id=event_id,
        event_type=event_type,
        processed_at=datetime.now(timezone.utc),
    )

    db.add(event)

    try:
        await db.flush()

    except IntegrityError:
        await db.rollback()
        return False

    return True