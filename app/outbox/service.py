from datetime import datetime, timedelta, timezone

from sqlalchemy import or_, select

from app.outbox.model import OutboxEvent, OutboxStatus


MAX_RETRIES = 5
MAX_RETRY_DELAY = 60

# How long an event can remain PROCESSING before we assume
# the worker that claimed it died.
CLAIM_TIMEOUT_SECONDS = 30


async def create_outbox_event(
    db,
    aggregate_id,
    event_type,
    payload,
):
    event = OutboxEvent(
        aggregate_id=aggregate_id,
        event_type=event_type,
        event_version=1,
        payload=payload,
        status=OutboxStatus.PENDING,
    )

    db.add(event)

    return event


async def claim_pending_events(
    db,
    limit=100,
):
    now = datetime.now(timezone.utc)

    result = await db.execute(
        select(OutboxEvent)
        .where(
            OutboxEvent.status == OutboxStatus.PENDING,
            or_(
                OutboxEvent.next_retry_at.is_(None),
                OutboxEvent.next_retry_at <= now,
            ),
        )
        .order_by(OutboxEvent.created_at.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )

    events = list(result.scalars().all())

    for event in events:
        event.status = OutboxStatus.PROCESSING
        event.claimed_at = now

    await db.commit()

    return events


async def recover_stale_events(
    db,
    limit=100,
):
    """
    Recover events whose worker probably crashed while
    processing them.

    PROCESSING -> PENDING
    """

    now = datetime.now(timezone.utc)

    stale_before = now - timedelta(
        seconds=CLAIM_TIMEOUT_SECONDS
    )

    result = await db.execute(
        select(OutboxEvent)
        .where(
            OutboxEvent.status == OutboxStatus.PROCESSING,
            OutboxEvent.claimed_at.is_not(None),
            OutboxEvent.claimed_at <= stale_before,
        )
        .order_by(OutboxEvent.claimed_at.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )

    events = list(result.scalars().all())

    for event in events:
        event.status = OutboxStatus.PENDING
        event.claimed_at = None

    await db.commit()

    return len(events)


async def mark_published(
    db,
    event,
):
    event.status = OutboxStatus.PUBLISHED
    event.published_at = datetime.now(timezone.utc)

    event.last_error = None
    event.next_retry_at = None
    event.claimed_at = None


async def mark_failed(
    db,
    event,
    error,
):
    event.retry_count += 1
    event.last_error = error[:1000]
    event.claimed_at = None

    if event.retry_count >= MAX_RETRIES:
        event.status = OutboxStatus.FAILED
        event.next_retry_at = None
        return

    delay_seconds = min(
        2 ** event.retry_count,
        MAX_RETRY_DELAY,
    )

    event.status = OutboxStatus.PENDING

    event.next_retry_at = (
        datetime.now(timezone.utc)
        + timedelta(seconds=delay_seconds)
    )