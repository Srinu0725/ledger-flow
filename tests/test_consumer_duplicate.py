import uuid
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.consumers.model import ProcessedEvent
from app.consumers.projection_model import AccountBalanceProjection
from app.consumers.projection_service import (
    apply_balance_projection,
)
from app.consumers.service import (
    mark_event_processed,
)


@pytest.mark.asyncio
async def test_duplicate_event_does_not_double_apply(
    db,
):
    account_id = uuid.uuid4()
    event_id = uuid.uuid4()

    event = {
        "event_id": str(event_id),
        "event_type": "DEPOSIT_POSTED",
        "payload": {
            "account_id": str(account_id),
            "amount": "1000.00",
        },
    }

    # ---------------------------------------------------------
    # First delivery
    # ---------------------------------------------------------
    registered = await mark_event_processed(
        db,
        event_id,
        event["event_type"],
    )

    assert registered is True

    await apply_balance_projection(
        db,
        event,
    )

    await db.commit()

    # ---------------------------------------------------------
    # Duplicate delivery
    # ---------------------------------------------------------
    duplicate_registered = await mark_event_processed(
        db,
        event_id,
        event["event_type"],
    )

    assert duplicate_registered is False

    # Because the event is already registered,
    # we MUST NOT apply the projection again.

    await db.rollback()

    # ---------------------------------------------------------
    # Verify balance
    # ---------------------------------------------------------
    result = await db.execute(
        select(AccountBalanceProjection)
        .where(
            AccountBalanceProjection.account_id
            == account_id
        )
    )

    projection = result.scalar_one()

    assert projection.balance == Decimal("1000.00")

    # ---------------------------------------------------------
    # Verify only one processed-event record exists
    # ---------------------------------------------------------
    result = await db.execute(
        select(ProcessedEvent)
        .where(
            ProcessedEvent.event_id == event_id
        )
    )

    processed_events = list(
        result.scalars().all()
    )

    assert len(processed_events) == 1