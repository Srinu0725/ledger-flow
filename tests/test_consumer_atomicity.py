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
async def test_idempotency_and_projection_rollback_together(db):
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
    # Start transaction
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

    # ---------------------------------------------------------
    # Simulate a failure BEFORE commit.
    #
    # Both operations must be rolled back.
    # ---------------------------------------------------------
    await db.rollback()

    # ---------------------------------------------------------
    # Verify processed_events was rolled back
    # ---------------------------------------------------------
    result = await db.execute(
        select(ProcessedEvent)
        .where(
            ProcessedEvent.event_id == event_id
        )
    )

    assert result.scalar_one_or_none() is None

    # ---------------------------------------------------------
    # Verify projection was rolled back
    # ---------------------------------------------------------
    result = await db.execute(
        select(AccountBalanceProjection)
        .where(
            AccountBalanceProjection.account_id
            == account_id
        )
    )

    assert result.scalar_one_or_none() is None