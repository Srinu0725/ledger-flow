import uuid
from decimal import Decimal

import pytest
from sqlalchemy import select

from app.consumers.projection_model import AccountBalanceProjection
from app.consumers.projection_service import (
    apply_balance_projection,
)


@pytest.mark.asyncio
async def test_deposit_updates_balance_projection(db):
    account_id = uuid.uuid4()

    event = {
        "event_type": "DEPOSIT_POSTED",
        "payload": {
            "account_id": str(account_id),
            "amount": "1000.00",
        },
    }

    await apply_balance_projection(
        db,
        event,
    )

    await db.commit()

    result = await db.execute(
        select(AccountBalanceProjection)
        .where(
            AccountBalanceProjection.account_id
            == account_id
        )
    )

    projection = result.scalar_one()

    assert projection.balance == Decimal("1000.00")


@pytest.mark.asyncio
async def test_withdrawal_reduces_balance_projection(db):
    account_id = uuid.uuid4()

    deposit = {
        "event_type": "DEPOSIT_POSTED",
        "payload": {
            "account_id": str(account_id),
            "amount": "1000.00",
        },
    }

    withdrawal = {
        "event_type": "WITHDRAWAL_POSTED",
        "payload": {
            "account_id": str(account_id),
            "amount": "250.00",
        },
    }

    await apply_balance_projection(
        db,
        deposit,
    )

    await apply_balance_projection(
        db,
        withdrawal,
    )

    await db.commit()

    result = await db.execute(
        select(AccountBalanceProjection)
        .where(
            AccountBalanceProjection.account_id
            == account_id
        )
    )

    projection = result.scalar_one()

    assert projection.balance == Decimal("750.00")


@pytest.mark.asyncio
async def test_transfer_updates_both_accounts(db):
    source_id = uuid.uuid4()
    destination_id = uuid.uuid4()

    deposit = {
        "event_type": "DEPOSIT_POSTED",
        "payload": {
            "account_id": str(source_id),
            "amount": "1000.00",
        },
    }

    transfer = {
        "event_type": "TRANSFER_POSTED",
        "payload": {
            "source_account_id": str(source_id),
            "destination_account_id": str(destination_id),
            "amount": "300.00",
        },
    }

    await apply_balance_projection(
        db,
        deposit,
    )

    await apply_balance_projection(
        db,
        transfer,
    )

    await db.commit()

    result = await db.execute(
        select(AccountBalanceProjection)
    )

    projections = {
        row.account_id: row.balance
        for row in result.scalars()
    }

    assert projections[source_id] == Decimal("700.00")
    assert projections[destination_id] == Decimal("300.00")