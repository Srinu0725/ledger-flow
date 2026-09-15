from datetime import datetime, timezone
from decimal import Decimal
import uuid

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.consumers.projection_model import AccountBalanceProjection


async def apply_balance_projection(
    db: AsyncSession,
    event: dict,
):
    """
    Apply a ledger event to the derived account balance projection.

    The projection is derived data.
    The ledger remains the source of truth.
    """

    event_type = event["event_type"]
    payload = event["payload"]

    if event_type == "DEPOSIT_POSTED":
        account_id = uuid.UUID(payload["account_id"])
        amount = Decimal(payload["amount"])

        await _apply_delta(
            db,
            account_id,
            amount,
        )

    elif event_type == "WITHDRAWAL_POSTED":
        account_id = uuid.UUID(payload["account_id"])
        amount = Decimal(payload["amount"])

        await _apply_delta(
            db,
            account_id,
            -amount,
        )

    elif event_type == "TRANSFER_POSTED":
        source_account_id = uuid.UUID(
            payload["source_account_id"]
        )

        destination_account_id = uuid.UUID(
            payload["destination_account_id"]
        )

        amount = Decimal(payload["amount"])

        # Keep transfer updates deterministic.
        #
        # The two accounts are updated independently,
        # but the caller's transaction guarantees that
        # both changes commit or both roll back.
        await _apply_delta(
            db,
            source_account_id,
            -amount,
        )

        await _apply_delta(
            db,
            destination_account_id,
            amount,
        )


async def _apply_delta(
    db: AsyncSession,
    account_id: uuid.UUID,
    delta: Decimal,
):
    """
    Atomically apply a balance delta using PostgreSQL UPSERT.

    This handles both cases safely:

    1. Projection row does not exist:
       INSERT account_id, delta

    2. Projection row already exists:
       UPDATE balance = balance + delta

    PostgreSQL serializes conflicting UPSERTs on the same
    primary key, preventing lost updates during concurrent
    event processing.
    """

    now = datetime.now(timezone.utc)

    statement = insert(
        AccountBalanceProjection
    ).values(
        account_id=account_id,
        balance=delta,
        updated_at=now,
    )

    statement = statement.on_conflict_do_update(
        index_elements=[
            AccountBalanceProjection.account_id
        ],
        set_={
            "balance": (
                AccountBalanceProjection.balance
                + statement.excluded.balance
            ),
            "updated_at": statement.excluded.updated_at,
        },
    )

    await db.execute(statement)
