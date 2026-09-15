import asyncio
import uuid
from decimal import Decimal

import httpx
from sqlalchemy import select

from app.consumers.model import ProcessedEvent
from app.consumers.projection_model import AccountBalanceProjection
from app.db.database import AsyncSessionLocal
from app.db.models import Base
from app.outbox.model import OutboxEvent, OutboxStatus

# IMPORTANT:
# Adjust this import if your Account model lives somewhere else.
from app.accounts.model import Account, AccountType

BASE_URL = "http://127.0.0.1:8000"

DEPOSIT_AMOUNT = Decimal("1000.00")


async def create_test_account():
    """
    Create an account directly in PostgreSQL.

    We do this instead of depending on the account HTTP endpoint,
    because this test is specifically focused on the ledger ->
    outbox -> Kafka -> consumer pipeline.
    """
    account_id = uuid.uuid4()

    async with AsyncSessionLocal() as db:
        account = Account(
            account_id=account_id,
            owner_name="Full E2E Test User",
            account_type=AccountType.SAVINGS,
            currency="INR",
        )

        db.add(account)
        await db.commit()

    return account_id

async def wait_for_outbox_event(
    account_id: uuid.UUID,
    timeout: float = 10.0,
):
    """
    Wait until the OutboxWorker has created
    the DEPOSIT_POSTED event for this account.

    The application uses transaction_id as the
    outbox aggregate_id, so we identify the
    event using payload.account_id instead.
    """

    deadline = (
        asyncio.get_running_loop().time()
        + timeout
    )

    while (
        asyncio.get_running_loop().time()
        < deadline
    ):

        async with AsyncSessionLocal() as db:

            result = await db.execute(
                select(OutboxEvent)
                .where(
                    OutboxEvent.event_type
                    == "DEPOSIT_POSTED"
                )
                .order_by(
                    OutboxEvent.created_at.desc()
                )
            )

            events = result.scalars().all()

            for event in events:
                if (
                    event.payload.get("account_id")
                    == str(account_id)
                ):
                    return event

        await asyncio.sleep(0.25)

    raise AssertionError(
        "Timed out waiting for outbox event"
    )



async def wait_for_processed_event(
    event_id: uuid.UUID,
    timeout: float = 15.0,
):
    """
    Wait until the real Kafka consumer has processed
    the event.
    """

    deadline = asyncio.get_running_loop().time() + timeout

    while asyncio.get_running_loop().time() < deadline:

        async with AsyncSessionLocal() as db:

            result = await db.execute(
                select(ProcessedEvent)
                .where(
                    ProcessedEvent.event_id == event_id
                )
            )

            processed = result.scalar_one_or_none()

            if processed is not None:
                return processed

        await asyncio.sleep(0.25)

    raise AssertionError(
        f"Timed out waiting for processed event {event_id}"
    )


async def wait_for_projection(
    account_id: uuid.UUID,
    expected_balance: Decimal,
    timeout: float = 15.0,
):
    """
    Wait until the real Kafka consumer has updated
    the balance projection.
    """

    deadline = asyncio.get_running_loop().time() + timeout

    while asyncio.get_running_loop().time() < deadline:

        async with AsyncSessionLocal() as db:

            result = await db.execute(
                select(AccountBalanceProjection)
                .where(
                    AccountBalanceProjection.account_id
                    == account_id
                )
            )

            projection = result.scalar_one_or_none()

            if (
                projection is not None
                and projection.balance == expected_balance
            ):
                return projection

        await asyncio.sleep(0.25)

    raise AssertionError(
        "Timed out waiting for balance projection"
    )


async def get_ledger_balance(
    account_id: uuid.UUID,
):
    """
    Read the balance from the ledger/source of truth
    through the real API.
    """

    async with httpx.AsyncClient(
        base_url=BASE_URL,
        timeout=10.0,
    ) as client:

        response = await client.get(
            f"/api/v1/ledger/accounts/{account_id}/balance"
        )

        response.raise_for_status()

        return Decimal(
            str(response.json()["balance"])
        )


async def main():

    print("=" * 60)
    print("LedgerFlow Full E2E Test")
    print("=" * 60)

    # ---------------------------------------------------------
    # 1. Create test account
    # ---------------------------------------------------------

    account_id = await create_test_account()

    print(f"Created account: {account_id}")

    # ---------------------------------------------------------
    # 2. Call the REAL deposit API
    # ---------------------------------------------------------

    async with httpx.AsyncClient(
        base_url=BASE_URL,
        timeout=10.0,
    ) as client:

        response = await client.post(
            "/api/v1/ledger/deposit",
            json={
                "account_id": str(account_id),
                "amount": str(DEPOSIT_AMOUNT),
            },
        )

        response.raise_for_status()

        deposit_response = response.json()

    transaction_id = deposit_response["transaction_id"]

    print(
        f"Deposit created: "
        f"{transaction_id}"
    )

    assert (
        Decimal(str(deposit_response["amount"]))
        == DEPOSIT_AMOUNT
    )

    # ---------------------------------------------------------
    # 3. Verify ledger/source-of-truth balance
    # ---------------------------------------------------------

    ledger_balance = await get_ledger_balance(
        account_id
    )

    print(
        f"Ledger balance: "
        f"{ledger_balance}"
    )

    assert ledger_balance == DEPOSIT_AMOUNT

    # ---------------------------------------------------------
    # 4. Verify Outbox event exists
    # ---------------------------------------------------------

    outbox_event = await wait_for_outbox_event(
        account_id
    )

    print(
        f"Outbox event: "
        f"{outbox_event.event_id}"
    )

    assert (
        outbox_event.event_type
        == "DEPOSIT_POSTED"
    )

    # The worker should eventually publish it.
    #
    # Give the worker a little time to transition
    # the event from PENDING/PROCESSING to PUBLISHED.

    deadline = (
        asyncio.get_running_loop().time()
        + 10
    )

    while (
        outbox_event.status
        != OutboxStatus.PUBLISHED
        and asyncio.get_running_loop().time()
        < deadline
    ):

        await asyncio.sleep(0.25)

        async with AsyncSessionLocal() as db:

            refreshed = await db.get(
                OutboxEvent,
                outbox_event.event_id,
            )

            if refreshed is not None:
                outbox_event = refreshed

    assert (
        outbox_event.status
        == OutboxStatus.PUBLISHED
    )

    print("Outbox event published")

    # ---------------------------------------------------------
    # 5. Wait for the REAL Kafka consumer
    # ---------------------------------------------------------

    processed = await wait_for_processed_event(
        outbox_event.event_id
    )

    print(
        f"Consumer processed event: "
        f"{processed.event_id}"
    )

    assert (
        processed.event_id
        == outbox_event.event_id
    )

    # ---------------------------------------------------------
    # 6. Verify projection
    # ---------------------------------------------------------

    projection = await wait_for_projection(
        account_id,
        DEPOSIT_AMOUNT,
    )

    print(
        f"Projection balance: "
        f"{projection.balance}"
    )

    assert (
        projection.balance
        == DEPOSIT_AMOUNT
    )

    # ---------------------------------------------------------
    # 7. Final consistency check
    # ---------------------------------------------------------

    ledger_balance = await get_ledger_balance(
        account_id
    )

    projection_balance = projection.balance

    print()
    print("Final consistency check")
    print("-----------------------")
    print(
        f"Ledger balance:     {ledger_balance}"
    )
    print(
        f"Projection balance: {projection_balance}"
    )

    assert (
        ledger_balance
        == projection_balance
        == DEPOSIT_AMOUNT
    )

    print()
    print("=" * 60)
    print("FULL E2E TEST PASSED")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())

