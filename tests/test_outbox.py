import pytest
from decimal import Decimal
from sqlalchemy import select
from app.ledger.model import LedgerEntry, Transaction
from app.accounts.model import (
    Account,
    AccountStatus,
    AccountType,
)

from app.ledger.service import (
    create_deposit,
    create_withdrawal,
    create_transfer,
)

from app.outbox.model import (
    OutboxEvent,
    OutboxStatus,
)


@pytest.mark.asyncio
async def test_deposit_creates_outbox_event(db):

    system = Account(
        owner_name="SYSTEM_CASH",
        account_type=AccountType.SYSTEM,
        currency="INR",
        status=AccountStatus.ACTIVE,
    )

    alice = Account(
        owner_name="Alice",
        account_type=AccountType.SAVINGS,
        currency="INR",
        status=AccountStatus.ACTIVE,
    )

    db.add_all([
        system,
        alice,
    ])

    await db.commit()

    transaction, account = await create_deposit(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("1000.00"),
    )

    result = await db.execute(
        select(OutboxEvent).where(
            OutboxEvent.aggregate_id
            == transaction.transaction_id
        )
    )

    event = result.scalar_one()

    assert event.event_type == "DEPOSIT_POSTED"
    assert event.event_version == 1
    assert event.status == OutboxStatus.PENDING

    assert (
        event.payload["transaction_id"]
        == str(transaction.transaction_id)
    )

    assert (
        event.payload["account_id"]
        == str(alice.account_id)
    )

    assert event.payload["amount"] == "1000.00"
    assert event.payload["currency"] == "INR"


@pytest.mark.asyncio
async def test_withdrawal_creates_outbox_event(db):

    system = Account(
        owner_name="SYSTEM_CASH",
        account_type=AccountType.SYSTEM,
        currency="INR",
        status=AccountStatus.ACTIVE,
    )

    alice = Account(
        owner_name="Alice",
        account_type=AccountType.SAVINGS,
        currency="INR",
        status=AccountStatus.ACTIVE,
    )

    db.add_all([
        system,
        alice,
    ])

    await db.commit()

    await create_deposit(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("1000.00"),
    )

    transaction, account = await create_withdrawal(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("300.00"),
    )

    result = await db.execute(
        select(OutboxEvent).where(
            OutboxEvent.aggregate_id
            == transaction.transaction_id
        )
    )

    event = result.scalar_one()

    assert event.event_type == "WITHDRAWAL_POSTED"
    assert event.event_version == 1
    assert event.status == OutboxStatus.PENDING

    assert (
        event.payload["transaction_id"]
        == str(transaction.transaction_id)
    )

    assert (
        event.payload["account_id"]
        == str(alice.account_id)
    )

    assert event.payload["amount"] == "300.00"
    assert event.payload["currency"] == "INR"


@pytest.mark.asyncio
async def test_transfer_creates_outbox_event(db):

    system = Account(
        owner_name="SYSTEM_CASH",
        account_type=AccountType.SYSTEM,
        currency="INR",
        status=AccountStatus.ACTIVE,
    )

    alice = Account(
        owner_name="Alice",
        account_type=AccountType.SAVINGS,
        currency="INR",
        status=AccountStatus.ACTIVE,
    )

    bob = Account(
        owner_name="Bob",
        account_type=AccountType.SAVINGS,
        currency="INR",
        status=AccountStatus.ACTIVE,
    )

    db.add_all([
        system,
        alice,
        bob,
    ])

    await db.commit()

    await create_deposit(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("1000.00"),
    )

    transaction = await create_transfer(
        db=db,
        from_account_id=alice.account_id,
        to_account_id=bob.account_id,
        amount=Decimal("250.00"),
        idempotency_key="outbox-transfer-001",
    )

    result = await db.execute(
        select(OutboxEvent).where(
            OutboxEvent.aggregate_id
            == transaction.transaction_id
        )
    )

    event = result.scalar_one()

    assert event.event_type == "TRANSFER_POSTED"
    assert event.event_version == 1
    assert event.status == OutboxStatus.PENDING

    assert (
        event.payload["transaction_id"]
        == str(transaction.transaction_id)
    )

    assert (
        event.payload["from_account_id"]
        == str(alice.account_id)
    )

    assert (
        event.payload["to_account_id"]
        == str(bob.account_id)
    )

    assert event.payload["amount"] == "250.00"
    assert event.payload["currency"] == "INR"
    
@pytest.mark.asyncio
async def test_outbox_and_ledger_commit_atomically(db):

    system = Account(
        owner_name="SYSTEM_CASH",
        account_type=AccountType.SYSTEM,
        currency="INR",
        status=AccountStatus.ACTIVE,
    )

    alice = Account(
        owner_name="Alice",
        account_type=AccountType.SAVINGS,
        currency="INR",
        status=AccountStatus.ACTIVE,
    )

    db.add_all([
        system,
        alice,
    ])

    await db.commit()

    transaction, account = await create_deposit(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("500.00"),
    )

    # Verify transaction exists
    result = await db.execute(
        select(Transaction).where(
            Transaction.transaction_id
            == transaction.transaction_id
        )
    )

    saved_transaction = result.scalar_one_or_none()

    assert saved_transaction is not None

    # Verify ledger entries exist
    result = await db.execute(
        select(LedgerEntry).where(
            LedgerEntry.transaction_id
            == transaction.transaction_id
        )
    )

    entries = result.scalars().all()

    assert len(entries) == 2

    # Verify outbox event exists
    result = await db.execute(
        select(OutboxEvent).where(
            OutboxEvent.aggregate_id
            == transaction.transaction_id
        )
    )

    event = result.scalar_one_or_none()

    assert event is not None
    assert event.status == OutboxStatus.PENDING   
    
@pytest.mark.asyncio
async def test_failed_transaction_does_not_create_outbox_event(db):

    system = Account(
        owner_name="SYSTEM_CASH",
        account_type=AccountType.SYSTEM,
        currency="INR",
        status=AccountStatus.ACTIVE,
    )

    alice = Account(
        owner_name="Alice",
        account_type=AccountType.SAVINGS,
        currency="INR",
        status=AccountStatus.ACTIVE,
    )

    db.add_all([
        system,
        alice,
    ])

    await db.commit()

    transaction = Transaction(
        transaction_type="DEPOSIT",
        status="POSTED",
        reference="Rollback test",
    )

    db.add(transaction)

    await db.flush()

    entry = LedgerEntry(
        transaction_id=transaction.transaction_id,
        account_id=alice.account_id,
        amount=Decimal("500.00"),
    )

    db.add(entry)

    await db.flush()

    # Simulate failure before commit
    await db.rollback()

    # Transaction should not exist
    result = await db.execute(
        select(Transaction).where(
            Transaction.transaction_id
            == transaction.transaction_id
        )
    )

    saved_transaction = result.scalar_one_or_none()

    assert saved_transaction is None

    # Outbox should not exist
    result = await db.execute(
        select(OutboxEvent).where(
            OutboxEvent.aggregate_id
            == transaction.transaction_id
        )
    )

    event = result.scalar_one_or_none()

    assert event is None     