from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.accounts.model import Account, AccountStatus, AccountType
from app.ledger.model import LedgerEntry
from app.ledger.service import (
    create_deposit,
    create_transfer,
    create_withdrawal,
    get_balance,
)


@pytest.mark.asyncio
async def test_every_transaction_is_balanced(db):

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

    db.add_all([system, alice, bob])
    await db.commit()

    await create_deposit(
        db,
        alice.account_id,
        Decimal("1000.00"),
    )

    await create_transfer(
        db=db,
        from_account_id=alice.account_id,
        to_account_id=bob.account_id,
        amount=Decimal("300.00"),
        idempotency_key="invariant-transfer-001",
    )

    await create_withdrawal(
        db,
        bob.account_id,
        Decimal("100.00"),
    )

    result = await db.execute(
        select(
            LedgerEntry.transaction_id,
            func.sum(LedgerEntry.amount),
        )
        .group_by(LedgerEntry.transaction_id)
    )

    transactions = result.all()

    for transaction_id, total in transactions:
        assert total == Decimal("0.00")


@pytest.mark.asyncio
async def test_transfer_preserves_total_money(db):

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

    db.add_all([system, alice, bob])
    await db.commit()

    await create_deposit(
        db,
        alice.account_id,
        Decimal("1000.00"),
    )

    await create_deposit(
        db,
        bob.account_id,
        Decimal("500.00"),
    )

    await create_transfer(
        db=db,
        from_account_id=alice.account_id,
        to_account_id=bob.account_id,
        amount=Decimal("300.00"),
        idempotency_key="conservation-transfer-001",
    )

    alice_balance = await get_balance(
        db,
        alice.account_id,
    )

    bob_balance = await get_balance(
        db,
        bob.account_id,
    )

    assert alice_balance == Decimal("700.00")
    assert bob_balance == Decimal("800.00")

    assert (
        alice_balance + bob_balance
        == Decimal("1500.00")
    )        