from decimal import Decimal

import pytest
from sqlalchemy import select

from app.accounts.model import (
    Account,
    AccountStatus,
    AccountType,
)

from app.ledger.model import (
    LedgerEntry,
    Transaction,
    TransactionStatus,
    TransactionType,
)

from app.ledger.service import (
    create_deposit,
    create_withdrawal,
    create_transfer,
    get_balance,
)

@pytest.mark.asyncio
async def test_withdrawal_reduces_balance(db):

    # Create SYSTEM_CASH
    system_account = Account(
        owner_name="SYSTEM_CASH",
        account_type=AccountType.SYSTEM,
        currency="INR",
        status=AccountStatus.ACTIVE,
    )

    # Create Alice
    alice = Account(
        owner_name="Alice",
        account_type=AccountType.SAVINGS,
        currency="INR",
        status=AccountStatus.ACTIVE,
    )

    db.add_all([
        system_account,
        alice,
    ])

    await db.commit()

    # Deposit ₹1000
    await create_deposit(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("1000.00"),
    )

    # Withdraw ₹300
    transaction, account = await create_withdrawal(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("300.00"),
    )

    # Balance should now be ₹700
    balance = await get_balance(
        db,
        alice.account_id,
    )

    assert balance == Decimal("700.00")

    assert transaction.transaction_type == (
        TransactionType.WITHDRAWAL
    )

    assert transaction.status == (
        TransactionStatus.POSTED
    )
    
@pytest.mark.asyncio
async def test_deposit_creates_double_entry(db):

    # Create SYSTEM_CASH
    system_account = Account(
        owner_name="SYSTEM_CASH",
        account_type=AccountType.SYSTEM,
        currency="INR",
        status=AccountStatus.ACTIVE,
    )

    # Create Alice
    alice = Account(
        owner_name="Alice",
        account_type=AccountType.SAVINGS,
        currency="INR",
        status=AccountStatus.ACTIVE,
    )

    db.add_all([
        system_account,
        alice,
    ])

    await db.commit()

    # Deposit ₹1000
    transaction, account = await create_deposit(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("1000.00"),
    )

    assert transaction.transaction_type == (
        TransactionType.DEPOSIT
    )

    assert transaction.status == (
        TransactionStatus.POSTED
    )

    # Check balance
    balance = await get_balance(
        db,
        alice.account_id,
    )

    assert balance == Decimal("1000.00")

    # Check ledger entries
    result = await db.execute(
        select(LedgerEntry).where(
            LedgerEntry.transaction_id
            == transaction.transaction_id
        )
    )

    entries = result.scalars().all()

    assert len(entries) == 2

    total = sum(
        (entry.amount for entry in entries),
        Decimal("0"),
    )

    assert total == Decimal("0")
    
@pytest.mark.asyncio
async def test_withdrawal_rejects_insufficient_funds(db):

    system_account = Account(
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
        system_account,
        alice,
    ])

    await db.commit()

    await create_deposit(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("100.00"),
    )

    with pytest.raises(
        ValueError,
        match="Insufficient funds",
    ):
        await create_withdrawal(
            db=db,
            account_id=alice.account_id,
            amount=Decimal("200.00"),
        )

    # Balance must remain unchanged
    balance = await get_balance(
        db,
        alice.account_id,
    )

    assert balance == Decimal("100.00")    


@pytest.mark.asyncio
async def test_transfer_moves_money_between_accounts(db):

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
        alice,
        bob,
    ])

    await db.commit()

    # We need SYSTEM_CASH for the initial deposit
    system_account = Account(
        owner_name="SYSTEM_CASH",
        account_type=AccountType.SYSTEM,
        currency="INR",
        status=AccountStatus.ACTIVE,
    )

    db.add(system_account)
    await db.commit()

    # Give Alice ₹1000
    await create_deposit(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("1000.00"),
    )

    # Transfer ₹300 from Alice → Bob
    transaction = await create_transfer(
        db=db,
        from_account_id=alice.account_id,
        to_account_id=bob.account_id,
        amount=Decimal("300.00"),
        idempotency_key="transfer-alice-bob-001",
    )

    assert transaction.transaction_type == (
        TransactionType.TRANSFER
    )

    assert transaction.status == (
        TransactionStatus.POSTED
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
    assert bob_balance == Decimal("300.00")    