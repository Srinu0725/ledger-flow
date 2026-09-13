import asyncio
from decimal import Decimal

import pytest
from sqlalchemy import func, select

from app.accounts.model import (
    Account,
    AccountStatus,
    AccountType,
)

from app.ledger.model import Transaction

from app.ledger.service import (
    create_deposit,
    create_transfer,
    create_withdrawal,
    get_balance,
)

@pytest.mark.asyncio
async def test_concurrent_withdrawals_cannot_overspend(db):

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

    # Alice starts with ₹1000
    await create_deposit(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("1000.00"),
    )

    # --------------------------------------------------
    # Create 20 independent database sessions
    # --------------------------------------------------

    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    engine = create_async_engine(
        "postgresql+asyncpg://"
        "ledger:ledger@localhost:5432/ledgerflow_test",
        echo=False,
    )

    SessionLocal = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async def withdraw():

        async with SessionLocal() as session:

            try:
                await create_withdrawal(
                    db=session,
                    account_id=alice.account_id,
                    amount=Decimal("100.00"),
                )

                return True

            except ValueError:
                return False

    results = await asyncio.gather(
        *(withdraw() for _ in range(20))
    )

    await engine.dispose()

    successful = sum(results)

    # Alice only had ₹1000.
    # Therefore only 10 withdrawals can succeed.
    assert successful == 10

    # Final balance must never become negative.
    balance = await get_balance(
        db,
        alice.account_id,
    )

    assert balance == Decimal("0.00")
    
@pytest.mark.asyncio
async def test_concurrent_opposite_transfers_do_not_deadlock(db):

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

    # Give both accounts enough money
    await create_deposit(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("1000.00"),
    )

    await create_deposit(
        db=db,
        account_id=bob.account_id,
        amount=Decimal("1000.00"),
    )

    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    engine = create_async_engine(
        "postgresql+asyncpg://"
        "ledger:ledger@localhost:5432/ledgerflow_test",
        echo=False,
    )

    SessionLocal = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async def alice_to_bob():

        async with SessionLocal() as session:

            return await create_transfer(
                db=session,
                from_account_id=alice.account_id,
                to_account_id=bob.account_id,
                amount=Decimal("100.00"),
                idempotency_key="alice-bob-concurrent",
            )

    async def bob_to_alice():

        async with SessionLocal() as session:

            return await create_transfer(
                db=session,
                from_account_id=bob.account_id,
                to_account_id=alice.account_id,
                amount=Decimal("100.00"),
                idempotency_key="bob-alice-concurrent",
            )

    # Run opposite transfers simultaneously
    results = await asyncio.gather(
        alice_to_bob(),
        bob_to_alice(),
    )

    await engine.dispose()

    # Both operations should succeed
    assert len(results) == 2

    alice_balance = await get_balance(
        db,
        alice.account_id,
    )

    bob_balance = await get_balance(
        db,
        bob.account_id,
    )

    # Both sent and received ₹100,
    # therefore balances return to the original values.
    assert alice_balance == Decimal("1000.00")
    assert bob_balance == Decimal("1000.00")    
    
@pytest.mark.asyncio
async def test_concurrent_same_idempotency_key_creates_one_transaction(db):

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

    # Alice starts with ₹1000
    await create_deposit(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("1000.00"),
    )

    from sqlalchemy.ext.asyncio import (
        AsyncSession,
        async_sessionmaker,
        create_async_engine,
    )

    engine = create_async_engine(
        "postgresql+asyncpg://"
        "ledger:ledger@localhost:5432/ledgerflow_test",
        echo=False,
    )

    SessionLocal = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async def send_transfer():

        async with SessionLocal() as session:

            try:
                transaction = await create_transfer(
                    db=session,
                    from_account_id=alice.account_id,
                    to_account_id=bob.account_id,
                    amount=Decimal("100.00"),
                    idempotency_key="concurrent-transfer-001",
                )

                return transaction.transaction_id

            except Exception as exc:
                return exc

    results = await asyncio.gather(
        *(send_transfer() for _ in range(20))
    )

    await engine.dispose()

    # --------------------------------------------------
    # Every successful request must point to the
    # same transaction.
    # --------------------------------------------------

    transaction_ids = [
        result
        for result in results
        if not isinstance(result, Exception)
    ]

    assert len(transaction_ids) == 20

    assert len(set(transaction_ids)) == 1

    # --------------------------------------------------
    # Only one transaction should exist.
    # --------------------------------------------------

    result = await db.execute(
        select(func.count(Transaction.transaction_id))
        .where(
            Transaction.idempotency_key
            == "concurrent-transfer-001"
        )
    )

    transaction_count = result.scalar_one()

    assert transaction_count == 1

    # --------------------------------------------------
    # Money must move only once.
    # --------------------------------------------------

    alice_balance = await get_balance(
        db,
        alice.account_id,
    )

    bob_balance = await get_balance(
        db,
        bob.account_id,
    )

    assert alice_balance == Decimal("900.00")
    assert bob_balance == Decimal("100.00")    