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
    get_balance,
)


@pytest.mark.asyncio
async def test_same_idempotency_key_returns_same_transaction(db):

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

    # Give Alice ₹1000
    await create_deposit(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("1000.00"),
    )

    key = "same-request-001"

    # First request
    first = await create_transfer(
        db=db,
        from_account_id=alice.account_id,
        to_account_id=bob.account_id,
        amount=Decimal("100.00"),
        idempotency_key=key,
    )

    # Same request again
    second = await create_transfer(
        db=db,
        from_account_id=alice.account_id,
        to_account_id=bob.account_id,
        amount=Decimal("100.00"),
        idempotency_key=key,
    )

    # Must return the exact same transaction
    assert first.transaction_id == second.transaction_id

    # Alice should only have sent ₹100 once
    alice_balance = await get_balance(
        db,
        alice.account_id,
    )

    # Bob should only have received ₹100 once
    bob_balance = await get_balance(
        db,
        bob.account_id,
    )

    assert alice_balance == Decimal("900.00")
    assert bob_balance == Decimal("100.00")

    # Only one transaction should exist for this key
    result = await db.execute(
        select(func.count(Transaction.transaction_id))
        .where(Transaction.idempotency_key == key)
    )

    count = result.scalar_one()

    assert count == 1
    
@pytest.mark.asyncio
async def test_same_idempotency_key_different_request_is_rejected(db):

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
        db=db,
        account_id=alice.account_id,
        amount=Decimal("1000.00"),
    )

    key = "different-request-001"

    # Original request
    await create_transfer(
        db=db,
        from_account_id=alice.account_id,
        to_account_id=bob.account_id,
        amount=Decimal("100.00"),
        idempotency_key=key,
    )

    # Same key, different amount
    with pytest.raises(
        ValueError,
        match="Idempotency key already used for a different request",
    ):
        await create_transfer(
            db=db,
            from_account_id=alice.account_id,
            to_account_id=bob.account_id,
            amount=Decimal("500.00"),
            idempotency_key=key,
        )

    # Balance must still reflect ONLY the original ₹100 transfer
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