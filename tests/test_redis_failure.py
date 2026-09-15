import pytest

from decimal import Decimal
from unittest.mock import AsyncMock

from app.accounts.model import (
    Account,
    AccountStatus,
    AccountType,
)

from app.db import redis as redis_db

from app.ledger.service import (
    create_deposit,
    create_withdrawal,
    create_transfer,
    get_balance,
)


@pytest.mark.asyncio
async def test_balance_falls_back_to_postgres_when_redis_is_down(db):

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

    # Simulate Redis being unavailable
    redis_db.redis_client = AsyncMock()

    redis_db.redis_client.get.side_effect = Exception(
        "Redis unavailable"
    )

    balance = await get_balance(
        db=db,
        account_id=alice.account_id,
    )

    assert balance == Decimal("1000.00")
    
@pytest.mark.asyncio
async def test_deposit_succeeds_when_redis_is_down(db):

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

    # Simulate Redis being unavailable
    redis_db.redis_client = AsyncMock()

    redis_db.redis_client.delete.side_effect = Exception(
        "Redis unavailable"
    )

    transaction, account = await create_deposit(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("1000.00"),
    )

    assert transaction.status.value == "POSTED"

    balance = await get_balance(
        db=db,
        account_id=alice.account_id,
    )

    assert balance == Decimal("1000.00")    
    
@pytest.mark.asyncio
async def test_withdrawal_succeeds_when_redis_is_down(db):

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

    # Simulate Redis being unavailable
    redis_db.redis_client = AsyncMock()

    redis_db.redis_client.delete.side_effect = Exception(
        "Redis unavailable"
    )

    transaction, account = await create_withdrawal(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("300.00"),
    )

    assert transaction.status.value == "POSTED"

    balance = await get_balance(
        db=db,
        account_id=alice.account_id,
    )

    assert balance == Decimal("700.00")    
    
@pytest.mark.asyncio
async def test_transfer_succeeds_when_redis_is_down(db):

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

    # Simulate Redis being unavailable
    redis_db.redis_client = AsyncMock()

    redis_db.redis_client.delete.side_effect = Exception(
        "Redis unavailable"
    )

    transaction = await create_transfer(
        db=db,
        from_account_id=alice.account_id,
        to_account_id=bob.account_id,
        amount=Decimal("250.00"),
        idempotency_key="redis-down-transfer-001",
    )

    assert transaction.status.value == "POSTED"

    alice_balance = await get_balance(
        db=db,
        account_id=alice.account_id,
    )

    bob_balance = await get_balance(
        db=db,
        account_id=bob.account_id,
    )

    assert alice_balance == Decimal("750.00")
    assert bob_balance == Decimal("250.00")    
    