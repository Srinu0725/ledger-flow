import pytest

from decimal import Decimal

from app.accounts.model import (
    Account,
    AccountStatus,
    AccountType,
)

from app.db import redis as redis_db

from app.ledger.cache import balance_cache_key
from app.ledger.service import (
    create_deposit,
    create_withdrawal,
    create_transfer,
    get_balance,
)

@pytest.mark.asyncio
async def test_balance_is_cached(db):

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

    # -----------------------------------------
    # Deposit ₹1000
    # -----------------------------------------

    await create_deposit(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("1000.00"),
    )

    key = balance_cache_key(
        alice.account_id
    )

    # Make sure cache is empty
    await redis_db.redis_client.delete(key)

    # -----------------------------------------
    # First request
    # PostgreSQL → Redis
    # -----------------------------------------

    balance = await get_balance(
        db=db,
        account_id=alice.account_id,
    )

    assert balance == Decimal("1000.00")

    # -----------------------------------------
    # Verify Redis contains balance
    # -----------------------------------------

    cached = await redis_db.redis_client.get(key)

    assert cached == "1000.00"

    # -----------------------------------------
    # Second request
    # Redis HIT
    # -----------------------------------------

    balance = await get_balance(
        db=db,
        account_id=alice.account_id,
    )

    assert balance == Decimal("1000.00")


@pytest.mark.asyncio
async def test_withdrawal_invalidates_balance_cache(db):

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

    # -----------------------------------------
    # Deposit ₹1000
    # -----------------------------------------

    await create_deposit(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("1000.00"),
    )

    key = balance_cache_key(
        alice.account_id
    )

    # -----------------------------------------
    # Populate cache
    # -----------------------------------------

    balance = await get_balance(
        db=db,
        account_id=alice.account_id,
    )

    assert balance == Decimal("1000.00")

    cached = await redis_db.redis_client.get(key)

    assert cached == "1000.00"

    # -----------------------------------------
    # Withdraw ₹200
    # -----------------------------------------

    await create_withdrawal(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("200.00"),
    )

    # -----------------------------------------
    # Cache MUST be invalidated
    # -----------------------------------------

    cached = await redis_db.redis_client.get(key)

    assert cached is None

    # -----------------------------------------
    # Next request
    # PostgreSQL → Redis
    # -----------------------------------------

    balance = await get_balance(
        db=db,
        account_id=alice.account_id,
    )

    assert balance == Decimal("800.00")

    # Verify Redis now contains fresh balance
    cached = await redis_db.redis_client.get(key)

    assert cached == "800.00"
    
    
@pytest.mark.asyncio
async def test_transfer_invalidates_both_balance_caches(db):

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

    # -----------------------------------------
    # Give Alice ₹1000
    # -----------------------------------------

    await create_deposit(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("1000.00"),
    )

    # -----------------------------------------
    # Populate both caches
    # -----------------------------------------

    alice_balance = await get_balance(
        db=db,
        account_id=alice.account_id,
    )

    bob_balance = await get_balance(
        db=db,
        account_id=bob.account_id,
    )

    assert alice_balance == Decimal("1000.00")
    assert bob_balance == Decimal("0.00")

    alice_key = balance_cache_key(
        alice.account_id
    )

    bob_key = balance_cache_key(
        bob.account_id
    )

    assert await redis_db.redis_client.get(
        alice_key
    ) == "1000.00"

    assert Decimal(
    await redis_db.redis_client.get(bob_key)
) == Decimal("0.00")

    # -----------------------------------------
    # Transfer ₹200
    # -----------------------------------------

    await create_transfer(
        db=db,
        from_account_id=alice.account_id,
        to_account_id=bob.account_id,
        amount=Decimal("200.00"),
        idempotency_key="cache-transfer-001",
    )

    # -----------------------------------------
    # BOTH caches must be invalidated
    # -----------------------------------------

    assert await redis_db.redis_client.get(
        alice_key
    ) is None

    assert await redis_db.redis_client.get(
        bob_key
    ) is None

    # -----------------------------------------
    # Fresh balances
    # -----------------------------------------

    alice_balance = await get_balance(
        db=db,
        account_id=alice.account_id,
    )

    bob_balance = await get_balance(
        db=db,
        account_id=bob.account_id,
    )

    assert alice_balance == Decimal("800.00")
    assert bob_balance == Decimal("200.00")    
    