from decimal import Decimal
from datetime import datetime, timezone
import pytest

from app.accounts.model import (
    Account,
    AccountStatus,
    AccountType,
)

from app.ledger.service import (
    create_deposit,
    create_withdrawal,
    get_balance_at,
)





@pytest.mark.asyncio
async def test_balance_at_timestamp(db):

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
    # Event 1
    # -----------------------------------------

    deposit, _ = await create_deposit(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("1000.00"),
    )

    deposit_time = deposit.created_at

    # -----------------------------------------
    # Event 2
    # -----------------------------------------

    withdrawal, _ = await create_withdrawal(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("200.00"),
    )

    withdrawal_time = withdrawal.created_at

    # -----------------------------------------
    # Before withdrawal
    # -----------------------------------------

    balance_before_withdrawal = await get_balance_at(
        db=db,
        account_id=alice.account_id,
        timestamp=deposit_time,
    )

    assert balance_before_withdrawal == Decimal("1000.00")

    # -----------------------------------------
    # After withdrawal
    # -----------------------------------------

    balance_after_withdrawal = await get_balance_at(
        db=db,
        account_id=alice.account_id,
        timestamp=withdrawal_time,
    )

    assert balance_after_withdrawal == Decimal("800.00")
    

@pytest.mark.asyncio
async def test_balance_at_before_first_transaction_is_zero(db):

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

    # Capture timestamp BEFORE the first transaction
    before = datetime.now(timezone.utc)

    # Create the first transaction
    await create_deposit(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("1000.00"),
    )

    balance = await get_balance_at(
        db=db,
        account_id=alice.account_id,
        timestamp=before,
    )

    assert balance == Decimal("0.00")