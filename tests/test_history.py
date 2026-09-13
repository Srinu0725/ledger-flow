from decimal import Decimal
import pytest
from uuid import uuid4

from app.accounts.model import (
    Account,
    AccountStatus,
    AccountType,
)

from app.ledger.service import (
    create_deposit,
    create_transfer,
    create_withdrawal,
    get_transaction_history,
)


async def create_test_accounts(db):

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

    return system, alice, bob


@pytest.mark.asyncio
async def test_transaction_history_returns_account_events(db):

    system, alice, bob = await create_test_accounts(db)

    # Alice gets ₹1000
    deposit, _ = await create_deposit(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("1000.00"),
    )

    # Alice sends ₹300 to Bob
    transfer = await create_transfer(
        db=db,
        from_account_id=alice.account_id,
        to_account_id=bob.account_id,
        amount=Decimal("300.00"),
        idempotency_key="history-transfer-001",
    )

    # Alice withdraws ₹100
    withdrawal, _ = await create_withdrawal(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("100.00"),
    )

    history = await get_transaction_history(
        db=db,
        account_id=alice.account_id,
    )

    assert len(history) == 3

    # Newest transaction first
    newest_transaction, newest_amount = history[0]

    assert (
        newest_transaction.transaction_id
        == withdrawal.transaction_id
    )

    assert newest_amount == Decimal("-100.00")

    # Transfer should be -₹300 for Alice
    transfer_transaction, transfer_amount = history[1]

    assert (
        transfer_transaction.transaction_id
        == transfer.transaction_id
    )

    assert transfer_amount == Decimal("-300.00")

    # Deposit should be +₹1000
    deposit_transaction, deposit_amount = history[2]

    assert (
        deposit_transaction.transaction_id
        == deposit.transaction_id
    )

    assert deposit_amount == Decimal("1000.00")
    
@pytest.mark.asyncio
async def test_transaction_history_is_account_specific(db):

    system, alice, bob = await create_test_accounts(db)

    await create_deposit(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("1000.00"),
    )

    transfer = await create_transfer(
        db=db,
        from_account_id=alice.account_id,
        to_account_id=bob.account_id,
        amount=Decimal("300.00"),
        idempotency_key="history-isolation-001",
    )

    alice_history = await get_transaction_history(
        db=db,
        account_id=alice.account_id,
    )

    bob_history = await get_transaction_history(
        db=db,
        account_id=bob.account_id,
    )

    assert len(alice_history) == 2
    assert len(bob_history) == 1

    # Alice sees -₹300
    alice_transfer, alice_amount = alice_history[0]

    assert (
        alice_transfer.transaction_id
        == transfer.transaction_id
    )

    assert alice_amount == Decimal("-300.00")

    # Bob sees +₹300
    bob_transfer, bob_amount = bob_history[0]

    assert (
        bob_transfer.transaction_id
        == transfer.transaction_id
    )

    assert bob_amount == Decimal("300.00")

@pytest.mark.asyncio
async def test_transaction_history_supports_pagination(db):

    system, alice, bob = await create_test_accounts(db)

    await create_deposit(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("1000.00"),
    )

    await create_withdrawal(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("100.00"),
    )

    await create_withdrawal(
        db=db,
        account_id=alice.account_id,
        amount=Decimal("200.00"),
    )

    # Newest first:
    #
    # -₹200
    # -₹100
    # +₹1000

    first_page = await get_transaction_history(
        db=db,
        account_id=alice.account_id,
        limit=2,
        offset=0,
    )

    second_page = await get_transaction_history(
        db=db,
        account_id=alice.account_id,
        limit=2,
        offset=2,
    )

    assert len(first_page) == 2
    assert len(second_page) == 1

    assert first_page[0][1] == Decimal("-200.00")
    assert first_page[1][1] == Decimal("-100.00")

    assert second_page[0][1] == Decimal("1000.00")    
    
@pytest.mark.asyncio
async def test_transaction_history_rejects_unknown_account(db):

    with pytest.raises(
        ValueError,
        match="Account not found",
    ):
        await get_transaction_history(
            db=db,
            account_id=uuid4(),
        )    