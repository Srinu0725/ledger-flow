from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.accounts.model import Account, AccountStatus, AccountType
from app.ledger.model import (
    LedgerEntry,
    Transaction,
    TransactionStatus,
    TransactionType,
)


async def create_deposit(
    db: AsyncSession,
    account_id: UUID,
    amount: Decimal,
):
    # Find user account
    result = await db.execute(
        select(Account).where(
            Account.account_id == account_id
        )
    )

    account = result.scalar_one_or_none()

    if account is None:
        raise ValueError("Account not found")

    if account.status != AccountStatus.ACTIVE:
        raise ValueError("Account is not active")

    # Find SYSTEM_CASH account
    result = await db.execute(
        select(Account).where(
            Account.account_type == AccountType.SYSTEM,
            Account.owner_name == "SYSTEM_CASH",
        )
    )

    system_account = result.scalar_one_or_none()

    if system_account is None:
        raise ValueError("SYSTEM_CASH account not found")

    # Create transaction
    transaction = Transaction(
        transaction_type=TransactionType.DEPOSIT,
        status=TransactionStatus.POSTED,
        reference="Deposit",
    )

    db.add(transaction)

    # Generate transaction ID
    await db.flush()

    # SYSTEM_CASH loses money
    system_entry = LedgerEntry(
        transaction_id=transaction.transaction_id,
        account_id=system_account.account_id,
        amount=-amount,
    )

    # USER receives money
    user_entry = LedgerEntry(
        transaction_id=transaction.transaction_id,
        account_id=account.account_id,
        amount=amount,
    )

    db.add_all([
        system_entry,
        user_entry,
    ])

    await db.commit()

    await db.refresh(transaction)

    return transaction, account


from sqlalchemy import func


async def get_balance(
    db: AsyncSession,
    account_id: UUID,
) -> Decimal:
    result = await db.execute(
        select(
            func.coalesce(
                func.sum(LedgerEntry.amount),
                0,
            )
        ).where(
            LedgerEntry.account_id == account_id
        )
    )

    balance = result.scalar_one()

    return Decimal(str(balance))

async def create_withdrawal(
    db: AsyncSession,
    account_id: UUID,
    amount: Decimal,
):
    result = await db.execute(
        select(Account).where(
            Account.account_id == account_id
        )
    )

    account = result.scalar_one_or_none()

    if account is None:
        raise ValueError("Account not found")

    if account.status != AccountStatus.ACTIVE:
        raise ValueError("Account is not active")

    current_balance = await get_balance(
        db,
        account_id,
    )

    if current_balance < amount:
        raise ValueError("Insufficient funds")

    result = await db.execute(
        select(Account).where(
            Account.account_type == AccountType.SYSTEM,
            Account.owner_name == "SYSTEM_CASH",
        )
    )

    system_account = result.scalar_one_or_none()

    if system_account is None:
        raise ValueError("SYSTEM_CASH account not found")

    transaction = Transaction(
        transaction_type=TransactionType.WITHDRAWAL,
        status=TransactionStatus.POSTED,
        reference="Withdrawal",
    )

    db.add(transaction)

    await db.flush()

    user_entry = LedgerEntry(
        transaction_id=transaction.transaction_id,
        account_id=account.account_id,
        amount=-amount,
    )

    system_entry = LedgerEntry(
        transaction_id=transaction.transaction_id,
        account_id=system_account.account_id,
        amount=amount,
    )

    db.add_all([
        user_entry,
        system_entry,
    ])

    await db.commit()

    await db.refresh(transaction)

    return transaction, account