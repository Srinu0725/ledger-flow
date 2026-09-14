from decimal import Decimal
from uuid import UUID
import hashlib
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError
from datetime import datetime,timezone


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

def generate_transfer_hash(
    from_account_id: UUID,
    to_account_id: UUID,
    amount: Decimal,
) -> str:

    payload = (
        f"{from_account_id}:"
        f"{to_account_id}:"
        f"{amount}"
    )

    return hashlib.sha256(
        payload.encode("utf-8")
    ).hexdigest()


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

    transaction = Transaction(
        transaction_type=TransactionType.DEPOSIT,
        status=TransactionStatus.POSTED,
        reference="Deposit",
    )

    db.add(transaction)

    await db.flush()

    # SYSTEM_CASH loses money
    system_entry = LedgerEntry(
        transaction_id=transaction.transaction_id,
        account_id=system_account.account_id,
        amount=-amount,
    )

    # User receives money
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


async def get_balance(
    db: AsyncSession,
    account_id: UUID,
) -> Decimal:

    # Make sure account exists
    result = await db.execute(
        select(Account).where(
            Account.account_id == account_id
        )
    )

    account = result.scalar_one_or_none()

    if account is None:
        raise ValueError("Account not found")

    # Reconstruct balance from ledger
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
    # Lock account before checking balance
    result = await db.execute(
        select(Account)
        .where(
            Account.account_id == account_id
        )
        .with_for_update()
    )

    account = result.scalar_one_or_none()

    if account is None:
        raise ValueError("Account not found")

    if account.status != AccountStatus.ACTIVE:
        raise ValueError("Account is not active")

    # Calculate balance while account is locked
    current_balance = await get_balance(
        db,
        account_id,
    )

    if current_balance < amount:
        raise ValueError("Insufficient funds")

    # Find SYSTEM_CASH
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

    # User loses money
    user_entry = LedgerEntry(
        transaction_id=transaction.transaction_id,
        account_id=account.account_id,
        amount=-amount,
    )

    # SYSTEM_CASH receives money
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

async def create_transfer(
    db: AsyncSession,
    from_account_id: UUID,
    to_account_id: UUID,
    amount: Decimal,
    idempotency_key: str,
):
    request_hash = generate_transfer_hash(
        from_account_id,
        to_account_id,
        amount,
    )

    # --------------------------------------------------
    # 1. Fast idempotency check
    # --------------------------------------------------

    result = await db.execute(
        select(Transaction).where(
            Transaction.idempotency_key == idempotency_key
        )
    )

    existing_transaction = result.scalar_one_or_none()

    if existing_transaction is not None:

        if existing_transaction.request_hash != request_hash:
            raise ValueError(
                "Idempotency key already used for a different request"
            )

        return existing_transaction

    # --------------------------------------------------
    # 2. Validate accounts
    # --------------------------------------------------

    if from_account_id == to_account_id:
        raise ValueError(
            "Source and destination accounts must be different"
        )

    account_ids = sorted([
        from_account_id,
        to_account_id,
    ])

    result = await db.execute(
        select(Account)
        .where(Account.account_id.in_(account_ids))
        .order_by(Account.account_id)
        .with_for_update()
    )

    accounts = result.scalars().all()

    if len(accounts) != 2:
        raise ValueError(
            "One or both accounts not found"
        )

    account_map = {
        account.account_id: account
        for account in accounts
    }

    source = account_map[from_account_id]
    destination = account_map[to_account_id]

    if source.status != AccountStatus.ACTIVE:
        raise ValueError(
            "Source account is not active"
        )

    if destination.status != AccountStatus.ACTIVE:
        raise ValueError(
            "Destination account is not active"
        )

    if source.currency != destination.currency:
        raise ValueError(
            "Currency mismatch"
        )

    # --------------------------------------------------
    # 3. Calculate source balance
    # --------------------------------------------------

    result = await db.execute(
        select(
            func.coalesce(
                func.sum(LedgerEntry.amount),
                0,
            )
        ).where(
            LedgerEntry.account_id == from_account_id
        )
    )

    source_balance = Decimal(
        str(result.scalar_one())
    )

    if source_balance < amount:
        raise ValueError(
            "Insufficient funds"
        )

    # --------------------------------------------------
    # 4. Create transaction inside SAVEPOINT
    # --------------------------------------------------

    try:

        async with db.begin_nested():

            transaction = Transaction(
                transaction_type=TransactionType.TRANSFER,
                status=TransactionStatus.POSTED,
                reference="Transfer",
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            )

            db.add(transaction)

            await db.flush()

            source_entry = LedgerEntry(
                transaction_id=transaction.transaction_id,
                account_id=from_account_id,
                amount=-amount,
            )

            destination_entry = LedgerEntry(
                transaction_id=transaction.transaction_id,
                account_id=to_account_id,
                amount=amount,
            )

            db.add_all([
                source_entry,
                destination_entry,
            ])

        # Savepoint succeeded.
        # Now commit the outer transaction.
        await db.commit()

        await db.refresh(transaction)

        return transaction

    except IntegrityError:

        # Another concurrent request won the idempotency race.
        await db.rollback()

        result = await db.execute(
            select(Transaction).where(
                Transaction.idempotency_key == idempotency_key
            )
        )

        existing_transaction = result.scalar_one_or_none()

        if existing_transaction is None:
            raise

        if existing_transaction.request_hash != request_hash:
            raise ValueError(
                "Idempotency key already used for a different request"
            )

        return existing_transaction

    # --------------------------------------------------
    # 2. Validate accounts
    # --------------------------------------------------

    if from_account_id == to_account_id:
        raise ValueError(
            "Source and destination accounts must be different"
        )

    # Always lock accounts in deterministic order.
    # This prevents deadlocks between opposite transfers.
    account_ids = sorted([
        from_account_id,
        to_account_id,
    ])

    result = await db.execute(
        select(Account)
        .where(
            Account.account_id.in_(account_ids)
        )
        .order_by(Account.account_id)
        .with_for_update()
    )

    accounts = result.scalars().all()

    if len(accounts) != 2:
        raise ValueError(
            "One or both accounts not found"
        )

    account_map = {
        account.account_id: account
        for account in accounts
    }

    source = account_map[from_account_id]
    destination = account_map[to_account_id]

    if source.status != AccountStatus.ACTIVE:
        raise ValueError(
            "Source account is not active"
        )

    if destination.status != AccountStatus.ACTIVE:
        raise ValueError(
            "Destination account is not active"
        )

    if source.currency != destination.currency:
        raise ValueError(
            "Currency mismatch"
        )

    # --------------------------------------------------
    # 3. Calculate source balance
    # --------------------------------------------------

    result = await db.execute(
        select(
            func.coalesce(
                func.sum(LedgerEntry.amount),
                0,
            )
        ).where(
            LedgerEntry.account_id == from_account_id
        )
    )

    source_balance = Decimal(
        str(result.scalar_one())
    )

    if source_balance < amount:
        raise ValueError(
            "Insufficient funds"
        )

    # --------------------------------------------------
    # 4. Create transaction
    # --------------------------------------------------

    transaction = Transaction(
        transaction_type=TransactionType.TRANSFER,
        status=TransactionStatus.POSTED,
        reference="Transfer",
        idempotency_key=idempotency_key,
        request_hash=request_hash,
    )

    db.add(transaction)

    await db.flush()

    # --------------------------------------------------
    # 5. Create double-entry ledger records
    # --------------------------------------------------

    source_entry = LedgerEntry(
        transaction_id=transaction.transaction_id,
        account_id=from_account_id,
        amount=-amount,
    )

    destination_entry = LedgerEntry(
        transaction_id=transaction.transaction_id,
        account_id=to_account_id,
        amount=amount,
    )

    db.add_all([
        source_entry,
        destination_entry,
    ])

    # --------------------------------------------------
    # 6. Commit atomically
    # --------------------------------------------------

    await db.commit()

    await db.refresh(transaction)

    return transaction


async def get_transaction_history(
    db: AsyncSession,
    account_id: UUID,
    limit: int = 50,
    offset: int = 0,
):
    # Make sure account exists
    result = await db.execute(
        select(Account).where(
            Account.account_id == account_id
        )
    )

    account = result.scalar_one_or_none()

    if account is None:
        raise ValueError("Account not found")

    result = await db.execute(
        select(
            Transaction,
            LedgerEntry.amount,
        )
        .join(
            LedgerEntry,
            LedgerEntry.transaction_id
            == Transaction.transaction_id,
        )
        .where(
            LedgerEntry.account_id == account_id
        )
        .order_by(
            Transaction.created_at.desc()
        )
        .limit(limit)
        .offset(offset)
    )

    rows = result.all()

    return rows


async def get_balance_at(
    db: AsyncSession,
    account_id: UUID,
    timestamp: datetime,
) -> Decimal:

    # Verify account exists
    result = await db.execute(
        select(Account).where(
            Account.account_id == account_id
        )
    )

    account = result.scalar_one_or_none()

    if account is None:
        raise ValueError("Account not found")

    # Reconstruct balance using transaction time
    result = await db.execute(
        select(
            func.coalesce(
                func.sum(LedgerEntry.amount),
                0,
            )
        )
        .join(
            Transaction,
            Transaction.transaction_id
            == LedgerEntry.transaction_id,
        )
        .where(
            LedgerEntry.account_id == account_id,
            Transaction.created_at <= timestamp,
        )
    )

    balance = result.scalar_one()

    return Decimal(str(balance))