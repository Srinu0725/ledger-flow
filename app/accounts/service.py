from sqlalchemy.ext.asyncio import AsyncSession

from app.accounts.model import Account, AccountType
from app.accounts.schemas import CreateAccountRequest


async def create_account(
    db: AsyncSession,
    request: CreateAccountRequest,
) -> Account:

    account = Account(
        owner_name=request.owner_name,
        account_type=AccountType(request.account_type),
        currency=request.currency.upper(),
    )

    db.add(account)

    await db.commit()
    await db.refresh(account)

    return account