from decimal import Decimal
from uuid import UUID

from app.db import redis as redis_db


def balance_cache_key(account_id: UUID) -> str:
    return f"account:{account_id}:balance"


async def get_cached_balance(
    account_id: UUID,
) -> Decimal | None:

    key = balance_cache_key(account_id)

    value = await redis_db.redis_client.get(key)

    if value is None:
        return None

    return Decimal(value)


async def set_cached_balance(
    account_id: UUID,
    balance: Decimal,
) -> None:

    key = balance_cache_key(account_id)

    await redis_db.redis_client.set(
        key,
        str(balance),
    )


async def invalidate_balance(
    account_id: UUID,
) -> None:

    key = balance_cache_key(account_id)

    await redis_db.redis_client.delete(key)