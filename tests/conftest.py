import pytest_asyncio

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.db.models import Base

# Import models so they are registered with Base.metadata
from app.accounts.model import Account
from app.ledger.model import Transaction, LedgerEntry


TEST_DATABASE_URL = (
    "postgresql+asyncpg://"
    "ledger:ledger@localhost:5432/ledgerflow_test"
)


@pytest_asyncio.fixture
async def db():
    engine = create_async_engine(
        TEST_DATABASE_URL,
        echo=False,
    )

    # Fresh database schema for every test
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.drop_all)
        await connection.run_sync(Base.metadata.create_all)

    SessionLocal = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    async with SessionLocal() as session:
        yield session

    await engine.dispose()