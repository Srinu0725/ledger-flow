from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.accounts.schemas import (
    AccountResponse,
    CreateAccountRequest,
)
from app.accounts.service import create_account
from app.db.database import get_db


router = APIRouter(
    prefix="/api/v1/accounts",
    tags=["accounts"],
)


@router.post(
    "",
    response_model=AccountResponse,
)
async def create(
    request: CreateAccountRequest,
    db: AsyncSession = Depends(get_db),
):
    return await create_account(db, request)