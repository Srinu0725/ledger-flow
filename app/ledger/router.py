from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID
from fastapi import Header
from app.db.database import get_db
from app.ledger.schemas import (
    DepositRequest,
    DepositResponse,
    WithdrawalRequest,
    WithdrawalResponse,
    TransferRequest,
    TransferResponse,
)

from app.ledger.service import (
    create_deposit,
    create_withdrawal,
    create_transfer,
    get_balance,
)

router = APIRouter(
    prefix="/api/v1/ledger",
    tags=["ledger"],
)


@router.post(
    "/deposit",
    response_model=DepositResponse,
)
async def deposit(
    request: DepositRequest,
    db: AsyncSession = Depends(get_db),
):
    try:
        transaction, account = await create_deposit(
            db=db,
            account_id=request.account_id,
            amount=request.amount,
        )

        return DepositResponse(
            transaction_id=transaction.transaction_id,
            account_id=account.account_id,
            amount=request.amount,
            status=transaction.status.value,
        )

    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=str(e),
        )
        
            
@router.get("/accounts/{account_id}/balance")
async def balance(
    account_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    try:
        current_balance = await get_balance(
            db,
            account_id,
        )

        return {
            "account_id": account_id,
            "balance": current_balance,
            "currency": "INR",
        }

    except ValueError as e:
        raise HTTPException(
            status_code=404,
            detail=str(e),
        )
        
@router.post(
    "/withdraw",
    response_model=WithdrawalResponse,
)
async def withdraw(
    request: WithdrawalRequest,
    db: AsyncSession = Depends(get_db),
):
    try:
        transaction, account = await create_withdrawal(
            db=db,
            account_id=request.account_id,
            amount=request.amount,
        )

        return WithdrawalResponse(
            transaction_id=transaction.transaction_id,
            account_id=account.account_id,
            amount=request.amount,
            status=transaction.status.value,
        )

    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=str(e),
        )
        
@router.post(
    "/transfer",
    response_model=TransferResponse,
)
async def transfer(
    request: TransferRequest,
    idempotency_key: str = Header(..., alias="Idempotency-Key"),
    db: AsyncSession = Depends(get_db),
):
    try:
        transaction = await create_transfer(
            db=db,
            from_account_id=request.from_account_id,
            to_account_id=request.to_account_id,
            amount=request.amount,
            idempotency_key=idempotency_key,
        )

        return TransferResponse(
            transaction_id=transaction.transaction_id,
            from_account_id=request.from_account_id,
            to_account_id=request.to_account_id,
            amount=request.amount,
            status=transaction.status.value,
        )

    except ValueError as e:
        raise HTTPException(
            status_code=400,
            detail=str(e),
        )        
        