from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, Field


class DepositRequest(BaseModel):
    account_id: UUID
    amount: Decimal = Field(gt=0)


class DepositResponse(BaseModel):
    transaction_id: UUID
    account_id: UUID
    amount: Decimal
    status: str
    
class WithdrawalRequest(BaseModel):
    account_id: UUID
    amount: Decimal = Field(gt=0)


class WithdrawalResponse(BaseModel):
    transaction_id: UUID
    account_id: UUID
    amount: Decimal
    status: str