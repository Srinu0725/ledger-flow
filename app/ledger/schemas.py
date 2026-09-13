from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, Field
from datetime import datetime

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
    
class TransferRequest(BaseModel):
    from_account_id: UUID
    to_account_id: UUID
    amount: Decimal = Field(gt=0)


class TransferResponse(BaseModel):
    transaction_id: UUID
    from_account_id: UUID
    to_account_id: UUID
    amount: Decimal
    status: str
    
class TransactionHistoryItem(BaseModel):
    transaction_id: UUID
    transaction_type: str
    status: str
    amount: Decimal
    reference: str | None
    created_at: datetime

class TransactionHistoryResponse(BaseModel):
    account_id: UUID
    transactions: list[TransactionHistoryItem]
    limit: int
    offset: int        