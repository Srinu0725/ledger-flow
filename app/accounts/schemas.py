from uuid import UUID

from pydantic import BaseModel, Field


class CreateAccountRequest(BaseModel):
    owner_name: str = Field(min_length=1, max_length=100)
    account_type: str
    currency: str = Field(default="INR", min_length=3, max_length=3)


class AccountResponse(BaseModel):
    account_id: UUID
    owner_name: str
    account_type: str
    currency: str
    status: str

    model_config = {
        "from_attributes": True
    }