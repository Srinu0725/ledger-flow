from datetime import datetime, timezone
from decimal import Decimal

import pytest
from httpx import AsyncClient, ASGITransport

from app.main import app


@pytest.mark.asyncio
async def test_balance_at_api_before_transaction():

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as client:

        # Create Alice
        response = await client.post(
            "/api/v1/accounts",
            json={
                "owner_name": "Alice",
                "account_type": "SAVINGS",
                "currency": "INR",
            },
        )

        assert response.status_code == 200

        alice_id = response.json()["account_id"]

        # A timestamp far in the past
        before = datetime(
            2020,
            1,
            1,
            tzinfo=timezone.utc,
        )

        # Deposit
        response = await client.post(
            "/api/v1/ledger/deposit",
            json={
                "account_id": alice_id,
                "amount": "1000.00",
            },
        )

        assert response.status_code == 200

        # Ask for balance before the deposit
        response = await client.get(
            f"/api/v1/ledger/accounts/{alice_id}/balance-at",
            params={
                "timestamp": before.isoformat(),
            },
        )

        assert response.status_code == 200

        data = response.json()

        assert Decimal(data["balance"]) == Decimal("0.00")