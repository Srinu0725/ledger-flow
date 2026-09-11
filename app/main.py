from fastapi import FastAPI

from app.accounts.router import router as accounts_router
from app.ledger.router import router as ledger_router


app = FastAPI(
    title="LedgerFlow",
    version="1.0.0",
)

app.include_router(accounts_router)
app.include_router(ledger_router)

@app.get("/health")
async def health():
    return {
        "status": "healthy"
    }