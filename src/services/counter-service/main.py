from fastapi import FastAPI, HTTPException
from models.transaction import TransactionMsg
import asyncio
from collections import defaultdict

app = FastAPI()

users_balance = {}
locks = defaultdict(asyncio.Lock)

@app.post("/transaction")
async def create_balance(data: TransactionMsg):
    async with locks[data.user_id]:
        balance = users_balance.get(data.user_id, 0)
        users_balance[data.user_id] = balance + data.amount
        return {"balance": users_balance[data.user_id]}

@app.get("/user/{user_id}")
async def get_balance(user_id: int):
    result = users_balance.get(user_id)
    if result is not None:
        return result
    else:
        raise HTTPException(status_code=404, detail="User not found")

@app.get("/accounts")
async def get_all_balances():
    return users_balance

@app.delete("/state")
async def clear_up():
    users_balance.clear()
    locks.clear()
