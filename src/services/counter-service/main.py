from fastapi import FastAPI, HTTPException
from src.models.transaction import TransactionMsg
import asyncpg
import os
from contextlib import asynccontextmanager

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@postgres:5432/counter_db")

@asynccontextmanager
async def lifespan(app):
    app.state.pool = await asyncpg.create_pool(DATABASE_URL)

    async with app.state.pool.acquire() as connection:
        await connection.execute('''
            CREATE TABLE IF NOT EXISTS users_balance (
                user_id BIGINT PRIMARY KEY,
                balance DOUBLE PRECISION NOT NULL DEFAULT 0.0
            )
        ''')
    yield
    await app.state.pool.close()

app = FastAPI(lifespan=lifespan)

@app.post("/transaction")
async def create_balance(data: TransactionMsg):
    async with app.state.pool.acquire() as connection:
        new_balance = await connection.fetchval("""
            INSERT INTO users_balance (user_id, balance) VALUES ($1, $2) 
            ON CONFLICT (user_id) DO UPDATE SET balance = users_balance.balance + EXCLUDED.balance 
            RETURNING balance;
        """, data.user_id, data.amount)

    return {"balance": new_balance}

@app.get("/user/{user_id}")
async def get_balance(user_id: int):
    async with app.state.pool.acquire() as connection:
        result = await connection.fetchval("SELECT balance FROM users_balance WHERE user_id = $1", user_id)

    if result is not None:
        return result
    else:
        raise HTTPException(status_code=404, detail="User not found")

@app.get("/accounts")
async def get_all_balances():
    async with app.state.pool.acquire() as connection:
        result = await connection.fetch("SELECT user_id, balance FROM users_balance")

    return {user['user_id']: user['balance'] for user in result}

@app.delete("/state")
async def clear_up():
    async with app.state.pool.acquire() as connection:
        await connection.execute("TRUNCATE TABLE users_balance")
    
    return {"message": "Successfully cleared users_balance table."}
