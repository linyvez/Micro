from fastapi import FastAPI, HTTPException
import asyncpg
import asyncio
import os
import aiohttp
from contextlib import asynccontextmanager
import hazelcast
import json

client = hazelcast.HazelcastClient(
    cluster_name="lab4",
    cluster_members=[
        "hz-1:5701",
        "hz-2:5701",
        "hz-3:5701"
    ]
)

transaction_queue = client.get_queue("transaction_queue")

CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8003")
MY_URL = os.getenv("MY_URL", "http://counter-service:8002")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:password@postgres:5432/counter_db")

async def register_in_config():
    info = {"service": "counter", "url": MY_URL}

    async with aiohttp.ClientSession() as session:
        for i in range(5):
            try:
                async with session.post(f"{CONFIG_SERVER_URL}/add_service", json=info) as response:
                    if response.status == 200:
                        print(f"Registered in config server")
                        return
            except aiohttp.ClientError:
                print(f"Couldn't register on {i + 1} try")
                await asyncio.sleep(2)
        print("Didn't register in config after 5 attempts")

async def consume_msg(app):
    def fetch_msg():
        return transaction_queue.take().result()

    while True:
        try:
            msg = await asyncio.to_thread(fetch_msg)
            data = json.loads(msg)

            user_id = data["user_id"]
            amount = data["amount"]

            async with app.state.pool.acquire() as connection:
                await connection.execute("""
                    INSERT INTO users_balance (user_id, balance) VALUES ($1, $2) 
                    ON CONFLICT (user_id) DO UPDATE SET balance = users_balance.balance + EXCLUDED.balance 
                    RETURNING balance;
                """, user_id, amount)
        except Exception as e:
            print(f"[Counter] Exception while consuming message: {e}", flush=True)
            await asyncio.sleep(1)

@asynccontextmanager
async def lifespan(app):
    await register_in_config()

    app.state.pool = await asyncpg.create_pool(DATABASE_URL)

    async with app.state.pool.acquire() as connection:
        await connection.execute('''
            CREATE TABLE IF NOT EXISTS users_balance (
                user_id BIGINT PRIMARY KEY,
                balance DOUBLE PRECISION NOT NULL DEFAULT 0.0
            )
        ''')
    
    task = asyncio.create_task(consume_msg(app))

    yield
    task.cancel()
    await app.state.pool.close()
    client.shutdown()

app = FastAPI(lifespan=lifespan)

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
