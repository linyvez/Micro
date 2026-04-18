from fastapi import FastAPI
from src.models.models import TransactionMsg
import hazelcast
import asyncio
import os
import aiohttp
from contextlib import asynccontextmanager

CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8003")
MY_URL = os.getenv("MY_URL", "http://logging-service:8001")

async def register_in_config():
    info = {"service": "logging", "url": MY_URL}

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

@asynccontextmanager
async def lifespan(app):
    await register_in_config()

    yield
    client.shutdown()

app = FastAPI(lifespan=lifespan)

client = hazelcast.HazelcastClient(
    cluster_name="lab4",
    cluster_members=[
        "hz-1:5701",
        "hz-2:5701",
        "hz-3:5701"
    ]
)

transactions = client.get_map("transactions").blocking()

@app.post("/logs")
def save_message(transaction_data: TransactionMsg):
    print(f"[Logger] Got transaction with id:{transaction_data.transaction_id}", flush=True)
    transactions.set(transaction_data.transaction_id, {"user_id": transaction_data.user_id, "amount": transaction_data.amount})
    return transaction_data.transaction_id

@app.get("/user/{user_id}")
def get_messages(user_id: int):
    result = [{"transaction_id": transaction_id, "amount": details.get("amount")} for transaction_id, details in transactions.entry_set() if details["user_id"] == user_id]
    return result

@app.delete("/state")
def clear_up():
    transactions.clear()
