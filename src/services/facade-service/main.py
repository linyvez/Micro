from fastapi import FastAPI
import time
import asyncio
import aiohttp
from contextlib import asynccontextmanager

class MainSession:
    session: aiohttp.ClientSession = None

main_session = MainSession()

@asynccontextmanager
async def lifespan(app):
    main_session.session = aiohttp.ClientSession()
    yield
    await main_session.session.close()

app = FastAPI(lifespan=lifespan)

LOGGING_SERVICE_URL = "http://localhost:8001"
COUNTER_SERVICE_URL = "http://localhost:8002"

@app.post("/")
async def process_transaction(user_id: int, amount: float) -> str:
    transaction_id = time.time_ns()

    request = {"transaction_id": transaction_id, "user_id": user_id, "amount": amount}

    session = main_session.session

    logging_task = session.post(url=f"{LOGGING_SERVICE_URL}/logs", json=request)
    counter_task = session.post(url=f"{COUNTER_SERVICE_URL}/transaction", json=request)

    results = await asyncio.gather(logging_task, counter_task, return_exceptions=True)

    if results[1].status != 200:
        return {"error": str(results[1])}

    balance = await results[1].json()

    return {"transaction_id": transaction_id, "balance": balance}

@app.get("/user/{user_id}")
async def get_user_info(user_id: int):
    session = main_session.session

    logging_task = session.get(url=f"{LOGGING_SERVICE_URL}/user/{user_id}")
    counter_task = session.get(url=f"{COUNTER_SERVICE_URL}/user/{user_id}")

    results = await asyncio.gather(logging_task, counter_task, return_exceptions=True)

    if results[0].status != 200:
        return {"error": str(results[0])}

    if results[1].status != 200:
        return {"error": str(results[1])}

    transactions = await results[0].json()
    balance = await results[1].json()
    
    return {"balance": balance.get("balance", 0), "transactions": transactions}

@app.get("/accounts")
async def get_all_balances():
    session = main_session.session

    result = await session.get(url=f"{COUNTER_SERVICE_URL}/accounts")
    balances = await result.json()
    return balances
