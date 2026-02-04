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

logging_time = 0
counter_time = 0

async def logging_service_wraper(session, request):
    global logging_time

    start = time.perf_counter()
    async with session.post(url=f"{LOGGING_SERVICE_URL}/logs", json=request) as response:
        await response.read()
        end = time.perf_counter()
        
        time_taken = end - start
        logging_time += time_taken

        return response

async def counter_service_wraper(session, request):
    global counter_time

    start = time.perf_counter()
    async with session.post(url=f"{COUNTER_SERVICE_URL}/transaction", json=request) as response:
        await response.read()
        end = time.perf_counter()
        
        time_taken = end - start
        counter_time += time_taken

        return response

@app.post("/")
async def process_transaction(user_id: int, amount: float):
    transaction_id = time.time_ns()

    request = {"transaction_id": transaction_id, "user_id": user_id, "amount": amount}

    session = main_session.session

    tasks = [logging_service_wraper(session, request), counter_service_wraper(session, request)]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    if results[1].status != 200:
        return {"error": str(results[1])}

    balance_data = await results[1].json()

    return {"transaction_id": transaction_id, "balance": balance_data.get("balance", 0)}

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
    
    return {"balance": balance, "transactions": transactions}

@app.get("/accounts")
async def get_all_balances():
    session = main_session.session

    async with session.get(url=f"{COUNTER_SERVICE_URL}/accounts") as response:
        balances = await response.json()
        return balances

@app.get("/time-taken")
def get_time():
    return {"logging_time": logging_time, "counter_time": counter_time}

@app.delete("/state")
async def clear_up():
    global logging_time, counter_time
    logging_time = 0
    counter_time = 0

    session = main_session.session

    async def send_clear_up(session, url):
        async with session.delete(url=url) as response:
            await response.read()

    await asyncio.gather(send_clear_up(session, f"{LOGGING_SERVICE_URL}/state"),
                         send_clear_up(session, f"{COUNTER_SERVICE_URL}/state"))
