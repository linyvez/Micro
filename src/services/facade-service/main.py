from fastapi import FastAPI, HTTPException
import time
import asyncio
import aiohttp
from contextlib import asynccontextmanager
import os
import random

class MainSession:
    session: aiohttp.ClientSession = None

main_session = MainSession()

@asynccontextmanager
async def lifespan(app):
    connector = aiohttp.TCPConnector(limit=0, limit_per_host=0)
    main_session.session = aiohttp.ClientSession(connector=connector)
    yield
    await main_session.session.close()

app = FastAPI(lifespan=lifespan)

LOGGING_SERVICE_URLS = os.getenv("LOGGING_URLS", "http://localhost:8001").split(",")
COUNTER_SERVICE_URL = os.getenv("COUNTER_URL", "http://localhost:8002")

logging_time = 0
counter_time = 0

async def logging_service_wraper(session, request):
    global logging_time
    start = time.perf_counter()

    urls = LOGGING_SERVICE_URLS.copy()
    random.shuffle(urls)

    for url in urls:
        try:
            async with session.post(url=f"{url}/logs", json=request) as response:
                status = response.status
                data = await response.read()

                end = time.perf_counter()
                
                time_taken = end - start
                logging_time += time_taken

                return status, data
        except aiohttp.ClientError:
            print(f"[Facade] Logger {url} is down.", flush=True)
            continue
    
    raise HTTPException(status_code=503, detail="All loggers are currently down.")

async def counter_service_wraper(session, request):
    global counter_time

    start = time.perf_counter()
    async with session.post(url=f"{COUNTER_SERVICE_URL}/transaction", json=request) as response:
        status = response.status
        data = await response.json() if status == 200 else await response.read()

        end = time.perf_counter()
        
        time_taken = end - start
        counter_time += time_taken

        return status, data

async def get_data_wraper(session, lst, path):
    urls = lst.copy()
    random.shuffle(urls)

    for url in urls:
        try:
            async with session.get(url=f"{url}{path}") as response:
                data = await response.json()
                return response.status, data
        except aiohttp.ClientError:
            print(f"[Facade] {url} is down.", flush=True)
            continue
    
    raise HTTPException(status_code=503, detail="Services are down.")

@app.post("/")
async def process_transaction(user_id: int, amount: float):
    transaction_id = str(time.time_ns())

    request = {"transaction_id": transaction_id, "user_id": user_id, "amount": amount}

    session = main_session.session

    tasks = [logging_service_wraper(session, request), counter_service_wraper(session, request)]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    if isinstance(results[0], Exception):
        raise HTTPException(status_code=503, detail="Loggers are unavailable")
    if isinstance(results[1], Exception):
        raise HTTPException(status_code=503, detail="Counter service unavailable")

    counter_status, counter_data = results[1]

    if counter_status != 200:
        raise HTTPException(status_code=counter_status, detail="Counter service error")

    return {"transaction_id": transaction_id, "balance": counter_data.get("balance", 0)}

@app.get("/user/{user_id}")
async def get_user_info(user_id: int):
    session = main_session.session

    tasks = [get_data_wraper(session, LOGGING_SERVICE_URLS, f"/user/{user_id}"), get_data_wraper(session, [COUNTER_SERVICE_URL], f"/user/{user_id}")]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    if isinstance(results[0], Exception):
        raise HTTPException(status_code=503, detail="Loggers are unavailable")
    if isinstance(results[1], Exception):
        raise HTTPException(status_code=503, detail="Counter service unavailable")

    (logging_status, logging_result), (counter_status, counter_result) = results

    if logging_status != 200:
        raise HTTPException(status_code=logging_status, detail="Logging service error")

    if counter_status == 404:
        raise HTTPException(status_code=404, detail=f"User {user_id} not found")

    if counter_status != 200:
       raise HTTPException(status_code=counter_status, detail="Counter service error")
    
    return {"balance": counter_result, "transactions": logging_result}

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

    async def send_clear_up(session, lst, path):
        urls = lst.copy()
        random.shuffle(urls)

        for url in urls:
            try:
                async with session.delete(url=f"{url}{path}") as response:
                    await response.read()
                    return
            except aiohttp.ClientError:
                continue

    await asyncio.gather(send_clear_up(session, LOGGING_SERVICE_URLS, "/state"),
                         send_clear_up(session, [COUNTER_SERVICE_URL], "/state"))

    return {"status": "Successfully cleared"}
