from fastapi import FastAPI, HTTPException
import time
import asyncio
import aiohttp
from contextlib import asynccontextmanager
import os
import random
import hazelcast
import json

class MainSession:
    session: aiohttp.ClientSession = None

main_session = MainSession()

@asynccontextmanager
async def lifespan(app):
    connector = aiohttp.TCPConnector(limit=0, limit_per_host=0)
    main_session.session = aiohttp.ClientSession(connector=connector)
    yield
    await main_session.session.close()
    client.shutdown()

app = FastAPI(lifespan=lifespan)

CONFIG_SERVER_URL = os.getenv("CONFIG_SERVER_URL", "http://config-server:8003")

client = hazelcast.HazelcastClient(
    cluster_name="lab4",
    cluster_members=[
        "hz-1:5701",
        "hz-2:5701",
        "hz-3:5701"
    ]
)

transaction_queue = client.get_queue("transaction_queue")

async def get_service_urls(service: str):
    session = main_session.session

    try:
        async with session.get(f"{CONFIG_SERVER_URL}/give_services") as response:
            if response.status == 200:
                data = await response.json()
                urls = data.get(service, [])

                if not urls:
                    raise HTTPException(status_code=503, detail=f"No {service} services")
                
                return urls
            raise HTTPException(status_code=500, detail="Config server error")
    except aiohttp.ClientError:
        raise HTTPException(status_code=503, detail="Couldn't reach config server")

logging_time = 0
counter_time = 0

async def logging_service_wraper(session, request):
    global logging_time
    start = time.perf_counter()

    urls = await get_service_urls("logging")
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
    
    msg = json.dumps(request)
    await asyncio.to_thread(transaction_queue.offer, msg)

    end = time.perf_counter()
    
    time_taken = end - start
    counter_time += time_taken

    return 200, {"balance": "In progress..."}

async def get_data_wraper(session, lst, path):
    urls = lst.copy()
    random.shuffle(urls)

    timeout = aiohttp.ClientTimeout(total=2)

    for url in urls:
        try:
            async with session.get(url=f"{url}{path}", timeout=timeout) as response:
                data = await response.json()
                return response.status, data
        except (aiohttp.ClientError, asyncio.TimeoutError):
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

    logging_urls = await get_service_urls("logging")
    counter_urls = await get_service_urls("counter")

    tasks = [get_data_wraper(session, logging_urls, f"/user/{user_id}"), get_data_wraper(session, counter_urls, f"/user/{user_id}")]

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
async def get_all_balances(): # for now there is only 1 counter, but shuffle for future
    session = main_session.session
    counter_urls = await get_service_urls("counter")

    urls = counter_urls.copy()
    random.shuffle(urls)

    timeout = aiohttp.ClientTimeout(total=2)

    for url in urls:
        try:
            async with session.get(url=f"{url}/accounts", timeout=timeout) as response:
                balances = await response.json()
                return balances
        except (aiohttp.ClientError, asyncio.TimeoutError):
            continue
    raise HTTPException(status_code=503, detail="Counter service(s) are down.")

@app.get("/time-taken")
def get_time():
    return {"logging_time": logging_time, "counter_time": counter_time}

@app.delete("/state")
async def clear_up():
    global logging_time, counter_time
    logging_time = 0
    counter_time = 0

    session = main_session.session

    logging_urls = await get_service_urls("logging")
    counter_urls = await get_service_urls("counter")

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

    await asyncio.gather(send_clear_up(session, logging_urls, "/state"),
                         send_clear_up(session, counter_urls, "/state"))

    return {"status": "Successfully cleared"}
