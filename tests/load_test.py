import time
import asyncio
import aiohttp

FACADE_SERVICE_URL = "http://localhost:8000"

async def client(user_id: int, session):
    request = {"user_id": user_id, "amount": 1}
    for _ in range(10000):
        await session.post(f"{FACADE_SERVICE_URL}/", params=request)

async def load_test(scenario: int = 1):
    '''
    Scenario = 1:
    10 клієнтів одночасно роблять по 10К однакових транзакцій
    по додаванню 1 на свій рахунок. У результаті кінцеве значення
    балансу на 10-и рахунках має бути по 10К.

    Scenario = 2:
    10 клієнтів одночасно роблять по 10К однакових транзакцій
    по додаванню 1 на один і той самий рахунок. У результаті
    кінцеве значення балансу на одному рахунках має бути 100К.
    '''
    async with aiohttp.ClientSession() as session:
        if scenario == 1:
            tasks = [client(i, session) for i in range(10)]
        elif scenario == 2:
            tasks = [client(1, session) for _ in range(10)]
        else:
            raise Exception("Scenario must be either 1 or 2")

        start = time.perf_counter()
        await asyncio.gather(*tasks)
        end = time.perf_counter()

        total_time = end - start

        await asyncio.sleep(0.5)

        async with session.get(f"{FACADE_SERVICE_URL}/time-taken") as response:
            services_time = await response.json()
        
        async with session.get(f"{FACADE_SERVICE_URL}/accounts") as response:
            all_balances = await response.json()
    
    correct = True
    for b in all_balances.values():
        if (scenario == 1 and b != 10000) or (scenario == 2 and b != 100000):
            correct = False
            break
    
    rps = 100000 / total_time
    
    return {"total_time": round(total_time, 2), 
            "rps": round(rps, 1), 
            "logging_time": round(services_time.get("logging_time"), 2), 
            "counter_time": round(services_time.get("counter_time"), 2), 
            "correct": correct}

async def main():
    async with aiohttp.ClientSession() as session:
        print("Clearing up...")
        await session.delete(url=f"{FACADE_SERVICE_URL}/state")

        print("Executing Test #1...\nExpected result: 10 accounts, each with balance of 10,000")

        result_1 = await load_test(scenario=1)
        print(result_1)

        print("Clearing up...")
        await session.delete(url=f"{FACADE_SERVICE_URL}/state")
        await asyncio.sleep(2)

        print("Executing Test #2...\nExpected result: 1 account with balance of 100,000")

        result_2 = await load_test(scenario=2)
        print(result_2)

        print("Clearing up...")
        await session.delete(url=f"{FACADE_SERVICE_URL}/state")

        print("Done")

if __name__ == "__main__":
    asyncio.run(main())
