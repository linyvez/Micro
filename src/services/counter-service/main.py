from fastapi import FastAPI, HTTPException
import asyncpg
import asyncio
import os
from contextlib import asynccontextmanager
import hazelcast
import json
import consul
import socket
from urllib.parse import urlparse

CONSUL_URL = os.getenv("CONSUL_URL", "http://consul:8500")
consul_host = urlparse(CONSUL_URL).hostname
consul_port = urlparse(CONSUL_URL).port

consul_client = consul.Consul(host=consul_host, port=consul_port)

SERVICE_HOST = socket.gethostbyname(socket.gethostname())
PORT = int(os.getenv("PORT", 8002))
SERVICE_ID = f"counter-{SERVICE_HOST}"

client = None
transaction_queue = None
DATABASE_URL = None

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
    global client, transaction_queue, DATABASE_URL

    _, hz_data = consul_client.kv.get("hazelcast_config")
    _, mq_data = consul_client.kv.get("mq_config")
    _, db_data = consul_client.kv.get("db_config")

    if (hz_data and mq_data and db_data):
        hz_config = json.loads(hz_data["Value"].decode("utf-8"))
        mq_config = json.loads(mq_data["Value"].decode("utf-8"))
        DATABASE_URL = db_data["Value"].decode("utf-8")
    else:
        raise Exception("No hazelcast_config, mq_config or db_config found in Consul")

    client = hazelcast.HazelcastClient(
        cluster_name=hz_config["cluster_name"],
        cluster_members=hz_config["cluster_members"]
    )

    transaction_queue = client.get_queue(mq_config["queue_name"])

    app.state.pool = await asyncpg.create_pool(DATABASE_URL)

    async with app.state.pool.acquire() as connection:
        await connection.execute('''
            CREATE TABLE IF NOT EXISTS users_balance (
                user_id BIGINT PRIMARY KEY,
                balance DOUBLE PRECISION NOT NULL DEFAULT 0.0
            )
        ''')
    
    health_check = consul.Check.http(f"http://{SERVICE_HOST}:{PORT}/health", interval='10s', timeout='5s')

    consul_client.agent.service.register(
        name="counter-service",
        service_id=SERVICE_ID,
        address=SERVICE_HOST,
        port=PORT,
        check=health_check
    )

    print(f"Registered in Consul as {SERVICE_ID}", flush=True)
    
    app.state.consumer_tasks = [asyncio.create_task(consume_msg(app)) for _ in range(20)]

    yield
    for task in app.state.consumer_tasks:
        task.cancel()
    await app.state.pool.close()
    consul_client.agent.service.deregister(SERVICE_ID)
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

@app.get("/health")
def health_check():
    return {"status": "ok"}
