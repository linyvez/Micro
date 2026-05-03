from fastapi import FastAPI
from src.models.models import TransactionMsg
import hazelcast
import os
from contextlib import asynccontextmanager
import consul
import socket
from urllib.parse import urlparse
import json

CONSUL_URL = os.getenv("CONSUL_URL", "http://consul:8500")
consul_host = urlparse(CONSUL_URL).hostname
consul_port = urlparse(CONSUL_URL).port

consul_client = consul.Consul(host=consul_host, port=consul_port)

SERVICE_HOST = socket.gethostbyname(socket.gethostname())
PORT = int(os.getenv("PORT", 8001))
SERVICE_ID = f"logging-{SERVICE_HOST}"

client = None
transactions = None

@asynccontextmanager
async def lifespan(app):
    global client, transactions

    _, data = consul_client.kv.get("hazelcast_config")

    if data and data.get("Value"):
        hz_config = json.loads(data["Value"].decode("utf-8"))
    else:
        raise Exception("No hazelcast_config found in Consul")
    
    client = hazelcast.HazelcastClient(
        cluster_name=hz_config["cluster_name"],
        cluster_members=hz_config["cluster_members"]
    )

    transactions = client.get_map("transactions").blocking()

    health_check = consul.Check.http(f"http://{SERVICE_HOST}:{PORT}/health", interval='10s', timeout='5s')

    consul_client.agent.service.register(
        name="logging-service",
        service_id=SERVICE_ID,
        address=SERVICE_HOST,
        port=PORT,
        check=health_check
    )

    print(f"Registered in Consul as {SERVICE_ID}", flush=True)

    yield
    consul_client.agent.service.deregister(SERVICE_ID)
    client.shutdown()

app = FastAPI(lifespan=lifespan)

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

@app.get("/health")
def health_check():
    return {"status": "ok"}
