from fastapi import FastAPI
from src.models.transaction import TransactionMsg
import hazelcast

app = FastAPI()

client = hazelcast.HazelcastClient(
    cluster_name="lab3",
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
