from fastapi import FastAPI
from models.transaction import TransactionMsg

app = FastAPI()

transactions = {}

@app.post("/logs")
def save_message(transaction_data: TransactionMsg):
    transactions[transaction_data.transaction_id] = {"user_id": transaction_data.user_id, "amount": transaction_data.amount}
    return transaction_data.transaction_id

@app.get("/user/{user_id}")
def get_messages(user_id: int):
    result = [transaction for transaction in transactions.values() if transaction["user_id"] == user_id]
    return result
