from fastapi import FastAPI, HTTPException
from models import Transaction

app = FastAPI()

users_balance = {}

@app.post("/transaction")
def create_balance(data: Transaction):
    balance = users_balance.get(data.user_id, 0)
    users_balance[data.user_id] = balance + data.amount
    return users_balance[data.user_id]

@app.get("/user/{user_id}")
def get_balance(user_id: int):
    result = users_balance.get(user_id)
    if result is not None:
        return result
    else:
        raise HTTPException(status_code=404, detail="User not found")

@app.get("/accounts")
def get_all_balances():
    return users_balance
