from pydantic import BaseModel

class TransactionMsg(BaseModel):
    transaction_id: int
    user_id: int
    amount: float
