from pydantic import BaseModel

class Transaction(BaseModel):
    user_id: int
    amount: float
