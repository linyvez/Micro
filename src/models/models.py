from pydantic import BaseModel

class TransactionMsg(BaseModel):
    transaction_id: str
    user_id: int
    amount: float

class ServiceInfo(BaseModel):
    service: str
    url: str
