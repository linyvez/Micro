from fastapi import FastAPI, HTTPException
from src.models.models import ServiceInfo

app = FastAPI()

LOGGING_SERVICES = set()
COUNTER_SERVICES = set()

@app.post("/add_service")
def add_service(info: ServiceInfo):
    if info.service == "logging":
        LOGGING_SERVICES.add(info.url)
    elif info.service == "counter":
        COUNTER_SERVICES.add(info.url)
    else:
        raise HTTPException(status_code=400, detail=f"Service {info.service} does not exist.")

    return {"message": f"Registered {info.service} at {info.url}"}

@app.get("/give_services")
def get_services():
    return {
        "logging": list(LOGGING_SERVICES),
        "counter": list(COUNTER_SERVICES)
    }
