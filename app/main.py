from fastapi import FastAPI
from app.core.config import settings

app = FastAPI(title="Stock API", version="1.0.0")

from app.db.base_class import Base
from app.db.session import engine
from app.api import stock
from app.core.scheduler import start_scheduler

@app.on_event("startup")
def on_startup():
    Base.metadata.create_all(bind=engine)
    start_scheduler()

app.include_router(stock.router)

@app.get("/")
def read_root():
    return {"message": "Welcome to Stock API"}
