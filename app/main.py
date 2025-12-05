from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from app.schemas.response import Response

app = FastAPI(title="Stock API", version="1.0.0")

@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content=Response.error(code=exc.status_code, msg=exc.detail).dict()
    )

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content=Response.error(code=422, msg=str(exc)).dict()
    )

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content=Response.error(code=500, msg=str(exc)).dict()
    )

from app.db.base_class import Base
from app.db.session import engine
from app.api import stock
from app.core.scheduler import start_scheduler

@app.on_event("startup")
async def startup_event():
    Base.metadata.create_all(bind=engine)
    # Check if we are in a test environment to avoid starting scheduler?
    # Or just let it run but handle the error?
    # Best is to mock it in tests.
    try:
        start_scheduler()
    except Exception as e:
        print(f"Scheduler start failed (might be already running): {e}")

app.include_router(stock.router)

@app.get("/")
def read_root():
    return {"message": "Welcome to Stock API"}
