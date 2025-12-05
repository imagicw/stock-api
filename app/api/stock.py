from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from app.db.session import SessionLocal
from app.services.stock_service import StockService

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_stock_service(db: Session = Depends(get_db)):
    return StockService(db)

@router.get("/stock/search")
def search_stock(
    name: str,
    service: StockService = Depends(get_stock_service)
):
    """模糊查询股票"""
    return service.search_stock(name)

@router.get("/stocks/price")
def batch_get_prices(
    symbols: str = Query(..., description="Comma separated list of symbols"),
    service: StockService = Depends(get_stock_service)
):
    """批量获取股票当前价格"""
    symbol_list = symbols.split(",")
    return service.batch_get_prices(symbol_list)

@router.get("/stock/{symbol}")
def get_stock_info(
    symbol: str,
    service: StockService = Depends(get_stock_service)
):
    """获取股票信息和当前价格"""
    data = service.get_stock_info(symbol)
    if not data:
        raise HTTPException(status_code=404, detail="Stock not found")
    return data

@router.get("/stock/{symbol}/price")
def get_price_history(
    symbol: str,
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
    service: StockService = Depends(get_stock_service)
):
    """获取股票指定日期的价格"""
    data = service.get_price_history(symbol, date)
    if not data:
        raise HTTPException(status_code=404, detail="Price data not found for this date")
    return data
