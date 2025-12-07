from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from app.db.session import SessionLocal
from app.services.stock_service import StockService
from app.schemas.response import Response

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_stock_service(db: Session = Depends(get_db)):
    return StockService(db)

@router.get("/stock/search", response_model=Response)
def search_stock(
    name: str,
    service: StockService = Depends(get_stock_service)
):
    """模糊查询股票"""
    data = service.search_stock(name)
    return Response.success(data=data)

@router.get("/stock/market/{market}", response_model=Response)
def get_stocks_by_market(
    market: str,
    service: StockService = Depends(get_stock_service)
):
    """获取指定市场的所有股票"""
    data = service.get_stocks_by_market(market)
    return Response.success(data=data)

@router.get("/stock/price", response_model=Response)
def batch_get_prices(
    symbols: List[str] = Query(..., description="List of symbols, can be comma separated"),
    mode: str = Query("normal", regex="^(normal|simple)$", description="Response mode: normal or simple"),
    service: StockService = Depends(get_stock_service)
):
    """批量获取股票当前价格"""
    # Flattens list of splitting comma separated strings
    symbol_list = []
    for s in symbols:
        symbol_list.extend(s.split(","))
    data = service.batch_get_prices(symbol_list)
    
    if mode == "simple":
        # Return KV structure: {symbol: price}
        simple_data = {item['symbol']: item['price'] for item in data if item}
        return Response.success(data=simple_data)
        
    return Response.success(data=data)

@router.get("/stock/{symbol}", response_model=Response)
def get_stock_info(
    symbol: str,
    service: StockService = Depends(get_stock_service)
):
    """获取股票信息和当前价格"""
    data = service.get_stock_info(symbol)
    if not data:
        raise HTTPException(status_code=404, detail="Stock not found")
    return Response.success(data=data)

@router.get("/stock/{symbol}/price", response_model=Response)
def get_price_history(
    symbol: str,
    date: str = Query(..., description="Date in YYYY-MM-DD format"),
    service: StockService = Depends(get_stock_service)
):
    """获取股票指定日期的价格"""
    data = service.get_price_history(symbol, date)
    if not data:
        raise HTTPException(status_code=404, detail="Price data not found for this date")
    return Response.success(data=data)
