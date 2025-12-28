from datetime import datetime, timedelta
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
    return Response.success(data=data, total=len(data))

@router.get("/stock/market/{market}", response_model=Response)
def get_stocks_by_market(
    market: str,
    service: StockService = Depends(get_stock_service)
):
    """获取指定市场的所有股票"""
    data = service.get_stocks_by_market(market)
    return Response.success(data=data, total=len(data))

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
        return Response.success(data=simple_data, total=len(simple_data))
        
    return Response.success(data=data, total=len(data))

@router.get("/stock/{symbol}", response_model=Response, response_model_exclude_none=True)
def get_stock_info(
    symbol: str,
    service: StockService = Depends(get_stock_service)
):
    """获取股票信息和当前价格"""
    data = service.get_stock_info(symbol)
    if not data:
        raise HTTPException(status_code=404, detail="Stock not found")
    return Response.success(data=data)

@router.get("/stock/{symbol}/price", response_model=Response, response_model_exclude_none=True)
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

@router.get("/stock/{symbol}/indicators", response_model=Response, response_model_exclude_none=True)
def get_stock_indicators(
    symbol: str,
    period: str = Query("7d", description="Data period (e.g. 1d, 5d, 1mo, 1y, ytd, max). Default 7d"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    service: StockService = Depends(get_stock_service)
):
    """
    获取股票价格及技术指标(MA, MACD, BOLL, TD9)
    Priority: start_date/end_date > period
    """
    target_end_date = datetime.now().date()
    target_start_date = target_end_date - timedelta(days=7) # Default
    
    # 1. Determine Range
    if start_date:
        try:
            target_start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid start_date format. Use YYYY-MM-DD")
            
    if end_date:
        try:
            target_end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid end_date format. Use YYYY-MM-DD")
            
    # If explicit range not fully provided, use period logic
    # But user might provide *only* start_date (implies start to Today)
    # OR *only* end_date (implies... what? maybe start is period?)
    # Let's verify standard behavior.
    # If start_date is set, we use it. If end_date is missing, we default to Today.
    # If start_date is MISSING, but end_date is set? We need a start.
    # In that case we can use period relative to end_date.
    
    if not start_date:
        # Check period
        # Map period string to delta
        # supported: 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max
        delta = timedelta(days=7)
        p = period.lower()
        
        if p.endswith("d"):
            try:
                days = int(p[:-1])
                delta = timedelta(days=days)
            except: pass
        elif p.endswith("mo"): # approx 30 days
            try:
                months = int(p[:-2])
                delta = timedelta(days=months * 30)
            except: pass
        elif p.endswith("y"): # approx 365 days
            try:
                years = int(p[:-1])
                delta = timedelta(days=years * 365)
            except: pass
        elif p == "ytd":
            # From Jan 1st of current year to target_end_date
            # Note: target_end_date might be set by user
            jan1 = datetime(target_end_date.year, 1, 1).date()
            target_start_date = jan1
            # Skip delta calculation
            delta = None
        elif p == "max":
            # Arbitrary long time
            delta = timedelta(days=365 * 50)
            
        if delta:
            target_start_date = target_end_date - delta

    # Convert to string
    s_date = target_start_date.strftime("%Y-%m-%d")
    e_date = target_end_date.strftime("%Y-%m-%d")
    
    data = service.get_stock_price_with_indicators(symbol, s_date, e_date)
    return Response.success(data=data, total=len(data))
