import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from sqlalchemy.orm import Session
from app.core.redis import get_redis_client
from app.db.session import SessionLocal
from app.models.stock import Stock, PriceHistory
from app.services.provider import DataProviderFactory

class StockService:
    def __init__(self, db: Session):
        self.db = db
        self.redis = get_redis_client()
        self.CACHE_EXPIRE = 3600 * 24 # 24 hours

    def get_stock_info(self, symbol: str) -> Optional[Dict]:
        # 1. Check Redis
        cache_key = f"stock:info:{symbol}"
        try:
            cached_data = self.redis.get(cache_key)
            if cached_data:
                return json.loads(cached_data)
        except Exception as e:
            print(f"Redis error: {e}")
            cached_data = None

        # 2. Fetch from Provider
        provider = DataProviderFactory.get_provider(symbol)
        data = provider.get_stock_info(symbol)
        
        if data:
            # 3. Save/Update DB (Basic Info)
            stock = self.db.query(Stock).filter(Stock.symbol == symbol).first()
            if not stock:
                stock = Stock(
                    code=symbol, # Using symbol as code for simplicity in this logic
                    symbol=symbol,
                    name=data['name'],
                    market=data['market'],
                    type="stock",
                    update_time=datetime.utcnow()
                )
                self.db.add(stock)
            else:
                stock.name = data['name']
                stock.update_time = datetime.utcnow()
            self.db.commit()

            # 4. Save to Redis
            try:
                self.redis.set(cache_key, json.dumps(data), ex=self.CACHE_EXPIRE)
            except Exception as e:
                print(f"Redis set error: {e}")
            
        return data

    def get_price_history(self, symbol: str, date: str) -> Optional[Dict]:
        # Check DB first
        # Convert date string to object
        target_date = datetime.strptime(date, "%Y-%m-%d").date()
        
        # Need to find the stock first to get the ID/Code
        stock = self.db.query(Stock).filter(Stock.symbol == symbol).first()
        if stock:
            history = self.db.query(PriceHistory).filter(
                PriceHistory.stock_code == stock.code,
                PriceHistory.date == target_date
            ).first()
            if history:
                return {
                    "date": date,
                    "open": history.open,
                    "close": history.close,
                    "high": history.high,
                    "low": history.low,
                    "volume": history.volume
                }

        # If not in DB, fetch range from provider (e.g., surrounding days or just that day)
        # Fetching a small range to be safe, or just the specific date if provider supports
        provider = DataProviderFactory.get_provider(symbol)
        # Fetching 1 month around the date to populate DB
        start_date = (target_date - timedelta(days=10)).strftime("%Y-%m-%d")
        end_date = (target_date + timedelta(days=10)).strftime("%Y-%m-%d")
        
        hist_data = provider.get_price_history(symbol, start_date, end_date)
        
        # Save to DB
        if hist_data:
            # Ensure stock exists
            if not stock:
                # We need basic info first, try to get it
                self.get_stock_info(symbol)
                stock = self.db.query(Stock).filter(Stock.symbol == symbol).first()
            
            if stock:
                for item in hist_data:
                    item_date = datetime.strptime(item['date'], "%Y-%m-%d").date()
                    # Check if exists
                    exists = self.db.query(PriceHistory).filter(
                        PriceHistory.stock_code == stock.code,
                        PriceHistory.date == item_date
                    ).first()
                    if not exists:
                        ph = PriceHistory(
                            stock_code=stock.code,
                            date=item_date,
                            open=item['open'],
                            close=item['close'],
                            high=item['high'],
                            low=item['low'],
                            volume=item['volume']
                        )
                        self.db.add(ph)
                self.db.commit()

        # Return specific date
        for item in hist_data:
            if item['date'] == date:
                return item
        return None

    def search_stock(self, name: str) -> List[Dict]:
        # Search in local DB
        # Using ILIKE for case-insensitive search if supported, but SQLite uses LIKE (case-insensitive for ASCII)
        # For Chinese characters, standard LIKE works.
        query = f"%{name}%"
        stocks = self.db.query(Stock).filter(
            (Stock.code.like(query)) | 
            (Stock.name.like(query)) |
            (Stock.symbol.like(query))
        ).limit(20).all()
        
        return [{
            "symbol": s.symbol,
            "name": s.name,
            "market": s.market
        } for s in stocks]

    def sync_all_stocks(self):
        """Sync basic info for all stocks from providers"""
        markets = ["CN", "US", "HK"]
        count = 0
        for market in markets:
            provider = DataProviderFactory.get_provider_for_market(market)
            stocks = provider.get_stock_list(market)
            for s in stocks:
                # Upsert
                existing = self.db.query(Stock).filter(Stock.symbol == s['symbol']).first()
                if not existing:
                    new_stock = Stock(
                        code=s['symbol'],
                        symbol=s['symbol'],
                        name=s['name'],
                        market=s['market'],
                        type="stock",
                        update_time=datetime.utcnow()
                    )
                    self.db.add(new_stock)
                else:
                    existing.name = s['name']
                    existing.market = s['market']
                    existing.update_time = datetime.utcnow()
                count += 1
            self.db.commit()
        return count

    def batch_get_prices(self, symbols: List[str]) -> List[Dict]:
        results = []
        for symbol in symbols:
            data = self.get_stock_info(symbol)
            if data:
                results.append(data)
        return results
