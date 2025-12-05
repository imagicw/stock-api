from abc import ABC, abstractmethod
from typing import List, Dict, Optional
import akshare as ak
import yfinance as yf
from datetime import datetime

class DataProvider(ABC):
    @abstractmethod
    def get_stock_info(self, symbol: str) -> Dict:
        """Get basic stock info and current price"""
        pass

    @abstractmethod
    def get_price_history(self, symbol: str, start_date: str, end_date: str) -> List[Dict]:
        """Get historical price data"""
        pass

    @abstractmethod
    def search_stock(self, name: str) -> List[Dict]:
        """Fuzzy search stock by name"""
        pass

    @abstractmethod
    def get_stock_list(self, market: str) -> List[Dict]:
        """Get list of all stocks for a market"""
        pass

class AkShareProvider(DataProvider):
    def get_stock_info(self, symbol: str) -> Dict:
        # Symbol format: sh600000
        code = symbol[2:]
        try:
            # Using stock_zh_a_spot_em for real-time data
            df = ak.stock_zh_a_spot_em()
            # Filter by code
            row = df[df['代码'] == code]
            if row.empty:
                return None
            
            data = row.iloc[0]
            return {
                "symbol": symbol,
                "name": data['名称'],
                "price": float(data['最新价']),
                "open": float(data['今开']),
                "high": float(data['最高']),
                "low": float(data['最低']),
                "volume": float(data['成交量']),
                "market": "CN"
            }
        except Exception as e:
            print(f"AkShare error: {e}")
            return None

    def get_price_history(self, symbol: str, start_date: str, end_date: str) -> List[Dict]:
        code = symbol[2:]
        try:
            # stock_zh_a_hist
            df = ak.stock_zh_a_hist(symbol=code, start_date=start_date.replace("-", ""), end_date=end_date.replace("-", ""), adjust="qfq")
            if df.empty:
                return []
            
            result = []
            for _, row in df.iterrows():
                result.append({
                    "date": row['日期'],
                    "open": float(row['开盘']),
                    "close": float(row['收盘']),
                    "high": float(row['最高']),
                    "low": float(row['最低']),
                    "volume": float(row['成交量'])
                })
            return result
        except Exception as e:
            print(f"AkShare history error: {e}")
            return []

    def search_stock(self, name: str) -> List[Dict]:
        # This is heavy, in production we should cache the list
        try:
            df = ak.stock_zh_a_spot_em()
            # Filter by name contains
            mask = df['名称'].str.contains(name)
            filtered = df[mask].head(10)
            
            result = []
            for _, row in filtered.iterrows():
                # Determine prefix based on logic or just return raw code
                # For simplicity, assuming standard A-share rules or just returning code
                code = row['代码']
                # Simple heuristic for prefix
                prefix = "sh" if code.startswith("6") else "sz"
                result.append({
                    "symbol": f"{prefix}{code}",
                    "name": row['名称'],
                    "market": "CN"
                })
            return result
        except Exception as e:
            print(f"AkShare search error: {e}")
            return []

    def get_stock_list(self, market: str) -> List[Dict]:
        if market != "CN":
            return []
        try:
            df = ak.stock_info_a_code_name()
            result = []
            for _, row in df.iterrows():
                code = row['code']
                name = row['name']
                # Infer symbol for YFinance compatibility (which we use for price)
                # Or just store raw code and let provider handle normalization?
                # StockService expects 'symbol' to be the unique ID.
                # If we store "600000", YFinanceProvider._normalize_symbol handles it.
                # So we can just store the code.
                result.append({
                    "symbol": code,
                    "name": name,
                    "market": "CN"
                })
            return result
        except Exception as e:
            print(f"AkShare get_stock_list error: {e}")
            return []

class YFinanceProvider(DataProvider):
    def get_stock_info(self, symbol: str) -> Dict:
        # Normalize symbol for YFinance
        yf_symbol = self._normalize_symbol(symbol)
        
        try:
            ticker = yf.Ticker(yf_symbol)
            # Use history to get price
            history = ticker.history(period="1d")
            if history.empty:
                return None
            
            current_price = history['Close'].iloc[-1]
            
            # Try to get name from ticker, or fallback
            name = symbol
            market = "US"
            if yf_symbol.endswith(".HK"):
                market = "HK"
            elif yf_symbol.endswith(".SS") or yf_symbol.endswith(".SZ"):
                market = "CN"
            
            return {
                "symbol": symbol, # Return original requested symbol
                "name": name, 
                "price": float(current_price),
                "open": float(history['Open'].iloc[-1]),
                "high": float(history['High'].iloc[-1]),
                "low": float(history['Low'].iloc[-1]),
                "volume": float(history['Volume'].iloc[-1]),
                "market": market
            }
        except Exception as e:
            print(f"YFinance error for {yf_symbol}: {e}")
            return None

    def get_price_history(self, symbol: str, start_date: str, end_date: str) -> List[Dict]:
        yf_symbol = self._normalize_symbol(symbol)
        try:
            ticker = yf.Ticker(yf_symbol)
            df = ticker.history(start=start_date, end=end_date)
            
            result = []
            for date, row in df.iterrows():
                result.append({
                    "date": date.strftime("%Y-%m-%d"),
                    "open": float(row['Open']),
                    "close": float(row['Close']),
                    "high": float(row['High']),
                    "low": float(row['Low']),
                    "volume": float(row['Volume'])
                })
            return result
        except Exception as e:
            print(f"YFinance history error for {yf_symbol}: {e}")
            return []

    def search_stock(self, name: str) -> List[Dict]:
        import requests
        url = f"https://query1.finance.yahoo.com/v1/finance/search?q={name}&quotesCount=10&newsCount=0"
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        try:
            resp = requests.get(url, headers=headers, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                quotes = data.get('quotes', [])
                result = []
                for q in quotes:
                    # Filter for Equity or ETF
                    if q.get('quoteType') not in ['EQUITY', 'ETF', 'MUTUALFUND']:
                        continue
                        
                    symbol = q.get('symbol')
                    name = q.get('shortname') or q.get('longname') or symbol
                    market = "US"
                    if symbol.endswith(".SS") or symbol.endswith(".SZ"):
                        market = "CN"
                    elif symbol.endswith(".HK"):
                        market = "HK"
                    
                    result.append({
                        "symbol": symbol,
                        "name": name,
                        "market": market
                    })
                return result
            else:
                print(f"Yahoo Search Error: {resp.status_code}")
                return []
        except Exception as e:
            print(f"Yahoo Search Exception: {e}")
            return []

    def _normalize_symbol(self, symbol: str) -> str:
        # Handle China A-shares logic
        # 1. If it's 6 digits, infer suffix
        if symbol.isdigit() and len(symbol) == 6:
            if symbol.startswith("6"):
                return f"{symbol}.SS"
            elif symbol.startswith("0") or symbol.startswith("3"):
                return f"{symbol}.SZ"
            elif symbol.startswith("4") or symbol.startswith("8"):
                return f"{symbol}.BJ"
        
        # 2. Handle sh/sz prefix (legacy support)
        if symbol.startswith("sh") and symbol[2:].isdigit():
            return f"{symbol[2:]}.SS"
        if symbol.startswith("sz") and symbol[2:].isdigit():
            return f"{symbol[2:]}.SZ"
            
        # 3. Default (US/HK/Already formatted)
        return symbol

    def get_stock_list(self, market: str) -> List[Dict]:
        # YFinance does not provide a way to get ALL stocks.
        # This is a placeholder. In a real scenario, we would need:
        # 1. A static file/database of tickers.
        # 2. Another API (like NASDAQ FTP, or EastMoney for CN).
        
        # For demonstration, returning a small list of popular stocks.
        if market == "CN":
            return [
                {"symbol": "600000", "name": "浦发银行", "market": "CN"},
                {"symbol": "601318", "name": "中国平安", "market": "CN"},
                {"symbol": "000001", "name": "平安银行", "market": "CN"},
                {"symbol": "600519", "name": "贵州茅台", "market": "CN"},
            ]
        elif market == "US":
            return [
                {"symbol": "AAPL", "name": "Apple Inc.", "market": "US"},
                {"symbol": "MSFT", "name": "Microsoft Corp.", "market": "US"},
                {"symbol": "GOOGL", "name": "Alphabet Inc.", "market": "US"},
                {"symbol": "NVDA", "name": "NVIDIA Corp.", "market": "US"},
                {"symbol": "TSLA", "name": "Tesla Inc.", "market": "US"},
            ]
        elif market == "HK":
            return [
                {"symbol": "0700.HK", "name": "Tencent", "market": "HK"},
                {"symbol": "9988.HK", "name": "Alibaba", "market": "HK"},
                {"symbol": "3690.HK", "name": "Meituan", "market": "HK"},
            ]
        return []

class DataProviderFactory:
    @staticmethod
    def get_provider(symbol: str) -> DataProvider:
        # Always use YFinance now for price/info
        return YFinanceProvider()

    @staticmethod
    def get_provider_for_market(market: str) -> DataProvider:
        if market == "CN":
            return AkShareProvider()
        else:
            return YFinanceProvider()
