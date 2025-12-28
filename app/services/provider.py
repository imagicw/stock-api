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

    @abstractmethod
    def batch_get_stock_info(self, symbols: List[str]) -> List[Dict]:
        """Batch get stock info"""
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

    def batch_get_stock_info(self, symbols: List[str]) -> List[Dict]:
        # For AkShare, just loop for now or optimize later if needed
        results = []
        for symbol in symbols:
            info = self.get_stock_info(symbol)
            if info:
                results.append(info)
        return results

class YFinanceProvider(DataProvider):
    def get_stock_info(self, symbol: str) -> Dict:
        # Normalize symbol for YFinance
        yf_symbol = self._normalize_symbol(symbol)
        
        try:
            ticker = yf.Ticker(yf_symbol)
            
            # Try fast_info first (faster and often more up-to-date for current stats)
            try:
                info = ticker.fast_info
                current_price = info.last_price
                open_price = info.open
                high_price = info.day_high
                low_price = info.day_low
                # fast_info doesn't always have volume, or it's 'last_volume'
                # But fast_info object usually has 'last_volume' or we can get it from history if needed.
                # Actually fast_info has 'last_volume' but sometimes it's None.
                # Let's check documentation/dir. 
                # For safety, let's fallback to history for volume if needed or just use what we have.
                # fast_info keys: 'currency', 'day_high', 'day_low', 'exchange', 'fifty_day_average', 'last_price', 'last_volume', 'market_cap', 'open', 'previous_close', 'quote_type', 'regular_market_previous_close', 'shares', 'ten_day_average_volume', 'three_month_average_volume', 'timezone', 'two_hundred_day_average', 'year_change', 'year_high', 'year_low'
                volume = info.last_volume if hasattr(info, 'last_volume') else 0
                
                # If values are None (e.g. pre-market), might need fallback
                if current_price is None:
                    raise ValueError("fast_info missing price")

            except Exception:
                # Fallback to history
                history = ticker.history(period="1d")
                if history.empty:
                    return None
                current_price = history['Close'].iloc[-1]
                open_price = history['Open'].iloc[-1]
                high_price = history['High'].iloc[-1]
                low_price = history['Low'].iloc[-1]
                volume = history['Volume'].iloc[-1]

            name = symbol # Default name
            # Try to get better name from ticker.info if cached or cheap, but it's slow.
            # We can use the one from search/sync if available in DB, but here we are in provider.
            # Let's stick to symbol or try to fetch name if we really need it, but user didn't complain about name.
            
            market = "US"
            if yf_symbol.endswith(".SS") or yf_symbol.endswith(".SZ"):
                market = "CN"
            elif yf_symbol.endswith(".HK"):
                market = "HK"

            return {
                "symbol": symbol, # Return the requested symbol (e.g. sh600000) or normalized? 
                                  # Service expects the requested symbol to match DB. 
                                  # But here we return what we found. 
                                  # The service uses this dict. 
                                  # Let's return the input symbol to be safe.
                "name": name, 
                "price": round(float(current_price), 3),
                "open": round(float(open_price), 3),
                "high": round(float(high_price), 3),
                "low": round(float(low_price), 3),
                "volume": float(volume), # Volume doesn't need rounding usually, but float is fine
                "market": market
            }
        except Exception as e:
            print(f"YFinance error for {symbol}: {e}")
            return None

    def get_price_history(self, symbol: str, start_date: str, end_date: str) -> List[Dict]:
        yf_symbol = self._normalize_symbol(symbol)
        try:
            ticker = yf.Ticker(yf_symbol)
            df = ticker.history(start=start_date, end=end_date)
            # Convert to list of dicts
            result = []
            for date, row in df.iterrows():
                result.append({
                    "date": date.strftime("%Y-%m-%d"),
                    "open": round(float(row['Open']), 3),
                    "close": round(float(row['Close']), 3),
                    "high": round(float(row['High']), 3),
                    "low": round(float(row['Low']), 3),
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
        return []

    def batch_get_stock_info(self, symbols: List[str]) -> List[Dict]:
        if not symbols:
            return []
            
        # Normalize symbols
        yf_symbols = [self._normalize_symbol(s) for s in symbols]
        
        # Mapping back to original symbols
        # Note: yf_symbols might have duplicates if input has duplicates
        # But we can map by index or use a dictionary map
        # yf.download might return columns with Tickers matching what was passed
        
        try:
            # yf.download
            # group_by='ticker' makes the top level column the Ticker
            df = yf.download(tickers=yf_symbols, period="1d", group_by='ticker', threads=True, auto_adjust=True)
            
            results = []
            
            # If only one symbol, df columns are NOT MultiIndex with ticker at level 0
            # It changes structure.
            if len(yf_symbols) == 1:
                symbol = symbols[0]
                yf_sym = yf_symbols[0]
                if df.empty:
                    return results
                
                # df is just single level columns or MultiIndex if more data
                # With group_by='ticker' and 1 ticker, it might still look like single ticker
                # Let's check typical yf behavior.
                # If 1 ticker, columns are just Open, High etc.
                # We can handle 1 ticker case separately or wrapped
                
                try:
                    current_price = df['Close'].iloc[-1]
                    open_price = df['Open'].iloc[-1]
                    high_price = df['High'].iloc[-1]
                    low_price = df['Low'].iloc[-1]
                    volume = df['Volume'].iloc[-1]
                    
                    results.append({
                        "symbol": symbol,
                        "name": symbol, # Detailed name not available in download
                        "price": round(float(current_price), 3),
                        "open": round(float(open_price), 3),
                        "high": round(float(high_price), 3),
                        "low": round(float(low_price), 3),
                        "volume": float(volume),
                        "market": "US" # Default or infer
                    })
                except Exception:
                    pass
                return results

            # Multiple symbols
            # df.columns is MultiIndex: (Ticker, PriceType) if group_by='ticker'
            # ACTUALLY, group_by='ticker' makes Ticker the TOP level. 
            # Columns: (Ticker, Open), (Ticker, Close)... 
            # WAIT. Let's check documentation or assumption.
            # yf.download(..., group_by='ticker') -> Columns: MultiIndex (Ticker, 'Open'), (Ticker, 'Close')...
            
            # Iterate over our requested symbols (preserving order)
            for i, sym in enumerate(symbols):
                yf_sym = yf_symbols[i]
                
                try:
                    # Check if yf_sym is in df columns (top level)
                    if yf_sym not in df.columns.levels[0]:
                        # Maybe invalid or no data
                        # Also yf sometimes uppercases things
                        continue
                        
                    data = df[yf_sym]
                    if data.empty or data['Close'].isna().all():
                        continue
                        
                    # Get last row
                    last_row = data.iloc[-1]
                    
                    if True in last_row.isna().values:
                        # If today's data is partial?
                        # Just take what we have, but ensure price exists
                        pass
                        
                    current_price = last_row['Close']
                    open_price = last_row['Open']
                    high_price = last_row['High']
                    low_price = last_row['Low']
                    volume = last_row['Volume']
                    
                    # infer market
                    market = "US"
                    if yf_sym.endswith(".SS") or yf_sym.endswith(".SZ"):
                        market = "CN"
                    elif yf_sym.endswith(".HK"):
                        market = "HK"
                        
                    results.append({
                        "symbol": sym, # Original symbol
                        "name": sym, # detailed name not in batch
                        "price": round(float(current_price), 3),
                        "open": round(float(open_price), 3),
                        "high": round(float(high_price), 3),
                        "low": round(float(low_price), 3),
                        "volume": float(volume),
                        "market": market
                    })
                except KeyError:
                    pass
                except Exception as e:
                    print(f"Error parsing {sym}: {e}")
                    
            return results
            
        except Exception as e:
            print(f"YFinance batch error: {e}")
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
