import pandas as pd
import numpy as np

def calculate_ma(df: pd.DataFrame, window: int) -> pd.Series:
    """Calculate Moving Average"""
    return df['close'].rolling(window=window).mean()

def calculate_macd(df: pd.DataFrame, slow: int = 26, fast: int = 12, signal: int = 9) -> pd.DataFrame:
    """Calculate MACD, Signal, Hist"""
    exp1 = df['close'].ewm(span=fast, adjust=False).mean()
    exp2 = df['close'].ewm(span=slow, adjust=False).mean()
    macd = exp1 - exp2
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    hist = macd - signal_line
    return pd.DataFrame({
        'dif': macd,
        'dea': signal_line,
        'macd': hist * 2 
    })

def calculate_boll(df: pd.DataFrame, window: int = 20, num_std: int = 2) -> pd.DataFrame:
    """Calculate Bollinger Bands"""
    mid = df['close'].rolling(window=window).mean()
    std = df['close'].rolling(window=window).std()
    upper = mid + (std * num_std)
    lower = mid - (std * num_std)
    return pd.DataFrame({
        'mid': mid,
        'upper': upper,
        'lower': lower
    })

def calculate_td9(df: pd.DataFrame) -> pd.Series:
    """
    Calculate TD Sequential (TD9) Setup.
    Returns a Series with values 1-9 (or higher for perfection).
    Positive for Buy Setup (Red numbers in text, Price dropping),
    Negative for Sell Setup (Green numbers in text, Price rising).
    
    Rule:
    Buy Setup: Close < Close 4 bars ago.
    Sell Setup: Close > Close 4 bars ago.
    """
    close = df['close']
    td_values = pd.Series(0, index=df.index, dtype=int)
    
    # Needs at least 4 bars shift
    if len(df) < 5:
        return td_values
        
    # We iterate to simulate the sequential nature, 
    # though vectorization is harder for sequential counts reset.
    # Simple loop is effective enough for typical stock history length.
    
    buy_setup_count = 0
    sell_setup_count = 0
    
    for i in range(4, len(df)):
        c = close.iloc[i]
        c_4 = close.iloc[i-4]
        
        # Buy Setup (Price drop)
        if c < c_4:
            buy_setup_count += 1
            sell_setup_count = 0 # Reset sell
        elif c > c_4:
            sell_setup_count += 1
            buy_setup_count = 0 # Reset buy
        else:
            buy_setup_count = 0
            sell_setup_count = 0
            
        if buy_setup_count > 0:
            td_values.iloc[i] = buy_setup_count
        elif sell_setup_count > 0:
            td_values.iloc[i] = -sell_setup_count # Negative for Sell setup
            
    return td_values
