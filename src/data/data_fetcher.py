import os
import json
from datetime import datetime, timedelta, date
from typing import Optional, Any

import pandas as pd
import yfinance as yf

CONFIG_PATH = os.path.join('config', 'config.json')
CACHE_DIR = 'cache'
os.makedirs(CACHE_DIR, exist_ok=True)

# Cleaning the DataFrame fetched 

def clean_dataframe(df:pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    
    df = df.copy()

    # Handle MultiIndex columns from yfinance (e.g., ('Close', '^NSEI'))
    if isinstance(df.columns, pd.MultiIndex):
        # Flatten: keep only the column name (first level), drop ticker (second level)
        df.columns = df.columns.get_level_values(0)

    if 'Price' in df.columns:
        df.rename(columns={'Price':'DateTime'}, inplace=True)
    
    if 'Datetime' in df.columns:
        df['Datetime'] = pd.to_datetime(df['Datetime'], errors='coerce', utc=True)
        df.set_index('Datetime', inplace=True)
    
    elif not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, errors='coerce', utc=True)
    
    # Rename columns (case-insensitive)
    rename_map = {
        'Adj Close':'Adj_Close',
        'Open':'Open',
        'High':'High',
        'Low':'Low',
        'Close':'Close'
    }
    df.rename(columns=rename_map, inplace=True)

    # Keep only valid columns that actually exist in the DataFrame
    valid_cols = ['Open', 'High', 'Low', 'Close', 'Adj_Close', 'Volume']
    cols_to_keep = [c for c in df.columns if c in valid_cols]
    
    if cols_to_keep:
        df = df[cols_to_keep]
    else:
        # If no valid columns, return empty DataFrame
        return pd.DataFrame()
    
    # Convert all numeric columns to float
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
        if col != 'Volume':
            df[col] = df[col].round(2)
    
    # Drop rows with NaN Close prices
    df = df.dropna(subset=['Close'])
    
    # Convert UTC timezone to Asia/Kolkata
    if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is not None:
        df.index = df.index.tz_convert('Asia/Kolkata')
    
    return df

# Normalizing Dates

def _normalize_date_str(dt: Any) -> str:
    try:
        if isinstance(dt, datetime):
            return dt.strftime('%Y-%m-%d')
        if isinstance(dt, date):
            return dt.strftime('%Y-%m-%d')
    except Exception:
        pass
    # fallback to str()
    return str(dt)

# Assigning Cache file path

def cache_path_for(symbol: str, start: Any, end: Any, timeframe: str) -> str:
    
    safe_symbol = symbol.replace(" ", "_").upper()
    start_s = _normalize_date_str(start)
    end_s = _normalize_date_str(end)
    return os.path.join(CACHE_DIR, f"{safe_symbol}_{start_s}_{end_s}_{timeframe}.csv")

def load_config() -> dict:
    if not os.path.exists(CONFIG_PATH):
        raise FileNotFoundError(f"Configuration file not found at {CONFIG_PATH}")
    with open(CONFIG_PATH, "r") as file:
        return json.load(file)

# Loading Configuration

config = load_config()
MODE = config.get("mode", "backtest").lower()
SYMBOL = config.get('symbol', "NIFTY 50")
TIMEFRAME = config.get('timeframe', '5m')
USE_CACHE = bool(config.get('use_cache', True))

def is_empty_df(df: Optional[pd.DataFrame]) -> bool:
    return df is None or getattr(df, "empty", True)

# Appending Data into cached file

def append_to_cache(cache_file: str, new_data:pd.DataFrame) -> pd.DataFrame:
    if new_data is None:
        return pd.DataFrame()
    try:
        if os.path.exists(cache_file):
            old_df = pd.read_csv(cache_file, index_col = 0, parse_dates = True)
            combined = pd.concat([old_df, new_data])
            # Drop duplicates by index, keeping the last (newest) occurrence
            combined = combined[~combined.index.duplicated(keep='last')]
            combined.sort_index(inplace=True)
            combined.to_csv(cache_file)

            print(f"[CACHE] Appended {len(new_data)} new rows to {cache_file}")
            return combined
    
        else:
            new_data.to_csv(cache_file)
            print(f'[CACHE] Wrote new cache file {cache_file}')
            return new_data
    except Exception as e2:
        print(f"[ERROR] Failed to write cache file: {e2}")
        return new_data if new_data is not None else pd.DataFrame()

# Data Fetching Functions

def get_history(symbol: str,
                start: Any,
                end: Any,
                index: bool = False,
                option_type: Optional[str] = None,
                strike_price: Optional[float] = None,
                expiry_date: Optional[str] = None,
                futures: bool = False) -> pd.DataFrame:
    
    try:
        cache_file = cache_path_for(symbol, start, end, TIMEFRAME)

        if USE_CACHE and os.path.exists(cache_file):
            print(f"[CACHE] Found existing file: {cache_file}")
            cached = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            # Apply timezone conversion to cached data
            if isinstance(cached.index, pd.DatetimeIndex) and cached.index.tz is not None:
                cached.index = cached.index.tz_convert('Asia/Kolkata')
            
            last_cached_date = cached.index[-1].date()

            if last_cached_date >= end:
                print(f'[CACHE] Data already up to date.')
                return cached
            
            new_start = last_cached_date + timedelta(days=1)
            print(f'[CACHE] Fetching new data from {new_start}, to {end}...')
            
            ticker = '^NSEI' if index or symbol.upper() in ['NIFTY 50', 'NIFTY'] else f"{symbol}.NS"

            df_new = yf.download(ticker,
                                 start=new_start,
                                 end=end,
                                 interval=TIMEFRAME,
                                 progress=False,
                                 auto_adjust=False)
            
            if df_new is None or getattr(df_new, "empty", True):
                print("[CACHE] No new data fetched, returning cached data.")
                return cached
            
            df_new = clean_dataframe(df_new)
            return append_to_cache(cache_file, df_new)

        if index or symbol.upper() in ['NIFTY 50', 'NIFTY']:
            ticker = '^NSEI'
        else:
            ticker = f"{symbol}.NS"
        
        df = yf.download(ticker,
                         start=start,
                         end=end,
                         interval=TIMEFRAME,
                         progress=False,
                         auto_adjust=False)
        if not isinstance(df, pd.DataFrame) or df.empty:

            df = yf.download(ticker,
                             start=start,
                             end=end,
                             interval='1d',
                             progress=False,
                             auto_adjust=False)

        if not isinstance(df, pd.DataFrame) or df.empty:
            raise ValueError(f"No data returned from yfinance for {symbol}")
        
        # Clean the DataFrame (rename columns, convert types, handle timezone)
        df = clean_dataframe(df)
        
        if USE_CACHE:
            print(f"[DEBUG] Cache path: {cache_file}")
            print(f"[DEBUG] DataFrame shape: {df.shape}")

            append_to_cache(cache_file, df)
        return df
    
    except Exception as e:
        print('[ERROR] get_history() failed, returning empty DataFrame:', e)
        return pd.DataFrame()
    
def fetch_nifty_data(start: Optional[date] = None,
                     end: Optional[date] = None,
                     force_refresh: bool = False) -> pd.DataFrame:
    
    if end is None:
        end = date.today()
    if start is None:
        start = end - timedelta(days=59)

    fname = cache_path_for(SYMBOL, start, end, TIMEFRAME)

    if os.path.exists(fname) and USE_CACHE and not force_refresh:
        print(f"Loaded cached NIFTY data from {fname}")
        cached_df = pd.read_csv(fname, index_col=0, parse_dates=True)
        # Apply timezone conversion to cached data as well
        if isinstance(cached_df.index, pd.DatetimeIndex) and cached_df.index.tz is not None:
            cached_df.index = cached_df.index.tz_convert('Asia/Kolkata')
        return cached_df
    
    print(f'Fetching NIFTY data from {start} to {end}...')

    df = get_history(SYMBOL, start, end, index=True)

    if df is None or getattr(df, "empty", True):
        raise ValueError("Failed to fetch NIFTY data.")
    
    append_to_cache(fname, df)
    print(f"Saved NIFTY data to cache at {fname}")
    return df

# Fetching Live NIFTY Data via Nsetools

from nsetools import Nse
nse = Nse()

def fetch_live_nifty() -> Optional[float]:
    try:
        quote = nse.get_index_quote(index="NIFTY 50")
        if not isinstance(quote, dict):
            raise ValueError('Unexpected response from Nsetools.get_index_quote()')
        last_price = quote.get("last")
        if last_price is None:
            raise KeyError('"last" missing in Nsetools response.')
        return float(str(last_price).replace(' ', ''))
    except Exception as e:
        print("Live price fetch failed:", e)
        return None

def get_data(start: Optional[date] = None,
             end: Optional[date] = None):
    
    if MODE == 'live':
        print("[MODE] Fetching Live Data")
        return fetch_live_nifty()
    elif MODE == 'backtest':
        print("[MODE] Fetching Historical Data")
        return fetch_nifty_data(start=start, end=end)
    else:
        raise ValueError(f"Invalid MODE:{MODE} in config, must be 'live' or 'backtest'" )

if __name__ == "__main__":
    nifty_data = get_data()
    if isinstance(nifty_data, pd.DataFrame):
        print(nifty_data.head())
        print(nifty_data.tail())
    else:
        print('Live NIFTY Price:', nifty_data)