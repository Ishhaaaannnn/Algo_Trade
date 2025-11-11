import os
import json
from datetime import datetime, timedelta, date
from typing import Optional, Any

import pandas as pd
import yfinance as yf

CONFIG_PATH = os.path.join('config', 'config.json')
CACHE_DIR = 'cache'
os.makedirs(CACHE_DIR, exist_ok=True)

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
        safe_symbol = symbol.replace(" ", "_").upper()
        cache_file = os.path.join(CACHE_DIR, f"{safe_symbol}_{start}_{end}_{TIMEFRAME}.csv")

        if USE_CACHE and os.path.exists(cache_file):
            print(f"[CACHE] Loading data from {cache_file}")
            df = pd.read_csv(cache_file, index_col=0, parse_dates=True)
            return df

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
        
        if USE_CACHE:
            print(f"[DEBUG] Cache path: {cache_file}")
            print(f"[DEBUG] DataFrame shape: {df.shape}")

            df.to_csv(cache_file)
            print(f'[CACHE] Saved data to {cache_file}')

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
        start = end - timedelta(days=30)

    cache_name = f"{SYMBOL.replace(' ', '_')}_{start}_{end}_{TIMEFRAME}.csv"
    fname = os.path.join(CACHE_DIR, cache_name)

    if os.path.exists(fname) and USE_CACHE and not force_refresh:
        print(f"Loaded cached NIFTY data from {fname}")
        return pd.read_csv(fname, index_col=0, parse_dates=True)
    
    print(f'Fetching NIFTY data from {start} to {end}...')

    df = get_history(SYMBOL, start, end, index=True)

    if df.empty:
        raise ValueError("Failed to fetch NIFTY data.")
    df.to_csv(fname)
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