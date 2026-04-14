from typing import Dict, List
import pandas as pd

def fetch_ohlcv_df(ex, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
    ohlcv = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    df = pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","volume"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df.set_index("ts", inplace=True)
    return df

def fetch_universe(ex, symbols: List[str], timeframe: str, limit: int) -> Dict[str, pd.DataFrame]:
    data = {}
    for s in symbols:
        data[s] = fetch_ohlcv_df(ex, s, timeframe, limit)
    return data
