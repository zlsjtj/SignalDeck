from dataclasses import dataclass
from typing import Dict, Optional
import ccxt

@dataclass
class Broker:
    ex: any
    paper: bool

def make_exchange(name: str, apiKey: str, secret: str, password: str = "", position_mode: str = "oneway"):
    cls = getattr(ccxt, name)
    params = {"enableRateLimit": True, "timeout": 1000}
    if apiKey and secret:
        params.update({"apiKey": apiKey, "secret": secret})
    if password:
        params.update({"password": password})
    ex = cls(params)
    try:
        ex.timeout = 1000
    except Exception:
        pass
    # Avoid synchronous pre-loading of all exchange markets.
    # In restricted environments, loading markets can be slow (exchangeInfo timeout),
    # and it's safe to defer until first real trading call.
    try:
        ex.load_markets()
    except Exception:
        pass

    # Best-effort position mode setup (futures only). Ignore failures for paper runs.
    if position_mode:
        try:
            if name in ("binanceusdm", "binance"):
                # True = hedge mode, False = one-way
                ex.set_position_mode(position_mode == "hedge")
            elif name == "bybit":
                ex.set_position_mode(position_mode == "hedge")
            elif name == "okx":
                # OKX uses set_position_mode in ccxt as well
                ex.set_position_mode(position_mode == "hedge")
        except Exception:
            pass
    return ex
