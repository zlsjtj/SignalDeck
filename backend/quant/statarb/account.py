from typing import Dict, List, Optional


def _safe_float(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def get_equity_usdt(ex) -> float:
    """
    Return total account equity in USDT (or stablecoin quote for linear futures).
    """
    bal = ex.fetch_balance()
    # Prefer unified balance fields
    for key in ("USDT", "USD"):
        if key in bal.get("total", {}):
            return _safe_float(bal["total"][key])
    if "total" in bal and isinstance(bal["total"], dict) and bal["total"]:
        # Fallback: take largest total as equity proxy
        return max(_safe_float(v) for v in bal["total"].values())
    # Exchange-specific fallbacks
    info = bal.get("info", {}) or {}
    for k in ("totalEquity", "equity", "accountEquity"):
        if k in info:
            return _safe_float(info[k])
    return 0.0


def get_current_weights(
    ex,
    symbols: List[str],
    equity_usdt: float,
) -> Dict[str, float]:
    """
    Return current position weights per symbol (notional / equity).
    Positive = long, Negative = short.
    """
    if equity_usdt <= 0:
        return {s: 0.0 for s in symbols}

    weights: Dict[str, float] = {s: 0.0 for s in symbols}

    try:
        positions = ex.fetch_positions(symbols)
    except Exception:
        positions = []

    # Cache tickers only if needed
    ticker_cache: Dict[str, float] = {}

    for p in positions:
        sym = p.get("symbol")
        if sym not in weights:
            continue

        # Determine direction
        side = p.get("side")
        if side in ("long", "LONG"):
            sign = 1.0
        elif side in ("short", "SHORT"):
            sign = -1.0
        else:
            # Some exchanges encode signed contracts
            contracts = _safe_float(p.get("contracts") or p.get("positionAmt") or 0.0)
            sign = 1.0 if contracts >= 0 else -1.0

        # Notional estimation
        notional = _safe_float(p.get("notional") or p.get("notionalValue") or 0.0)
        if notional == 0.0:
            contracts = abs(_safe_float(p.get("contracts") or p.get("positionAmt") or 0.0))
            contract_size = _safe_float(p.get("contractSize") or 1.0, default=1.0)
            price = _safe_float(p.get("markPrice") or p.get("lastPrice") or 0.0)
            if price == 0.0:
                if sym not in ticker_cache:
                    try:
                        ticker_cache[sym] = _safe_float(ex.fetch_ticker(sym).get("last"))
                    except Exception:
                        ticker_cache[sym] = 0.0
                price = ticker_cache[sym]
            notional = contracts * contract_size * price

        weights[sym] = sign * (notional / equity_usdt)

    return weights
