import json
import math
import time
from datetime import datetime, timezone
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from urllib.error import URLError
from typing import Any, Dict, List, Optional, Tuple


SPOT_BASE_URL = "https://api.binance.com"
FUTURES_BASE_URL = "https://fapi.binance.com"
FUTURES_DATA_BASE_URL = "https://fapi.binance.com"

DEFAULT_TIMEOUT_SECONDS = 4.0


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        out = float(value)
        if math.isfinite(out):
            return out
    except Exception:
        pass
    return default


def _binance_symbol(symbol: str) -> str:
    text = str(symbol or "").strip()
    if not text:
        return ""
    base = text.split(":", 1)[0]
    return base.replace("/", "").replace("-", "").replace("_", "").upper()


def _ccxt_symbol(binance_symbol: str, venue: str) -> str:
    text = str(binance_symbol or "").upper().strip()
    if text.endswith("USDT") and len(text) > 4:
        base = text[:-4]
        return f"{base}/USDT:USDT" if venue == "futures" else f"{base}/USDT"
    return text


def _http_get_json(base_url: str, path: str, params: Dict[str, Any], timeout: float = DEFAULT_TIMEOUT_SECONDS) -> Any:
    query = urllib_parse.urlencode({k: v for k, v in params.items() if v is not None})
    url = f"{base_url}{path}"
    if query:
        url = f"{url}?{query}"
    req = urllib_request.Request(url, headers={"User-Agent": "SignalDeck/market-intel"})
    try:
        with urllib_request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read(2_000_000)
    except URLError as exc:
        raise RuntimeError(str(exc)) from exc
    return json.loads(raw.decode("utf-8"))


def _depth_endpoint(venue: str) -> Tuple[str, str]:
    if venue == "spot":
        return SPOT_BASE_URL, "/api/v3/depth"
    return FUTURES_BASE_URL, "/fapi/v1/depth"


def _agg_trades_endpoint(venue: str) -> Tuple[str, str]:
    if venue == "spot":
        return SPOT_BASE_URL, "/api/v3/aggTrades"
    return FUTURES_BASE_URL, "/fapi/v1/aggTrades"


def _klines_endpoint(venue: str) -> Tuple[str, str]:
    if venue == "spot":
        return SPOT_BASE_URL, "/api/v3/klines"
    return FUTURES_BASE_URL, "/fapi/v1/klines"


def _fetch_depth(venue: str, symbol: str, limit: int) -> Dict[str, Any]:
    base, path = _depth_endpoint(venue)
    data = _http_get_json(base, path, {"symbol": symbol, "limit": limit})
    bids = _levels(data.get("bids", []))
    asks = _levels(data.get("asks", []))
    bid_notional = sum(x["notional"] for x in bids)
    ask_notional = sum(x["notional"] for x in asks)
    best_bid = bids[0]["price"] if bids else 0.0
    best_ask = asks[0]["price"] if asks else 0.0
    mid = (best_bid + best_ask) / 2.0 if best_bid > 0 and best_ask > 0 else 0.0
    spread = best_ask - best_bid if best_bid > 0 and best_ask > 0 else 0.0
    denom = bid_notional + ask_notional
    imbalance = (bid_notional - ask_notional) / denom if denom > 0 else 0.0
    return {
        "bids": bids,
        "asks": asks,
        "bestBid": best_bid,
        "bestAsk": best_ask,
        "mid": mid,
        "spread": spread,
        "spreadPct": spread / mid if mid > 0 else 0.0,
        "bidNotional": bid_notional,
        "askNotional": ask_notional,
        "imbalance": imbalance,
        "lastUpdateId": data.get("lastUpdateId"),
    }


def _levels(rows: Any) -> List[Dict[str, float]]:
    out: List[Dict[str, float]] = []
    if not isinstance(rows, list):
        return out
    for item in rows:
        if not isinstance(item, (list, tuple)) or len(item) < 2:
            continue
        price = _to_float(item[0])
        qty = _to_float(item[1])
        if price <= 0 or qty <= 0:
            continue
        out.append({"price": price, "qty": qty, "notional": price * qty})
    return out


def _fetch_agg_trade_flow(venue: str, symbol: str, limit: int = 500) -> Dict[str, Any]:
    base, path = _agg_trades_endpoint(venue)
    rows = _http_get_json(base, path, {"symbol": symbol, "limit": limit})
    buy_qty = 0.0
    sell_qty = 0.0
    buy_notional = 0.0
    sell_notional = 0.0
    latest_ts = ""
    for row in rows if isinstance(rows, list) else []:
        qty = _to_float(row.get("q"))
        price = _to_float(row.get("p"))
        notional = qty * price
        ts_ms = int(_to_float(row.get("T"), 0.0))
        if ts_ms > 0:
            latest_ts = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()
        # Binance aggTrade `m=true` means the buyer is maker, so the taker sold.
        if bool(row.get("m")):
            sell_qty += qty
            sell_notional += notional
        else:
            buy_qty += qty
            buy_notional += notional
    total_qty = buy_qty + sell_qty
    total_notional = buy_notional + sell_notional
    return {
        "source": "aggTrades",
        "buyQty": buy_qty,
        "sellQty": sell_qty,
        "buyNotional": buy_notional,
        "sellNotional": sell_notional,
        "takerBuyRatio": buy_qty / total_qty if total_qty > 0 else 0.0,
        "takerBuyNotionalRatio": buy_notional / total_notional if total_notional > 0 else 0.0,
        "tradeImbalance": (buy_notional - sell_notional) / total_notional if total_notional > 0 else 0.0,
        "tradeCount": len(rows) if isinstance(rows, list) else 0,
        "latestTs": latest_ts,
    }


def _fetch_klines(venue: str, symbol: str, interval: str, limit: int) -> List[Dict[str, Any]]:
    base, path = _klines_endpoint(venue)
    raw = _http_get_json(base, path, {"symbol": symbol, "interval": interval, "limit": limit})
    rows: List[Dict[str, Any]] = []
    for item in raw if isinstance(raw, list) else []:
        if not isinstance(item, (list, tuple)) or len(item) < 6:
            continue
        ts_ms = int(_to_float(item[0], 0.0))
        open_price = _to_float(item[1])
        high = _to_float(item[2])
        low = _to_float(item[3])
        close = _to_float(item[4])
        volume = _to_float(item[5])
        if ts_ms <= 0 or close <= 0:
            continue
        rows.append(
            {
                "time": int(ts_ms / 1000),
                "ts": datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat(),
                "open": open_price,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "returnPct": (close / open_price - 1.0) if open_price > 0 else 0.0,
            }
        )
    return rows


def _volume_ratio(rows: List[Dict[str, Any]]) -> float:
    if len(rows) < 3:
        return 0.0
    latest = _to_float(rows[-1].get("volume"))
    history = [_to_float(item.get("volume")) for item in rows[:-1]]
    history = [x for x in history if x > 0]
    avg = sum(history) / len(history) if history else 0.0
    return latest / avg if avg > 0 else 0.0


def _session_effect(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[int, Dict[str, float]] = {}
    for row in rows:
        try:
            hour = datetime.fromtimestamp(int(row["time"]), tz=timezone.utc).hour
        except Exception:
            continue
        b = buckets.setdefault(hour, {"count": 0.0, "returnTotal": 0.0, "volumeTotal": 0.0})
        b["count"] += 1.0
        b["returnTotal"] += _to_float(row.get("returnPct"))
        b["volumeTotal"] += _to_float(row.get("volume"))
    out: List[Dict[str, Any]] = []
    for hour, b in sorted(buckets.items()):
        count = max(1.0, b["count"])
        out.append(
            {
                "hourUtc": hour,
                "count": int(b["count"]),
                "avgReturnPct": b["returnTotal"] / count,
                "avgVolume": b["volumeTotal"] / count,
            }
        )
    return out


def _fetch_futures_derivatives(symbol: str, interval: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "fundingRate": None,
        "fundingTime": "",
        "openInterest": None,
        "openInterestChangePct": None,
        "periodTakerBuyRatio": None,
        "errors": [],
    }
    try:
        funding = _http_get_json(FUTURES_BASE_URL, "/fapi/v1/fundingRate", {"symbol": symbol, "limit": 1})
        if isinstance(funding, list) and funding:
            row = funding[-1]
            out["fundingRate"] = _to_float(row.get("fundingRate"))
            ts_ms = int(_to_float(row.get("fundingTime"), 0.0))
            out["fundingTime"] = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat() if ts_ms > 0 else ""
    except Exception as exc:
        out["errors"].append(f"funding: {exc}")

    period = _binance_period(interval)
    try:
        oi = _http_get_json(
            FUTURES_DATA_BASE_URL,
            "/futures/data/openInterestHist",
            {"symbol": symbol, "period": period, "limit": 2},
        )
        if isinstance(oi, list) and oi:
            latest = _to_float(oi[-1].get("sumOpenInterest"))
            prev = _to_float(oi[-2].get("sumOpenInterest")) if len(oi) > 1 else 0.0
            out["openInterest"] = latest
            out["openInterestChangePct"] = latest / prev - 1.0 if prev > 0 else None
    except Exception as exc:
        out["errors"].append(f"openInterest: {exc}")

    try:
        taker = _http_get_json(
            FUTURES_DATA_BASE_URL,
            "/futures/data/takerlongshortRatio",
            {"symbol": symbol, "period": period, "limit": 1},
        )
        if isinstance(taker, list) and taker:
            latest = taker[-1]
            buy = _to_float(latest.get("buyVol"))
            sell = _to_float(latest.get("sellVol"))
            out["periodTakerBuyRatio"] = buy / (buy + sell) if buy + sell > 0 else None
    except Exception as exc:
        out["errors"].append(f"takerPeriod: {exc}")

    return out


def _binance_period(interval: str) -> str:
    allowed = {"5m", "15m", "30m", "1h", "2h", "4h", "6h", "12h", "1d"}
    return interval if interval in allowed else "15m"


def _returns_by_symbol(klines_by_symbol: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[float]]:
    out: Dict[str, List[float]] = {}
    for symbol, rows in klines_by_symbol.items():
        rets = [_to_float(row.get("returnPct")) for row in rows][-96:]
        if len(rets) >= 4:
            out[symbol] = rets
    return out


def _corr(a: List[float], b: List[float]) -> Optional[float]:
    n = min(len(a), len(b))
    if n < 4:
        return None
    aa = a[-n:]
    bb = b[-n:]
    ma = sum(aa) / n
    mb = sum(bb) / n
    va = sum((x - ma) ** 2 for x in aa)
    vb = sum((x - mb) ** 2 for x in bb)
    if va <= 0 or vb <= 0:
        return None
    cov = sum((aa[i] - ma) * (bb[i] - mb) for i in range(n))
    return cov / math.sqrt(va * vb)


def _correlation_matrix(returns: Dict[str, List[float]]) -> List[Dict[str, Any]]:
    symbols = sorted(returns.keys())
    rows: List[Dict[str, Any]] = []
    for left in symbols:
        values: Dict[str, Optional[float]] = {}
        for right in symbols:
            values[right] = 1.0 if left == right else _corr(returns[left], returns[right])
        rows.append({"symbol": left, "values": values})
    return rows


def build_market_intel_summary(
    *,
    symbols: List[str],
    selected_symbol: str,
    interval: str = "15m",
    lookback_bars: int = 96,
    depth_limit: int = 20,
) -> Dict[str, Any]:
    binance_symbols = [_binance_symbol(symbol) for symbol in symbols]
    binance_symbols = list(dict.fromkeys(symbol for symbol in binance_symbols if symbol))
    if not binance_symbols:
        binance_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"]

    selected = _binance_symbol(selected_symbol) or binance_symbols[0]
    if selected not in binance_symbols:
        binance_symbols.insert(0, selected)

    lookback_bars = max(24, min(int(lookback_bars), 240))
    depth_limit = max(5, min(int(depth_limit), 100))
    interval = _binance_period(interval)

    venues: Dict[str, Any] = {}
    klines_for_corr: Dict[str, List[Dict[str, Any]]] = {}

    for venue in ("spot", "futures"):
        venue_payload: Dict[str, Any] = {
            "venue": venue,
            "symbol": _ccxt_symbol(selected, venue),
            "binanceSymbol": selected,
            "ok": True,
            "error": "",
            "orderbook": None,
            "flow": None,
            "volumeRatio": 0.0,
            "sessionEffect": [],
            "derivatives": None,
        }
        try:
            venue_payload["orderbook"] = _fetch_depth(venue, selected, depth_limit)
            venue_payload["flow"] = _fetch_agg_trade_flow(venue, selected)
            rows = _fetch_klines(venue, selected, interval, lookback_bars)
            venue_payload["volumeRatio"] = _volume_ratio(rows)
            venue_payload["sessionEffect"] = _session_effect(rows)
            if venue == "futures":
                venue_payload["derivatives"] = _fetch_futures_derivatives(selected, interval)
        except Exception as exc:
            venue_payload["ok"] = False
            venue_payload["error"] = str(exc)
        venues[venue] = venue_payload

    for symbol in binance_symbols[:8]:
        try:
            klines_for_corr[symbol] = _fetch_klines("futures", symbol, interval, lookback_bars)
        except Exception:
            continue

    return {
        "ts": _now_iso(),
        "source": "binance-public",
        "symbols": [_ccxt_symbol(symbol, "futures") for symbol in binance_symbols],
        "selectedSymbol": _ccxt_symbol(selected, "futures"),
        "selectedBinanceSymbol": selected,
        "interval": interval,
        "lookbackBars": lookback_bars,
        "venues": venues,
        "correlation": {
            "venue": "futures",
            "symbols": [_ccxt_symbol(symbol, "futures") for symbol in sorted(_returns_by_symbol(klines_for_corr).keys())],
            "matrix": _correlation_matrix(_returns_by_symbol(klines_for_corr)),
        },
        "liquidations": {
            "status": "stream_not_configured",
            "message": "Binance liquidation data is a public WebSocket stream; this snapshot endpoint does not run a collector yet.",
            "rows": [],
        },
        "news": {
            "status": "source_not_configured",
            "message": "News sentiment needs a news source or local NLP feed before it is scored.",
            "rows": [],
        },
    }


_CACHE: Dict[str, Any] = {"key": "", "ts": 0.0, "payload": None}


def cached_market_intel_summary(**kwargs: Any) -> Dict[str, Any]:
    ttl_seconds = max(10, min(int(kwargs.pop("ttl_seconds", 60)), 300))
    key = json.dumps(kwargs, sort_keys=True, default=str)
    now = time.time()
    if _CACHE.get("key") == key and _CACHE.get("payload") and now - float(_CACHE.get("ts") or 0.0) < ttl_seconds:
        payload = dict(_CACHE["payload"])
        payload["cache"] = {"hit": True, "ttlSeconds": ttl_seconds}
        return payload
    payload = build_market_intel_summary(**kwargs)
    payload["cache"] = {"hit": False, "ttlSeconds": ttl_seconds}
    _CACHE.update({"key": key, "ts": now, "payload": payload})
    return payload
