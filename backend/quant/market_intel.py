import asyncio
import json
import math
import threading
import time
from collections import deque
from datetime import datetime, timezone
from urllib import parse as urllib_parse
from urllib import request as urllib_request
from urllib.error import URLError
from typing import Any, Dict, Deque, List, Optional, Tuple


SPOT_BASE_URL = "https://api.binance.com"
FUTURES_BASE_URL = "https://fapi.binance.com"
FUTURES_DATA_BASE_URL = "https://fapi.binance.com"
SPOT_WS_BASE_URL = "wss://stream.binance.com:9443/stream"
FUTURES_WS_BASE_URL = "wss://fstream.binance.com/stream"

DEFAULT_TIMEOUT_SECONDS = 4.0
STREAM_WINDOW_MS = 5 * 60 * 1000
STREAM_WINDOW_SECONDS_OPTIONS = (5 * 60, 15 * 60, 60 * 60)
STREAM_RAW_WINDOW_MAXLEN = 12000
MAX_STREAM_SYMBOLS = 4


_STREAM_LOCK = threading.Lock()
_STREAM_STATE: Dict[str, Any] = {
    "status": "stopped",
    "startedAt": "",
    "updatedAt": "",
    "errors": deque(maxlen=20),
    "connections": {},
    "orderbooks": {"spot": {}, "futures": {}},
    "ofiWindows": {"spot": {}, "futures": {}},
    "tradeWindows": {"spot": {}, "futures": {}},
    "liquidations": deque(maxlen=100),
}


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
    return _orderbook_metrics(data, venue=venue)


def _orderbook_metrics(data: Dict[str, Any], venue: str) -> Dict[str, Any]:
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
        "venue": venue,
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


def _session_heatmap(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    buckets: Dict[Tuple[int, int], Dict[str, float]] = {}
    for row in rows:
        try:
            dt = datetime.fromtimestamp(int(row["time"]), tz=timezone.utc)
        except Exception:
            continue
        key = (dt.weekday(), dt.hour)
        b = buckets.setdefault(key, {"count": 0.0, "returnTotal": 0.0, "volumeTotal": 0.0})
        b["count"] += 1.0
        b["returnTotal"] += _to_float(row.get("returnPct"))
        b["volumeTotal"] += _to_float(row.get("volume"))
    out: List[Dict[str, Any]] = []
    for (weekday, hour), b in sorted(buckets.items()):
        count = max(1.0, b["count"])
        out.append(
            {
                "weekdayUtc": weekday,
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
        "openInterestWindows": [],
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

    for oi_period in ("15m", "30m", "1h", "4h", "1d"):
        try:
            rows = _http_get_json(
                FUTURES_DATA_BASE_URL,
                "/futures/data/openInterestHist",
                {"symbol": symbol, "period": oi_period, "limit": 30},
            )
            points: List[Dict[str, Any]] = []
            for row in rows if isinstance(rows, list) else []:
                ts_ms = int(_to_float(row.get("timestamp"), 0.0))
                open_interest = _to_float(row.get("sumOpenInterest"))
                open_interest_value = _to_float(row.get("sumOpenInterestValue"))
                if ts_ms <= 0 or open_interest <= 0:
                    continue
                prev_open_interest = _to_float(points[-1].get("openInterest")) if points else 0.0
                points.append(
                    {
                        "ts": datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat(),
                        "openInterest": open_interest,
                        "openInterestValue": open_interest_value if open_interest_value > 0 else None,
                        "changePct": open_interest / prev_open_interest - 1.0 if prev_open_interest > 0 else None,
                    }
                )
            latest = points[-1] if points else {}
            previous = points[-2] if len(points) > 1 else {}
            latest_oi = _to_float(latest.get("openInterest"))
            previous_oi = _to_float(previous.get("openInterest"))
            out["openInterestWindows"].append(
                {
                    "period": oi_period,
                    "latest": latest_oi if latest_oi > 0 else None,
                    "changePct": latest_oi / previous_oi - 1.0 if latest_oi > 0 and previous_oi > 0 else None,
                    "points": points,
                }
            )
        except Exception as exc:
            out["errors"].append(f"openInterest[{oi_period}]: {exc}")

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


def _core_correlation_pairs(symbols: List[str]) -> List[Tuple[str, str]]:
    available = set(symbols)
    preferred = [("BTCUSDT", "ETHUSDT"), ("BTCUSDT", "SOLUSDT"), ("BTCUSDT", "BNBUSDT")]
    pairs = [pair for pair in preferred if pair[0] in available and pair[1] in available]
    if pairs:
        return pairs
    ordered = sorted(available)
    return [(ordered[0], symbol) for symbol in ordered[1:4]] if len(ordered) >= 2 else []


def _rolling_correlation_series(
    klines_by_symbol: Dict[str, List[Dict[str, Any]]],
    *,
    window: int = 24,
) -> List[Dict[str, Any]]:
    symbols = sorted(symbol for symbol, rows in klines_by_symbol.items() if len(rows) >= max(8, window))
    out: List[Dict[str, Any]] = []
    for left, right in _core_correlation_pairs(symbols):
        left_rows = klines_by_symbol.get(left, [])
        right_rows = klines_by_symbol.get(right, [])
        n = min(len(left_rows), len(right_rows))
        if n < window:
            continue
        points: List[Dict[str, Any]] = []
        for end in range(window, n + 1):
            left_slice = left_rows[end - window:end]
            right_slice = right_rows[end - window:end]
            corr = _corr(
                [_to_float(row.get("returnPct")) for row in left_slice],
                [_to_float(row.get("returnPct")) for row in right_slice],
            )
            if corr is None:
                continue
            points.append(
                {
                    "ts": str(left_slice[-1].get("ts") or right_slice[-1].get("ts") or ""),
                    "correlation": corr,
                    "samples": window,
                    "window": window,
                }
            )
        if points:
            out.append(
                {
                    "pair": f"{_ccxt_symbol(left, 'futures')}|{_ccxt_symbol(right, 'futures')}",
                    "left": _ccxt_symbol(left, "futures"),
                    "right": _ccxt_symbol(right, "futures"),
                    "points": points[-48:],
                    "window": window,
                }
            )
    return out


def _correlation_breaks(rolling: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    breaks: List[Dict[str, Any]] = []
    for item in rolling:
        points = item.get("points") if isinstance(item.get("points"), list) else []
        values = [_to_float(point.get("correlation"), float("nan")) for point in points if isinstance(point, dict)]
        values = [value for value in values if math.isfinite(value)]
        if len(values) < 6:
            continue
        current = values[-1]
        previous = values[:-1]
        recent_mean = sum(previous[-12:]) / min(len(previous), 12)
        prior_high = max(previous[-12:]) if previous else current
        severity = ""
        reason = ""
        if prior_high >= 0.75 and current <= 0.45:
            severity = "warning"
            reason = "high_corr_break"
        elif recent_mean - current >= 0.30:
            severity = "notice"
            reason = "mean_drop"
        if not severity:
            continue
        breaks.append(
            {
                "pair": item.get("pair", ""),
                "left": item.get("left", ""),
                "right": item.get("right", ""),
                "current": current,
                "recentMean": recent_mean,
                "priorHigh": prior_high,
                "severity": severity,
                "reason": reason,
                "message": "Rolling correlation has weakened versus its recent baseline; treat this as a structure-change monitor, not a trading signal.",
            }
        )
    return breaks[:8]


def _basis_metrics(venues: Dict[str, Any]) -> Dict[str, Any]:
    spot_ob = venues.get("spot", {}).get("orderbook") if isinstance(venues.get("spot"), dict) else None
    futures_ob = venues.get("futures", {}).get("orderbook") if isinstance(venues.get("futures"), dict) else None
    spot_mid = _to_float(spot_ob.get("mid")) if isinstance(spot_ob, dict) else 0.0
    futures_mid = _to_float(futures_ob.get("mid")) if isinstance(futures_ob, dict) else 0.0
    basis = futures_mid - spot_mid if spot_mid > 0 and futures_mid > 0 else 0.0
    return {
        "ok": spot_mid > 0 and futures_mid > 0,
        "spotMid": spot_mid if spot_mid > 0 else None,
        "futuresMid": futures_mid if futures_mid > 0 else None,
        "basis": basis if spot_mid > 0 and futures_mid > 0 else None,
        "basisPct": basis / spot_mid if spot_mid > 0 and futures_mid > 0 else None,
        "status": "ok" if spot_mid > 0 and futures_mid > 0 else "insufficient_data",
        "message": "Spot-futures basis is computed from public Spot and USD-M futures mid prices.",
    }


def _stream_symbols(symbols: List[str], limit: int = MAX_STREAM_SYMBOLS) -> List[str]:
    out = [_binance_symbol(symbol) for symbol in symbols]
    out = [symbol for symbol in dict.fromkeys(out) if symbol]
    if not out:
        out = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT"]
    return out[: max(1, min(int(limit), 12))]


def _set_stream_status(status: str, *, error: str = "", venue: str = "") -> None:
    with _STREAM_LOCK:
        _STREAM_STATE["status"] = status
        _STREAM_STATE["updatedAt"] = _now_iso()
        if error:
            _STREAM_STATE["errors"].appendleft({"ts": _now_iso(), "venue": venue, "message": error})


def _set_connection_status(venue: str, status: str, *, streams: int = 0, error: str = "") -> None:
    with _STREAM_LOCK:
        _STREAM_STATE["connections"][venue] = {
            "status": status,
            "streams": streams,
            "updatedAt": _now_iso(),
            "error": error,
        }
        if error:
            _STREAM_STATE["errors"].appendleft({"ts": _now_iso(), "venue": venue, "message": error})


def _window_for(kind: str, venue: str, symbol: str, maxlen: int = STREAM_RAW_WINDOW_MAXLEN) -> Deque[Dict[str, Any]]:
    bucket = _STREAM_STATE[kind][venue]
    if symbol not in bucket:
        bucket[symbol] = deque(maxlen=maxlen)
    return bucket[symbol]


def _stream_window_ms(window_seconds: int) -> int:
    try:
        seconds = int(window_seconds)
    except Exception:
        seconds = STREAM_WINDOW_SECONDS_OPTIONS[0]
    if seconds not in STREAM_WINDOW_SECONDS_OPTIONS:
        seconds = min(STREAM_WINDOW_SECONDS_OPTIONS, key=lambda item: abs(item - seconds))
    return seconds * 1000


def _series_bucket_ms(window_ms: int) -> int:
    if window_ms <= 5 * 60 * 1000:
        return 30 * 1000
    if window_ms <= 15 * 60 * 1000:
        return 60 * 1000
    return 5 * 60 * 1000


def _ms_to_iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat()


def _window_available_seconds(rows: List[Dict[str, Any]]) -> int:
    if len(rows) < 2:
        return 0
    first = int(_to_float(rows[0].get("tsMs")))
    last = int(_to_float(rows[-1].get("tsMs")))
    return max(0, int((last - first) / 1000))


def _ofi_series(rows: List[Dict[str, Any]], window_ms: int) -> List[Dict[str, Any]]:
    bucket_ms = _series_bucket_ms(window_ms)
    buckets: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        ts_ms = int(_to_float(row.get("tsMs")))
        if ts_ms <= 0:
            continue
        bucket_key = (ts_ms // bucket_ms) * bucket_ms
        bucket = buckets.setdefault(bucket_key, {"tsMs": bucket_key, "ofi": 0.0, "ofiNormTotal": 0.0, "samples": 0})
        bucket["ofi"] += _to_float(row.get("ofi"))
        bucket["ofiNormTotal"] += _to_float(row.get("ofiNorm"))
        bucket["samples"] += 1
    out: List[Dict[str, Any]] = []
    for bucket_key in sorted(buckets.keys()):
        bucket = buckets[bucket_key]
        samples = int(bucket.get("samples") or 0)
        out.append(
            {
                "ts": _ms_to_iso(bucket_key),
                "ofi": _to_float(bucket.get("ofi")),
                "ofiNorm": _to_float(bucket.get("ofiNormTotal")) / samples if samples > 0 else 0.0,
                "samples": samples,
            }
        )
    return out


def _trade_series(rows: List[Dict[str, Any]], window_ms: int) -> List[Dict[str, Any]]:
    bucket_ms = _series_bucket_ms(window_ms)
    buckets: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        ts_ms = int(_to_float(row.get("tsMs")))
        if ts_ms <= 0:
            continue
        bucket_key = (ts_ms // bucket_ms) * bucket_ms
        bucket = buckets.setdefault(bucket_key, {"tsMs": bucket_key, "buyNotional": 0.0, "sellNotional": 0.0, "samples": 0})
        if row.get("side") == "buy":
            bucket["buyNotional"] += _to_float(row.get("notional"))
        elif row.get("side") == "sell":
            bucket["sellNotional"] += _to_float(row.get("notional"))
        bucket["samples"] += 1
    out: List[Dict[str, Any]] = []
    for bucket_key in sorted(buckets.keys()):
        bucket = buckets[bucket_key]
        buy = _to_float(bucket.get("buyNotional"))
        sell = _to_float(bucket.get("sellNotional"))
        total = buy + sell
        out.append(
            {
                "ts": _ms_to_iso(bucket_key),
                "buyNotional": buy,
                "sellNotional": sell,
                "takerBuyRatio": buy / total if total > 0 else 0.0,
                "imbalance": (buy - sell) / total if total > 0 else 0.0,
                "samples": int(bucket.get("samples") or 0),
            }
        )
    return out


def _record_depth_event(venue: str, symbol: str, payload: Dict[str, Any]) -> None:
    normalized = {
        "lastUpdateId": payload.get("lastUpdateId") or payload.get("u"),
        "bids": payload.get("bids") or payload.get("b") or [],
        "asks": payload.get("asks") or payload.get("a") or [],
    }
    metrics = _orderbook_metrics(normalized, venue=venue)
    metrics["symbol"] = _ccxt_symbol(symbol, venue)
    metrics["binanceSymbol"] = symbol
    metrics["ts"] = _now_iso()
    metrics["eventTimeMs"] = int(_to_float(payload.get("E") or payload.get("T"), time.time() * 1000))

    with _STREAM_LOCK:
        venue_books = _STREAM_STATE["orderbooks"][venue]
        previous = venue_books.get(symbol)
        venue_books[symbol] = metrics
        if previous:
            bid_delta = _to_float(metrics.get("bidNotional")) - _to_float(previous.get("bidNotional"))
            ask_delta = _to_float(metrics.get("askNotional")) - _to_float(previous.get("askNotional"))
            ofi = bid_delta - ask_delta
            denom = _to_float(metrics.get("bidNotional")) + _to_float(metrics.get("askNotional"))
            _window_for("ofiWindows", venue, symbol).append(
                {
                    "ts": metrics["ts"],
                    "tsMs": metrics["eventTimeMs"],
                    "ofi": ofi,
                    "ofiNorm": ofi / denom if denom > 0 else 0.0,
                    "bidDelta": bid_delta,
                    "askDelta": ask_delta,
                }
            )


def _record_agg_trade_event(venue: str, payload: Dict[str, Any]) -> None:
    symbol = str(payload.get("s") or "").upper()
    if not symbol:
        return
    qty = _to_float(payload.get("q"))
    price = _to_float(payload.get("p"))
    if qty <= 0 or price <= 0:
        return
    ts_ms = int(_to_float(payload.get("T") or payload.get("E"), time.time() * 1000))
    side = "sell" if bool(payload.get("m")) else "buy"
    with _STREAM_LOCK:
        _window_for("tradeWindows", venue, symbol).append(
            {
                "ts": datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat(),
                "tsMs": ts_ms,
                "side": side,
                "qty": qty,
                "notional": qty * price,
                "price": price,
            }
        )


def _record_liquidation_event(payload: Dict[str, Any]) -> None:
    order = payload.get("o") if isinstance(payload.get("o"), dict) else {}
    symbol = str(order.get("s") or payload.get("s") or "").upper()
    if not symbol:
        return
    qty = _to_float(order.get("z") or order.get("q") or order.get("l"))
    price = _to_float(order.get("ap") or order.get("p"))
    ts_ms = int(_to_float(order.get("T") or payload.get("E"), time.time() * 1000))
    with _STREAM_LOCK:
        _STREAM_STATE["liquidations"].appendleft(
            {
                "ts": datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat(),
                "symbol": _ccxt_symbol(symbol, "futures"),
                "side": str(order.get("S") or ""),
                "orderType": str(order.get("o") or ""),
                "status": str(order.get("X") or ""),
                "qty": qty,
                "price": price,
                "notional": qty * price,
            }
        )


def _summarize_ofi_window(venue: str, symbol: str, now_ms: int, window_ms: int) -> Dict[str, Any]:
    rows = list(_STREAM_STATE["ofiWindows"][venue].get(symbol) or [])
    rows = [row for row in rows if now_ms - int(row.get("tsMs", 0)) <= window_ms]
    if not rows:
        return {"samples": 0, "ofi": 0.0, "ofiNorm": 0.0, "latestTs": "", "availableSeconds": 0, "series": []}
    return {
        "samples": len(rows),
        "ofi": sum(_to_float(row.get("ofi")) for row in rows),
        "ofiNorm": sum(_to_float(row.get("ofiNorm")) for row in rows) / len(rows),
        "latestTs": str(rows[-1].get("ts") or ""),
        "availableSeconds": _window_available_seconds(rows),
        "series": _ofi_series(rows, window_ms),
    }


def _summarize_trade_window(venue: str, symbol: str, now_ms: int, window_ms: int) -> Dict[str, Any]:
    rows = list(_STREAM_STATE["tradeWindows"][venue].get(symbol) or [])
    rows = [row for row in rows if now_ms - int(row.get("tsMs", 0)) <= window_ms]
    if not rows:
        return {
            "samples": 0,
            "buyNotional": 0.0,
            "sellNotional": 0.0,
            "takerBuyRatio": 0.0,
            "imbalance": 0.0,
            "latestTs": "",
            "availableSeconds": 0,
            "series": [],
        }
    buy = sum(_to_float(row.get("notional")) for row in rows if row.get("side") == "buy")
    sell = sum(_to_float(row.get("notional")) for row in rows if row.get("side") == "sell")
    total = buy + sell
    return {
        "samples": len(rows),
        "buyNotional": buy,
        "sellNotional": sell,
        "takerBuyRatio": buy / total if total > 0 else 0.0,
        "imbalance": (buy - sell) / total if total > 0 else 0.0,
        "latestTs": str(rows[-1].get("ts") or ""),
        "availableSeconds": _window_available_seconds(rows),
        "series": _trade_series(rows, window_ms),
    }


def _liquidation_direction(side: str) -> str:
    normalized = str(side or "").upper()
    if normalized == "SELL":
        return "long"
    if normalized == "BUY":
        return "short"
    return "unknown"


def _liquidation_aggregate(rows: List[Dict[str, Any]], now_ms: int) -> Dict[str, Any]:
    by_direction: Dict[str, Dict[str, Any]] = {
        "long": {"count": 0, "notional": 0.0},
        "short": {"count": 0, "notional": 0.0},
        "unknown": {"count": 0, "notional": 0.0},
    }
    five_minute: Dict[str, Dict[str, Any]] = {
        "long": {"count": 0, "notional": 0.0},
        "short": {"count": 0, "notional": 0.0},
        "unknown": {"count": 0, "notional": 0.0},
    }
    max_event: Optional[Dict[str, Any]] = None
    for row in rows:
        direction = _liquidation_direction(str(row.get("side") or ""))
        notional = _to_float(row.get("notional"))
        by_direction[direction]["count"] += 1
        by_direction[direction]["notional"] += notional
        if max_event is None or notional > _to_float(max_event.get("notional")):
            max_event = row
        try:
            ts_ms = int(datetime.fromisoformat(str(row.get("ts")).replace("Z", "+00:00")).timestamp() * 1000)
        except Exception:
            ts_ms = 0
        if ts_ms > 0 and now_ms - ts_ms <= 5 * 60 * 1000:
            five_minute[direction]["count"] += 1
            five_minute[direction]["notional"] += notional

    five_minute_total = sum(_to_float(item.get("notional")) for item in five_minute.values())
    return {
        "byDirection": by_direction,
        "maxEvent": dict(max_event) if max_event else None,
        "last5m": {
            "byDirection": five_minute,
            "longNotionalRatio": five_minute["long"]["notional"] / five_minute_total if five_minute_total > 0 else None,
            "shortNotionalRatio": five_minute["short"]["notional"] / five_minute_total if five_minute_total > 0 else None,
            "totalNotional": five_minute_total,
            "count": sum(int(item.get("count") or 0) for item in five_minute.values()),
        },
    }


def market_intel_stream_snapshot(selected_symbol: str = "", stream_window_seconds: int = 5 * 60) -> Dict[str, Any]:
    selected = _binance_symbol(selected_symbol)
    now_ms = int(time.time() * 1000)
    window_ms = _stream_window_ms(stream_window_seconds)
    with _STREAM_LOCK:
        venues: Dict[str, Any] = {}
        for venue in ("spot", "futures"):
            symbols = sorted(set(_STREAM_STATE["orderbooks"][venue].keys()) | set(_STREAM_STATE["tradeWindows"][venue].keys()))
            if selected and selected in symbols:
                symbols = [selected]
            venues[venue] = {
                symbol: {
                    "orderbook": dict(_STREAM_STATE["orderbooks"][venue].get(symbol) or {}),
                    "ofi": _summarize_ofi_window(venue, symbol, now_ms, window_ms),
                    "flow": _summarize_trade_window(venue, symbol, now_ms, window_ms),
                }
                for symbol in symbols[:12]
            }
        return {
            "status": _STREAM_STATE["status"],
            "startedAt": _STREAM_STATE["startedAt"],
            "updatedAt": _STREAM_STATE["updatedAt"],
            "connections": dict(_STREAM_STATE["connections"]),
            "venues": venues,
            "liquidations": list(_STREAM_STATE["liquidations"])[:50],
            "errors": list(_STREAM_STATE["errors"])[:20],
            "windowSeconds": int(window_ms / 1000),
        }


def _venue_streams(venue: str, symbols: List[str]) -> List[str]:
    streams: List[str] = []
    for symbol in symbols:
        lower = symbol.lower()
        if venue == "spot":
            streams.extend([f"{lower}@depth20", f"{lower}@aggTrade"])
        else:
            streams.extend([f"{lower}@depth20@500ms", f"{lower}@aggTrade", f"{lower}@forceOrder"])
    return streams


def _stream_url(venue: str, streams: List[str]) -> str:
    base = SPOT_WS_BASE_URL if venue == "spot" else FUTURES_WS_BASE_URL
    return f"{base}?streams={'/'.join(streams)}"


def _handle_stream_message(venue: str, payload: Dict[str, Any]) -> None:
    stream = str(payload.get("stream") or "")
    data = payload.get("data") if isinstance(payload.get("data"), dict) else payload
    event = str(data.get("e") or "")
    symbol = str(data.get("s") or stream.split("@", 1)[0]).upper()
    if "depth" in stream or event == "depthUpdate" or "bids" in data:
        _record_depth_event(venue, symbol, data)
    elif event == "aggTrade":
        _record_agg_trade_event(venue, data)
    elif event == "forceOrder":
        _record_liquidation_event(data)


async def _run_market_intel_venue_stream(venue: str, symbols: List[str]) -> None:
    import aiohttp

    streams = _venue_streams(venue, symbols)
    url = _stream_url(venue, streams)
    backoff = 1.0
    while True:
        try:
            _set_connection_status(venue, "connecting", streams=len(streams))
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url, heartbeat=30, receive_timeout=70) as ws:
                    _set_connection_status(venue, "open", streams=len(streams))
                    backoff = 1.0
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                _handle_stream_message(venue, json.loads(msg.data))
                            except Exception as exc:
                                _set_stream_status("running", error=f"parse error: {exc}", venue=venue)
                        elif msg.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                            break
        except asyncio.CancelledError:
            _set_connection_status(venue, "stopped", streams=len(streams))
            raise
        except Exception as exc:
            _set_connection_status(venue, "error", streams=len(streams), error=str(exc))
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 30.0)


async def run_market_intel_stream_collector(symbols: List[str], symbol_limit: int = MAX_STREAM_SYMBOLS) -> None:
    stream_symbols = _stream_symbols(symbols, limit=symbol_limit)
    if not stream_symbols:
        _set_stream_status("disabled", error="no symbols configured")
        return
    with _STREAM_LOCK:
        _STREAM_STATE["status"] = "running"
        _STREAM_STATE["startedAt"] = _now_iso()
        _STREAM_STATE["updatedAt"] = _STREAM_STATE["startedAt"]
    try:
        await asyncio.gather(
            _run_market_intel_venue_stream("spot", stream_symbols),
            _run_market_intel_venue_stream("futures", stream_symbols),
        )
    except asyncio.CancelledError:
        _set_stream_status("stopped")
        raise


def build_market_intel_summary(
    *,
    symbols: List[str],
    selected_symbol: str,
    interval: str = "15m",
    lookback_bars: int = 96,
    depth_limit: int = 20,
    stream_window_seconds: int = 5 * 60,
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
            "sourceErrors": [],
            "orderbook": None,
            "flow": None,
            "volumeRatio": 0.0,
            "sessionEffect": [],
            "sessionHeatmap": [],
            "derivatives": None,
        }

        source_errors: List[str] = []
        try:
            venue_payload["orderbook"] = _fetch_depth(venue, selected, depth_limit)
        except Exception as exc:
            source_errors.append(f"depth: {exc}")

        try:
            venue_payload["flow"] = _fetch_agg_trade_flow(venue, selected)
        except Exception as exc:
            source_errors.append(f"aggTrades: {exc}")

        try:
            rows = _fetch_klines(venue, selected, interval, lookback_bars)
            venue_payload["volumeRatio"] = _volume_ratio(rows)
            venue_payload["sessionEffect"] = _session_effect(rows)
            venue_payload["sessionHeatmap"] = _session_heatmap(rows)
        except Exception as exc:
            source_errors.append(f"klines: {exc}")

        if venue == "futures":
            try:
                venue_payload["derivatives"] = _fetch_futures_derivatives(selected, interval)
            except Exception as exc:
                source_errors.append(f"derivatives: {exc}")

        venue_payload["sourceErrors"] = source_errors
        if source_errors and not venue_payload["orderbook"] and not venue_payload["flow"] and not venue_payload["sessionEffect"]:
            venue_payload["ok"] = False
            venue_payload["error"] = "; ".join(source_errors[:3])
        venues[venue] = venue_payload

    for symbol in binance_symbols[:8]:
        try:
            klines_for_corr[symbol] = _fetch_klines("futures", symbol, interval, lookback_bars)
        except Exception:
            continue

    stream = market_intel_stream_snapshot(selected, stream_window_seconds=stream_window_seconds)
    stream_venues = stream.get("venues", {}) if isinstance(stream.get("venues"), dict) else {}
    for venue in ("spot", "futures"):
        venue_streams = stream_venues.get(venue, {}) if isinstance(stream_venues.get(venue), dict) else {}
        venues[venue]["stream"] = venue_streams.get(selected) or {}

    liquidations = stream.get("liquidations", []) if isinstance(stream.get("liquidations"), list) else []
    stream_status = str(stream.get("status") or "stopped")
    now_ms = int(time.time() * 1000)

    returns_for_corr = _returns_by_symbol(klines_for_corr)
    rolling_corr = _rolling_correlation_series(klines_for_corr, window=24)

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
            "symbols": [_ccxt_symbol(symbol, "futures") for symbol in sorted(returns_for_corr.keys())],
            "matrix": _correlation_matrix(returns_for_corr),
            "rolling": rolling_corr,
            "breaks": _correlation_breaks(rolling_corr),
        },
        "basis": _basis_metrics(venues),
        "stream": stream,
        "liquidations": {
            "status": "running" if liquidations else stream_status,
            "message": "Binance forceOrder stream is connected; rows appear only when liquidations occur."
            if stream_status == "running"
            else "Binance forceOrder stream is not running.",
            "rows": liquidations,
            "aggregate": _liquidation_aggregate(liquidations, now_ms),
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
