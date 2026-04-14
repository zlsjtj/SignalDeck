import argparse
import math
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List
import time
import sys
from pathlib import Path
import glob

import pandas as pd
import ccxt

if __package__ in (None, ""):
    repo_root = Path(__file__).resolve().parent.parent
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

from statarb.config import load_config
from statarb.factors import compute_scores
from statarb.portfolio import target_weights_from_scores
from statarb.execution import _apply_precision
from statarb.paper import PaperAccount
from main import build_order_intents


def parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def timeframe_to_minutes(tf: str) -> int:
    unit = tf[-1]
    val = int(tf[:-1])
    if unit == "m":
        return val
    if unit == "h":
        return val * 60
    if unit == "d":
        return val * 1440
    raise ValueError(f"Unsupported timeframe: {tf}")


def normalize_weights(target_w: pd.Series, gross_leverage: float, max_w: float) -> pd.Series:
    w = target_w.copy()
    if w.empty:
        return w
    if max_w and max_w > 0:
        w = w.clip(-max_w, max_w)
    gross = float(w.abs().sum())
    if gross > 0 and gross_leverage > 0:
        w = w * (gross_leverage / gross)
    return w


def annualized_vol_from_closes(closes: pd.Series, tf_minutes: int) -> float:
    if closes is None or closes.empty or len(closes) < 3:
        return 0.0
    rets = closes.pct_change().dropna()
    if rets.empty:
        return 0.0
    vol = float(rets.std())
    if vol <= 0 or math.isnan(vol):
        return 0.0
    ann_factor = 365.0 * 24.0 * 60.0 / max(1, tf_minutes)
    return vol * math.sqrt(ann_factor)


def abs_momentum_from_closes(closes: pd.Series, tf_minutes: int, lookback_hours: float) -> float:
    if closes is None or closes.empty or tf_minutes <= 0:
        return 0.0
    bars = max(3, int(lookback_hours * 60 / tf_minutes))
    closes = closes.tail(bars)
    if len(closes) < bars:
        return 0.0
    first = float(closes.iloc[0])
    last = float(closes.iloc[-1])
    if first <= 0:
        return 0.0
    return (last / first) - 1.0


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def _cache_path(symbol: str, timeframe: str, start_ms: int, end_ms: int) -> str:
    safe_symbol = symbol.replace("/", "_").replace(":", "_")
    return os.path.join("logs", "cache", f"{safe_symbol}_{timeframe}_{start_ms}_{end_ms}.csv")


def _funding_cache_glob(symbol: str) -> str:
    safe_symbol = symbol.replace("/", "_").replace(":", "_")
    return os.path.join("logs", "cache", f"funding_{safe_symbol}_*.csv")


def load_funding_series_from_cache(symbol: str, start: datetime, end_exclusive: datetime) -> pd.Series:
    paths = sorted(glob.glob(_funding_cache_glob(symbol)))
    if not paths:
        return pd.Series(dtype=float)

    frames = []
    for path in paths:
        try:
            df = pd.read_csv(path)
        except Exception:
            continue
        if df.empty or "ts" not in df.columns or "funding_rate" not in df.columns:
            continue
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        df["funding_rate"] = pd.to_numeric(df["funding_rate"], errors="coerce")
        df = df.dropna(subset=["ts", "funding_rate"])
        if df.empty:
            continue
        frames.append(df[["ts", "funding_rate"]])

    if not frames:
        return pd.Series(dtype=float)

    all_df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ts"]).sort_values("ts")
    all_df = all_df[(all_df["ts"] >= start) & (all_df["ts"] < end_exclusive)]
    if all_df.empty:
        return pd.Series(dtype=float)
    all_df = all_df.set_index("ts")
    return all_df["funding_rate"]


def fetch_ohlcv_range(
    ex, symbol: str, timeframe: str, start_ms: int, end_ms: int, use_cache: bool = True
) -> pd.DataFrame:
    cache_path = _cache_path(symbol, timeframe, start_ms, end_ms)
    if use_cache and os.path.exists(cache_path):
        df = pd.read_csv(cache_path)
        if not df.empty:
            df["ts"] = pd.to_datetime(df["ts"], utc=True)
            df.set_index("ts", inplace=True)
            return df
    tf_ms = timeframe_to_minutes(timeframe) * 60_000
    all_rows: List[List[float]] = []
    since = start_ms
    limit = 1500
    while True:
        batch = ex.fetch_ohlcv(symbol, timeframe=timeframe, since=since, limit=limit)
        if not batch:
            break
        for row in batch:
            ts = row[0]
            if ts >= end_ms:
                break
            all_rows.append(row)
        last_ts = batch[-1][0]
        if last_ts >= end_ms or last_ts == since:
            break
        since = last_ts + tf_ms
        time.sleep(0.05)
    if not all_rows:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume"]).set_index("ts")
    df = pd.DataFrame(all_rows, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df.set_index("ts", inplace=True)
    if use_cache:
        ensure_dir(cache_path)
        df.to_csv(cache_path)
    return df


def backtest():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--config", default="config.yaml")
    ap.add_argument("--out", default="logs/backtest_equity.csv")
    ap.add_argument("--trades", default="logs/backtest_trades.csv")
    ap.add_argument("--metrics", default="logs/backtest_metrics.txt")
    ap.add_argument("--plot", default="logs/backtest_equity.png")
    args = ap.parse_args()

    cfg = load_config(args.config)
    start = parse_date(args.start)
    # Treat --end as inclusive date boundary.
    end_exclusive = parse_date(args.end) + timedelta(days=1)

    ex = getattr(ccxt, cfg.exchange)({"enableRateLimit": True})
    if not bool(cfg.raw.get("backtest_skip_markets", False)):
        ex.load_markets()

    tf_minutes = timeframe_to_minutes(cfg.timeframe)
    rebalance_every_minutes = int(cfg.raw.get("rebalance_every_minutes") or 0)
    # Backtest should respect the same rebalance cadence as live/paper loop.
    # If unset/<=0, default to "rebalance every bar".
    if rebalance_every_minutes > 0:
        rebalance_every_bars = max(1, int(round(rebalance_every_minutes / float(tf_minutes))))
    else:
        rebalance_every_bars = 1
    lookback = int(cfg.lookback_hours)
    warmup = lookback + 30
    start_fetch = start - timedelta(minutes=warmup * tf_minutes)

    data: Dict[str, pd.DataFrame] = {}
    for sym in cfg.symbols:
        df = fetch_ohlcv_range(
            ex,
            sym,
            cfg.timeframe,
            int(start_fetch.timestamp() * 1000),
            int(end_exclusive.timestamp() * 1000),
            use_cache=bool(cfg.raw.get("backtest_cache", True)),
        )
        if df.empty:
            raise RuntimeError(f"No data for {sym}")
        data[sym] = df

    common_idx = None
    for df in data.values():
        idx = df.index
        common_idx = idx if common_idx is None else common_idx.intersection(idx)
    common_idx = common_idx[(common_idx >= start) & (common_idx < end_exclusive)]
    if len(common_idx) < 2:
        raise RuntimeError("Not enough common timestamps across symbols")
    total_progress_steps = max(1, len(common_idx) - 1)
    progress_emit_every = max(
        1,
        int(
            cfg.raw.get(
                "backtest_progress_emit_every_bars",
                max(1, total_progress_steps // 100),
            )
        ),
    )
    last_progress_pct = -1

    def emit_progress(done_steps: int, ts_value: datetime) -> None:
        nonlocal last_progress_pct
        safe_done_steps = max(0, min(int(done_steps), total_progress_steps))
        pct = int(round((safe_done_steps * 100.0) / float(total_progress_steps)))
        if pct <= last_progress_pct and pct < 100:
            return
        last_progress_pct = max(last_progress_pct, pct)
        ts_text = ts_value.isoformat() if isinstance(ts_value, datetime) else ""
        print(
            f"BACKTEST_PROGRESS pct={pct} done={safe_done_steps} total={total_progress_steps} ts={ts_text}",
            flush=True,
        )

    emit_progress(0, common_idx[0])

    acct = PaperAccount(
        cash=float(cfg.raw.get("paper_equity_usdt", 10000)),
        fee_bps=float(cfg.raw.get("portfolio", {}).get("fee_bps", 0.0)),
    )
    equity_peak = acct.cash
    stop_out_until = None
    riskoff_active = False
    riskoff_cooldown_until_idx = -1
    riskoff_active_bars = 0
    riskoff_toggle_count = 0
    riskoff_first_ts = None
    riskoff_last_ts = None
    regime_delev_active_bars = 0
    regime_delev_trigger_count = 0
    rows = []
    trade_rows = []
    order_events = []
    day_key = None
    day_start_equity = None

    s_cfg = cfg.raw["strategy"]
    score_cfg = s_cfg.get("score", {})
    p_cfg = cfg.raw["portfolio"]
    e_cfg = cfg.raw["execution"]
    slippage_bps = float(p_cfg.get("slippage_bps", 0.0))
    order_type = str(e_cfg.get("order_type", "limit")).lower()
    limit_offset_bps = float(e_cfg.get("limit_price_offset_bps", 0.0))
    orders_attempted = 0
    orders_filled = 0
    orders_unfilled = 0
    orders_partial = 0
    orders_canceled_timeout = 0
    order_seq = 0
    limit_ttl_bars = int(cfg.raw.get("backtest_limit_ttl_bars", 1))
    max_participation_rate = float(cfg.raw.get("backtest_max_participation_rate", 1.0))
    max_participation_rate = max(0.0, min(1.0, max_participation_rate))
    exec_delay_bars = max(1, int(cfg.raw.get("backtest_exec_delay_bars", 1)))
    impact_enabled = bool(cfg.raw.get("backtest_impact_enabled", False))
    impact_base_bps = float(cfg.raw.get("backtest_impact_base_bps", 0.0))
    impact_exponent = float(cfg.raw.get("backtest_impact_exponent", 0.5))
    impact_cost_total = 0.0
    funding_enabled = bool(cfg.raw.get("backtest_funding_enabled", False))
    funding_const_rate = float(cfg.raw.get("backtest_funding_bps_per_8h", 0.0)) / 10000.0
    funding_series_by_symbol: Dict[str, pd.Series] = {}
    funding_fee_total = 0.0
    funding_events = 0
    if funding_enabled:
        for sym in cfg.symbols:
            funding_series_by_symbol[sym] = load_funding_series_from_cache(sym, start, end_exclusive)

    margin_enabled = bool(cfg.raw.get("backtest_margin_enabled", False))
    maintenance_margin_ratio = float(cfg.raw.get("backtest_maintenance_margin_ratio", 0.05))
    liquidation_penalty_bps = float(cfg.raw.get("backtest_liquidation_penalty_bps", 20.0))
    liquidated = False
    liquidation_count = 0
    regime_delev_cfg = cfg.raw.get("strategy", {}).get("regime_deleverage", {})

    pending_fills_by_ts: Dict[pd.Timestamp, List[Dict]] = {}
    pending_orders: List[Dict] = []

    def process_pending_orders(i: int, ts: pd.Timestamp, prices_ts: Dict[str, float]) -> None:
        nonlocal orders_filled, orders_unfilled, orders_partial, orders_canceled_timeout, impact_cost_total

        if not pending_orders:
            return

        bar_fill_capacity_used: Dict[str, float] = {}
        for order in list(pending_orders):
            if order["activate_i"] > i:
                continue

            if i > order["expire_i"] and order["remaining"] > 0:
                orders_unfilled += 1
                orders_canceled_timeout += 1
                order_events.append(
                    {
                        "ts_event_utc": ts.isoformat(),
                        "event": "canceled_timeout",
                        "order_id": order["order_id"],
                        "symbol": order["symbol"],
                        "side": order["side"],
                        "order_type": order["order_type"],
                        "limit_price": order["price"],
                        "amount_original": order["amount_original"],
                        "amount_remaining": order["remaining"],
                        "ts_signal_utc": order["ts_signal_utc"],
                        "ts_submit_utc": order["ts_submit_utc"],
                    }
                )
                pending_orders.remove(order)
                continue

            sym = order["symbol"]
            side = order["side"]
            px_high = float(data[sym].loc[ts, "high"])
            px_low = float(data[sym].loc[ts, "low"])

            fillable = (px_low <= order["price"]) if side == "buy" else (px_high >= order["price"])
            if not fillable:
                continue

            bar_vol = float(data[sym].loc[ts, "volume"])
            bar_capacity_amount = max(0.0, bar_vol * max_participation_rate)
            used = bar_fill_capacity_used.get(sym, 0.0)
            available = max(0.0, bar_capacity_amount - used)
            if max_participation_rate >= 1.0:
                available = order["remaining"]
            fill_amount = min(order["remaining"], available)
            if fill_amount <= 0:
                continue

            bar_fill_capacity_used[sym] = used + fill_amount
            fill_price = float(order["price"])
            if impact_enabled and bar_vol > 0 and impact_base_bps > 0:
                participation = max(0.0, min(1.0, fill_amount / bar_vol))
                impact_bps = impact_base_bps * (participation ** max(0.0, impact_exponent))
                impact_rate = impact_bps / 10000.0
                if side == "buy":
                    fill_price = fill_price * (1.0 + impact_rate)
                else:
                    fill_price = fill_price * (1.0 - impact_rate)
            notional = fill_amount * fill_price
            fee = notional * (float(p_cfg.get("fee_bps", 0.0)) / 10000.0)
            acct.apply_fills([{"symbol": sym, "side": side, "amount": fill_amount, "price": fill_price, "fee": fee}])
            impact_cost_total += abs(fill_amount * (fill_price - float(order["price"])))
            order["remaining"] -= fill_amount
            trade_rows.append(
                {
                    "ts_signal_utc": order["ts_signal_utc"],
                    "ts_exec_utc": ts.isoformat(),
                    "symbol": sym,
                    "side": side,
                    "delta_w": order["delta_w"],
                    "amount": fill_amount,
                    "price": fill_price,
                    "notional": notional,
                    "fee": fee,
                    "slippage_bps": slippage_bps,
                    "order_type": order["order_type"],
                    "reduce_only": bool(order.get("reduce_only", False)),
                    "position_side": order.get("position_side"),
                    "equity_before": order.get("equity_before", 0.0),
                    "order_id": order["order_id"],
                }
            )

            if order["remaining"] <= 1e-12:
                orders_filled += 1
                event = "filled"
                remaining = 0.0
                pending_orders.remove(order)
            else:
                orders_partial += 1
                event = "partial_fill"
                remaining = order["remaining"]

            order_events.append(
                {
                    "ts_event_utc": ts.isoformat(),
                    "event": event,
                    "order_id": order["order_id"],
                    "symbol": sym,
                    "side": side,
                    "order_type": order["order_type"],
                    "limit_price": order["price"],
                    "fill_price": fill_price,
                    "fill_amount": fill_amount,
                    "amount_original": order["amount_original"],
                    "amount_remaining": remaining,
                    "ts_signal_utc": order["ts_signal_utc"],
                    "ts_submit_utc": order["ts_submit_utc"],
                }
            )

    for i in range(len(common_idx) - 1):
        ts = common_idx[i]

        # Apply fills scheduled for this bar (typically generated at prior bar signal time).
        due_fills = pending_fills_by_ts.pop(ts, [])
        if due_fills:
            acct.apply_fills(due_fills)

        prices_ts = {s: float(df.loc[ts, "close"]) for s, df in data.items()}

        if funding_enabled and i > 0:
            prev_ts = common_idx[i - 1]
            dt_hours = max(0.0, (ts - prev_ts).total_seconds() / 3600.0)
            if dt_hours > 0 and acct.positions:
                interval_scale = dt_hours / 8.0
                for sym, qty in list(acct.positions.items()):
                    if abs(qty) <= 1e-12:
                        continue
                    px = float(prices_ts.get(sym, 0.0))
                    if px <= 0:
                        continue
                    fs = funding_series_by_symbol.get(sym)
                    if fs is not None and not fs.empty:
                        hist = fs.loc[:ts]
                        rate = float(hist.iloc[-1]) if not hist.empty else funding_const_rate
                    else:
                        rate = funding_const_rate
                    if rate == 0.0:
                        continue
                    signed_notional = qty * px
                    funding_fee = -signed_notional * rate * interval_scale
                    acct.cash -= funding_fee
                    funding_fee_total += funding_fee
                    funding_events += 1

        process_pending_orders(i, ts, prices_ts)

        equity_ts = acct.equity_from_prices(prices_ts)
        if equity_ts > equity_peak:
            equity_peak = equity_ts
        dd = 1.0 - (equity_ts / equity_peak) if equity_peak > 0 else 0.0
        ts_day = ts.date()
        if day_key is None or ts_day != day_key:
            day_key = ts_day
            day_start_equity = equity_ts
        if margin_enabled and not liquidated:
            gross_notional = 0.0
            for sym, qty in acct.positions.items():
                px = float(prices_ts.get(sym, 0.0))
                gross_notional += abs(qty * px)
            margin_ratio = (equity_ts / gross_notional) if gross_notional > 0 else float("inf")
            if equity_ts <= 0 or (gross_notional > 0 and margin_ratio < maintenance_margin_ratio):
                liq_fills = []
                penalty = liquidation_penalty_bps / 10000.0
                for sym, qty in list(acct.positions.items()):
                    if abs(qty) <= 1e-12:
                        continue
                    close_px = float(prices_ts.get(sym, 0.0))
                    if close_px <= 0:
                        continue
                    if qty > 0:
                        side = "sell"
                        liq_px = close_px * (1 - penalty)
                        amount = qty
                    else:
                        side = "buy"
                        liq_px = close_px * (1 + penalty)
                        amount = abs(qty)
                    amount, liq_px = _apply_precision(ex, sym, amount, liq_px)
                    if amount <= 0 or liq_px <= 0:
                        continue
                    notion = amount * liq_px
                    fee = notion * (float(p_cfg.get("fee_bps", 0.0)) / 10000.0)
                    liq_fills.append(
                        {"symbol": sym, "side": side, "amount": amount, "price": liq_px, "fee": fee}
                    )
                if liq_fills:
                    acct.apply_fills(liq_fills)
                liquidated = True
                liquidation_count += 1

        # Only rebalance on the configured cadence. Between rebalances, just mark-to-market.
        rebalance_now = (i % rebalance_every_bars) == 0
        regime_delev_active = False
        fills = []
        if rebalance_now and not liquidated:
            sliced = {s: df.loc[:ts] for s, df in data.items()}

            score = compute_scores(
                sliced,
                w_reversal=float(score_cfg.get("w_reversal", 0.0)),
                w_momentum=float(score_cfg.get("w_momentum", 1.0)),
                w_trend=float(score_cfg.get("w_trend", 0.0)),
                w_flow=float(score_cfg.get("w_flow", 0.0)),
                w_volz=float(score_cfg.get("w_volz", 0.0)),
                w_volume=float(score_cfg.get("w_volume", 0.0)),
                lookback=int(cfg.lookback_hours),
                mom_lookback=int(score_cfg.get("mom_lookback", max(3, int(cfg.lookback_hours / 2)))),
                trend_lookback=int(score_cfg.get("trend_lookback", max(6, int(cfg.lookback_hours * 3)))),
                flow_lookback=int(score_cfg.get("flow_lookback", max(6, int(cfg.lookback_hours)))),
                vol_lookback=int(score_cfg.get("vol_lookback", 12)),
                volume_lookback=int(score_cfg.get("volume_lookback", 24)),
                zscore_clip=float(score_cfg.get("zscore_clip", 3.0)),
                use_market_neutral=bool(score_cfg.get("use_market_neutral", False)),
                benchmark_symbol=str(
                    score_cfg.get(
                        "benchmark_symbol", cfg.symbols[0] if cfg.symbols else ""
                    )
                ),
                min_notional_usdt=float(score_cfg.get("min_notional_usdt", 0.0)),
                max_vol=float(score_cfg.get("max_vol", 0.0)),
            )

            risk_off = False
            score_threshold = s_cfg.get("score_threshold", None)
            if score_threshold is not None and not score.empty:
                max_score = float(score.max())
                if max_score < float(score_threshold):
                    risk_off = True

            risk_cfg = s_cfg.get("risk_off", {})
            if risk_cfg.get("enabled", False):
                bench = str(
                    risk_cfg.get(
                        "benchmark_symbol",
                        score_cfg.get("benchmark_symbol", cfg.symbols[0] if cfg.symbols else ""),
                    )
                )
                mode = str(risk_cfg.get("mode", "benchmark_mom"))
                rb_hours = float(risk_cfg.get("lookback_hours", cfg.lookback_hours * 2))
                threshold = float(risk_cfg.get("threshold", 0.0))
                if bench in sliced and tf_minutes > 0:
                    bars = max(3, int(rb_hours * 60 / tf_minutes))
                    closes = sliced[bench]["close"].tail(bars)
                    if len(closes) >= bars:
                        last = float(closes.iloc[-1])
                        first = float(closes.iloc[0])
                        if mode == "benchmark_sma":
                            sma = float(closes.mean())
                            if sma > 0 and last < sma * (1.0 - threshold):
                                risk_off = True
                        else:
                            if first > 0 and (last / first - 1.0) < threshold:
                                risk_off = True

            target_w = target_weights_from_scores(
                score,
                long_q=float(s_cfg.get("long_quantile", 1.0)),
                short_q=float(s_cfg.get("short_quantile", 0.0)),
                gross_leverage=float(p_cfg["gross_leverage"]),
                max_w=float(p_cfg["max_weight_per_symbol"]),
                min_score_spread=float(p_cfg.get("min_score_spread", 0.0)),
                long_high_score=bool(s_cfg.get("long_high_score", True)),
                weight_mode=str(s_cfg.get("weight_mode", "equal")),
                score_weight_clip=float(s_cfg.get("score_weight_clip", 3.0)),
            )

            force_rebalance = False
            risk_cfg = s_cfg.get("risk_off", {})
            if risk_cfg.get("enabled", False) and tf_minutes > 0:
                bench = str(
                    risk_cfg.get(
                        "benchmark_symbol",
                        score_cfg.get("benchmark_symbol", cfg.symbols[0] if cfg.symbols else ""),
                    )
                )
                mode = str(risk_cfg.get("mode", "btc_trend"))
                hysteresis = float(risk_cfg.get("hysteresis", 0.0))
                cooldown_bars = int(risk_cfg.get("cooldown_bars", 0))
                risk_off_scale = float(risk_cfg.get("risk_off_scale", 1.0))
                btc_sma_bars = int(risk_cfg.get("btc_sma_bars", 0))
                if btc_sma_bars <= 0:
                    lb_hours = float(risk_cfg.get("lookback_hours", cfg.lookback_hours * 2))
                    btc_sma_bars = max(3, int(lb_hours * 60 / tf_minutes))
                desired = riskoff_active
                if bench in sliced and mode == "btc_trend":
                    closes = sliced[bench]["close"].tail(btc_sma_bars)
                    if len(closes) >= btc_sma_bars:
                        sma = float(closes.mean())
                        last = float(closes.iloc[-1])
                        if not riskoff_active and sma > 0 and last < sma:
                            desired = True
                        if riskoff_active and sma > 0 and last > sma * (1.0 + hysteresis):
                            desired = False
                if cooldown_bars > 0 and i < riskoff_cooldown_until_idx:
                    desired = riskoff_active
                if desired != riskoff_active:
                    riskoff_active = desired
                    riskoff_toggle_count += 1
                    force_rebalance = True
                    if riskoff_first_ts is None:
                        riskoff_first_ts = ts
                    riskoff_last_ts = ts
                    if cooldown_bars > 0:
                        riskoff_cooldown_until_idx = i + cooldown_bars
                if riskoff_active and risk_off_scale < 1.0:
                    target_w = target_w * risk_off_scale

            abs_mom_cfg = s_cfg.get("abs_mom_filter", {})
            if abs_mom_cfg.get("enabled", False) and tf_minutes > 0 and not target_w.empty:
                apply_abs_filter = True
                if abs_mom_cfg.get("regime_gate_enabled", False):
                    bench = str(
                        abs_mom_cfg.get(
                            "benchmark_symbol",
                            score_cfg.get("benchmark_symbol", cfg.symbols[0] if cfg.symbols else ""),
                        )
                    )
                    rg_hours = float(abs_mom_cfg.get("regime_lookback_hours", 24.0 * 14.0))
                    rg_bars = max(6, int(rg_hours * 60 / tf_minutes))
                    rg_mom_thr = float(abs_mom_cfg.get("regime_mom_threshold", -0.02))
                    rg_vol_q = float(abs_mom_cfg.get("regime_vol_quantile", 0.3))
                    rg_vol_q = min(1.0, max(0.0, rg_vol_q))
                    apply_abs_filter = False
                    if bench in sliced:
                        bclose = sliced[bench]["close"].tail(max(rg_bars * 4, rg_bars + 3))
                        if len(bclose) >= rg_bars + 2:
                            b_mom = (float(bclose.iloc[-1]) / float(bclose.iloc[-rg_bars])) - 1.0
                            brets = bclose.pct_change()
                            bvol = brets.rolling(rg_bars).std() * math.sqrt(
                                (365.0 * 24.0 * 60.0) / max(1, tf_minutes)
                            )
                            bvol = bvol.dropna()
                            if not bvol.empty:
                                bvol_now = float(bvol.iloc[-1])
                                bvol_thr = float(bvol.quantile(rg_vol_q))
                                apply_abs_filter = (b_mom <= rg_mom_thr) and (bvol_now <= bvol_thr)

                if apply_abs_filter:
                    lb_hours = float(abs_mom_cfg.get("lookback_hours", cfg.lookback_hours))
                    min_ret = float(abs_mom_cfg.get("min_return", 0.0))
                    for sym in list(target_w.index):
                        df = sliced.get(sym)
                        if df is None or df.empty:
                            target_w.loc[sym] = 0.0
                            continue
                        ret = abs_momentum_from_closes(df["close"], tf_minutes, lb_hours)
                        if ret < min_ret:
                            target_w.loc[sym] = 0.0
                    target_w = normalize_weights(
                        target_w,
                        float(p_cfg["gross_leverage"]),
                        float(p_cfg["max_weight_per_symbol"]),
                    )

            inv_cfg = s_cfg.get("inv_vol", {})
            if inv_cfg.get("enabled", False) and tf_minutes > 0 and not target_w.empty:
                iv_hours = float(inv_cfg.get("lookback_hours", cfg.lookback_hours))
                bars = max(3, int(iv_hours * 60 / tf_minutes))
                inv = {}
                for sym in target_w.index:
                    df = sliced.get(sym)
                    if df is None or df.empty:
                        inv[sym] = 0.0
                        continue
                    vol = annualized_vol_from_closes(df["close"].tail(bars), tf_minutes)
                    inv[sym] = 0.0 if vol <= 0 else 1.0 / vol
                inv_s = pd.Series(inv)
                target_w = target_w * inv_s
                target_w = normalize_weights(
                    target_w,
                    float(p_cfg["gross_leverage"]),
                    float(p_cfg["max_weight_per_symbol"]),
                )

            vol_cfg = s_cfg.get("vol_target", {})
            if vol_cfg.get("enabled", False) and tf_minutes > 0 and not target_w.empty:
                vb = str(vol_cfg.get("bench_symbol", cfg.symbols[0] if cfg.symbols else ""))
                vb_hours = float(vol_cfg.get("lookback_hours", cfg.lookback_hours * 3))
                bars = max(3, int(vb_hours * 60 / tf_minutes))
                if vb in sliced:
                    ann_vol = annualized_vol_from_closes(sliced[vb]["close"].tail(bars), tf_minutes)
                    target_vol = float(vol_cfg.get("target_annual_vol", 0.4))
                    if ann_vol > 0 and target_vol > 0:
                        scale = target_vol / ann_vol
                        scale = clamp(
                            scale,
                            float(vol_cfg.get("min_leverage", 0.3)),
                            float(vol_cfg.get("max_leverage", 1.5)),
                        )
                        target_w = target_w * scale

            regime_delev_active = False
            if regime_delev_cfg.get("enabled", False) and tf_minutes > 0 and not target_w.empty:
                bench = str(
                    regime_delev_cfg.get(
                        "benchmark_symbol",
                        score_cfg.get("benchmark_symbol", cfg.symbols[0] if cfg.symbols else ""),
                    )
                )
                if bench in sliced:
                    lb_hours = float(regime_delev_cfg.get("lookback_hours", 24.0 * 14.0))
                    bars = max(6, int(lb_hours * 60 / tf_minutes))
                    closes = sliced[bench]["close"].tail(max(bars * 4, bars + 3))
                    if len(closes) >= bars + 2:
                        mom = (float(closes.iloc[-1]) / float(closes.iloc[-bars])) - 1.0
                        rets = closes.pct_change()
                        roll_vol = rets.rolling(bars).std() * math.sqrt((365.0 * 24.0 * 60.0) / max(1, tf_minutes))
                        roll_vol = roll_vol.dropna()
                        if not roll_vol.empty:
                            curr_vol = float(roll_vol.iloc[-1])
                            q = float(regime_delev_cfg.get("vol_quantile", 0.3))
                            q = min(1.0, max(0.0, q))
                            vol_thr = float(roll_vol.quantile(q))
                            mom_thr = float(regime_delev_cfg.get("mom_threshold", -0.02))
                            if mom <= mom_thr and curr_vol <= vol_thr:
                                regime_scale = float(regime_delev_cfg.get("scale", 0.5))
                                regime_scale = max(0.0, min(1.0, regime_scale))
                                target_w = target_w * regime_scale
                                regime_delev_active = True
                                regime_delev_trigger_count += 1

            if risk_off:
                scale = float(risk_cfg.get("risk_off_scale", 0.0))
                target_w = target_w * scale

            max_dd = float(cfg.raw.get("risk", {}).get("max_strategy_dd", 0.0))
            if max_dd and dd > 0:
                ratio = float(cfg.raw.get("risk", {}).get("dd_deleverage_ratio", 1.0))
                if dd >= max_dd:
                    scale = ratio
                else:
                    scale = ratio + (1.0 - ratio) * (1.0 - dd / max_dd)
                if scale < 1.0:
                    target_w = target_w * scale

            stop_dd = float(cfg.raw.get("risk", {}).get("stop_out_dd", 0.0))
            cool_hours = float(cfg.raw.get("risk", {}).get("cool_off_hours", 0.0))
            if stop_out_until and ts < stop_out_until:
                target_w = target_w * 0.0
            elif stop_dd and dd >= stop_dd:
                if cool_hours > 0:
                    stop_out_until = ts + timedelta(hours=cool_hours)
                target_w = target_w * 0.0

            current_w = acct.weights_from_prices(prices_ts, equity_ts)
            drift_threshold = float(p_cfg.get("drift_threshold", 0.0))
            intents = build_order_intents(
                cfg.symbols,
                current_w,
                target_w,
                drift_threshold=drift_threshold,
                force_rebalance=force_rebalance,
            )

            for intent in intents:
                sym = intent["symbol"]
                dw = float(intent["delta_w"])
                notion = abs(dw) * equity_ts
                min_cost = float(p_cfg["min_order_usdt"])
                if notion < min_cost:
                    continue
                exec_i = min(i + exec_delay_bars, len(common_idx) - 1)
                ts_exec = common_idx[exec_i]
                px_open = float(data[sym].loc[ts_exec, "open"])
                px_high = float(data[sym].loc[ts_exec, "high"])
                px_low = float(data[sym].loc[ts_exec, "low"])
                side = "buy" if dw > 0 else "sell"
                orders_attempted += 1

                fillable = True
                if order_type == "market":
                    slip = slippage_bps / 10000.0
                    price = px_open * (1 + slip) if side == "buy" else px_open * (1 - slip)
                else:
                    off = limit_offset_bps / 10000.0
                    price = px_open * (1 - off) if side == "buy" else px_open * (1 + off)
                    fillable = px_low <= price if side == "buy" else px_high >= price

                amount = notion / price
                amount, price = _apply_precision(ex, sym, amount, price)
                if amount <= 0 or price <= 0:
                    continue
                order_seq += 1
                order_id = f"bt_{order_seq}"

                if order_type == "market":
                    fill_price = float(price)
                    if impact_enabled:
                        bar_vol_exec = float(data[sym].loc[ts_exec, "volume"])
                        if bar_vol_exec > 0 and impact_base_bps > 0:
                            participation = max(0.0, min(1.0, amount / bar_vol_exec))
                            impact_bps = impact_base_bps * (participation ** max(0.0, impact_exponent))
                            impact_rate = impact_bps / 10000.0
                            if side == "buy":
                                fill_price = fill_price * (1.0 + impact_rate)
                            else:
                                fill_price = fill_price * (1.0 - impact_rate)
                    notional = amount * price
                    notional = amount * fill_price
                    fee = notional * (float(p_cfg.get("fee_bps", 0.0)) / 10000.0)
                    fills.append({"symbol": sym, "side": side, "amount": amount, "price": fill_price, "fee": fee})
                    impact_cost_total += abs(amount * (fill_price - float(price)))
                    orders_filled += 1
                    trade_rows.append(
                        {
                            "ts_signal_utc": ts.isoformat(),
                            "ts_exec_utc": ts_exec.isoformat(),
                            "symbol": sym,
                            "side": side,
                            "delta_w": dw,
                            "amount": amount,
                            "price": fill_price,
                            "notional": notional,
                            "fee": fee,
                            "slippage_bps": slippage_bps,
                            "order_type": order_type,
                            "reduce_only": bool(intent.get("reduce_only", False)),
                            "position_side": intent.get("position_side"),
                            "equity_before": equity_ts,
                            "order_id": order_id,
                        }
                    )
                    order_events.append(
                        {
                            "ts_event_utc": ts_exec.isoformat(),
                            "event": "filled",
                            "order_id": order_id,
                            "symbol": sym,
                            "side": side,
                            "order_type": order_type,
                            "limit_price": price,
                            "fill_price": fill_price,
                            "fill_amount": amount,
                            "amount_original": amount,
                            "amount_remaining": 0.0,
                            "ts_signal_utc": ts.isoformat(),
                            "ts_submit_utc": ts_exec.isoformat(),
                        }
                    )
                else:
                    expire_i = exec_i + max(1, limit_ttl_bars)
                    pending_orders.append(
                        {
                            "order_id": order_id,
                            "symbol": sym,
                            "side": side,
                            "price": price,
                            "amount_original": amount,
                            "remaining": amount,
                            "delta_w": dw,
                            "order_type": order_type,
                            "activate_i": exec_i,
                            "expire_i": expire_i,
                            "ts_signal_utc": ts.isoformat(),
                            "ts_submit_utc": ts_exec.isoformat(),
                            "reduce_only": bool(intent.get("reduce_only", False)),
                            "position_side": intent.get("position_side"),
                            "equity_before": equity_ts,
                        }
                    )
                    order_events.append(
                        {
                            "ts_event_utc": ts_exec.isoformat(),
                            "event": "submitted",
                            "order_id": order_id,
                            "symbol": sym,
                            "side": side,
                            "order_type": order_type,
                            "limit_price": price,
                            "amount_original": amount,
                            "amount_remaining": amount,
                            "ts_signal_utc": ts.isoformat(),
                            "ts_submit_utc": ts_exec.isoformat(),
                            "fillable_next_bar": int(fillable),
                        }
                    )

            if fills:
                pending_fills_by_ts.setdefault(ts_exec, []).extend(fills)

        equity = acct.equity_from_prices(prices_ts)
        ts_day = ts.date()
        if day_key is None or ts_day != day_key:
            day_key = ts_day
            day_start_equity = equity
        if day_start_equity and day_start_equity > 0:
            dloss = (day_start_equity - equity) / day_start_equity
        else:
            dloss = 0.0
        rows.append(
            {
                "ts_utc": ts.isoformat(),
                "equity": equity,
                "cash": acct.cash,
                "day_start_equity": day_start_equity,
                "dloss": dloss,
            }
        )
        if riskoff_active:
            riskoff_active_bars += 1
        if rebalance_now and regime_delev_active:
            regime_delev_active_bars += 1
        done_steps = i + 1
        if done_steps % progress_emit_every == 0 or done_steps >= total_progress_steps:
            emit_progress(done_steps, ts)

    # Finalize pending orders/fills at the last timestamp, then append final mark-to-market row.
    final_ts = common_idx[-1]
    final_prices = {s: float(df.loc[final_ts, "close"]) for s, df in data.items()}
    process_pending_orders(len(common_idx) - 1, final_ts, final_prices)
    if pending_orders:
        for order in list(pending_orders):
            if order["remaining"] > 0:
                orders_unfilled += 1
                orders_canceled_timeout += 1
                order_events.append(
                    {
                        "ts_event_utc": final_ts.isoformat(),
                        "event": "canceled_end_of_backtest",
                        "order_id": order["order_id"],
                        "symbol": order["symbol"],
                        "side": order["side"],
                        "order_type": order["order_type"],
                        "limit_price": order["price"],
                        "amount_original": order["amount_original"],
                        "amount_remaining": order["remaining"],
                        "ts_signal_utc": order["ts_signal_utc"],
                        "ts_submit_utc": order["ts_submit_utc"],
                    }
                )
                pending_orders.remove(order)
    final_due_fills = pending_fills_by_ts.pop(final_ts, [])
    if final_due_fills:
        acct.apply_fills(final_due_fills)
    if funding_enabled and acct.positions:
        # Final funding accrual for last interval using latest known rate.
        prev_ts = common_idx[-2] if len(common_idx) >= 2 else final_ts
        dt_hours = max(0.0, (final_ts - prev_ts).total_seconds() / 3600.0)
        if dt_hours > 0:
            interval_scale = dt_hours / 8.0
            for sym, qty in list(acct.positions.items()):
                if abs(qty) <= 1e-12:
                    continue
                px = float(final_prices.get(sym, 0.0))
                if px <= 0:
                    continue
                fs = funding_series_by_symbol.get(sym)
                if fs is not None and not fs.empty:
                    hist = fs.loc[:final_ts]
                    rate = float(hist.iloc[-1]) if not hist.empty else funding_const_rate
                else:
                    rate = funding_const_rate
                if rate == 0.0:
                    continue
                signed_notional = qty * px
                funding_fee = -signed_notional * rate * interval_scale
                acct.cash -= funding_fee
                funding_fee_total += funding_fee
                funding_events += 1
    final_equity = acct.equity_from_prices(final_prices)
    final_day = final_ts.date()
    if day_key is None or final_day != day_key:
        day_key = final_day
        day_start_equity = final_equity
    if day_start_equity and day_start_equity > 0:
        final_dloss = (day_start_equity - final_equity) / day_start_equity
    else:
        final_dloss = 0.0
    rows.append(
        {
            "ts_utc": final_ts.isoformat(),
            "equity": final_equity,
            "cash": acct.cash,
            "day_start_equity": day_start_equity,
            "dloss": final_dloss,
        }
    )
    emit_progress(total_progress_steps, final_ts)

    out = pd.DataFrame(rows)
    if out.empty:
        raise RuntimeError("Backtest produced no equity rows")
    ensure_dir(args.out)
    out.to_csv(args.out, index=False)

    if trade_rows:
        trade_df = pd.DataFrame(trade_rows)
        ensure_dir(args.trades)
        trade_df.to_csv(args.trades, index=False)
    if order_events:
        order_events_path = str(Path(args.trades).with_name(Path(args.trades).stem + "_orders.csv"))
        order_events_df = pd.DataFrame(order_events)
        ensure_dir(order_events_path)
        order_events_df.to_csv(order_events_path, index=False)

    ts_series = pd.to_datetime(out["ts_utc"])
    equity_series = out["equity"]
    returns = equity_series.pct_change().dropna()
    max_dd = (equity_series / equity_series.cummax() - 1.0).min()
    if len(ts_series) >= 2:
        total_hours = (ts_series.iloc[-1] - ts_series.iloc[0]).total_seconds() / 3600.0
    else:
        total_hours = 0.0
    ann_factor = 365.0 * 24.0
    if total_hours > 0 and equity_series.iloc[0] > 0:
        ann_return = (equity_series.iloc[-1] / equity_series.iloc[0]) ** (ann_factor / total_hours) - 1.0
    else:
        ann_return = 0.0
    if returns.std() and returns.std() > 0:
        sharpe_legacy = returns.mean() / returns.std() * math.sqrt(ann_factor)
    else:
        sharpe_legacy = 0.0
    periods_per_year = (365.0 * 24.0 * 60.0) / max(1, tf_minutes)
    if returns.std() and returns.std() > 0:
        sharpe_correct = returns.mean() / returns.std() * math.sqrt(periods_per_year)
    else:
        sharpe_correct = 0.0
    # Keep `sharpe` as the canonical metric consumed by optimizers.
    sharpe = sharpe_correct

    total_bars = max(1, len(common_idx) - 1)
    riskoff_active_ratio = riskoff_active_bars / float(total_bars)
    riskoff_first = riskoff_first_ts.isoformat() if riskoff_first_ts is not None else ""
    riskoff_last = riskoff_last_ts.isoformat() if riskoff_last_ts is not None else ""
    fill_rate = (orders_filled / float(orders_attempted)) if orders_attempted > 0 else 0.0

    metrics_text = (
        f"start_utc={ts_series.iloc[0].isoformat()}\n"
        f"end_utc={ts_series.iloc[-1].isoformat()}\n"
        f"equity_start={equity_series.iloc[0]:.6f}\n"
        f"equity_end={equity_series.iloc[-1]:.6f}\n"
        f"annualized_return={ann_return:.6f}\n"
        f"max_drawdown={max_dd:.6f}\n"
        f"sharpe={sharpe:.6f}\n"
        f"sharpe_legacy={sharpe_legacy:.6f}\n"
        f"sharpe_correct={sharpe_correct:.6f}\n"
        f"riskoff_active_ratio={riskoff_active_ratio:.6f}\n"
        f"riskoff_toggle_count={riskoff_toggle_count}\n"
        f"riskoff_first_ts={riskoff_first}\n"
        f"riskoff_last_ts={riskoff_last}\n"
        f"regime_delev_enabled={int(bool(regime_delev_cfg.get('enabled', False)))}\n"
        f"regime_delev_active_bars={regime_delev_active_bars}\n"
        f"regime_delev_trigger_count={regime_delev_trigger_count}\n"
        f"orders_attempted={orders_attempted}\n"
        f"orders_filled={orders_filled}\n"
        f"orders_unfilled={orders_unfilled}\n"
        f"orders_partial={orders_partial}\n"
        f"orders_canceled_timeout={orders_canceled_timeout}\n"
        f"order_fill_rate={fill_rate:.6f}\n"
        f"impact_enabled={int(impact_enabled)}\n"
        f"impact_base_bps={impact_base_bps:.6f}\n"
        f"impact_exponent={impact_exponent:.6f}\n"
        f"impact_cost_total={impact_cost_total:.6f}\n"
        f"funding_enabled={int(funding_enabled)}\n"
        f"funding_fee_total={funding_fee_total:.6f}\n"
        f"funding_events={funding_events}\n"
        f"margin_enabled={int(margin_enabled)}\n"
        f"liquidated={int(liquidated)}\n"
        f"liquidation_count={liquidation_count}\n"
    )
    ensure_dir(args.metrics)
    with open(args.metrics, "w", encoding="utf-8") as f:
        f.write(metrics_text)

    try:
        import matplotlib.pyplot as plt

        plt.figure(figsize=(10, 4))
        plt.plot(ts_series, equity_series, linewidth=1.2)
        plt.title("Backtest Equity Curve")
        plt.xlabel("UTC Time")
        plt.ylabel("Equity")
        plt.tight_layout()
        ensure_dir(args.plot)
        plt.savefig(args.plot, dpi=150)
        plt.close()
    except Exception as e:
        print(f"Plot skipped: {e}")

    print(f"Backtest complete. Equity curve saved to {args.out}")
    if trade_rows:
        print(f"Trade log saved to {args.trades}")
    print(f"Metrics saved to {args.metrics}")
    print(f"Equity curve plot saved to {args.plot}")


if __name__ == "__main__":
    backtest()
