import argparse
import atexit
import math
import threading
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from statarb.account import get_equity_usdt, get_current_weights
from statarb.broker import make_exchange
from statarb.config import load_config
from statarb.data import fetch_universe
from statarb.diagnostics import RuntimeDiagnostics
from statarb.execution import place_orders
from statarb.factors import compute_scores
from statarb.logger import get_logger
from statarb.paper import PaperAccount
from statarb.portfolio import target_weights_from_scores
from statarb.risk import RiskState, daily_loss, drawdown, update_equity
from statarb.utils import utc_day_key


def build_order_intents(
    symbols,
    current_w,
    target_w,
    drift_threshold: float = 0.0,
    force_rebalance: bool = False,
):
    """
    Build order intents with reduce-only for flips.
    Each intent: {symbol, delta_w, reduce_only, position_side?}
    """
    intents = []
    threshold = 0.0 if force_rebalance else drift_threshold
    for sym in symbols:
        cw = float(current_w.get(sym, 0.0))
        tw = float(target_w.get(sym, 0.0))

        if abs(cw) < 1e-6 and abs(tw) < 1e-6:
            continue

        if cw == 0.0 or tw == 0.0 or cw * tw > 0:
            dw = tw - cw
            if abs(dw) > max(1e-6, threshold):
                reduce_only = (cw != 0.0 and dw * cw < 0.0)
                intent = {"symbol": sym, "delta_w": dw, "reduce_only": reduce_only}
                if reduce_only:
                    intent["position_side"] = "LONG" if cw > 0 else "SHORT"
                intents.append(intent)
        else:
            # Flip: close current (reduceOnly), then open new
            if abs(cw) > max(1e-6, threshold):
                intents.append(
                    {
                        "symbol": sym,
                        "delta_w": -cw,
                        "reduce_only": True,
                        "position_side": "LONG" if cw > 0 else "SHORT",
                    }
                )
            if abs(tw) > max(1e-6, threshold):
                intents.append(
                    {
                        "symbol": sym,
                        "delta_w": tw,
                        "reduce_only": False,
                        "position_side": "LONG" if tw > 0 else "SHORT",
                    }
                )
    return intents


def timeframe_to_minutes(tf: str) -> int:
    unit = tf[-1]
    val = int(tf[:-1])
    if unit == "m":
        return val
    if unit == "h":
        return val * 60
    if unit == "d":
        return val * 1440
    return 0


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


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_live_positions(raw_positions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for pos in raw_positions or []:
        symbol = str(pos.get("symbol") or "")
        if not symbol:
            continue
        qty_raw = (
            pos.get("contracts")
            if pos.get("contracts") is not None
            else pos.get("positionAmt")
        )
        qty = _safe_float(qty_raw, 0.0)
        side_raw = str(pos.get("side") or "").lower()
        if side_raw in {"long", "buy"}:
            side = "long"
        elif side_raw in {"short", "sell"}:
            side = "short"
        else:
            side = "long" if qty >= 0 else "short"
        qty_abs = abs(qty)
        avg_price = _safe_float(pos.get("entryPrice"), _safe_float(pos.get("avgPrice"), 0.0))
        unrealized = _safe_float(
            pos.get("unrealizedPnl"),
            _safe_float(pos.get("unrealizedProfit"), 0.0),
        )
        normalized.append(
            {
                "symbol": symbol,
                "side": side,
                "qty": qty_abs,
                "avg_price": avg_price,
                "unrealized_pnl": unrealized,
            }
        )
    return normalized


def _normalize_open_orders(raw_orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for order in raw_orders or []:
        normalized.append(
            {
                "id": str(order.get("id", "")),
                "symbol": str(order.get("symbol", "")),
                "side": str(order.get("side", "")),
                "type": str(order.get("type", "")),
                "status": str(order.get("status", "")),
                "qty": _safe_float(order.get("amount"), 0.0),
                "filled": _safe_float(order.get("filled"), 0.0),
                "price": _safe_float(order.get("price"), 0.0),
                "timestamp": order.get("datetime") or order.get("timestamp"),
            }
        )
    return normalized


def _probe_exchange_connectivity(
    ex,
    symbols: List[str],
    diagnostics: RuntimeDiagnostics,
    *,
    skip_private_probe: bool = False,
    skip_reason: str = "",
) -> Dict[str, Any]:
    now = _iso_now()
    status_balance: Dict[str, Any] = {"ok": False, "ts": now, "detail": ""}
    status_positions: Dict[str, Any] = {"ok": False, "ts": now, "detail": ""}
    status_orders: Dict[str, Any] = {"ok": False, "ts": now, "detail": ""}
    positions_snapshot: List[Dict[str, Any]] = []
    orders_snapshot: List[Dict[str, Any]] = []

    if skip_private_probe:
        detail = f"probe skipped: {skip_reason}".strip()
        if detail == "probe skipped:":
            detail = "probe skipped"
        status_balance = {"ok": True, "ts": now, "detail": detail}
        status_positions = {"ok": True, "ts": now, "detail": detail}
        status_orders = {"ok": True, "ts": now, "detail": detail}
        return {
            "balance": status_balance,
            "positions": status_positions,
            "open_orders": status_orders,
            "positions_snapshot": positions_snapshot,
            "open_orders_snapshot": orders_snapshot,
        }

    try:
        ex.fetch_balance()
        status_balance = {"ok": True, "ts": _iso_now(), "detail": ""}
    except Exception as exc:
        status_balance = {"ok": False, "ts": _iso_now(), "detail": str(exc)}
        diagnostics.note_api_error("fetch_balance", exc)

    try:
        raw_positions = ex.fetch_positions(symbols)
        positions_snapshot = _normalize_live_positions(raw_positions)
        status_positions = {"ok": True, "ts": _iso_now(), "detail": f"count={len(positions_snapshot)}"}
    except Exception as exc:
        status_positions = {"ok": False, "ts": _iso_now(), "detail": str(exc)}
        diagnostics.note_api_error("fetch_positions", exc)

    raw_orders: List[Dict[str, Any]] = []
    open_orders_fetch_ok = False
    try:
        raw_orders = ex.fetch_open_orders()
        open_orders_fetch_ok = True
    except Exception as first_exc:
        # Some exchanges require symbol parameter for fetch_open_orders.
        for sym in symbols[:10]:
            try:
                rows = ex.fetch_open_orders(sym)
                raw_orders.extend(rows or [])
                open_orders_fetch_ok = True
            except Exception:
                continue
        if not raw_orders:
            diagnostics.note_api_error("fetch_open_orders", first_exc)

    if open_orders_fetch_ok:
        orders_snapshot = _normalize_open_orders(raw_orders)
        status_orders = {"ok": True, "ts": _iso_now(), "detail": f"count={len(orders_snapshot)}"}
    else:
        status_orders = {"ok": False, "ts": _iso_now(), "detail": "fetch failed"}

    return {
        "balance": status_balance,
        "positions": status_positions,
        "open_orders": status_orders,
        "positions_snapshot": positions_snapshot,
        "open_orders_snapshot": orders_snapshot,
    }


def _paper_positions_snapshot(paper_account: Optional[PaperAccount], prices: Dict[str, float]) -> List[Dict[str, Any]]:
    if paper_account is None:
        return []
    out: List[Dict[str, Any]] = []
    for symbol, qty in sorted(paper_account.positions.items()):
        side = "long" if qty >= 0 else "short"
        px = _safe_float(prices.get(symbol), 0.0)
        out.append(
            {
                "symbol": symbol,
                "side": side,
                "qty": abs(float(qty)),
                "avg_price": None,
                "unrealized_pnl": None,
                "mark_price": px,
                "notional": abs(float(qty)) * px,
            }
        )
    return out


def main(config_path: str = "config.yaml"):
    project_root = Path(__file__).resolve().parent
    resolved_config_path = Path(config_path).expanduser()
    if not resolved_config_path.is_absolute():
        resolved_config_path = (project_root / resolved_config_path).resolve()
    cfg = load_config(str(resolved_config_path))
    log = get_logger()

    diagnostics = RuntimeDiagnostics(
        project_root=project_root,
        config_path=str(resolved_config_path),
        cfg_raw=cfg.raw,
        logger=log,
    )
    diagnostics.start_heartbeat()
    atexit.register(diagnostics.stop_heartbeat)

    keys = cfg.raw.get("keys", {})
    position_mode = str(cfg.raw.get("position_mode", "oneway"))
    has_private_credentials = bool(
        str(keys.get("apiKey", "")).strip() and str(keys.get("secret", "")).strip()
    )
    if not cfg.paper:
        if not has_private_credentials:
            raise ValueError("Live trading requires apiKey and secret in config.yaml")
    ex = make_exchange(
        cfg.exchange,
        keys.get("apiKey", ""),
        keys.get("secret", ""),
        keys.get("password", ""),
        position_mode=position_mode,
    )

    paper_account = None
    paper_log_path = None
    paper_account_lock: Optional[threading.Lock] = None
    paper_mtm_stop_event: Optional[threading.Event] = None
    paper_mtm_interval_seconds = 0.0
    paper_mtm_idle_write_seconds = 0.0
    if cfg.paper:
        initial_equity = float(cfg.raw.get("paper_equity_usdt", 10000))
        fee_bps = float(cfg.raw.get("portfolio", {}).get("fee_bps", 0.0))
        paper_account = PaperAccount(cash=initial_equity, fee_bps=fee_bps)
        paper_log_path = str(cfg.raw.get("paper_log_path", "logs/paper_equity.csv"))
        paper_account_lock = threading.Lock()
        paper_mtm_stop_event = threading.Event()
        atexit.register(paper_mtm_stop_event.set)
        paper_mtm_interval_seconds = _safe_float(
            cfg.raw.get("paper_mark_to_market_interval_seconds", 5.0),
            5.0,
        )
        if paper_mtm_interval_seconds < 0:
            paper_mtm_interval_seconds = 0.0
        paper_mtm_idle_write_seconds = _safe_float(
            cfg.raw.get(
                "paper_mark_to_market_idle_write_seconds",
                paper_mtm_interval_seconds,
            ),
            paper_mtm_interval_seconds,
        )
        if paper_mtm_idle_write_seconds < paper_mtm_interval_seconds:
            paper_mtm_idle_write_seconds = paper_mtm_interval_seconds
        try:
            with paper_account_lock:
                paper_account.log_equity_curve(paper_log_path, initial_equity)
        except Exception:
            log.exception("Failed to seed paper equity log")
        log.info(f"Paper account initialized: equity={initial_equity:.2f} fee_bps={fee_bps:.2f}")

    rs = RiskState(equity_peak=1.0, equity=1.0, day_start_equity=1.0)
    riskoff_active = False
    riskoff_cooldown_until_ts = 0.0
    day = utc_day_key()

    lookback_limit = max(200, cfg.lookback_hours + 50)

    log.info(f"Start StatArb | exchange={cfg.exchange} paper={cfg.paper} symbols={len(cfg.symbols)}")
    if cfg.paper and not has_private_credentials:
        log.info(
            "Paper mode without full private API credentials: "
            "skip balance/positions/open_orders connectivity probes."
        )
    log.info(
        f"Diagnostics enabled | heartbeat={diagnostics.heartbeat_minutes}m "
        f"snapshot={diagnostics.snapshot_path}"
    )

    if (
        cfg.paper
        and paper_account is not None
        and paper_log_path
        and paper_account_lock is not None
        and paper_mtm_stop_event is not None
        and paper_mtm_interval_seconds > 0
    ):
        idle_write_seconds = paper_mtm_idle_write_seconds or paper_mtm_interval_seconds

        def _paper_mark_to_market_loop() -> None:
            # Use a dedicated exchange instance to avoid cross-thread state on CCXT client.
            mtm_ex = make_exchange(
                cfg.exchange,
                keys.get("apiKey", ""),
                keys.get("secret", ""),
                keys.get("password", ""),
                position_mode=position_mode,
            )
            last_logged_equity: Optional[float] = None
            last_logged_cash: Optional[float] = None
            last_logged_at = 0.0

            while not paper_mtm_stop_event.is_set():
                loop_started = time.time()
                try:
                    live_prices = paper_account.get_mark_prices(mtm_ex, cfg.symbols)
                    with paper_account_lock:
                        live_equity = paper_account.equity_from_prices(live_prices)
                        live_cash = float(paper_account.cash)
                        changed = (
                            last_logged_equity is None
                            or abs(live_equity - last_logged_equity) > 1e-8
                            or last_logged_cash is None
                            or abs(live_cash - last_logged_cash) > 1e-8
                        )
                        stale = (loop_started - last_logged_at) >= idle_write_seconds
                        if changed or stale:
                            paper_account.log_equity_curve(paper_log_path, live_equity)
                            last_logged_equity = live_equity
                            last_logged_cash = live_cash
                            last_logged_at = loop_started
                except Exception:
                    log.exception("[PAPER] mark-to-market update failed")

                elapsed = time.time() - loop_started
                wait_seconds = max(0.5, paper_mtm_interval_seconds - elapsed)
                paper_mtm_stop_event.wait(wait_seconds)

        threading.Thread(
            target=_paper_mark_to_market_loop,
            daemon=True,
            name="paper-mark-to-market",
        ).start()
        log.info(
            f"[PAPER] mark-to-market enabled: interval={paper_mtm_interval_seconds:.2f}s "
            f"idle_write={idle_write_seconds:.2f}s "
            f"(config: paper_mark_to_market_interval_seconds)"
        )

    while True:
        try:
            new_day = utc_day_key()
            if new_day != day:
                day = new_day
                rs.day_start_equity = rs.equity
                log.info(f"New UTC day {day}, reset day_start_equity={rs.day_start_equity:.4f}")

            prices: Dict[str, float] = {}
            if cfg.paper:
                prices = paper_account.get_mark_prices(ex, cfg.symbols)
                diagnostics.record_tick_time()
                if paper_account_lock is not None:
                    with paper_account_lock:
                        equity = paper_account.equity_from_prices(prices)
                else:
                    equity = paper_account.equity_from_prices(prices)
            else:
                equity = get_equity_usdt(ex)
            if rs.day_start_equity == 1.0 and rs.equity_peak == 1.0:
                rs.day_start_equity = equity
                rs.equity_peak = equity
            update_equity(rs, equity)

            dd = drawdown(rs)
            dloss = daily_loss(rs)

            risk_section = cfg.raw.get("risk", {})
            if not isinstance(risk_section, dict):
                risk_section = {}
            max_daily_loss = float(risk_section.get("max_daily_loss", 1.0))
            if dloss > max_daily_loss:
                diagnostics.set_state(
                    "SAFE_MODE",
                    f"daily_loss {dloss:.6f} exceeds max_daily_loss {max_daily_loss:.6f}",
                )
                diagnostics.record_signal_evaluation(
                    conditions=[
                        {
                            "name": "daily_loss<=max_daily_loss",
                            "current": dloss,
                            "threshold": max_daily_loss,
                            "pass": False,
                        }
                    ],
                    entry_signal=False,
                    filter_reasons=["daily_loss_limit_reached"],
                    details={"equity": equity, "drawdown": dd, "intents": 0},
                )
                diagnostics.record_stop_levels(
                    sl=risk_section.get("stop_loss_pct", risk_section.get("stop_out_dd")),
                    tp=risk_section.get("take_profit_pct"),
                    ts=risk_section.get("trailing_stop_pct"),
                    price_source="mark_price",
                    note="daily loss protection active",
                )
                diagnostics.write_snapshot()
                log.warning(f"Daily loss {dloss:.2%} > {max_daily_loss:.2%}. STOP trading for today.")
                time.sleep(cfg.rebalance_every_minutes * 60)
                continue

            data = fetch_universe(ex, cfg.symbols, cfg.timeframe, limit=lookback_limit)
            diagnostics.record_data_snapshot(data)
            diagnostics.record_data_source_status("ok", "fetch_universe succeeded")

            s_cfg = cfg.raw["strategy"]
            score_cfg = s_cfg.get("score", {})
            tf_minutes = timeframe_to_minutes(cfg.timeframe)
            score = compute_scores(
                data,
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
                benchmark_symbol=str(score_cfg.get("benchmark_symbol", cfg.symbols[0] if cfg.symbols else "")),
                min_notional_usdt=float(score_cfg.get("min_notional_usdt", 0.0)),
                max_vol=float(score_cfg.get("max_vol", 0.0)),
            )

            p_cfg = cfg.raw["portfolio"]
            risk_off = False
            risk_off_reasons: List[str] = []
            score_threshold = s_cfg.get("score_threshold", None)
            max_score = float(score.max()) if not score.empty else 0.0
            score_threshold_pass = True
            if score_threshold is not None and not score.empty:
                if max_score < float(score_threshold):
                    risk_off = True
                    score_threshold_pass = False
                    risk_off_reasons.append(
                        f"max_score {max_score:.6f} < score_threshold {float(score_threshold):.6f}"
                    )

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
                if bench in data and tf_minutes > 0:
                    bars = max(3, int(rb_hours * 60 / tf_minutes))
                    closes = data[bench]["close"].tail(bars)
                    if len(closes) >= bars:
                        last = float(closes.iloc[-1])
                        first = float(closes.iloc[0])
                        if mode == "benchmark_sma":
                            sma = float(closes.mean())
                            if sma > 0 and last < sma * (1.0 - threshold):
                                risk_off = True
                                risk_off_reasons.append(
                                    f"risk_off benchmark_sma triggered: last={last:.6f}, sma={sma:.6f}, threshold={threshold:.6f}"
                                )
                        else:
                            if first > 0 and (last / first - 1.0) < threshold:
                                risk_off = True
                                risk_off_reasons.append(
                                    "risk_off benchmark_mom triggered: "
                                    f"ret={(last / first - 1.0):.6f}, threshold={threshold:.6f}"
                                )

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

            risk_cfg = s_cfg.get("risk_off", {})
            force_rebalance = False
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
                if bench in data and mode == "btc_trend":
                    closes = data[bench]["close"].tail(btc_sma_bars)
                    if len(closes) >= btc_sma_bars:
                        sma = float(closes.mean())
                        last = float(closes.iloc[-1])
                        if not riskoff_active and sma > 0 and last < sma:
                            desired = True
                        if riskoff_active and sma > 0 and last > sma * (1.0 + hysteresis):
                            desired = False
                now_ts = time.time()
                if cooldown_bars > 0 and now_ts < riskoff_cooldown_until_ts:
                    desired = riskoff_active
                if desired != riskoff_active:
                    riskoff_active = desired
                    force_rebalance = True
                    if cooldown_bars > 0:
                        riskoff_cooldown_until_ts = now_ts + cooldown_bars * tf_minutes * 60.0
                if riskoff_active and risk_off_scale < 1.0:
                    target_w = target_w * risk_off_scale

            abs_mom_cfg = s_cfg.get("abs_mom_filter", {})
            abs_mom_blocked: List[str] = []
            if abs_mom_cfg.get("enabled", False) and tf_minutes > 0 and not target_w.empty:
                lb_hours = float(abs_mom_cfg.get("lookback_hours", cfg.lookback_hours))
                min_ret = float(abs_mom_cfg.get("min_return", 0.0))
                for sym in list(target_w.index):
                    df = data.get(sym)
                    if df is None or df.empty:
                        target_w.loc[sym] = 0.0
                        abs_mom_blocked.append(sym)
                        continue
                    ret = abs_momentum_from_closes(df["close"], tf_minutes, lb_hours)
                    if ret < min_ret:
                        target_w.loc[sym] = 0.0
                        abs_mom_blocked.append(sym)
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
                    df = data.get(sym)
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
                if vb in data:
                    ann_vol = annualized_vol_from_closes(data[vb]["close"].tail(bars), tf_minutes)
                    target_vol = float(vol_cfg.get("target_annual_vol", 0.4))
                    if ann_vol > 0 and target_vol > 0:
                        scale = target_vol / ann_vol
                        scale = clamp(
                            scale,
                            float(vol_cfg.get("min_leverage", 0.3)),
                            float(vol_cfg.get("max_leverage", 1.5)),
                        )
                        target_w = target_w * scale

            if risk_off:
                scale = float(risk_cfg.get("risk_off_scale", 0.0))
                target_w = target_w * scale

            max_dd = float(risk_section.get("max_strategy_dd", 0.0))
            if max_dd and dd > 0:
                ratio = float(risk_section.get("dd_deleverage_ratio", 1.0))
                if dd >= max_dd:
                    scale = ratio
                else:
                    scale = ratio + (1.0 - ratio) * (1.0 - dd / max_dd)
                if scale < 1.0:
                    target_w = target_w * scale
                    log.warning(f"DD {dd:.2%} / {max_dd:.2%}, scale weights to {scale:.2f}x")

            stop_dd = float(risk_section.get("stop_out_dd", 0.0))
            cool_hours = float(risk_section.get("cool_off_hours", 0.0))
            now_ts = time.time()
            if rs.stop_out_until_ts and now_ts < rs.stop_out_until_ts:
                target_w = target_w * 0.0
                risk_off_reasons.append("stop_out cooldown active")
            elif stop_dd and dd >= stop_dd:
                if cool_hours > 0:
                    rs.stop_out_until_ts = now_ts + cool_hours * 3600.0
                target_w = target_w * 0.0
                risk_off_reasons.append(f"drawdown {dd:.6f} reached stop_out_dd {stop_dd:.6f}")

            if cfg.paper:
                if paper_account_lock is not None:
                    with paper_account_lock:
                        current_w = paper_account.weights_from_prices(prices, equity)
                else:
                    current_w = paper_account.weights_from_prices(prices, equity)
            else:
                current_w = get_current_weights(ex, cfg.symbols, equity)

            drift_threshold = float(p_cfg.get("drift_threshold", 0.0))
            intents = build_order_intents(
                cfg.symbols,
                current_w,
                target_w,
                drift_threshold=drift_threshold,
                force_rebalance=force_rebalance,
            )

            target_non_zero = int((target_w.abs() > 1e-8).sum()) if not target_w.empty else 0
            entry_signal = bool(
                (len(intents) > 0)
                and score_threshold_pass
                and (not risk_off)
                and (dloss <= max_daily_loss)
            )
            signal_conditions: List[Dict[str, Any]] = [
                {
                    "name": "daily_loss<=max_daily_loss",
                    "current": dloss,
                    "threshold": max_daily_loss,
                    "pass": dloss <= max_daily_loss,
                },
                {
                    "name": "max_score>=score_threshold",
                    "current": max_score,
                    "threshold": score_threshold,
                    "pass": score_threshold_pass,
                },
                {
                    "name": "risk_off==False",
                    "current": risk_off,
                    "threshold": False,
                    "pass": not risk_off,
                },
                {
                    "name": "target_non_zero>0",
                    "current": target_non_zero,
                    "threshold": 1,
                    "pass": target_non_zero > 0,
                },
                {
                    "name": "intents_count>0",
                    "current": len(intents),
                    "threshold": 1,
                    "pass": len(intents) > 0,
                },
            ]
            filter_reasons: List[str] = []
            if not score_threshold_pass and score_threshold is not None:
                filter_reasons.append("score_threshold_not_met")
            if risk_off_reasons:
                filter_reasons.extend(risk_off_reasons)
            if abs_mom_blocked:
                filter_reasons.append(
                    f"abs_mom_filtered_symbols={','.join(sorted(set(abs_mom_blocked))[:10])}"
                )
            if len(intents) == 0:
                filter_reasons.append("no_order_intents_after_filters")

            top_scores: List[Dict[str, Any]] = []
            if not score.empty:
                top_series = score.sort_values(ascending=False).head(5)
                for sym, val in top_series.items():
                    top_scores.append(
                        {
                            "symbol": str(sym),
                            "score": float(val),
                            "target_weight": float(target_w.get(sym, 0.0)),
                            "current_weight": float(current_w.get(sym, 0.0)),
                        }
                    )

            diagnostics.record_signal_evaluation(
                conditions=signal_conditions,
                entry_signal=entry_signal,
                filter_reasons=filter_reasons,
                details={
                    "equity": equity,
                    "drawdown": dd,
                    "daily_loss": dloss,
                    "risk_off_active": riskoff_active,
                    "force_rebalance": force_rebalance,
                    "top_scores": top_scores,
                },
            )
            diagnostics.record_stop_levels(
                sl=risk_section.get("stop_loss_pct", risk_section.get("stop_out_dd")),
                tp=risk_section.get("take_profit_pct"),
                ts=risk_section.get("trailing_stop_pct"),
                price_source="mark_price",
                note="drawdown/daily-loss controls are active; explicit SL/TP/TS depends on strategy config",
            )

            if risk_off or riskoff_active or (rs.stop_out_until_ts and now_ts < rs.stop_out_until_ts):
                diagnostics.set_state("SAFE_MODE", "risk-off or stop-out protection active")
            else:
                diagnostics.set_state("RUNNING", "normal loop")

            log.info(f"Equity={equity:.2f} DD={dd:.2%} DLoss={dloss:.2%} | trades={len(intents)}")
            e_cfg = cfg.raw["execution"]
            fills = place_orders(
                ex,
                paper=bool(cfg.paper),
                order_intents=intents,
                equity_usdt=float(equity),
                limit_offset_bps=float(e_cfg["limit_price_offset_bps"]),
                min_order_usdt=float(p_cfg["min_order_usdt"]),
                order_type=str(e_cfg["order_type"]),
                position_mode=position_mode,
                logger=log,
                on_order_attempt=diagnostics.record_order_attempt,
            )
            if cfg.paper:
                if prices is None:
                    prices = paper_account.get_mark_prices(ex, cfg.symbols)
                if paper_account_lock is not None:
                    with paper_account_lock:
                        if fills:
                            paper_account.apply_fills(fills)
                        equity_after = paper_account.equity_from_prices(prices)
                        paper_account.log_equity_curve(paper_log_path, equity_after)
                        pos_str = paper_account.format_positions(
                            prices, min_notional=float(p_cfg["min_order_usdt"])
                        )
                        paper_cash = float(paper_account.cash)
                        paper_positions_snapshot = _paper_positions_snapshot(paper_account, prices)
                else:
                    if fills:
                        paper_account.apply_fills(fills)
                    equity_after = paper_account.equity_from_prices(prices)
                    paper_account.log_equity_curve(paper_log_path, equity_after)
                    pos_str = paper_account.format_positions(
                        prices, min_notional=float(p_cfg["min_order_usdt"])
                    )
                    paper_cash = float(paper_account.cash)
                    paper_positions_snapshot = _paper_positions_snapshot(paper_account, prices)
                log.info(
                    f"[PAPER] equity={equity_after:.2f} cash={paper_cash:.2f} positions={pos_str}"
                )
                diagnostics.record_positions_and_orders(
                    paper_positions_snapshot,
                    [],
                )

            probe = _probe_exchange_connectivity(
                ex,
                cfg.symbols,
                diagnostics,
                skip_private_probe=bool(cfg.paper and not has_private_credentials),
                skip_reason="paper mode without apiKey+secret",
            )
            diagnostics.record_exchange_probe(
                balance=probe["balance"],
                positions=probe["positions"],
                open_orders=probe["open_orders"],
            )
            if not cfg.paper:
                diagnostics.record_positions_and_orders(
                    probe["positions_snapshot"],
                    probe["open_orders_snapshot"],
                )

            diagnostics.write_snapshot()

        except Exception as e:
            diagnostics.set_state("ERROR", f"loop error: {e}")
            diagnostics.record_data_source_status("error", str(e))
            diagnostics.record_exception("main_loop", e, traceback.format_exc())
            diagnostics.write_snapshot()
            log.exception(f"Loop error: {e}")

        time.sleep(cfg.rebalance_every_minutes * 60)


def _cli():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()
    main(config_path=args.config)


if __name__ == "__main__":
    _cli()
