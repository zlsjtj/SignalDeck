from typing import Any, Callable, Dict, List, Optional

OrderAttemptCallback = Callable[[Dict[str, Any]], None]


def mid_price(ex, symbol: str) -> float:
    ob = ex.fetch_order_book(symbol, limit=5)
    bid = ob["bids"][0][0] if ob["bids"] else None
    ask = ob["asks"][0][0] if ob["asks"] else None
    if bid and ask:
        return (bid + ask) / 2
    if bid:
        return bid
    if ask:
        return ask
    raise RuntimeError(f"No orderbook for {symbol}")


def _min_cost_for_symbol(ex, symbol: str) -> float:
    try:
        market = ex.market(symbol)
    except Exception:
        return 0.0
    limits = market.get("limits") or {}
    cost = limits.get("cost") or {}
    return float(cost.get("min") or 0.0)


def _min_amount_for_symbol(ex, symbol: str) -> float:
    try:
        market = ex.market(symbol)
    except Exception:
        return 0.0
    limits = market.get("limits") or {}
    amount = limits.get("amount") or {}
    return float(amount.get("min") or 0.0)


def _apply_precision(ex, symbol: str, amount: float, price: float):
    try:
        amount = float(ex.amount_to_precision(symbol, amount))
    except Exception:
        pass
    try:
        price = float(ex.price_to_precision(symbol, price))
    except Exception:
        pass
    return amount, price


def _classify_failure_reason(message: str) -> str:
    msg = message.lower()
    if "minnotional" in msg or "min notional" in msg:
        return "minNotional"
    if "lot size" in msg or "step size" in msg or "min_qty" in msg:
        return "lot_size"
    if "insufficient" in msg and "balance" in msg:
        return "insufficient_balance"
    if "price filter" in msg or "tick size" in msg:
        return "price_filter"
    if "reduceonly" in msg or "reduce only" in msg:
        return "reduce_only_rejected"
    if "position side" in msg or "position mode" in msg:
        return "position_mode_mismatch"
    if "timeout" in msg:
        return "timeout"
    if "network" in msg or "dns" in msg:
        return "network"
    return "unknown"


def _emit_attempt(callback: Optional[OrderAttemptCallback], payload: Dict[str, Any]) -> None:
    if callback is None:
        return
    try:
        callback(payload)
    except Exception:
        # Diagnostics must never block trading flow.
        return


def place_orders(
    ex,
    paper: bool,
    order_intents: List[Dict],
    equity_usdt: float,
    limit_offset_bps: float,
    min_order_usdt: float,
    order_type: str = "limit",
    position_mode: str = "oneway",
    logger=None,
    on_order_attempt: Optional[OrderAttemptCallback] = None,
):
    """
    order_intents: list of dicts: {symbol, delta_w, reduce_only?, position_side?}
    delta_w: target weight delta (positive = buy, negative = sell)
    order notional = abs(delta_w) * equity_usdt
    """
    fills: List[Dict] = []
    for intent in order_intents:
        sym = intent["symbol"]
        dw = float(intent["delta_w"])
        reduce_only = bool(intent.get("reduce_only", False))
        position_side = intent.get("position_side")
        side = "buy" if dw > 0 else "sell"

        notion = abs(dw) * equity_usdt
        min_cost = max(min_order_usdt, _min_cost_for_symbol(ex, sym))
        base_attempt = {
            "symbol": sym,
            "side": side,
            "delta_w": dw,
            "notional": notion,
            "reduce_only": reduce_only,
            "position_side": position_side,
            "order_type": order_type,
        }

        if notion < min_cost:
            if logger:
                logger.info(f"Skip {sym} delta_w={dw:.4f} notion={notion:.2f} < min_cost={min_cost:.2f}")
            _emit_attempt(
                on_order_attempt,
                {
                    **base_attempt,
                    "status": "skipped",
                    "failure_reason": "minNotional",
                    "detail": f"notional={notion:.8f} < min_cost={min_cost:.8f}",
                },
            )
            continue

        px = mid_price(ex, sym)

        amount = notion / px
        off = limit_offset_bps / 10000.0
        price = px * (1 - off) if side == "buy" else px * (1 + off)
        amount, price = _apply_precision(ex, sym, amount, price)

        min_amount = _min_amount_for_symbol(ex, sym)
        if amount < min_amount:
            if logger:
                logger.info(f"Skip {sym} amount={amount:.8f} < min_amount={min_amount}")
            _emit_attempt(
                on_order_attempt,
                {
                    **base_attempt,
                    "status": "skipped",
                    "amount": amount,
                    "price": price,
                    "failure_reason": "lot_size",
                    "detail": f"amount={amount:.8f} < min_amount={min_amount:.8f}",
                },
            )
            continue

        if amount * price < min_cost:
            if logger:
                logger.info(f"Skip {sym} cost={amount*price:.2f} < min_cost={min_cost:.2f}")
            _emit_attempt(
                on_order_attempt,
                {
                    **base_attempt,
                    "status": "skipped",
                    "amount": amount,
                    "price": price,
                    "failure_reason": "minNotional",
                    "detail": f"cost={amount * price:.8f} < min_cost={min_cost:.8f}",
                },
            )
            continue

        if paper:
            ro = " reduceOnly" if reduce_only else ""
            ps = f" positionSide={position_side}" if position_side else ""
            if logger:
                logger.info(
                    f"[PAPER]{ro}{ps} {side} {sym} amount={amount:.6f} price={price:.4f} notion={notion:.2f}"
                )
            fill = {
                "symbol": sym,
                "side": side,
                "amount": amount,
                "price": price,
                "reduce_only": reduce_only,
                "position_side": position_side,
            }
            fills.append(fill)
            _emit_attempt(
                on_order_attempt,
                {
                    **base_attempt,
                    "status": "filled_paper",
                    "amount": amount,
                    "price": price,
                    "exchange_response": {"mode": "paper"},
                },
            )
            continue

        params: Dict[str, Any] = {}
        if reduce_only:
            params["reduceOnly"] = True

        if position_mode == "hedge" and position_side:
            if ex.id in ("binanceusdm", "binance"):
                params["positionSide"] = position_side
            elif ex.id == "bybit":
                params["positionIdx"] = 1 if position_side == "LONG" else 2
            elif ex.id == "okx":
                params["posSide"] = position_side.lower()

        try:
            if order_type == "limit":
                resp = ex.create_order(sym, "limit", side, amount, price, params)
            else:
                resp = ex.create_order(sym, "market", side, amount, None, params)
        except Exception as exc:
            message = str(exc)
            _emit_attempt(
                on_order_attempt,
                {
                    **base_attempt,
                    "status": "failed",
                    "amount": amount,
                    "price": price,
                    "params": params,
                    "error": message,
                    "failure_reason": _classify_failure_reason(message),
                },
            )
            raise

        if logger:
            logger.info(f"[LIVE] placed {side} {sym} amount={amount:.6f} price={price:.4f}")
        _emit_attempt(
            on_order_attempt,
            {
                **base_attempt,
                "status": "submitted",
                "amount": amount,
                "price": price,
                "params": params,
                "exchange_response": {
                    "id": str(resp.get("id", "")) if isinstance(resp, dict) else "",
                    "status": str(resp.get("status", "")) if isinstance(resp, dict) else "",
                    "type": str(resp.get("type", "")) if isinstance(resp, dict) else "",
                    "raw": resp if isinstance(resp, dict) else {"repr": repr(resp)},
                },
            },
        )

    return fills
