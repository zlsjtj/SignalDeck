from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, List
import csv
import os


def _safe_float(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


@dataclass
class PaperAccount:
    cash: float
    fee_bps: float = 0.0
    positions: Dict[str, float] = field(default_factory=dict)

    def apply_fills(self, fills: List[Dict]) -> None:
        for f in fills:
            sym = f.get("symbol")
            side = f.get("side")
            amount = _safe_float(f.get("amount"))
            price = _safe_float(f.get("price"))
            if not sym or side not in ("buy", "sell") or amount <= 0 or price <= 0:
                continue

            cost = amount * price
            fee = _safe_float(f.get("fee"))
            if fee <= 0:
                fee = cost * (self.fee_bps / 10000.0)
            if side == "buy":
                self.cash -= cost + fee
                self.positions[sym] = self.positions.get(sym, 0.0) + amount
            else:
                self.cash += cost - fee
                self.positions[sym] = self.positions.get(sym, 0.0) - amount
                if abs(self.positions[sym]) < 1e-12:
                    del self.positions[sym]

    def get_mark_prices(self, ex, symbols: List[str]) -> Dict[str, float]:
        prices: Dict[str, float] = {}
        for sym in symbols:
            try:
                t = ex.fetch_ticker(sym)
                prices[sym] = _safe_float(t.get("last") or t.get("mark") or t.get("close"))
            except Exception:
                prices[sym] = 0.0
        return prices

    def equity_from_prices(self, prices: Dict[str, float]) -> float:
        equity = self.cash
        for sym, qty in self.positions.items():
            equity += qty * _safe_float(prices.get(sym))
        return equity

    def weights_from_prices(self, prices: Dict[str, float], equity: float) -> Dict[str, float]:
        if equity == 0:
            return {s: 0.0 for s in prices.keys()}
        w: Dict[str, float] = {}
        for sym in prices.keys():
            qty = self.positions.get(sym, 0.0)
            notional = qty * _safe_float(prices.get(sym))
            w[sym] = notional / equity
        return w

    def format_positions(self, prices: Dict[str, float], min_notional: float = 5.0) -> str:
        items: List[str] = []
        for sym in sorted(self.positions.keys()):
            qty = self.positions.get(sym, 0.0)
            px = _safe_float(prices.get(sym))
            notional = qty * px
            if abs(notional) < min_notional:
                continue
            items.append(f"{sym} qty={qty:.6f} notion={notional:.2f}")
        return ", ".join(items) if items else "none"

    def log_equity_curve(self, path: str, equity: float, ts: str = None) -> None:
        if ts is None:
            ts = datetime.now(timezone.utc).isoformat()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        new_file = not os.path.exists(path)
        with open(path, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new_file:
                w.writerow(["ts_utc", "equity", "cash"])
            w.writerow([ts, f"{equity:.8f}", f"{self.cash:.8f}"])
