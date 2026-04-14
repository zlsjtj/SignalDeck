from dataclasses import dataclass

@dataclass
class RiskState:
    equity_peak: float = 1.0
    equity: float = 1.0
    day_start_equity: float = 1.0
    stop_out_until_ts: float = 0.0

def update_equity(r: RiskState, new_equity: float):
    r.equity = new_equity
    r.equity_peak = max(r.equity_peak, new_equity)

def drawdown(r: RiskState) -> float:
    if r.equity_peak <= 0:
        return 0.0
    return (r.equity_peak - r.equity) / r.equity_peak

def daily_loss(r: RiskState) -> float:
    if r.day_start_equity <= 0:
        return 0.0
    return (r.day_start_equity - r.equity) / r.day_start_equity
