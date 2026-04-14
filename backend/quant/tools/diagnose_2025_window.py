#!/usr/bin/env python3
import argparse
import glob
import os
from pathlib import Path

import numpy as np
import pandas as pd


def _safe_symbol(sym: str) -> str:
    return sym.replace("/", "_").replace(":", "_")


def _load_price_series(symbol: str, cache_dir: str) -> pd.Series:
    patt = os.path.join(cache_dir, f"{_safe_symbol(symbol)}_*.csv")
    paths = sorted(glob.glob(patt))
    frames = []
    for p in paths:
        try:
            df = pd.read_csv(p)
        except Exception:
            continue
        if "ts" not in df.columns or "close" not in df.columns:
            continue
        df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["ts", "close"])
        if df.empty:
            continue
        frames.append(df[["ts", "close"]])
    if not frames:
        return pd.Series(dtype=float)
    all_df = pd.concat(frames, ignore_index=True).drop_duplicates(subset=["ts"]).sort_values("ts")
    return all_df.set_index("ts")["close"]


def _max_drawdown(s: pd.Series) -> float:
    if s.empty:
        return 0.0
    return float((s / s.cummax() - 1.0).min())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--equity", required=True)
    ap.add_argument("--trades", required=True)
    ap.add_argument("--btc-symbol", default="BTC/USDT:USDT")
    ap.add_argument("--cache-dir", default="logs/cache")
    ap.add_argument("--out-dir", default="logs/audit_runs/phase2/diagnose_2025")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    eq = pd.read_csv(args.equity)
    eq["ts_utc"] = pd.to_datetime(eq["ts_utc"], utc=True)
    eq = eq.sort_values("ts_utc").set_index("ts_utc")
    eq["ret"] = eq["equity"].pct_change().fillna(0.0)
    eq["month"] = eq.index.to_period("M").astype(str)

    monthly = eq.groupby("month").agg(
        equity_start=("equity", "first"),
        equity_end=("equity", "last"),
        bars=("equity", "count"),
    )
    monthly["return"] = monthly["equity_end"] / monthly["equity_start"] - 1.0
    monthly["max_drawdown"] = [ _max_drawdown(eq.loc[eq["month"] == m, "equity"]) for m in monthly.index ]
    monthly = monthly.reset_index()
    monthly.to_csv(out_dir / "monthly_breakdown.csv", index=False)

    btc = _load_price_series(args.btc_symbol, args.cache_dir)
    if not btc.empty:
        btc = btc.reindex(eq.index).ffill()
        btc_ret = btc.pct_change().fillna(0.0)
        lookback = 56  # 14 days on 6h bars
        btc_mom = btc.pct_change(lookback)
        btc_vol = btc_ret.rolling(lookback).std() * np.sqrt(365 * 4)
        vol_thr = float(btc_vol.quantile(0.7))

        trend = np.where(btc_mom > 0.02, "up", np.where(btc_mom < -0.02, "down", "flat"))
        vol = np.where(btc_vol > vol_thr, "highvol", "lowvol")
        regime = pd.Series(trend, index=eq.index).astype(str) + "_" + pd.Series(vol, index=eq.index).astype(str)
        eq["regime"] = regime
        eq["btc_ret"] = btc_ret

        reg = eq.dropna(subset=["regime"]).groupby("regime").agg(
            bars=("ret", "count"),
            ret_sum=("ret", "sum"),
            ret_mean=("ret", "mean"),
            ret_std=("ret", "std"),
        )
        reg["sharpe_like"] = reg["ret_mean"] / reg["ret_std"].replace(0, np.nan)
        reg = reg.sort_values("ret_sum")
        reg.to_csv(out_dir / "regime_breakdown.csv")

        corr_all = float(eq["ret"].corr(eq["btc_ret"]))
        down_mask = eq["btc_ret"] < 0
        if down_mask.any():
            downside_beta = float(np.cov(eq.loc[down_mask, "ret"], eq.loc[down_mask, "btc_ret"])[0, 1] / np.var(eq.loc[down_mask, "btc_ret"]))
        else:
            downside_beta = float("nan")
    else:
        corr_all = float("nan")
        downside_beta = float("nan")

    tr = pd.read_csv(args.trades)
    tr["ts_exec_utc"] = pd.to_datetime(tr["ts_exec_utc"], utc=True)
    tr["signed_qty"] = np.where(tr["side"] == "buy", tr["amount"], -tr["amount"])
    tr["signed_cash"] = np.where(tr["side"] == "buy", -tr["notional"], tr["notional"])
    tr["fee"] = pd.to_numeric(tr["fee"], errors="coerce").fillna(0.0)

    sym_rows = []
    if not tr.empty:
        end_ts = eq.index.max()
        for sym, g in tr.groupby("symbol"):
            qty = float(g["signed_qty"].sum())
            cash = float(g["signed_cash"].sum())
            fees = float(g["fee"].sum())
            px = _load_price_series(sym, args.cache_dir)
            if px.empty:
                end_px = float(g["price"].iloc[-1])
            else:
                px = px.loc[:end_ts]
                end_px = float(px.iloc[-1]) if not px.empty else float(g["price"].iloc[-1])
            mtm = qty * end_px
            pnl = cash + mtm - fees
            sym_rows.append(
                {
                    "symbol": sym,
                    "turnover_notional": float(g["notional"].abs().sum()),
                    "fees": fees,
                    "net_qty_end": qty,
                    "end_price": end_px,
                    "mtm_end": mtm,
                    "pnl_total": pnl,
                }
            )
    sym_df = pd.DataFrame(sym_rows).sort_values("pnl_total")
    sym_df.to_csv(out_dir / "symbol_pnl_breakdown.csv", index=False)

    lines = []
    lines.append("# 2025 退化窗口归因")
    lines.append("")
    lines.append(f"- 区间: {eq.index.min().isoformat()} -> {eq.index.max().isoformat()}")
    lines.append(f"- 起始净值: {eq['equity'].iloc[0]:.6f}")
    lines.append(f"- 期末净值: {eq['equity'].iloc[-1]:.6f}")
    lines.append(f"- 区间收益: {eq['equity'].iloc[-1] / eq['equity'].iloc[0] - 1.0:.6%}")
    lines.append(f"- 最大回撤: {_max_drawdown(eq['equity']):.6%}")
    lines.append(f"- 策略-BTC相关性: {corr_all:.6f}")
    lines.append(f"- 下跌beta(仅BTC下跌bar): {downside_beta:.6f}")
    lines.append("")
    lines.append("## 最差月份")
    for _, r in monthly.sort_values("return").head(3).iterrows():
        lines.append(f"- {r['month']}: return={r['return']:.4%}, max_dd={r['max_drawdown']:.4%}")
    lines.append("")
    if not sym_df.empty:
        lines.append("## 标的贡献（最差/最好）")
        for _, r in sym_df.head(3).iterrows():
            lines.append(f"- WORST {r['symbol']}: pnl_total={r['pnl_total']:.6f}, fees={r['fees']:.6f}")
        for _, r in sym_df.tail(3).iloc[::-1].iterrows():
            lines.append(f"- BEST {r['symbol']}: pnl_total={r['pnl_total']:.6f}, fees={r['fees']:.6f}")
    lines.append("")
    lines.append("产物:")
    lines.append("- monthly_breakdown.csv")
    lines.append("- regime_breakdown.csv (if btc cache available)")
    lines.append("- symbol_pnl_breakdown.csv")
    (out_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"Diagnosis artifacts saved to: {out_dir}")


if __name__ == "__main__":
    main()
