import argparse
import copy
import csv
import os
import random
import subprocess
import sys

import yaml


def read_metrics(path: str):
    if not os.path.exists(path):
        return None
    out = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or "=" not in line:
                continue
            k, v = line.split("=", 1)
            out[k] = v
    try:
        return {
            "ann": float(out.get("annualized_return", 0.0)),
            "mdd": float(out.get("max_drawdown", 0.0)),
            "sharpe": float(out.get("sharpe", 0.0)),
        }
    except Exception:
        return None


def read_trade_stats(trades_path: str, equity_path: str):
    trades = {"count": 0, "total_notional": 0.0, "fees": 0.0, "turnover": 0.0}
    if os.path.exists(trades_path):
        with open(trades_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                trades["count"] += 1
                try:
                    trades["total_notional"] += float(row.get("notional", 0.0))
                except Exception:
                    pass
                try:
                    trades["fees"] += float(row.get("fee", 0.0))
                except Exception:
                    pass
    avg_equity = 0.0
    if os.path.exists(equity_path):
        eq_sum = 0.0
        eq_n = 0
        with open(equity_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    eq_sum += float(row.get("equity", 0.0))
                    eq_n += 1
                except Exception:
                    continue
        if eq_n > 0:
            avg_equity = eq_sum / eq_n
    if avg_equity > 0:
        trades["turnover"] = trades["total_notional"] / avg_equity
    return trades


def run_backtest(py_exe, cfg_path, start, end, tag):
    out_dir = os.path.join("logs", "opt_runs")
    os.makedirs(out_dir, exist_ok=True)
    metrics = os.path.join(out_dir, f"metrics_{tag}.txt")
    equity = os.path.join(out_dir, f"equity_{tag}.csv")
    trades = os.path.join(out_dir, f"trades_{tag}.csv")
    cmd = [
        py_exe,
        "statarb/backtest.py",
        "--start",
        start,
        "--end",
        end,
        "--config",
        cfg_path,
        "--metrics",
        metrics,
        "--out",
        equity,
        "--trades",
        trades,
        "--plot",
        os.path.join(out_dir, "skip.png"),
    ]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
    return read_metrics(metrics), read_trade_stats(trades, equity)


def set_nested(cfg, path, value):
    cur = cfg
    for key in path[:-1]:
        if key not in cur or not isinstance(cur[key], dict):
            cur[key] = {}
        cur = cur[key]
    cur[path[-1]] = value


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="config_2025_bch_bnb_btc_equal_combo.yaml")
    ap.add_argument("--out", default="results/opt_results_combo_slim.csv")
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--max_trials", type=int, default=200)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--tol_ret", type=float, default=0.05)
    ap.add_argument("--tol_dd", type=float, default=0.05)
    args = ap.parse_args()

    random.seed(args.seed)
    py_exe = sys.executable
    os.makedirs("results", exist_ok=True)
    os.makedirs("logs/opt_configs", exist_ok=True)

    with open(args.base, "r", encoding="utf-8") as f:
        base = yaml.safe_load(f)

    candidates = []
    long_qs = [0.34, 0.67, 1.0]
    score_thresholds = [None, 0.0]
    risk_modes = [
        {"enabled": False},
        {"enabled": True, "mode": "benchmark_mom", "lookback_hours": 240, "threshold": -0.02, "risk_off_scale": 0.0},
        {"enabled": True, "mode": "benchmark_mom", "lookback_hours": 240, "threshold": 0.0, "risk_off_scale": 0.0},
    ]
    inv_vols = [
        {"enabled": False},
        {"enabled": True, "lookback_hours": 120},
    ]
    vol_targets = [
        {"enabled": False},
        {
            "enabled": True,
            "target_annual_vol": 0.35,
            "lookback_hours": 120,
            "min_leverage": 0.5,
            "max_leverage": 1.2,
        },
    ]
    rebalance_minutes = [10080, 20160]
    drift_thresholds = [0.0, 0.02, 0.05, 0.08, 0.1]

    for long_q in long_qs:
        for score_th in score_thresholds:
            for risk_cfg in risk_modes:
                for inv_cfg in inv_vols:
                    for vol_cfg in vol_targets:
                        for reb_min in rebalance_minutes:
                            for drift_th in drift_thresholds:
                                candidates.append(
                                    {
                                        "long_quantile": long_q,
                                        "score_threshold": score_th,
                                        "risk_off": risk_cfg,
                                        "inv_vol": inv_cfg,
                                        "vol_target": vol_cfg,
                                        "rebalance_every_minutes": reb_min,
                                        "drift_threshold": drift_th,
                                    }
                                )

    fieldnames = [
        "trial",
        "score",
        "ann_2023",
        "mdd_2023",
        "sharpe_2023",
        "trades_2023",
        "notional_2023",
        "fees_2023",
        "turnover_2023",
        "ann_2024",
        "mdd_2024",
        "sharpe_2024",
        "trades_2024",
        "notional_2024",
        "fees_2024",
        "turnover_2024",
        "ann_2025",
        "mdd_2025",
        "sharpe_2025",
        "trades_2025",
        "notional_2025",
        "fees_2025",
        "turnover_2025",
        "config_path",
        "params",
    ]

    best = None
    rows = []
    baseline = {
        "long_quantile": 1.0,
        "score_threshold": None,
        "risk_off": {"enabled": False},
        "inv_vol": {"enabled": False},
        "vol_target": {"enabled": False},
        "rebalance_every_minutes": 20160,
        "drift_threshold": 0.0,
    }
    priority = {
        "long_quantile": 1.0,
        "score_threshold": None,
        "risk_off": {"enabled": False},
        "inv_vol": {"enabled": False},
        "vol_target": {"enabled": False},
        "rebalance_every_minutes": 20160,
        "drift_threshold": 0.05,
    }
    random.shuffle(candidates)
    candidates = [baseline, priority] + candidates
    if args.max_trials and args.max_trials > 0:
        candidates = candidates[: args.max_trials]

    for i, cand in enumerate(candidates):
        cfg = copy.deepcopy(base)
        cfg["rebalance_every_minutes"] = cand["rebalance_every_minutes"]
        set_nested(cfg, ["strategy", "long_quantile"], cand["long_quantile"])
        if cand["score_threshold"] is None:
            if "score_threshold" in cfg.get("strategy", {}):
                cfg["strategy"].pop("score_threshold", None)
        else:
            set_nested(cfg, ["strategy", "score_threshold"], cand["score_threshold"])
        set_nested(cfg, ["strategy", "risk_off"], cand["risk_off"])
        set_nested(cfg, ["strategy", "inv_vol"], cand["inv_vol"])
        set_nested(cfg, ["strategy", "vol_target"], cand["vol_target"])
        set_nested(cfg, ["portfolio", "drift_threshold"], cand["drift_threshold"])

        cfg_path = os.path.join("logs", "opt_configs", f"cfg_{i}.yaml")
        with open(cfg_path, "w", encoding="utf-8") as cf:
            yaml.safe_dump(cfg, cf, allow_unicode=False, sort_keys=False)

        m2023, t2023 = run_backtest(py_exe, cfg_path, "2023-01-01", "2023-12-31", f"{i}_2023")
        if not m2023:
            continue
        ann_2023 = m2023["ann"]
        mdd_2023 = abs(m2023["mdd"])
        if ann_2023 < 1.322159 * (1 - args.tol_ret):
            continue
        if mdd_2023 > 0.262046 * (1 + args.tol_dd):
            continue

        m2024, t2024 = run_backtest(py_exe, cfg_path, "2024-01-01", "2024-12-31", f"{i}_2024")
        if not m2024:
            continue
        ann_2024 = m2024["ann"]
        mdd_2024 = abs(m2024["mdd"])
        if ann_2024 < 1.275071 * (1 - args.tol_ret):
            continue
        if mdd_2024 > 0.415700 * (1 + args.tol_dd):
            continue

        m2025, t2025 = run_backtest(py_exe, cfg_path, "2025-01-01", "2025-12-31", f"{i}_2025")
        if not m2025:
            continue

        score = (
            m2025["ann"]
            - 0.8 * abs(m2025["mdd"])
            + 20 * m2025["sharpe"]
            - 0.2 * float(t2025.get("turnover", 0.0))
        )
        row = {
            "trial": i,
            "score": score,
            "ann_2023": m2023["ann"],
            "mdd_2023": m2023["mdd"],
            "sharpe_2023": m2023["sharpe"],
            "trades_2023": t2023["count"],
            "notional_2023": t2023["total_notional"],
            "fees_2023": t2023["fees"],
            "turnover_2023": t2023["turnover"],
            "ann_2024": m2024["ann"],
            "mdd_2024": m2024["mdd"],
            "sharpe_2024": m2024["sharpe"],
            "trades_2024": t2024["count"],
            "notional_2024": t2024["total_notional"],
            "fees_2024": t2024["fees"],
            "turnover_2024": t2024["turnover"],
            "ann_2025": m2025["ann"],
            "mdd_2025": m2025["mdd"],
            "sharpe_2025": m2025["sharpe"],
            "trades_2025": t2025["count"],
            "notional_2025": t2025["total_notional"],
            "fees_2025": t2025["fees"],
            "turnover_2025": t2025["turnover"],
            "config_path": cfg_path,
            "params": cand,
        }
        rows.append(row)
        if best is None or score > best["score"]:
            best = row

    rows = sorted(rows, key=lambda r: r["score"], reverse=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    top_path = os.path.join("results", "opt_results_combo_slim_top10.csv")
    with open(top_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows[: max(args.top, 1)]:
            w.writerow(r)

    if best:
        with open(best["config_path"], "r", encoding="utf-8") as f:
            best_cfg = yaml.safe_load(f)
        best_path = "config_2025_bch_bnb_btc_equal_combo_optimized.yaml"
        with open(best_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(best_cfg, f, allow_unicode=False, sort_keys=False)
        print("Best config:", best_path)
    else:
        print("No valid configs met constraints.")


if __name__ == "__main__":
    main()
