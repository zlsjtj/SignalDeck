import argparse
import copy
import csv
import os
import random
import subprocess
import sys

import yaml


BASELINE_2023_ANN = 1.344547
BASELINE_2023_MDD = 0.266348
BASELINE_2024_ANN = 1.280797
BASELINE_2024_MDD = 0.416919


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
            "sharpe_correct": float(out.get("sharpe_correct", out.get("sharpe", 0.0))),
            "riskoff_active_ratio": float(out.get("riskoff_active_ratio", 0.0)),
            "riskoff_toggle_count": float(out.get("riskoff_toggle_count", 0.0)),
        }
    except Exception:
        return None


def read_trade_stats(trades_path: str):
    trades = {"count": 0, "total_notional": 0.0, "fees": 0.0, "turnover": 0.0}
    if not os.path.exists(trades_path):
        return trades
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
            try:
                eq_before = float(row.get("equity_before", 0.0))
            except Exception:
                eq_before = 0.0
            if eq_before > 0:
                try:
                    trades["turnover"] += float(row.get("notional", 0.0)) / eq_before
                except Exception:
                    pass
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
    return read_metrics(metrics), read_trade_stats(trades)


def set_nested(cfg, path, value):
    cur = cfg
    for key in path[:-1]:
        if key not in cur or not isinstance(cur[key], dict):
            cur[key] = {}
        cur = cur[key]
    cur[path[-1]] = value


def score_from_metrics(m, trades):
    sharpe = float(m.get("sharpe_correct", m.get("sharpe", 0.0)))
    return (
        float(m["ann"])
        - 1.2 * abs(float(m["mdd"]))
        + 10.0 * sharpe
        - 0.2 * float(trades.get("turnover", 0.0))
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="config_2025_bch_bnb_btc_equal_combo_baseline_v2.yaml")
    ap.add_argument("--out", default="results/riskoff_opt_results.csv")
    ap.add_argument("--top", type=int, default=10)
    ap.add_argument("--max_trials", type=int, default=400)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--tol_ret", type=float, default=0.05)
    ap.add_argument("--tol_dd", type=float, default=0.02)
    ap.add_argument("--resume", action="store_true")
    args = ap.parse_args()

    random.seed(args.seed)
    py_exe = sys.executable
    os.makedirs("results", exist_ok=True)
    os.makedirs("logs/opt_configs", exist_ok=True)

    with open(args.base, "r", encoding="utf-8") as f:
        base = yaml.safe_load(f)

    risk_scales = [0.75, 0.80, 0.85, 0.90, 0.95]
    sma_bars = [40, 80, 160, 320]
    hysteresis_vals = [0.0, 0.01, 0.02, 0.03]
    cooldown_bars = [0, 8, 20, 40]

    candidates = []
    for scale in risk_scales:
        for bars in sma_bars:
            for hysteresis in hysteresis_vals:
                for cooldown in cooldown_bars:
                    candidates.append(
                        {
                            "risk_off_scale": scale,
                            "btc_sma_bars": bars,
                            "hysteresis": hysteresis,
                            "cooldown_bars": cooldown,
                        }
                    )

    random.shuffle(candidates)
    if args.max_trials and args.max_trials > 0:
        candidates = candidates[: args.max_trials]

    fieldnames = [
        "trial",
        "score",
        "ann_2023",
        "mdd_2023",
        "sharpe_2023",
        "sharpe_correct_2023",
        "trades_2023",
        "notional_2023",
        "fees_2023",
        "turnover_2023",
        "riskoff_active_ratio_2023",
        "riskoff_toggle_count_2023",
        "ann_2024",
        "mdd_2024",
        "sharpe_2024",
        "sharpe_correct_2024",
        "trades_2024",
        "notional_2024",
        "fees_2024",
        "turnover_2024",
        "riskoff_active_ratio_2024",
        "riskoff_toggle_count_2024",
        "ann_2025",
        "mdd_2025",
        "sharpe_2025",
        "sharpe_correct_2025",
        "trades_2025",
        "notional_2025",
        "fees_2025",
        "turnover_2025",
        "riskoff_active_ratio_2025",
        "riskoff_toggle_count_2025",
        "config_path",
        "params",
    ]

    rows = []
    done_trials = set()
    if args.resume and os.path.exists(args.out):
        with open(args.out, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
                try:
                    done_trials.add(int(row.get("trial", -1)))
                except Exception:
                    pass
    best = None
    if rows:
        best = max(rows, key=lambda r: float(r.get("score", -1e9)))
    file_exists = os.path.exists(args.out)
    write_header = not file_exists or not args.resume
    out_f = open(args.out, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(out_f, fieldnames=fieldnames)
    if write_header:
        writer.writeheader()
        out_f.flush()
    for i, cand in enumerate(candidates):
        if i in done_trials:
            continue
        cfg = copy.deepcopy(base)
        set_nested(cfg, ["strategy", "risk_off"], {
            "enabled": True,
            "mode": "btc_trend",
            "benchmark_symbol": cfg.get("strategy", {}).get("score", {}).get(
                "benchmark_symbol", cfg.get("symbols", [""])[0]
            ),
            "btc_sma_bars": int(cand["btc_sma_bars"]),
            "hysteresis": float(cand["hysteresis"]),
            "cooldown_bars": int(cand["cooldown_bars"]),
            "risk_off_scale": float(cand["risk_off_scale"]),
        })
        set_nested(cfg, ["strategy", "inv_vol", "enabled"], False)
        set_nested(cfg, ["strategy", "vol_target", "enabled"], False)
        set_nested(cfg, ["strategy", "abs_mom_filter", "enabled"], False)

        cfg_path = os.path.join("logs", "opt_configs", f"riskoff_{i}.yaml")
        with open(cfg_path, "w", encoding="utf-8") as cf:
            yaml.safe_dump(cfg, cf, allow_unicode=False, sort_keys=False)

        m2024, t2024 = run_backtest(py_exe, cfg_path, "2024-01-01", "2024-12-31", f"riskoff_{i}_2024")
        if not m2024:
            continue
        ann_2024 = m2024["ann"]
        mdd_2024 = abs(m2024["mdd"])
        if ann_2024 < BASELINE_2024_ANN * (1 - args.tol_ret):
            continue
        if mdd_2024 > BASELINE_2024_MDD * (1 + args.tol_dd):
            continue

        m2023, t2023 = run_backtest(py_exe, cfg_path, "2023-01-01", "2023-12-31", f"riskoff_{i}_2023")
        if not m2023:
            continue
        ann_2023 = m2023["ann"]
        mdd_2023 = abs(m2023["mdd"])
        if ann_2023 < BASELINE_2023_ANN * (1 - args.tol_ret):
            continue
        if mdd_2023 > BASELINE_2023_MDD * (1 + args.tol_dd):
            continue

        m2025, t2025 = run_backtest(py_exe, cfg_path, "2025-01-01", "2025-12-31", f"riskoff_{i}_2025")
        if not m2025:
            continue

        score_2024 = score_from_metrics(m2024, t2024)
        score_2025 = score_from_metrics(m2025, t2025)
        score = 0.3 * score_2024 + 0.7 * score_2025

        row = {
            "trial": i,
            "score": score,
            "ann_2023": m2023["ann"],
            "mdd_2023": m2023["mdd"],
            "sharpe_2023": m2023["sharpe"],
            "sharpe_correct_2023": m2023.get("sharpe_correct", m2023["sharpe"]),
            "trades_2023": t2023["count"],
            "notional_2023": t2023["total_notional"],
            "fees_2023": t2023["fees"],
            "turnover_2023": t2023["turnover"],
            "riskoff_active_ratio_2023": m2023.get("riskoff_active_ratio", 0.0),
            "riskoff_toggle_count_2023": m2023.get("riskoff_toggle_count", 0.0),
            "ann_2024": m2024["ann"],
            "mdd_2024": m2024["mdd"],
            "sharpe_2024": m2024["sharpe"],
            "sharpe_correct_2024": m2024.get("sharpe_correct", m2024["sharpe"]),
            "trades_2024": t2024["count"],
            "notional_2024": t2024["total_notional"],
            "fees_2024": t2024["fees"],
            "turnover_2024": t2024["turnover"],
            "riskoff_active_ratio_2024": m2024.get("riskoff_active_ratio", 0.0),
            "riskoff_toggle_count_2024": m2024.get("riskoff_toggle_count", 0.0),
            "ann_2025": m2025["ann"],
            "mdd_2025": m2025["mdd"],
            "sharpe_2025": m2025["sharpe"],
            "sharpe_correct_2025": m2025.get("sharpe_correct", m2025["sharpe"]),
            "trades_2025": t2025["count"],
            "notional_2025": t2025["total_notional"],
            "fees_2025": t2025["fees"],
            "turnover_2025": t2025["turnover"],
            "riskoff_active_ratio_2025": m2025.get("riskoff_active_ratio", 0.0),
            "riskoff_toggle_count_2025": m2025.get("riskoff_toggle_count", 0.0),
            "config_path": cfg_path,
            "params": cand,
        }
        rows.append(row)
        writer.writerow(row)
        out_f.flush()
        if best is None or score > float(best["score"]):
            best = row
    out_f.close()

    rows = sorted(rows, key=lambda r: float(r["score"]), reverse=True) if rows else []
    top_path = "results/riskoff_opt_results_top10.csv"
    with open(top_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows[: max(args.top, 1)]:
            w.writerow(r)

    if best:
        with open(best["config_path"], "r", encoding="utf-8") as f:
            best_cfg = yaml.safe_load(f)
        best_path = "config_2025_bch_bnb_btc_equal_combo_baseline_v2_riskoff_best.yaml"
        with open(best_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(best_cfg, f, allow_unicode=False, sort_keys=False)
        print("Best config:", best_path)
    else:
        print("No valid configs met constraints.")


if __name__ == "__main__":
    main()
