#!/usr/bin/env python3
import argparse
import csv
import os
import subprocess
import sys
from pathlib import Path

import yaml


def _read_metrics(path: Path) -> dict:
    out = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if "=" not in line:
                continue
            k, v = line.split("=", 1)
            out[k] = v
    return out


def _chunks(seq, sizes):
    out = []
    for s in sizes:
        out.append(seq[:s])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config_market.yaml")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--universes", default="3,5,all")
    ap.add_argument("--timeframes", default="4h,6h,12h")
    ap.add_argument("--out-dir", default="logs/audit_runs/robustness_matrix")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(args.config, "r", encoding="utf-8") as f:
        base_cfg = yaml.safe_load(f)

    syms = list(base_cfg.get("symbols", []))
    uni_sizes = []
    for u in [x.strip() for x in args.universes.split(",") if x.strip()]:
        if u == "all":
            uni_sizes.append(len(syms))
        else:
            uni_sizes.append(int(u))
    uni_sets = _chunks(syms, uni_sizes)
    tfs = [x.strip() for x in args.timeframes.split(",") if x.strip()]

    py = sys.executable
    bt = "statarb/backtest.py"
    rows = []
    for usz, u_syms in zip(uni_sizes, uni_sets):
        for tf in tfs:
            cfg = yaml.safe_load(yaml.safe_dump(base_cfg))
            cfg["symbols"] = u_syms
            cfg["timeframe"] = tf
            slug = f"u{usz}_{tf}"
            cfg_path = out_dir / f"cfg_{slug}.yaml"
            with cfg_path.open("w", encoding="utf-8") as f:
                yaml.safe_dump(cfg, f, sort_keys=False, allow_unicode=True)
            mpath = out_dir / f"metrics_{slug}.txt"
            cmd = [
                py,
                bt,
                "--start",
                args.start,
                "--end",
                args.end,
                "--config",
                str(cfg_path),
                "--out",
                str(out_dir / f"equity_{slug}.csv"),
                "--trades",
                str(out_dir / f"trades_{slug}.csv"),
                "--metrics",
                str(mpath),
                "--plot",
                str(out_dir / f"plot_{slug}.png"),
            ]
            subprocess.run(cmd, check=True, cwd=os.getcwd(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            mt = _read_metrics(mpath)
            rows.append(
                {
                    "universe_size": usz,
                    "timeframe": tf,
                    "equity_end": mt.get("equity_end", ""),
                    "annualized_return": mt.get("annualized_return", ""),
                    "max_drawdown": mt.get("max_drawdown", ""),
                    "sharpe": mt.get("sharpe", ""),
                    "order_fill_rate": mt.get("order_fill_rate", ""),
                }
            )

    summary = out_dir / "summary.csv"
    with summary.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "universe_size",
                "timeframe",
                "equity_end",
                "annualized_return",
                "max_drawdown",
                "sharpe",
                "order_fill_rate",
            ],
        )
        w.writeheader()
        w.writerows(rows)
    print(f"Robustness summary saved: {summary}")


if __name__ == "__main__":
    main()
