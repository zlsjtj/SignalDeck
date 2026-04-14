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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config_market.yaml")
    ap.add_argument("--start", required=True)
    ap.add_argument("--end", required=True)
    ap.add_argument("--participation", default="1.0,0.2,0.05")
    ap.add_argument("--impact-bps", default="0,10,20")
    ap.add_argument("--impact-exp", type=float, default=0.5)
    ap.add_argument("--out-dir", default="logs/audit_runs/rmid02_capacity")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(args.config, "r", encoding="utf-8") as f:
        base_cfg = yaml.safe_load(f)

    ps = [float(x.strip()) for x in args.participation.split(",") if x.strip()]
    bs = [float(x.strip()) for x in args.impact_bps.split(",") if x.strip()]
    py = sys.executable
    bt = "statarb/backtest.py"
    rows = []

    for p in ps:
        for b in bs:
            cfg = yaml.safe_load(yaml.safe_dump(base_cfg))
            cfg["backtest_max_participation_rate"] = p
            cfg["backtest_impact_enabled"] = b > 0
            cfg["backtest_impact_base_bps"] = b
            cfg["backtest_impact_exponent"] = args.impact_exp
            slug = f"p{p}_b{b}".replace(".", "_")
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
                    "participation": p,
                    "impact_base_bps": b,
                    "equity_end": mt.get("equity_end", ""),
                    "annualized_return": mt.get("annualized_return", ""),
                    "max_drawdown": mt.get("max_drawdown", ""),
                    "sharpe": mt.get("sharpe", ""),
                    "order_fill_rate": mt.get("order_fill_rate", ""),
                    "impact_cost_total": mt.get("impact_cost_total", ""),
                }
            )

    summary = out_dir / "summary.csv"
    with summary.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "participation",
                "impact_base_bps",
                "equity_end",
                "annualized_return",
                "max_drawdown",
                "sharpe",
                "order_fill_rate",
                "impact_cost_total",
            ],
        )
        w.writeheader()
        w.writerows(rows)
    print(f"Capacity/impact summary saved: {summary}")


if __name__ == "__main__":
    main()
