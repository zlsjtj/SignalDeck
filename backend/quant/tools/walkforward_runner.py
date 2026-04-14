#!/usr/bin/env python3
import argparse
import csv
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import yaml


def _read_metrics(path: Path) -> dict:
    out = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or "=" not in line:
                continue
            k, v = line.split("=", 1)
            out[k] = v
    return out


def _set_nested(d: dict, dotted_key: str, value):
    cur = d
    parts = dotted_key.split(".")
    for p in parts[:-1]:
        if p not in cur or not isinstance(cur[p], dict):
            cur[p] = {}
        cur = cur[p]
    cur[parts[-1]] = value


@dataclass
class SplitResult:
    train_start: str
    train_end: str
    test_start: str
    test_end: str
    best_param: str
    train_equity_end: float
    test_equity_end: float
    test_annualized_return: float
    test_max_drawdown: float
    test_sharpe: float


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config_market.yaml")
    ap.add_argument("--start-year", type=int, required=True)
    ap.add_argument("--end-year", type=int, required=True)
    ap.add_argument("--train-years", type=int, default=1)
    ap.add_argument("--test-years", type=int, default=1)
    ap.add_argument("--param-key", default="strategy.score.mom_lookback")
    ap.add_argument("--param-values", default="8,12,16")
    ap.add_argument("--out-dir", default="logs/audit_runs/walkforward")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(args.config, "r", encoding="utf-8") as f:
        base_cfg = yaml.safe_load(f)

    values = [v.strip() for v in args.param_values.split(",") if v.strip()]
    py = sys.executable
    bt = "statarb/backtest.py"
    rows: list[SplitResult] = []

    first_train_start = args.start_year
    last_test_end = args.end_year
    y = first_train_start
    while y + args.train_years + args.test_years - 1 <= last_test_end:
        train_start_y = y
        train_end_y = y + args.train_years - 1
        test_start_y = train_end_y + 1
        test_end_y = test_start_y + args.test_years - 1

        train_start = f"{train_start_y}-01-01"
        train_end = f"{train_end_y}-12-31"
        test_start = f"{test_start_y}-01-01"
        test_end = f"{test_end_y}-12-31"

        best_param = None
        best_train_equity = float("-inf")
        for v in values:
            cfg = dict(base_cfg)
            cfg = yaml.safe_load(yaml.safe_dump(cfg))
            try:
                vv = int(v)
            except ValueError:
                try:
                    vv = float(v)
                except ValueError:
                    vv = v
            _set_nested(cfg, args.param_key, vv)
            cfg_path = out_dir / f"cfg_train_{train_start_y}_{train_end_y}_{args.param_key.replace('.', '_')}_{v}.yaml"
            with cfg_path.open("w", encoding="utf-8") as f:
                yaml.safe_dump(cfg, f, sort_keys=False, allow_unicode=True)
            mpath = out_dir / f"metrics_train_{train_start_y}_{train_end_y}_{v}.txt"
            cmd = [
                py,
                bt,
                "--start",
                train_start,
                "--end",
                train_end,
                "--config",
                str(cfg_path),
                "--out",
                str(out_dir / f"equity_train_{train_start_y}_{train_end_y}_{v}.csv"),
                "--trades",
                str(out_dir / f"trades_train_{train_start_y}_{train_end_y}_{v}.csv"),
                "--metrics",
                str(mpath),
                "--plot",
                str(out_dir / f"plot_train_{train_start_y}_{train_end_y}_{v}.png"),
            ]
            subprocess.run(cmd, check=True, cwd=os.getcwd(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            mt = _read_metrics(mpath)
            eq = float(mt.get("equity_end", "nan"))
            if eq > best_train_equity:
                best_train_equity = eq
                best_param = v

        cfg = yaml.safe_load(yaml.safe_dump(base_cfg))
        try:
            best_v = int(best_param)
        except ValueError:
            try:
                best_v = float(best_param)
            except ValueError:
                best_v = best_param
        _set_nested(cfg, args.param_key, best_v)
        cfg_path = out_dir / f"cfg_test_{test_start_y}_{test_end_y}_{args.param_key.replace('.', '_')}_{best_param}.yaml"
        with cfg_path.open("w", encoding="utf-8") as f:
            yaml.safe_dump(cfg, f, sort_keys=False, allow_unicode=True)
        mpath = out_dir / f"metrics_test_{test_start_y}_{test_end_y}_{best_param}.txt"
        cmd = [
            py,
            bt,
            "--start",
            test_start,
            "--end",
            test_end,
            "--config",
            str(cfg_path),
            "--out",
            str(out_dir / f"equity_test_{test_start_y}_{test_end_y}_{best_param}.csv"),
            "--trades",
            str(out_dir / f"trades_test_{test_start_y}_{test_end_y}_{best_param}.csv"),
            "--metrics",
            str(mpath),
            "--plot",
            str(out_dir / f"plot_test_{test_start_y}_{test_end_y}_{best_param}.png"),
        ]
        subprocess.run(cmd, check=True, cwd=os.getcwd(), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        mt = _read_metrics(mpath)
        rows.append(
            SplitResult(
                train_start=train_start,
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
                best_param=str(best_param),
                train_equity_end=best_train_equity,
                test_equity_end=float(mt.get("equity_end", "nan")),
                test_annualized_return=float(mt.get("annualized_return", "nan")),
                test_max_drawdown=float(mt.get("max_drawdown", "nan")),
                test_sharpe=float(mt.get("sharpe", "nan")),
            )
        )
        y += args.test_years

    summary = out_dir / "summary.csv"
    with summary.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "train_start",
                "train_end",
                "test_start",
                "test_end",
                "best_param",
                "train_equity_end",
                "test_equity_end",
                "test_annualized_return",
                "test_max_drawdown",
                "test_sharpe",
            ]
        )
        for r in rows:
            w.writerow(
                [
                    r.train_start,
                    r.train_end,
                    r.test_start,
                    r.test_end,
                    r.best_param,
                    f"{r.train_equity_end:.6f}",
                    f"{r.test_equity_end:.6f}",
                    f"{r.test_annualized_return:.6f}",
                    f"{r.test_max_drawdown:.6f}",
                    f"{r.test_sharpe:.6f}",
                ]
            )
    print(f"Walk-forward summary saved: {summary}")


if __name__ == "__main__":
    main()
