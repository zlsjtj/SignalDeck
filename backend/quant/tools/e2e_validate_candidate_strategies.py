#!/usr/bin/env python3
import argparse
import csv
import time
from pathlib import Path
import sys

from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api_server import app


CANDIDATE_IDS = ["strategy_candidate_v009", "strategy_candidate_v010"]


def _to_float(val):
    try:
        return float(val)
    except Exception:
        return None


def main():
    parser = argparse.ArgumentParser(description="Validate candidate strategy API E2E backtest flow")
    parser.add_argument("--start-at", default="2025-01-01", help="Backtest start date (YYYY-MM-DD)")
    parser.add_argument("--end-at", default="2025-12-31", help="Backtest end date (YYYY-MM-DD)")
    args = parser.parse_args()

    out_dir = Path("logs/audit_runs/phase2/api_e2e")
    out_dir.mkdir(parents=True, exist_ok=True)
    report_rows = []

    with TestClient(app) as client:
        r = client.get("/api/strategies")
        r.raise_for_status()
        strategies = r.json()
        strategy_ids = {s.get("id") for s in strategies}
        missing = [sid for sid in CANDIDATE_IDS if sid not in strategy_ids]
        if missing:
            raise RuntimeError(f"Candidate strategy ids missing from /api/strategies: {missing}")

        for sid in CANDIDATE_IDS:
            strategy = next(s for s in strategies if s.get("id") == sid)
            symbols = ((strategy.get("config") or {}).get("symbols") or [])
            symbol = symbols[0] if symbols else "BTC/USDT:USDT"
            payload = {
                "strategyId": sid,
                "symbol": symbol,
                "startAt": args.start_at,
                "endAt": args.end_at,
                "initialCapital": 1000,
                "feeRate": 0.0006,
                "slippage": 0.0002,
            }
            cr = client.post("/api/backtests", json=payload)
            cr.raise_for_status()
            run = cr.json()
            run_id = run.get("id")
            if not run_id:
                raise RuntimeError(f"No run id returned for {sid}: {run}")
            metrics_path = Path(f"logs/api_backtest_{run_id}_metrics.txt")
            deadline = time.time() + 240
            while time.time() < deadline:
                if metrics_path.exists() and metrics_path.stat().st_size > 0:
                    break
                time.sleep(0.5)
            if not (metrics_path.exists() and metrics_path.stat().st_size > 0):
                raise RuntimeError(f"Backtest artifacts not ready for {sid}, run_id={run_id}")

            metrics = {}
            for line in metrics_path.read_text(encoding="utf-8").splitlines():
                if "=" not in line:
                    continue
                k, v = line.split("=", 1)
                metrics[k.strip()] = v.strip()

            detail = None
            status = "completed"
            dr = client.get(f"/api/backtests/{run_id}")
            if dr.status_code == 200:
                detail = dr.json()
                detail_status = str(detail.get("status") or "")
                if detail_status in {"completed", "failed"}:
                    status = detail_status
                else:
                    status = "completed_inferred"
                perf = detail.get("performance") or {}
            else:
                perf = {}

            report_rows.append(
                {
                    "strategy_id": sid,
                    "run_id": run_id,
                    "status": status,
                    "equity_end": _to_float(metrics.get("equity_end")) or _to_float(perf.get("netProfit")),
                    "annualized_return": _to_float(metrics.get("annualized_return")) or _to_float(perf.get("annualizedReturn")),
                    "max_drawdown": _to_float(metrics.get("max_drawdown")) or _to_float(perf.get("maxDrawdown")),
                    "sharpe": _to_float(metrics.get("sharpe")) or _to_float(perf.get("sharpe")),
                    "win_rate": _to_float(perf.get("winRate")),
                    "total_trades": _to_float(perf.get("totalTrades")),
                }
            )

    csv_path = out_dir / "candidate_api_e2e_summary.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "strategy_id",
                "run_id",
                "status",
                "equity_end",
                "annualized_return",
                "max_drawdown",
                "sharpe",
                "win_rate",
                "total_trades",
            ],
        )
        w.writeheader()
        w.writerows(report_rows)

    md_path = out_dir / "candidate_api_e2e_summary.md"
    lines = [
        "# Candidate Strategy API E2E Validation",
        "",
        f"Window: `{args.start_at}` to `{args.end_at}`",
        "",
        "Validated endpoints:",
        "- `GET /api/strategies`",
        "- `POST /api/backtests`",
        "- `GET /api/backtests/{run_id}`",
        "",
        "Results:",
    ]
    for row in report_rows:
        lines.append(
            f"- `{row['strategy_id']}` run `{row['run_id']}`: status={row['status']}, "
            f"annualized_return={row['annualized_return']}, max_drawdown={row['max_drawdown']}, sharpe={row['sharpe']}"
        )
    md_path.write_text("\n".join(lines), encoding="utf-8")

    print(f"ok: {csv_path}")
    print(f"ok: {md_path}")


if __name__ == "__main__":
    main()
