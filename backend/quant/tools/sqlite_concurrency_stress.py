#!/usr/bin/env python3
"""
SQLite concurrent write stress for multi-user strategy/backtest creation.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from db_store import SQLiteStore


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pctl(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * p))))
    return float(ordered[idx])


def _as_ms(seconds: float) -> float:
    return round(float(seconds) * 1000.0, 4)


def _write_markdown_report(path: Path, payload: Dict[str, Any]) -> None:
    lines = [
        "# SQLite Concurrency Stress Report",
        "",
        f"- Generated At (UTC): `{payload.get('generated_at')}`",
        f"- DB Path: `{payload.get('db_path')}`",
        f"- Threads: `{payload.get('threads')}`",
        f"- Ops/Thread: `{payload.get('ops_per_thread')}`",
        f"- Total Ops: `{payload.get('total_ops')}`",
        f"- Errors: `{payload.get('errors')}`",
        f"- Error Rate: `{payload.get('error_rate')}`",
        f"- Throughput Ops/s: `{payload.get('throughput_ops_per_sec')}`",
        f"- P95 Latency (ms): `{payload.get('latency_ms', {}).get('p95')}`",
        f"- Pass: `{payload.get('pass')}`",
        "",
        "## Operation Latency (ms)",
        "",
        f"- min: `{payload.get('latency_ms', {}).get('min')}`",
        f"- p50: `{payload.get('latency_ms', {}).get('p50')}`",
        f"- p95: `{payload.get('latency_ms', {}).get('p95')}`",
        f"- p99: `{payload.get('latency_ms', {}).get('p99')}`",
        f"- max: `{payload.get('latency_ms', {}).get('max')}`",
        "",
        "## Criteria",
        "",
        f"- max_error_rate: `{payload.get('criteria', {}).get('max_error_rate')}`",
        f"- max_p95_ms: `{payload.get('criteria', {}).get('max_p95_ms')}`",
        "",
        "## Verdict",
        "",
        "Pass" if payload.get("pass") else "Fail",
        "",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run concurrent strategy/backtest write stress on SQLiteStore")
    parser.add_argument("--db-path", default="logs/quant_stress.db", help="SQLite database path")
    parser.add_argument("--threads", type=int, default=8, help="worker thread count")
    parser.add_argument("--ops-per-thread", type=int, default=200, help="operations per worker")
    parser.add_argument("--users", type=int, default=16, help="logical user count")
    parser.add_argument("--seed", type=int, default=42, help="random seed")
    parser.add_argument("--max-error-rate", type=float, default=0.01, help="pass criteria: maximum error ratio")
    parser.add_argument("--max-p95-ms", type=float, default=120.0, help="pass criteria: maximum p95 latency in ms")
    parser.add_argument("--report-md", default="", help="optional markdown report output path")
    args = parser.parse_args()

    db_path = Path(args.db_path).expanduser()
    if not db_path.is_absolute():
        db_path = (Path.cwd() / db_path).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    rng = random.Random(int(args.seed))
    store = SQLiteStore(db_path)
    store.initialize()

    threads = max(1, int(args.threads))
    ops_per_thread = max(1, int(args.ops_per_thread))
    users = max(1, int(args.users))
    total_ops = threads * ops_per_thread

    lock = threading.Lock()
    latencies_ms: List[float] = []
    errors: List[str] = []

    def worker(worker_idx: int) -> None:
        local_rng = random.Random(rng.randint(1, 10**9))
        for op_idx in range(ops_per_thread):
            user_idx = (worker_idx + op_idx) % users
            owner = f"user_{user_idx:02d}"
            strategy_id = f"stress_strategy_{worker_idx}_{op_idx}"
            strategy_key = f"usr__{owner}__{strategy_id}"
            run_id = f"stress_bt_{worker_idx}_{op_idx}"
            started = time.perf_counter()
            try:
                record = {
                    "id": strategy_id,
                    "name": f"stress-{strategy_id}",
                    "status": "running" if (op_idx % 2 == 0) else "stopped",
                    "createdAt": _now_iso(),
                    "updatedAt": _now_iso(),
                    "owner": owner,
                    "config": {
                        "symbols": ["BTC/USDT:USDT"],
                        "timeframe": "1h",
                        "params": {
                            "alpha": local_rng.random(),
                            "beta": local_rng.uniform(-1.0, 1.0),
                            "enabled": bool(op_idx % 2 == 0),
                        },
                    },
                }
                store.upsert_strategy(strategy_key, owner, record)
                bt_record = {
                    "id": run_id,
                    "owner": owner,
                    "strategyId": strategy_id,
                    "strategyName": record["name"],
                    "symbol": "BTC/USDT:USDT",
                    "startAt": "2026-01-01",
                    "endAt": "2026-01-31",
                    "status": "finished",
                    "createdAt": _now_iso(),
                    "updatedAt": _now_iso(),
                    "initialCapital": 10000.0,
                    "metrics": {
                        "pnlTotal": local_rng.uniform(-1000.0, 1500.0),
                        "sharpe": local_rng.uniform(-1.0, 2.0),
                        "calmar": local_rng.uniform(-1.0, 2.0),
                        "maxDrawdown": local_rng.uniform(0.0, 0.5),
                    },
                }
                store.upsert_backtest(run_id, owner, bt_record)
                store.upsert_risk_state(
                    owner,
                    strategy_key,
                    {
                        "enabled": True,
                        "maxDrawdownPct": local_rng.uniform(0.05, 0.3),
                        "updatedAt": _now_iso(),
                        "triggered": [],
                    },
                )
                store.append_audit_log(
                    owner=owner,
                    action="stress.write",
                    entity="stress",
                    entity_id=run_id,
                    detail={"worker": worker_idx, "op": op_idx},
                )
            except Exception as exc:
                with lock:
                    errors.append(str(exc))
            finally:
                elapsed_ms = _as_ms(time.perf_counter() - started)
                with lock:
                    latencies_ms.append(elapsed_ms)

    started_all = time.perf_counter()
    workers = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(threads)]
    for t in workers:
        t.start()
    for t in workers:
        t.join()
    elapsed_total = max(time.perf_counter() - started_all, 1e-9)

    error_count = len(errors)
    error_rate = float(error_count) / float(total_ops)
    p95 = _pctl(latencies_ms, 0.95)
    payload: Dict[str, Any] = {
        "generated_at": _now_iso(),
        "db_path": str(db_path),
        "threads": threads,
        "ops_per_thread": ops_per_thread,
        "users": users,
        "total_ops": total_ops,
        "errors": error_count,
        "error_rate": round(error_rate, 6),
        "throughput_ops_per_sec": round(float(total_ops) / elapsed_total, 4),
        "latency_ms": {
            "min": min(latencies_ms) if latencies_ms else 0.0,
            "p50": _pctl(latencies_ms, 0.50),
            "p95": p95,
            "p99": _pctl(latencies_ms, 0.99),
            "max": max(latencies_ms) if latencies_ms else 0.0,
        },
        "criteria": {
            "max_error_rate": float(args.max_error_rate),
            "max_p95_ms": float(args.max_p95_ms),
        },
        "pass": bool(error_rate <= float(args.max_error_rate) and p95 <= float(args.max_p95_ms)),
        "error_samples": errors[:10],
    }

    report_md = str(args.report_md or "").strip()
    if report_md:
        report_path = Path(report_md).expanduser()
        if not report_path.is_absolute():
            report_path = (Path.cwd() / report_path).resolve()
        _write_markdown_report(report_path, payload)
        payload["report_md"] = str(report_path)

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if not payload["pass"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
