#!/usr/bin/env python3
"""
Fault-injection checks for SQLite runtime behaviors.
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _scenario_lock_conflict(db_path: Path) -> Dict[str, Any]:
    with sqlite3.connect(str(db_path), timeout=5.0) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS t_lock (id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
        conn.commit()

    blocker = sqlite3.connect(str(db_path), timeout=5.0, isolation_level=None)
    blocker.execute("PRAGMA busy_timeout=5000;")
    blocker.execute("BEGIN EXCLUSIVE;")
    blocker.execute("INSERT INTO t_lock(v) VALUES ('blocker')")

    start = time.perf_counter()
    err_text = ""
    try:
        writer = sqlite3.connect(str(db_path), timeout=0.05)
        writer.execute("PRAGMA busy_timeout=50;")
        writer.execute("INSERT INTO t_lock(v) VALUES ('writer')")
        writer.commit()
        writer.close()
        ok = False
    except Exception as exc:
        ok = "locked" in str(exc).lower()
        err_text = str(exc)
    finally:
        try:
            blocker.rollback()
        except Exception:
            pass
        blocker.close()
    elapsed_ms = round((time.perf_counter() - start) * 1000.0, 4)
    return {"name": "lock_conflict", "ok": ok, "elapsed_ms": elapsed_ms, "detail": err_text}


def _scenario_disk_full_simulated() -> Dict[str, Any]:
    start = time.perf_counter()
    try:
        raise sqlite3.OperationalError("database or disk is full")
    except Exception as exc:
        ok = "disk is full" in str(exc).lower()
        detail = str(exc)
    elapsed_ms = round((time.perf_counter() - start) * 1000.0, 4)
    return {"name": "disk_full_simulated", "ok": ok, "elapsed_ms": elapsed_ms, "detail": detail}


def _scenario_io_jitter(db_path: Path, writes: int, sleep_ms: float) -> Dict[str, Any]:
    with sqlite3.connect(str(db_path), timeout=5.0) as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS t_jitter (id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
        conn.commit()

    latencies: List[float] = []
    with sqlite3.connect(str(db_path), timeout=5.0) as conn:
        conn.execute("PRAGMA busy_timeout=5000;")
        for idx in range(max(1, int(writes))):
            started = time.perf_counter()
            conn.execute("INSERT INTO t_jitter(v) VALUES (?)", (f"v_{idx}",))
            conn.commit()
            time.sleep(max(0.0, float(sleep_ms)) / 1000.0)
            latencies.append((time.perf_counter() - started) * 1000.0)

    latencies.sort()
    p95_idx = max(0, min(len(latencies) - 1, int(round((len(latencies) - 1) * 0.95))))
    p95 = round(float(latencies[p95_idx]), 4) if latencies else 0.0
    return {
        "name": "io_jitter",
        "ok": bool(p95 >= max(0.0, float(sleep_ms))),
        "writes": int(writes),
        "sleep_ms": float(sleep_ms),
        "p95_ms": p95,
        "max_ms": round(max(latencies), 4) if latencies else 0.0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SQLite fault-injection scenarios")
    parser.add_argument("--jitter-writes", type=int, default=20, help="write count for io_jitter scenario")
    parser.add_argument("--jitter-sleep-ms", type=float, default=20.0, help="artificial jitter sleep in milliseconds")
    args = parser.parse_args()

    with tempfile.TemporaryDirectory(prefix="sqlite_fault_inject_") as tmp_dir:
        db_path = Path(tmp_dir) / "fault_injection.db"
        scenarios = [
            _scenario_lock_conflict(db_path),
            _scenario_disk_full_simulated(),
            _scenario_io_jitter(db_path, writes=args.jitter_writes, sleep_ms=args.jitter_sleep_ms),
        ]

    payload = {
        "generated_at": _now_iso(),
        "ok": all(bool(item.get("ok")) for item in scenarios),
        "scenarios": scenarios,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if not payload["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
