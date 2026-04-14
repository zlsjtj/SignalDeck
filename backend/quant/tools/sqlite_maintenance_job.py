#!/usr/bin/env python3
import argparse
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def _pragma_int(conn: sqlite3.Connection, name: str) -> int:
    row = conn.execute(f"PRAGMA {name};").fetchone()
    if row is None:
        return 0
    try:
        return int(row[0])
    except Exception:
        return 0


def _stats(conn: sqlite3.Connection) -> Dict[str, Any]:
    page_size = _pragma_int(conn, "page_size")
    page_count = _pragma_int(conn, "page_count")
    freelist_count = _pragma_int(conn, "freelist_count")
    db_size_bytes = page_size * page_count
    free_bytes = page_size * freelist_count
    fragmentation_pct = 0.0
    if db_size_bytes > 0:
        fragmentation_pct = (free_bytes / db_size_bytes) * 100.0
    return {
        "page_size": page_size,
        "page_count": page_count,
        "freelist_count": freelist_count,
        "db_size_bytes": db_size_bytes,
        "free_bytes": free_bytes,
        "fragmentation_pct": round(fragmentation_pct, 4),
    }


def _run_checkpoint(conn: sqlite3.Connection, mode: str) -> Dict[str, int]:
    row = conn.execute(f"PRAGMA wal_checkpoint({mode});").fetchone()
    if row is None:
        return {"busy": 0, "log_frames": 0, "checkpointed_frames": 0}
    return {
        "busy": int(row[0]),
        "log_frames": int(row[1]),
        "checkpointed_frames": int(row[2]),
    }


def _load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _save_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _parse_iso_epoch(ts_text: Any) -> float:
    raw = str(ts_text or "").strip()
    if not raw:
        return 0.0
    try:
        dt = datetime.fromisoformat(raw)
    except Exception:
        return 0.0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def _should_run(now_epoch: float, last_run_iso: Any, every_hours: float) -> bool:
    if every_hours <= 0:
        return True
    last_epoch = _parse_iso_epoch(last_run_iso)
    if last_epoch <= 0:
        return True
    return (now_epoch - last_epoch) >= every_hours * 3600.0


def main() -> None:
    parser = argparse.ArgumentParser(description="Automated SQLite maintenance job for quant_api.db")
    parser.add_argument("--db-path", default="logs/quant_api.db", help="SQLite database path")
    parser.add_argument("--state-path", default="logs/sqlite_maintenance_state.json", help="Job state JSON path")
    parser.add_argument("--report-path", default="logs/sqlite_maintenance_latest.json", help="Latest report JSON path")
    parser.add_argument(
        "--checkpoint",
        choices=["PASSIVE", "FULL", "RESTART", "TRUNCATE"],
        default="PASSIVE",
        help="WAL checkpoint mode",
    )
    parser.add_argument("--analyze-every-hours", type=float, default=24.0, help="Run ANALYZE every N hours")
    parser.add_argument(
        "--vacuum-fragmentation-threshold",
        type=float,
        default=20.0,
        help="Run VACUUM when fragmentation_pct reaches this value",
    )
    parser.add_argument(
        "--vacuum-free-bytes-threshold",
        type=int,
        default=256 * 1024 * 1024,
        help="Run VACUUM when free_bytes reaches this value",
    )
    parser.add_argument("--force-vacuum", action="store_true", help="Force VACUUM regardless of thresholds")
    parser.add_argument("--skip-vacuum", action="store_true", help="Skip VACUUM regardless of thresholds")
    parser.add_argument(
        "--alert-fragmentation-threshold",
        type=float,
        default=35.0,
        help="Exit with code 2 when post-maintenance fragmentation_pct reaches this value",
    )
    parser.add_argument(
        "--alert-free-bytes-threshold",
        type=int,
        default=512 * 1024 * 1024,
        help="Exit with code 2 when post-maintenance free_bytes reaches this value",
    )
    args = parser.parse_args()

    db_path = Path(args.db_path).expanduser()
    if not db_path.is_absolute():
        db_path = (Path.cwd() / db_path).resolve()
    if not db_path.exists():
        raise SystemExit(f"database not found: {db_path}")

    state_path = Path(args.state_path).expanduser()
    if not state_path.is_absolute():
        state_path = (Path.cwd() / state_path).resolve()
    report_path = Path(args.report_path).expanduser()
    if not report_path.is_absolute():
        report_path = (Path.cwd() / report_path).resolve()

    now_iso = _now_iso()
    now_epoch = time.time()
    state = _load_json(state_path)
    if not isinstance(state, dict):
        state = {}

    report: Dict[str, Any] = {
        "ts_utc": now_iso,
        "db_path": str(db_path),
        "state_path": str(state_path),
        "report_path": str(report_path),
        "operations": {},
        "warnings": [],
    }

    with _connect(db_path) as conn:
        before = _stats(conn)
        report["before"] = before

        report["operations"]["checkpoint"] = _run_checkpoint(conn, args.checkpoint)
        conn.commit()

        if _should_run(now_epoch, state.get("last_analyze_at"), float(args.analyze_every_hours)):
            conn.execute("ANALYZE;")
            conn.commit()
            report["operations"]["analyze"] = "ok"
            state["last_analyze_at"] = now_iso
        else:
            report["operations"]["analyze"] = "skipped"

        vacuum_reason = ""
        should_vacuum = False
        if args.force_vacuum:
            should_vacuum = True
            vacuum_reason = "forced"
        elif args.skip_vacuum:
            should_vacuum = False
            vacuum_reason = "skip_flag"
        elif float(before.get("fragmentation_pct", 0.0)) >= float(args.vacuum_fragmentation_threshold):
            should_vacuum = True
            vacuum_reason = "fragmentation_threshold"
        elif int(before.get("free_bytes", 0)) >= int(args.vacuum_free_bytes_threshold):
            should_vacuum = True
            vacuum_reason = "free_bytes_threshold"

        if should_vacuum:
            conn.execute("VACUUM;")
            conn.commit()
            report["operations"]["vacuum"] = {"status": "ok", "reason": vacuum_reason}
            state["last_vacuum_at"] = now_iso
        else:
            report["operations"]["vacuum"] = {"status": "skipped", "reason": vacuum_reason or "below_threshold"}

        after = _stats(conn)
        report["after"] = after

    state["last_run_at"] = now_iso
    _save_json(state_path, state)
    report["state"] = state

    after_fragmentation = float(report.get("after", {}).get("fragmentation_pct", 0.0))
    after_free_bytes = int(report.get("after", {}).get("free_bytes", 0))
    if after_fragmentation >= float(args.alert_fragmentation_threshold):
        report["warnings"].append(
            f"fragmentation_pct={after_fragmentation:.4f} exceeds threshold={float(args.alert_fragmentation_threshold):.4f}"
        )
    if after_free_bytes >= int(args.alert_free_bytes_threshold):
        report["warnings"].append(
            f"free_bytes={after_free_bytes} exceeds threshold={int(args.alert_free_bytes_threshold)}"
        )

    _save_json(report_path, report)
    print(json.dumps(report, ensure_ascii=False, indent=2))

    if report["warnings"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
