#!/usr/bin/env python3
import argparse
import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def _parse_statuses(raw: str) -> List[str]:
    output: List[str] = []
    for item in str(raw or "").split(","):
        status = item.strip().lower()
        if not status:
            continue
        if status not in output:
            output.append(status)
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description="SQLite retention cleanup for audit/runtime logs and backtest metadata")
    parser.add_argument("--db-path", default="logs/quant_api.db", help="SQLite database path")
    parser.add_argument("--audit-ttl-days", type=float, default=180.0, help="Delete audit logs older than N days")
    parser.add_argument("--runtime-log-ttl-days", type=float, default=30.0, help="Delete runtime_logs older than N days")
    parser.add_argument("--backtest-ttl-days", type=float, default=90.0, help="Delete backtests older than N days")
    parser.add_argument(
        "--backtest-final-statuses",
        default="finished,failed,stopped,cancelled",
        help="Backtest statuses eligible for TTL cleanup (comma separated)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Only report matched rows, do not delete")
    args = parser.parse_args()

    db_path = Path(args.db_path).expanduser()
    if not db_path.is_absolute():
        db_path = (Path.cwd() / db_path).resolve()
    if not db_path.exists():
        raise SystemExit(f"database not found: {db_path}")

    now = _now_utc()
    now_iso = now.isoformat()
    final_statuses = _parse_statuses(args.backtest_final_statuses)

    result: Dict[str, Any] = {
        "ts_utc": now_iso,
        "db_path": str(db_path),
        "dry_run": bool(args.dry_run),
        "audit": {
            "ttl_days": float(args.audit_ttl_days),
            "cutoff_utc": "",
            "matched": 0,
            "deleted": 0,
            "enabled": float(args.audit_ttl_days) > 0,
        },
        "runtime_logs": {
            "ttl_days": float(args.runtime_log_ttl_days),
            "cutoff_utc": "",
            "matched": 0,
            "deleted": 0,
            "enabled": float(args.runtime_log_ttl_days) > 0,
            "table_exists": False,
        },
        "backtests": {
            "ttl_days": float(args.backtest_ttl_days),
            "cutoff_utc": "",
            "matched": 0,
            "deleted": 0,
            "enabled": float(args.backtest_ttl_days) > 0 and bool(final_statuses),
            "final_statuses": final_statuses,
        },
    }

    with _connect(db_path) as conn:
        if float(args.audit_ttl_days) > 0:
            audit_cutoff = (now - timedelta(days=float(args.audit_ttl_days))).isoformat()
            audit_matched_row = conn.execute(
                "SELECT COUNT(1) AS cnt FROM audit_logs WHERE ts_utc < ?",
                (audit_cutoff,),
            ).fetchone()
            audit_matched = int(audit_matched_row["cnt"]) if audit_matched_row is not None else 0
            result["audit"]["cutoff_utc"] = audit_cutoff
            result["audit"]["matched"] = audit_matched
            if not args.dry_run and audit_matched > 0:
                conn.execute("DELETE FROM audit_logs WHERE ts_utc < ?", (audit_cutoff,))
                result["audit"]["deleted"] = audit_matched

        runtime_logs_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='runtime_logs'"
        ).fetchone()
        result["runtime_logs"]["table_exists"] = runtime_logs_table is not None
        if float(args.runtime_log_ttl_days) > 0 and runtime_logs_table is not None:
            runtime_cutoff = (now - timedelta(days=float(args.runtime_log_ttl_days))).isoformat()
            runtime_matched_row = conn.execute(
                "SELECT COUNT(1) AS cnt FROM runtime_logs WHERE ts_utc < ?",
                (runtime_cutoff,),
            ).fetchone()
            runtime_matched = int(runtime_matched_row["cnt"]) if runtime_matched_row is not None else 0
            result["runtime_logs"]["cutoff_utc"] = runtime_cutoff
            result["runtime_logs"]["matched"] = runtime_matched
            if not args.dry_run and runtime_matched > 0:
                conn.execute("DELETE FROM runtime_logs WHERE ts_utc < ?", (runtime_cutoff,))
                result["runtime_logs"]["deleted"] = runtime_matched

        if float(args.backtest_ttl_days) > 0 and final_statuses:
            backtest_cutoff = (now - timedelta(days=float(args.backtest_ttl_days))).isoformat()
            status_placeholders = ",".join(["?"] * len(final_statuses))
            params = [backtest_cutoff, *final_statuses]
            backtest_matched_row = conn.execute(
                f"""
                SELECT COUNT(1) AS cnt
                FROM backtests
                WHERE created_at < ?
                  AND LOWER(status) IN ({status_placeholders})
                """,
                tuple(params),
            ).fetchone()
            backtest_matched = int(backtest_matched_row["cnt"]) if backtest_matched_row is not None else 0
            result["backtests"]["cutoff_utc"] = backtest_cutoff
            result["backtests"]["matched"] = backtest_matched
            if not args.dry_run and backtest_matched > 0:
                conn.execute(
                    f"""
                    DELETE FROM backtests
                    WHERE created_at < ?
                      AND LOWER(status) IN ({status_placeholders})
                    """,
                    tuple(params),
                )
                result["backtests"]["deleted"] = backtest_matched

        if not args.dry_run:
            conn.commit()

    result["deleted_total"] = (
        int(result["audit"]["deleted"])
        + int(result["runtime_logs"]["deleted"])
        + int(result["backtests"]["deleted"])
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
