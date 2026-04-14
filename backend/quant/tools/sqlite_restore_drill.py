#!/usr/bin/env python3
import argparse
import json
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _connect(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def _copy_if_exists(src: Path, dst: Path) -> None:
    if src.exists():
        shutil.copy2(src, dst)


def _required_tables(conn: sqlite3.Connection) -> List[str]:
    required = {
        "schema_version",
        "strategies",
        "backtests",
        "risk_states",
        "audit_logs",
    }
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    existing = {str(row[0]) for row in rows}
    return sorted(required - existing)


def main() -> None:
    parser = argparse.ArgumentParser(description="SQLite backup restore drill helper")
    parser.add_argument("--backup-file", required=True, help="Backup database file path")
    parser.add_argument("--output-dir", default="logs/restore_drills", help="Drill workspace directory")
    parser.add_argument("--cleanup", action="store_true", help="Delete restored drill DB after checks")
    args = parser.parse_args()

    backup_file = Path(args.backup_file).expanduser()
    if not backup_file.is_absolute():
        backup_file = (Path.cwd() / backup_file).resolve()
    if not backup_file.exists():
        raise SystemExit(f"backup file not found: {backup_file}")

    output_dir = Path(args.output_dir).expanduser()
    if not output_dir.is_absolute():
        output_dir = (Path.cwd() / output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    now = _now_utc()
    ts = now.strftime("%Y%m%d_%H%M%S_%f")
    restored_db = output_dir / f"restore_drill_{ts}.db"

    shutil.copy2(backup_file, restored_db)
    _copy_if_exists(Path(str(backup_file) + "-wal"), Path(str(restored_db) + "-wal"))
    _copy_if_exists(Path(str(backup_file) + "-shm"), Path(str(restored_db) + "-shm"))

    result: Dict[str, Any] = {
        "ts_utc": now.isoformat(),
        "backup_file": str(backup_file),
        "restored_db": str(restored_db),
        "checks": {},
    }

    ok = True
    try:
        with _connect(restored_db) as conn:
            integrity_row = conn.execute("PRAGMA integrity_check;").fetchone()
            integrity_message = str(integrity_row[0] if integrity_row is not None else "")
            integrity_ok = integrity_message.lower() == "ok"
            result["checks"]["integrity"] = {"ok": integrity_ok, "message": integrity_message}
            if not integrity_ok:
                ok = False

            missing_tables = _required_tables(conn)
            result["checks"]["required_tables"] = {
                "ok": len(missing_tables) == 0,
                "missing": missing_tables,
            }
            if missing_tables:
                ok = False

            if ok:
                probe_ts = _now_utc().isoformat()
                conn.execute(
                    """
                    INSERT INTO audit_logs (ts_utc, owner, action, entity, entity_id, detail_json)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        probe_ts,
                        "restore_drill",
                        "restore.drill.probe",
                        "drill",
                        "restore_probe",
                        "{}",
                    ),
                )
                probe_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                conn.execute("DELETE FROM audit_logs WHERE id = ?", (probe_id,))
                conn.commit()
                result["checks"]["read_write_probe"] = {"ok": True, "probe_id": probe_id}
            else:
                result["checks"]["read_write_probe"] = {"ok": False, "reason": "integrity_or_schema_failed"}
    except Exception as exc:
        ok = False
        result["checks"]["exception"] = {"ok": False, "error": str(exc)}

    if args.cleanup:
        Path(str(restored_db) + "-wal").unlink(missing_ok=True)
        Path(str(restored_db) + "-shm").unlink(missing_ok=True)
        restored_db.unlink(missing_ok=True)
        result["cleanup"] = "done"

    result["ok"] = ok
    print(json.dumps(result, ensure_ascii=False, indent=2))

    if not ok:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
