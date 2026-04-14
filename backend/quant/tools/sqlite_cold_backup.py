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


def _verify_integrity(path: Path) -> Dict[str, Any]:
    with _connect(path) as conn:
        row = conn.execute("PRAGMA integrity_check;").fetchone()
        msg = str(row[0] if row is not None else "")
    return {"ok": msg.lower() == "ok", "message": msg}


def _cleanup_old_backups(backup_dir: Path, pattern: str, retain: int) -> List[str]:
    if retain <= 0:
        return []
    files = sorted([p for p in backup_dir.glob(pattern) if p.is_file()], key=lambda p: p.name, reverse=True)
    removed: List[str] = []
    for path in files[retain:]:
        path.unlink(missing_ok=True)
        for suffix in ("-wal", "-shm"):
            Path(str(path) + suffix).unlink(missing_ok=True)
        removed.append(str(path))
    return removed


def _copy_sidecar(source_db: Path, target_db: Path, suffix: str) -> str:
    src = Path(str(source_db) + suffix)
    if not src.exists():
        return ""
    dst = Path(str(target_db) + suffix)
    shutil.copy2(src, dst)
    return str(dst)


def main() -> None:
    parser = argparse.ArgumentParser(description="SQLite cold backup helper (copy db file in stop-write window)")
    parser.add_argument("--db-path", default="logs/quant_api.db", help="Source SQLite DB path")
    parser.add_argument("--backup-dir", default="logs/db_cold_backups", help="Backup output directory")
    parser.add_argument("--prefix", default="quant_api_cold", help="Backup filename prefix")
    parser.add_argument("--retain", type=int, default=7, help="Number of backups to keep")
    parser.add_argument("--verify", action="store_true", help="Run integrity_check on backup")
    args = parser.parse_args()

    db_path = Path(args.db_path).expanduser()
    if not db_path.is_absolute():
        db_path = (Path.cwd() / db_path).resolve()
    if not db_path.exists():
        raise SystemExit(f"database not found: {db_path}")

    backup_dir = Path(args.backup_dir).expanduser()
    if not backup_dir.is_absolute():
        backup_dir = (Path.cwd() / backup_dir).resolve()
    backup_dir.mkdir(parents=True, exist_ok=True)

    now = _now_utc()
    ts = now.strftime("%Y%m%d_%H%M%S_%f")
    backup_path = backup_dir / f"{args.prefix}_{ts}.db"

    shutil.copy2(db_path, backup_path)
    copied_sidecars = [
        path
        for path in (
            _copy_sidecar(db_path, backup_path, "-wal"),
            _copy_sidecar(db_path, backup_path, "-shm"),
        )
        if path
    ]

    verify_result = {"ok": True, "message": "skipped"}
    if args.verify:
        verify_result = _verify_integrity(backup_path)

    removed = _cleanup_old_backups(backup_dir, f"{args.prefix}_*.db", max(0, int(args.retain)))
    result: Dict[str, Any] = {
        "ts_utc": now.isoformat(),
        "source_db_path": str(db_path),
        "backup_path": str(backup_path),
        "backup_size_bytes": backup_path.stat().st_size if backup_path.exists() else 0,
        "sidecars": copied_sidecars,
        "verify": verify_result,
        "removed_backups": removed,
    }
    print(json.dumps(result, ensure_ascii=False, indent=2))
    if not verify_result.get("ok", False):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
