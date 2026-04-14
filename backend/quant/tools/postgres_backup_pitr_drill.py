#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:  # pragma: no cover - environment dependent
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tail_lines(text: str, max_lines: int = 80) -> List[str]:
    lines = [line.rstrip("\n") for line in str(text or "").splitlines()]
    if len(lines) <= max_lines:
        return lines
    return lines[-max_lines:]


def _cleanup_old_backups(backup_dir: Path, pattern: str, retain: int) -> List[str]:
    if retain <= 0:
        return []
    files = sorted([p for p in backup_dir.glob(pattern) if p.is_file()], key=lambda p: p.name, reverse=True)
    removed: List[str] = []
    for path in files[retain:]:
        path.unlink(missing_ok=True)
        removed.append(str(path))
    return removed


def _connect(dsn: str):
    if psycopg is None or dict_row is None:
        raise RuntimeError("postgres mode requires psycopg package")
    return psycopg.connect(dsn, autocommit=False, row_factory=dict_row)


def _query_wal_settings(dsn: str) -> Dict[str, Any]:
    with _connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW wal_level")
            wal_level = str((cur.fetchone() or {}).get("wal_level") or "")
            cur.execute("SHOW archive_mode")
            archive_mode = str((cur.fetchone() or {}).get("archive_mode") or "")
            cur.execute("SHOW archive_command")
            archive_command = str((cur.fetchone() or {}).get("archive_command") or "")
            cur.execute("SELECT pg_is_in_recovery() AS in_recovery")
            in_recovery = bool((cur.fetchone() or {}).get("in_recovery"))

    wal_level_ok = wal_level.lower() in {"replica", "logical"}
    archive_mode_ok = archive_mode.lower() in {"on", "always"}
    archive_command_ok = bool(archive_command.strip()) and archive_command.strip().lower() not in {"(disabled)", "false"}
    return {
        "walLevel": wal_level,
        "archiveMode": archive_mode,
        "archiveCommand": archive_command,
        "inRecovery": in_recovery,
        "walLevelOk": wal_level_ok,
        "archiveModeOk": archive_mode_ok,
        "archiveCommandOk": archive_command_ok,
        "ok": wal_level_ok and archive_mode_ok and archive_command_ok,
    }


def _create_restore_point(dsn: str, *, name_prefix: str) -> Dict[str, Any]:
    point_name = f"{name_prefix}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    try:
        with _connect(dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_is_in_recovery() AS in_recovery")
                in_recovery = bool((cur.fetchone() or {}).get("in_recovery"))
                if in_recovery:
                    conn.rollback()
                    return {
                        "ok": False,
                        "created": False,
                        "name": "",
                        "error": "connected instance is in recovery mode; cannot create restore point",
                    }
                cur.execute("SELECT pg_create_restore_point(%s) AS restore_point", (point_name,))
                row = cur.fetchone() or {}
                created_name = str(row.get("restore_point") or point_name)
                conn.commit()
        return {
            "ok": True,
            "created": True,
            "name": created_name,
            "error": "",
        }
    except Exception as exc:
        return {
            "ok": False,
            "created": False,
            "name": "",
            "error": str(exc),
        }


def _run_pg_dump(
    *,
    dsn: str,
    backup_dir: Path,
    prefix: str,
    pg_dump_bin: str,
    timeout_seconds: float,
    retain: int,
) -> Dict[str, Any]:
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"{prefix}_{ts}.dump"

    cmd = [
        str(pg_dump_bin),
        "--format=custom",
        "--no-owner",
        "--no-privileges",
        "--file",
        str(backup_path),
        str(dsn),
    ]
    try:
        completed = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            timeout=max(5.0, float(timeout_seconds)),
        )
    except Exception as exc:
        return {
            "ok": False,
            "path": str(backup_path),
            "sizeBytes": 0,
            "removedBackups": [],
            "command": cmd,
            "returncode": -1,
            "stdoutTail": [],
            "stderrTail": [str(exc)],
            "error": str(exc),
        }

    ok = int(completed.returncode) == 0 and backup_path.exists()
    backup_size = backup_path.stat().st_size if backup_path.exists() else 0
    removed = _cleanup_old_backups(backup_dir, f"{prefix}_*.dump", max(0, int(retain)))
    return {
        "ok": bool(ok),
        "path": str(backup_path),
        "sizeBytes": int(backup_size),
        "removedBackups": removed,
        "command": cmd,
        "returncode": int(completed.returncode),
        "stdoutTail": _tail_lines(completed.stdout),
        "stderrTail": _tail_lines(completed.stderr),
        "error": "" if ok else "pg_dump failed",
    }


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PostgreSQL backup + WAL + PITR drill helper")
    parser.add_argument(
        "--postgres-dsn",
        default=str(os.getenv("QUANT_E2E_POSTGRES_DSN", "") or os.getenv("API_DB_POSTGRES_DSN", "")),
        help="PostgreSQL DSN",
    )
    parser.add_argument("--backup-dir", default="logs/postgres_backups", help="Output directory for pg_dump artifacts")
    parser.add_argument("--prefix", default="quant_pg", help="Backup filename prefix")
    parser.add_argument("--retain", type=int, default=14, help="How many dump files to keep")
    parser.add_argument("--pg-dump-bin", default="pg_dump", help="pg_dump binary path")
    parser.add_argument("--timeout-seconds", type=float, default=120.0, help="pg_dump timeout seconds")
    parser.add_argument("--skip-pg-dump", action="store_true", help="Skip pg_dump baseline backup")
    parser.add_argument("--skip-pitr-drill", action="store_true", help="Skip restore point creation drill")
    parser.add_argument("--restore-point-prefix", default="quant_pitr_drill", help="Restore point name prefix")
    parser.add_argument("--allow-wal-unconfigured", action="store_true", help="Do not fail when WAL archive settings are not ready")
    parser.add_argument("--allow-pitr-fail", action="store_true", help="Do not fail when restore-point drill fails")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    dsn = str(args.postgres_dsn or "").strip()
    if not dsn:
        payload = {
            "ok": False,
            "error": "postgres dsn is required; pass --postgres-dsn or set QUANT_E2E_POSTGRES_DSN",
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 1

    if shutil.which(str(args.pg_dump_bin)) is None and not bool(args.skip_pg_dump):
        payload = {
            "ok": False,
            "error": f"pg_dump binary not found: {args.pg_dump_bin}",
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 1

    report: Dict[str, Any] = {
        "ok": False,
        "ts": _now_iso(),
        "postgresDsn": "<configured>",
        "wal": {},
        "backup": {"enabled": not bool(args.skip_pg_dump)},
        "pitr": {"enabled": not bool(args.skip_pitr_drill)},
    }

    try:
        wal_report = _query_wal_settings(dsn)
        report["wal"] = wal_report

        backup_report: Dict[str, Any] = {"enabled": not bool(args.skip_pg_dump), "ok": True}
        if not bool(args.skip_pg_dump):
            backup_dir = Path(str(args.backup_dir or "")).expanduser()
            if not backup_dir.is_absolute():
                backup_dir = (Path.cwd() / backup_dir).resolve()
            backup_report = _run_pg_dump(
                dsn=dsn,
                backup_dir=backup_dir,
                prefix=str(args.prefix or "quant_pg"),
                pg_dump_bin=str(args.pg_dump_bin),
                timeout_seconds=float(args.timeout_seconds),
                retain=int(args.retain),
            )
        report["backup"] = backup_report

        pitr_report: Dict[str, Any] = {"enabled": not bool(args.skip_pitr_drill), "ok": True}
        if not bool(args.skip_pitr_drill):
            pitr_report = _create_restore_point(dsn, name_prefix=str(args.restore_point_prefix or "quant_pitr_drill"))
        report["pitr"] = pitr_report

        wal_ok = bool(report["wal"].get("ok")) or bool(args.allow_wal_unconfigured)
        backup_ok = bool(report["backup"].get("ok", True))
        pitr_ok = bool(report["pitr"].get("ok", True)) or bool(args.allow_pitr_fail)
        report["ok"] = bool(wal_ok and backup_ok and pitr_ok)

        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if bool(report["ok"]) else 2
    except Exception as exc:
        report["ok"] = False
        report["error"] = str(exc)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
