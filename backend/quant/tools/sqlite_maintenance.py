#!/usr/bin/env python3
import argparse
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict


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


def main() -> None:
    parser = argparse.ArgumentParser(description="SQLite maintenance helper for quant_api.db")
    parser.add_argument("--db-path", default="logs/quant_api.db", help="SQLite database path")
    parser.add_argument(
        "--checkpoint",
        choices=["PASSIVE", "FULL", "RESTART", "TRUNCATE"],
        default="PASSIVE",
        help="Run WAL checkpoint mode",
    )
    parser.add_argument("--no-checkpoint", action="store_true", help="Skip WAL checkpoint")
    parser.add_argument("--analyze", action="store_true", help="Run ANALYZE")
    parser.add_argument("--vacuum", action="store_true", help="Run VACUUM")
    args = parser.parse_args()

    db_path = Path(args.db_path).expanduser()
    if not db_path.is_absolute():
        db_path = (Path.cwd() / db_path).resolve()
    if not db_path.exists():
        raise SystemExit(f"database not found: {db_path}")

    result: Dict[str, Any] = {
        "db_path": str(db_path),
        "operations": {},
    }

    with _connect(db_path) as conn:
        result["before"] = _stats(conn)

        if not args.no_checkpoint:
            result["operations"]["checkpoint"] = _run_checkpoint(conn, args.checkpoint)
            conn.commit()

        if args.analyze:
            conn.execute("ANALYZE;")
            conn.commit()
            result["operations"]["analyze"] = "ok"

        if args.vacuum:
            conn.execute("VACUUM;")
            conn.commit()
            result["operations"]["vacuum"] = "ok"

        result["after"] = _stats(conn)

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
