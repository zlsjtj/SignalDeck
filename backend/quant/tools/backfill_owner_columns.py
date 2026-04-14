#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:  # pragma: no cover - sqlite path is the default in unit tests
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None


_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_owner(value: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(value or "").strip().lower())
    return safe or "admin"


def _owner_from_scoped_strategy_key(strategy_key: str) -> Optional[str]:
    text = str(strategy_key or "").strip()
    if not text.startswith("usr__"):
        return None
    remainder = text[len("usr__") :]
    if "__" not in remainder:
        return None
    owner_key, _ = remainder.split("__", 1)
    owner_key = owner_key.strip()
    return _normalize_owner(owner_key) if owner_key else None


def _safe_table_name(name: str) -> Optional[str]:
    text = str(name or "").strip()
    if not text or not _TABLE_NAME_RE.match(text):
        return None
    return text


def _decode_json_owner(raw: Any) -> Optional[str]:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        payload = json.loads(text)
    except Exception:
        return None
    if isinstance(payload, dict):
        candidate = payload.get("owner")
        if candidate is None:
            candidate = payload.get("username")
        if candidate is None:
            return None
        normalized = _normalize_owner(str(candidate))
        return normalized if normalized else None
    return None


def _derive_target_owner(
    row: Dict[str, Any],
    *,
    default_owner: str,
    normalize_existing: bool,
) -> str:
    old_owner = str(row.get("owner") or "").strip()
    if old_owner:
        return _normalize_owner(old_owner) if normalize_existing else old_owner

    strategy_owner = _owner_from_scoped_strategy_key(str(row.get("strategy_key") or ""))
    if strategy_owner:
        return strategy_owner

    for json_field in ("record_json", "detail_json", "state_json", "source_config_json", "preferences_json"):
        if json_field not in row:
            continue
        decoded_owner = _decode_json_owner(row.get(json_field))
        if decoded_owner:
            return decoded_owner

    return default_owner


def _owner_role(owner: str, *, default_owner: str) -> str:
    if owner == default_owner:
        return "admin"
    if owner == "guest":
        return "guest"
    return "user"


def _sqlite_owner_tables(conn: sqlite3.Connection) -> List[str]:
    tables: List[str] = []
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name ASC"
    ).fetchall()
    for row in rows:
        table_name = str(row["name"] or "").strip()
        safe_name = _safe_table_name(table_name)
        if not safe_name:
            continue
        cols = conn.execute(f"PRAGMA table_info({safe_name})").fetchall()
        col_names = {str(col["name"] or "") for col in cols}
        if "owner" in col_names:
            tables.append(safe_name)
    return tables


def _sqlite_backfill(
    *,
    db_path: Path,
    default_owner: str,
    normalize_existing: bool,
    dry_run: bool,
) -> Dict[str, Any]:
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000;")

    touched_owners: set[str] = set()
    table_reports: List[Dict[str, Any]] = []
    users_upserted = 0
    now_iso = _now_iso()
    try:
        tables = _sqlite_owner_tables(conn)
        for table in tables:
            rows = conn.execute(f"SELECT rowid AS _rid, * FROM {table}").fetchall()
            scanned = len(rows)
            updated = 0
            for row in rows:
                raw = dict(row)
                old_owner = str(raw.get("owner") or "").strip()
                target_owner = _derive_target_owner(
                    raw,
                    default_owner=default_owner,
                    normalize_existing=normalize_existing,
                )
                if target_owner:
                    touched_owners.add(target_owner)
                if target_owner == old_owner:
                    continue
                updated += 1
                if not dry_run:
                    conn.execute(
                        f"UPDATE {table} SET owner = ? WHERE rowid = ?",
                        (target_owner, int(raw["_rid"])),
                    )
            table_reports.append(
                {
                    "table": table,
                    "scanned": scanned,
                    "updated": updated,
                }
            )

        users_table = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='users'"
        ).fetchone()
        if users_table is not None:
            existing_users = {
                str(row["username"] or "").strip()
                for row in conn.execute("SELECT username FROM users").fetchall()
                if str(row["username"] or "").strip()
            }
            missing = sorted(owner for owner in touched_owners if owner and owner not in existing_users)
            users_upserted = len(missing)
            if not dry_run:
                for owner in missing:
                    conn.execute(
                        """
                        INSERT INTO users (username, status, display_name, role, created_at, last_login_at)
                        VALUES (?, 'active', ?, ?, ?, '')
                        ON CONFLICT(username) DO UPDATE SET
                            status='active',
                            display_name=excluded.display_name,
                            role=excluded.role
                        """,
                        (
                            owner,
                            owner,
                            _owner_role(owner, default_owner=default_owner),
                            now_iso,
                        ),
                    )

        if not dry_run:
            conn.commit()

        return {
            "backend": "sqlite",
            "db_path": str(db_path),
            "tables": table_reports,
            "updated_total": int(sum(int(row["updated"]) for row in table_reports)),
            "owners_touched": sorted(touched_owners),
            "users_upserted": users_upserted,
        }
    finally:
        conn.close()


def _postgres_owner_tables(cur) -> List[str]:
    cur.execute(
        """
        SELECT table_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND column_name = 'owner'
        ORDER BY table_name ASC
        """
    )
    rows = cur.fetchall()
    tables: List[str] = []
    for row in rows:
        table_name = str(row["table_name"] or "").strip()
        safe_name = _safe_table_name(table_name)
        if safe_name:
            tables.append(safe_name)
    return tables


def _postgres_backfill(
    *,
    dsn: str,
    default_owner: str,
    normalize_existing: bool,
    dry_run: bool,
) -> Dict[str, Any]:
    if psycopg is None or dict_row is None:
        raise RuntimeError("postgres mode requires psycopg package")
    touched_owners: set[str] = set()
    table_reports: List[Dict[str, Any]] = []
    users_upserted = 0
    now_iso = _now_iso()
    with psycopg.connect(dsn, autocommit=False, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            tables = _postgres_owner_tables(cur)
            for table in tables:
                cur.execute(f"SELECT ctid::text AS _tid, * FROM {table}")
                rows = cur.fetchall()
                scanned = len(rows)
                updated = 0
                for row in rows:
                    raw = dict(row)
                    old_owner = str(raw.get("owner") or "").strip()
                    target_owner = _derive_target_owner(
                        raw,
                        default_owner=default_owner,
                        normalize_existing=normalize_existing,
                    )
                    if target_owner:
                        touched_owners.add(target_owner)
                    if target_owner == old_owner:
                        continue
                    updated += 1
                    if not dry_run:
                        cur.execute(
                            f"UPDATE {table} SET owner = %s WHERE ctid = %s::tid",
                            (target_owner, str(raw["_tid"])),
                        )
                table_reports.append(
                    {
                        "table": table,
                        "scanned": scanned,
                        "updated": updated,
                    }
                )

            cur.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = 'users'
                LIMIT 1
                """
            )
            users_exists = cur.fetchone() is not None
            if users_exists:
                cur.execute("SELECT username FROM users")
                existing_users = {
                    str(row["username"] or "").strip()
                    for row in cur.fetchall()
                    if str(row["username"] or "").strip()
                }
                missing = sorted(owner for owner in touched_owners if owner and owner not in existing_users)
                users_upserted = len(missing)
                if not dry_run:
                    for owner in missing:
                        cur.execute(
                            """
                            INSERT INTO users (username, status, display_name, role, created_at, last_login_at)
                            VALUES (%s, 'active', %s, %s, %s, '')
                            ON CONFLICT(username) DO UPDATE SET
                                status='active',
                                display_name=EXCLUDED.display_name,
                                role=EXCLUDED.role
                            """,
                            (
                                owner,
                                owner,
                                _owner_role(owner, default_owner=default_owner),
                                now_iso,
                            ),
                        )

        if dry_run:
            conn.rollback()
        else:
            conn.commit()

    return {
        "backend": "postgres",
        "dsn": "<configured>",
        "tables": table_reports,
        "updated_total": int(sum(int(row["updated"]) for row in table_reports)),
        "owners_touched": sorted(touched_owners),
        "users_upserted": users_upserted,
    }


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill/normalize owner columns in quant database tables")
    parser.add_argument(
        "--backend",
        default="sqlite",
        choices=["sqlite", "postgres"],
        help="Database backend type",
    )
    parser.add_argument("--db-path", default="logs/quant_api.db", help="SQLite database path")
    parser.add_argument("--postgres-dsn", default="", help="PostgreSQL DSN when backend=postgres")
    parser.add_argument("--default-owner", default="admin", help="Fallback owner used when source owner is missing")
    parser.add_argument(
        "--no-normalize-existing",
        action="store_true",
        help="Do not normalize non-empty owner values (only fill missing owner)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview updates without writing")
    return parser.parse_args(argv)


def main() -> None:
    args = _parse_args()
    default_owner = _normalize_owner(str(args.default_owner or "admin"))
    normalize_existing = not bool(args.no_normalize_existing)
    dry_run = bool(args.dry_run)
    try:
        if args.backend == "postgres":
            dsn = str(args.postgres_dsn or "").strip()
            if not dsn:
                raise RuntimeError("--postgres-dsn is required when backend=postgres")
            core = _postgres_backfill(
                dsn=dsn,
                default_owner=default_owner,
                normalize_existing=normalize_existing,
                dry_run=dry_run,
            )
        else:
            db_path = Path(args.db_path).expanduser()
            if not db_path.is_absolute():
                db_path = (Path.cwd() / db_path).resolve()
            if not db_path.exists():
                raise RuntimeError(f"database not found: {db_path}")
            core = _sqlite_backfill(
                db_path=db_path,
                default_owner=default_owner,
                normalize_existing=normalize_existing,
                dry_run=dry_run,
            )
        payload = {
            "ok": True,
            "ts_utc": _now_iso(),
            "default_owner": default_owner,
            "normalize_existing": normalize_existing,
            "dry_run": dry_run,
            **core,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
    except Exception as exc:
        payload = {
            "ok": False,
            "ts_utc": _now_iso(),
            "error": str(exc),
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
