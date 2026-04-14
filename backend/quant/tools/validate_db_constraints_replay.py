#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:  # pragma: no cover - only used in postgres mode
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None


Rule = Tuple[str, str, str, str]


_SQLITE_RULES: List[Rule] = [
    (
        "strategy_compiler_jobs.status.enum",
        "strategy_compiler_jobs",
        """
        SELECT COUNT(1) AS cnt
        FROM strategy_compiler_jobs
        WHERE COALESCE(status, '') = ''
           OR LOWER(TRIM(status)) NOT IN ('pending', 'running', 'success', 'failed')
        """,
        """
        SELECT id, status
        FROM strategy_compiler_jobs
        WHERE COALESCE(status, '') = ''
           OR LOWER(TRIM(status)) NOT IN ('pending', 'running', 'success', 'failed')
        ORDER BY id DESC
        LIMIT 5
        """,
    ),
    (
        "risk_events.event_type.enum",
        "risk_events",
        """
        SELECT COUNT(1) AS cnt
        FROM risk_events
        WHERE COALESCE(event_type, '') = ''
           OR LOWER(TRIM(event_type)) NOT IN ('triggered', 'recovered', 'manual_update')
        """,
        """
        SELECT id, owner, strategy_key, event_type
        FROM risk_events
        WHERE COALESCE(event_type, '') = ''
           OR LOWER(TRIM(event_type)) NOT IN ('triggered', 'recovered', 'manual_update')
        ORDER BY id DESC
        LIMIT 5
        """,
    ),
    (
        "runtime_logs.level.enum",
        "runtime_logs",
        """
        SELECT COUNT(1) AS cnt
        FROM runtime_logs
        WHERE COALESCE(level, '') = ''
           OR LOWER(TRIM(level)) NOT IN ('debug', 'info', 'warn', 'error', 'critical')
        """,
        """
        SELECT id, owner, log_type, level
        FROM runtime_logs
        WHERE COALESCE(level, '') = ''
           OR LOWER(TRIM(level)) NOT IN ('debug', 'info', 'warn', 'error', 'critical')
        ORDER BY id DESC
        LIMIT 5
        """,
    ),
    (
        "alert_deliveries.status.enum",
        "alert_deliveries",
        """
        SELECT COUNT(1) AS cnt
        FROM alert_deliveries
        WHERE COALESCE(status, '') = ''
           OR LOWER(TRIM(status)) NOT IN ('sent', 'failed')
        """,
        """
        SELECT id, owner, status
        FROM alert_deliveries
        WHERE COALESCE(status, '') = ''
           OR LOWER(TRIM(status)) NOT IN ('sent', 'failed')
        ORDER BY id DESC
        LIMIT 5
        """,
    ),
    (
        "alert_deliveries.severity.enum",
        "alert_deliveries",
        """
        SELECT COUNT(1) AS cnt
        FROM alert_deliveries
        WHERE COALESCE(severity, '') = ''
           OR LOWER(TRIM(severity)) NOT IN ('info', 'warn', 'error', 'critical')
        """,
        """
        SELECT id, owner, severity
        FROM alert_deliveries
        WHERE COALESCE(severity, '') = ''
           OR LOWER(TRIM(severity)) NOT IN ('info', 'warn', 'error', 'critical')
        ORDER BY id DESC
        LIMIT 5
        """,
    ),
    (
        "alert_deliveries.numeric.check",
        "alert_deliveries",
        """
        SELECT COUNT(1) AS cnt
        FROM alert_deliveries
        WHERE COALESCE(retry_count, -1) < 0
           OR COALESCE(duration_ms, -1) < 0
        """,
        """
        SELECT id, owner, retry_count, duration_ms
        FROM alert_deliveries
        WHERE COALESCE(retry_count, -1) < 0
           OR COALESCE(duration_ms, -1) < 0
        ORDER BY id DESC
        LIMIT 5
        """,
    ),
    (
        "ws_connection_events.refresh_ms.check",
        "ws_connection_events",
        """
        SELECT COUNT(1) AS cnt
        FROM ws_connection_events
        WHERE refresh_ms IS NULL OR refresh_ms < 0
        """,
        """
        SELECT id, owner, event_type, refresh_ms
        FROM ws_connection_events
        WHERE refresh_ms IS NULL OR refresh_ms < 0
        ORDER BY id DESC
        LIMIT 5
        """,
    ),
    (
        "account_security_events.severity.enum",
        "account_security_events",
        """
        SELECT COUNT(1) AS cnt
        FROM account_security_events
        WHERE COALESCE(severity, '') = ''
           OR LOWER(TRIM(severity)) NOT IN ('info', 'warn', 'error', 'critical')
        """,
        """
        SELECT id, owner, event_type, severity
        FROM account_security_events
        WHERE COALESCE(severity, '') = ''
           OR LOWER(TRIM(severity)) NOT IN ('info', 'warn', 'error', 'critical')
        ORDER BY id DESC
        LIMIT 5
        """,
    ),
    (
        "api_tokens.active_name.unique",
        "api_tokens",
        """
        SELECT COUNT(1) AS cnt
        FROM (
            SELECT owner, token_name, COUNT(1) AS dup_cnt
            FROM api_tokens
            WHERE token_name <> '' AND revoked_at = ''
            GROUP BY owner, token_name
            HAVING COUNT(1) > 1
        ) t
        """,
        """
        SELECT owner, token_name, COUNT(1) AS dup_cnt
        FROM api_tokens
        WHERE token_name <> '' AND revoked_at = ''
        GROUP BY owner, token_name
        HAVING COUNT(1) > 1
        ORDER BY dup_cnt DESC, owner ASC, token_name ASC
        LIMIT 5
        """,
    ),
]


_POSTGRES_RULES: List[Rule] = [
    (
        "strategy_compiler_jobs.status.enum",
        "strategy_compiler_jobs",
        """
        SELECT COUNT(1) AS cnt
        FROM strategy_compiler_jobs
        WHERE COALESCE(status, '') = ''
           OR LOWER(BTRIM(status)) NOT IN ('pending', 'running', 'success', 'failed')
        """,
        """
        SELECT id, status
        FROM strategy_compiler_jobs
        WHERE COALESCE(status, '') = ''
           OR LOWER(BTRIM(status)) NOT IN ('pending', 'running', 'success', 'failed')
        ORDER BY id DESC
        LIMIT 5
        """,
    ),
    (
        "risk_events.event_type.enum",
        "risk_events",
        """
        SELECT COUNT(1) AS cnt
        FROM risk_events
        WHERE COALESCE(event_type, '') = ''
           OR LOWER(BTRIM(event_type)) NOT IN ('triggered', 'recovered', 'manual_update')
        """,
        """
        SELECT id, owner, strategy_key, event_type
        FROM risk_events
        WHERE COALESCE(event_type, '') = ''
           OR LOWER(BTRIM(event_type)) NOT IN ('triggered', 'recovered', 'manual_update')
        ORDER BY id DESC
        LIMIT 5
        """,
    ),
    (
        "runtime_logs.level.enum",
        "runtime_logs",
        """
        SELECT COUNT(1) AS cnt
        FROM runtime_logs
        WHERE COALESCE(level, '') = ''
           OR LOWER(BTRIM(level)) NOT IN ('debug', 'info', 'warn', 'error', 'critical')
        """,
        """
        SELECT id, owner, log_type, level
        FROM runtime_logs
        WHERE COALESCE(level, '') = ''
           OR LOWER(BTRIM(level)) NOT IN ('debug', 'info', 'warn', 'error', 'critical')
        ORDER BY id DESC
        LIMIT 5
        """,
    ),
    (
        "alert_deliveries.status.enum",
        "alert_deliveries",
        """
        SELECT COUNT(1) AS cnt
        FROM alert_deliveries
        WHERE COALESCE(status, '') = ''
           OR LOWER(BTRIM(status)) NOT IN ('sent', 'failed')
        """,
        """
        SELECT id, owner, status
        FROM alert_deliveries
        WHERE COALESCE(status, '') = ''
           OR LOWER(BTRIM(status)) NOT IN ('sent', 'failed')
        ORDER BY id DESC
        LIMIT 5
        """,
    ),
    (
        "alert_deliveries.severity.enum",
        "alert_deliveries",
        """
        SELECT COUNT(1) AS cnt
        FROM alert_deliveries
        WHERE COALESCE(severity, '') = ''
           OR LOWER(BTRIM(severity)) NOT IN ('info', 'warn', 'error', 'critical')
        """,
        """
        SELECT id, owner, severity
        FROM alert_deliveries
        WHERE COALESCE(severity, '') = ''
           OR LOWER(BTRIM(severity)) NOT IN ('info', 'warn', 'error', 'critical')
        ORDER BY id DESC
        LIMIT 5
        """,
    ),
    (
        "alert_deliveries.numeric.check",
        "alert_deliveries",
        """
        SELECT COUNT(1) AS cnt
        FROM alert_deliveries
        WHERE COALESCE(retry_count, -1) < 0
           OR COALESCE(duration_ms, -1) < 0
        """,
        """
        SELECT id, owner, retry_count, duration_ms
        FROM alert_deliveries
        WHERE COALESCE(retry_count, -1) < 0
           OR COALESCE(duration_ms, -1) < 0
        ORDER BY id DESC
        LIMIT 5
        """,
    ),
    (
        "ws_connection_events.refresh_ms.check",
        "ws_connection_events",
        """
        SELECT COUNT(1) AS cnt
        FROM ws_connection_events
        WHERE refresh_ms IS NULL OR refresh_ms < 0
        """,
        """
        SELECT id, owner, event_type, refresh_ms
        FROM ws_connection_events
        WHERE refresh_ms IS NULL OR refresh_ms < 0
        ORDER BY id DESC
        LIMIT 5
        """,
    ),
    (
        "account_security_events.severity.enum",
        "account_security_events",
        """
        SELECT COUNT(1) AS cnt
        FROM account_security_events
        WHERE COALESCE(severity, '') = ''
           OR LOWER(BTRIM(severity)) NOT IN ('info', 'warn', 'error', 'critical')
        """,
        """
        SELECT id, owner, event_type, severity
        FROM account_security_events
        WHERE COALESCE(severity, '') = ''
           OR LOWER(BTRIM(severity)) NOT IN ('info', 'warn', 'error', 'critical')
        ORDER BY id DESC
        LIMIT 5
        """,
    ),
    (
        "api_tokens.active_name.unique",
        "api_tokens",
        """
        SELECT COUNT(1) AS cnt
        FROM (
            SELECT owner, token_name, COUNT(1) AS dup_cnt
            FROM api_tokens
            WHERE token_name <> '' AND revoked_at = ''
            GROUP BY owner, token_name
            HAVING COUNT(1) > 1
        ) t
        """,
        """
        SELECT owner, token_name, COUNT(1) AS dup_cnt
        FROM api_tokens
        WHERE token_name <> '' AND revoked_at = ''
        GROUP BY owner, token_name
        HAVING COUNT(1) > 1
        ORDER BY dup_cnt DESC, owner ASC, token_name ASC
        LIMIT 5
        """,
    ),
]


def _as_jsonable(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def _rows_to_json(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append({str(k): _as_jsonable(v) for k, v in dict(row).items()})
    return out


def _sqlite_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT COUNT(1) AS cnt FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    if row is None:
        return False
    return int(row["cnt"] or 0) > 0


def _postgres_table_exists(cur, table_name: str) -> bool:
    cur.execute(
        """
        SELECT COUNT(1) AS cnt
        FROM information_schema.tables
        WHERE table_schema = current_schema() AND table_name = %s
        """,
        (table_name,),
    )
    row = cur.fetchone()
    if row is None:
        return False
    return int(row["cnt"] or 0) > 0


def _sqlite_validate(db_path: Path) -> Dict[str, Any]:
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000;")
    rules_result: List[Dict[str, Any]] = []
    try:
        for name, table_name, count_sql, sample_sql in _SQLITE_RULES:
            if not _sqlite_table_exists(conn, table_name):
                rules_result.append(
                    {
                        "name": name,
                        "table": table_name,
                        "skipped": True,
                        "reason": "table_not_found",
                        "violations": 0,
                        "sample": [],
                    }
                )
                continue
            count_row = conn.execute(count_sql).fetchone()
            violations = int(count_row["cnt"] if count_row is not None else 0)
            sample_rows = conn.execute(sample_sql).fetchall()
            rules_result.append(
                {
                    "name": name,
                    "table": table_name,
                    "skipped": False,
                    "violations": violations,
                    "sample": _rows_to_json(sample_rows),
                }
            )
    finally:
        conn.close()

    violations_total = int(sum(int(item.get("violations", 0)) for item in rules_result if not item.get("skipped")))
    return {
        "backend": "sqlite",
        "db_path": str(db_path),
        "violations_total": violations_total,
        "rules": rules_result,
        "ok": violations_total == 0,
    }


def _postgres_validate(dsn: str) -> Dict[str, Any]:
    if psycopg is None or dict_row is None:
        raise RuntimeError("postgres mode requires psycopg package")
    rules_result: List[Dict[str, Any]] = []
    with psycopg.connect(dsn, autocommit=False, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            for name, table_name, count_sql, sample_sql in _POSTGRES_RULES:
                if not _postgres_table_exists(cur, table_name):
                    rules_result.append(
                        {
                            "name": name,
                            "table": table_name,
                            "skipped": True,
                            "reason": "table_not_found",
                            "violations": 0,
                            "sample": [],
                        }
                    )
                    continue
                cur.execute(count_sql)
                count_row = cur.fetchone()
                violations = int(count_row["cnt"] if count_row is not None else 0)
                cur.execute(sample_sql)
                sample_rows = cur.fetchall()
                rules_result.append(
                    {
                        "name": name,
                        "table": table_name,
                        "skipped": False,
                        "violations": violations,
                        "sample": _rows_to_json(sample_rows),
                    }
                )

    violations_total = int(sum(int(item.get("violations", 0)) for item in rules_result if not item.get("skipped")))
    return {
        "backend": "postgres",
        "dsn": "<configured>",
        "violations_total": violations_total,
        "rules": rules_result,
        "ok": violations_total == 0,
    }


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate DB enum/check/unique constraints against historical data.")
    parser.add_argument("--backend", choices=["sqlite", "postgres"], default="sqlite")
    parser.add_argument("--db-path", default="logs/quant_api.db", help="SQLite path (used in --backend sqlite)")
    parser.add_argument(
        "--postgres-dsn",
        default=(str(os.getenv("QUANT_E2E_POSTGRES_DSN", "") or os.getenv("API_DB_POSTGRES_DSN", ""))),
        help="PostgreSQL DSN (used in --backend postgres)",
    )
    parser.add_argument(
        "--allow-violations",
        action="store_true",
        help="Always exit 0 and report violations in JSON payload",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    try:
        backend = str(args.backend or "sqlite").strip().lower()
        if backend == "postgres":
            dsn = str(args.postgres_dsn or "").strip()
            if not dsn:
                raise RuntimeError("postgres backend requires --postgres-dsn or QUANT_E2E_POSTGRES_DSN")
            result = _postgres_validate(dsn)
        else:
            db_path = Path(str(args.db_path or "")).expanduser().resolve()
            result = _sqlite_validate(db_path)
    except Exception as exc:
        payload = {
            "ok": False,
            "error": str(exc),
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 1

    print(json.dumps(result, ensure_ascii=False, indent=2))
    if bool(result.get("ok")):
        return 0
    return 0 if bool(args.allow_violations) else 1


if __name__ == "__main__":
    raise SystemExit(main())
