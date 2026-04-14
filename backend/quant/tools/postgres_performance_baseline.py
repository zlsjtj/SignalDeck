#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from postgres_store import PostgresStore

try:  # pragma: no cover - environment dependent
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_ms(seconds: float) -> float:
    return round(float(seconds) * 1000.0, 4)


def _pctl(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, int(round((len(ordered) - 1) * p))))
    return float(ordered[idx])


def _connect(dsn: str):
    if psycopg is None or dict_row is None:
        raise RuntimeError("postgres performance baseline requires psycopg package")
    return psycopg.connect(dsn, autocommit=False, row_factory=dict_row)


def _seed_if_needed(dsn: str, *, seed_rows: int, owner: str) -> Dict[str, Any]:
    if seed_rows <= 0:
        return {"requested": 0, "insertedAudit": 0, "insertedRisk": 0}

    inserted_audit = 0
    inserted_risk = 0
    with _connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(1) AS cnt FROM audit_logs")
            current_audit = int((cur.fetchone() or {}).get("cnt") or 0)
            need_audit = max(0, int(seed_rows) - current_audit)
            if need_audit > 0:
                cur.execute(
                    """
                    INSERT INTO audit_logs (
                        ts_utc, owner, action, entity, entity_id, detail_json, prev_hash, row_hash, chain_version
                    )
                    SELECT
                        CURRENT_TIMESTAMP::TEXT,
                        %s,
                        'perf.seed.action',
                        'perf_seed',
                        ('perf_seed_' || gs::text),
                        '{}',
                        REPEAT('0', 64),
                        MD5((random())::text),
                        1
                    FROM generate_series(1, %s) AS gs
                    """,
                    (owner, need_audit),
                )
                inserted_audit = int(cur.rowcount or 0)

            cur.execute("SELECT COUNT(1) AS cnt FROM risk_events")
            current_risk = int((cur.fetchone() or {}).get("cnt") or 0)
            need_risk = max(0, int(seed_rows) - current_risk)
            if need_risk > 0:
                cur.execute(
                    """
                    INSERT INTO risk_events (
                        ts_utc, owner, strategy_key, event_type, rule, message, detail_json
                    )
                    SELECT
                        CURRENT_TIMESTAMP::TEXT,
                        %s,
                        ('usr__' || %s || '__perf_seed_' || gs::text),
                        'manual_update',
                        'perf_seed',
                        'seed event',
                        '{}'
                    FROM generate_series(1, %s) AS gs
                    """,
                    (owner, owner, need_risk),
                )
                inserted_risk = int(cur.rowcount or 0)
        conn.commit()

    return {
        "requested": int(seed_rows),
        "insertedAudit": inserted_audit,
        "insertedRisk": inserted_risk,
    }


def _measure_write_tps(
    dsn: str,
    *,
    write_ops: int,
    commit_every: int,
    owner: str,
) -> Dict[str, Any]:
    ops = max(1, int(write_ops))
    every = max(1, int(commit_every))
    started = time.perf_counter()

    with _connect(dsn) as conn:
        with conn.cursor() as cur:
            for idx in range(ops):
                cur.execute(
                    """
                    INSERT INTO runtime_logs (
                        ts_utc, owner, log_type, level, source, message, strategy_id, backtest_id, detail_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        _now_iso(),
                        owner,
                        "system",
                        "info",
                        "perf_baseline",
                        f"perf write {idx}",
                        "",
                        "",
                        "{}",
                    ),
                )
                if (idx + 1) % every == 0:
                    conn.commit()
        conn.commit()

    elapsed = max(time.perf_counter() - started, 1e-9)
    tps = float(ops) / float(elapsed)
    return {
        "operations": ops,
        "commitEvery": every,
        "elapsedMs": _as_ms(elapsed),
        "tps": round(tps, 4),
    }


def _measure_pagination_p95(
    dsn: str,
    *,
    queries: int,
    page_size: int,
    seed: int,
) -> Dict[str, Any]:
    q = max(1, int(queries))
    size = max(1, int(page_size))
    rng = random.Random(int(seed))

    latencies_ms: List[float] = []
    max_offset = 0
    total_rows = 0
    with _connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(1) AS cnt FROM audit_logs")
            total_rows = int((cur.fetchone() or {}).get("cnt") or 0)
            max_offset = max(0, total_rows - size)

            for _ in range(q):
                offset = rng.randint(0, max_offset) if max_offset > 0 else 0
                started = time.perf_counter()
                cur.execute(
                    """
                    SELECT id, ts_utc, owner, action, entity, entity_id
                    FROM audit_logs
                    ORDER BY id DESC
                    LIMIT %s OFFSET %s
                    """,
                    (size, offset),
                )
                cur.fetchall()
                latencies_ms.append(_as_ms(time.perf_counter() - started))

    return {
        "queries": q,
        "pageSize": size,
        "totalRows": int(total_rows),
        "maxOffset": int(max_offset),
        "latencyMs": {
            "min": min(latencies_ms) if latencies_ms else 0.0,
            "p50": _pctl(latencies_ms, 0.50),
            "p95": _pctl(latencies_ms, 0.95),
            "p99": _pctl(latencies_ms, 0.99),
            "max": max(latencies_ms) if latencies_ms else 0.0,
        },
    }


def _measure_report_p95(store: PostgresStore, *, queries: int, limit_top: int) -> Dict[str, Any]:
    q = max(1, int(queries))
    safe_top = max(1, min(int(limit_top), 100))

    latencies_ms: List[float] = []
    sample_summary: Dict[str, Any] = {}
    for _ in range(q):
        started = time.perf_counter()
        summary = store.build_db_report_summary(owner=None, start_ts=None, end_ts=None, limit_top=safe_top)
        latencies_ms.append(_as_ms(time.perf_counter() - started))
        if not sample_summary:
            sample_summary = {
                "auditTotal": int(summary.get("auditTotal") or 0),
                "riskEventTotal": int(summary.get("riskEventTotal") or 0),
                "riskStateHistoryTotal": int(summary.get("riskStateHistoryTotal") or 0),
            }

    return {
        "queries": q,
        "limitTop": safe_top,
        "latencyMs": {
            "min": min(latencies_ms) if latencies_ms else 0.0,
            "p50": _pctl(latencies_ms, 0.50),
            "p95": _pctl(latencies_ms, 0.95),
            "p99": _pctl(latencies_ms, 0.99),
            "max": max(latencies_ms) if latencies_ms else 0.0,
        },
        "sampleSummary": sample_summary,
    }


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PostgreSQL performance baseline for write TPS and query P95")
    parser.add_argument(
        "--postgres-dsn",
        default=str(os.getenv("QUANT_E2E_POSTGRES_DSN", "") or os.getenv("API_DB_POSTGRES_DSN", "")),
        help="PostgreSQL DSN",
    )
    parser.add_argument("--seed-rows", type=int, default=2000, help="Target seed rows for audit/risk tables")
    parser.add_argument("--write-ops", type=int, default=1200, help="Number of runtime log writes for TPS metric")
    parser.add_argument("--commit-every", type=int, default=50, help="Commit interval for write benchmark")
    parser.add_argument("--pagination-queries", type=int, default=200, help="Number of pagination queries")
    parser.add_argument("--page-size", type=int, default=50, help="Pagination query page size")
    parser.add_argument("--report-queries", type=int, default=80, help="Number of db summary aggregation queries")
    parser.add_argument("--limit-top", type=int, default=10, help="limit_top for db summary query")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for pagination offsets")
    parser.add_argument("--owner", default="perf_baseline", help="Owner key used for synthetic writes")

    parser.add_argument("--min-write-tps", type=float, default=50.0, help="Pass criterion: minimal write TPS")
    parser.add_argument(
        "--max-pagination-p95-ms",
        type=float,
        default=120.0,
        help="Pass criterion: maximal pagination query P95 in ms",
    )
    parser.add_argument(
        "--max-report-p95-ms",
        type=float,
        default=250.0,
        help="Pass criterion: maximal report aggregation P95 in ms",
    )
    parser.add_argument("--allow-threshold-fail", action="store_true", help="Always exit 0 even if criteria failed")
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

    if psycopg is None or dict_row is None:
        payload = {
            "ok": False,
            "error": "psycopg package is required for postgres performance baseline",
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 1

    try:
        store = PostgresStore(dsn)
        store.initialize()

        seed_report = _seed_if_needed(
            dsn,
            seed_rows=int(args.seed_rows),
            owner=str(args.owner or "perf_baseline"),
        )
        write_report = _measure_write_tps(
            dsn,
            write_ops=int(args.write_ops),
            commit_every=int(args.commit_every),
            owner=str(args.owner or "perf_baseline"),
        )
        pagination_report = _measure_pagination_p95(
            dsn,
            queries=int(args.pagination_queries),
            page_size=int(args.page_size),
            seed=int(args.seed),
        )
        report_query_report = _measure_report_p95(
            store,
            queries=int(args.report_queries),
            limit_top=int(args.limit_top),
        )

        min_write_tps = float(args.min_write_tps)
        max_pagination_p95 = float(args.max_pagination_p95_ms)
        max_report_p95 = float(args.max_report_p95_ms)

        write_ok = float(write_report.get("tps") or 0.0) >= min_write_tps
        pagination_p95 = float((pagination_report.get("latencyMs") or {}).get("p95") or 0.0)
        report_p95 = float((report_query_report.get("latencyMs") or {}).get("p95") or 0.0)
        pagination_ok = pagination_p95 <= max_pagination_p95
        report_ok = report_p95 <= max_report_p95

        payload = {
            "ok": bool(write_ok and pagination_ok and report_ok),
            "ts": _now_iso(),
            "postgresDsn": "<configured>",
            "seed": seed_report,
            "metrics": {
                "write": write_report,
                "pagination": pagination_report,
                "report": report_query_report,
            },
            "criteria": {
                "minWriteTps": min_write_tps,
                "maxPaginationP95Ms": max_pagination_p95,
                "maxReportP95Ms": max_report_p95,
            },
            "checks": {
                "writeTps": bool(write_ok),
                "paginationP95": bool(pagination_ok),
                "reportP95": bool(report_ok),
            },
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))

        if bool(payload["ok"]):
            return 0
        return 0 if bool(args.allow_threshold_fail) else 2
    except Exception as exc:
        payload = {
            "ok": False,
            "error": str(exc),
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
