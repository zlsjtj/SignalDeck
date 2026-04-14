#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

_BACKEND_ROOT = Path(__file__).resolve().parents[1]
if str(_BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(_BACKEND_ROOT))

from db_store import SQLiteStore
from postgres_store import PostgresStore


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _normalize_count_map(items: Any, key_field: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    if not isinstance(items, list):
        return out
    for item in items:
        if not isinstance(item, dict):
            continue
        key = str(item.get(key_field) or "")
        if not key:
            continue
        out[key] = out.get(key, 0) + _as_int(item.get("count"))
    return out


def _get_path(summary: Dict[str, Any], path: Sequence[str]) -> Any:
    cur: Any = summary
    for part in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def _load_summary(
    *,
    backend: str,
    sqlite_path: str,
    postgres_dsn: str,
    owner: Optional[str],
    start_ts: Optional[str],
    end_ts: Optional[str],
    limit_top: int,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    backend_text = str(backend or "").strip().lower()
    if backend_text == "sqlite":
        path = Path(str(sqlite_path or "")).expanduser().resolve()
        store = SQLiteStore(path)
        store.initialize()
        summary = store.build_db_report_summary(
            owner=owner,
            start_ts=start_ts,
            end_ts=end_ts,
            limit_top=limit_top,
        )
        meta = {
            "backend": "sqlite",
            "sqlitePath": str(path),
        }
        return meta, summary

    if backend_text == "postgres":
        dsn = str(postgres_dsn or "").strip()
        if not dsn:
            raise RuntimeError("postgres backend requires postgres dsn")
        store = PostgresStore(dsn)
        store.initialize()
        summary = store.build_db_report_summary(
            owner=owner,
            start_ts=start_ts,
            end_ts=end_ts,
            limit_top=limit_top,
        )
        meta = {
            "backend": "postgres",
            "postgresDsn": "<configured>",
        }
        return meta, summary

    raise RuntimeError(f"unsupported backend: {backend_text}")


def _compare_summaries(left: Dict[str, Any], right: Dict[str, Any]) -> List[Dict[str, Any]]:
    diffs: List[Dict[str, Any]] = []

    scalar_paths = [
        ("auditTotal",),
        ("riskEventTotal",),
        ("riskStateHistoryTotal",),
        ("alertDeliveryTotal",),
        ("alertDeliveryFailedTotal",),
        ("wsConnectionEventTotal",),
        ("marketTimeseries", "ticks"),
        ("marketTimeseries", "klines"),
    ]
    for path in scalar_paths:
        left_val = _as_int(_get_path(left, path))
        right_val = _as_int(_get_path(right, path))
        if left_val != right_val:
            diffs.append(
                {
                    "type": "scalar",
                    "path": ".".join(path),
                    "left": left_val,
                    "right": right_val,
                    "delta": left_val - right_val,
                }
            )

    list_specs = [
        ("topActions", "action"),
        ("topEntities", "entity"),
        ("riskEventsByType", "eventType"),
        ("alertDeliveriesByEvent", "event"),
        ("wsEventsByType", "eventType"),
    ]
    for field, key_field in list_specs:
        left_map = _normalize_count_map(left.get(field), key_field)
        right_map = _normalize_count_map(right.get(field), key_field)
        keys = sorted(set(left_map.keys()) | set(right_map.keys()))
        for key in keys:
            lv = _as_int(left_map.get(key))
            rv = _as_int(right_map.get(key))
            if lv != rv:
                diffs.append(
                    {
                        "type": "distribution",
                        "path": field,
                        "key": key,
                        "left": lv,
                        "right": rv,
                        "delta": lv - rv,
                    }
                )

    return diffs


def _emit_webhook(
    *,
    webhook_url: str,
    payload: Dict[str, Any],
    timeout_seconds: float,
) -> Tuple[bool, str, Optional[int]]:
    url = str(webhook_url or "").strip()
    if not url:
        return False, "", None
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=body,
        headers={"Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=max(1.0, float(timeout_seconds))) as resp:
            code = int(getattr(resp, "status", 200) or 200)
            return 200 <= code < 300, "", code
    except urllib.error.HTTPError as exc:
        return False, str(exc), int(getattr(exc, "code", 0) or 0)
    except Exception as exc:
        return False, str(exc), None


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconcile DB report summary between two DB backends.")
    parser.add_argument("--left-backend", choices=["sqlite", "postgres"], default="sqlite")
    parser.add_argument("--left-sqlite-path", default="logs/quant_api.db")
    parser.add_argument(
        "--left-postgres-dsn",
        default=str(os.getenv("QUANT_E2E_POSTGRES_DSN", "") or os.getenv("API_DB_POSTGRES_DSN", "")),
    )
    parser.add_argument("--right-backend", choices=["sqlite", "postgres"], default="postgres")
    parser.add_argument("--right-sqlite-path", default="logs/quant_api.db")
    parser.add_argument(
        "--right-postgres-dsn",
        default=str(os.getenv("QUANT_E2E_POSTGRES_DSN", "") or os.getenv("API_DB_POSTGRES_DSN", "")),
    )
    parser.add_argument("--owner", default="")
    parser.add_argument("--start-ts", default="")
    parser.add_argument("--end-ts", default="")
    parser.add_argument("--limit-top", type=int, default=10)
    parser.add_argument("--alert-webhook-url", default="")
    parser.add_argument("--alert-timeout-seconds", type=float, default=5.0)
    parser.add_argument("--allow-diff", action="store_true")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    owner = str(args.owner or "").strip() or None
    start_ts = str(args.start_ts or "").strip() or None
    end_ts = str(args.end_ts or "").strip() or None
    safe_top = max(1, min(int(args.limit_top), 100))

    try:
        left_meta, left_summary = _load_summary(
            backend=str(args.left_backend),
            sqlite_path=str(args.left_sqlite_path),
            postgres_dsn=str(args.left_postgres_dsn),
            owner=owner,
            start_ts=start_ts,
            end_ts=end_ts,
            limit_top=safe_top,
        )
        right_meta, right_summary = _load_summary(
            backend=str(args.right_backend),
            sqlite_path=str(args.right_sqlite_path),
            postgres_dsn=str(args.right_postgres_dsn),
            owner=owner,
            start_ts=start_ts,
            end_ts=end_ts,
            limit_top=safe_top,
        )
    except Exception as exc:
        payload = {
            "ok": False,
            "error": str(exc),
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 1

    diffs = _compare_summaries(left_summary, right_summary)
    ok = len(diffs) == 0

    alert_status = {
        "attempted": False,
        "sent": False,
        "httpStatus": None,
        "error": "",
    }
    webhook = str(args.alert_webhook_url or "").strip()
    if (not ok) and webhook:
        alert_status["attempted"] = True
        sent, err, status_code = _emit_webhook(
            webhook_url=webhook,
            timeout_seconds=float(args.alert_timeout_seconds),
            payload={
                "event": "cross_db_summary_reconcile_mismatch",
                "severity": "critical",
                "ts": _now_iso(),
                "diffCount": len(diffs),
                "owner": owner or "",
                "startTs": start_ts or "",
                "endTs": end_ts or "",
                "left": left_meta,
                "right": right_meta,
                "diffs": diffs[:100],
            },
        )
        alert_status["sent"] = bool(sent)
        alert_status["httpStatus"] = int(status_code) if status_code is not None else None
        alert_status["error"] = str(err or "")

    payload = {
        "ok": bool(ok),
        "ts": _now_iso(),
        "owner": owner or "",
        "startTs": start_ts or "",
        "endTs": end_ts or "",
        "left": left_meta,
        "right": right_meta,
        "diffCount": len(diffs),
        "diffs": diffs,
        "alert": alert_status,
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))

    if ok:
        return 0
    return 0 if bool(args.allow_diff) else 1


if __name__ == "__main__":
    raise SystemExit(main())
