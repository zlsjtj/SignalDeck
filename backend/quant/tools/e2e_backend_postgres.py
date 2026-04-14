#!/usr/bin/env python3
"""
End-to-end smoke script for backend + real PostgreSQL integration.

Validation scope:
1) PostgreSQL connectivity and schema migration readiness.
2) Strategy/risk/audit read-write persistence on PostgreSQL.
3) Runtime DB switch path: postgres -> sqlite -> postgres with state preserved.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import quote_plus

import requests


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pick_free_port(host: str) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def _tail_lines(text: str, max_lines: int = 120) -> List[str]:
    lines = [line.rstrip("\n") for line in str(text or "").splitlines()]
    if len(lines) <= max_lines:
        return lines
    return lines[-max_lines:]


def _mask_postgres_dsn(dsn: str) -> str:
    text = str(dsn or "").strip()
    if not text:
        return ""
    if "@" not in text:
        return "<configured>"
    prefix, suffix = text.rsplit("@", 1)
    if "://" in prefix:
        scheme, rest = prefix.split("://", 1)
        if ":" in rest:
            user, _ = rest.split(":", 1)
            return f"{scheme}://{user}:***@{suffix}"
        return f"{scheme}://***@{suffix}"
    return f"***@{suffix}"


def _request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    timeout_seconds: float,
    **kwargs: Any,
) -> Tuple[int, Any, str]:
    resp = session.request(method=method, url=url, timeout=timeout_seconds, **kwargs)
    text = resp.text
    try:
        payload = resp.json()
    except Exception:
        payload = None
    return int(resp.status_code), payload, text


def _wait_backend_ready(
    session: requests.Session,
    health_url: str,
    *,
    startup_timeout_seconds: float,
    http_timeout_seconds: float,
) -> Dict[str, Any]:
    deadline = time.time() + startup_timeout_seconds
    last_error = "not started"
    while time.time() < deadline:
        try:
            status, payload, body = _request_json(
                session,
                "GET",
                health_url,
                timeout_seconds=http_timeout_seconds,
            )
            if status == 200 and isinstance(payload, dict):
                return payload
            last_error = f"status={status}, body={body[:240]}"
        except Exception as exc:
            last_error = str(exc)
        time.sleep(0.3)
    raise RuntimeError(f"backend health check timeout: {last_error}")


def _import_psycopg():
    try:
        import psycopg  # type: ignore
    except Exception as exc:  # pragma: no cover - runtime environment dependent
        raise RuntimeError(
            "psycopg is required for PostgreSQL E2E, install with: pip install -r requirements-postgres.txt"
        ) from exc
    return psycopg


def _wait_postgres_ready(dsn: str, *, timeout_seconds: float) -> None:
    psycopg = _import_psycopg()
    deadline = time.time() + timeout_seconds
    last_error = "postgres not ready"
    while time.time() < deadline:
        try:
            with psycopg.connect(dsn, autocommit=True, connect_timeout=3) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            return
        except Exception as exc:
            last_error = str(exc)
        time.sleep(0.5)
    raise RuntimeError(f"postgres readiness timeout: {last_error}")


def _postgres_scalar(dsn: str, sql: str, params: Sequence[Any] = ()) -> Any:
    psycopg = _import_psycopg()
    with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            row = cur.fetchone()
            if row is None:
                return None
            return row[0]


def _postgres_counts(dsn: str) -> Dict[str, int]:
    tables = ["strategies", "backtests", "risk_states", "risk_events", "audit_logs"]
    out: Dict[str, int] = {}
    for table in tables:
        exists = int(
            _postgres_scalar(
                dsn,
                """
                SELECT COUNT(1)
                FROM information_schema.tables
                WHERE table_schema = current_schema() AND table_name = %s
                """,
                (table,),
            )
            or 0
        )
        if exists <= 0:
            out[table] = 0
            continue
        count = int(_postgres_scalar(dsn, f'SELECT COUNT(1) FROM "{table}"') or 0)
        out[table] = count
    return out


def _sqlite_counts(path: Path) -> Dict[str, int]:
    tables = ["strategies", "backtests", "risk_states", "risk_events", "audit_logs"]
    out: Dict[str, int] = {}
    if not path.exists():
        return out
    with sqlite3.connect(str(path), timeout=30.0) as conn:
        for table in tables:
            exists_row = conn.execute(
                "SELECT COUNT(1) FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            exists = int(exists_row[0]) > 0 if exists_row is not None else False
            if not exists:
                out[table] = 0
                continue
            cnt_row = conn.execute(f'SELECT COUNT(1) FROM "{table}"').fetchone()
            out[table] = int(cnt_row[0]) if cnt_row is not None else 0
    return out


def _run_command(cmd: Sequence[str], *, timeout_seconds: float = 30.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(cmd),
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout_seconds,
    )


def _start_temporary_postgres_container(args: argparse.Namespace) -> Dict[str, Any]:
    if shutil.which("docker") is None:
        raise RuntimeError("docker binary is required for --use-docker-postgres")

    host_port = int(args.postgres_port) if int(args.postgres_port) > 0 else _pick_free_port("127.0.0.1")
    container_name = str(args.docker_container_name or "").strip() or f"quant-e2e-pg-{int(time.time())}-{os.getpid()}"
    image = str(args.docker_image)

    run_cmd = [
        "docker",
        "run",
        "--rm",
        "-d",
        "--name",
        container_name,
        "-e",
        f"POSTGRES_USER={args.postgres_user}",
        "-e",
        f"POSTGRES_PASSWORD={args.postgres_password}",
        "-e",
        f"POSTGRES_DB={args.postgres_database}",
        "-p",
        f"{host_port}:5432",
        image,
    ]
    completed = _run_command(run_cmd, timeout_seconds=45.0)
    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()
        raise RuntimeError(f"failed to start docker postgres: {detail[:400]}")

    dsn = (
        f"postgresql://{quote_plus(str(args.postgres_user))}:{quote_plus(str(args.postgres_password))}"
        f"@127.0.0.1:{host_port}/{quote_plus(str(args.postgres_database))}"
    )
    return {
        "enabled": True,
        "container_name": container_name,
        "image": image,
        "host_port": host_port,
        "dsn": dsn,
    }


def _stop_temporary_postgres_container(container_name: str) -> None:
    if not container_name:
        return
    _run_command(["docker", "rm", "-f", container_name], timeout_seconds=30.0)


def _resolve_postgres_runtime(args: argparse.Namespace) -> Tuple[str, str, Optional[Dict[str, Any]]]:
    arg_dsn = str(args.postgres_dsn or "").strip()
    if arg_dsn:
        return arg_dsn, "arg", None

    env_dsn = str(os.getenv("QUANT_E2E_POSTGRES_DSN", "") or os.getenv("API_DB_POSTGRES_DSN", "")).strip()
    if env_dsn:
        return env_dsn, "env", None

    if bool(args.use_docker_postgres):
        docker_meta = _start_temporary_postgres_container(args)
        return str(docker_meta["dsn"]), "docker", docker_meta

    raise RuntimeError(
        "postgres dsn is required; pass --postgres-dsn, set QUANT_E2E_POSTGRES_DSN, or use --use-docker-postgres"
    )


def _terminate_process(proc: subprocess.Popen[str], timeout_seconds: float = 8.0) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=timeout_seconds)
        return
    except subprocess.TimeoutExpired:
        pass
    proc.kill()
    proc.wait(timeout=timeout_seconds)


def run(args: argparse.Namespace) -> Dict[str, Any]:
    backend_root = Path(args.backend_root).expanduser().resolve()
    if not backend_root.exists():
        raise RuntimeError(f"backend root not found: {backend_root}")

    report: Dict[str, Any] = {
        "ok": False,
        "ts_utc": _now_iso(),
        "backend": {
            "root": str(backend_root),
            "base_url": "",
            "health": {},
            "steps": [],
            "strategy_id": "",
            "server_log_tail": [],
        },
        "postgres": {
            "source": "",
            "dsn_masked": "",
            "schema_version_max": 0,
            "pre_counts": {},
            "post_counts": {},
            "docker": {"enabled": False},
        },
        "runtime_switch": {
            "sqlite_db_path": "",
            "sqlite_counts": {},
        },
    }

    docker_meta: Optional[Dict[str, Any]] = None
    server: Optional[subprocess.Popen[str]] = None
    try:
        dsn, source, docker_meta = _resolve_postgres_runtime(args)
        report["postgres"]["source"] = source
        report["postgres"]["dsn_masked"] = _mask_postgres_dsn(dsn)
        if docker_meta:
            report["postgres"]["docker"] = {k: v for k, v in docker_meta.items() if k != "dsn"}

        _wait_postgres_ready(dsn, timeout_seconds=float(args.postgres_ready_timeout_seconds))
        report["backend"]["steps"].append("postgres.ready")
        report["postgres"]["pre_counts"] = _postgres_counts(dsn)

        with tempfile.TemporaryDirectory(prefix="quant_pg_e2e_") as tmp_dir:
            temp_root = Path(tmp_dir)
            sqlite_reload_path = (temp_root / "runtime_reload.sqlite3").resolve()
            report["runtime_switch"]["sqlite_db_path"] = str(sqlite_reload_path)

            host = str(args.host)
            port = int(args.port) if int(args.port) > 0 else _pick_free_port(host)
            base_url = f"http://{host}:{port}/api"
            report["backend"]["base_url"] = base_url

            env = os.environ.copy()
            env["API_DB_ENABLED"] = "true"
            env["API_DB_BACKEND"] = "postgres"
            env["API_DB_POSTGRES_DSN"] = dsn
            env["API_AUTH_REQUIRED"] = "false"
            env["PYTHONUNBUFFERED"] = "1"

            server_cmd = [
                str(args.python_bin),
                "-m",
                "uvicorn",
                "api_server:app",
                "--host",
                host,
                "--port",
                str(port),
                "--log-level",
                str(args.server_log_level),
            ]

            server = subprocess.Popen(
                server_cmd,
                cwd=str(backend_root),
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )

            session = requests.Session()
            health = _wait_backend_ready(
                session,
                f"{base_url}/health",
                startup_timeout_seconds=float(args.startup_timeout_seconds),
                http_timeout_seconds=float(args.http_timeout_seconds),
            )
            report["backend"]["health"] = health
            if str(health.get("db_backend")) != "postgres":
                raise RuntimeError(f"unexpected db_backend on startup: {health.get('db_backend')}")
            if str(health.get("db")) in {"error", "disabled"}:
                raise RuntimeError(f"unexpected db status on startup: {health.get('db')}")
            report["backend"]["steps"].append("health.postgres.ok")

            status, payload, body = _request_json(
                session,
                "GET",
                f"{base_url}/admin/db/config",
                timeout_seconds=float(args.http_timeout_seconds),
            )
            if status != 200 or not isinstance(payload, dict):
                raise RuntimeError(f"admin db config failed: status={status}, body={body[:400]}")
            if str(payload.get("backend")) != "postgres":
                raise RuntimeError(f"admin db config backend mismatch: {payload.get('backend')}")
            report["backend"]["steps"].append("admin.db.config")

            schema_version = int(_postgres_scalar(dsn, "SELECT COALESCE(MAX(version), 0) FROM schema_version") or 0)
            report["postgres"]["schema_version_max"] = schema_version
            if schema_version < int(args.min_schema_version):
                raise RuntimeError(
                    f"schema_version too old: got={schema_version}, required>={int(args.min_schema_version)}"
                )
            report["backend"]["steps"].append("postgres.migration.ok")

            now_tag = int(time.time() * 1000)
            strategy_payload = {
                "name": f"e2e-postgres-{now_tag}",
                "type": "custom",
                "config": {
                    "symbols": ["BTC/USDT:USDT"],
                    "timeframe": "1h",
                    "params": {"smoke": True, "postgres": True, "runTag": now_tag},
                },
            }
            status, payload, body = _request_json(
                session,
                "POST",
                f"{base_url}/strategies",
                timeout_seconds=float(args.http_timeout_seconds),
                json=strategy_payload,
            )
            if status != 200 or not isinstance(payload, dict) or not payload.get("id"):
                raise RuntimeError(f"create strategy failed: status={status}, body={body[:400]}")
            strategy_id = str(payload["id"])
            report["backend"]["strategy_id"] = strategy_id
            report["backend"]["steps"].append("strategy.create")

            strategy_count = int(
                _postgres_scalar(dsn, "SELECT COUNT(1) FROM strategies WHERE strategy_id = %s", (strategy_id,)) or 0
            )
            if strategy_count < 1:
                raise RuntimeError("postgres strategies table missing created strategy")
            report["backend"]["steps"].append("postgres.strategy.persisted")

            risk_update_payload = {
                "maxDrawdownPct": 0.111,
                "triggered": [
                    {
                        "rule": "max_drawdown",
                        "ts": _now_iso(),
                        "message": "e2e postgres runtime risk trigger",
                    }
                ],
            }
            status, payload, body = _request_json(
                session,
                "PUT",
                f"{base_url}/risk",
                timeout_seconds=float(args.http_timeout_seconds),
                params={"strategy_id": strategy_id},
                json=risk_update_payload,
            )
            if status != 200 or not isinstance(payload, dict):
                raise RuntimeError(f"update risk failed: status={status}, body={body[:400]}")
            report["backend"]["steps"].append("risk.update")

            risk_event_count = int(
                _postgres_scalar(dsn, "SELECT COUNT(1) FROM risk_events WHERE strategy_key = %s", (strategy_id,)) or 0
            )
            if risk_event_count < 1:
                raise RuntimeError("postgres risk_events table missing inserted event")
            report["backend"]["steps"].append("postgres.risk_event.persisted")

            status, payload, body = _request_json(
                session,
                "GET",
                f"{base_url}/risk/events",
                timeout_seconds=float(args.http_timeout_seconds),
                params={"strategy_id": strategy_id, "limit": 50},
            )
            if status != 200 or not isinstance(payload, list):
                raise RuntimeError(f"query risk events failed: status={status}, body={body[:400]}")
            if not payload:
                raise RuntimeError("risk events are empty after risk update")
            report["backend"]["steps"].append("risk.events.read")

            status, payload, body = _request_json(
                session,
                "GET",
                f"{base_url}/audit/logs",
                timeout_seconds=float(args.http_timeout_seconds),
                params={"limit": 200, "action": "strategy.create"},
            )
            if status != 200 or not isinstance(payload, list):
                raise RuntimeError(f"query audit logs failed: status={status}, body={body[:400]}")
            strategy_create_events = [
                item for item in payload if isinstance(item, dict) and str(item.get("action")) == "strategy.create"
            ]
            if not strategy_create_events:
                raise RuntimeError("audit logs missing strategy.create events")
            matched_entity = any(str(item.get("entity_id")) == strategy_id for item in strategy_create_events)
            audit_count = int(_postgres_scalar(dsn, "SELECT COUNT(1) FROM audit_logs WHERE action = %s", ("strategy.create",)) or 0)
            if not matched_entity and audit_count <= 0:
                raise RuntimeError("audit logs missing persisted strategy.create evidence")
            report["backend"]["steps"].append("audit.read")

            status, payload, body = _request_json(
                session,
                "POST",
                f"{base_url}/admin/db/reload",
                timeout_seconds=float(args.http_timeout_seconds),
                json={
                    "enabled": True,
                    "backend": "sqlite",
                    "dbPath": str(sqlite_reload_path),
                    "preserveState": True,
                },
            )
            if status != 200 or not isinstance(payload, dict):
                raise RuntimeError(f"db reload to sqlite failed: status={status}, body={body[:400]}")
            current_payload = payload.get("current") if isinstance(payload, dict) else {}
            if not isinstance(current_payload, dict) or str(current_payload.get("backend")) != "sqlite":
                raise RuntimeError(f"db reload to sqlite unexpected payload: {payload}")
            report["backend"]["steps"].append("db.reload.sqlite")

            status, payload, body = _request_json(
                session,
                "GET",
                f"{base_url}/health",
                timeout_seconds=float(args.http_timeout_seconds),
            )
            if status != 200 or not isinstance(payload, dict) or str(payload.get("db_backend")) != "sqlite":
                raise RuntimeError(f"health after sqlite reload failed: status={status}, body={body[:400]}")
            report["backend"]["steps"].append("health.sqlite.ok")

            status, payload, body = _request_json(
                session,
                "GET",
                f"{base_url}/strategies",
                timeout_seconds=float(args.http_timeout_seconds),
            )
            if status != 200 or not isinstance(payload, list):
                raise RuntimeError(f"query strategies after sqlite reload failed: status={status}, body={body[:400]}")
            if not any(str(item.get("id")) == strategy_id for item in payload if isinstance(item, dict)):
                raise RuntimeError("strategy missing after sqlite reload with preserveState")
            report["backend"]["steps"].append("strategy.read.after.sqlite_reload")
            report["runtime_switch"]["sqlite_counts"] = _sqlite_counts(sqlite_reload_path)

            status, payload, body = _request_json(
                session,
                "POST",
                f"{base_url}/admin/db/reload",
                timeout_seconds=float(args.http_timeout_seconds),
                json={
                    "enabled": True,
                    "backend": "postgres",
                    "postgresDsn": dsn,
                    "preserveState": True,
                },
            )
            if status != 200 or not isinstance(payload, dict):
                raise RuntimeError(f"db reload back to postgres failed: status={status}, body={body[:400]}")
            current_payload = payload.get("current") if isinstance(payload, dict) else {}
            if not isinstance(current_payload, dict) or str(current_payload.get("backend")) != "postgres":
                raise RuntimeError(f"db reload to postgres unexpected payload: {payload}")
            report["backend"]["steps"].append("db.reload.postgres")

            status, payload, body = _request_json(
                session,
                "GET",
                f"{base_url}/health",
                timeout_seconds=float(args.http_timeout_seconds),
            )
            if status != 200 or not isinstance(payload, dict) or str(payload.get("db_backend")) != "postgres":
                raise RuntimeError(f"health after postgres reload failed: status={status}, body={body[:400]}")
            report["backend"]["steps"].append("health.postgres.reloaded")

            status, payload, body = _request_json(
                session,
                "GET",
                f"{base_url}/strategies",
                timeout_seconds=float(args.http_timeout_seconds),
            )
            if status != 200 or not isinstance(payload, list):
                raise RuntimeError(f"query strategies after postgres reload failed: status={status}, body={body[:400]}")
            if not any(str(item.get("id")) == strategy_id for item in payload if isinstance(item, dict)):
                raise RuntimeError("strategy missing after postgres reload with preserveState")
            report["backend"]["steps"].append("strategy.read.after.postgres_reload")

            status, payload, body = _request_json(
                session,
                "GET",
                f"{base_url}/reports/db/summary",
                timeout_seconds=float(args.http_timeout_seconds),
                params={"limit_top": 3},
            )
            if status != 200 or not isinstance(payload, dict):
                raise RuntimeError(f"query db summary failed: status={status}, body={body[:400]}")
            required_summary_keys = {"auditTotal", "topActions", "topEntities", "riskEventTotal", "riskEventsByType"}
            missing_summary = sorted(key for key in required_summary_keys if key not in payload)
            if missing_summary:
                raise RuntimeError(f"db summary payload missing keys: {missing_summary}")
            report["backend"]["steps"].append("reports.summary.read")

            status, payload, body = _request_json(
                session,
                "DELETE",
                f"{base_url}/strategies/{strategy_id}",
                timeout_seconds=float(args.http_timeout_seconds),
            )
            if status != 200 or not isinstance(payload, dict) or not bool(payload.get("deleted")):
                raise RuntimeError(f"delete strategy failed: status={status}, body={body[:400]}")
            report["backend"]["steps"].append("strategy.delete")

            report["postgres"]["post_counts"] = _postgres_counts(dsn)
            report["ok"] = True
            return report
    finally:
        if server is not None:
            _terminate_process(server)
            try:
                output = ""
                if server.stdout is not None:
                    output = server.stdout.read()
                report["backend"]["server_log_tail"] = _tail_lines(output)
            except Exception:
                report["backend"]["server_log_tail"] = []

        if docker_meta and not bool(args.keep_docker_postgres):
            _stop_temporary_postgres_container(str(docker_meta.get("container_name") or ""))


def _build_parser() -> argparse.ArgumentParser:
    backend_root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Run backend + PostgreSQL end-to-end smoke check")
    parser.add_argument("--backend-root", default=str(backend_root), help="Backend project root")
    parser.add_argument("--python-bin", default=sys.executable, help="Python executable used to start uvicorn")
    parser.add_argument("--host", default="127.0.0.1", help="Backend bind host")
    parser.add_argument("--port", type=int, default=0, help="Backend bind port, 0 means auto-pick free port")
    parser.add_argument("--server-log-level", default="warning", help="uvicorn log level")
    parser.add_argument("--startup-timeout-seconds", type=float, default=45.0, help="Backend startup timeout")
    parser.add_argument("--http-timeout-seconds", type=float, default=12.0, help="Single HTTP request timeout")
    parser.add_argument("--postgres-ready-timeout-seconds", type=float, default=45.0, help="Postgres ready timeout")
    parser.add_argument(
        "--postgres-dsn",
        default="",
        help="PostgreSQL DSN (fallback to QUANT_E2E_POSTGRES_DSN/API_DB_POSTGRES_DSN if empty)",
    )
    parser.add_argument(
        "--min-schema-version",
        type=int,
        default=13,
        help="Required minimal schema_version after migration",
    )
    parser.add_argument(
        "--use-docker-postgres",
        action="store_true",
        help="Auto start temporary postgres container when DSN is not provided",
    )
    parser.add_argument("--docker-image", default="postgres:16-alpine", help="Docker image for temporary postgres")
    parser.add_argument("--docker-container-name", default="", help="Optional docker container name override")
    parser.add_argument(
        "--keep-docker-postgres",
        action="store_true",
        help="Keep temporary docker postgres after script exits",
    )
    parser.add_argument("--postgres-user", default="quant", help="Temporary postgres user for docker mode")
    parser.add_argument("--postgres-password", default="quant", help="Temporary postgres password for docker mode")
    parser.add_argument("--postgres-database", default="quant_e2e", help="Temporary postgres db name for docker mode")
    parser.add_argument(
        "--postgres-port",
        type=int,
        default=0,
        help="Host port for temporary docker postgres, 0 means auto-pick free port",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    try:
        report = run(args)
    except Exception as exc:
        failed = {
            "ok": False,
            "ts_utc": _now_iso(),
            "error": str(exc),
        }
        print(json.dumps(failed, ensure_ascii=False, indent=2))
        raise SystemExit(1)

    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not bool(report.get("ok")):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
