#!/usr/bin/env python3
"""
End-to-end smoke script for frontend/backend/SQLite integration.

Default behavior:
1) Start backend API server with an isolated SQLite database.
2) Execute a minimal API flow (strategy create -> risk update -> risk/audit query -> strategy delete).
3) Run frontend type check as a lightweight frontend integration gate.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pick_free_port(host: str) -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def _tail_lines(text: str, max_lines: int = 80) -> List[str]:
    lines = [line.rstrip("\n") for line in str(text or "").splitlines()]
    if len(lines) <= max_lines:
        return lines
    return lines[-max_lines:]


def _request_json(
    session: requests.Session,
    method: str,
    url: str,
    *,
    timeout_seconds: float,
    **kwargs: Any,
) -> Tuple[int, Any, str]:
    resp = session.request(method=method, url=url, timeout=timeout_seconds, **kwargs)
    body_text = resp.text
    try:
        payload = resp.json()
    except Exception:
        payload = None
    return int(resp.status_code), payload, body_text


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
            status, payload, body_text = _request_json(
                session,
                "GET",
                health_url,
                timeout_seconds=http_timeout_seconds,
            )
            if status == 200 and isinstance(payload, dict):
                return payload
            last_error = f"status={status}, body={body_text[:200]}"
        except Exception as exc:
            last_error = str(exc)
        time.sleep(0.3)
    raise RuntimeError(f"backend health check timeout: {last_error}")


def _db_counts(db_path: Path) -> Dict[str, int]:
    tables = ["strategies", "backtests", "risk_states", "risk_events", "audit_logs"]
    output: Dict[str, int] = {}
    if not db_path.exists():
        return output
    with sqlite3.connect(str(db_path), timeout=30.0) as conn:
        for table in tables:
            exists_row = conn.execute(
                "SELECT COUNT(1) FROM sqlite_master WHERE type='table' AND name=?",
                (table,),
            ).fetchone()
            exists = int(exists_row[0]) > 0 if exists_row is not None else False
            if not exists:
                output[table] = 0
                continue
            cnt_row = conn.execute(f"SELECT COUNT(1) FROM {table}").fetchone()
            output[table] = int(cnt_row[0]) if cnt_row is not None else 0
    return output


def _run_frontend_check(
    *,
    frontend_root: Path,
    command: str,
    timeout_seconds: float,
) -> Dict[str, Any]:
    if not frontend_root.exists():
        raise RuntimeError(f"frontend root not found: {frontend_root}")
    started = time.perf_counter()
    proc = subprocess.run(
        shlex.split(command),
        cwd=str(frontend_root),
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout_seconds,
    )
    elapsed_ms = round((time.perf_counter() - started) * 1000.0, 3)
    result = {
        "skipped": False,
        "command": command,
        "cwd": str(frontend_root),
        "returncode": int(proc.returncode),
        "elapsed_ms": elapsed_ms,
        "stdout_tail": _tail_lines(proc.stdout),
        "stderr_tail": _tail_lines(proc.stderr),
    }
    if proc.returncode != 0:
        raise RuntimeError(f"frontend check failed with exit={proc.returncode}")
    return result


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
    frontend_root = Path(args.frontend_root).expanduser().resolve()
    if not backend_root.exists():
        raise RuntimeError(f"backend root not found: {backend_root}")

    report: Dict[str, Any] = {
        "ok": False,
        "ts_utc": _now_iso(),
        "backend": {
            "root": str(backend_root),
            "base_url": "",
            "health": {},
            "db_path": "",
            "db_counts": {},
            "steps": [],
            "server_log_tail": [],
        },
        "frontend": {
            "skipped": bool(args.skip_frontend_check),
            "command": args.frontend_command,
            "cwd": str(frontend_root),
        },
    }

    with tempfile.TemporaryDirectory(prefix="quant_e2e_") as tmp_dir:
        temp_root = Path(tmp_dir)
        db_path = temp_root / "quant_api.db"
        report["backend"]["db_path"] = str(db_path)

        host = str(args.host)
        port = int(args.port) if int(args.port) > 0 else _pick_free_port(host)
        base_url = f"http://{host}:{port}/api"
        report["backend"]["base_url"] = base_url

        env = os.environ.copy()
        env["API_DB_ENABLED"] = "true"
        env["API_DB_BACKEND"] = "sqlite"
        env["API_DB_PATH"] = str(db_path)
        env["API_DB_POSTGRES_DSN"] = ""
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

        try:
            session = requests.Session()
            health = _wait_backend_ready(
                session,
                f"{base_url}/health",
                startup_timeout_seconds=float(args.startup_timeout_seconds),
                http_timeout_seconds=float(args.http_timeout_seconds),
            )
            report["backend"]["health"] = health
            report["backend"]["steps"].append("health.ok")

            strategy_payload = {
                "name": "e2e-smoke-strategy",
                "type": "custom",
                "config": {
                    "symbols": ["BTC/USDT:USDT"],
                    "timeframe": "1h",
                    "params": {"smoke": True, "v": 1},
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
            report["backend"]["steps"].append("strategy.create")

            risk_update_payload = {
                "maxDrawdownPct": 0.123,
                "triggered": [
                    {
                        "rule": "max_drawdown",
                        "ts": _now_iso(),
                        "message": "e2e threshold breached",
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
                params={"limit": 100},
            )
            if status != 200 or not isinstance(payload, list):
                raise RuntimeError(f"query audit logs failed: status={status}, body={body[:400]}")
            if not any(str(item.get("action")) == "strategy.create" for item in payload if isinstance(item, dict)):
                raise RuntimeError("audit logs missing strategy.create event")
            report["backend"]["steps"].append("audit.read")

            status, payload, body = _request_json(
                session,
                "DELETE",
                f"{base_url}/strategies/{strategy_id}",
                timeout_seconds=float(args.http_timeout_seconds),
            )
            if status != 200 or not isinstance(payload, dict) or not bool(payload.get("deleted")):
                raise RuntimeError(f"delete strategy failed: status={status}, body={body[:400]}")
            report["backend"]["steps"].append("strategy.delete")

            report["backend"]["db_counts"] = _db_counts(db_path)

            if not args.skip_frontend_check:
                report["frontend"] = _run_frontend_check(
                    frontend_root=frontend_root,
                    command=str(args.frontend_command),
                    timeout_seconds=float(args.frontend_timeout_seconds),
                )
            else:
                report["frontend"] = {
                    "skipped": True,
                    "command": str(args.frontend_command),
                    "cwd": str(frontend_root),
                }

            report["ok"] = True
            return report
        finally:
            _terminate_process(server)
            try:
                output = ""
                if server.stdout is not None:
                    output = server.stdout.read()
                report["backend"]["server_log_tail"] = _tail_lines(output)
            except Exception:
                report["backend"]["server_log_tail"] = []


def _build_parser() -> argparse.ArgumentParser:
    backend_root = Path(__file__).resolve().parents[1]
    frontend_root = backend_root.parents[1] / "frontweb" / "www.zlsjtj.tech"
    parser = argparse.ArgumentParser(description="Run frontend/backend/SQLite end-to-end smoke check")
    parser.add_argument("--backend-root", default=str(backend_root), help="Backend project root")
    parser.add_argument("--frontend-root", default=str(frontend_root), help="Frontend project root")
    parser.add_argument("--python-bin", default=sys.executable, help="Python executable used to start uvicorn")
    parser.add_argument("--host", default="127.0.0.1", help="Backend bind host")
    parser.add_argument("--port", type=int, default=0, help="Backend bind port, 0 means auto-pick free port")
    parser.add_argument("--server-log-level", default="warning", help="uvicorn log level")
    parser.add_argument("--startup-timeout-seconds", type=float, default=30.0, help="Backend startup timeout")
    parser.add_argument("--http-timeout-seconds", type=float, default=10.0, help="Single HTTP request timeout")
    parser.add_argument(
        "--skip-frontend-check",
        action="store_true",
        help="Skip frontend check phase (backend + sqlite only)",
    )
    parser.add_argument(
        "--frontend-command",
        default="npm run typecheck",
        help="Frontend command to validate frontend side in E2E",
    )
    parser.add_argument("--frontend-timeout-seconds", type=float, default=300.0, help="Frontend command timeout")
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
