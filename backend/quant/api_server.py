import os
import subprocess
import sys
import asyncio
import threading
import time
import gc
import ctypes
import re
import signal
import logging
import base64
import hashlib
import hmac
import json
import io
import csv
import contextvars
import uuid
import secrets
from urllib import error as urllib_error
from urllib import request as urllib_request
from contextlib import suppress, contextmanager
from collections import deque
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

import pandas as pd
import yaml
from fastapi import FastAPI, HTTPException, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from statarb.config import Cfg, load_config
from statarb.broker import make_exchange
from postgres_store import PostgresStore
from db_store import SQLiteStore
from db_service import PersistenceService
from db_repository import DBRepository


PROJECT_ROOT = Path(__file__).resolve().parent
LOG_DIR = PROJECT_ROOT / "logs"
_LOGGER = logging.getLogger("quant.api_server.db")


def _normalize_db_backend(value: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "postgres"
    if text in {"sqlite", "sqlite3"}:
        return "sqlite"
    if text in {"postgres", "postgresql"}:
        return "postgres"
    raise ValueError("API_DB_BACKEND must be sqlite or postgres")


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


def _resolve_runtime_db_path_with_text(path_text: str) -> Path:
    text = str(path_text or "").strip()
    if text.lower().startswith("postgres"):
        return Path("/dev/null")
    db_path = Path(text).expanduser()
    if not db_path.is_absolute():
        db_path = (PROJECT_ROOT / db_path).resolve()
    return db_path


def _parse_positive_int_raw(raw: str, default: int) -> int:
    try:
        parsed = int(raw)
        return parsed if parsed > 0 else default
    except Exception:
        return default


def _parse_positive_float_raw(raw: str, default: float) -> float:
    try:
        parsed = float(raw)
        return parsed if parsed > 0 else default
    except Exception:
        return default


def _build_db_store(
    *,
    backend: str,
    db_path_text: str,
    postgres_dsn: str,
) -> tuple[DBRepository, str, Path, str]:
    normalized = _normalize_db_backend(backend)
    resolved_path = _resolve_runtime_db_path_with_text(db_path_text)
    dsn = str(postgres_dsn or "").strip()
    if normalized == "sqlite":
        return SQLiteStore(resolved_path), normalized, resolved_path, ""
    pool_enabled = os.getenv("API_DB_POSTGRES_POOL_ENABLED", "true").strip().lower() != "false"
    pool_min_size = _parse_positive_int_raw(os.getenv("API_DB_POSTGRES_POOL_MIN_SIZE", "1"), 1)
    pool_max_size = _parse_positive_int_raw(os.getenv("API_DB_POSTGRES_POOL_MAX_SIZE", "10"), 10)
    if pool_max_size < pool_min_size:
        pool_max_size = pool_min_size
    pool_timeout_seconds = _parse_positive_float_raw(
        os.getenv("API_DB_POSTGRES_POOL_TIMEOUT_SECONDS", "5"),
        5.0,
    )
    return (
        PostgresStore(
            dsn,
            pool_enabled=pool_enabled,
            pool_min_size=pool_min_size,
            pool_max_size=pool_max_size,
            pool_timeout_seconds=pool_timeout_seconds,
        ),
        normalized,
        resolved_path,
        dsn,
    )


def _read_startup_secret_file_text(path_text: str) -> str:
    path = Path(path_text).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    try:
        return path.read_text(encoding="utf-8").strip()
    except Exception:
        return ""


_DB_ENABLED = True
_DB_PATH_TEXT = os.getenv("API_DB_PATH", "/dev/null").strip() or "/dev/null"
_DB_BACKEND = _normalize_db_backend(os.getenv("API_DB_BACKEND", "postgres"))
_DB_POSTGRES_DSN_FILE = os.getenv("API_DB_POSTGRES_DSN_FILE", "").strip()
_DB_POSTGRES_DSN = (
    _read_startup_secret_file_text(_DB_POSTGRES_DSN_FILE).strip()
    if _DB_POSTGRES_DSN_FILE
    else os.getenv("API_DB_POSTGRES_DSN", "").strip()
)
_DB_PATH = _resolve_runtime_db_path_with_text(_DB_PATH_TEXT)
_DB, _DB_BACKEND, _DB_PATH, _DB_POSTGRES_DSN = _build_db_store(
    backend=_DB_BACKEND,
    db_path_text=_DB_PATH_TEXT,
    postgres_dsn=_DB_POSTGRES_DSN,
)
_DB_SERVICE = PersistenceService(lambda: _DB)
_DB_READY = False
_DB_INIT_ERROR = ""
try:
    _DB_SERVICE.initialize()
    _DB_READY = True
except Exception as _db_exc:
    _DB_INIT_ERROR = str(_db_exc)
    _DB_READY = False


def _close_db_repository(repo: Optional[DBRepository]) -> None:
    close_fn = getattr(repo, "close", None)
    if not callable(close_fn):
        return
    try:
        close_fn()
    except Exception as exc:
        _LOGGER.warning("closing db repository failed: %s", exc)


_DB_RUNTIME_LOCK = threading.Lock()
_DB_RUNTIME_STATS: Dict[str, Any] = {
    "strategy_write_failures": 0,
    "backtest_write_failures": 0,
    "risk_write_failures": 0,
    "risk_event_write_failures": 0,
    "audit_write_failures": 0,
    "audit_read_failures": 0,
    "risk_event_read_failures": 0,
    "runtime_log_write_failures": 0,
    "runtime_log_read_failures": 0,
    "alert_delivery_write_failures": 0,
    "alert_outbox_enqueue_failures": 0,
    "alert_outbox_read_failures": 0,
    "alert_outbox_update_failures": 0,
    "alert_outbox_delivery_failures": 0,
    "ws_event_write_failures": 0,
    "ws_event_read_failures": 0,
    "strategy_diag_write_failures": 0,
    "strategy_diag_read_failures": 0,
    "backtest_detail_write_failures": 0,
    "backtest_detail_read_failures": 0,
    "write_ops_total": 0,
    "write_ops_slow_total": 0,
    "read_ops_total": 0,
    "read_ops_slow_total": 0,
    "lock_contention_total": 0,
    "lock_wait_ms_total": 0.0,
    "last_slow_kind": "",
    "last_slow_ms": 0.0,
    "last_slow_at": "",
    "last_error": "",
    "last_error_at": "",
    "last_write_kind": "",
    "last_write_ms": 0.0,
    "last_write_at": "",
    "max_write_ms": 0.0,
}
_DB_RUNTIME_ALERT_STATE: Dict[str, Any] = {
    "last_alert_at": "",
    "last_alert_total": 0,
    "last_alert_error": "",
    "last_alert_epoch": 0.0,
    "last_webhook_status": "",
}
_DB_INIT_ALERT_SENT = False
_DEFAULT_CONFIG_PATH = os.getenv(
    "DEFAULT_STRATEGY_CONFIG_PATH",
    "config_2025_bch_bnb_btc_equal_combo_baseline_v2_invvol_best.yaml",
)
def _read_secret_file(path_text: str) -> str:
    path = Path(path_text).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    try:
        content = path.read_text(encoding="utf-8").strip()
        if not content:
            return ""
        # Support key=value and plain value.
        if "=" in content and "\n" not in content:
            _, value = content.split("=", 1)
            return value.strip()
        return content.splitlines()[0].strip()
    except Exception:
        return ""


def _read_secret_file_text(path_text: str) -> str:
    path = Path(path_text).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    try:
        return path.read_text(encoding="utf-8").strip()
    except Exception:
        return ""


def _load_auth_pair_from_file(path_text: str) -> Tuple[str, str]:
    text = _read_secret_file_text(path_text)
    if not text:
        return "", ""
    if ":" not in text:
        return "", ""
    username, password = text.split(":", 1)
    return username.strip(), password.strip()


def _load_dashboard_credentials_from_file(path_text: str) -> Dict[str, str]:
    text = _read_secret_file_text(path_text)
    if not text:
        return {}

    credentials: Dict[str, str] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            continue
        username, password = line.split(":", 1)
        username = username.strip()
        password = password.strip()
        if username and password:
            credentials[username] = password
    if credentials:
        return credentials

    # Fallback for single-line value.
    if ":" in text:
        username, password = text.split(":", 1)
        username = username.strip()
        password = password.strip()
        if username and password:
            return {username: password}
    return {}


def _parse_positive_int(raw: str, default: int) -> int:
    try:
        parsed = int(raw)
        return parsed if parsed > 0 else default
    except Exception:
        return default


def _parse_positive_float(raw: str, default: float) -> float:
    try:
        parsed = float(raw)
        return parsed if parsed > 0 else default
    except Exception:
        return default


_DB_ALERT_WEBHOOK_URL = os.getenv("API_DB_ALERT_WEBHOOK_URL", "").strip()
_DB_ALERT_THRESHOLD = _parse_positive_int(os.getenv("API_DB_ALERT_THRESHOLD", "1"), 1)
_DB_ALERT_COOLDOWN_SECONDS = _parse_positive_int(os.getenv("API_DB_ALERT_COOLDOWN_SECONDS", "300"), 300)
_DB_ALERT_TIMEOUT_SECONDS = _parse_positive_float(os.getenv("API_DB_ALERT_TIMEOUT_SECONDS", "3"), 3.0)
_DB_ALERT_MAX_RETRIES = max(0, int(_parse_positive_int(os.getenv("API_DB_ALERT_MAX_RETRIES", "0"), 1) - 1))
_DB_ALERT_RETRY_BACKOFF_MS = _parse_positive_int(os.getenv("API_DB_ALERT_RETRY_BACKOFF_MS", "200"), 200)
_DB_ALERT_OUTBOX_ENABLED = os.getenv("API_DB_ALERT_OUTBOX_ENABLED", "true").strip().lower() != "false"
_DB_ALERT_OUTBOX_POLL_SECONDS = _parse_positive_float(os.getenv("API_DB_ALERT_OUTBOX_POLL_SECONDS", "1"), 1.0)
_DB_ALERT_OUTBOX_BATCH_SIZE = _parse_positive_int(os.getenv("API_DB_ALERT_OUTBOX_BATCH_SIZE", "20"), 20)
_DB_HEALTH_STATS_TTL_SECONDS = _parse_positive_int(os.getenv("API_DB_HEALTH_STATS_TTL_SECONDS", "30"), 30)
_DB_SLOW_OP_THRESHOLD_MS = _parse_positive_float(os.getenv("API_DB_SLOW_OP_THRESHOLD_MS", "200"), 200.0)
_DB_HEALTH_LOCK = threading.Lock()
_DB_HEALTH_STATS_CACHE: Dict[str, Any] = {
    "ts_epoch": 0.0,
    "stats": {},
    "error": "",
}
_DB_SWITCH_LOCK = threading.Lock()


_AUTH_REQUIRED = os.getenv("API_AUTH_REQUIRED", "false").lower() != "false"
_AUTH_TOKEN_FILE = os.getenv("API_AUTH_TOKEN_FILE", "").strip()
_AUTH_TOKEN = (
    _read_secret_file(_AUTH_TOKEN_FILE)
    if _AUTH_TOKEN_FILE
    else os.getenv("API_AUTH_TOKEN", "").strip()
)
_DASHBOARD_AUTH_FILE = os.getenv("DASHBOARD_AUTH_FILE", "").strip()
_DASHBOARD_CREDENTIALS: Dict[str, str] = (
    _load_dashboard_credentials_from_file(_DASHBOARD_AUTH_FILE)
    if _DASHBOARD_AUTH_FILE
    else {}
)
_ENV_DASHBOARD_LOGIN_USERNAME = os.getenv("DASHBOARD_LOGIN_USERNAME", "").strip()
_ENV_DASHBOARD_LOGIN_PASSWORD = os.getenv("DASHBOARD_LOGIN_PASSWORD", "").strip()
if _ENV_DASHBOARD_LOGIN_USERNAME and _ENV_DASHBOARD_LOGIN_PASSWORD:
    _DASHBOARD_CREDENTIALS.setdefault(_ENV_DASHBOARD_LOGIN_USERNAME, _ENV_DASHBOARD_LOGIN_PASSWORD)
_GUEST_USERNAME = os.getenv("DASHBOARD_GUEST_USERNAME", "guest").strip() or "guest"
_DASHBOARD_PRIMARY_USERNAME = next(iter(_DASHBOARD_CREDENTIALS.keys()), "admin")
_DASHBOARD_LOGIN_USERNAME = _DASHBOARD_PRIMARY_USERNAME
_DASHBOARD_LOGIN_PASSWORD = _DASHBOARD_CREDENTIALS.get(_DASHBOARD_PRIMARY_USERNAME, "")

_SESSION_COOKIE_NAME = os.getenv("API_SESSION_COOKIE_NAME", "quant_session").strip() or "quant_session"
_SESSION_TTL_SECONDS = _parse_positive_int(os.getenv("API_SESSION_TTL_SECONDS", "43200"), 43200)
_SESSION_COOKIE_SECURE = os.getenv("API_SESSION_COOKIE_SECURE", "false").lower() == "true"
_SESSION_COOKIE_SAMESITE = os.getenv("API_SESSION_COOKIE_SAMESITE", "lax").strip().lower()
if _SESSION_COOKIE_SAMESITE not in {"lax", "strict", "none"}:
    _SESSION_COOKIE_SAMESITE = "lax"
_SESSION_SECRET_FILE = os.getenv("API_SESSION_SECRET_FILE", "").strip()
_SESSION_SECRET = (
    _read_secret_file(_SESSION_SECRET_FILE)
    if _SESSION_SECRET_FILE
    else os.getenv("API_SESSION_SECRET", "").strip()
)
if not _SESSION_SECRET:
    # Fallback to API token to avoid breaking auth when only token is configured.
    _SESSION_SECRET = _AUTH_TOKEN

_AUTH_EXEMPT_PATHS = {
    "/api/health",
    "/api/auth/login",
    "/api/auth/guest",
    "/api/auth/logout",
    "/api/auth/status",
    "/api/market/ticks",
    "/api/market/klines",
}
_LOGIN_RATE_LIMIT_WINDOW_SECONDS = _parse_positive_int(os.getenv("API_LOGIN_RATE_LIMIT_WINDOW_SECONDS", "300"), 300)
_LOGIN_RATE_LIMIT_MAX_ATTEMPTS = _parse_positive_int(os.getenv("API_LOGIN_RATE_LIMIT_MAX_ATTEMPTS", "10"), 10)
_LOGIN_LOCKOUT_SECONDS = _parse_positive_int(os.getenv("API_LOGIN_LOCKOUT_SECONDS", "900"), 900)
_LOGIN_RATE_LOCK = threading.Lock()
_LOGIN_ATTEMPTS: Dict[str, Deque[float]] = {}
_LOGIN_LOCKED_UNTIL: Dict[str, float] = {}
_MARKET_EXCHANGE_LOCK = threading.Lock()
_MARKET_EXCHANGE_CACHE: Dict[str, Any] = {
    "config_path": None,
    "config_mtime": None,
    "exchange": None,
}
_MARKET_TICKS_LOCK = threading.Lock()
_MARKET_TICKS_CACHE: Dict[str, Any] = {
    "config_path": None,
    "ticks": [],
    "ts_ms": 0.0,
}
_MARKET_TICK_REFRESHING: Dict[str, bool] = {}
_MARKET_TICK_MIN_FETCH_MS = 2_000
_WS_CONNECTIONS: Dict[WebSocket, Dict[str, Any]] = {}
_WS_BROADCAST_TASK: Optional[asyncio.Task[None]] = None
_WS_LAST_LOG_KEY: Dict[str, Optional[str]] = {}
_WS_EQUITY_CACHE: Dict[str, Dict[str, Any]] = {}
_WS_LOG_CACHE: Dict[str, Dict[str, Any]] = {}
_WS_BROADCAST_INTERVAL_SEC = 0.1
_DATA_FILE_DB_SYNC_MAX_BYTES = _parse_positive_int(
    os.getenv("API_DATA_FILE_DB_SYNC_MAX_BYTES", str(5 * 1024 * 1024)),
    5 * 1024 * 1024,
)
_PAPER_EQUITY_CHUNK_ROWS = _parse_positive_int(os.getenv("API_PAPER_EQUITY_CHUNK_ROWS", "50000"), 50000)
_PAPER_EQUITY_FULL_READ_MAX_BYTES = _parse_positive_int(
    os.getenv("API_PAPER_EQUITY_FULL_READ_MAX_BYTES", str(80 * 1024 * 1024)),
    80 * 1024 * 1024,
)
_PORTFOLIO_EQUITY_CURVE_MAX_POINTS = _parse_positive_int(
    os.getenv("API_PORTFOLIO_EQUITY_CURVE_MAX_POINTS", "1200"),
    1200,
)
_DB_ALERT_OUTBOX_LOCK = threading.Lock()
_DB_ALERT_OUTBOX_EVENT = threading.Event()
_DB_ALERT_OUTBOX_STOP = threading.Event()
_DB_ALERT_OUTBOX_WORKER: Optional[threading.Thread] = None
_STRATEGY_STORE: Dict[str, Dict[str, Any]] = {}
_BACKTEST_STORE: Dict[str, Dict[str, Any]] = {}
_RISK_STATE_STORE: Dict[str, Dict[str, Any]] = {}
_STRATEGY_RUNNERS: Dict[str, "ManagedProcess"] = {}
_STRATEGY_RUNNERS_LOCK = threading.Lock()
_BACKTEST_RUNNERS: Dict[str, "ManagedProcess"] = {}
_BACKTEST_RUNNERS_LOCK = threading.Lock()
_BACKTEST_CREATE_DEDUP_TTL_SECONDS = _parse_positive_int(
    os.getenv("API_BACKTEST_CREATE_DEDUP_TTL_SECONDS", "30"),
    30,
)
_BACKTEST_PROGRESS_PERSIST_INTERVAL_SECONDS = _parse_positive_int(
    os.getenv("API_BACKTEST_PROGRESS_PERSIST_INTERVAL_SECONDS", "10"),
    10,
)
_BACKTEST_PROGRESS_MIN_DELTA_PCT = _parse_positive_float(
    os.getenv("API_BACKTEST_PROGRESS_MIN_DELTA_PCT", "1"),
    1.0,
)
_BACKTEST_CREATE_DEDUP_LOCK = threading.Lock()
_BACKTEST_CREATE_RECENT: Dict[str, Dict[str, Any]] = {}
_BACKTEST_PROGRESS_PERSIST_LOCK = threading.Lock()
_BACKTEST_PROGRESS_PERSIST_STATE: Dict[str, Dict[str, float]] = {}
_STRATEGY_COMPILE_QUEUE: Deque[Dict[str, Any]] = deque()
_STRATEGY_COMPILE_LOCK = threading.Lock()
_STRATEGY_COMPILE_EVENT = threading.Event()
_STRATEGY_COMPILE_STOP = threading.Event()
_STRATEGY_COMPILE_WORKER: Optional[threading.Thread] = None
_STRATEGY_COMPILE_WAIT_SECONDS = _parse_positive_float(
    os.getenv("API_STRATEGY_COMPILE_WAIT_SECONDS", "0.25"),
    0.25,
)
_DEFAULT_STRATEGY_ID = "quant-default"
_AUTH_FALLBACK_USER = "admin"
_USER_STRATEGY_SCOPE_PREFIX = "usr__"
_RUNTIME_CONFIG_NAME_RE = re.compile(r"^strategy_(?P<strategy_id>.+)_(?P<ts>\d{8,})\.ya?ml$")
_EXTERNAL_STRATEGY_SCAN_LOCK = threading.Lock()
_EXTERNAL_STRATEGY_SCAN_CACHE: Dict[str, Any] = {
    "ts_ms": 0,
    "rows": {},
}
_EXTERNAL_STRATEGY_SCAN_TTL_MS = 1_500
_CURRENT_AUTH_USER: contextvars.ContextVar[str] = contextvars.ContextVar(
    "current_auth_user",
    default=_AUTH_FALLBACK_USER,
)
_AUTH_THREAD_LOCAL = threading.local()
_PRESET_STRATEGIES = {
    "strategy_config": "config.yaml",
    "strategy_invvol_best": "config_2025_bch_bnb_btc_equal_combo_baseline_v2_invvol_best.yaml",
    "strategy_candidate_v009": "config_candidate_v009_tv035_mn05_mx11.yaml",
    "strategy_candidate_v010": "config_candidate_v010_tv035_mn05_mx13.yaml",
}

_PAPER_FILL_RE = re.compile(
    r"\[PAPER\](?:\s+reduceOnly)?(?:\s+positionSide=[^\s]+)?\s+(?P<side>buy|sell)\s+(?P<symbol>[^\s]+)\s+amount=(?P<amount>[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s+price=(?P<price>[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s+notion=(?P<notion>[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)"
)
_PAPER_EQUITY_POS_RE = re.compile(r"\[PAPER\].*positions=(?P<positions>.*)")
_PAPER_POSITION_ENTRY_RE = re.compile(
    r"(?P<symbol>[A-Za-z0-9./:-]+)\s+qty=(?P<qty>[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s+notion=(?P<notion>[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)"
)
_BACKTEST_PROGRESS_RE = re.compile(r"BACKTEST_PROGRESS\s+pct=(?P<pct>\d{1,3}(?:\.\d+)?)", re.IGNORECASE)


def _normalize_auth_username(username: Optional[str]) -> str:
    candidate = str(username or "").strip()
    return candidate or _AUTH_FALLBACK_USER


def _resolve_effective_auth_username(username: Optional[str] = None) -> str:
    if username is None:
        return _current_auth_username()
    return _normalize_auth_username(username)


def _current_auth_username() -> str:
    thread_value = getattr(_AUTH_THREAD_LOCAL, "auth_username", "")
    if isinstance(thread_value, str) and thread_value.strip():
        return _normalize_auth_username(thread_value)
    return _normalize_auth_username(_CURRENT_AUTH_USER.get())


def _safe_user_key(username: str) -> str:
    safe = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in username.strip().lower())
    return safe or "user"


def _is_admin_username(username: Optional[str] = None) -> bool:
    candidate = _resolve_effective_auth_username(username).lower()
    if candidate == _AUTH_FALLBACK_USER:
        return True
    if _DASHBOARD_PRIMARY_USERNAME and candidate == _DASHBOARD_PRIMARY_USERNAME.lower():
        return True
    return False


def _user_scope_prefix(username: Optional[str] = None) -> str:
    return f"{_USER_STRATEGY_SCOPE_PREFIX}{_safe_user_key(_resolve_effective_auth_username(username))}__"


def _scoped_strategy_id(strategy_id: str, username: Optional[str] = None) -> str:
    plain = str(strategy_id or "").strip()
    if not plain:
        return plain
    user = _resolve_effective_auth_username(username)
    if _is_admin_username(user):
        return plain
    prefix = _user_scope_prefix(user)
    if plain.startswith(prefix):
        return plain
    return f"{prefix}{plain}"


def _unscoped_strategy_id(strategy_id: str, username: Optional[str] = None) -> str:
    scoped = str(strategy_id or "").strip()
    if not scoped:
        return scoped
    user = _resolve_effective_auth_username(username)
    if _is_admin_username(user):
        return scoped
    prefix = _user_scope_prefix(user)
    if scoped.startswith(prefix):
        return scoped[len(prefix):] or scoped
    return scoped


def _strategy_owner_user_key(scoped_strategy_id: str) -> Optional[str]:
    text = str(scoped_strategy_id or "")
    if not text.startswith(_USER_STRATEGY_SCOPE_PREFIX):
        return None
    remainder = text[len(_USER_STRATEGY_SCOPE_PREFIX):]
    if "__" not in remainder:
        return None
    owner_key, _ = remainder.split("__", 1)
    return owner_key or None


def _runner_visible_to_user(scoped_strategy_id: str, username: Optional[str] = None) -> bool:
    owner_key = _strategy_owner_user_key(scoped_strategy_id)
    user = _resolve_effective_auth_username(username)
    if _is_admin_username(user):
        # Admin session only sees unscoped (admin-owned) runners.
        return owner_key is None
    return owner_key == _safe_user_key(user)


def _record_owner_key() -> str:
    return _safe_user_key(_current_auth_username())


@contextmanager
def _auth_user_context(username: Optional[str]):
    normalized = _normalize_auth_username(username)
    token = _CURRENT_AUTH_USER.set(normalized)
    previous_thread_value = getattr(_AUTH_THREAD_LOCAL, "auth_username", None)
    _AUTH_THREAD_LOCAL.auth_username = normalized
    try:
        yield
    finally:
        _CURRENT_AUTH_USER.reset(token)
        if previous_thread_value is None:
            try:
                delattr(_AUTH_THREAD_LOCAL, "auth_username")
            except Exception:
                pass
        else:
            _AUTH_THREAD_LOCAL.auth_username = previous_thread_value


def _request_auth_username(request: Optional[Request]) -> str:
    if request is None:
        return _current_auth_username()
    try:
        return _normalize_auth_username(getattr(request.state, "auth_username", ""))
    except Exception:
        return _current_auth_username()

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso_datetime_param(value: Optional[str], field_name: str) -> Optional[str]:
    if value is None:
        return None
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    normalized = raw
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(normalized)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"{field_name} must be ISO datetime") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


def _db_is_enabled() -> bool:
    return _DB_ENABLED and _DB_READY


def _db_runtime_failures_total_from_stats(stats: Dict[str, Any]) -> int:
    total = 0
    for key, value in stats.items():
        if not str(key).endswith("_failures"):
            continue
        try:
            total += int(value)
        except Exception:
            continue
    return total


def _db_runtime_failure_counters(stats: Dict[str, Any]) -> Dict[str, int]:
    counters: Dict[str, int] = {}
    for key, value in stats.items():
        if not str(key).endswith("_failures"):
            continue
        try:
            counters[str(key)] = int(value)
        except Exception:
            counters[str(key)] = 0
    return counters


def _db_operation_type(kind: str) -> str:
    text = str(kind or "").strip().lower()
    if text.endswith("_write"):
        return "write"
    if text.endswith("_read"):
        return "read"
    return "other"


def _safe_elapsed_ms(elapsed_ms: Optional[float]) -> float:
    if elapsed_ms is None:
        return 0.0
    try:
        return max(0.0, float(elapsed_ms))
    except Exception:
        return 0.0


def _record_db_operation_stats_locked(kind: str, elapsed_ms: float) -> None:
    op_type = _db_operation_type(kind)
    if op_type not in {"write", "read"}:
        return
    total_key = "write_ops_total" if op_type == "write" else "read_ops_total"
    slow_key = "write_ops_slow_total" if op_type == "write" else "read_ops_slow_total"
    _DB_RUNTIME_STATS[total_key] = int(_DB_RUNTIME_STATS.get(total_key) or 0) + 1
    if elapsed_ms >= _DB_SLOW_OP_THRESHOLD_MS:
        _DB_RUNTIME_STATS[slow_key] = int(_DB_RUNTIME_STATS.get(slow_key) or 0) + 1
        _DB_RUNTIME_STATS["last_slow_kind"] = str(kind)
        _DB_RUNTIME_STATS["last_slow_ms"] = round(elapsed_ms, 4)
        _DB_RUNTIME_STATS["last_slow_at"] = _now_iso()


def _is_db_lock_contention_error(exc: Exception) -> bool:
    text = str(exc or "").strip().lower()
    if not text:
        return False
    if "database is locked" in text:
        return True
    if "database table is locked" in text:
        return True
    if "database schema is locked" in text:
        return True
    return "database" in text and "locked" in text


def _dispatch_db_alert_webhook_once(webhook_url: str, payload: Dict[str, Any]) -> Tuple[bool, Optional[int], str, str, float]:
    started = time.perf_counter()
    body_text = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    body = body_text.encode("utf-8")
    last_status: Optional[int] = None
    last_error = ""
    response_body = ""
    req = urllib_request.Request(
        str(webhook_url or ""),
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib_request.urlopen(req, timeout=_DB_ALERT_TIMEOUT_SECONDS) as resp:
            status = getattr(resp, "status", None)
            if status is None:
                status = int(resp.getcode())
            last_status = int(status)
            try:
                raw_response = resp.read()
                if isinstance(raw_response, bytes):
                    response_body = raw_response.decode("utf-8", errors="replace")
                else:
                    response_body = str(raw_response or "")
            except Exception:
                response_body = ""
        if int(last_status) < 400:
            return True, last_status, "", response_body, (time.perf_counter() - started) * 1000.0
        last_error = f"http_status={last_status}"
    except urllib_error.URLError as exc:
        last_error = str(exc)
    except Exception as exc:
        last_error = str(exc)
    return False, last_status, str(last_error or "alert delivery failed"), response_body, (time.perf_counter() - started) * 1000.0


def _outbox_next_available_at(retry_count: int) -> str:
    delay_ms = max(0, int(_DB_ALERT_RETRY_BACKOFF_MS)) * max(1, int(retry_count))
    if delay_ms <= 0:
        return _now_iso()
    next_dt = datetime.now(timezone.utc) + timedelta(milliseconds=delay_ms)
    return next_dt.isoformat()


def _process_db_alert_outbox_once(limit: Optional[int] = None) -> int:
    if not _db_is_enabled():
        return 0
    if not _DB_ALERT_WEBHOOK_URL:
        return 0
    safe_limit = max(1, int(limit or _DB_ALERT_OUTBOX_BATCH_SIZE))
    rows = _db_list_due_alert_outbox(limit=safe_limit, now_ts=_now_iso())
    if not rows:
        return 0

    processed = 0
    for row in rows:
        outbox_id = int(row.get("id") or 0)
        if outbox_id <= 0:
            continue
        payload = row.get("payload")
        payload_dict = payload if isinstance(payload, dict) else {}
        event = str(row.get("event") or "")
        severity = str(row.get("severity") or "info")
        message = str(row.get("message") or "")
        owner = str(row.get("owner") or "")
        webhook_url = str(row.get("webhookUrl") or _DB_ALERT_WEBHOOK_URL)
        retry_count = max(0, int(row.get("retryCount") or 0))
        max_retries = max(0, int(row.get("maxRetries") or 0))
        ts_utc = str(payload_dict.get("ts_utc") or _now_iso())

        sent, last_status, last_error, response_body, duration_ms = _dispatch_db_alert_webhook_once(webhook_url, payload_dict)
        if sent:
            _db_finalize_alert_outbox(
                outbox_id,
                status="sent",
                retry_count=retry_count,
                http_status=last_status,
                error_message="",
                response_body=response_body,
                dispatched_at=_now_iso(),
            )
            _db_append_alert_delivery(
                owner=owner,
                event=event,
                severity=severity,
                message=message,
                webhook_url=webhook_url,
                status="sent",
                retry_count=retry_count,
                http_status=last_status,
                error_message="",
                payload=payload_dict,
                response_body=response_body,
                ts_utc=ts_utc,
                duration_ms=duration_ms,
            )
            with _DB_RUNTIME_LOCK:
                _DB_RUNTIME_ALERT_STATE["last_webhook_status"] = "sent"
        else:
            next_retry_count = retry_count + 1
            if next_retry_count <= max_retries:
                _db_finalize_alert_outbox(
                    outbox_id,
                    status="pending",
                    retry_count=next_retry_count,
                    available_at=_outbox_next_available_at(next_retry_count),
                    http_status=last_status,
                    error_message=str(last_error),
                    response_body=response_body,
                )
                with _DB_RUNTIME_LOCK:
                    _DB_RUNTIME_ALERT_STATE["last_webhook_status"] = (
                        f"retry:{next_retry_count}/{max_retries}:{last_error}"
                    )
            else:
                _db_finalize_alert_outbox(
                    outbox_id,
                    status="failed",
                    retry_count=retry_count,
                    http_status=last_status,
                    error_message=str(last_error),
                    response_body=response_body,
                    dispatched_at=_now_iso(),
                )
                _db_append_alert_delivery(
                    owner=owner,
                    event=event,
                    severity=severity,
                    message=message,
                    webhook_url=webhook_url,
                    status="failed",
                    retry_count=retry_count,
                    http_status=last_status,
                    error_message=str(last_error),
                    payload=payload_dict,
                    response_body=response_body,
                    ts_utc=ts_utc,
                    duration_ms=duration_ms,
                )
                with _DB_RUNTIME_LOCK:
                    _DB_RUNTIME_ALERT_STATE["last_webhook_status"] = f"error:{last_error}"
                    _DB_RUNTIME_STATS["alert_outbox_delivery_failures"] = (
                        int(_DB_RUNTIME_STATS.get("alert_outbox_delivery_failures") or 0) + 1
                    )
        processed += 1
    return processed


def _db_alert_outbox_worker_loop() -> None:
    while not _DB_ALERT_OUTBOX_STOP.is_set():
        handled = 0
        try:
            handled = _process_db_alert_outbox_once(limit=_DB_ALERT_OUTBOX_BATCH_SIZE)
        except Exception as exc:
            _LOGGER.error("db alert outbox worker failed: %s", exc)
        if handled > 0:
            continue
        _DB_ALERT_OUTBOX_EVENT.wait(timeout=_DB_ALERT_OUTBOX_POLL_SECONDS)
        _DB_ALERT_OUTBOX_EVENT.clear()


def _ensure_db_alert_outbox_worker() -> None:
    global _DB_ALERT_OUTBOX_WORKER
    if (not _DB_ALERT_OUTBOX_ENABLED) or (not _DB_ALERT_WEBHOOK_URL) or (not _db_is_enabled()):
        return
    with _DB_ALERT_OUTBOX_LOCK:
        worker = _DB_ALERT_OUTBOX_WORKER
        if worker is not None and worker.is_alive():
            return
        _DB_ALERT_OUTBOX_STOP.clear()
        _DB_ALERT_OUTBOX_EVENT.clear()
        worker = threading.Thread(
            target=_db_alert_outbox_worker_loop,
            name="db-alert-outbox-worker",
            daemon=True,
        )
        worker.start()
        _DB_ALERT_OUTBOX_WORKER = worker


def _stop_db_alert_outbox_worker(join_timeout: float = 2.0) -> None:
    global _DB_ALERT_OUTBOX_WORKER
    with _DB_ALERT_OUTBOX_LOCK:
        _DB_ALERT_OUTBOX_STOP.set()
        _DB_ALERT_OUTBOX_EVENT.set()
        worker = _DB_ALERT_OUTBOX_WORKER
        if worker is not None and worker.is_alive():
            worker.join(timeout=max(0.1, float(join_timeout)))
        _DB_ALERT_OUTBOX_WORKER = None


def _wake_db_alert_outbox_worker() -> None:
    _DB_ALERT_OUTBOX_EVENT.set()


def _emit_db_alert(event: str, severity: str, message: str, detail: Optional[Dict[str, Any]] = None) -> Tuple[bool, str]:
    if not _DB_ALERT_WEBHOOK_URL:
        return False, "disabled"
    payload: Dict[str, Any] = {
        "service": "quant-api",
        "component": "db",
        "event": str(event),
        "severity": str(severity),
        "message": str(message),
        "ts_utc": _now_iso(),
        "db_path": str(_DB_PATH),
        "db_ready": bool(_DB_READY),
    }
    if isinstance(detail, dict):
        payload["detail"] = detail
    if _DB_ALERT_OUTBOX_ENABLED:
        outbox_id = _db_enqueue_alert_outbox(
            owner=_record_owner_key(),
            event=str(event),
            severity=str(severity),
            message=str(message),
            webhook_url=str(_DB_ALERT_WEBHOOK_URL),
            payload=payload,
            max_retries=int(_DB_ALERT_MAX_RETRIES),
            available_at=str(payload.get("ts_utc") or _now_iso()),
            created_at=str(payload.get("ts_utc") or _now_iso()),
        )
        if outbox_id > 0:
            _ensure_db_alert_outbox_worker()
            _wake_db_alert_outbox_worker()
            return True, "queued"
        _LOGGER.warning("db alert outbox enqueue failed, fallback to synchronous delivery")

    total_attempts = max(1, int(_DB_ALERT_MAX_RETRIES) + 1)
    attempts_used = 0
    sent = False
    last_error = ""
    last_status: Optional[int] = None
    response_body = ""
    duration_ms = 0.0
    for attempt in range(1, total_attempts + 1):
        attempts_used = attempt
        sent, last_status, last_error, response_body, duration_ms = _dispatch_db_alert_webhook_once(
            _DB_ALERT_WEBHOOK_URL,
            payload,
        )
        if sent:
            break
        if attempt < total_attempts and _DB_ALERT_RETRY_BACKOFF_MS > 0:
            time.sleep(float(_DB_ALERT_RETRY_BACKOFF_MS) / 1000.0)
    _db_append_alert_delivery(
        owner=_record_owner_key(),
        event=str(event),
        severity=str(severity),
        message=str(message),
        webhook_url=str(_DB_ALERT_WEBHOOK_URL),
        status="sent" if sent else "failed",
        retry_count=max(0, attempts_used - 1),
        http_status=last_status,
        error_message=str(last_error),
        payload=payload,
        response_body=response_body,
        ts_utc=str(payload.get("ts_utc") or _now_iso()),
        duration_ms=duration_ms,
    )
    if sent:
        return True, ""
    return False, str(last_error or "alert delivery failed")


def _maybe_alert_db_runtime_failure(kind: str, exc: Exception, stats_snapshot: Dict[str, Any], total: int) -> None:
    now_epoch = time.time()
    should_alert = False
    with _DB_RUNTIME_LOCK:
        last_alert_epoch = float(_DB_RUNTIME_ALERT_STATE.get("last_alert_epoch") or 0.0)
        if total >= _DB_ALERT_THRESHOLD and now_epoch - last_alert_epoch >= _DB_ALERT_COOLDOWN_SECONDS:
            _DB_RUNTIME_ALERT_STATE["last_alert_epoch"] = now_epoch
            _DB_RUNTIME_ALERT_STATE["last_alert_at"] = _now_iso()
            _DB_RUNTIME_ALERT_STATE["last_alert_total"] = int(total)
            _DB_RUNTIME_ALERT_STATE["last_alert_error"] = f"{kind}: {exc}"
            should_alert = True
    if not should_alert:
        return

    detail = {
        "kind": str(kind),
        "error": str(exc),
        "failure_total": int(total),
        "failure_by_kind": _db_runtime_failure_counters(stats_snapshot),
    }
    _LOGGER.warning(
        "db runtime failures reached alert threshold total=%s threshold=%s kind=%s error=%s",
        total,
        _DB_ALERT_THRESHOLD,
        kind,
        exc,
    )
    sent, alert_err = _emit_db_alert(
        event="db_runtime_persistence_failure",
        severity="critical",
        message=f"db runtime persistence failures reached threshold: total={total}",
        detail=detail,
    )
    webhook_status = "queued" if sent and str(alert_err) == "queued" else ("sent" if sent else f"error:{alert_err}")
    with _DB_RUNTIME_LOCK:
        _DB_RUNTIME_ALERT_STATE["last_webhook_status"] = webhook_status
    if sent:
        if str(alert_err) == "queued":
            _LOGGER.warning("db runtime alert queued via outbox")
        else:
            _LOGGER.warning("db runtime alert sent via webhook")
    elif alert_err != "disabled":
        _LOGGER.error("db runtime alert webhook failed: %s", alert_err)


def _maybe_alert_db_init_failure() -> None:
    global _DB_INIT_ALERT_SENT
    if _DB_INIT_ALERT_SENT:
        return
    if not _DB_ENABLED or _DB_READY or not _DB_INIT_ERROR:
        return
    _DB_INIT_ALERT_SENT = True
    _LOGGER.error("database initialization failed: %s", _DB_INIT_ERROR)
    sent, alert_err = _emit_db_alert(
        event="db_init_failure",
        severity="critical",
        message="database initialization failed",
        detail={"db_error": _DB_INIT_ERROR},
    )
    with _DB_RUNTIME_LOCK:
        _DB_RUNTIME_ALERT_STATE["last_webhook_status"] = (
            "queued" if sent and str(alert_err) == "queued" else ("sent" if sent else f"error:{alert_err}")
        )
    if sent:
        if str(alert_err) == "queued":
            _LOGGER.warning("db init alert queued via outbox")
        else:
            _LOGGER.warning("db init alert sent via webhook")
    elif alert_err != "disabled":
        _LOGGER.error("db init alert webhook failed: %s", alert_err)


def _record_db_runtime_failure(kind: str, exc: Exception, elapsed_ms: Optional[float] = None) -> None:
    if not _DB_ENABLED:
        return
    safe_ms = _safe_elapsed_ms(elapsed_ms)
    failure_key = f"{kind}_failures"
    with _DB_RUNTIME_LOCK:
        if elapsed_ms is not None:
            _record_db_operation_stats_locked(kind, safe_ms)
        if _is_db_lock_contention_error(exc):
            _DB_RUNTIME_STATS["lock_contention_total"] = int(_DB_RUNTIME_STATS.get("lock_contention_total") or 0) + 1
            lock_wait = float(_DB_RUNTIME_STATS.get("lock_wait_ms_total") or 0.0) + safe_ms
            _DB_RUNTIME_STATS["lock_wait_ms_total"] = round(lock_wait, 4)
        current = int(_DB_RUNTIME_STATS.get(failure_key) or 0)
        _DB_RUNTIME_STATS[failure_key] = current + 1
        _DB_RUNTIME_STATS["last_error"] = f"{kind}: {exc}"
        _DB_RUNTIME_STATS["last_error_at"] = _now_iso()
        snapshot = deepcopy(_DB_RUNTIME_STATS)
    total = _db_runtime_failures_total_from_stats(snapshot)
    trace = getattr(exc, "__traceback__", None)
    if trace is not None:
        _LOGGER.error(
            "db persistence operation failed kind=%s total=%s error=%s",
            kind,
            total,
            exc,
            exc_info=(type(exc), exc, trace),
        )
    else:
        _LOGGER.error(
            "db persistence operation failed kind=%s total=%s error=%s",
            kind,
            total,
            exc,
        )
    _maybe_alert_db_runtime_failure(kind=kind, exc=exc, stats_snapshot=snapshot, total=total)


def _record_db_write_success(kind: str, elapsed_ms: float) -> None:
    safe_ms = _safe_elapsed_ms(elapsed_ms)
    with _DB_RUNTIME_LOCK:
        _record_db_operation_stats_locked(kind, safe_ms)
        _DB_RUNTIME_STATS["last_write_kind"] = str(kind)
        _DB_RUNTIME_STATS["last_write_ms"] = round(safe_ms, 4)
        _DB_RUNTIME_STATS["last_write_at"] = _now_iso()
        prev_max = float(_DB_RUNTIME_STATS.get("max_write_ms") or 0.0)
        if safe_ms > prev_max:
            _DB_RUNTIME_STATS["max_write_ms"] = round(safe_ms, 4)


def _record_db_read_success(kind: str, elapsed_ms: float) -> None:
    safe_ms = _safe_elapsed_ms(elapsed_ms)
    with _DB_RUNTIME_LOCK:
        _record_db_operation_stats_locked(kind, safe_ms)


def _collect_db_storage_stats() -> Tuple[Dict[str, Any], str]:
    if not _db_is_enabled():
        return {}, ""
    now = time.time()
    with _DB_HEALTH_LOCK:
        cached_ts = float(_DB_HEALTH_STATS_CACHE.get("ts_epoch") or 0.0)
        cached_stats = _DB_HEALTH_STATS_CACHE.get("stats")
        cached_error = str(_DB_HEALTH_STATS_CACHE.get("error") or "")
        if (cached_stats or cached_error) and now - cached_ts < float(_DB_HEALTH_STATS_TTL_SECONDS):
            return deepcopy(cached_stats if isinstance(cached_stats, dict) else {}), cached_error

    backend = str(_DB_BACKEND or "").strip() or "postgres"
    stats: Dict[str, Any] = {
        "backend": backend,
        "db_path": str(_DB_PATH),
        "db_size_bytes": 0,
        "free_bytes": 0,
        "fragmentation_pct": 0.0,
    }
    error_text = ""
    try:
        if backend == "sqlite":
            db_path = Path(getattr(_DB, "db_path", _DB_PATH))
            file_size = int(db_path.stat().st_size) if db_path.exists() else 0
            page_size = 0
            page_count = 0
            freelist_count = 0
            connect_fn = getattr(_DB, "_connect", None)
            if callable(connect_fn):
                conn = connect_fn()
                try:
                    page_size_row = conn.execute("PRAGMA page_size").fetchone()
                    page_count_row = conn.execute("PRAGMA page_count").fetchone()
                    freelist_row = conn.execute("PRAGMA freelist_count").fetchone()
                    page_size = int(page_size_row[0] or 0) if page_size_row is not None else 0
                    page_count = int(page_count_row[0] or 0) if page_count_row is not None else 0
                    freelist_count = int(freelist_row[0] or 0) if freelist_row is not None else 0
                finally:
                    close_fn = getattr(conn, "close", None)
                    if callable(close_fn):
                        close_fn()
            logical_size = int(page_size * page_count) if page_size > 0 and page_count >= 0 else file_size
            free_bytes = int(page_size * freelist_count) if page_size > 0 and freelist_count >= 0 else 0
            stats.update(
                {
                    "db_path": str(db_path),
                    "db_size_bytes": logical_size,
                    "file_size_bytes": file_size,
                    "free_bytes": free_bytes,
                    "fragmentation_pct": round((free_bytes / logical_size) * 100.0, 4) if logical_size > 0 else 0.0,
                    "page_size": page_size,
                    "page_count": page_count,
                    "freelist_count": freelist_count,
                }
            )
        elif backend == "postgres":
            connect_fn = getattr(_DB, "_connect", None)
            if callable(connect_fn):
                with connect_fn() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT pg_database_size(current_database()) AS db_size_bytes")
                        row = cur.fetchone()
                if isinstance(row, dict):
                    stats["db_size_bytes"] = int(row.get("db_size_bytes") or 0)
                elif row is not None:
                    stats["db_size_bytes"] = int(row[0] or 0)
    except Exception as exc:
        error_text = str(exc)

    with _DB_HEALTH_LOCK:
        _DB_HEALTH_STATS_CACHE["ts_epoch"] = now
        _DB_HEALTH_STATS_CACHE["stats"] = deepcopy(stats)
        _DB_HEALTH_STATS_CACHE["error"] = error_text
    return deepcopy(stats), error_text


def _db_runtime_failures_total() -> int:
    with _DB_RUNTIME_LOCK:
        return _db_runtime_failures_total_from_stats(_DB_RUNTIME_STATS)


def _db_runtime_failure_snapshot() -> Dict[str, Any]:
    with _DB_RUNTIME_LOCK:
        snapshot = deepcopy(_DB_RUNTIME_STATS)
        snapshot["alert"] = {
            "enabled": bool(_DB_ALERT_WEBHOOK_URL),
            "threshold": int(_DB_ALERT_THRESHOLD),
            "cooldown_seconds": int(_DB_ALERT_COOLDOWN_SECONDS),
            "last_alert_at": str(_DB_RUNTIME_ALERT_STATE.get("last_alert_at") or ""),
            "last_alert_total": int(_DB_RUNTIME_ALERT_STATE.get("last_alert_total") or 0),
            "last_alert_error": str(_DB_RUNTIME_ALERT_STATE.get("last_alert_error") or ""),
            "last_webhook_status": str(_DB_RUNTIME_ALERT_STATE.get("last_webhook_status") or ""),
        }
        return snapshot


def _iso_datetime_to_epoch(value: str) -> float:
    text = str(value or "").strip()
    if not text:
        return 0.0
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        return float(datetime.fromisoformat(normalized).timestamp())
    except Exception:
        return 0.0


def _current_db_status() -> str:
    db_status = "disabled"
    if _DB_ENABLED:
        db_status = "ok" if _DB_READY else "error"
    if db_status == "ok" and _db_runtime_failures_total() > 0:
        db_status = "degraded"
    return db_status


def _audit_event(
    action: str,
    *,
    entity: str,
    entity_id: str = "",
    detail: Optional[Dict[str, Any]] = None,
    owner: Optional[str] = None,
) -> None:
    if not _db_is_enabled():
        return
    owner_key = _safe_user_key(owner) if owner else _record_owner_key()
    started = time.perf_counter()
    try:
        _DB_SERVICE.append_audit_log(
            owner=owner_key,
            action=str(action),
            entity=str(entity),
            entity_id=str(entity_id or ""),
            detail=detail or {},
        )
        _record_db_write_success("audit_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        # Audit must not break API behavior.
        _record_db_runtime_failure("audit_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return


def _persist_strategy_record(strategy_key: str, strategy: Dict[str, Any]) -> None:
    if not _db_is_enabled():
        return
    if not strategy_key or not isinstance(strategy, dict):
        return
    owner = str(strategy.get("owner") or "").strip() or _record_owner_key()
    started = time.perf_counter()
    try:
        _DB_SERVICE.upsert_strategy(strategy_key, owner, deepcopy(strategy))
        _record_db_write_success("strategy_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("strategy_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return


def _delete_strategy_record(strategy_key: str) -> None:
    if not _db_is_enabled():
        return
    if not strategy_key:
        return
    started = time.perf_counter()
    try:
        _DB_SERVICE.delete_strategy(strategy_key)
        _record_db_write_success("strategy_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("strategy_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return


def _persist_backtest_record(run_id: str, record: Dict[str, Any]) -> None:
    if not _db_is_enabled():
        return
    if not run_id or not isinstance(record, dict):
        return
    owner = str(record.get("owner") or "").strip() or _record_owner_key()
    started = time.perf_counter()
    try:
        _DB_SERVICE.upsert_backtest(run_id, owner, deepcopy(record))
        _record_db_write_success("backtest_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("backtest_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return


def _persist_risk_state(strategy_key: str, state: Dict[str, Any], owner: Optional[str] = None) -> None:
    if not _db_is_enabled():
        return
    if not strategy_key or not isinstance(state, dict):
        return
    owner_key = _safe_user_key(owner) if owner else _record_owner_key()
    started = time.perf_counter()
    try:
        _DB_SERVICE.upsert_risk_state(owner_key, strategy_key, deepcopy(state))
        _record_db_write_success("risk_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("risk_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return


def _delete_risk_state(strategy_key: str, owner: Optional[str] = None) -> None:
    if not _db_is_enabled():
        return
    if not strategy_key:
        return
    owner_key = _safe_user_key(owner) if owner else _record_owner_key()
    started = time.perf_counter()
    try:
        _DB_SERVICE.delete_risk_state(owner_key, strategy_key)
        _record_db_write_success("risk_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("risk_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return


def _append_risk_event(
    strategy_key: str,
    event_type: str,
    *,
    rule: str,
    message: str,
    detail: Optional[Dict[str, Any]] = None,
    owner: Optional[str] = None,
    ts_utc: Optional[str] = None,
) -> None:
    if not _db_is_enabled():
        return
    if not strategy_key:
        return
    owner_key = _safe_user_key(owner) if owner else _record_owner_key()
    started = time.perf_counter()
    try:
        _DB_SERVICE.append_risk_event(
            owner=owner_key,
            strategy_key=strategy_key,
            event_type=str(event_type or "").strip(),
            rule=str(rule or "").strip(),
            message=str(message or "").strip(),
            detail=deepcopy(detail or {}),
            ts_utc=ts_utc,
        )
        _record_db_write_success("risk_event_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("risk_event_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return


def _persist_market_ticks(config_path: str, ticks: List[Dict[str, Any]]) -> None:
    if not _db_is_enabled():
        return
    if not isinstance(ticks, list) or not ticks:
        return
    started = time.perf_counter()
    try:
        _DB_SERVICE.upsert_market_ticks(ticks, source_config_path=str(config_path or ""))
        _record_db_write_success("market_tick_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("market_tick_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return


def _persist_market_klines(config_path: str, timeframe: str, rows: List[Dict[str, Any]]) -> None:
    if not _db_is_enabled():
        return
    if not isinstance(rows, list) or not rows:
        return
    started = time.perf_counter()
    try:
        _DB_SERVICE.upsert_market_klines(
            rows,
            timeframe=str(timeframe or ""),
            source_config_path=str(config_path or ""),
        )
        _record_db_write_success("market_kline_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("market_kline_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return


def _db_ensure_user(username: str, *, role: str = "user") -> Optional[Dict[str, Any]]:
    if not _db_is_enabled():
        return None
    started = time.perf_counter()
    try:
        row = _DB_SERVICE.ensure_user(username, role=role, display_name=username)
        _record_db_write_success("auth_user_write", (time.perf_counter() - started) * 1000.0)
        return row
    except Exception as exc:
        _record_db_runtime_failure("auth_user_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return None


def _pbkdf2_password_hash(password: str, username: str) -> str:
    salt = hashlib.sha256(f"quant:{username.lower()}".encode("utf-8")).hexdigest()[:32]
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        str(password or "").encode("utf-8"),
        salt.encode("utf-8"),
        120000,
        dklen=32,
    ).hex()
    return f"pbkdf2_sha256$120000${salt}${digest}"


def _db_upsert_user_credential(username: str, password: str) -> None:
    if not _db_is_enabled():
        return
    started = time.perf_counter()
    try:
        password_hash = _pbkdf2_password_hash(password, username)
        _DB_SERVICE.upsert_user_credential(
            username=str(username or "").strip().lower(),
            password_hash=password_hash,
            algorithm="pbkdf2_sha256",
        )
        _record_db_write_success("auth_user_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("auth_user_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)


def _db_create_auth_session(
    *,
    session_id: str,
    username: str,
    expires_at: str,
    client_ip: str = "",
    user_agent: str = "",
) -> bool:
    if not _db_is_enabled():
        return False
    started = time.perf_counter()
    try:
        _DB_SERVICE.create_auth_session(
            session_id=session_id,
            username=str(username or "").strip().lower(),
            expires_at=str(expires_at or ""),
            client_ip=str(client_ip or ""),
            user_agent=str(user_agent or ""),
        )
        _record_db_write_success("auth_session_write", (time.perf_counter() - started) * 1000.0)
        return True
    except Exception as exc:
        _record_db_runtime_failure("auth_session_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return False


def _db_get_auth_session(session_id: str) -> Optional[Dict[str, Any]]:
    if not _db_is_enabled():
        return None
    started = time.perf_counter()
    try:
        row = _DB_SERVICE.get_auth_session(str(session_id or ""))
        _record_db_read_success("auth_session_read", (time.perf_counter() - started) * 1000.0)
        return row if isinstance(row, dict) else None
    except Exception as exc:
        _record_db_runtime_failure("auth_session_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return None


def _db_revoke_auth_session(session_id: str) -> None:
    if not _db_is_enabled():
        return
    started = time.perf_counter()
    try:
        _DB_SERVICE.revoke_auth_session(str(session_id or ""))
        _record_db_write_success("auth_session_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("auth_session_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)


def _db_record_login_attempt(*, username: str, client_ip: str, success: bool, reason: str = "") -> None:
    if not _db_is_enabled():
        return
    started = time.perf_counter()
    try:
        _DB_SERVICE.record_login_attempt(
            username=str(username or "").strip().lower(),
            client_ip=str(client_ip or "").strip() or "unknown",
            success=bool(success),
            reason=str(reason or ""),
        )
        _record_db_write_success("auth_login_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("auth_login_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)


def _db_set_lockout(lock_key: str, locked_until_epoch: float) -> None:
    if not _db_is_enabled():
        return
    if not lock_key:
        return
    locked_until_iso = datetime.fromtimestamp(float(locked_until_epoch), tz=timezone.utc).isoformat()
    started = time.perf_counter()
    try:
        _DB_SERVICE.set_lockout(lock_key=str(lock_key), locked_until=locked_until_iso)
        _record_db_write_success("auth_login_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("auth_login_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)


def _db_get_active_lockout_seconds(lock_keys: List[str], now_ts: float) -> int:
    if not _db_is_enabled() or not lock_keys:
        return 0
    started = time.perf_counter()
    try:
        rows = _DB_SERVICE.get_active_lockouts(
            lock_keys=[str(item or "") for item in lock_keys if str(item or "").strip()],
            now_ts=datetime.fromtimestamp(float(now_ts), tz=timezone.utc).isoformat(),
        )
        _record_db_read_success("auth_login_read", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("auth_login_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return 0

    max_remaining = 0
    for value in (rows or {}).values():
        remaining = int(_iso_datetime_to_epoch(str(value)) - float(now_ts))
        if remaining > max_remaining:
            max_remaining = remaining
    return max(0, max_remaining)


def _db_clear_lockouts(lock_keys: List[str]) -> None:
    if not _db_is_enabled() or not lock_keys:
        return
    started = time.perf_counter()
    try:
        _DB_SERVICE.clear_lockouts([str(item or "") for item in lock_keys if str(item or "").strip()])
        _record_db_write_success("auth_login_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("auth_login_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)


def _sync_auth_users_to_database() -> None:
    if not _db_is_enabled():
        return
    known_users = set()
    for username in _DASHBOARD_CREDENTIALS.keys():
        user_text = str(username or "").strip().lower()
        if user_text:
            known_users.add(user_text)
    known_users.add(_safe_user_key(_AUTH_FALLBACK_USER))
    known_users.add(str(_GUEST_USERNAME or "guest").strip().lower() or "guest")
    for username in sorted(known_users):
        role = "admin" if username == _safe_user_key(_AUTH_FALLBACK_USER) else ("guest" if username == "guest" else "user")
        _db_ensure_user(username, role=role)
        password = _DASHBOARD_CREDENTIALS.get(username)
        if password:
            _db_upsert_user_credential(username, password)


def _default_user_preferences() -> Dict[str, Any]:
    return {
        "theme": "dark",
        "language": "zh",
        "selectedLiveStrategyId": "",
        "logsFilters": {},
        "backtestsFilters": {},
        "liveFilters": {},
    }


def _normalized_user_preferences(payload: Dict[str, Any]) -> Dict[str, Any]:
    defaults = _default_user_preferences()
    out = deepcopy(defaults)
    if not isinstance(payload, dict):
        return out
    theme = str(payload.get("theme") or out["theme"]).strip().lower()
    if theme in {"light", "dark"}:
        out["theme"] = theme
    language = str(payload.get("language") or out["language"]).strip().lower()
    if language in {"zh", "en"}:
        out["language"] = language
    selected = payload.get("selectedLiveStrategyId")
    if selected is None:
        out["selectedLiveStrategyId"] = ""
    else:
        out["selectedLiveStrategyId"] = str(selected).strip()
    logs_filters = payload.get("logsFilters")
    if isinstance(logs_filters, dict):
        out["logsFilters"] = deepcopy(logs_filters)
    backtests_filters = payload.get("backtestsFilters")
    if isinstance(backtests_filters, dict):
        out["backtestsFilters"] = deepcopy(backtests_filters)
    live_filters = payload.get("liveFilters")
    if isinstance(live_filters, dict):
        out["liveFilters"] = deepcopy(live_filters)
    return out


def _db_upsert_user_preferences(owner: str, preferences: Dict[str, Any]) -> None:
    if not _db_is_enabled():
        return
    owner_key = _safe_user_key(owner)
    if not owner_key:
        return
    started = time.perf_counter()
    try:
        _DB_SERVICE.upsert_user_preferences(owner_key, _normalized_user_preferences(preferences))
        _record_db_write_success("user_pref_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("user_pref_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)


def _db_get_user_preferences(owner: str) -> Dict[str, Any]:
    owner_key = _safe_user_key(owner)
    defaults = _default_user_preferences()
    if not _db_is_enabled() or not owner_key:
        return defaults
    started = time.perf_counter()
    try:
        row = _DB_SERVICE.get_user_preferences(owner_key)
        _record_db_read_success("user_pref_read", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("user_pref_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return defaults
    if not isinstance(row, dict):
        return defaults
    return _normalized_user_preferences(row.get("preferences") if isinstance(row.get("preferences"), dict) else {})


def _db_append_account_security_event(
    *,
    owner: str,
    event_type: str,
    severity: str,
    message: str,
    detail: Optional[Dict[str, Any]] = None,
    ts_utc: Optional[str] = None,
) -> None:
    if not _db_is_enabled():
        return
    owner_key = _safe_user_key(owner)
    if not owner_key:
        return
    started = time.perf_counter()
    try:
        _DB_SERVICE.append_account_security_event(
            owner=owner_key,
            event_type=str(event_type or "").strip(),
            severity=str(severity or "info").strip().lower() or "info",
            message=str(message or ""),
            detail=deepcopy(detail) if isinstance(detail, dict) else {},
            ts_utc=str(ts_utc or _now_iso()),
        )
        _record_db_write_success("auth_security_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("auth_security_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)


def _db_list_account_security_events(
    *,
    owner: Optional[str] = None,
    event_type: Optional[str] = None,
    start_ts: Optional[str] = None,
    end_ts: Optional[str] = None,
    cursor_id: Optional[int] = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    if not _db_is_enabled():
        return []
    owner_key = _safe_user_key(owner) if owner else None
    started = time.perf_counter()
    try:
        rows = _DB_SERVICE.list_account_security_events(
            owner=owner_key,
            event_type=str(event_type) if event_type else None,
            start_ts=start_ts,
            end_ts=end_ts,
            cursor_id=cursor_id,
            limit=limit,
        )
        _record_db_read_success("auth_security_read", (time.perf_counter() - started) * 1000.0)
        return [row for row in rows if isinstance(row, dict)]
    except Exception as exc:
        _record_db_runtime_failure("auth_security_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return []


def _db_create_api_token(
    *,
    owner: str,
    token_name: str,
    token_prefix: str,
    token_hash: str,
    scopes: List[str],
    expires_at: str = "",
    created_by: str = "",
) -> Optional[Dict[str, Any]]:
    if not _db_is_enabled():
        return None
    owner_key = _safe_user_key(owner)
    if not owner_key:
        return None
    started = time.perf_counter()
    try:
        row = _DB_SERVICE.create_api_token(
            owner=owner_key,
            token_name=str(token_name or ""),
            token_prefix=str(token_prefix or ""),
            token_hash=str(token_hash or ""),
            scopes=[str(item or "").strip() for item in (scopes or []) if str(item or "").strip()],
            expires_at=str(expires_at or ""),
            created_by=_safe_user_key(created_by) if created_by else "",
        )
        _record_db_write_success("auth_token_write", (time.perf_counter() - started) * 1000.0)
        return row if isinstance(row, dict) else None
    except Exception as exc:
        _record_db_runtime_failure("auth_token_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return None


def _db_list_api_tokens(
    *,
    owner: Optional[str] = None,
    include_revoked: bool = False,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    if not _db_is_enabled():
        return []
    owner_key = _safe_user_key(owner) if owner else None
    started = time.perf_counter()
    try:
        rows = _DB_SERVICE.list_api_tokens(
            owner=owner_key,
            include_revoked=bool(include_revoked),
            limit=int(limit),
        )
        _record_db_read_success("auth_token_read", (time.perf_counter() - started) * 1000.0)
        return [row for row in rows if isinstance(row, dict)]
    except Exception as exc:
        _record_db_runtime_failure("auth_token_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return []


def _db_get_active_api_token(plain_token: str) -> Optional[Dict[str, Any]]:
    if not _db_is_enabled():
        return None
    token_hash = _hash_api_token_value(plain_token)
    if not token_hash:
        return None
    started = time.perf_counter()
    try:
        row = _DB_SERVICE.get_active_api_token_by_hash(
            token_hash=token_hash,
            now_ts=_now_iso(),
        )
        _record_db_read_success("auth_token_read", (time.perf_counter() - started) * 1000.0)
        return row if isinstance(row, dict) else None
    except Exception as exc:
        _record_db_runtime_failure("auth_token_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return None


def _db_touch_api_token_last_used(token_id: int) -> None:
    if not _db_is_enabled():
        return
    try:
        token_id_int = int(token_id)
    except Exception:
        return
    if token_id_int <= 0:
        return
    started = time.perf_counter()
    try:
        _DB_SERVICE.touch_api_token_last_used(token_id_int)
        _record_db_write_success("auth_token_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("auth_token_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)


def _db_revoke_api_token(token_id: int, *, revoked_by: str = "") -> None:
    if not _db_is_enabled():
        return
    try:
        token_id_int = int(token_id)
    except Exception:
        return
    if token_id_int <= 0:
        return
    started = time.perf_counter()
    try:
        _DB_SERVICE.revoke_api_token(
            token_id_int,
            revoked_at=_now_iso(),
            revoked_by=_safe_user_key(revoked_by) if revoked_by else "",
        )
        _record_db_write_success("auth_token_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("auth_token_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)


def _db_has_active_api_tokens() -> bool:
    rows = _db_list_api_tokens(include_revoked=False, limit=1)
    return len(rows) > 0


def _db_list_roles() -> List[Dict[str, Any]]:
    if not _db_is_enabled():
        return []
    started = time.perf_counter()
    try:
        rows = _DB_SERVICE.list_roles()
        _record_db_read_success("rbac_read", (time.perf_counter() - started) * 1000.0)
        return [row for row in rows if isinstance(row, dict)]
    except Exception as exc:
        _record_db_runtime_failure("rbac_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return []


def _db_list_permissions() -> List[Dict[str, Any]]:
    if not _db_is_enabled():
        return []
    started = time.perf_counter()
    try:
        rows = _DB_SERVICE.list_permissions()
        _record_db_read_success("rbac_read", (time.perf_counter() - started) * 1000.0)
        return [row for row in rows if isinstance(row, dict)]
    except Exception as exc:
        _record_db_runtime_failure("rbac_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return []


def _db_list_user_roles(username: str) -> List[str]:
    if not _db_is_enabled():
        return []
    username_key = _safe_user_key(username)
    if not username_key:
        return []
    started = time.perf_counter()
    try:
        rows = _DB_SERVICE.list_user_roles(username_key)
        _record_db_read_success("rbac_read", (time.perf_counter() - started) * 1000.0)
        return [str(role) for role in rows if str(role or "").strip()]
    except Exception as exc:
        _record_db_runtime_failure("rbac_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return []


def _db_replace_user_roles(username: str, roles: List[str]) -> List[str]:
    if not _db_is_enabled():
        raise RuntimeError("db is disabled")
    username_key = _safe_user_key(username)
    if not username_key:
        raise ValueError("username is required")
    started = time.perf_counter()
    try:
        rows = _DB_SERVICE.replace_user_roles(
            username_key,
            [str(role or "").strip().lower() for role in (roles or []) if str(role or "").strip()],
        )
        _record_db_write_success("rbac_write", (time.perf_counter() - started) * 1000.0)
        return [str(role) for role in rows if str(role or "").strip()]
    except Exception as exc:
        _record_db_runtime_failure("rbac_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        raise


def _db_user_has_permission(username: str, permission_code: str) -> bool:
    if not _db_is_enabled():
        return False
    username_key = _safe_user_key(username)
    permission = str(permission_code or "").strip()
    if not username_key or not permission:
        return False
    started = time.perf_counter()
    try:
        allowed = bool(_DB_SERVICE.user_has_permission(username_key, permission))
        _record_db_read_success("rbac_read", (time.perf_counter() - started) * 1000.0)
        return allowed
    except Exception as exc:
        _record_db_runtime_failure("rbac_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return False


def _has_permission(permission_code: str, username: Optional[str] = None) -> bool:
    permission = str(permission_code or "").strip()
    if not permission:
        return False
    candidate = _resolve_effective_auth_username(username)
    if _is_admin_username(candidate):
        return True
    if _db_user_has_permission(candidate, permission):
        return True
    role = "guest" if _safe_user_key(candidate) == _safe_user_key(_GUEST_USERNAME) else "user"
    _db_ensure_user(candidate, role=role)
    return _db_user_has_permission(candidate, permission)


def _require_permission(permission_code: str) -> None:
    permission = str(permission_code or "").strip()
    if not permission:
        raise HTTPException(status_code=500, detail="permission code is empty")
    if _has_permission(permission):
        return
    raise HTTPException(status_code=403, detail=f"permission denied: {permission}")


def _db_append_runtime_log(
    *,
    owner: str,
    log_type: str,
    level: str,
    source: str,
    message: str,
    strategy_id: str = "",
    backtest_id: str = "",
    detail: Optional[Dict[str, Any]] = None,
    ts_utc: Optional[str] = None,
) -> None:
    if not _db_is_enabled():
        return
    owner_key = _safe_user_key(owner)
    if not owner_key:
        return
    started = time.perf_counter()
    try:
        _DB_SERVICE.append_runtime_log(
            owner=owner_key,
            log_type=str(log_type or "system"),
            level=str(level or "info"),
            source=str(source or "system"),
            message=str(message or ""),
            strategy_id=str(strategy_id or ""),
            backtest_id=str(backtest_id or ""),
            detail=deepcopy(detail) if isinstance(detail, dict) else {},
            ts_utc=str(ts_utc or _now_iso()),
        )
        _record_db_write_success("runtime_log_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("runtime_log_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)


def _db_list_runtime_logs(
    *,
    owner: str,
    log_type: Optional[str] = None,
    level: Optional[str] = None,
    q: Optional[str] = None,
    strategy_id: Optional[str] = None,
    start_ts: Optional[str] = None,
    end_ts: Optional[str] = None,
    cursor_id: Optional[int] = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    if not _db_is_enabled():
        return []
    owner_key = _safe_user_key(owner)
    if not owner_key:
        return []
    started = time.perf_counter()
    try:
        rows = _DB_SERVICE.list_runtime_logs(
            owner=owner_key,
            log_type=str(log_type) if log_type else None,
            level=str(level) if level else None,
            q=str(q) if q else None,
            strategy_id=str(strategy_id) if strategy_id else None,
            start_ts=start_ts,
            end_ts=end_ts,
            cursor_id=cursor_id,
            limit=limit,
        )
        _record_db_read_success("runtime_log_read", (time.perf_counter() - started) * 1000.0)
        return [row for row in rows if isinstance(row, dict)]
    except Exception as exc:
        _record_db_runtime_failure("runtime_log_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return []


def _db_append_alert_delivery(
    *,
    owner: str,
    event: str,
    severity: str,
    message: str,
    webhook_url: str,
    status: str,
    retry_count: int = 0,
    http_status: Optional[int] = None,
    error_message: str = "",
    payload: Optional[Dict[str, Any]] = None,
    response_body: str = "",
    ts_utc: Optional[str] = None,
    duration_ms: Optional[float] = None,
) -> None:
    if not _db_is_enabled():
        return
    owner_key = _safe_user_key(owner)
    if not owner_key:
        return
    started = time.perf_counter()
    try:
        _DB_SERVICE.append_alert_delivery(
            owner=owner_key,
            event=str(event or ""),
            severity=str(severity or ""),
            message=str(message or ""),
            webhook_url=str(webhook_url or ""),
            status=str(status or ""),
            retry_count=max(0, int(retry_count)),
            http_status=http_status,
            error_message=str(error_message or ""),
            payload=deepcopy(payload) if isinstance(payload, dict) else {},
            response_body=str(response_body or ""),
            ts_utc=str(ts_utc or _now_iso()),
            duration_ms=duration_ms,
        )
        _record_db_write_success("alert_delivery_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        with _DB_RUNTIME_LOCK:
            current = int(_DB_RUNTIME_STATS.get("alert_delivery_write_failures") or 0)
            _DB_RUNTIME_STATS["alert_delivery_write_failures"] = current + 1
            _DB_RUNTIME_STATS["last_error"] = f"alert_delivery_write: {exc}"
            _DB_RUNTIME_STATS["last_error_at"] = _now_iso()
            _record_db_operation_stats_locked("alert_delivery_write", _safe_elapsed_ms(elapsed_ms))
        _LOGGER.error("db alert delivery persist failed: %s", exc)


def _db_list_alert_deliveries(
    *,
    owner: Optional[str] = None,
    event: Optional[str] = None,
    status: Optional[str] = None,
    start_ts: Optional[str] = None,
    end_ts: Optional[str] = None,
    cursor_id: Optional[int] = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    if not _db_is_enabled():
        return []
    started = time.perf_counter()
    try:
        rows = _DB_SERVICE.list_alert_deliveries(
            owner=owner,
            event=event,
            status=status,
            start_ts=start_ts,
            end_ts=end_ts,
            cursor_id=cursor_id,
            limit=limit,
        )
        _record_db_read_success("alert_delivery_read", (time.perf_counter() - started) * 1000.0)
        return [row for row in rows if isinstance(row, dict)]
    except Exception as exc:
        _record_db_runtime_failure("alert_delivery_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return []


def _db_enqueue_alert_outbox(
    *,
    owner: str,
    event: str,
    severity: str,
    message: str,
    webhook_url: str,
    payload: Optional[Dict[str, Any]] = None,
    max_retries: int = 0,
    available_at: Optional[str] = None,
    created_at: Optional[str] = None,
) -> int:
    if not _db_is_enabled():
        return 0
    owner_key = _safe_user_key(owner)
    if not owner_key:
        return 0
    started = time.perf_counter()
    try:
        outbox_id = _DB_SERVICE.enqueue_alert_outbox(
            owner=owner_key,
            event=str(event or ""),
            severity=str(severity or ""),
            message=str(message or ""),
            webhook_url=str(webhook_url or ""),
            payload=deepcopy(payload) if isinstance(payload, dict) else {},
            max_retries=max(0, int(max_retries)),
            available_at=str(available_at or ""),
            created_at=str(created_at or ""),
        )
        _record_db_write_success("alert_outbox_enqueue_write", (time.perf_counter() - started) * 1000.0)
        return int(outbox_id or 0)
    except Exception as exc:
        _record_db_runtime_failure(
            "alert_outbox_enqueue_write",
            exc,
            elapsed_ms=(time.perf_counter() - started) * 1000.0,
        )
        return 0


def _db_list_due_alert_outbox(
    *,
    limit: int = 50,
    now_ts: Optional[str] = None,
) -> List[Dict[str, Any]]:
    if not _db_is_enabled():
        return []
    started = time.perf_counter()
    try:
        rows = _DB_SERVICE.list_due_alert_outbox(
            now_ts=str(now_ts or _now_iso()),
            limit=max(1, int(limit)),
        )
        _record_db_read_success("alert_outbox_read", (time.perf_counter() - started) * 1000.0)
        return [row for row in rows if isinstance(row, dict)]
    except Exception as exc:
        _record_db_runtime_failure("alert_outbox_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return []


def _db_finalize_alert_outbox(
    outbox_id: int,
    *,
    status: str,
    retry_count: int,
    available_at: Optional[str] = None,
    http_status: Optional[int] = None,
    error_message: str = "",
    response_body: str = "",
    dispatched_at: Optional[str] = None,
) -> None:
    if not _db_is_enabled():
        return
    started = time.perf_counter()
    try:
        _DB_SERVICE.finalize_alert_outbox(
            int(outbox_id),
            status=str(status or ""),
            retry_count=max(0, int(retry_count)),
            available_at=str(available_at or ""),
            http_status=http_status,
            error_message=str(error_message or ""),
            response_body=str(response_body or ""),
            dispatched_at=str(dispatched_at or ""),
        )
        _record_db_write_success("alert_outbox_update_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure(
            "alert_outbox_update_write",
            exc,
            elapsed_ms=(time.perf_counter() - started) * 1000.0,
        )


def _db_append_ws_connection_event(
    *,
    owner: str,
    event_type: str,
    connection_id: str,
    strategy_id: str = "",
    config_path: str = "",
    refresh_ms: int = 0,
    client_ip: str = "",
    user_agent: str = "",
    detail: Optional[Dict[str, Any]] = None,
    ts_utc: Optional[str] = None,
) -> None:
    if not _db_is_enabled():
        return
    owner_raw = str(owner or "").strip()
    owner_key = _safe_user_key(owner_raw) if owner_raw else "anonymous"
    if not str(connection_id or "").strip():
        return
    started = time.perf_counter()
    try:
        _DB_SERVICE.append_ws_connection_event(
            owner=owner_key,
            event_type=str(event_type or ""),
            connection_id=str(connection_id or ""),
            strategy_id=str(strategy_id or ""),
            config_path=str(config_path or ""),
            refresh_ms=max(0, int(refresh_ms)),
            client_ip=str(client_ip or ""),
            user_agent=str(user_agent or ""),
            detail=deepcopy(detail) if isinstance(detail, dict) else {},
            ts_utc=str(ts_utc or _now_iso()),
        )
        _record_db_write_success("ws_event_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("ws_event_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)


def _db_list_ws_connection_events(
    *,
    owner: Optional[str] = None,
    event_type: Optional[str] = None,
    strategy_id: Optional[str] = None,
    start_ts: Optional[str] = None,
    end_ts: Optional[str] = None,
    cursor_id: Optional[int] = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    if not _db_is_enabled():
        return []
    started = time.perf_counter()
    try:
        rows = _DB_SERVICE.list_ws_connection_events(
            owner=owner,
            event_type=event_type,
            strategy_id=strategy_id,
            start_ts=start_ts,
            end_ts=end_ts,
            cursor_id=cursor_id,
            limit=limit,
        )
        _record_db_read_success("ws_event_read", (time.perf_counter() - started) * 1000.0)
        return [row for row in rows if isinstance(row, dict)]
    except Exception as exc:
        _record_db_runtime_failure("ws_event_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return []


def _db_append_strategy_diagnostics_snapshot(
    *,
    owner: str,
    strategy_id: str,
    source_path: str,
    snapshot: Dict[str, Any],
    ts_utc: Optional[str] = None,
) -> None:
    if not _db_is_enabled():
        return
    owner_key = _safe_user_key(owner)
    if not owner_key:
        return
    started = time.perf_counter()
    try:
        _DB_SERVICE.append_strategy_diagnostics_snapshot(
            owner=owner_key,
            strategy_id=str(strategy_id or ""),
            source_path=str(source_path or ""),
            snapshot=deepcopy(snapshot) if isinstance(snapshot, dict) else {},
            ts_utc=str(ts_utc or _now_iso()),
        )
        _record_db_write_success("strategy_diag_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("strategy_diag_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)


def _db_list_strategy_diagnostics_snapshots(
    *,
    owner: Optional[str] = None,
    strategy_id: Optional[str] = None,
    start_ts: Optional[str] = None,
    end_ts: Optional[str] = None,
    cursor_id: Optional[int] = None,
    limit: int = 200,
    include_snapshot: bool = False,
) -> List[Dict[str, Any]]:
    if not _db_is_enabled():
        return []
    started = time.perf_counter()
    try:
        rows = _DB_SERVICE.list_strategy_diagnostics_snapshots(
            owner=owner,
            strategy_id=strategy_id,
            start_ts=start_ts,
            end_ts=end_ts,
            cursor_id=cursor_id,
            limit=limit,
            include_snapshot=include_snapshot,
        )
        _record_db_read_success("strategy_diag_read", (time.perf_counter() - started) * 1000.0)
        return [row for row in rows if isinstance(row, dict)]
    except Exception as exc:
        _record_db_runtime_failure("strategy_diag_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return []


def _db_replace_backtest_trades(
    *,
    run_id: str,
    owner: str,
    rows: List[Dict[str, Any]],
) -> int:
    if not _db_is_enabled():
        return 0
    run_key = str(run_id or "").strip()
    owner_key = _safe_user_key(owner)
    if not run_key or not owner_key:
        return 0
    started = time.perf_counter()
    try:
        count = int(
            _DB_SERVICE.replace_backtest_trades(
                run_id=run_key,
                owner=owner_key,
                rows=rows,
            )
            or 0
        )
        _record_db_write_success("backtest_detail_write", (time.perf_counter() - started) * 1000.0)
        return count
    except Exception as exc:
        _record_db_runtime_failure("backtest_detail_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return 0


def _db_replace_backtest_equity_points(
    *,
    run_id: str,
    owner: str,
    rows: List[Dict[str, Any]],
) -> int:
    if not _db_is_enabled():
        return 0
    run_key = str(run_id or "").strip()
    owner_key = _safe_user_key(owner)
    if not run_key or not owner_key:
        return 0
    started = time.perf_counter()
    try:
        count = int(
            _DB_SERVICE.replace_backtest_equity_points(
                run_id=run_key,
                owner=owner_key,
                rows=rows,
            )
            or 0
        )
        _record_db_write_success("backtest_detail_write", (time.perf_counter() - started) * 1000.0)
        return count
    except Exception as exc:
        _record_db_runtime_failure("backtest_detail_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return 0


def _db_list_backtest_trades(
    *,
    run_id: str,
    owner: str,
    limit: int = 10000,
) -> List[Dict[str, Any]]:
    if not _db_is_enabled():
        return []
    run_key = str(run_id or "").strip()
    owner_key = _safe_user_key(owner)
    if not run_key or not owner_key:
        return []
    started = time.perf_counter()
    try:
        rows = _DB_SERVICE.list_backtest_trades(
            run_id=run_key,
            owner=owner_key,
            limit=limit,
        )
        _record_db_read_success("backtest_detail_read", (time.perf_counter() - started) * 1000.0)
        return [row for row in rows if isinstance(row, dict)]
    except Exception as exc:
        _record_db_runtime_failure("backtest_detail_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return []


def _db_list_backtest_equity_points(
    *,
    run_id: str,
    owner: str,
    limit: int = 10000,
) -> List[Dict[str, Any]]:
    if not _db_is_enabled():
        return []
    run_key = str(run_id or "").strip()
    owner_key = _safe_user_key(owner)
    if not run_key or not owner_key:
        return []
    started = time.perf_counter()
    try:
        rows = _DB_SERVICE.list_backtest_equity_points(
            run_id=run_key,
            owner=owner_key,
            limit=limit,
        )
        _record_db_read_success("backtest_detail_read", (time.perf_counter() - started) * 1000.0)
        return [row for row in rows if isinstance(row, dict)]
    except Exception as exc:
        _record_db_runtime_failure("backtest_detail_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return []


def _db_upsert_data_file(
    *,
    owner: str,
    scope: str,
    file_key: str,
    file_name: str = "",
    source_path: str = "",
    content_type: str = "text/plain",
    content_encoding: str = "utf-8",
    content_text: str = "",
    meta: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    if not _db_is_enabled():
        return None
    owner_key = _safe_user_key(owner)
    scope_key = str(scope or "").strip().lower()
    key = str(file_key or "").strip()
    if not owner_key or not scope_key or not key:
        return None
    started = time.perf_counter()
    try:
        row = _DB_SERVICE.upsert_data_file(
            owner=owner_key,
            scope=scope_key,
            file_key=key,
            file_name=str(file_name or ""),
            source_path=str(source_path or ""),
            content_type=str(content_type or "text/plain"),
            content_encoding=str(content_encoding or "utf-8"),
            content_text=str(content_text or ""),
            meta=deepcopy(meta) if isinstance(meta, dict) else {},
        )
        _record_db_write_success("data_file_write", (time.perf_counter() - started) * 1000.0)
        return row if isinstance(row, dict) else None
    except Exception as exc:
        _record_db_runtime_failure("data_file_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return None


def _db_get_data_file(
    *,
    owner: str,
    scope: str,
    file_key: str,
) -> Optional[Dict[str, Any]]:
    if not _db_is_enabled():
        return None
    owner_key = _safe_user_key(owner)
    scope_key = str(scope or "").strip().lower()
    key = str(file_key or "").strip()
    if not owner_key or not scope_key or not key:
        return None
    started = time.perf_counter()
    try:
        row = _DB_SERVICE.get_data_file(
            owner=owner_key,
            scope=scope_key,
            file_key=key,
        )
        _record_db_read_success("data_file_read", (time.perf_counter() - started) * 1000.0)
        return row if isinstance(row, dict) else None
    except Exception as exc:
        _record_db_runtime_failure("data_file_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return None


def _db_enqueue_strategy_compile_job(strategy_key: str, owner: str) -> Optional[Dict[str, Any]]:
    if not _db_is_enabled():
        return None
    started = time.perf_counter()
    try:
        row = _DB_SERVICE.enqueue_strategy_compile_job(str(strategy_key), str(owner))
        _record_db_write_success("strategy_compile_write", (time.perf_counter() - started) * 1000.0)
        return row if isinstance(row, dict) else None
    except Exception as exc:
        _record_db_runtime_failure("strategy_compile_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return None


def _db_update_strategy_compile_job(
    job_id: int,
    *,
    status: str,
    error_message: str = "",
    started_at: Optional[str] = None,
    finished_at: Optional[str] = None,
) -> None:
    if not _db_is_enabled():
        return
    started = time.perf_counter()
    try:
        _DB_SERVICE.update_strategy_compile_job(
            int(job_id),
            status=str(status),
            error_message=str(error_message or ""),
            started_at=started_at,
            finished_at=finished_at,
        )
        _record_db_write_success("strategy_compile_write", (time.perf_counter() - started) * 1000.0)
    except Exception as exc:
        _record_db_runtime_failure("strategy_compile_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)


def _db_list_strategy_compile_jobs(
    *,
    owner: Optional[str] = None,
    strategy_key: Optional[str] = None,
    limit: int = 500,
) -> List[Dict[str, Any]]:
    if not _db_is_enabled():
        return []
    started = time.perf_counter()
    try:
        rows = _DB_SERVICE.list_strategy_compile_jobs(
            owner=str(owner).strip() if isinstance(owner, str) and owner.strip() else None,
            strategy_key=str(strategy_key).strip() if isinstance(strategy_key, str) and strategy_key.strip() else None,
            limit=max(1, min(int(limit), 2000)),
        )
        _record_db_read_success("strategy_compile_read", (time.perf_counter() - started) * 1000.0)
        return [row for row in rows if isinstance(row, dict)]
    except Exception as exc:
        _record_db_runtime_failure("strategy_compile_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return []


def _db_list_strategy_scripts(
    *,
    owner: str,
    strategy_key: str,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    if not _db_is_enabled():
        return []
    started = time.perf_counter()
    try:
        rows = _DB_SERVICE.list_strategy_scripts(owner=owner, strategy_key=strategy_key, limit=limit)
        _record_db_read_success("strategy_script_read", (time.perf_counter() - started) * 1000.0)
        return [row for row in rows if isinstance(row, dict)]
    except Exception as exc:
        _record_db_runtime_failure("strategy_script_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return []


def _db_get_latest_strategy_script(*, owner: str, strategy_key: str) -> Optional[Dict[str, Any]]:
    rows = _db_list_strategy_scripts(owner=owner, strategy_key=strategy_key, limit=1)
    return rows[0] if rows else None


def _db_add_strategy_script(
    *,
    strategy_key: str,
    owner: str,
    script_type: str,
    script_path: str,
    script_hash: str,
    source_config: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    if not _db_is_enabled():
        return None
    started = time.perf_counter()
    try:
        row = _DB_SERVICE.add_strategy_script(
            strategy_key=strategy_key,
            owner=owner,
            script_type=script_type,
            script_path=script_path,
            script_hash=script_hash,
            source_config=deepcopy(source_config),
        )
        _record_db_write_success("strategy_script_write", (time.perf_counter() - started) * 1000.0)
        return row if isinstance(row, dict) else None
    except Exception as exc:
        _record_db_runtime_failure("strategy_script_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
        return None


def _resolve_runtime_db_path(path_text: str) -> Path:
    text = str(path_text or "").strip() or str(_DB_PATH_TEXT)
    return _resolve_runtime_db_path_with_text(text)


def _reload_db_runtime(
    *,
    enabled: Optional[bool] = None,
    db_path_text: Optional[str] = None,
    backend: Optional[str] = None,
    postgres_dsn: Optional[str] = None,
    preserve_state: bool = True,
) -> Dict[str, Any]:
    global _DB_ENABLED, _DB_READY, _DB_INIT_ERROR, _DB, _DB_PATH, _DB_PATH_TEXT, _DB_BACKEND, _DB_POSTGRES_DSN
    with _DB_SWITCH_LOCK:
        old_db = _DB
        _stop_db_alert_outbox_worker()
        previous = {
            "enabled": bool(_DB_ENABLED),
            "ready": bool(_DB_READY),
            "backend": str(_DB_BACKEND),
            "db_path": str(_DB_PATH),
            "postgres_dsn": _mask_postgres_dsn(_DB_POSTGRES_DSN),
            "db_error": str(_DB_INIT_ERROR or ""),
        }
        if enabled is False:
            raise RuntimeError("database disable is not allowed: Postgres persistence is mandatory")
        next_backend = _normalize_db_backend(str(_DB_BACKEND) if backend is None else backend)
        next_path_text = str(db_path_text or _DB_PATH_TEXT).strip() or _DB_PATH_TEXT
        next_postgres_dsn = (
            str(_DB_POSTGRES_DSN).strip() if postgres_dsn is None else str(postgres_dsn or "").strip()
        )
        next_path = _resolve_runtime_db_path(next_path_text)

        trial_store, resolved_backend, resolved_path, resolved_postgres_dsn = _build_db_store(
            backend=next_backend,
            db_path_text=next_path_text,
            postgres_dsn=next_postgres_dsn,
        )
        try:
            trial_store.initialize()
        except Exception:
            _close_db_repository(trial_store)
            raise
        try:
            if preserve_state:
                for strategy_key, strategy in list(_STRATEGY_STORE.items()):
                    if not isinstance(strategy, dict):
                        continue
                    owner = str(strategy.get("owner") or _record_owner_key()).strip() or _record_owner_key()
                    trial_store.upsert_strategy(str(strategy_key), owner, deepcopy(strategy))
                for run_id, record in list(_BACKTEST_STORE.items()):
                    if not isinstance(record, dict):
                        continue
                    owner = str(record.get("owner") or _record_owner_key()).strip() or _record_owner_key()
                    trial_store.upsert_backtest(str(run_id), owner, deepcopy(record))
                for strategy_key, state in list(_RISK_STATE_STORE.items()):
                    if not isinstance(state, dict):
                        continue
                    owner_key = _strategy_owner_user_key(str(strategy_key)) or str(state.get("owner") or _record_owner_key())
                    owner_key = _safe_user_key(owner_key)
                    trial_store.upsert_risk_state(owner_key, str(strategy_key), deepcopy(state))
        except Exception:
            _close_db_repository(trial_store)
            raise

        _DB = trial_store
        _DB_ENABLED = True
        _DB_READY = True
        _DB_INIT_ERROR = ""
        _DB_BACKEND = resolved_backend
        _DB_POSTGRES_DSN = resolved_postgres_dsn
        _DB_PATH_TEXT = next_path_text
        _DB_PATH = resolved_path
        _sync_auth_users_to_database()
        _ensure_db_alert_outbox_worker()
        with _DB_HEALTH_LOCK:
            _DB_HEALTH_STATS_CACHE["ts_epoch"] = 0.0
            _DB_HEALTH_STATS_CACHE["stats"] = {}
            _DB_HEALTH_STATS_CACHE["error"] = ""
        if old_db is not trial_store:
            _close_db_repository(old_db)
        return {
            "ok": True,
            "previous": previous,
            "current": {
                "enabled": True,
                "ready": True,
                "backend": str(_DB_BACKEND),
                "db_path": str(_DB_PATH),
                "postgres_dsn": _mask_postgres_dsn(_DB_POSTGRES_DSN),
                "db_error": "",
                "preserve_state": bool(preserve_state),
            },
        }


def _persist_current_user_strategies() -> None:
    if not _db_is_enabled():
        return
    current_user = _current_auth_username()
    for strategy_key, strategy in _STRATEGY_STORE.items():
        if not _strategy_record_visible_to_user(strategy, current_user):
            continue
        _persist_strategy_record(strategy_key, strategy)


def _load_state_from_database() -> None:
    if not _db_is_enabled():
        return
    try:
        for row in _DB_SERVICE.load_strategies():
            strategy_key = str(row.get("strategy_key") or "").strip()
            if not strategy_key:
                continue
            payload = row.get("record")
            if not isinstance(payload, dict):
                continue
            owner = str(row.get("owner") or "").strip()
            if owner and not str(payload.get("owner") or "").strip():
                payload["owner"] = owner
            _STRATEGY_STORE[strategy_key] = payload

        for row in _DB_SERVICE.load_backtests():
            run_id = str(row.get("run_id") or "").strip()
            if not run_id:
                continue
            payload = row.get("record")
            if not isinstance(payload, dict):
                continue
            owner = str(row.get("owner") or "").strip()
            if owner and not str(payload.get("owner") or "").strip():
                payload["owner"] = owner
            _BACKTEST_STORE[run_id] = payload

        for row in _DB_SERVICE.load_risk_states():
            strategy_key = str(row.get("strategy_key") or "").strip()
            state = row.get("state")
            if not strategy_key or not isinstance(state, dict):
                continue
            _RISK_STATE_STORE[strategy_key] = state
    except Exception:
        return


def _resolve_path(path_text: str) -> Path:
    path = Path(path_text)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def _pid_exists(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False
    return True


def _read_process_cmdline(pid: int) -> List[str]:
    try:
        raw = Path(f"/proc/{int(pid)}/cmdline").read_bytes()
    except Exception:
        return []
    if not raw:
        return []
    return [part.decode("utf-8", errors="replace") for part in raw.split(b"\0") if part]


def _extract_config_path_from_cmdline(args: List[str]) -> Optional[Path]:
    config_text = ""
    for idx, token in enumerate(args):
        if token == "--config" and idx + 1 < len(args):
            config_text = args[idx + 1]
            break
        if token.startswith("--config="):
            config_text = token.split("=", 1)[1]
            break
    if not config_text:
        return None
    try:
        return _resolve_path(config_text)
    except Exception:
        return None


def _strategy_id_from_runtime_config_path(config_path: Path) -> Optional[str]:
    runtime_root = (LOG_DIR / "runtime_configs").resolve()
    try:
        resolved = config_path.resolve()
        resolved.relative_to(runtime_root)
    except Exception:
        return None
    match = _RUNTIME_CONFIG_NAME_RE.match(resolved.name)
    if not match:
        return None
    strategy_id = str(match.group("strategy_id") or "").strip()
    return strategy_id or None


def _load_external_runtime_metadata(config_path: Path, scoped_strategy_id: str) -> Dict[str, Any]:
    metadata: Dict[str, Any] = {
        "config_path": str(config_path),
        "strategy_id": _unscoped_strategy_id(scoped_strategy_id),
        "paper_log_path": _paper_log_path_for_strategy(scoped_strategy_id),
    }
    try:
        cfg = _load_config_with_db_fallback(config_path)
        raw = cfg.raw if isinstance(cfg.raw, dict) else {}
    except Exception:
        return metadata

    paper_log_path = raw.get("paper_log_path")
    if isinstance(paper_log_path, str) and paper_log_path.strip():
        try:
            metadata["paper_log_path"] = str(_resolve_path(paper_log_path.strip()))
        except Exception:
            metadata["paper_log_path"] = paper_log_path.strip()

    diagnostics_cfg = raw.get("diagnostics")
    if isinstance(diagnostics_cfg, dict):
        for field in ("snapshot_path", "exceptions_path"):
            path_text = diagnostics_cfg.get(field)
            if not isinstance(path_text, str) or not path_text.strip():
                continue
            meta_key = "diagnostics_path" if field == "snapshot_path" else "exceptions_path"
            try:
                metadata[meta_key] = str(_resolve_path(path_text.strip()))
            except Exception:
                metadata[meta_key] = path_text.strip()
    return metadata


def _scan_external_strategy_processes(force: bool = False) -> Dict[str, List[Dict[str, Any]]]:
    now_ms = int(time.time() * 1000)
    with _EXTERNAL_STRATEGY_SCAN_LOCK:
        cached_ts = int(_EXTERNAL_STRATEGY_SCAN_CACHE.get("ts_ms", 0))
        cached_rows = _EXTERNAL_STRATEGY_SCAN_CACHE.get("rows", {})
        if (
            not force
            and now_ms - cached_ts < _EXTERNAL_STRATEGY_SCAN_TTL_MS
            and isinstance(cached_rows, dict)
        ):
            return deepcopy(cached_rows)

    runtime_root = (LOG_DIR / "runtime_configs").resolve()
    discovered: Dict[str, List[Dict[str, Any]]] = {}

    proc_root = Path("/proc")
    try:
        proc_entries = list(proc_root.iterdir())
    except Exception:
        proc_entries = []

    for proc_entry in proc_entries:
        if not proc_entry.is_dir():
            continue
        name = proc_entry.name
        if not name.isdigit():
            continue
        pid = int(name)
        if pid <= 1 or pid == os.getpid():
            continue
        args = _read_process_cmdline(pid)
        if not args:
            continue
        if not any(Path(part).name == "main.py" for part in args):
            continue
        config_path = _extract_config_path_from_cmdline(args)
        if config_path is None:
            continue
        try:
            resolved_cfg = config_path.resolve()
            resolved_cfg.relative_to(runtime_root)
        except Exception:
            continue
        strategy_id = _strategy_id_from_runtime_config_path(resolved_cfg)
        if not strategy_id:
            continue
        discovered.setdefault(strategy_id, []).append(
            {
                "pid": pid,
                "command": args,
                "config_path": str(resolved_cfg),
            }
        )

    for rows in discovered.values():
        rows.sort(key=lambda item: int(item.get("pid", 0)))

    with _EXTERNAL_STRATEGY_SCAN_LOCK:
        _EXTERNAL_STRATEGY_SCAN_CACHE["ts_ms"] = now_ms
        _EXTERNAL_STRATEGY_SCAN_CACHE["rows"] = deepcopy(discovered)
    return discovered


def _visible_external_strategy_processes(username: Optional[str] = None) -> Dict[str, List[Dict[str, Any]]]:
    user = _resolve_effective_auth_username(username)
    scanned = _scan_external_strategy_processes()
    visible: Dict[str, List[Dict[str, Any]]] = {}
    for scoped_strategy_id, rows in scanned.items():
        if not rows:
            continue
        if not _runner_visible_to_user(scoped_strategy_id, user):
            continue
        strategy_id = _unscoped_strategy_id(scoped_strategy_id, user)
        visible.setdefault(strategy_id, []).extend(deepcopy(rows))

    for rows in visible.values():
        rows.sort(key=lambda item: int(item.get("pid", 0)))
    return visible


def _external_strategy_processes_for_strategy(
    strategy_id: str,
    username: Optional[str] = None,
) -> List[Dict[str, Any]]:
    scoped = _scoped_strategy_id(strategy_id, username)
    user = _resolve_effective_auth_username(username)
    if not _runner_visible_to_user(scoped, user):
        return []
    scanned = _scan_external_strategy_processes()
    rows = scanned.get(scoped, [])
    if not isinstance(rows, list):
        return []
    return deepcopy(rows)


def _terminate_pid(pid: int, timeout_seconds: float = 10.0) -> bool:
    if pid <= 1:
        return False
    if not _pid_exists(pid):
        return False

    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        return True
    except Exception:
        return False

    deadline = time.time() + max(0.5, timeout_seconds)
    while time.time() < deadline:
        if not _pid_exists(pid):
            return True
        time.sleep(0.1)

    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        return True
    except Exception:
        return False

    kill_deadline = time.time() + 5.0
    while time.time() < kill_deadline:
        if not _pid_exists(pid):
            return True
        time.sleep(0.05)
    return not _pid_exists(pid)


def _terminate_external_strategy_processes(
    strategy_id: Optional[str] = None,
    username: Optional[str] = None,
) -> Dict[str, List[int]]:
    user = _resolve_effective_auth_username(username)
    targets: Dict[str, List[Dict[str, Any]]] = {}
    if strategy_id:
        rows = _external_strategy_processes_for_strategy(strategy_id, user)
        if rows:
            targets[strategy_id] = rows
    else:
        targets = _visible_external_strategy_processes(user)

    stopped: Dict[str, List[int]] = {}
    for sid, rows in targets.items():
        stopped_pids: List[int] = []
        for row in rows:
            pid = int(row.get("pid", 0))
            if pid <= 0:
                continue
            if _terminate_pid(pid):
                stopped_pids.append(pid)
        if stopped_pids:
            stopped[sid] = sorted(set(stopped_pids))

    if stopped:
        _scan_external_strategy_processes(force=True)
    return stopped


def _resolve_under_root(path_text: str, root: Path, field_name: str) -> Path:
    path = Path(path_text)
    if not path.is_absolute():
        path = root / path
    resolved = path.resolve()
    root_resolved = root.resolve()
    try:
        resolved.relative_to(root_resolved)
    except ValueError as exc:
        raise HTTPException(
            status_code=403,
            detail=f"{field_name} must stay under {root_resolved}",
        ) from exc
    return resolved


def _resolve_config_path(path_text: str) -> Path:
    resolved = _resolve_under_root(path_text, PROJECT_ROOT, "config_path")
    if resolved.suffix.lower() not in {".yaml", ".yml"}:
        raise HTTPException(status_code=422, detail="config_path must be a .yaml/.yml file")
    return resolved


def _resolve_log_csv_path(path_text: str) -> Path:
    path = Path(path_text)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    resolved = _resolve_under_root(str(path), LOG_DIR, "path")
    if resolved.suffix.lower() != ".csv":
        raise HTTPException(status_code=422, detail="path must be a .csv file under logs/")
    return resolved


def _project_relative_path_text(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve())).replace("\\", "/")
    except Exception:
        return str(path.resolve())


def _guess_file_content_type(path: Optional[Path] = None, file_name: str = "") -> str:
    suffix = ""
    if path is not None:
        suffix = str(path.suffix or "").strip().lower()
    if not suffix:
        suffix = str(Path(file_name).suffix or "").strip().lower()
    if suffix in {".yaml", ".yml"}:
        return "application/x-yaml"
    if suffix == ".csv":
        return "text/csv"
    if suffix == ".json":
        return "application/json"
    if suffix == ".jsonl":
        return "application/x-ndjson"
    if suffix == ".txt":
        return "text/plain"
    if suffix == ".png":
        return "image/png"
    return "application/octet-stream"


def _config_blob_key(path: Path) -> str:
    return _project_relative_path_text(path)


def _config_available(path: Path) -> bool:
    if path.exists():
        return True
    row = _db_get_data_file(
        owner=_record_owner_key(),
        scope="config_yaml",
        file_key=_config_blob_key(path),
    )
    return isinstance(row, dict)


def _upsert_text_data_file(
    *,
    owner: str,
    scope: str,
    file_key: str,
    text_payload: str,
    file_name: str = "",
    source_path: str = "",
    content_type: str = "text/plain",
    meta: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    return _db_upsert_data_file(
        owner=owner,
        scope=scope,
        file_key=file_key,
        file_name=file_name,
        source_path=source_path,
        content_type=content_type,
        content_encoding="utf-8",
        content_text=text_payload,
        meta=meta or {},
    )


def _upsert_binary_data_file(
    *,
    owner: str,
    scope: str,
    file_key: str,
    binary_payload: bytes,
    file_name: str = "",
    source_path: str = "",
    content_type: str = "application/octet-stream",
    meta: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    encoded = base64.b64encode(binary_payload or b"").decode("ascii")
    payload_meta = deepcopy(meta) if isinstance(meta, dict) else {}
    payload_meta.setdefault("byte_size", len(binary_payload or b""))
    return _db_upsert_data_file(
        owner=owner,
        scope=scope,
        file_key=file_key,
        file_name=file_name,
        source_path=source_path,
        content_type=content_type,
        content_encoding="base64",
        content_text=encoded,
        meta=payload_meta,
    )


def _decode_data_file_text(row: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(row, dict):
        return None
    encoding = str(row.get("contentEncoding") or "utf-8").strip().lower()
    payload = str(row.get("contentText") or "")
    if not payload:
        return ""
    if encoding == "base64":
        try:
            raw = base64.b64decode(payload.encode("ascii"), validate=False)
        except Exception:
            return None
        return raw.decode("utf-8", errors="replace")
    return payload


def _decode_data_file_bytes(row: Optional[Dict[str, Any]]) -> Optional[bytes]:
    if not isinstance(row, dict):
        return None
    encoding = str(row.get("contentEncoding") or "utf-8").strip().lower()
    payload = str(row.get("contentText") or "")
    if encoding == "base64":
        try:
            return base64.b64decode(payload.encode("ascii"), validate=False)
        except Exception:
            return None
    return payload.encode("utf-8")


def _file_size_bytes(path: Path) -> Optional[int]:
    try:
        return int(path.stat().st_size)
    except Exception:
        return None


_LIBC_MALLOC_TRIM: Optional[Any] = None
_LIBC_MALLOC_TRIM_LOADED = False


def _trim_process_memory() -> None:
    global _LIBC_MALLOC_TRIM, _LIBC_MALLOC_TRIM_LOADED
    gc.collect()
    if not sys.platform.startswith("linux"):
        return
    if not _LIBC_MALLOC_TRIM_LOADED:
        _LIBC_MALLOC_TRIM_LOADED = True
        try:
            _LIBC_MALLOC_TRIM = ctypes.CDLL("libc.so.6").malloc_trim
        except Exception:
            _LIBC_MALLOC_TRIM = None
    if _LIBC_MALLOC_TRIM is None:
        return
    try:
        _LIBC_MALLOC_TRIM(0)
    except Exception:
        pass


def _sync_text_file_to_db(
    *,
    owner: str,
    scope: str,
    file_key: str,
    path: Path,
    content_type: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    if not path.exists():
        return None
    file_size = _file_size_bytes(path)
    if file_size is not None and file_size > _DATA_FILE_DB_SYNC_MAX_BYTES:
        return None
    try:
        text_payload = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return None
    _upsert_text_data_file(
        owner=owner,
        scope=scope,
        file_key=file_key,
        text_payload=text_payload,
        file_name=path.name,
        source_path=str(path),
        content_type=content_type or _guess_file_content_type(path=path),
        meta=meta or {},
    )
    return text_payload


def _sync_binary_file_to_db(
    *,
    owner: str,
    scope: str,
    file_key: str,
    path: Path,
    content_type: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> Optional[bytes]:
    if not path.exists():
        return None
    try:
        payload = path.read_bytes()
    except Exception:
        return None
    _upsert_binary_data_file(
        owner=owner,
        scope=scope,
        file_key=file_key,
        binary_payload=payload,
        file_name=path.name,
        source_path=str(path),
        content_type=content_type or _guess_file_content_type(path=path),
        meta=meta or {},
    )
    return payload


def _load_config_with_db_fallback(path: Path) -> Cfg:
    path_key = _config_blob_key(path)
    owner_key = _record_owner_key()
    if path.exists():
        cfg = load_config(str(path))
        text_payload = _sync_text_file_to_db(
            owner=owner_key,
            scope="config_yaml",
            file_key=path_key,
            path=path,
            content_type="application/x-yaml",
            meta={"kind": "config_yaml", "path": str(path)},
        )
        if text_payload is None:
            try:
                text_payload = yaml.safe_dump(cfg.raw if isinstance(cfg.raw, dict) else {}, allow_unicode=False, sort_keys=False)
            except Exception:
                text_payload = ""
            if text_payload:
                _upsert_text_data_file(
                    owner=owner_key,
                    scope="config_yaml",
                    file_key=path_key,
                    text_payload=text_payload,
                    file_name=path.name,
                    source_path=str(path),
                    content_type="application/x-yaml",
                    meta={"kind": "config_yaml", "path": str(path), "generated": True},
                )
        return cfg

    row = _db_get_data_file(owner=owner_key, scope="config_yaml", file_key=path_key)
    text_payload = _decode_data_file_text(row)
    if text_payload is None:
        raise FileNotFoundError(f"config file not found: {path}")
    raw = yaml.safe_load(text_payload)
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise RuntimeError(f"invalid config payload in database for {path}")
    return Cfg(raw=raw)


def _load_csv_with_db_fallback(
    *,
    path: Path,
    owner: str,
    scope: str,
    file_key: str,
) -> Optional[pd.DataFrame]:
    if path.exists():
        try:
            df = pd.read_csv(path)
        except Exception:
            df = None
        text_payload = _sync_text_file_to_db(
            owner=owner,
            scope=scope,
            file_key=file_key,
            path=path,
            content_type="text/csv",
            meta={"kind": "csv", "path": str(path)},
        )
        if df is not None:
            return df
        if isinstance(text_payload, str):
            try:
                return pd.read_csv(io.StringIO(text_payload))
            except Exception:
                return None

    row = _db_get_data_file(owner=owner, scope=scope, file_key=file_key)
    text_payload = _decode_data_file_text(row)
    if text_payload is None:
        return None
    try:
        return pd.read_csv(io.StringIO(text_payload))
    except Exception:
        return None


def _create_backtest_override_config(
    base_config_path: Path,
    *,
    initial_capital: Optional[float] = None,
    fee_rate: Optional[float] = None,
    slippage: Optional[float] = None,
) -> Path:
    cfg = _load_config_with_db_fallback(base_config_path)
    raw = deepcopy(cfg.raw if isinstance(cfg.raw, dict) else {})

    if initial_capital is not None:
        raw["paper_equity_usdt"] = float(initial_capital)

    portfolio = raw.get("portfolio")
    if not isinstance(portfolio, dict):
        portfolio = {}
    if fee_rate is not None:
        portfolio["fee_bps"] = float(fee_rate) * 10000.0
    if slippage is not None:
        portfolio["slippage_bps"] = float(slippage) * 10000.0
    raw["portfolio"] = portfolio

    out_dir = LOG_DIR / "backtest_configs"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"api_backtest_cfg_{int(time.time() * 1000)}.yaml"
    yaml_text = yaml.safe_dump(raw, allow_unicode=False, sort_keys=False)
    out_path.write_text(yaml_text, encoding="utf-8")
    _upsert_text_data_file(
        owner=_record_owner_key(),
        scope="backtest_config_yaml",
        file_key=_project_relative_path_text(out_path),
        text_payload=yaml_text,
        file_name=out_path.name,
        source_path=str(out_path),
        content_type="application/x-yaml",
        meta={
            "kind": "backtest_override_config",
            "base_config_path": str(base_config_path),
        },
    )
    return out_path


def _paper_log_path_for_strategy(strategy_id: str) -> str:
    safe_id = _safe_strategy_id(strategy_id)
    return str(LOG_DIR / "strategies" / safe_id / "paper_equity.csv")


def _safe_strategy_id(strategy_id: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in strategy_id)


def _create_strategy_runtime_config(base_config_path: Path, strategy_id: str) -> Path:
    cfg = _load_config_with_db_fallback(base_config_path)
    raw = deepcopy(cfg.raw if isinstance(cfg.raw, dict) else {})
    raw["paper_log_path"] = _paper_log_path_for_strategy(strategy_id)
    safe_id = _safe_strategy_id(strategy_id)
    diag_cfg = raw.get("diagnostics")
    if not isinstance(diag_cfg, dict):
        diag_cfg = {}
    diag_cfg.setdefault("snapshot_path", str(LOG_DIR / "diagnostics" / f"{safe_id}.json"))
    diag_cfg.setdefault("exceptions_path", str(LOG_DIR / "diagnostics" / f"{safe_id}_exceptions.jsonl"))
    diag_cfg.setdefault("heartbeat_minutes", 1)
    raw["diagnostics"] = diag_cfg

    out_dir = LOG_DIR / "runtime_configs"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts_ms = int(time.time() * 1000)
    out_path = out_dir / f"strategy_{strategy_id}_{ts_ms}.yaml"
    yaml_text = yaml.safe_dump(raw, allow_unicode=False, sort_keys=False)
    out_path.write_text(yaml_text, encoding="utf-8")
    _upsert_text_data_file(
        owner=_record_owner_key(),
        scope="strategy_runtime_config_yaml",
        file_key=_project_relative_path_text(out_path),
        text_payload=yaml_text,
        file_name=out_path.name,
        source_path=str(out_path),
        content_type="application/x-yaml",
        meta={
            "kind": "strategy_runtime_config",
            "strategy_id": str(strategy_id or ""),
            "base_config_path": str(base_config_path),
        },
    )
    return out_path


def _set_nested_config_value(root: Dict[str, Any], dotted_key: str, value: Any) -> None:
    key = str(dotted_key or "").strip()
    if not key:
        return
    parts = [part.strip() for part in key.split(".") if part.strip()]
    if not parts:
        return
    node = root
    for part in parts[:-1]:
        child = node.get(part)
        if not isinstance(child, dict):
            child = {}
            node[part] = child
        node = child
    node[parts[-1]] = value


def _build_compiled_strategy_source_config(base_raw: Dict[str, Any], strategy: Dict[str, Any]) -> Dict[str, Any]:
    raw = deepcopy(base_raw if isinstance(base_raw, dict) else {})
    config = strategy.get("config")
    if isinstance(config, dict):
        symbols = config.get("symbols")
        if isinstance(symbols, list):
            normalized_symbols = [str(item or "").strip() for item in symbols if str(item or "").strip()]
            if normalized_symbols:
                raw["symbols"] = normalized_symbols
        timeframe = str(config.get("timeframe") or "").strip()
        if timeframe:
            raw["timeframe"] = _normalize_timeframe(timeframe)
        params = config.get("params")
        if isinstance(params, dict):
            for key, value in params.items():
                key_text = str(key or "").strip()
                if not key_text:
                    continue
                _set_nested_config_value(raw, key_text, deepcopy(value))

    strategy_mode = str(strategy.get("type") or "").strip()
    strategy_node = raw.get("strategy")
    if not isinstance(strategy_node, dict):
        strategy_node = {}
    if strategy_mode:
        strategy_node["mode"] = strategy_mode
    raw["strategy"] = strategy_node

    meta_node = raw.get("api_strategy")
    if not isinstance(meta_node, dict):
        meta_node = {}
    meta_node["id"] = str(strategy.get("id") or "")
    meta_node["name"] = str(strategy.get("name") or "")
    meta_node["owner"] = str(strategy.get("owner") or "")
    meta_node["compiled_at"] = _now_iso()
    raw["api_strategy"] = meta_node
    return raw


def _compile_strategy_script(strategy_key: str, owner: Optional[str] = None) -> Dict[str, Any]:
    scoped_strategy_key = str(strategy_key or "").strip()
    if not scoped_strategy_key:
        raise RuntimeError("strategy key is required for compile")

    strategy = _STRATEGY_STORE.get(scoped_strategy_key)
    if not isinstance(strategy, dict):
        raise RuntimeError(f"strategy not found for compile: {scoped_strategy_key}")

    owner_key = str(owner or strategy.get("owner") or _strategy_owner_user_key(scoped_strategy_key) or "").strip()
    owner_key = _safe_user_key(owner_key or _record_owner_key())
    if str(strategy.get("owner") or "").strip() and str(strategy.get("owner") or "").strip() != owner_key:
        raise RuntimeError("owner mismatch for compile")

    source_path_text = (
        str(strategy.get("_source_config_path") or "").strip()
        or str(strategy.get("_config_path") or "").strip()
        or _DEFAULT_CONFIG_PATH
    )
    source_path = _resolve_config_path(source_path_text)
    if not _config_available(source_path):
        source_path = _resolve_config_path(_DEFAULT_CONFIG_PATH)

    cfg = _load_config_with_db_fallback(source_path)
    source_raw = cfg.raw if isinstance(cfg.raw, dict) else {}
    compiled_raw = _build_compiled_strategy_source_config(source_raw, strategy)

    latest = _db_get_latest_strategy_script(owner=owner_key, strategy_key=scoped_strategy_key)
    next_version = int((latest or {}).get("version") or 0) + 1
    out_dir = LOG_DIR / "strategy_scripts" / _safe_strategy_id(owner_key) / _safe_strategy_id(scoped_strategy_key)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{_safe_strategy_id(scoped_strategy_key)}_v{next_version}.yaml"
    compiled_yaml = yaml.safe_dump(compiled_raw, allow_unicode=False, sort_keys=False)
    out_path.write_text(compiled_yaml, encoding="utf-8")
    script_hash = hashlib.sha256(compiled_yaml.encode("utf-8")).hexdigest()
    _upsert_text_data_file(
        owner=owner_key,
        scope="strategy_script_yaml",
        file_key=f"{scoped_strategy_key}:v{next_version}",
        text_payload=compiled_yaml,
        file_name=out_path.name,
        source_path=str(out_path),
        content_type="application/x-yaml",
        meta={
            "kind": "strategy_compiled_script",
            "strategy_key": scoped_strategy_key,
            "version": int(next_version),
            "script_hash": script_hash,
        },
    )

    script_row = _db_add_strategy_script(
        strategy_key=scoped_strategy_key,
        owner=owner_key,
        script_type="yaml_config",
        script_path=str(out_path),
        script_hash=script_hash,
        source_config=compiled_raw,
    )
    if _db_is_enabled() and script_row is None:
        raise RuntimeError("failed to persist strategy script metadata")
    if script_row is None:
        script_row = {
            "id": 0,
            "strategyKey": scoped_strategy_key,
            "owner": owner_key,
            "version": next_version,
            "scriptType": "yaml_config",
            "scriptPath": str(out_path),
            "scriptHash": script_hash,
            "sourceConfig": compiled_raw,
            "createdAt": _now_iso(),
        }

    strategy["owner"] = owner_key
    strategy["_source_config_path"] = str(source_path)
    strategy["_config_path"] = str(out_path)
    strategy["_compiled_script_path"] = str(out_path)
    strategy["_compiled_script_version"] = int(script_row.get("version") or next_version)
    strategy["updatedAt"] = _now_iso()
    _STRATEGY_STORE[scoped_strategy_key] = strategy
    _persist_strategy_record(scoped_strategy_key, strategy)
    return script_row


def _run_strategy_compile_job(strategy_key: str, owner: str, job_id: int = 0) -> Dict[str, Any]:
    started_at = _now_iso()
    if job_id > 0:
        _db_update_strategy_compile_job(job_id, status="running", started_at=started_at, error_message="")
    try:
        script = _compile_strategy_script(strategy_key, owner)
    except Exception as exc:
        if job_id > 0:
            _db_update_strategy_compile_job(
                job_id,
                status="failed",
                error_message=str(exc),
                finished_at=_now_iso(),
            )
        _audit_event(
            "strategy.compile.failed",
            entity="strategy",
            entity_id=_unscoped_strategy_id(strategy_key),
            detail={"error": str(exc), "job_id": job_id},
            owner=owner,
        )
        return {"ok": False, "error": str(exc), "job_id": int(job_id)}
    if job_id > 0:
        _db_update_strategy_compile_job(job_id, status="success", finished_at=_now_iso(), error_message="")
    _audit_event(
        "strategy.compile.success",
        entity="strategy",
        entity_id=_unscoped_strategy_id(strategy_key),
        detail={"job_id": job_id, "script_path": script.get("scriptPath"), "version": script.get("version")},
        owner=owner,
    )
    return {
        "ok": True,
        "job_id": int(job_id),
        "script": script,
    }


def _strategy_compile_worker_loop() -> None:
    while not _STRATEGY_COMPILE_STOP.is_set():
        payload: Optional[Dict[str, Any]] = None
        with _STRATEGY_COMPILE_LOCK:
            if _STRATEGY_COMPILE_QUEUE:
                payload = _STRATEGY_COMPILE_QUEUE.popleft()
        if payload is None:
            _STRATEGY_COMPILE_EVENT.wait(timeout=_STRATEGY_COMPILE_WAIT_SECONDS)
            _STRATEGY_COMPILE_EVENT.clear()
            continue
        strategy_key = str(payload.get("strategy_key") or "").strip()
        owner = str(payload.get("owner") or "").strip()
        job_id = int(payload.get("job_id") or 0)
        if not strategy_key or not owner:
            if job_id > 0:
                _db_update_strategy_compile_job(
                    job_id,
                    status="failed",
                    error_message="invalid compile payload",
                    finished_at=_now_iso(),
                )
            continue
        _run_strategy_compile_job(strategy_key, owner, job_id=job_id)


def _ensure_strategy_compile_worker() -> None:
    global _STRATEGY_COMPILE_WORKER
    with _STRATEGY_COMPILE_LOCK:
        worker = _STRATEGY_COMPILE_WORKER
        if worker is not None and worker.is_alive():
            return
        _STRATEGY_COMPILE_STOP.clear()
        worker = threading.Thread(
            target=_strategy_compile_worker_loop,
            name="strategy-compile-worker",
            daemon=True,
        )
        worker.start()
        _STRATEGY_COMPILE_WORKER = worker


def _recover_pending_strategy_compile_jobs(limit: int = 500) -> int:
    """
    Recover compiler jobs that were pending/running before process restart.
    """
    rows = _db_list_strategy_compile_jobs(limit=limit)
    if not rows:
        return 0

    candidates: List[Dict[str, Any]] = []
    for row in reversed(rows):
        try:
            job_id = int(row.get("id") or 0)
        except Exception:
            job_id = 0
        strategy_key = str(row.get("strategyKey") or "").strip()
        owner_raw = str(row.get("owner") or "").strip()
        status_text = str(row.get("status") or "").strip().lower()
        if job_id <= 0 or not strategy_key or not owner_raw:
            continue
        if status_text not in {"pending", "running"}:
            continue
        owner_key = _safe_user_key(owner_raw)
        if status_text == "running":
            # Previous process exited before finishing this job; put it back to pending.
            _db_update_strategy_compile_job(
                job_id,
                status="pending",
                error_message=str(row.get("errorMessage") or ""),
                started_at="",
                finished_at="",
            )
        candidates.append(
            {
                "job_id": job_id,
                "strategy_key": strategy_key,
                "owner": owner_key,
            }
        )

    if not candidates:
        return 0

    queued = 0
    with _STRATEGY_COMPILE_LOCK:
        queued_job_ids: set[int] = set()
        for item in _STRATEGY_COMPILE_QUEUE:
            try:
                queued_id = int(item.get("job_id") or 0)
            except Exception:
                queued_id = 0
            if queued_id > 0:
                queued_job_ids.add(queued_id)
        for item in candidates:
            if int(item["job_id"]) in queued_job_ids:
                continue
            _STRATEGY_COMPILE_QUEUE.append(
                {
                    "strategy_key": str(item["strategy_key"]),
                    "owner": str(item["owner"]),
                    "job_id": int(item["job_id"]),
                }
            )
            queued_job_ids.add(int(item["job_id"]))
            queued += 1

    if queued > 0:
        _ensure_strategy_compile_worker()
        _STRATEGY_COMPILE_EVENT.set()
    return queued


def _enqueue_strategy_compile(strategy_key: str, owner: str) -> Dict[str, Any]:
    scoped_strategy_key = str(strategy_key or "").strip()
    owner_key = _safe_user_key(owner)
    if not scoped_strategy_key or not owner_key:
        raise RuntimeError("invalid strategy compile enqueue arguments")
    job_row = _db_enqueue_strategy_compile_job(scoped_strategy_key, owner_key)
    job_id = int((job_row or {}).get("id") or 0)
    with _STRATEGY_COMPILE_LOCK:
        _STRATEGY_COMPILE_QUEUE.append(
            {
                "strategy_key": scoped_strategy_key,
                "owner": owner_key,
                "job_id": job_id,
            }
        )
    _ensure_strategy_compile_worker()
    _STRATEGY_COMPILE_EVENT.set()
    if isinstance(job_row, dict):
        return job_row
    return {
        "id": job_id,
        "strategyKey": scoped_strategy_key,
        "owner": owner_key,
        "status": "pending",
    }


def _compile_strategy_now(strategy_key: str, owner: str) -> Dict[str, Any]:
    result = _run_strategy_compile_job(strategy_key, _safe_user_key(owner), job_id=0)
    if not bool(result.get("ok")):
        raise RuntimeError(str(result.get("error") or "compile failed"))
    script = result.get("script")
    if not isinstance(script, dict):
        raise RuntimeError("compile finished but script metadata missing")
    return script


def _latest_compiled_script_path(strategy_key: str, owner: str) -> Optional[str]:
    script = _db_get_latest_strategy_script(owner=_safe_user_key(owner), strategy_key=str(strategy_key))
    if isinstance(script, dict):
        script_path = str(script.get("scriptPath") or "").strip()
        if script_path:
            path = _resolve_path(script_path)
            if path.exists():
                return str(path)
    strategy = _STRATEGY_STORE.get(str(strategy_key))
    if isinstance(strategy, dict):
        script_path = str(strategy.get("_compiled_script_path") or "").strip()
        if script_path:
            path = _resolve_path(script_path)
            if path.exists():
                return str(path)
    return None


def _extract_bearer_token(authorization: Optional[str]) -> Optional[str]:
    if not isinstance(authorization, str):
        return None
    if not authorization.lower().startswith("bearer "):
        return None
    token = authorization[7:].strip()
    return token or None


def _hash_api_token_value(raw_token: str) -> str:
    token_text = str(raw_token or "").strip()
    if not token_text:
        return ""
    secret = str(_SESSION_SECRET or _AUTH_TOKEN or "quant_api_token").strip()
    return hmac.new(secret.encode("utf-8"), token_text.encode("utf-8"), hashlib.sha256).hexdigest()


def _generate_api_token_value() -> str:
    return f"qat_{secrets.token_urlsafe(32)}"


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def _b64url_decode(data: str) -> bytes:
    pad = "=" * ((4 - len(data) % 4) % 4)
    return base64.urlsafe_b64decode((data + pad).encode("ascii"))


def _create_session_token(username: str, session_id: Optional[str] = None) -> str:
    payload = {
        "u": username,
        "exp": int(time.time()) + _SESSION_TTL_SECONDS,
    }
    if session_id:
        payload["sid"] = str(session_id)
    payload_raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    payload_b64 = _b64url_encode(payload_raw)
    sig = hmac.new(_SESSION_SECRET.encode("utf-8"), payload_b64.encode("utf-8"), hashlib.sha256).digest()
    sig_b64 = _b64url_encode(sig)
    return f"{payload_b64}.{sig_b64}"


def _validate_session_token_payload(token: Optional[str]) -> Optional[Dict[str, Any]]:
    if not token or not _SESSION_SECRET:
        return None
    if "." not in token:
        return None
    payload_b64, sig_b64 = token.split(".", 1)
    if not payload_b64 or not sig_b64:
        return None

    expected_sig = hmac.new(
        _SESSION_SECRET.encode("utf-8"),
        payload_b64.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    try:
        provided_sig = _b64url_decode(sig_b64)
    except Exception:
        return None
    if not hmac.compare_digest(expected_sig, provided_sig):
        return None

    try:
        payload = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None

    username = str(payload.get("u", "")).strip()
    exp = int(payload.get("exp", 0))
    session_id = str(payload.get("sid", "")).strip()
    now = int(time.time())
    if not username or exp <= now:
        return None
    return {
        "username": username,
        "exp": int(exp),
        "session_id": session_id,
    }


def _validate_session_token(token: Optional[str]) -> Optional[str]:
    payload = _validate_session_token_payload(token)
    if not payload:
        return None
    username = str(payload.get("username") or "").strip()
    if not username:
        return None
    session_id = str(payload.get("session_id") or "").strip()
    if not session_id:
        return username
    row = _db_get_auth_session(session_id)
    if not isinstance(row, dict):
        return None
    if str(row.get("username") or "").strip().lower() != username.lower():
        return None
    if str(row.get("userStatus") or "active").strip().lower() != "active":
        return None
    revoked_at = str(row.get("revokedAt") or "").strip()
    if revoked_at:
        return None
    expires_at = str(row.get("expiresAt") or "").strip()
    if expires_at and _iso_datetime_to_epoch(expires_at) <= time.time():
        return None
    return username


def _login_rate_keys(username: str, client_ip: str) -> List[str]:
    normalized_user = username.strip().lower()
    normalized_ip = client_ip.strip() or "unknown"
    return [
        f"ip:{normalized_ip}",
        f"ip_user:{normalized_ip}|{normalized_user}",
    ]


def _extract_client_ip(request: Request) -> str:
    x_forwarded_for = request.headers.get("x-forwarded-for", "")
    if x_forwarded_for:
        candidate = x_forwarded_for.split(",", 1)[0].strip()
        if candidate:
            return candidate

    x_real_ip = request.headers.get("x-real-ip", "").strip()
    if x_real_ip:
        return x_real_ip

    client = getattr(request, "client", None)
    host = getattr(client, "host", "")
    return str(host).strip() or "unknown"


def _cleanup_login_attempts_locked(now_ts: float, keys: List[str]) -> None:
    cutoff_ts = now_ts - _LOGIN_RATE_LIMIT_WINDOW_SECONDS
    for key in keys:
        attempts = _LOGIN_ATTEMPTS.get(key)
        if not attempts:
            continue
        while attempts and attempts[0] <= cutoff_ts:
            attempts.popleft()
        if not attempts:
            _LOGIN_ATTEMPTS.pop(key, None)


def _login_rate_limit_check(username: str, client_ip: str) -> int:
    if _LOGIN_RATE_LIMIT_MAX_ATTEMPTS <= 0:
        return 0
    now_ts = time.time()
    keys = _login_rate_keys(username, client_ip)
    persisted_retry_after = _db_get_active_lockout_seconds(keys, now_ts)
    if persisted_retry_after > 0:
        return persisted_retry_after
    with _LOGIN_RATE_LOCK:
        _cleanup_login_attempts_locked(now_ts, keys)
        for key in keys:
            locked_until = _LOGIN_LOCKED_UNTIL.get(key, 0.0)
            if locked_until > now_ts:
                return max(1, int(locked_until - now_ts))
            attempts = _LOGIN_ATTEMPTS.get(key)
            if attempts and len(attempts) >= _LOGIN_RATE_LIMIT_MAX_ATTEMPTS:
                locked_until = now_ts + _LOGIN_LOCKOUT_SECONDS
                _LOGIN_LOCKED_UNTIL[key] = locked_until
                _LOGIN_ATTEMPTS.pop(key, None)
                _db_set_lockout(key, locked_until)
                return _LOGIN_LOCKOUT_SECONDS
    return 0


def _login_rate_limit_record_failure(username: str, client_ip: str) -> None:
    if _LOGIN_RATE_LIMIT_MAX_ATTEMPTS <= 0:
        return
    now_ts = time.time()
    keys = _login_rate_keys(username, client_ip)
    with _LOGIN_RATE_LOCK:
        _cleanup_login_attempts_locked(now_ts, keys)
        for key in keys:
            attempts = _LOGIN_ATTEMPTS.setdefault(key, deque())
            attempts.append(now_ts)
            if len(attempts) >= _LOGIN_RATE_LIMIT_MAX_ATTEMPTS:
                locked_until = now_ts + _LOGIN_LOCKOUT_SECONDS
                _LOGIN_LOCKED_UNTIL[key] = locked_until
                _LOGIN_ATTEMPTS.pop(key, None)
                _db_set_lockout(key, locked_until)
    _db_record_login_attempt(
        username=username,
        client_ip=client_ip,
        success=False,
        reason="invalid_credential",
    )


def _login_rate_limit_reset(username: str, client_ip: str) -> None:
    keys = _login_rate_keys(username, client_ip)
    with _LOGIN_RATE_LOCK:
        for key in keys:
            _LOGIN_ATTEMPTS.pop(key, None)
            _LOGIN_LOCKED_UNTIL.pop(key, None)
    _db_clear_lockouts(keys)
    _db_record_login_attempt(
        username=username,
        client_ip=client_ip,
        success=True,
        reason="login_success",
    )


def _resolve_auth_username(
    authorization: Optional[str],
    x_api_key: Optional[str],
    session_token: Optional[str] = None,
) -> Optional[str]:
    provided = (x_api_key or "").strip() or (_extract_bearer_token(authorization) or "")
    if _AUTH_TOKEN and provided and hmac.compare_digest(str(provided), str(_AUTH_TOKEN)):
        return _DASHBOARD_PRIMARY_USERNAME or _AUTH_FALLBACK_USER
    if provided:
        token_row = _db_get_active_api_token(provided)
        if isinstance(token_row, dict):
            token_id = int(token_row.get("id") or 0)
            if token_id > 0:
                _db_touch_api_token_last_used(token_id)
            token_owner = _safe_user_key(str(token_row.get("owner") or ""))
            if token_owner:
                return token_owner
    session_username = _validate_session_token(session_token)
    if session_username:
        return session_username
    return None


def _auth_error(
    authorization: Optional[str],
    x_api_key: Optional[str],
    session_token: Optional[str] = None,
) -> Optional[HTTPException]:
    if not _AUTH_REQUIRED:
        return None

    provided = (x_api_key or "").strip() or (_extract_bearer_token(authorization) or "")
    if _resolve_auth_username(
        authorization=authorization,
        x_api_key=x_api_key,
        session_token=session_token,
    ):
        return None

    # If all credential sources are absent, auth cannot work.
    if not _AUTH_TOKEN and not _DASHBOARD_CREDENTIALS and not _db_has_active_api_tokens():
        return HTTPException(
            status_code=503,
            detail="API auth misconfigured: configure token, dashboard credentials, or api tokens",
        )

    # Provide an explicit hint when token mode is enabled but token is missing.
    if _AUTH_TOKEN and not provided and not session_token:
        return HTTPException(status_code=401, detail="Unauthorized: missing credential")

    return HTTPException(status_code=401, detail="Unauthorized")


def _require_dashboard_credentials() -> Optional[HTTPException]:
    if _DASHBOARD_CREDENTIALS:
        return None
    return HTTPException(
        status_code=503,
        detail="Dashboard login is not configured",
    )


def _session_expire_iso() -> str:
    expire_epoch = int(time.time()) + _SESSION_TTL_SECONDS
    return datetime.fromtimestamp(expire_epoch, tz=timezone.utc).isoformat()


def _set_session_cookie(
    response: Response,
    username: str,
    *,
    client_ip: str = "",
    user_agent: str = "",
) -> Optional[str]:
    if not _SESSION_SECRET:
        raise HTTPException(status_code=503, detail="API auth misconfigured: API_SESSION_SECRET is empty")
    normalized_user = str(username or "").strip().lower()
    session_id: Optional[str] = None
    if _db_is_enabled():
        candidate_sid = uuid.uuid4().hex
        if _db_create_auth_session(
            session_id=candidate_sid,
            username=normalized_user,
            expires_at=_session_expire_iso(),
            client_ip=client_ip,
            user_agent=user_agent,
        ):
            session_id = candidate_sid
    token = _create_session_token(normalized_user, session_id=session_id)
    response.set_cookie(
        key=_SESSION_COOKIE_NAME,
        value=token,
        max_age=_SESSION_TTL_SECONDS,
        httponly=True,
        samesite=_SESSION_COOKIE_SAMESITE,
        secure=_SESSION_COOKIE_SECURE,
        path="/",
    )
    return session_id


def _clear_session_cookie(response: Response) -> None:
    response.delete_cookie(
        key=_SESSION_COOKIE_NAME,
        path="/",
    )
    return None


def _parse_yyyy_mm_dd(value: str, field_name: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"{field_name} must use YYYY-MM-DD format",
        ) from exc


def _to_iso_date_only(value: str) -> Optional[str]:
    if not value:
        return None
    parsed = value[:10]
    try:
        datetime.strptime(parsed, "%Y-%m-%d")
        return parsed
    except Exception:
        return None


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _infer_log_level(text: str) -> str:
    lower = text.lower()
    if "error" in lower or "exception" in lower or "traceback" in lower or "fail" in lower:
        return "error"
    if "warn" in lower:
        return "warn"
    return "info"


def _strategy_store_key(strategy_id: str, username: Optional[str] = None) -> str:
    return _scoped_strategy_id(str(strategy_id or "").strip(), username)


def _strategy_record_visible_to_user(record: Dict[str, Any], username: Optional[str] = None) -> bool:
    owner_key = str(record.get("owner") or "").strip()
    current_user_key = _safe_user_key(_resolve_effective_auth_username(username))
    if owner_key:
        return owner_key == current_user_key
    # Backward compatibility: old records without owner are treated as admin-owned.
    return _is_admin_username(username)


def _strategy_store_get(strategy_id: str, username: Optional[str] = None) -> Optional[Dict[str, Any]]:
    key = _strategy_store_key(strategy_id, username)
    strategy = _STRATEGY_STORE.get(key)
    if isinstance(strategy, dict):
        return strategy

    # Legacy fallback for old in-memory records keyed by unscoped id.
    legacy = _STRATEGY_STORE.get(str(strategy_id or "").strip())
    if isinstance(legacy, dict) and _strategy_record_visible_to_user(legacy, username):
        return legacy
    return None


def _strategy_id_from_config_path(config_path: str) -> Optional[str]:
    try:
        resolved = str(_resolve_path(config_path))
    except Exception:
        return None

    current_user = _current_auth_username()
    for strategy_key, strategy in _STRATEGY_STORE.items():
        if not _strategy_record_visible_to_user(strategy, current_user):
            continue
        configured = strategy.get("_config_path")
        if not isinstance(configured, str) or not configured:
            continue
        try:
            if str(_resolve_path(configured)) == resolved:
                return _unscoped_strategy_id(strategy_key, current_user)
        except Exception:
            continue
    return None


def _current_running_strategy_id() -> Optional[str]:
    candidates: set[str] = set()
    current_user = _current_auth_username()
    with _STRATEGY_RUNNERS_LOCK:
        items = list(_STRATEGY_RUNNERS.items())
    for scoped_strategy_id, runner in items:
        if not _runner_visible_to_user(scoped_strategy_id, current_user):
            continue
        status = runner.status()
        if bool(status.get("running", False)):
            candidates.add(_unscoped_strategy_id(scoped_strategy_id, current_user))
    for strategy_id, rows in _visible_external_strategy_processes(current_user).items():
        if rows:
            candidates.add(strategy_id)
    if not candidates:
        return None
    return sorted(candidates)[0]


def _sync_strategy_store_statuses() -> Optional[str]:
    running_id = _current_running_strategy_id()
    running_ids: set[str] = set()
    current_user = _current_auth_username()
    with _STRATEGY_RUNNERS_LOCK:
        items = list(_STRATEGY_RUNNERS.items())
    for scoped_strategy_id, runner in items:
        if not _runner_visible_to_user(scoped_strategy_id, current_user):
            continue
        if bool(runner.status().get("running", False)):
            running_ids.add(_unscoped_strategy_id(scoped_strategy_id, current_user))
    for strategy_id, rows in _visible_external_strategy_processes(current_user).items():
        if rows:
            running_ids.add(strategy_id)
    now = _now_iso()
    dirty_keys: List[str] = []
    for strategy_key, strategy in _STRATEGY_STORE.items():
        if not _strategy_record_visible_to_user(strategy, current_user):
            continue
        strategy_id = str(strategy.get("id") or _unscoped_strategy_id(strategy_key, current_user))
        next_status = "running" if strategy_id in running_ids else "stopped"
        if str(strategy.get("status") or "") != next_status:
            strategy["status"] = next_status
            strategy["updatedAt"] = now
            dirty_keys.append(strategy_key)
    for strategy_key in dirty_keys:
        strategy = _STRATEGY_STORE.get(strategy_key)
        if isinstance(strategy, dict):
            _persist_strategy_record(strategy_key, strategy)
    return running_id


def _resolve_market_ticks_payload(config_path: str, refresh_ms: int = 1000) -> Dict[str, Any]:
    path = _resolve_config_path(config_path)
    if not _config_available(path):
        raise HTTPException(status_code=404, detail=f"config file not found: {path}")

    now_ms = time.time() * 1000.0
    path_key = str(path)
    effective_refresh_ms = max(int(refresh_ms), _MARKET_TICK_MIN_FETCH_MS)

    with _MARKET_TICKS_LOCK:
        cached_path = _MARKET_TICKS_CACHE.get("config_path")
        cached_ticks = _MARKET_TICKS_CACHE.get("ticks", [])
        cached_ts_ms = float(_MARKET_TICKS_CACHE.get("ts_ms", 0.0))
        if cached_path == path_key and cached_ticks and now_ms - cached_ts_ms < refresh_ms:
            return {"ticks": cached_ticks, "count": len(cached_ticks), "ts_utc": _now_iso()}

        # Prevent bursty fetch at short refresh intervals by only refreshing every X ms.
        if cached_path == path_key and cached_ticks and now_ms - cached_ts_ms < effective_refresh_ms:
            return {"ticks": cached_ticks, "count": len(cached_ticks), "ts_utc": _now_iso()}

        if bool(_MARKET_TICK_REFRESHING.get(path_key, False)):
            return {"ticks": cached_ticks if cached_ticks else [], "count": len(cached_ticks), "ts_utc": _now_iso()}

        _MARKET_TICK_REFRESHING[path_key] = True

    try:
        cfg = _load_config_with_db_fallback(path)
        symbols = cfg.symbols
        if not symbols:
            return {"ticks": [], "count": 0, "ts_utc": _now_iso()}

        ex = _get_market_exchange(path)

        ticker_map: Dict[str, Any] = {}
        has_fetch_tickers = False
        ex_has = getattr(ex, "has", {})
        if isinstance(ex_has, dict):
            has_fetch_tickers = bool(ex_has.get("fetchTickers"))

        if has_fetch_tickers:
            try:
                ticker_map = ex.fetch_tickers(symbols)
            except Exception:
                ticker_map = {}

        ticks: List[Dict[str, Any]] = []
        for symbol in symbols:
            data = ticker_map.get(symbol) if ticker_map else None
            try:
                if data is None:
                    data = ex.fetch_ticker(symbol)
            except Exception as exc:
                if cached_path == path_key and cached_ticks:
                    # Keep last known tick when this symbol is temporarily unreachable.
                    fallback = next((item for item in cached_ticks if str(item.get("symbol")) == symbol), None)
                    if isinstance(fallback, dict):
                        fallback = {**fallback, "error": str(exc)}
                        ticks.append(fallback)
                        continue
                ticks.append(
                    {
                        "symbol": symbol,
                        "ts_utc": _now_iso(),
                        "price": 0.0,
                        "bid": 0.0,
                        "ask": 0.0,
                        "volume": 0.0,
                        "error": str(exc),
                    }
                )
                continue

            last = _to_float(data.get("last") or data.get("mark") or data.get("close"), 0.0)
            bid = _to_float(data.get("bid"), last)
            ask = _to_float(data.get("ask"), last)
            volume = _to_float(data.get("baseVolume") or data.get("quoteVolume"), 0.0)
            ts = data.get("datetime") or _now_iso()

            ticks.append(
                {
                    "symbol": symbol,
                    "ts_utc": ts,
                    "price": last,
                    "bid": bid,
                    "ask": ask,
                    "volume": volume,
                }
            )

        with _MARKET_TICKS_LOCK:
            _MARKET_TICKS_CACHE["config_path"] = str(path)
            _MARKET_TICKS_CACHE["ticks"] = ticks
            _MARKET_TICKS_CACHE["ts_ms"] = time.time() * 1000.0

        _persist_market_ticks(str(path), ticks)
        return {"ticks": ticks, "count": len(ticks), "ts_utc": _now_iso()}

    except Exception:
        if cached_path == path_key and cached_ticks:
            return {"ticks": cached_ticks, "count": len(cached_ticks), "ts_utc": _now_iso(), "stale": True}
        return {"ticks": [], "count": 0, "ts_utc": _now_iso()}
    finally:
        with _MARKET_TICKS_LOCK:
            _MARKET_TICK_REFRESHING[path_key] = False


def _timeframe_to_minutes(timeframe: str) -> int:
    tf = str(timeframe).strip().lower()
    if not tf:
        return 0
    unit = tf[-1]
    try:
        value = int(tf[:-1])
    except Exception:
        return 0
    if value <= 0:
        return 0
    if unit == "m":
        return value
    if unit == "h":
        return value * 60
    if unit == "d":
        return value * 24 * 60
    return 0


def _symbol_aliases(canonical: str) -> List[str]:
    """
    Build a small set of aliases for a canonical CCXT symbol.

    Examples (canonical: "BTC/USDT:USDT"):
      - "BTC/USDT:USDT" (canonical)
      - "BTC/USDT" (spot-style)
      - "BTCUSDT", "BTC-USDT", "BTC_USDT" (common UI formats)
    """
    c = str(canonical).strip()
    if not c:
        return []

    aliases = {c, c.upper(), c.lower()}

    base_quote = c.split(":", 1)[0]  # drop settle currency suffix if present
    aliases.update({base_quote, base_quote.upper(), base_quote.lower()})

    if "/" in base_quote:
        aliases.update(
            {
                base_quote.replace("/", "-"),
                base_quote.replace("/", "_"),
                base_quote.replace("/", "").upper(),
                base_quote.replace("/", "").lower(),
                base_quote.replace("/", "-").upper(),
                base_quote.replace("/", "-").lower(),
                base_quote.replace("/", "_").upper(),
                base_quote.replace("/", "_").lower(),
            }
        )
    else:
        # Still accept case variants for already-collapsed formats like "BTCUSDT".
        aliases.update({base_quote.upper(), base_quote.lower()})

    # Preserve insertion order for stability in debugging.
    return list(dict.fromkeys(a for a in aliases if a))


def _canonicalize_symbol(symbol: str, allowed_symbols: List[str]) -> str:
    s = str(symbol or "").strip()
    if not s or not allowed_symbols:
        return s
    if s in allowed_symbols:
        return s

    alias_to_canon: Dict[str, str] = {}
    for canon in allowed_symbols:
        canon_s = str(canon).strip()
        if not canon_s:
            continue
        for alias in _symbol_aliases(canon_s):
            alias_to_canon.setdefault(alias, canon_s)

    return alias_to_canon.get(s) or alias_to_canon.get(s.upper()) or alias_to_canon.get(s.lower()) or s


def _resolve_market_klines_payload(
    *,
    config_path: str,
    symbol: str,
    timeframe: str = "15m",
    lookback_hours: int = 24,
) -> Dict[str, Any]:
    path = _resolve_config_path(config_path)
    if not _config_available(path):
        raise HTTPException(status_code=404, detail=f"config file not found: {path}")

    cfg = _load_config_with_db_fallback(path)
    symbols = cfg.symbols or []
    symbol = _canonicalize_symbol(symbol, symbols)
    if symbol not in symbols:
        raise HTTPException(status_code=422, detail=f"symbol must be one of: {', '.join(symbols)}")

    tf_minutes = _timeframe_to_minutes(timeframe)
    if tf_minutes <= 0:
        raise HTTPException(status_code=422, detail="timeframe must be like 1m/5m/15m/1h/1d")

    lookback_hours = max(1, min(int(lookback_hours), 24 * 7))
    bars = max(2, int((lookback_hours * 60) / tf_minutes) + 2)
    bars = min(bars, 1000)

    since_ms = int((time.time() - lookback_hours * 3600) * 1000)
    ex = _get_market_exchange(path)
    try:
        raw = ex.fetch_ohlcv(symbol, timeframe=timeframe, since=since_ms, limit=bars)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"failed to fetch market klines: {exc}") from exc

    rows: List[Dict[str, Any]] = []
    cutoff_ms = int((time.time() - lookback_hours * 3600) * 1000)
    for item in raw or []:
        if not isinstance(item, (list, tuple)) or len(item) < 6:
            continue
        ts_ms = int(_to_float(item[0], 0))
        if ts_ms <= 0 or ts_ms < cutoff_ms:
            continue
        o = _to_float(item[1], 0.0)
        h = _to_float(item[2], 0.0)
        l = _to_float(item[3], 0.0)
        c = _to_float(item[4], 0.0)
        v = _to_float(item[5], 0.0)
        if c <= 0:
            continue
        rows.append(
            {
                "symbol": symbol,
                "time": int(ts_ms / 1000),
                "ts_utc": datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).isoformat(),
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": v,
            }
        )

    rows.sort(key=lambda x: int(x.get("time", 0)))
    _persist_market_klines(str(path), timeframe, rows)
    return {"symbol": symbol, "timeframe": timeframe, "lookback_hours": lookback_hours, "count": len(rows), "rows": rows}


def _get_market_exchange(config_path: Path):
    resolved = config_path.resolve()
    mtime = resolved.stat().st_mtime

    with _MARKET_EXCHANGE_LOCK:
        cached_path = _MARKET_EXCHANGE_CACHE.get("config_path")
        cached_mtime = _MARKET_EXCHANGE_CACHE.get("config_mtime")
        cached_exchange = _MARKET_EXCHANGE_CACHE.get("exchange")
        if cached_exchange is not None and cached_path == str(resolved) and cached_mtime == mtime:
            return cached_exchange

    cfg = _load_config_with_db_fallback(resolved)
    keys = cfg.raw.get("keys", {}) if isinstance(cfg.raw.get("keys"), dict) else {}
    ex = make_exchange(
        cfg.exchange,
        str(keys.get("apiKey", "")),
        str(keys.get("secret", "")),
        str(keys.get("password", "")),
        position_mode=str(cfg.raw.get("position_mode", "oneway")),
    )

    with _MARKET_EXCHANGE_LOCK:
        _MARKET_EXCHANGE_CACHE["config_path"] = str(resolved)
        _MARKET_EXCHANGE_CACHE["config_mtime"] = mtime
        _MARKET_EXCHANGE_CACHE["exchange"] = ex
    return ex


def _redact_config(raw: Dict[str, Any]) -> Dict[str, Any]:
    cleaned = deepcopy(raw)
    keys = cleaned.get("keys")
    if isinstance(keys, dict):
        for secret_key in ("apiKey", "secret", "password"):
            if secret_key in keys and keys[secret_key]:
                keys[secret_key] = "***"
    return cleaned


def _normalize_timeframe(value: str) -> str:
    if value in {"1m", "5m", "15m", "1h", "1d"}:
        return value
    if value in {"15", "30", "60", "240", "360", "720", "d", "h", "1h"}:
        return "1h"
    if value.endswith("m"):
        return value[:-1] + "m"
    return "1h"


def _normalize_backtest_status(running: bool, return_code: Optional[int], fallback: str = "failed") -> str:
    if running:
        return "running"
    if return_code is None:
        return fallback
    return "success" if return_code == 0 else "failed"


def _to_float_or_default(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _load_paper_seed_payload(path: str = _DEFAULT_CONFIG_PATH) -> Dict[str, float]:
    try:
        cfg = _load_config_with_db_fallback(_resolve_path(path))
        if not bool(cfg.raw.get("paper", False)):
            return {"equity": 0.0, "cash": 0.0}

        initial_equity = _to_float_or_default(cfg.raw.get("paper_equity_usdt"), 0.0)
        return {
            "equity": float(initial_equity),
            "cash": float(initial_equity),
        }
    except Exception:
        return {"equity": 0.0, "cash": 0.0}


def _empty_portfolio_response() -> Dict[str, Any]:
    now_ts = _now_iso()
    seed = _load_paper_seed_payload()
    return {
        "ts": now_ts,
        "equity": seed["equity"],
        "cash": seed["cash"],
        "pnlToday": 0.0,
        "pnlWeek": 0.0,
        "maxDrawdown": 0.0,
        "winRate": 0.0,
        "tradesToday": 0,
        "tradesWeek": 0,
        "equityCurve": [{"ts": now_ts, "equity": seed["equity"]}],
        "running": False,
        "stale": True,
    }


def _stopped_portfolio_response() -> Dict[str, Any]:
    now_ts = _now_iso()
    return {
        "ts": now_ts,
        "equity": 0.0,
        "cash": 0.0,
        "pnlToday": 0.0,
        "pnlWeek": 0.0,
        "maxDrawdown": 0.0,
        "winRate": 0.0,
        "tradesToday": 0,
        "tradesWeek": 0,
        "equityCurve": [],
        "running": False,
        "stale": True,
    }


_PORTFOLIO_EQ_SPIKE_THRESHOLD = 0.10
_PORTFOLIO_EQ_REBOUND_TOLERANCE = 0.05


def _filter_transient_equity_spikes(
    equity_series: pd.Series,
    spike_threshold: float = _PORTFOLIO_EQ_SPIKE_THRESHOLD,
    rebound_tolerance: float = _PORTFOLIO_EQ_REBOUND_TOLERANCE,
) -> pd.Series:
    if equity_series.empty:
        return equity_series
    if len(equity_series) < 3:
        return equity_series

    prev = equity_series.shift(1)
    nxt = equity_series.shift(-1)
    neighbor_min = pd.concat([prev, nxt], axis=1).min(axis=1)
    neighbor_max = pd.concat([prev, nxt], axis=1).max(axis=1)
    valid_neighbors = prev.notna() & nxt.notna() & (neighbor_min > 0) & (neighbor_max > 0)
    if not bool(valid_neighbors.any()):
        return equity_series

    drop_ratio = (neighbor_min - equity_series) / neighbor_min
    rise_ratio = (equity_series - neighbor_max) / neighbor_max
    rebound_gap = (prev - nxt).abs() / neighbor_max
    transient_mask = valid_neighbors & rebound_gap.le(rebound_tolerance) & (
        drop_ratio.ge(spike_threshold) | rise_ratio.ge(spike_threshold)
    )
    if not bool(transient_mask.any()):
        return equity_series
    filtered = equity_series[~transient_mask]
    return filtered if not filtered.empty else equity_series


def _compute_max_drawdown(equity_series: pd.Series) -> float:
    if equity_series.empty:
        return 0.0
    sanitized = _filter_transient_equity_spikes(equity_series)
    peaks = sanitized.cummax()
    valid_peaks = peaks > 0
    if not bool(valid_peaks.any()):
        return 0.0
    drawdown = (peaks - sanitized) / peaks
    max_drawdown = float(drawdown[valid_peaks].max())
    if max_drawdown < 0:
        return 0.0
    if max_drawdown > 1:
        return 1.0
    return max_drawdown


def _parse_iso_utc(value: Any) -> Optional[datetime]:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except Exception:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _clean_paper_equity_frame(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    required_cols = {"ts_utc", "equity"}
    if not required_cols.issubset(set(df.columns)):
        return None

    ts_series = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")
    equity_series = pd.to_numeric(df["equity"], errors="coerce")
    if "cash" in df.columns:
        cash_series = pd.to_numeric(df["cash"], errors="coerce")
    else:
        cash_series = pd.Series(index=df.index, dtype="float64")

    clean_df = pd.DataFrame(
        {
            "ts_utc": ts_series,
            "equity": equity_series,
            "cash": cash_series,
        }
    )
    clean_df = clean_df.dropna(subset=["ts_utc", "equity"])
    clean_df = clean_df[clean_df["equity"] > 0]
    if clean_df.empty:
        return clean_df
    clean_df = clean_df.sort_values("ts_utc").drop_duplicates(subset=["ts_utc"], keep="last")
    clean_df["cash"] = clean_df["cash"].where(clean_df["cash"].notna(), clean_df["equity"])
    return clean_df


def _iter_clean_paper_equity_chunks(csv_path: Path):
    try:
        reader = pd.read_csv(
            csv_path,
            usecols=lambda col: col in {"ts_utc", "equity", "cash"},
            chunksize=_PAPER_EQUITY_CHUNK_ROWS,
        )
    except Exception:
        return

    try:
        for chunk in reader:
            clean_df = _clean_paper_equity_frame(chunk)
            if clean_df is None:
                return
            if not clean_df.empty:
                yield clean_df
    except Exception:
        return


def _finite_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except Exception:
        return float(default)
    if parsed != parsed or parsed in {float("inf"), float("-inf")}:
        return float(default)
    return parsed


def _sample_clean_equity_curve(clean_df: pd.DataFrame, max_points: int = _PORTFOLIO_EQUITY_CURVE_MAX_POINTS) -> List[Dict[str, Any]]:
    if clean_df.empty:
        return []
    max_points = max(1, int(max_points))
    stride = max(1, (len(clean_df) + max_points - 1) // max_points)
    sample_df = clean_df.iloc[::stride]
    latest_df = clean_df.tail(1)
    if not sample_df.empty and not latest_df.empty and sample_df.index[-1] != latest_df.index[-1]:
        sample_df = pd.concat([sample_df, latest_df])
    curve = [{"ts": ts.isoformat(), "equity": float(eq)} for ts, eq in zip(sample_df["ts_utc"], sample_df["equity"])]
    while len(curve) > max_points and len(curve) > 1:
        curve.pop(-2)
    return curve


def _paper_equity_summary_from_clean_df(clean_df: pd.DataFrame, *, include_curve: bool) -> Optional[Dict[str, Any]]:
    if clean_df.empty:
        return None

    latest_ts_obj = clean_df["ts_utc"].iloc[-1]
    latest_dt = latest_ts_obj.to_pydatetime() if hasattr(latest_ts_obj, "to_pydatetime") else datetime.now(timezone.utc)
    latest_equity = _finite_float(clean_df["equity"].iloc[-1], 0.0)
    latest_cash = _finite_float(clean_df["cash"].iloc[-1], latest_equity)
    previous_equity = (
        _finite_float(clean_df["equity"].iloc[-2], latest_equity)
        if len(clean_df) > 1
        else latest_equity
    )

    today_key = latest_dt.date()
    today_rows = clean_df[clean_df["ts_utc"].dt.date == today_key]
    today_base = _finite_float(today_rows["equity"].iloc[0], latest_equity) if not today_rows.empty else latest_equity

    week_ago_ts = latest_dt.timestamp() - 7 * 24 * 3600
    week_ago_dt = pd.Timestamp.fromtimestamp(week_ago_ts, tz=timezone.utc)
    week_rows = clean_df[clean_df["ts_utc"] >= week_ago_dt]
    week_base = _finite_float(week_rows["equity"].iloc[0], latest_equity) if not week_rows.empty else latest_equity

    equities = clean_df["equity"].astype("float64")
    peaks = equities.cummax()
    max_equity = _finite_float(peaks.iloc[-1], latest_equity) if not peaks.empty else latest_equity
    current_drawdown = (max_equity - latest_equity) / max_equity if max_equity > 0 else 0.0
    current_drawdown = max(0.0, min(1.0, float(current_drawdown)))

    return {
        "latest_ts": latest_dt.isoformat(),
        "latest_equity": latest_equity,
        "latest_cash": latest_cash,
        "previous_equity": previous_equity,
        "today_base": today_base,
        "week_base": week_base,
        "max_drawdown": _compute_max_drawdown(equities),
        "current_drawdown": current_drawdown,
        "equity_curve": _sample_clean_equity_curve(clean_df) if include_curve else [],
        "row_count": len(clean_df),
    }


def _read_paper_equity_summary_from_file(csv_path: Path, *, include_curve: bool) -> Optional[Dict[str, Any]]:
    if not csv_path.exists():
        return None

    if not include_curve:
        tail_rows = _tail_paper_equity_rows_from_file(csv_path, 2)
        if tail_rows:
            latest = tail_rows[-1]
            base = tail_rows[0]
            latest_equity = _finite_float(latest.get("equity"), 0.0)
            return {
                "latest_ts": str(latest.get("ts_utc") or _now_iso()),
                "latest_equity": latest_equity,
                "latest_cash": _finite_float(latest.get("cash"), latest_equity),
                "previous_equity": _finite_float(base.get("equity"), latest_equity),
                "today_base": latest_equity,
                "week_base": latest_equity,
                "max_drawdown": 0.0,
                "current_drawdown": 0.0,
                "equity_curve": [],
                "row_count": len(tail_rows),
            }

    file_size = _file_size_bytes(csv_path)
    if include_curve and file_size is not None and file_size <= _PAPER_EQUITY_FULL_READ_MAX_BYTES:
        try:
            df = pd.read_csv(csv_path)
        except Exception:
            df = None
        if df is not None:
            clean_df = _clean_paper_equity_frame(df)
            if clean_df is not None:
                summary = _paper_equity_summary_from_clean_df(clean_df, include_curve=include_curve)
                del clean_df
                del df
                _trim_process_memory()
                return summary

    valid_count = 0
    max_equity = 0.0
    max_drawdown = 0.0
    previous_row: Optional[Tuple[Any, Any, Any]] = None
    latest_row: Optional[Tuple[Any, Any, Any]] = None

    for clean_df in _iter_clean_paper_equity_chunks(csv_path):
        equities = clean_df["equity"].astype("float64")
        peaks = equities.cummax()
        if max_equity > 0:
            peaks = peaks.where(peaks > max_equity, max_equity)
        valid_peaks = peaks > 0
        if bool(valid_peaks.any()):
            drawdown = (peaks - equities) / peaks
            chunk_max_dd = drawdown[valid_peaks].max()
            if pd.notna(chunk_max_dd):
                max_drawdown = max(max_drawdown, float(chunk_max_dd))
        chunk_max_equity = equities.max()
        if pd.notna(chunk_max_equity):
            max_equity = max(max_equity, float(chunk_max_equity))

        tail_rows = list(clean_df[["ts_utc", "equity", "cash"]].tail(2).itertuples(index=False, name=None))
        if len(tail_rows) == 1:
            previous_row = latest_row
            latest_row = tail_rows[0]
        elif len(tail_rows) >= 2:
            previous_row = tail_rows[-2]
            latest_row = tail_rows[-1]
        valid_count += len(clean_df)

    if latest_row is None or valid_count <= 0:
        return None

    latest_ts_obj = latest_row[0]
    latest_equity = _finite_float(latest_row[1], 0.0)
    latest_cash = _finite_float(latest_row[2], latest_equity)
    previous_equity = _finite_float(previous_row[1], latest_equity) if previous_row is not None else latest_equity
    max_drawdown = max(0.0, min(1.0, float(max_drawdown)))
    current_drawdown = (max_equity - latest_equity) / max_equity if max_equity > 0 else 0.0
    current_drawdown = max(0.0, min(1.0, float(current_drawdown)))

    curve: List[Dict[str, Any]] = []
    today_base: Optional[float] = None
    week_base: Optional[float] = None
    if include_curve:
        max_points = max(1, int(_PORTFOLIO_EQUITY_CURVE_MAX_POINTS))
        stride = max(1, (valid_count + max_points - 1) // max_points)
        row_index = 0
        today_key = latest_ts_obj.date()
        week_ago_ts = latest_ts_obj - pd.Timedelta(days=7)
        last_point: Optional[Dict[str, Any]] = None
        for clean_df in _iter_clean_paper_equity_chunks(csv_path):
            for ts_obj, equity in clean_df[["ts_utc", "equity"]].itertuples(index=False, name=None):
                equity_value = _finite_float(equity, 0.0)
                if today_base is None and ts_obj.date() == today_key:
                    today_base = equity_value
                if week_base is None and ts_obj >= week_ago_ts:
                    week_base = equity_value
                point = {"ts": ts_obj.isoformat(), "equity": equity_value}
                if row_index % stride == 0:
                    curve.append(point)
                last_point = point
                row_index += 1
        if last_point is not None and (not curve or curve[-1] != last_point):
            curve.append(last_point)
        while len(curve) > max_points and len(curve) > 1:
            curve.pop(-2)

    result = {
        "latest_ts": latest_ts_obj.isoformat(),
        "latest_equity": latest_equity,
        "latest_cash": latest_cash,
        "previous_equity": previous_equity,
        "today_base": today_base if today_base is not None else latest_equity,
        "week_base": week_base if week_base is not None else latest_equity,
        "max_drawdown": max_drawdown,
        "current_drawdown": current_drawdown,
        "equity_curve": curve,
        "row_count": valid_count,
    }
    if include_curve:
        _trim_process_memory()
    return result


def _tail_paper_equity_rows_from_file(csv_path: Path, limit: int) -> Optional[List[Dict[str, Any]]]:
    if not csv_path.exists():
        return None
    limit = max(1, int(limit))
    try:
        with csv_path.open("rb") as handle:
            header = handle.readline().decode("utf-8", errors="replace").strip()
            if not header:
                return []
            handle.seek(0, os.SEEK_END)
            pos = handle.tell()
            chunks: List[bytes] = []
            newline_count = 0
            block_size = 64 * 1024
            while pos > 0 and newline_count <= limit + 1:
                read_size = min(block_size, pos)
                pos -= read_size
                handle.seek(pos)
                chunk = handle.read(read_size)
                chunks.append(chunk)
                newline_count += chunk.count(b"\n")

        text = b"".join(reversed(chunks)).decode("utf-8", errors="replace")
        lines = text.splitlines()
        if pos > 0 and lines:
            lines = lines[1:]
        if lines and lines[0].strip() == header:
            lines = lines[1:]
        tail_lines = lines[-limit:]
        if not tail_lines:
            return []
        csv_text = header + "\n" + "\n".join(tail_lines) + "\n"
        reader = csv.DictReader(io.StringIO(csv_text))
        if not {"ts_utc", "equity", "cash"}.issubset(set(reader.fieldnames or [])):
            return None
        rows = [dict(row) for row in reader]
    except Exception:
        return None
    for row in rows:
        equity = _finite_float(row.get("equity"), 0.0)
        row["equity"] = equity
        row["cash"] = _finite_float(row.get("cash"), equity)
    return rows


def _strategy_from_config(
    strategy_id: str,
    config_path: str = _DEFAULT_CONFIG_PATH,
    updated_at: Optional[str] = None,
    source_name: Optional[str] = None,
) -> Dict[str, Any]:
    resolved = _resolve_config_path(config_path)
    if not _config_available(resolved):
        raise HTTPException(status_code=404, detail=f"config file not found: {resolved}")

    cfg = _load_config_with_db_fallback(resolved)
    raw = cfg.raw if isinstance(cfg.raw, dict) else {}
    symbols = cfg.symbols or ["BTC/USDT"]

    strategy_cfg = raw.get("strategy", {})
    strategy_mode = "custom"
    if isinstance(strategy_cfg, dict):
        mode_raw = strategy_cfg.get("mode")
        if mode_raw in {"mean_reversion", "trend_following", "market_making", "custom"}:
            strategy_mode = str(mode_raw)

    strategy_status = "running" if _current_running_strategy_id() == strategy_id else "stopped"
    strategy_raw_ts = _now_iso()
    if updated_at is None:
        updated_at = strategy_raw_ts

    params: Dict[str, Any] = {}
    for key, value in raw.items():
        if key in {"symbols", "timeframe", "strategy"}:
            continue
        if isinstance(value, (str, int, float, bool)):
            params[key] = value

    if isinstance(strategy_cfg, dict):
        for key, value in strategy_cfg.items():
            if isinstance(value, (str, int, float, bool)):
                params[f"strategy.{key}"] = value
            elif isinstance(value, dict):
                for k, v in value.items():
                    if isinstance(v, (str, int, float, bool)):
                        params[f"strategy.{k}"] = v

    created_at = _now_iso()
    created_ts = resolved.stat().st_mtime
    if created_ts:
        try:
            created_at = datetime.fromtimestamp(created_ts, tz=timezone.utc).isoformat()
        except Exception:
            created_at = strategy_raw_ts

    strategy_record = {
        "id": strategy_id,
        "name": f"Strategy ({resolved.name})",
        "sourceName": source_name or f"Strategy ({resolved.name})",
        "type": strategy_mode,
        "status": strategy_status,
        "config": {
            "symbols": symbols,
            "timeframe": _normalize_timeframe(str(cfg.timeframe)),
            "params": params,
        },
        "createdAt": created_at,
        "updatedAt": updated_at,
        "_config_path": str(resolved),
        "owner": _record_owner_key(),
    }
    strategy_key = _strategy_store_key(strategy_id)
    _STRATEGY_STORE[strategy_key] = strategy_record
    _persist_strategy_record(strategy_key, strategy_record)
    return strategy_record


def _ensure_preset_strategies() -> None:
    if not _is_admin_username():
        return
    for strategy_id, path in _PRESET_STRATEGIES.items():
        if _strategy_store_get(strategy_id) is None:
            _strategy_from_config(
                strategy_id=strategy_id,
                config_path=path,
                source_name=path,
            )


def _config_path_for_strategy_id(strategy_id: str) -> str:
    if strategy_id == _DEFAULT_STRATEGY_ID:
        if not _is_admin_username():
            raise HTTPException(status_code=404, detail=f"strategy not found: {strategy_id}")
        strategy = _ensure_default_strategy()
        configured = strategy.get("_config_path")
        if isinstance(configured, str) and configured:
            return configured
        return str(_resolve_config_path(_DEFAULT_CONFIG_PATH))

    _ensure_preset_strategies()
    preset_path = _PRESET_STRATEGIES.get(strategy_id)
    if preset_path:
        if not _is_admin_username():
            raise HTTPException(status_code=404, detail=f"strategy not found: {strategy_id}")
        return str(_resolve_config_path(preset_path))

    strategy = _strategy_store_get(strategy_id)
    if not strategy:
        raise HTTPException(status_code=404, detail=f"strategy not found: {strategy_id}")

    owner_key = str(strategy.get("owner") or _record_owner_key()).strip() or _record_owner_key()
    compiled_path = _latest_compiled_script_path(_strategy_store_key(strategy_id), owner_key)
    if compiled_path:
        return compiled_path

    configured = strategy.get("_config_path")
    if isinstance(configured, str) and configured:
        return configured
    return str(_resolve_config_path(_DEFAULT_CONFIG_PATH))


def _ensure_default_strategy() -> Dict[str, Any]:
    _ensure_preset_strategies()
    strategy = _strategy_store_get(_DEFAULT_STRATEGY_ID)
    if strategy is None:
        strategy = _strategy_from_config(_DEFAULT_STRATEGY_ID)
    _sync_strategy_store_statuses()
    return strategy


def _normalize_risk_trigger_item(raw: Any, fallback_ts: Optional[str] = None) -> Optional[Dict[str, str]]:
    if not isinstance(raw, dict):
        return None
    rule = str(raw.get("rule") or raw.get("id") or raw.get("type") or "").strip()
    message = str(raw.get("message") or raw.get("reason") or raw.get("detail") or "").strip()
    ts_raw = str(raw.get("ts") or raw.get("updatedAt") or fallback_ts or _now_iso()).strip()
    parsed_ts = _parse_iso_utc(ts_raw)
    ts_iso = parsed_ts.isoformat() if parsed_ts is not None else _now_iso()
    if not rule and not message:
        return None
    if not rule:
        rule = "unspecified"
    return {
        "rule": rule,
        "ts": ts_iso,
        "message": message,
    }


def _normalize_risk_triggered_list(raw: Any, fallback_ts: Optional[str] = None) -> List[Dict[str, str]]:
    if not isinstance(raw, list):
        return []
    rows: List[Dict[str, str]] = []
    seen = set()
    for item in raw:
        normalized = _normalize_risk_trigger_item(item, fallback_ts=fallback_ts)
        if normalized is None:
            continue
        key = (normalized["rule"], normalized["message"], normalized["ts"])
        if key in seen:
            continue
        seen.add(key)
        rows.append(normalized)
    rows.sort(key=lambda row: str(row.get("ts") or ""))
    return rows


def _risk_trigger_identity(item: Dict[str, Any]) -> Tuple[str, str]:
    return (
        str(item.get("rule") or "").strip(),
        str(item.get("message") or "").strip(),
    )


def _risk_from_config(
    config_path: str = _DEFAULT_CONFIG_PATH,
    strategy_id: Optional[str] = None,
) -> Dict[str, Any]:
    key = _scoped_strategy_id(strategy_id or _DEFAULT_STRATEGY_ID)
    current = _RISK_STATE_STORE.get(key)
    if current:
        state = deepcopy(current)
        state["triggered"] = _normalize_risk_triggered_list(
            state.get("triggered"),
            fallback_ts=str(state.get("updatedAt") or _now_iso()),
        )
        return state

    cfg = _load_config_with_db_fallback(_resolve_path(config_path))
    raw = cfg.raw if isinstance(cfg.raw, dict) else {}
    risk_cfg = raw.get("risk", {})
    if not isinstance(risk_cfg, dict):
        risk_cfg = {}
    portfolio_cfg = raw.get("portfolio", {})
    if not isinstance(portfolio_cfg, dict):
        portfolio_cfg = {}

    max_drawdown = _to_float_or_default(risk_cfg.get("max_strategy_dd"), 0.0)
    if max_drawdown > 1:
        max_drawdown = max_drawdown / 100.0
    max_daily_loss = _to_float_or_default(risk_cfg.get("max_daily_loss"), 0.0)
    if max_daily_loss > 1:
        max_daily_loss = max_daily_loss / 100.0

    state = {
        "enabled": True,
        "maxDrawdownPct": max_drawdown,
        "maxPositionPct": _to_float_or_default(portfolio_cfg.get("max_weight_per_symbol"), 0.0),
        "maxRiskPerTradePct": _to_float_or_default(risk_cfg.get("risk_per_trade", 0.02), 0.02),
        "maxLeverage": _to_float_or_default(portfolio_cfg.get("gross_leverage"), 1.0),
        "dailyLossLimitPct": max_daily_loss,
        "updatedAt": _now_iso(),
        "triggered": [],
    }

    _RISK_STATE_STORE[key] = deepcopy(state)
    _persist_risk_state(key, state)
    return deepcopy(state)


def _update_risk_state(payload: Dict[str, Any], strategy_id: Optional[str] = None) -> Dict[str, Any]:
    bucket = strategy_id or _DEFAULT_STRATEGY_ID
    scoped_bucket = _scoped_strategy_id(bucket)
    cfg_path = _config_path_for_strategy_id(bucket) if strategy_id else _DEFAULT_CONFIG_PATH
    state = _risk_from_config(config_path=cfg_path, strategy_id=bucket)
    before_state = deepcopy(state)
    changed_fields: List[str] = []

    for field in ["enabled", "maxDrawdownPct", "maxPositionPct", "maxRiskPerTradePct", "maxLeverage", "dailyLossLimitPct"]:
        if field in payload:
            next_value = payload[field]
            if state.get(field) != next_value:
                changed_fields.append(field)
            state[field] = next_value

    if "triggered" in payload:
        normalized_triggered = _normalize_risk_triggered_list(
            payload.get("triggered"),
            fallback_ts=_now_iso(),
        )
        before_triggers = _normalize_risk_triggered_list(
            before_state.get("triggered"),
            fallback_ts=str(before_state.get("updatedAt") or _now_iso()),
        )
        before_index = {_risk_trigger_identity(item): item for item in before_triggers}
        after_index = {_risk_trigger_identity(item): item for item in normalized_triggered}

        for trigger_key, trigger in after_index.items():
            if trigger_key in before_index:
                continue
            _append_risk_event(
                scoped_bucket,
                "triggered",
                rule=str(trigger.get("rule") or "unspecified"),
                message=str(trigger.get("message") or ""),
                detail={"trigger": trigger, "strategy_id": bucket},
                ts_utc=str(trigger.get("ts") or _now_iso()),
            )
        for trigger_key, trigger in before_index.items():
            if trigger_key in after_index:
                continue
            _append_risk_event(
                scoped_bucket,
                "recovered",
                rule=str(trigger.get("rule") or "unspecified"),
                message=str(trigger.get("message") or ""),
                detail={"trigger": trigger, "strategy_id": bucket},
                ts_utc=_now_iso(),
            )

        state["triggered"] = normalized_triggered

    state["updatedAt"] = _now_iso()
    _RISK_STATE_STORE[scoped_bucket] = deepcopy(state)
    _persist_risk_state(scoped_bucket, state)
    if changed_fields:
        _append_risk_event(
            scoped_bucket,
            "manual_update",
            rule="manual_update",
            message=f"risk params updated: {','.join(changed_fields)}",
            detail={
                "strategy_id": bucket,
                "changed_fields": changed_fields,
                "payload": deepcopy(payload),
            },
            ts_utc=state["updatedAt"],
        )
    _audit_event(
        "risk.update",
        entity="risk",
        entity_id=str(bucket),
        detail={"strategy_id": bucket, "payload": payload},
    )
    return deepcopy(state)


def _clamp_backtest_progress(value: Any) -> int:
    try:
        pct = float(value)
    except Exception:
        pct = 0.0
    if pct < 0.0:
        pct = 0.0
    if pct > 100.0:
        pct = 100.0
    return int(round(pct))


def _extract_backtest_progress_from_message(message: str) -> Optional[int]:
    text = str(message or "")
    if not text:
        return None
    match = _BACKTEST_PROGRESS_RE.search(text)
    if match is None:
        return None
    return _clamp_backtest_progress(match.group("pct"))


def _extract_backtest_progress_from_logs(logs: List[Dict[str, Any]]) -> Optional[int]:
    for row in reversed(logs):
        progress = _extract_backtest_progress_from_message(str(row.get("message") or ""))
        if progress is not None:
            return progress
    return None


def _safe_runner_tail_logs(runner: Any, limit: int) -> List[Dict[str, Any]]:
    if limit <= 0:
        return []
    tail_logs = getattr(runner, "tail_logs", None)
    if not callable(tail_logs):
        return []
    try:
        rows = tail_logs(limit)
    except Exception:
        return []
    if not isinstance(rows, list):
        return []
    return [row for row in rows if isinstance(row, dict)]


def _should_persist_backtest_progress(run_id: str, progress: int, *, force: bool = False) -> bool:
    if not run_id:
        return False
    safe_progress = _clamp_backtest_progress(progress)
    now_epoch = time.time()
    with _BACKTEST_PROGRESS_PERSIST_LOCK:
        row = _BACKTEST_PROGRESS_PERSIST_STATE.get(run_id) or {}
        last_progress = _clamp_backtest_progress(row.get("last_progress", 0))
        last_persist_epoch = float(row.get("last_persist_epoch") or 0.0)
        if safe_progress < last_progress and not force:
            return False
        progress_delta = float(safe_progress - last_progress)
        elapsed = now_epoch - last_persist_epoch
        should = bool(
            force
            or safe_progress >= 100
            or progress_delta >= float(_BACKTEST_PROGRESS_MIN_DELTA_PCT)
            or elapsed >= float(_BACKTEST_PROGRESS_PERSIST_INTERVAL_SECONDS)
        )
        if not should:
            return False
        _BACKTEST_PROGRESS_PERSIST_STATE[run_id] = {
            "last_progress": float(safe_progress),
            "last_persist_epoch": now_epoch,
        }
        return True


def _runtime_log_type_from_process_name(name: str) -> str:
    text = str(name or "").strip()
    if text.startswith("strategy:"):
        return "strategy"
    return "system"


def _on_runtime_process_log(event: Dict[str, Any]) -> None:
    message = str(event.get("message") or "").strip()
    if not message:
        return
    metadata = event.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}

    process_name = str(event.get("name") or "")
    log_type = _runtime_log_type_from_process_name(process_name)

    owner = str(metadata.get("owner") or "").strip()
    if not owner and process_name.startswith("strategy:"):
        scoped_strategy_id = process_name.split(":", 1)[1]
        owner = str(_strategy_owner_user_key(scoped_strategy_id) or "").strip()
    if not owner:
        owner = _safe_user_key(_AUTH_FALLBACK_USER)

    strategy_id = str(metadata.get("strategy_id") or event.get("strategy_id") or "").strip()
    backtest_id = str(metadata.get("run_id") or event.get("backtest_id") or "").strip()
    source = str(event.get("source") or "system")
    ts_utc = str(event.get("ts_utc") or _now_iso())
    level = _infer_log_level(message)

    detail: Dict[str, Any] = {"process": process_name}
    if strategy_id:
        detail["strategy_id"] = strategy_id
    if backtest_id:
        detail["backtest_id"] = backtest_id

    _db_append_runtime_log(
        owner=owner,
        log_type=log_type,
        level=level,
        source=source,
        message=message,
        strategy_id=strategy_id,
        backtest_id=backtest_id,
        detail=detail,
        ts_utc=ts_utc,
    )


def _on_backtest_process_log(event: Dict[str, Any]) -> None:
    _on_runtime_process_log(event)
    metadata = event.get("metadata")
    if not isinstance(metadata, dict):
        return
    run_id = str(metadata.get("run_id") or "").strip()
    if not run_id:
        return
    progress = _extract_backtest_progress_from_message(str(event.get("message") or ""))
    if progress is None:
        return

    record = _BACKTEST_STORE.get(run_id)
    if not isinstance(record, dict):
        return

    owner = str(metadata.get("owner") or "").strip()
    if owner and str(record.get("owner") or "").strip() != owner:
        return

    before_progress = _clamp_backtest_progress(record.get("progress", 0))
    if progress < before_progress:
        return
    if progress != before_progress:
        record["progress"] = progress
        record["updatedAt"] = str(event.get("ts_utc") or _now_iso())
    if _should_persist_backtest_progress(run_id, progress):
        _persist_backtest_record(run_id, record)


def _sync_backtest_record_status(record: Dict[str, Any]) -> None:
    runner = _get_backtest_runner(create=False)
    if runner is None:
        return
    metadata = runner.metadata()
    if not metadata:
        return
    current_run_id = metadata.get("run_id")
    if not current_run_id or record.get("id") != str(current_run_id):
        return

    if str(record.get("owner") or "") != _record_owner_key():
        return

    status = runner.status()
    before_status = str(record.get("status") or "")
    before_updated = str(record.get("updatedAt") or "")
    before_progress = _clamp_backtest_progress(record.get("progress", 0))
    record["status"] = _normalize_backtest_status(
        running=status.get("running", False),
        return_code=status.get("return_code"),
    )
    progress = _extract_backtest_progress_from_logs(_safe_runner_tail_logs(runner, 500))
    if progress is not None and progress >= before_progress:
        record["progress"] = progress
    if not bool(status.get("running", False)) and status.get("return_code") == 0:
        record["progress"] = 100
    if status.get("ended_at"):
        record["updatedAt"] = str(status.get("ended_at"))
    run_id = str(record.get("id") or "")
    status_changed = str(record.get("status") or "") != before_status or str(record.get("updatedAt") or "") != before_updated
    progress_now = _clamp_backtest_progress(record.get("progress", 0))
    progress_changed = progress_now != before_progress
    if not run_id:
        return
    if status_changed:
        _should_persist_backtest_progress(run_id, progress_now, force=True)
        _persist_backtest_record(run_id, record)
        return
    if progress_changed and _should_persist_backtest_progress(run_id, progress_now):
        _persist_backtest_record(run_id, record)


def _collect_backtest_records() -> List[Dict[str, Any]]:
    _sync_backtest_status_everywhere()
    owner_key = _record_owner_key()
    records = [item for item in _BACKTEST_STORE.values() if str(item.get("owner") or "") == owner_key]
    records.sort(key=lambda item: str(item.get("createdAt", "")), reverse=True)
    return records


def _build_portfolio_response(
    path: str = "logs/paper_equity.csv",
    strategy_id: Optional[str] = None,
) -> Dict[str, Any]:
    target_strategy_id = strategy_id or _current_running_strategy_id()
    if not target_strategy_id:
        return _stopped_portfolio_response()

    # Keep reporting historical portfolio metrics from paper log even when strategy is stopped.
    # The response is marked as stale in that case instead of zero-filling key metrics.
    try:
        running = bool(_strategy_status(target_strategy_id, log_limit=0).get("running", False))
    except Exception:
        running = False

    csv_path = _resolve_path(path)
    owner_key = _record_owner_key()
    paper_file_key = _safe_strategy_id(target_strategy_id or _project_relative_path_text(csv_path))
    summary = _read_paper_equity_summary_from_file(csv_path, include_curve=True)
    if summary is not None:
        latest_ts = str(summary.get("latest_ts") or _now_iso())
        latest_dt = _parse_iso_utc(latest_ts) or datetime.now(timezone.utc)
        latest_equity = _finite_float(summary.get("latest_equity"), 0.0)
        latest_cash = _finite_float(summary.get("latest_cash"), latest_equity)
        today_base = _finite_float(summary.get("today_base"), latest_equity)
        week_base = _finite_float(summary.get("week_base"), latest_equity)
        pnl_today = latest_equity - today_base
        pnl_week = latest_equity - week_base
        max_drawdown = _finite_float(summary.get("max_drawdown"), 0.0)
        curve = [dict(item) for item in summary.get("equity_curve", []) if isinstance(item, dict)]
        today_key = latest_dt.date()
        week_ago_ts = latest_dt.timestamp() - 7 * 24 * 3600
    else:
        df = _load_csv_with_db_fallback(
            path=csv_path,
            owner=owner_key,
            scope="paper_equity_csv",
            file_key=paper_file_key,
        )
        if df is None:
            return _empty_portfolio_response()

        if df.empty:
            return _empty_portfolio_response()

        clean_df = _clean_paper_equity_frame(df)
        if clean_df is None:
            raise HTTPException(status_code=500, detail="paper equity csv columns are invalid")
        if clean_df.empty:
            return _empty_portfolio_response()

        curve = _sample_clean_equity_curve(clean_df)

        latest_ts_obj = clean_df["ts_utc"].iloc[-1]
        latest_dt = latest_ts_obj.to_pydatetime() if hasattr(latest_ts_obj, "to_pydatetime") else datetime.now(timezone.utc)
        latest_ts = latest_dt.isoformat()
        latest_equity = float(clean_df["equity"].iloc[-1])
        latest_cash = float(clean_df["cash"].iloc[-1])

        today_key = latest_dt.date()
        today_rows = clean_df[clean_df["ts_utc"].dt.date == today_key]
        today_base = float(today_rows["equity"].iloc[0]) if not today_rows.empty else latest_equity
        pnl_today = latest_equity - today_base

        week_ago_ts = latest_dt.timestamp() - 7 * 24 * 3600
        week_ago_dt = pd.Timestamp.fromtimestamp(week_ago_ts, tz=timezone.utc)
        week_rows = clean_df[clean_df["ts_utc"] >= week_ago_dt]
        week_base = float(week_rows["equity"].iloc[0]) if not week_rows.empty else latest_equity
        pnl_week = latest_equity - week_base

        max_drawdown = _compute_max_drawdown(clean_df["equity"])

    fills = _build_live_fills_payload(strategy_id=target_strategy_id or _DEFAULT_STRATEGY_ID)
    trades_today = 0
    trades_week = 0
    for fill in fills:
        fill_ts = str(fill.get("ts") or "")
        fill_dt = _parse_iso_utc(fill_ts)
        if fill_dt is None:
            if fill_ts.startswith(today_key.isoformat()):
                trades_today += 1
            continue
        if fill_dt.date() == today_key:
            trades_today += 1
        if fill_dt.timestamp() >= week_ago_ts:
            trades_week += 1

    return {
        "ts": latest_ts,
        "equity": latest_equity,
        "cash": latest_cash,
        "pnlToday": float(pnl_today),
        "pnlWeek": float(pnl_week),
        "maxDrawdown": max_drawdown,
        "winRate": 0.0,
        "tradesToday": int(trades_today),
        "tradesWeek": int(trades_week),
        "equityCurve": curve,
        "running": bool(running),
        "stale": not bool(running),
    }


def _collect_process_log_entries(
    source: str,
    entries: List[Dict[str, Any]],
    *,
    id_prefix: str,
    extra: str = "",
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for idx, entry in enumerate(entries):
        ts = str(entry.get("ts_utc") or _now_iso())
        message = str(entry.get("message") or "")
        out.append(
            {
                "id": f"{id_prefix}_{extra}_{idx}_{ts}",
                "ts": ts,
                "level": _infer_log_level(message),
                "source": source,
                "message": message,
                "strategyId": entry.get("strategy_id"),
                "backtestId": entry.get("backtest_id"),
            }
        )
    return out


def _parse_paper_fill_events(logs: List[Dict[str, str]]) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    for raw in logs:
        message = str(raw.get("message") or "")
        match = _PAPER_FILL_RE.search(message)
        if not match:
            continue
        side = str(match.group("side"))
        symbol = str(match.group("symbol"))
        qty = _to_float(match.group("amount"), 0.0)
        price = _to_float(match.group("price"), 0.0)
        if qty <= 0 or price <= 0:
            continue
        ts = str(raw.get("ts_utc") or _now_iso())
        events.append(
            {
                "ts": ts,
                "side": side,
                "symbol": symbol,
                "qty": qty,
                "price": price,
                "notion": _to_float(match.group("notion"), 0.0),
            }
        )
    return events


def _parse_latest_paper_positions(logs: List[Dict[str, str]]) -> Dict[str, Dict[str, float]]:
    latest_raw_positions: Dict[str, Dict[str, float]] = {}
    has_snapshot = False

    for raw in logs:
        message = str(raw.get("message") or "")

        match = _PAPER_EQUITY_POS_RE.search(message)
        if not match:
            continue

        payload = str(match.group("positions") or "").strip()
        if payload.lower() == "none" or not payload:
            latest_raw_positions = {}
            continue

        snapshot: Dict[str, Dict[str, float]] = {}
        for pos in _PAPER_POSITION_ENTRY_RE.finditer(payload):
            symbol = str(pos.group("symbol"))
            qty = _to_float(pos.group("qty"), 0.0)
            notion = _to_float(pos.group("notion"), 0.0)
            if qty == 0:
                continue
            snapshot[symbol] = {"qty": qty, "notion": notion}

        latest_raw_positions = snapshot
        has_snapshot = True

    # keep ts as iso string for returned rows
    if not has_snapshot:
        return {}
    return latest_raw_positions


def _resolve_strategy_config_path(strategy_id: Optional[str] = None) -> str:
    target_id = strategy_id or _current_running_strategy_id() or _DEFAULT_STRATEGY_ID
    runner = _get_strategy_runner(target_id, create=False)
    if runner is not None:
        metadata = runner.metadata()
        configured = metadata.get("config_path")
        if isinstance(configured, str) and configured:
            return configured
    try:
        return _config_path_for_strategy_id(target_id)
    except Exception:
        return _DEFAULT_CONFIG_PATH


def _resolve_strategy_paper_log_path(strategy_id: Optional[str] = None) -> str:
    target_id = strategy_id or _current_running_strategy_id() or _DEFAULT_STRATEGY_ID
    runner = _get_strategy_runner(target_id, create=False)
    if runner is not None:
        metadata = runner.metadata()
        configured = metadata.get("paper_log_path")
        if isinstance(configured, str) and configured:
            return configured

    try:
        cfg_path = _config_path_for_strategy_id(target_id)
        cfg = _load_config_with_db_fallback(_resolve_path(cfg_path))
        raw = cfg.raw if isinstance(cfg.raw, dict) else {}
        configured = raw.get("paper_log_path")
        if isinstance(configured, str) and configured:
            if _is_admin_username():
                return configured
    except Exception:
        pass
    return _paper_log_path_for_strategy(_scoped_strategy_id(target_id))


def _resolve_strategy_diagnostics_path(
    strategy_id: Optional[str] = None,
    path_override: Optional[str] = None,
) -> Path:
    if isinstance(path_override, str) and path_override.strip():
        candidate = _resolve_path(path_override.strip())
        try:
            candidate.relative_to(LOG_DIR.resolve())
        except ValueError as exc:
            raise HTTPException(
                status_code=403,
                detail=f"path must stay under {LOG_DIR.resolve()}",
            ) from exc
        if not _is_admin_username():
            expected_prefix = _safe_strategy_id(_user_scope_prefix(_current_auth_username()))
            if not candidate.name.startswith(expected_prefix):
                raise HTTPException(
                    status_code=403,
                    detail="path override is restricted to current user scope",
                )
        return candidate

    target_id = strategy_id or _current_running_strategy_id() or _DEFAULT_STRATEGY_ID
    runner = _get_strategy_runner(target_id, create=False)
    if runner is not None:
        metadata = runner.metadata()
        configured = metadata.get("diagnostics_path")
        if isinstance(configured, str) and configured:
            return _resolve_path(configured)
        cfg_path = metadata.get("config_path")
        if isinstance(cfg_path, str) and cfg_path:
            try:
                cfg = _load_config_with_db_fallback(_resolve_path(cfg_path))
                raw = cfg.raw if isinstance(cfg.raw, dict) else {}
                diag = raw.get("diagnostics")
                if isinstance(diag, dict):
                    snapshot_path = diag.get("snapshot_path")
                    if isinstance(snapshot_path, str) and snapshot_path:
                        return _resolve_path(snapshot_path)
            except Exception:
                pass

    fallback_path = LOG_DIR / "diagnostics" / f"{_safe_strategy_id(_scoped_strategy_id(target_id))}.json"
    if fallback_path.exists():
        return fallback_path

    # When strategy_id is explicitly provided, avoid falling back to other
    # strategy snapshots (which is misleading for per-strategy diagnostics).
    if strategy_id:
        raise HTTPException(status_code=404, detail=f"diagnostics snapshot not found: {fallback_path}")

    # Best-effort fallback only for requests without explicit strategy_id.
    diagnostics_dir = LOG_DIR / "diagnostics"
    if diagnostics_dir.exists():
        current_user = _current_auth_username()
        expected_prefix = _user_scope_prefix(current_user)
        candidates = sorted(
            diagnostics_dir.glob("*.json"),
            key=lambda item: item.stat().st_mtime if item.exists() else 0.0,
            reverse=True,
        )
        for candidate in candidates:
            if candidate.name.endswith("_exceptions.json"):
                continue
            stem = candidate.stem
            if _is_admin_username(current_user):
                if stem.startswith(_USER_STRATEGY_SCOPE_PREFIX):
                    continue
            else:
                if not stem.startswith(expected_prefix):
                    continue
            return candidate

    raise HTTPException(status_code=404, detail="diagnostics snapshot not found")


def _simulate_paper_positions(fill_events: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    state: Dict[str, Dict[str, float]] = {}
    for event in fill_events:
        side = str(event.get("side"))
        symbol = str(event.get("symbol"))
        qty = _to_float(event.get("qty"), 0.0)
        price = _to_float(event.get("price"), 0.0)
        if qty <= 0 or price <= 0 or not symbol:
            continue

        delta = qty if side == "buy" else -qty
        record = state.get(symbol)
        if record is None:
            record = {"qty": 0.0, "avgPrice": 0.0}
            state[symbol] = record

        current_qty = _to_float(record.get("qty"), 0.0)
        current_avg = _to_float(record.get("avgPrice"), 0.0)
        next_qty = current_qty + delta

        if abs(current_qty) <= 1e-12:
            record["qty"] = next_qty
            record["avgPrice"] = price
            continue

        if current_qty * delta > 0:
            total_abs_qty = abs(current_qty) + qty
            if total_abs_qty > 0:
                record["avgPrice"] = (abs(current_qty) * current_avg + qty * price) / total_abs_qty
            record["qty"] = next_qty
            continue

        abs_current = abs(current_qty)
        if abs(delta) < abs_current:
            # partial close: average price keeps previous entry price
            record["qty"] = next_qty
        elif abs(delta) == abs_current:
            # fully closed
            record["qty"] = 0.0
            record["avgPrice"] = 0.0
        else:
            # full close + reverse; average reset to new side entry
            record["qty"] = next_qty
            record["avgPrice"] = price

        if abs(record["qty"]) <= 1e-12:
            record["qty"] = 0.0
            record["avgPrice"] = 0.0

    return state


def _latest_tick_prices(config_path: str, refresh_ms: int = 1000) -> Dict[str, float]:
    try:
        snapshot = _resolve_market_ticks_payload(config_path=config_path, refresh_ms=refresh_ms)
        ticks = snapshot.get("ticks", [])
        prices: Dict[str, float] = {}
        for tick in ticks:
            symbol = str(tick.get("symbol", ""))
            if not symbol:
                continue
            prices[symbol] = _to_float(tick.get("price"), 0.0)
        return prices
    except Exception:
        return {}


def _load_live_strategy_logs(strategy_id: str, limit: int = 2000) -> List[Dict[str, str]]:
    """
    Prefer persisted runtime_logs for live payload parsing.
    Fallback to in-process tail logs when database rows are unavailable.
    """
    target_id = str(strategy_id or "").strip()
    safe_limit = max(1, min(int(limit), 5000))
    if target_id and _db_is_enabled():
        rows = _db_list_runtime_logs(
            owner=_record_owner_key(),
            log_type="strategy",
            strategy_id=target_id,
            limit=safe_limit,
        )
        if rows:
            payload: List[Dict[str, str]] = []
            for row in reversed(rows):
                payload.append(
                    {
                        "ts_utc": str(row.get("ts") or row.get("ts_utc") or _now_iso()),
                        "source": str(row.get("source") or "strategy"),
                        "message": str(row.get("message") or ""),
                    }
                )
            if payload:
                return payload
    return _tail_strategy_logs(target_id, safe_limit)


def _load_live_strategy_diagnostics_snapshot(strategy_id: str) -> Dict[str, Any]:
    target_id = str(strategy_id or "").strip()
    if not target_id:
        return {}
    try:
        path = _resolve_strategy_diagnostics_path(strategy_id=target_id)
    except Exception:
        return {}
    diag_key = f"{_safe_strategy_id(target_id)}:{_project_relative_path_text(path)}"
    diag_text: Optional[str] = None
    if path.exists():
        diag_text = _sync_text_file_to_db(
            owner=_record_owner_key(),
            scope="strategy_diagnostics_json",
            file_key=diag_key,
            path=path,
            content_type="application/json",
            meta={"kind": "strategy_diagnostics", "strategy_id": target_id, "path": str(path)},
        )
    if diag_text is None:
        diag_row = _db_get_data_file(
            owner=_record_owner_key(),
            scope="strategy_diagnostics_json",
            file_key=diag_key,
        )
        diag_text = _decode_data_file_text(diag_row)
    if diag_text is None:
        return {}
    try:
        raw = json.loads(diag_text)
    except Exception:
        return {}
    if not isinstance(raw, dict):
        return {}
    nested = raw.get("snapshot")
    if isinstance(nested, dict):
        return nested
    return raw


def _parse_diagnostics_fill_events(snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = snapshot.get("recent_order_attempts")
    if not isinstance(rows, list):
        return []
    events: List[Dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "").lower()
        if "filled" not in status:
            continue
        side = str(item.get("side") or "").strip().lower()
        if side not in {"buy", "sell"}:
            continue
        symbol = str(item.get("symbol") or "").strip()
        qty = _to_float(item.get("qty"), 0.0)
        if qty <= 0:
            qty = _to_float(item.get("amount"), 0.0)
        price = _to_float(item.get("price"), 0.0)
        if not symbol or qty <= 0 or price <= 0:
            continue
        ts = str(item.get("ts") or _now_iso())
        notion = _to_float(item.get("notional"), 0.0)
        if notion <= 0:
            notion = qty * price
        events.append(
            {
                "ts": ts,
                "side": side,
                "symbol": symbol,
                "qty": qty,
                "price": price,
                "notion": notion,
            }
        )
    events.sort(key=lambda row: str(row.get("ts") or ""))
    return events


def _build_positions_from_diagnostics(snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
    generated_at = str(snapshot.get("generated_at") or _now_iso())
    pos_root = snapshot.get("positions_and_orders")
    if not isinstance(pos_root, dict):
        return []
    rows = pos_root.get("positions")
    if not isinstance(rows, list):
        return []
    payload: List[Dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").strip()
        if not symbol:
            continue
        qty = _to_float(item.get("qty"), 0.0)
        side = str(item.get("side") or "").strip().lower()
        if qty <= 0:
            continue
        if side in {"short", "sell"}:
            qty = -abs(qty)
        avg_price = _to_float(item.get("avg_price"), 0.0)
        last_price = _to_float(item.get("mark_price"), 0.0)
        notion = _to_float(item.get("notional"), 0.0)
        if avg_price <= 0 and notion > 0 and abs(qty) > 0:
            avg_price = notion / abs(qty)
        if last_price <= 0:
            last_price = avg_price
        pnl = _to_float(item.get("unrealized_pnl"), 0.0)
        if pnl == 0.0 and avg_price > 0 and last_price > 0:
            pnl = (last_price - avg_price) * qty
        payload.append(
            {
                "ts": generated_at,
                "symbol": symbol,
                "qty": qty,
                "avgPrice": avg_price,
                "lastPrice": last_price,
                "unrealizedPnl": pnl,
            }
        )
    return sorted(payload, key=lambda row: str(row.get("symbol")))


def _build_live_positions_payload(strategy_id: Optional[str] = None) -> List[Dict[str, Any]]:
    target_id = strategy_id or _current_running_strategy_id() or _DEFAULT_STRATEGY_ID
    logs: List[Dict[str, str]] = _load_live_strategy_logs(target_id, 2000)
    fill_events = _parse_paper_fill_events(logs)
    snapshot_positions = _parse_latest_paper_positions(logs)
    if not fill_events and not snapshot_positions:
        snapshot = _load_live_strategy_diagnostics_snapshot(target_id)
        fill_events = _parse_diagnostics_fill_events(snapshot)
    state = _simulate_paper_positions(fill_events)

    # Prefer latest snapshot for qty (it is derived by bot internal pnl accounting),
    # then keep fill-based average price as much as possible.
    for symbol, item in snapshot_positions.items():
        state.setdefault(symbol, {"qty": 0.0, "avgPrice": 0.0})
        qty = _to_float(item.get("qty"), 0.0)
        if qty == 0:
            state[symbol]["qty"] = 0.0
            state[symbol]["avgPrice"] = 0.0
        else:
            state[symbol]["qty"] = qty
            notion = _to_float(item.get("notion"), 0.0)
            if abs(qty) > 0 and notion != 0:
                state[symbol]["avgPrice"] = notion / qty

    # Keep only non-zero positions so empty book does not flood UI.
    if not state:
        if not logs:
            snapshot = _load_live_strategy_diagnostics_snapshot(target_id)
            fallback_rows = _build_positions_from_diagnostics(snapshot)
            if fallback_rows:
                return fallback_rows
        return []

    config_path = _resolve_strategy_config_path(target_id)
    market_prices = _latest_tick_prices(config_path, refresh_ms=1000)
    latest_ts = _now_iso()
    if logs:
        latest_ts = str(logs[-1].get("ts_utc") or latest_ts)

    rows: List[Dict[str, Any]] = []
    for symbol, item in state.items():
        qty = _to_float(item.get("qty"), 0.0)
        if abs(qty) <= 1e-12:
            continue
        avg_price = _to_float(item.get("avgPrice"), 0.0)
        if avg_price <= 0 and symbol in state:
            maybe_notion = _to_float(snapshot_positions.get(symbol, {}).get("notion"), 0.0)
            if abs(qty) > 0 and maybe_notion != 0:
                avg_price = maybe_notion / qty
        last_price = _to_float(market_prices.get(symbol), avg_price)
        pnl = (last_price - avg_price) * qty
        rows.append(
            {
                "ts": latest_ts,
                "symbol": symbol,
                "qty": qty,
                "avgPrice": avg_price,
                "lastPrice": last_price,
                "unrealizedPnl": pnl,
            }
        )

    return sorted(rows, key=lambda row: str(row.get("symbol")))


def _build_live_orders_payload(strategy_id: Optional[str] = None) -> List[Dict[str, Any]]:
    target_id = strategy_id or _current_running_strategy_id() or _DEFAULT_STRATEGY_ID
    fill_events = _parse_paper_fill_events(_load_live_strategy_logs(target_id, 2000))
    if not fill_events:
        snapshot = _load_live_strategy_diagnostics_snapshot(target_id)
        fill_events = _parse_diagnostics_fill_events(snapshot)
    payload: List[Dict[str, Any]] = []
    seen = set()

    for idx, event in enumerate(fill_events):
        ts = str(event.get("ts", _now_iso()))
        symbol = str(event.get("symbol"))
        side = str(event.get("side"))
        qty = _to_float(event.get("qty"), 0.0)
        price = _to_float(event.get("price"), 0.0)

        key = (ts, symbol, side, round(qty, 8), round(price, 8))
        if key in seen:
            continue
        seen.add(key)

        payload.append(
            {
                "id": f"ord_{idx}_{ts.replace(':', '').replace('.', '')}",
                "ts": ts,
                "symbol": symbol,
                "side": side,
                "type": "limit",
                "qty": qty,
                "price": price,
                "filledQty": qty,
                "status": "filled",
            }
        )

    return sorted(payload, key=lambda row: str(row.get("ts")), reverse=True)


def _build_live_fills_payload(strategy_id: Optional[str] = None) -> List[Dict[str, Any]]:
    target_id = strategy_id or _current_running_strategy_id() or _DEFAULT_STRATEGY_ID
    fill_events = _parse_paper_fill_events(_load_live_strategy_logs(target_id, 2000))
    if not fill_events:
        snapshot = _load_live_strategy_diagnostics_snapshot(target_id)
        fill_events = _parse_diagnostics_fill_events(snapshot)
    payload: List[Dict[str, Any]] = []
    seen = set()

    for idx, event in enumerate(fill_events):
        ts = str(event.get("ts", _now_iso()))
        symbol = str(event.get("symbol"))
        side = str(event.get("side"))
        qty = _to_float(event.get("qty"), 0.0)
        price = _to_float(event.get("price"), 0.0)
        key = (ts, symbol, side, round(qty, 8), round(price, 8))
        if key in seen:
            continue
        seen.add(key)
        payload.append(
            {
                "id": f"fill_{idx}_{ts.replace(':', '').replace('.', '')}",
                "ts": ts,
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "price": price,
                "fee": 0,
                "orderId": f"ord_{idx}_{ts.replace(':', '').replace('.', '')}",
            }
        )

    return sorted(payload, key=lambda row: str(row.get("ts")), reverse=True)


def _sync_backtest_status_everywhere() -> None:
    runner = _get_backtest_runner(create=False)
    if runner is None:
        return
    metadata = runner.metadata()
    current_run_id = metadata.get("run_id")
    if current_run_id:
        record = _BACKTEST_STORE.get(str(current_run_id))
        if record is not None:
            _sync_backtest_record_status(record)


def _build_backtest_record(
    run_id: str,
    payload: Dict[str, Any],
    start: str,
    end: str,
    initial_capital: float,
    fee_rate: float,
    slippage: float,
) -> Dict[str, Any]:
    strategy = _ensure_default_strategy()
    strategy_id = payload.get("strategyId", strategy["id"])
    strategy_name = strategy.get("name", f"Strategy ({run_id})")
    symbol = str(payload.get("symbol", strategy["config"]["symbols"][0] if strategy["config"]["symbols"] else "UNKNOWN"))

    now_iso = _now_iso()
    return {
        "id": run_id,
        "owner": _record_owner_key(),
        "strategyId": strategy_id,
        "strategyName": strategy_name,
        "symbol": symbol,
        "startAt": start,
        "endAt": end,
        "initialCapital": float(initial_capital),
        "feeRate": float(fee_rate),
        "slippage": float(slippage),
        "status": "running",
        "progress": 0,
        "createdAt": now_iso,
        "updatedAt": now_iso,
    }


def _backtest_create_fingerprint(
    *,
    owner: str,
    strategy_id: str,
    symbol: str,
    start: str,
    end: str,
    initial_capital: float,
    fee_rate: float,
    slippage: float,
) -> str:
    payload = {
        "owner": str(owner),
        "strategy_id": str(strategy_id),
        "symbol": str(symbol),
        "start": str(start),
        "end": str(end),
        "initial_capital": round(float(initial_capital), 10),
        "fee_rate": round(float(fee_rate), 10),
        "slippage": round(float(slippage), 10),
    }
    serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _prune_backtest_create_dedup_locked(now_epoch: float) -> None:
    ttl = max(1, int(_BACKTEST_CREATE_DEDUP_TTL_SECONDS))
    stale_keys: List[str] = []
    for fingerprint, row in _BACKTEST_CREATE_RECENT.items():
        ts_epoch = float(row.get("ts_epoch") or 0.0)
        if now_epoch - ts_epoch > ttl:
            stale_keys.append(fingerprint)
            continue
        run_id = str(row.get("run_id") or "")
        if run_id and run_id not in _BACKTEST_STORE:
            stale_keys.append(fingerprint)
    for key in stale_keys:
        _BACKTEST_CREATE_RECENT.pop(key, None)


def _remember_backtest_create_fingerprint(fingerprint: str, run_id: str) -> None:
    if not fingerprint or not run_id:
        return
    owner = _record_owner_key()
    now_epoch = time.time()
    with _BACKTEST_CREATE_DEDUP_LOCK:
        _prune_backtest_create_dedup_locked(now_epoch)
        _BACKTEST_CREATE_RECENT[fingerprint] = {
            "run_id": str(run_id),
            "owner": owner,
            "ts_epoch": now_epoch,
        }


def _resolve_backtest_by_fingerprint(fingerprint: str) -> Optional[Dict[str, Any]]:
    if not fingerprint:
        return None
    owner = _record_owner_key()
    now_epoch = time.time()
    run_id = ""
    with _BACKTEST_CREATE_DEDUP_LOCK:
        _prune_backtest_create_dedup_locked(now_epoch)
        row = _BACKTEST_CREATE_RECENT.get(fingerprint)
        if not isinstance(row, dict):
            return None
        if str(row.get("owner") or "") != owner:
            return None
        run_id = str(row.get("run_id") or "")
    if not run_id:
        return None
    record = _resolve_backtest_record(run_id)
    if not isinstance(record, dict):
        return None
    if str(record.get("owner") or "") != owner:
        return None
    return record


def _backtest_metadata_matches_request(
    metadata: Dict[str, Any],
    *,
    start: str,
    end: str,
    config_path: Path,
    request_fingerprint: Optional[str] = None,
) -> bool:
    if not isinstance(metadata, dict):
        return False
    owner = _record_owner_key()
    if str(metadata.get("owner") or "") != owner:
        return False
    if request_fingerprint and str(metadata.get("request_fingerprint") or "") == str(request_fingerprint):
        return True
    if str(metadata.get("start") or "") != str(start):
        return False
    if str(metadata.get("end") or "") != str(end):
        return False
    meta_config_path = str(metadata.get("config_path") or "")
    if not meta_config_path:
        return False
    try:
        return _resolve_path(meta_config_path) == config_path
    except Exception:
        return meta_config_path == str(config_path)


def _build_running_backtest_response(runner: "ManagedProcess", metadata: Dict[str, Any]) -> Dict[str, Any]:
    status = runner.status()
    run_id = str(metadata.get("run_id") or "")
    artifacts = metadata.get("artifacts", {})
    progress = _extract_backtest_progress_from_logs(_safe_runner_tail_logs(runner, 500))
    if progress is not None:
        status["progress"] = progress
    status["run_id"] = run_id
    status["artifacts"] = artifacts if isinstance(artifacts, dict) else {}
    status["already_running"] = True
    if run_id:
        record = _BACKTEST_STORE.get(run_id)
        if isinstance(record, dict):
            before_progress = _clamp_backtest_progress(record.get("progress", 0))
            record["status"] = _normalize_backtest_status(
                running=True,
                return_code=status.get("return_code"),
            )
            if progress is not None and progress >= before_progress:
                record["progress"] = progress
            record["updatedAt"] = _now_iso()
            progress_now = _clamp_backtest_progress(record.get("progress", 0))
            _should_persist_backtest_progress(run_id, progress_now, force=True)
            _persist_backtest_record(run_id, record)
    return status


def _backtest_artifact_file_key(run_id: str, artifact_name: str) -> str:
    return f"{str(run_id or '').strip()}:{str(artifact_name or '').strip().lower()}"


def _sync_backtest_artifacts_to_db(
    *,
    run_id: str,
    owner: str,
    artifacts: Dict[str, Any],
) -> None:
    run_key = str(run_id or "").strip()
    owner_key = _safe_user_key(owner)
    if not run_key or not owner_key or not isinstance(artifacts, dict):
        return
    for artifact_name, path_text in artifacts.items():
        artifact = str(artifact_name or "").strip().lower()
        if not artifact:
            continue
        try:
            path = _resolve_path(str(path_text or ""))
        except Exception:
            continue
        file_key = _backtest_artifact_file_key(run_key, artifact)
        content_type = _guess_file_content_type(path=path)
        meta = {
            "kind": "backtest_artifact",
            "run_id": run_key,
            "artifact_name": artifact,
            "path": str(path),
        }
        if content_type.startswith("image/"):
            _sync_binary_file_to_db(
                owner=owner_key,
                scope="backtest_artifact",
                file_key=file_key,
                path=path,
                content_type=content_type,
                meta=meta,
            )
        else:
            _sync_text_file_to_db(
                owner=owner_key,
                scope="backtest_artifact",
                file_key=file_key,
                path=path,
                content_type=content_type,
                meta=meta,
            )


def _read_backtest_csv_rows(
    *,
    owner: str,
    run_id: str,
    artifact_name: str,
    path: str,
) -> List[Dict[str, Any]]:
    owner_key = _safe_user_key(owner)
    run_key = str(run_id or "").strip()
    artifact = str(artifact_name or "").strip().lower()
    if not owner_key or not run_key or not artifact:
        return []
    file_path = _resolve_path(path)
    file_key = _backtest_artifact_file_key(run_key, artifact)
    df = _load_csv_with_db_fallback(
        path=file_path,
        owner=owner_key,
        scope="backtest_artifact",
        file_key=file_key,
    )
    if df is None:
        return []

    rows = df.to_dict(orient="records")
    return [dict(item) for item in rows]


def _to_finite_float_or_default(value: Any, default: float = 0.0) -> float:
    parsed = _to_float_or_default(value, default)
    if parsed != parsed:  # NaN
        return float(default)
    if parsed == float("inf") or parsed == float("-inf"):
        return float(default)
    return float(parsed)


def _normalize_backtest_trade_rows(
    rows: List[Dict[str, Any]],
    *,
    run_id: str,
    default_symbol: str,
) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for idx, row in enumerate(rows):
        if not isinstance(row, dict):
            continue
        side_raw = str(row.get("side") or row.get("direction") or "buy").strip().lower()
        side = "sell" if side_raw == "sell" else "buy"
        ts = str(
            row.get("ts")
            or row.get("ts_utc")
            or row.get("ts_exec_utc")
            or row.get("time")
            or _now_iso()
        )
        normalized.append(
            {
                "id": str(
                    row.get("id")
                    or row.get("trade_id")
                    or row.get("order_id")
                    or row.get("orderId")
                    or f"{run_id}_{idx}"
                ),
                "ts": ts,
                "symbol": str(row.get("symbol") or default_symbol or "UNKNOWN"),
                "side": side,
                "qty": _to_finite_float_or_default(
                    row.get("qty"),
                    _to_finite_float_or_default(
                        row.get("amount"),
                        _to_finite_float_or_default(row.get("fill_amount"), 0.0),
                    ),
                ),
                "price": _to_finite_float_or_default(
                    row.get("price"),
                    _to_finite_float_or_default(
                        row.get("fill_price"),
                        _to_finite_float_or_default(row.get("limit_price"), 0.0),
                    ),
                ),
                "fee": _to_finite_float_or_default(
                    row.get("fee"),
                    _to_finite_float_or_default(row.get("commission"), 0.0),
                ),
                "pnl": _to_finite_float_or_default(
                    row.get("pnl"),
                    _to_finite_float_or_default(row.get("realized_pnl"), 0.0),
                ),
                "orderId": str(row.get("orderId") or row.get("order_id") or ""),
            }
        )
    return normalized


def _normalize_backtest_equity_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    peak_equity = 0.0
    prev_equity: Optional[float] = None
    for row in rows:
        if not isinstance(row, dict):
            continue
        ts = str(row.get("ts") or row.get("ts_utc") or row.get("time") or _now_iso())
        equity = _to_finite_float_or_default(row.get("equity"), 0.0)
        if prev_equity is None:
            default_pnl = 0.0
        else:
            default_pnl = equity - prev_equity
        peak_equity = max(peak_equity, equity)
        default_dd = 0.0 if peak_equity <= 0 else max(0.0, (peak_equity - equity) / peak_equity)
        normalized.append(
            {
                "ts": ts,
                "equity": equity,
                "pnl": _to_finite_float_or_default(row.get("pnl"), default_pnl),
                "dd": _to_finite_float_or_default(
                    row.get("dd"),
                    _to_finite_float_or_default(row.get("drawdown"), default_dd),
                ),
            }
        )
        prev_equity = equity
    return normalized


def _sync_backtest_details_from_artifacts(
    *,
    run_id: str,
    owner: str,
    artifacts: Dict[str, Any],
    default_symbol: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    equity_path = str(artifacts.get("equity_csv") or f"logs/api_backtest_{run_id}_equity.csv")
    trades_path = str(artifacts.get("trades_csv") or f"logs/api_backtest_{run_id}_trades.csv")
    _sync_backtest_artifacts_to_db(run_id=run_id, owner=owner, artifacts=artifacts)
    equity_rows = _normalize_backtest_equity_rows(
        _read_backtest_csv_rows(
            owner=owner,
            run_id=run_id,
            artifact_name="equity_csv",
            path=equity_path,
        )
    )
    trades = _normalize_backtest_trade_rows(
        _read_backtest_csv_rows(
            owner=owner,
            run_id=run_id,
            artifact_name="trades_csv",
            path=trades_path,
        ),
        run_id=run_id,
        default_symbol=default_symbol,
    )
    if equity_rows:
        _db_replace_backtest_equity_points(run_id=run_id, owner=owner, rows=equity_rows)
    if trades:
        _db_replace_backtest_trades(run_id=run_id, owner=owner, rows=trades)
    return equity_rows, trades


def _load_backtest_detail_rows(
    *,
    run_id: str,
    owner: str,
    artifacts: Dict[str, Any],
    default_symbol: str,
    prefer_db: bool,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    db_equity_rows: List[Dict[str, Any]] = []
    db_trades: List[Dict[str, Any]] = []
    if prefer_db:
        db_equity_rows = _normalize_backtest_equity_rows(
            _db_list_backtest_equity_points(run_id=run_id, owner=owner, limit=50000)
        )
        db_trades = _normalize_backtest_trade_rows(
            _db_list_backtest_trades(run_id=run_id, owner=owner, limit=50000),
            run_id=run_id,
            default_symbol=default_symbol,
        )
        if db_equity_rows and db_trades:
            return db_equity_rows, db_trades

    csv_equity_rows, csv_trades = _sync_backtest_details_from_artifacts(
        run_id=run_id,
        owner=owner,
        artifacts=artifacts,
        default_symbol=default_symbol,
    )
    if prefer_db:
        if not db_equity_rows:
            db_equity_rows = csv_equity_rows
        if not db_trades:
            db_trades = csv_trades
        return db_equity_rows, db_trades

    if not csv_equity_rows:
        csv_equity_rows = _normalize_backtest_equity_rows(
            _db_list_backtest_equity_points(run_id=run_id, owner=owner, limit=50000)
        )
    if not csv_trades:
        csv_trades = _normalize_backtest_trade_rows(
            _db_list_backtest_trades(run_id=run_id, owner=owner, limit=50000),
            run_id=run_id,
            default_symbol=default_symbol,
        )
    return csv_equity_rows, csv_trades


def _resolve_backtest_record(run_id: str) -> Optional[Dict[str, Any]]:
    _sync_backtest_status_everywhere()

    record = deepcopy(_BACKTEST_STORE.get(run_id))
    if record:
        if str(record.get("owner") or "") != _record_owner_key():
            return None
        return record

    runner = _get_backtest_runner(create=False)
    if runner is None:
        return None
    metadata = runner.metadata()
    if str(metadata.get("run_id")) != str(run_id):
        return None

    start = str(metadata.get("start", ""))
    end = str(metadata.get("end", ""))
    artifacts = metadata.get("artifacts", {})
    if not start or not end:
        now_iso = _now_iso()
        start = now_iso[:10]
        end = now_iso[:10]

    cfg = _resolve_path(metadata.get("config_path", _DEFAULT_CONFIG_PATH))
    if not _config_available(cfg):
        cfg = _resolve_path(_DEFAULT_CONFIG_PATH)
    config = _load_config_with_db_fallback(cfg)

    initial_capital = _to_float_or_default(config.raw.get("paper_equity_usdt"), 0.0)
    default_fee = _to_float_or_default(config.raw.get("portfolio", {}).get("fee_bps"), 0.0) / 10_000.0
    default_slippage = _to_float_or_default(config.raw.get("portfolio", {}).get("slippage_bps"), 0.0) / 10_000.0
    strategy = _ensure_default_strategy()

    record = _build_backtest_record(
        run_id=run_id,
        payload={"strategyId": str(strategy.get("id")), "symbol": config.symbols[0] if config.symbols else "UNKNOWN"},
        start=start[:10],
        end=end[:10],
        initial_capital=initial_capital,
        fee_rate=default_fee,
        slippage=default_slippage,
    )
    record["artifacts"] = artifacts
    status = runner.status()
    record["status"] = _normalize_backtest_status(
        running=status.get("running", False),
        return_code=status.get("return_code"),
        fallback="failed",
    )
    if status.get("return_code") is not None:
        record["updatedAt"] = str(status.get("ended_at") or record["updatedAt"])
    if not _BACKTEST_STORE.get(run_id):
        _BACKTEST_STORE[run_id] = record
        _persist_backtest_record(run_id, record)
    return record


def _on_backtest_process_exit(event: Dict[str, Any]) -> None:
    metadata = event.get("metadata")
    if not isinstance(metadata, dict):
        return
    run_id = str(metadata.get("run_id") or "").strip()
    if not run_id:
        return
    record = _BACKTEST_STORE.get(run_id)
    if not isinstance(record, dict):
        return

    owner = str(metadata.get("owner") or "").strip()
    if owner and str(record.get("owner") or "").strip() != owner:
        return

    return_code_raw = event.get("return_code")
    return_code: Optional[int] = None
    if isinstance(return_code_raw, int):
        return_code = return_code_raw
    else:
        try:
            return_code = int(return_code_raw)
        except Exception:
            return_code = None

    record["status"] = _normalize_backtest_status(
        running=False,
        return_code=return_code,
        fallback="failed",
    )
    if return_code == 0:
        record["progress"] = 100
    record["updatedAt"] = str(event.get("ended_at") or _now_iso())
    _persist_backtest_record(run_id, record)
    artifacts = metadata.get("artifacts")
    if isinstance(artifacts, dict):
        owner_key = str(owner or record.get("owner") or "").strip()
        _sync_backtest_details_from_artifacts(
            run_id=run_id,
            owner=owner_key,
            artifacts=artifacts,
            default_symbol=str(record.get("symbol") or ""),
        )
    with _BACKTEST_PROGRESS_PERSIST_LOCK:
        _BACKTEST_PROGRESS_PERSIST_STATE.pop(run_id, None)


class ManagedProcess:
    def __init__(
        self,
        name: str,
        max_logs: int = 2000,
        on_exit: Optional[Callable[[Dict[str, Any]], None]] = None,
        on_log: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> None:
        self.name = name
        self.max_logs = max_logs
        self._on_exit = on_exit
        self._on_log = on_log
        self._lock = threading.Lock()
        self._process: Optional[subprocess.Popen] = None
        self._logs: Deque[Dict[str, str]] = deque(maxlen=max_logs)
        self._command: List[str] = []
        self._started_at: Optional[str] = None
        self._ended_at: Optional[str] = None
        self._metadata: Dict[str, Any] = {}

    def _append_log_locked(self, source: str, message: str, ts_utc: Optional[str] = None) -> str:
        ts = str(ts_utc or _now_iso())
        self._logs.append(
            {
                "ts_utc": ts,
                "source": source,
                "message": message,
            }
        )
        return ts

    def _emit_log_callback(self, *, source: str, message: str, ts_utc: str) -> None:
        if self._on_log is None:
            return
        payload = {
            "name": self.name,
            "source": source,
            "message": message,
            "ts_utc": ts_utc,
            "metadata": deepcopy(self._metadata),
        }
        try:
            self._on_log(payload)
        except Exception:
            pass

    def _read_stream(self, proc: subprocess.Popen, stream, source: str) -> None:
        try:
            for line in iter(stream.readline, ""):
                text = line.rstrip()
                if not text:
                    continue
                ts_utc = ""
                with self._lock:
                    if proc is self._process:
                        ts_utc = self._append_log_locked(source, text)
                if ts_utc:
                    self._emit_log_callback(source=source, message=text, ts_utc=ts_utc)
        finally:
            try:
                stream.close()
            except Exception:
                pass

    def _wait_process(self, proc: subprocess.Popen) -> None:
        return_code = proc.wait()
        callback: Optional[Callable[[Dict[str, Any]], None]] = None
        callback_payload: Dict[str, Any] = {}
        with self._lock:
            if proc is self._process:
                self._ended_at = _now_iso()
                ts_utc = self._append_log_locked("system", f"process exited with code {return_code}")
                if self._on_exit is not None:
                    callback = self._on_exit
                    callback_payload = {
                        "name": self.name,
                        "return_code": return_code,
                        "started_at": self._started_at,
                        "ended_at": self._ended_at,
                        "metadata": deepcopy(self._metadata),
                    }
            else:
                ts_utc = ""
        if ts_utc:
            self._emit_log_callback(source="system", message=f"process exited with code {return_code}", ts_utc=ts_utc)
        if callback is not None:
            try:
                callback(callback_payload)
            except Exception:
                pass

    def start(self, command: List[str], cwd: Path, metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        with self._lock:
            if self._process is not None and self._process.poll() is None:
                raise RuntimeError(f"{self.name} is already running")

            self._logs.clear()
            self._command = command[:]
            self._started_at = _now_iso()
            self._ended_at = None
            self._metadata = deepcopy(metadata) if metadata else {}

            self._process = subprocess.Popen(
                command,
                cwd=str(cwd),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
            proc = self._process
            start_msg = f"start: {' '.join(command)}"
            ts_utc = self._append_log_locked("system", start_msg)
        self._emit_log_callback(source="system", message=start_msg, ts_utc=ts_utc)

        if proc.stdout is not None:
            threading.Thread(
                target=self._read_stream,
                args=(proc, proc.stdout, "stdout"),
                daemon=True,
            ).start()
        if proc.stderr is not None:
            threading.Thread(
                target=self._read_stream,
                args=(proc, proc.stderr, "stderr"),
                daemon=True,
            ).start()
        threading.Thread(target=self._wait_process, args=(proc,), daemon=True).start()
        return self.status()

    def stop(self, timeout_seconds: float = 10.0) -> bool:
        with self._lock:
            proc = self._process
            if proc is None or proc.poll() is not None:
                return False
            stop_msg = "termination requested"
            ts_utc = self._append_log_locked("system", stop_msg)
            proc.terminate()
        self._emit_log_callback(source="system", message=stop_msg, ts_utc=ts_utc)

        try:
            proc.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)

        with self._lock:
            if proc is self._process and self._ended_at is None:
                self._ended_at = _now_iso()
        return True

    def status(self) -> Dict[str, Any]:
        with self._lock:
            proc = self._process
            return_code = proc.poll() if proc is not None else None
            running = proc is not None and return_code is None
            return {
                "name": self.name,
                "running": running,
                "pid": proc.pid if proc is not None else None,
                "return_code": return_code,
                "started_at": self._started_at,
                "ended_at": self._ended_at,
                "command": self._command[:],
            }

    def tail_logs(self, limit: int) -> List[Dict[str, str]]:
        if limit <= 0:
            return []
        with self._lock:
            return list(self._logs)[-limit:]

    def metadata(self) -> Dict[str, Any]:
        with self._lock:
            return deepcopy(self._metadata)


def _get_strategy_runner(strategy_id: str, create: bool = True) -> Optional[ManagedProcess]:
    if not strategy_id:
        return None
    if _is_admin_username() and _strategy_owner_user_key(strategy_id) is not None:
        return None
    scoped_strategy_id = _scoped_strategy_id(strategy_id)
    with _STRATEGY_RUNNERS_LOCK:
        runner = _STRATEGY_RUNNERS.get(scoped_strategy_id)
        if runner is None and create:
            runner = ManagedProcess(
                name=f"strategy:{scoped_strategy_id}",
                on_log=_on_runtime_process_log,
            )
            _STRATEGY_RUNNERS[scoped_strategy_id] = runner
        return runner


def _all_strategy_runners() -> Dict[str, ManagedProcess]:
    current_user = _current_auth_username()
    with _STRATEGY_RUNNERS_LOCK:
        items = list(_STRATEGY_RUNNERS.items())
    visible: Dict[str, ManagedProcess] = {}
    for scoped_strategy_id, runner in items:
        if not _runner_visible_to_user(scoped_strategy_id, current_user):
            continue
        visible[_unscoped_strategy_id(scoped_strategy_id, current_user)] = runner
    return visible


def _get_backtest_runner(create: bool = True) -> Optional[ManagedProcess]:
    owner_key = _record_owner_key()
    with _BACKTEST_RUNNERS_LOCK:
        runner = _BACKTEST_RUNNERS.get(owner_key)
        if runner is None and create:
            runner = ManagedProcess(
                name=f"backtest:{owner_key}",
                on_exit=_on_backtest_process_exit,
                on_log=_on_backtest_process_log,
            )
            _BACKTEST_RUNNERS[owner_key] = runner
        return runner


def _strategy_status(strategy_id: str, log_limit: int = 0) -> Dict[str, Any]:
    external_rows = _external_strategy_processes_for_strategy(strategy_id)
    external_rows.sort(key=lambda item: int(item.get("pid", 0)))
    latest_external = external_rows[-1] if external_rows else None

    runner = _get_strategy_runner(strategy_id, create=False)
    if runner is None:
        metadata: Dict[str, Any] = {"strategy_id": strategy_id}
        running = False
        pid: Optional[int] = None
        command: List[str] = []
        if latest_external:
            running = True
            pid = int(latest_external.get("pid", 0)) or None
            command = list(latest_external.get("command") or [])
            config_text = str(latest_external.get("config_path") or "")
            if config_text:
                metadata.update(
                    _load_external_runtime_metadata(
                        _resolve_path(config_text),
                        _scoped_strategy_id(strategy_id),
                    )
                )
            metadata["external_pids"] = [int(item.get("pid", 0)) for item in external_rows if int(item.get("pid", 0)) > 0]
        status = {
            "name": f"strategy:{strategy_id}",
            "running": running,
            "pid": pid,
            "return_code": None,
            "started_at": None,
            "ended_at": None,
            "command": command,
            "metadata": metadata,
        }
        if log_limit > 0:
            status["logs"] = []
        return status

    status = runner.status()
    if not bool(status.get("running", False)) and latest_external:
        status["running"] = True
        status["pid"] = int(latest_external.get("pid", 0)) or None
        status["return_code"] = None
        status["command"] = list(latest_external.get("command") or [])

    status["name"] = f"strategy:{strategy_id}"
    metadata = runner.metadata()
    if latest_external:
        config_text = str(latest_external.get("config_path") or "")
        if config_text:
            external_meta = _load_external_runtime_metadata(
                _resolve_path(config_text),
                _scoped_strategy_id(strategy_id),
            )
            for key in ("config_path", "paper_log_path", "diagnostics_path", "exceptions_path"):
                if not metadata.get(key) and external_meta.get(key):
                    metadata[key] = external_meta[key]
        metadata["external_pids"] = [int(item.get("pid", 0)) for item in external_rows if int(item.get("pid", 0)) > 0]
    metadata["strategy_id"] = strategy_id
    status["metadata"] = metadata
    if log_limit > 0:
        status["logs"] = runner.tail_logs(log_limit)
    return status


def _tail_strategy_logs(strategy_id: str, limit: int) -> List[Dict[str, str]]:
    runner = _get_strategy_runner(strategy_id, create=False)
    if runner is None:
        return []
    return runner.tail_logs(limit)


def _tail_strategy_logs_all(limit_per_strategy: int = 100) -> List[Dict[str, Any]]:
    merged: List[Dict[str, Any]] = []
    for strategy_id, runner in _all_strategy_runners().items():
        rows = runner.tail_logs(limit_per_strategy)
        for row in rows:
            item = dict(row)
            item["strategy_id"] = strategy_id
            merged.append(item)
    merged.sort(key=lambda item: str(item.get("ts_utc")), reverse=True)
    return merged


class StrategyStartRequest(BaseModel):
    config_path: str = Field(default=_DEFAULT_CONFIG_PATH)
    strategy_id: Optional[str] = Field(default=None)


class BacktestStartRequest(BaseModel):
    start: str = Field(..., description="YYYY-MM-DD")
    end: str = Field(..., description="YYYY-MM-DD")
    config_path: str = Field(default=_DEFAULT_CONFIG_PATH)


class StrategyCreateRequest(BaseModel):
    name: str
    type: str = "custom"
    config: Dict[str, Any]


class BacktestCreateRequest(BaseModel):
    strategyId: str
    symbol: str
    startAt: str
    endAt: str
    initialCapital: float = Field(..., ge=0)
    feeRate: float = Field(default=0.0, ge=0)
    slippage: float = Field(default=0.0, ge=0)


class RiskUpdateRequest(BaseModel):
    enabled: bool = True
    maxDrawdownPct: float = 0.0
    maxPositionPct: float = 0.0
    maxRiskPerTradePct: float = 0.02
    maxLeverage: float = 1.0
    dailyLossLimitPct: float = 0.0
    triggered: Optional[List[Dict[str, Any]]] = None


class UserPreferencesUpdateRequest(BaseModel):
    theme: Optional[str] = None
    language: Optional[str] = None
    selectedLiveStrategyId: Optional[str] = None
    logsFilters: Optional[Dict[str, Any]] = None
    backtestsFilters: Optional[Dict[str, Any]] = None
    liveFilters: Optional[Dict[str, Any]] = None


class AuthLoginRequest(BaseModel):
    username: str
    password: str


class ApiTokenCreateRequest(BaseModel):
    owner: Optional[str] = None
    tokenName: Optional[str] = None
    scopes: Optional[List[str]] = None
    expiresAt: Optional[str] = None


class UserRolesUpdateRequest(BaseModel):
    username: str
    roles: List[str] = Field(default_factory=list)


class DbReloadRequest(BaseModel):
    enabled: Optional[bool] = None
    dbPath: Optional[str] = None
    backend: Optional[str] = None
    postgresDsn: Optional[str] = None
    preserveState: bool = True


app = FastAPI(
    title="Quant API Backend",
    version="1.0.0",
)

origins = [
    item.strip().replace("\\,", ",").rstrip("\\")
    for item in os.getenv("CORS_ORIGINS", "").split(",")
    if item.strip()
]
if origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )


@app.middleware("http")
async def api_auth_middleware(request: Request, call_next):
    path = request.url.path
    auth_username: Optional[str] = None
    if path.startswith("/api") and path not in _AUTH_EXEMPT_PATHS:
        err = _auth_error(
            authorization=request.headers.get("authorization"),
            x_api_key=request.headers.get("x-api-key"),
            session_token=request.cookies.get(_SESSION_COOKIE_NAME),
        )
        if err is not None:
            return JSONResponse(status_code=err.status_code, content={"detail": err.detail})
        auth_username = _resolve_auth_username(
            authorization=request.headers.get("authorization"),
            x_api_key=request.headers.get("x-api-key"),
            session_token=request.cookies.get(_SESSION_COOKIE_NAME),
        )
    request.state.auth_username = _normalize_auth_username(auth_username)
    with _auth_user_context(request.state.auth_username):
        return await call_next(request)


def _record_ws_connection_event(
    event_type: str,
    state: Optional[Dict[str, Any]],
    *,
    detail: Optional[Dict[str, Any]] = None,
) -> None:
    if not isinstance(state, dict):
        return
    _db_append_ws_connection_event(
        owner=str(state.get("auth_user") or "anonymous"),
        event_type=str(event_type or ""),
        connection_id=str(state.get("connection_id") or ""),
        strategy_id=str(state.get("strategy_id") or ""),
        config_path=str(state.get("config_path") or ""),
        refresh_ms=int(state.get("refresh_ms") or 0),
        client_ip=str(state.get("client_ip") or ""),
        user_agent=str(state.get("user_agent") or ""),
        detail=detail or {},
        ts_utc=_now_iso(),
    )


async def _ws_disconnect_clients(stale: List[WebSocket]) -> None:
    for ws in stale:
        state = _WS_CONNECTIONS.pop(ws, None)
        try:
            await ws.close(code=1000)
        except Exception:
            pass
        _record_ws_connection_event(
            "server_disconnect",
            state if isinstance(state, dict) else None,
            detail={"reason": "stale_client"},
        )


async def _send_ws_message(websocket: WebSocket, message: Dict[str, Any]) -> bool:
    try:
        await websocket.send_json(message)
        return True
    except WebSocketDisconnect:
        state = _WS_CONNECTIONS.pop(websocket, None)
        _record_ws_connection_event(
            "send_disconnect",
            state if isinstance(state, dict) else None,
            detail={"reason": "client_disconnected_while_sending"},
        )
        return False
    except Exception as exc:
        state = _WS_CONNECTIONS.pop(websocket, None)
        _record_ws_connection_event(
            "send_error",
            state if isinstance(state, dict) else None,
            detail={"error": str(exc)},
        )
        return False


def _build_market_messages(path: str, refresh_ms: int) -> List[Dict[str, Any]]:
    snapshot = _resolve_market_ticks_payload(config_path=path, refresh_ms=refresh_ms)
    ticks: List[Dict[str, Any]] = snapshot.get("ticks", [])

    messages: List[Dict[str, Any]] = []
    for tick in ticks:
        ts = tick.get("ts_utc") or tick.get("ts") or _now_iso()
        messages.append(
            {
                "type": "tick",
                "symbol": tick.get("symbol", ""),
                "ts": ts,
                "price": _to_float(tick.get("price"), 0.0),
                "bid": _to_float(tick.get("bid"), 0.0),
                "ask": _to_float(tick.get("ask"), 0.0),
                "volume": _to_float(tick.get("volume"), 0.0),
            }
        )
    return messages


def _build_equity_message(strategy_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if strategy_id:
        target_strategy_id = strategy_id
        try:
            if not bool(_strategy_status(target_strategy_id, log_limit=0).get("running", False)):
                return None
        except Exception:
            return None
    else:
        target_strategy_id = _current_running_strategy_id()
        if not target_strategy_id:
            return None

    seed = _load_paper_seed_payload()
    now_ts = _now_iso()
    if seed["equity"] <= 0 and seed["cash"] <= 0:
        return None

    csv_path = _resolve_path(_resolve_strategy_paper_log_path(strategy_id))
    owner_key = _record_owner_key()
    paper_file_key = _safe_strategy_id(target_strategy_id or _project_relative_path_text(csv_path))
    summary = _read_paper_equity_summary_from_file(csv_path, include_curve=False)
    if summary is not None:
        latest_equity = _finite_float(summary.get("latest_equity"), seed["equity"])
        latest_cash = _finite_float(summary.get("latest_cash"), latest_equity)
        base = _finite_float(summary.get("previous_equity"), latest_equity)
        return {
            "type": "equity",
            "strategyId": target_strategy_id,
            "ts": str(summary.get("latest_ts") or now_ts),
            "equity": latest_equity,
            "pnl": latest_equity - base,
            "dd": _finite_float(summary.get("current_drawdown"), 0.0),
            "cash": latest_cash,
        }

    df = _load_csv_with_db_fallback(
        path=csv_path,
        owner=owner_key,
        scope="paper_equity_csv",
        file_key=paper_file_key,
    )
    if df is None:
        return {
            "type": "equity",
            "strategyId": target_strategy_id,
            "ts": now_ts,
            "equity": seed["equity"],
            "pnl": 0.0,
            "dd": 0.0,
            "cash": seed["cash"],
        }

    required_cols = {"ts_utc", "equity"}
    if not required_cols.issubset(set(df.columns)):
        return {
            "type": "equity",
            "strategyId": target_strategy_id,
            "ts": now_ts,
            "equity": seed["equity"],
            "pnl": 0.0,
            "dd": 0.0,
            "cash": seed["cash"],
        }

    rows = df.tail(2).to_dict(orient="records")
    if not rows:
        return {
            "type": "equity",
            "strategyId": target_strategy_id,
            "ts": now_ts,
            "equity": seed["equity"],
            "pnl": 0.0,
            "dd": 0.0,
            "cash": seed["cash"],
        }

    latest = rows[-1]
    latest_equity = _to_float(latest.get("equity"), 0.0)
    latest_cash = _to_float(latest.get("cash"), latest_equity)
    base = _to_float(rows[0].get("equity"), latest_equity)
    pnl = latest_equity - base

    if df["equity"].shape[0] > 1:
        peaks = pd.to_numeric(df["equity"], errors="coerce").cummax()
        max_equity = _to_float(peaks.iloc[-1], latest_equity)
        dd = (max_equity - latest_equity) / max_equity if max_equity else 0.0
    else:
        dd = 0.0

    return {
        "type": "equity",
        "strategyId": target_strategy_id,
        "ts": latest.get("ts_utc", _now_iso()),
        "equity": latest_equity,
        "pnl": pnl,
        "dd": dd,
        "cash": latest_cash,
    }


def _collect_logs(
    cache_ms: int = 200,
    strategy_id: Optional[str] = None,
    auth_user: Optional[str] = None,
) -> List[Dict[str, Any]]:
    now_ms = int(time.time() * 1000)
    cache_ms = max(50, min(10_000, int(cache_ms)))
    resolved_user = _normalize_auth_username(auth_user)
    cache_key = f"{resolved_user}|{strategy_id or '__all__'}"
    cache_entry = _WS_LOG_CACHE.get(cache_key) or {}
    cached_ts = int(cache_entry.get("ts_ms", 0))
    cached_msg = cache_entry.get("msg", [])
    if now_ms - cached_ts < cache_ms and isinstance(cached_msg, list) and cached_msg:
        return [*cached_msg]

    output: List[Dict[str, Any]] = []
    with _auth_user_context(resolved_user):
        if strategy_id:
            strategy_logs = []
            for row in _tail_strategy_logs(strategy_id, 30):
                item = dict(row)
                item["strategy_id"] = strategy_id
                strategy_logs.append(item)
        else:
            strategy_logs = _tail_strategy_logs_all(limit_per_strategy=30)
    if not strategy_logs:
        return output

    for entry in reversed(strategy_logs):
        message = str(entry.get("message") or "")
        source = str(entry.get("source") or "system")
        ts = str(entry.get("ts_utc") or _now_iso())
        entry_strategy_id = str(entry.get("strategy_id") or "")
        key = f"{entry_strategy_id}|{ts}|{source}|{message}"
        if _WS_LAST_LOG_KEY.get(cache_key) == key:
            break
        output.append(
            {
                "type": "log",
                "level": _infer_log_level(message),
                "source": source,
                "ts": ts,
                "message": message,
                "strategyId": entry_strategy_id,
            }
        )

    if output:
        last = strategy_logs[-1]
        last_message = str(last.get("message") or "")
        last_source = str(last.get("source") or "system")
        last_ts = str(last.get("ts_utc") or _now_iso())
        last_strategy_id = str(last.get("strategy_id") or "")
        _WS_LAST_LOG_KEY[cache_key] = f"{last_strategy_id}|{last_ts}|{last_source}|{last_message}"
    output.reverse()
    _WS_LOG_CACHE[cache_key] = {"ts_ms": now_ms, "msg": output}
    return output


def _normalize_ws_refresh(refresh_ms: int) -> int:
    return max(200, min(10_000, int(refresh_ms)))


def _build_equity_message_cached(
    cache_ms: int = 200,
    strategy_id: Optional[str] = None,
    auth_user: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    now_ms = int(time.time() * 1000)
    cache_ms = max(50, min(10_000, int(cache_ms)))
    resolved_user = _normalize_auth_username(auth_user)
    cache_key = f"{resolved_user}|{strategy_id or '__all__'}"
    cache_entry = _WS_EQUITY_CACHE.get(cache_key) or {}
    cached_ts = int(cache_entry.get("ts_ms", 0))
    cached_msg = cache_entry.get("msg")
    if now_ms - cached_ts < cache_ms and isinstance(cached_msg, dict):
        return dict(cached_msg)

    with _auth_user_context(resolved_user):
        msg = _build_equity_message(strategy_id=strategy_id)
    if msg is not None:
        _WS_EQUITY_CACHE[cache_key] = {"ts_ms": now_ms, "msg": msg}
    return msg


async def _broadcast_loop(interval: float = 1.0) -> None:
    # interval in seconds, but actual send pace is controlled by each client refresh_ms.
    while True:
        if not _WS_CONNECTIONS:
            await asyncio.sleep(interval)
            continue

        now_ms = int(time.time() * 1000)
        stale: set[WebSocket] = set()
        groups: Dict[tuple[str, int, str, str], list[WebSocket]] = {}

        for ws, meta in list(_WS_CONNECTIONS.items()):
            refresh_ms = _normalize_ws_refresh(int(meta.get("refresh_ms", 1_000)))
            next_send_ms = int(meta.get("next_send_ms", 0))

            if now_ms < next_send_ms:
                continue

            path = str(meta.get("config_path", _DEFAULT_CONFIG_PATH))
            strategy_id = str(meta.get("strategy_id", "") or "")
            auth_user = _normalize_auth_username(str(meta.get("auth_user", "") or ""))
            _WS_CONNECTIONS[ws]["next_send_ms"] = now_ms + refresh_ms
            groups.setdefault((path, refresh_ms, strategy_id, auth_user), []).append(ws)

        if not groups:
            await asyncio.sleep(interval)
            continue

        for (path, refresh_ms, strategy_id, auth_user), ws_list in groups.items():
            payloads: List[Dict[str, Any]] = []
            try:
                payloads.extend(await asyncio.to_thread(_build_market_messages, path, refresh_ms))
            except Exception:
                pass

            equity = _build_equity_message_cached(
                refresh_ms,
                strategy_id=strategy_id or None,
                auth_user=auth_user,
            )
            if equity is not None:
                payloads.append(equity)

            payloads.extend(
                _collect_logs(
                    cache_ms=refresh_ms,
                    strategy_id=strategy_id or None,
                    auth_user=auth_user,
                )
            )

            for ws in ws_list:
                for msg in payloads:
                    ok = await _send_ws_message(ws, msg)
                    if not ok:
                        stale.add(ws)
                        break

        for ws in stale:
            _WS_CONNECTIONS.pop(ws, None)

        await asyncio.sleep(interval)


@app.on_event("startup")
async def _startup_event() -> None:
    global _WS_BROADCAST_TASK
    _maybe_alert_db_init_failure()
    try:
        with _auth_user_context(_AUTH_FALLBACK_USER):
            _sync_auth_users_to_database()
            _load_state_from_database()
            _ensure_db_alert_outbox_worker()
            _scan_external_strategy_processes(force=True)
            _ensure_default_strategy()
            _sync_strategy_store_statuses()
            _recover_pending_strategy_compile_jobs(limit=1000)
            _ensure_strategy_compile_worker()
    except Exception:
        # Keep service boot resilient even if process reconciliation fails.
        pass
    if _WS_BROADCAST_TASK is None or _WS_BROADCAST_TASK.done():
        _WS_BROADCAST_TASK = asyncio.create_task(_broadcast_loop(interval=_WS_BROADCAST_INTERVAL_SEC))


@app.on_event("shutdown")
async def _shutdown_event() -> None:
    global _WS_BROADCAST_TASK, _STRATEGY_COMPILE_WORKER
    if _WS_BROADCAST_TASK is not None:
        _WS_BROADCAST_TASK.cancel()
        with suppress(asyncio.CancelledError):
            await _WS_BROADCAST_TASK
        _WS_BROADCAST_TASK = None
    _stop_db_alert_outbox_worker(join_timeout=2.0)
    _STRATEGY_COMPILE_STOP.set()
    _STRATEGY_COMPILE_EVENT.set()
    worker = _STRATEGY_COMPILE_WORKER
    if worker is not None and worker.is_alive():
        worker.join(timeout=2.0)
    _STRATEGY_COMPILE_WORKER = None
    _WS_CONNECTIONS.clear()
    _close_db_repository(_DB)


@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    config_path: str = Query(default=_DEFAULT_CONFIG_PATH),
    strategy_id: Optional[str] = Query(default=None),
    refresh_ms: int = Query(default=1000, ge=200, le=10000),
) -> None:
    connection_id = uuid.uuid4().hex
    requested_refresh = _normalize_ws_refresh(refresh_ms)
    requested_strategy_id = strategy_id or ""
    client_ip = ""
    try:
        if websocket.client is not None and getattr(websocket.client, "host", None):
            client_ip = str(websocket.client.host or "")
    except Exception:
        client_ip = ""
    user_agent = str(websocket.headers.get("user-agent") or "")
    auth_username = _resolve_auth_username(
        authorization=websocket.headers.get("authorization"),
        x_api_key=websocket.headers.get("x-api-key"),
        session_token=websocket.cookies.get(_SESSION_COOKIE_NAME),
    )
    err = _auth_error(
        authorization=websocket.headers.get("authorization"),
        x_api_key=websocket.headers.get("x-api-key"),
        session_token=websocket.cookies.get(_SESSION_COOKIE_NAME),
    )
    if err is not None:
        _db_append_ws_connection_event(
            owner="anonymous",
            event_type="auth_rejected",
            connection_id=connection_id,
            strategy_id=requested_strategy_id,
            config_path=str(config_path or ""),
            refresh_ms=requested_refresh,
            client_ip=client_ip,
            user_agent=user_agent,
            detail={"reason": str(err.detail)},
            ts_utc=_now_iso(),
        )
        await websocket.close(code=1008, reason=str(err.detail))
        return

    auth_user = _normalize_auth_username(auth_username)
    try:
        resolved_config = str(_resolve_config_path(config_path))
    except HTTPException as exc:
        _db_append_ws_connection_event(
            owner=auth_user,
            event_type="config_rejected",
            connection_id=connection_id,
            strategy_id=requested_strategy_id,
            config_path=str(config_path or ""),
            refresh_ms=requested_refresh,
            client_ip=client_ip,
            user_agent=user_agent,
            detail={"reason": str(exc.detail)},
            ts_utc=_now_iso(),
        )
        await websocket.close(code=1008, reason=str(exc.detail))
        return
    state = {
        "config_path": resolved_config,
        "strategy_id": requested_strategy_id,
        "auth_user": auth_user,
        "refresh_ms": requested_refresh,
        "next_send_ms": 0,
        "connection_id": connection_id,
        "client_ip": client_ip,
        "user_agent": user_agent,
    }

    await websocket.accept()
    _WS_CONNECTIONS[websocket] = state
    _record_ws_connection_event("connected", state)

    disconnect_detail: Dict[str, Any] = {"reason": "closed"}

    try:
        while True:
            try:
                # Wait for client message; timeout to keep connection alive
                msg = await asyncio.wait_for(websocket.receive_text(), timeout=30)
                if msg == "ping":
                    await websocket.send_text("pong")
                else:
                    _record_ws_connection_event(
                        "client_message",
                        state,
                        detail={"message": str(msg)[:200]},
                    )
            except asyncio.TimeoutError:
                # Heartbeat to prevent idle disconnects
                await websocket.send_text("ping")
    except WebSocketDisconnect:
        disconnect_detail = {"reason": "client_disconnect"}
    except Exception as exc:
        disconnect_detail = {"reason": "server_exception", "error": str(exc)}
    finally:
        current_state = _WS_CONNECTIONS.pop(websocket, None)
        state_for_event = current_state if isinstance(current_state, dict) else state
        _record_ws_connection_event("disconnected", state_for_event, detail=disconnect_detail)


@app.get("/")
def root() -> Dict[str, str]:
    return {"service": "quant-api", "docs": "/docs"}


@app.get("/api/health")
def health() -> Dict[str, Any]:
    db_status = _current_db_status()
    payload: Dict[str, Any] = {"status": "ok", "ts_utc": _now_iso(), "db": db_status}
    payload["db_backend"] = str(_DB_BACKEND)
    payload["db_path"] = str(_DB_PATH)
    payload["db_postgres_dsn"] = _mask_postgres_dsn(_DB_POSTGRES_DSN)
    if str(_DB_BACKEND) == "postgres":
        payload["db_postgres_pool"] = {
            "enabled": bool(getattr(_DB, "pool_enabled", False)),
            "supported": bool(getattr(_DB, "pool_supported", False)),
            "active": bool(getattr(_DB, "_pool", None) is not None),
            "min_size": int(getattr(_DB, "pool_min_size", 0) or 0),
            "max_size": int(getattr(_DB, "pool_max_size", 0) or 0),
            "timeout_seconds": float(getattr(_DB, "pool_timeout_seconds", 0.0) or 0.0),
        }
    payload["db_runtime_reload_supported"] = True
    if _DB_ENABLED and not _DB_READY and _DB_INIT_ERROR:
        payload["db_error"] = _DB_INIT_ERROR
    if _DB_ENABLED and _DB_READY:
        runtime_total = _db_runtime_failures_total()
        runtime_snapshot = _db_runtime_failure_snapshot()
        storage_stats, storage_err = _collect_db_storage_stats()
        if runtime_total > 0:
            payload["db_runtime_failures"] = runtime_total
            payload["db_runtime_failure_detail"] = runtime_snapshot
        payload["db_last_write_kind"] = str(runtime_snapshot.get("last_write_kind") or "")
        payload["db_last_write_ms"] = float(runtime_snapshot.get("last_write_ms") or 0.0)
        payload["db_last_write_at"] = str(runtime_snapshot.get("last_write_at") or "")
        payload["db_max_write_ms"] = float(runtime_snapshot.get("max_write_ms") or 0.0)
        payload["db_slow_op_threshold_ms"] = float(_DB_SLOW_OP_THRESHOLD_MS)
        payload["db_write_ops_total"] = int(runtime_snapshot.get("write_ops_total") or 0)
        payload["db_write_ops_slow_total"] = int(runtime_snapshot.get("write_ops_slow_total") or 0)
        payload["db_read_ops_total"] = int(runtime_snapshot.get("read_ops_total") or 0)
        payload["db_read_ops_slow_total"] = int(runtime_snapshot.get("read_ops_slow_total") or 0)
        payload["db_lock_contention_total"] = int(runtime_snapshot.get("lock_contention_total") or 0)
        payload["db_lock_wait_ms_total"] = float(runtime_snapshot.get("lock_wait_ms_total") or 0.0)
        payload["db_last_slow_kind"] = str(runtime_snapshot.get("last_slow_kind") or "")
        payload["db_last_slow_ms"] = float(runtime_snapshot.get("last_slow_ms") or 0.0)
        payload["db_last_slow_at"] = str(runtime_snapshot.get("last_slow_at") or "")
        if storage_stats:
            payload["db_storage"] = storage_stats
        if storage_err:
            payload["db_storage_error"] = storage_err
    outbox_worker = _DB_ALERT_OUTBOX_WORKER
    payload["db_alerting"] = {
        "enabled": bool(_DB_ALERT_WEBHOOK_URL),
        "threshold": int(_DB_ALERT_THRESHOLD),
        "cooldown_seconds": int(_DB_ALERT_COOLDOWN_SECONDS),
        "outbox_enabled": bool(_DB_ALERT_OUTBOX_ENABLED),
        "outbox_poll_seconds": float(_DB_ALERT_OUTBOX_POLL_SECONDS),
        "outbox_batch_size": int(_DB_ALERT_OUTBOX_BATCH_SIZE),
        "outbox_worker_alive": bool(outbox_worker is not None and outbox_worker.is_alive()),
    }
    return payload


@app.get("/api/metrics")
def metrics() -> Response:
    snapshot = _db_runtime_failure_snapshot()
    counters = _db_runtime_failure_counters(snapshot)
    total = _db_runtime_failures_total_from_stats(snapshot)
    storage_stats, _ = _collect_db_storage_stats()
    status = _current_db_status()
    db_states = ["disabled", "ok", "degraded", "error"]
    lines = [
        "# HELP quant_db_runtime_failures_total Total number of runtime DB persistence failures.",
        "# TYPE quant_db_runtime_failures_total counter",
        f"quant_db_runtime_failures_total {total}",
    ]
    for key in sorted(counters.keys()):
        kind = key.replace("_failures", "")
        lines.append(f'quant_db_runtime_failure_total{{kind="{kind}"}} {counters[key]}')
    lines.extend(
        [
            "# HELP quant_db_status Current DB status exposed by health endpoint.",
            "# TYPE quant_db_status gauge",
        ]
    )
    for state in db_states:
        lines.append(f'quant_db_status{{state="{state}"}} {1 if state == status else 0}')
    lines.extend(
        [
            "# HELP quant_db_backend Current configured DB backend.",
            "# TYPE quant_db_backend gauge",
        ]
    )
    for backend in ("postgres",):
        lines.append(f'quant_db_backend{{backend="{backend}"}} {1 if backend == str(_DB_BACKEND) else 0}')
    lines.extend(
        [
            "# HELP quant_db_postgres_pool_enabled Whether PostgreSQL connection pool is enabled (1/0).",
            "# TYPE quant_db_postgres_pool_enabled gauge",
            f"quant_db_postgres_pool_enabled {1 if bool(getattr(_DB, 'pool_enabled', False)) else 0}",
            "# HELP quant_db_postgres_pool_active Whether PostgreSQL connection pool is active (1/0).",
            "# TYPE quant_db_postgres_pool_active gauge",
            f"quant_db_postgres_pool_active {1 if bool(getattr(_DB, '_pool', None) is not None) else 0}",
            "# HELP quant_db_postgres_pool_min_size Configured PostgreSQL pool minimum size.",
            "# TYPE quant_db_postgres_pool_min_size gauge",
            f"quant_db_postgres_pool_min_size {int(getattr(_DB, 'pool_min_size', 0) or 0)}",
            "# HELP quant_db_postgres_pool_max_size Configured PostgreSQL pool maximum size.",
            "# TYPE quant_db_postgres_pool_max_size gauge",
            f"quant_db_postgres_pool_max_size {int(getattr(_DB, 'pool_max_size', 0) or 0)}",
        ]
    )
    lines.extend(
        [
            "# HELP quant_db_alerting_enabled Whether webhook alerting is enabled (1/0).",
            "# TYPE quant_db_alerting_enabled gauge",
            f"quant_db_alerting_enabled {1 if _DB_ALERT_WEBHOOK_URL else 0}",
            "# HELP quant_db_alert_outbox_enabled Whether alert outbox async delivery is enabled (1/0).",
            "# TYPE quant_db_alert_outbox_enabled gauge",
            f"quant_db_alert_outbox_enabled {1 if _DB_ALERT_OUTBOX_ENABLED else 0}",
            "# HELP quant_db_alert_outbox_worker_alive Whether alert outbox worker is running (1/0).",
            "# TYPE quant_db_alert_outbox_worker_alive gauge",
            f"quant_db_alert_outbox_worker_alive {1 if (_DB_ALERT_OUTBOX_WORKER is not None and _DB_ALERT_OUTBOX_WORKER.is_alive()) else 0}",
            "# HELP quant_db_alert_threshold Runtime DB failure threshold for alerting.",
            "# TYPE quant_db_alert_threshold gauge",
            f"quant_db_alert_threshold {_DB_ALERT_THRESHOLD}",
            "# HELP quant_db_alert_cooldown_seconds Alert cooldown window in seconds.",
            "# TYPE quant_db_alert_cooldown_seconds gauge",
            f"quant_db_alert_cooldown_seconds {_DB_ALERT_COOLDOWN_SECONDS}",
        ]
    )
    with _DB_RUNTIME_LOCK:
        last_alert_epoch = float(_DB_RUNTIME_ALERT_STATE.get("last_alert_epoch") or 0.0)
        last_alert_total = int(_DB_RUNTIME_ALERT_STATE.get("last_alert_total") or 0)
    lines.extend(
        [
            "# HELP quant_db_alert_last_sent_timestamp_seconds Last DB alert send unix timestamp.",
            "# TYPE quant_db_alert_last_sent_timestamp_seconds gauge",
            f"quant_db_alert_last_sent_timestamp_seconds {last_alert_epoch:.3f}",
            "# HELP quant_db_alert_last_sent_failure_total Runtime failure total at last alert.",
            "# TYPE quant_db_alert_last_sent_failure_total gauge",
            f"quant_db_alert_last_sent_failure_total {last_alert_total}",
            "# HELP quant_db_last_write_duration_ms Last successful DB write duration in milliseconds.",
            "# TYPE quant_db_last_write_duration_ms gauge",
            f"quant_db_last_write_duration_ms {float(snapshot.get('last_write_ms') or 0.0):.4f}",
            "# HELP quant_db_max_write_duration_ms Max successful DB write duration in milliseconds.",
            "# TYPE quant_db_max_write_duration_ms gauge",
            f"quant_db_max_write_duration_ms {float(snapshot.get('max_write_ms') or 0.0):.4f}",
            "# HELP quant_db_write_ops_total Total number of DB write operations (success + failure).",
            "# TYPE quant_db_write_ops_total counter",
            f"quant_db_write_ops_total {int(snapshot.get('write_ops_total') or 0)}",
            "# HELP quant_db_write_ops_slow_total Total number of DB write operations slower than threshold.",
            "# TYPE quant_db_write_ops_slow_total counter",
            f"quant_db_write_ops_slow_total {int(snapshot.get('write_ops_slow_total') or 0)}",
            "# HELP quant_db_read_ops_total Total number of DB read operations (success + failure).",
            "# TYPE quant_db_read_ops_total counter",
            f"quant_db_read_ops_total {int(snapshot.get('read_ops_total') or 0)}",
            "# HELP quant_db_read_ops_slow_total Total number of DB read operations slower than threshold.",
            "# TYPE quant_db_read_ops_slow_total counter",
            f"quant_db_read_ops_slow_total {int(snapshot.get('read_ops_slow_total') or 0)}",
            "# HELP quant_db_lock_contention_total Total number of DB lock contention errors.",
            "# TYPE quant_db_lock_contention_total counter",
            f"quant_db_lock_contention_total {int(snapshot.get('lock_contention_total') or 0)}",
            "# HELP quant_db_lock_wait_ms_total Estimated cumulative lock wait time in milliseconds.",
            "# TYPE quant_db_lock_wait_ms_total counter",
            f"quant_db_lock_wait_ms_total {float(snapshot.get('lock_wait_ms_total') or 0.0):.4f}",
            "# HELP quant_db_slow_op_threshold_ms Configured slow DB operation threshold in milliseconds.",
            "# TYPE quant_db_slow_op_threshold_ms gauge",
            f"quant_db_slow_op_threshold_ms {float(_DB_SLOW_OP_THRESHOLD_MS):.4f}",
            "# HELP quant_db_last_slow_duration_ms Last observed slow DB operation duration in milliseconds.",
            "# TYPE quant_db_last_slow_duration_ms gauge",
            f"quant_db_last_slow_duration_ms {float(snapshot.get('last_slow_ms') or 0.0):.4f}",
            "# HELP quant_db_last_slow_timestamp_seconds Last observed slow DB operation unix timestamp.",
            "# TYPE quant_db_last_slow_timestamp_seconds gauge",
            f"quant_db_last_slow_timestamp_seconds {_iso_datetime_to_epoch(str(snapshot.get('last_slow_at') or '')):.3f}",
        ]
    )
    if storage_stats:
        lines.extend(
            [
                "# HELP quant_db_size_bytes SQLite DB logical size in bytes.",
                "# TYPE quant_db_size_bytes gauge",
                f"quant_db_size_bytes {int(storage_stats.get('db_size_bytes') or 0)}",
                "# HELP quant_db_free_bytes SQLite DB freelist bytes.",
                "# TYPE quant_db_free_bytes gauge",
                f"quant_db_free_bytes {int(storage_stats.get('free_bytes') or 0)}",
                "# HELP quant_db_fragmentation_percent SQLite DB freelist ratio percentage.",
                "# TYPE quant_db_fragmentation_percent gauge",
                f"quant_db_fragmentation_percent {float(storage_stats.get('fragmentation_pct') or 0.0):.4f}",
            ]
        )
    return Response(content="\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")


@app.get("/api/admin/db/config")
def admin_db_config(request: Request) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        _require_permission("ops.admin.db")
        postgres_pool: Dict[str, Any] = {}
        if str(_DB_BACKEND) == "postgres":
            postgres_pool = {
                "enabled": bool(getattr(_DB, "pool_enabled", False)),
                "supported": bool(getattr(_DB, "pool_supported", False)),
                "active": bool(getattr(_DB, "_pool", None) is not None),
                "min_size": int(getattr(_DB, "pool_min_size", 0) or 0),
                "max_size": int(getattr(_DB, "pool_max_size", 0) or 0),
                "timeout_seconds": float(getattr(_DB, "pool_timeout_seconds", 0.0) or 0.0),
            }
        return {
            "enabled": bool(_DB_ENABLED),
            "ready": bool(_DB_READY),
            "backend": str(_DB_BACKEND),
            "db_path": str(_DB_PATH),
            "postgres_dsn": _mask_postgres_dsn(_DB_POSTGRES_DSN),
            "postgres_pool": postgres_pool,
            "db_error": str(_DB_INIT_ERROR or ""),
        }


@app.post("/api/admin/db/reload")
def admin_db_reload(payload: DbReloadRequest, request: Request) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        _require_permission("ops.admin.db")
        started = time.perf_counter()
        try:
            result = _reload_db_runtime(
                enabled=payload.enabled,
                db_path_text=payload.dbPath,
                backend=payload.backend,
                postgres_dsn=payload.postgresDsn,
                preserve_state=bool(payload.preserveState),
            )
            _record_db_write_success("db_reload_write", (time.perf_counter() - started) * 1000.0)
            return result
        except Exception as exc:
            _record_db_runtime_failure("db_reload_write", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
            raise HTTPException(status_code=500, detail=f"db reload failed: {exc}") from exc


@app.post("/api/auth/login")
def auth_login(payload: AuthLoginRequest, response: Response, request: Request) -> Dict[str, Any]:
    cred_err = _require_dashboard_credentials()
    if cred_err is not None:
        raise cred_err

    username = payload.username.strip()
    normalized_username = username.lower()
    password = payload.password
    client_ip = _extract_client_ip(request)
    user_agent = str(request.headers.get("user-agent") or "")
    retry_after = _login_rate_limit_check(username=username, client_ip=client_ip)
    if retry_after > 0:
        _audit_event(
            "auth.login.blocked",
            entity="auth",
            entity_id=username,
            detail={"client_ip": client_ip, "retry_after_seconds": retry_after},
            owner=username,
        )
        _db_record_login_attempt(
            username=username,
            client_ip=client_ip,
            success=False,
            reason="rate_limited",
        )
        _db_append_account_security_event(
            owner=normalized_username or username,
            event_type="login_blocked",
            severity="warn",
            message="login blocked by rate limit",
            detail={"client_ip": client_ip, "retry_after_seconds": retry_after},
        )
        raise HTTPException(status_code=429, detail=f"Too many login attempts. Retry after {retry_after}s")

    expected_password = _DASHBOARD_CREDENTIALS.get(username)
    if expected_password is None:
        expected_password = _DASHBOARD_CREDENTIALS.get(normalized_username)
    if not expected_password or not hmac.compare_digest(password, expected_password):
        _login_rate_limit_record_failure(username=username, client_ip=client_ip)
        _audit_event(
            "auth.login.failed",
            entity="auth",
            entity_id=username,
            detail={"client_ip": client_ip},
            owner=username,
        )
        _db_append_account_security_event(
            owner=normalized_username or username,
            event_type="login_failed",
            severity="warn",
            message="login failed: invalid credential",
            detail={"client_ip": client_ip},
        )
        raise HTTPException(status_code=401, detail="Invalid username or password")

    _login_rate_limit_reset(username=username, client_ip=client_ip)
    role = "admin" if _is_admin_username(username) else ("guest" if normalized_username == _GUEST_USERNAME.lower() else "user")
    _db_ensure_user(normalized_username, role=role)
    _db_upsert_user_credential(normalized_username, expected_password)
    _set_session_cookie(
        response,
        username=normalized_username,
        client_ip=client_ip,
        user_agent=user_agent,
    )
    _audit_event(
        "auth.login.success",
        entity="auth",
        entity_id=normalized_username,
        detail={"client_ip": client_ip},
        owner=normalized_username,
    )
    _db_append_account_security_event(
        owner=normalized_username,
        event_type="login_success",
        severity="info",
        message="login succeeded",
        detail={"client_ip": client_ip, "user_agent": user_agent},
    )
    return {"ok": True, "authenticated": True, "username": normalized_username}


@app.post("/api/auth/guest")
def auth_guest(response: Response, request: Request) -> Dict[str, Any]:
    guest_user = str(_GUEST_USERNAME or "guest").strip().lower() or "guest"
    client_ip = _extract_client_ip(request)
    user_agent = str(request.headers.get("user-agent") or "")
    _db_ensure_user(guest_user, role="guest")
    _set_session_cookie(
        response,
        username=guest_user,
        client_ip=client_ip,
        user_agent=user_agent,
    )
    _audit_event(
        "auth.guest.enter",
        entity="auth",
        entity_id=guest_user,
        detail={"client_ip": client_ip},
        owner=guest_user,
    )
    _db_append_account_security_event(
        owner=guest_user,
        event_type="guest_enter",
        severity="info",
        message="guest session entered",
        detail={"client_ip": client_ip, "user_agent": user_agent},
    )
    return {"ok": True, "authenticated": True, "username": guest_user, "guest": True}


@app.get("/api/auth/status")
def auth_status(request: Request) -> Dict[str, Any]:
    username = _validate_session_token(request.cookies.get(_SESSION_COOKIE_NAME))
    return {"authenticated": bool(username), "username": username or ""}


@app.post("/api/auth/logout")
def auth_logout(response: Response, request: Request) -> Dict[str, bool]:
    token = request.cookies.get(_SESSION_COOKIE_NAME)
    payload = _validate_session_token_payload(token)
    username = str((payload or {}).get("username") or _current_auth_username())
    session_id = str((payload or {}).get("session_id") or "")
    if session_id:
        _db_revoke_auth_session(session_id)
    _audit_event("auth.logout", entity="auth", entity_id=username, owner=username)
    _db_append_account_security_event(
        owner=username,
        event_type="logout",
        severity="info",
        message="logout succeeded",
        detail={"session_id": session_id},
    )
    _clear_session_cookie(response)
    return {"ok": True}


@app.get("/api/auth/security-events")
def auth_security_events(
    request: Request,
    owner: Optional[str] = Query(default=None),
    event_type: Optional[str] = Query(default=None),
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    cursor: Optional[int] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
) -> List[Dict[str, Any]]:
    with _auth_user_context(_request_auth_username(request)):
        owner_filter: Optional[str]
        if _has_permission("security.read.all"):
            owner_filter = _safe_user_key(owner) if isinstance(owner, str) and owner.strip() else None
        else:
            owner_filter = _record_owner_key()
        start_ts = _parse_iso_datetime_param(start, "start")
        end_ts = _parse_iso_datetime_param(end, "end")
        if start_ts and end_ts and start_ts > end_ts:
            raise HTTPException(status_code=422, detail="start must be <= end")
        safe_cursor: Optional[int] = None
        if isinstance(cursor, (int, float, str)):
            try:
                parsed_cursor = int(cursor)
            except Exception as exc:
                raise HTTPException(status_code=422, detail="cursor must be > 0") from exc
            if parsed_cursor <= 0:
                raise HTTPException(status_code=422, detail="cursor must be > 0")
            safe_cursor = parsed_cursor
        return _db_list_account_security_events(
            owner=owner_filter,
            event_type=str(event_type).strip() if isinstance(event_type, str) and event_type.strip() else None,
            start_ts=start_ts,
            end_ts=end_ts,
            cursor_id=safe_cursor,
            limit=limit,
        )


@app.post("/api/auth/tokens")
def auth_tokens_create(payload: ApiTokenCreateRequest, request: Request) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        _require_permission("auth.token.manage")
        target_owner_raw = str(payload.owner or _record_owner_key()).strip()
        if not target_owner_raw:
            raise HTTPException(status_code=422, detail="owner is required")
        target_owner = _safe_user_key(target_owner_raw)
        token_value = _generate_api_token_value()
        token_hash = _hash_api_token_value(token_value)
        token_row = _db_create_api_token(
            owner=target_owner,
            token_name=str(payload.tokenName or "").strip(),
            token_prefix=token_value[:12],
            token_hash=token_hash,
            scopes=[str(item or "").strip() for item in (payload.scopes or []) if str(item or "").strip()],
            expires_at=str(payload.expiresAt or "").strip(),
            created_by=_record_owner_key(),
        )
        if not isinstance(token_row, dict):
            raise HTTPException(status_code=500, detail="failed to create api token")
        _audit_event(
            "auth.token.create",
            entity="auth_token",
            entity_id=str(token_row.get("id") or ""),
            detail={"owner": target_owner, "scopes": token_row.get("scopes") or []},
            owner=_record_owner_key(),
        )
        _db_append_account_security_event(
            owner=target_owner,
            event_type="api_token_created",
            severity="info",
            message="api token created",
            detail={
                "token_id": int(token_row.get("id") or 0),
                "created_by": _record_owner_key(),
            },
        )
        return {
            "token": token_value,
            "meta": token_row,
        }


@app.get("/api/auth/tokens")
def auth_tokens_list(
    request: Request,
    owner: Optional[str] = Query(default=None),
    include_revoked: bool = Query(default=False),
    limit: int = Query(default=200, ge=1, le=2000),
) -> List[Dict[str, Any]]:
    with _auth_user_context(_request_auth_username(request)):
        owner_filter: Optional[str]
        if _has_permission("auth.token.manage"):
            owner_filter = _safe_user_key(owner) if isinstance(owner, str) and owner.strip() else None
        else:
            owner_filter = _record_owner_key()
        return _db_list_api_tokens(
            owner=owner_filter,
            include_revoked=include_revoked,
            limit=limit,
        )


@app.post("/api/auth/tokens/{token_id}/revoke")
def auth_token_revoke(token_id: int, request: Request) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        _require_permission("auth.token.manage")
        if int(token_id) <= 0:
            raise HTTPException(status_code=422, detail="token_id must be > 0")
        _db_revoke_api_token(token_id, revoked_by=_record_owner_key())
        _audit_event(
            "auth.token.revoke",
            entity="auth_token",
            entity_id=str(token_id),
            detail={"revoked_by": _record_owner_key()},
            owner=_record_owner_key(),
        )
        _db_append_account_security_event(
            owner=_record_owner_key(),
            event_type="api_token_revoked",
            severity="info",
            message="api token revoked",
            detail={"token_id": int(token_id)},
        )
        return {"ok": True, "tokenId": int(token_id)}


@app.get("/api/auth/roles")
def auth_roles(request: Request) -> List[Dict[str, Any]]:
    with _auth_user_context(_request_auth_username(request)):
        _require_permission("rbac.manage")
        return _db_list_roles()


@app.get("/api/auth/permissions")
def auth_permissions(request: Request) -> List[Dict[str, Any]]:
    with _auth_user_context(_request_auth_username(request)):
        _require_permission("rbac.manage")
        return _db_list_permissions()


@app.get("/api/auth/user-roles")
def auth_user_roles(
    request: Request,
    username: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        target_raw = str(username or _current_auth_username()).strip().lower()
        if not target_raw:
            raise HTTPException(status_code=422, detail="username is required")
        target_username = _safe_user_key(target_raw)
        if target_username != _record_owner_key() and not _has_permission("rbac.manage"):
            raise HTTPException(status_code=403, detail="permission denied: rbac.manage")
        return {
            "username": target_username,
            "roles": _db_list_user_roles(target_username),
        }


@app.put("/api/auth/user-roles")
def auth_user_roles_update(payload: UserRolesUpdateRequest, request: Request) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        _require_permission("rbac.manage")
        target_raw = str(payload.username or "").strip().lower()
        if not target_raw:
            raise HTTPException(status_code=422, detail="username is required")
        target_username = _safe_user_key(target_raw)
        try:
            roles = _db_replace_user_roles(target_username, payload.roles or [])
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        _audit_event(
            "auth.rbac.update",
            entity="auth_rbac",
            entity_id=target_username,
            detail={"roles": roles},
            owner=_record_owner_key(),
        )
        _db_append_account_security_event(
            owner=target_username,
            event_type="rbac_roles_updated",
            severity="info",
            message="rbac roles updated",
            detail={"roles": roles, "updated_by": _record_owner_key()},
        )
        return {
            "ok": True,
            "username": target_username,
            "roles": roles,
        }


@app.get("/api/user/preferences")
def user_preferences_get(request: Request) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        owner_key = _record_owner_key()
        payload = _db_get_user_preferences(owner_key)
        return payload


@app.put("/api/user/preferences")
def user_preferences_put(payload: UserPreferencesUpdateRequest, request: Request) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        owner_key = _record_owner_key()
        current = _db_get_user_preferences(owner_key)
        merged = deepcopy(current)
        try:
            incoming = payload.model_dump(exclude_unset=True)  # pydantic v2
        except AttributeError:
            incoming = payload.dict(exclude_unset=True)  # pragma: no cover - pydantic v1 fallback
        merged.update({k: v for k, v in incoming.items()})
        normalized = _normalized_user_preferences(merged)
        _db_upsert_user_preferences(owner_key, normalized)
        _audit_event(
            "user.preferences.update",
            entity="user_preferences",
            entity_id=owner_key,
            detail={"keys": sorted(list(incoming.keys()))},
            owner=owner_key,
        )
        return normalized


@app.get("/api/config")
def get_config(config_path: str = Query(default=_DEFAULT_CONFIG_PATH)) -> Dict[str, Any]:
    path = _resolve_config_path(config_path)
    if not _config_available(path):
        raise HTTPException(status_code=404, detail=f"config file not found: {path}")
    cfg = _load_config_with_db_fallback(path)
    _audit_event(
        "config.read",
        entity="config",
        entity_id=str(path),
        detail={"config_path": str(path)},
    )
    return {
        "config_path": str(path),
        "config": _redact_config(cfg.raw),
    }


@app.get("/api/market/ticks")
def market_ticks(
    config_path: str = Query(default=_DEFAULT_CONFIG_PATH),
    refresh_ms: int = Query(default=1000, ge=200, le=10000),
) -> Dict[str, Any]:
    return _resolve_market_ticks_payload(config_path=config_path, refresh_ms=refresh_ms)


@app.get("/api/market/klines")
def market_klines(
    symbol: str = Query(..., min_length=1),
    config_path: str = Query(default=_DEFAULT_CONFIG_PATH),
    timeframe: str = Query(default="15m"),
    lookback_hours: int = Query(default=24, ge=1, le=24 * 7),
) -> Dict[str, Any]:
    return _resolve_market_klines_payload(
        config_path=config_path,
        symbol=symbol,
        timeframe=timeframe,
        lookback_hours=lookback_hours,
    )


def _start_strategy_impl(payload: StrategyStartRequest) -> Dict[str, Any]:
    config_path = _resolve_config_path(payload.config_path)
    if not _config_available(config_path):
        raise HTTPException(status_code=404, detail=f"config file not found: {config_path}")

    requested_strategy_id = payload.strategy_id or _strategy_id_from_config_path(str(config_path))
    if not requested_strategy_id:
        requested_strategy_id = f"strategy_{config_path.stem}"
    if not _is_admin_username() and (
        requested_strategy_id == _DEFAULT_STRATEGY_ID or requested_strategy_id in _PRESET_STRATEGIES
    ):
        raise HTTPException(status_code=404, detail=f"strategy not found: {requested_strategy_id}")

    external_rows = _external_strategy_processes_for_strategy(requested_strategy_id)
    if external_rows:
        _sync_strategy_store_statuses()
        _persist_current_user_strategies()
        _audit_event(
            "strategy.start.already_running",
            entity="strategy",
            entity_id=requested_strategy_id,
            detail={"external": True, "config_path": str(config_path)},
        )
        return {
            "ok": True,
            "status": _strategy_status(requested_strategy_id),
            "strategy_id": requested_strategy_id,
            "already_running": True,
            "external": True,
        }

    scoped_strategy_id = _scoped_strategy_id(requested_strategy_id)
    runner = _get_strategy_runner(requested_strategy_id, create=True)
    if runner is None:
        raise HTTPException(status_code=500, detail="failed to allocate strategy runner")

    running_status = runner.status()
    running_now = bool(running_status.get("running", False))
    running_meta = runner.metadata()
    running_base_config = str(
        running_meta.get("base_config_path")
        or running_meta.get("config_path")
        or ""
    )
    if running_now and running_base_config:
        try:
            if _resolve_path(running_base_config) == config_path:
                _sync_strategy_store_statuses()
                running_status["name"] = f"strategy:{requested_strategy_id}"
                running_status["metadata"] = dict(running_meta or {})
                running_status["metadata"]["strategy_id"] = requested_strategy_id
                _audit_event(
                    "strategy.start.already_running",
                    entity="strategy",
                    entity_id=requested_strategy_id,
                    detail={"external": False, "config_path": str(config_path)},
                )
                return {
                    "ok": True,
                    "status": running_status,
                    "strategy_id": requested_strategy_id,
                    "already_running": True,
                }
        except Exception:
            pass

    runtime_config_path = _create_strategy_runtime_config(config_path, scoped_strategy_id)
    runtime_cfg = _load_config_with_db_fallback(runtime_config_path)
    runtime_diag_cfg = runtime_cfg.raw.get("diagnostics", {})
    if not isinstance(runtime_diag_cfg, dict):
        runtime_diag_cfg = {}
    diagnostics_path = runtime_diag_cfg.get(
        "snapshot_path",
        str(LOG_DIR / "diagnostics" / f"{_safe_strategy_id(scoped_strategy_id)}.json"),
    )
    exceptions_path = runtime_diag_cfg.get(
        "exceptions_path",
        str(LOG_DIR / "diagnostics" / f"{_safe_strategy_id(scoped_strategy_id)}_exceptions.jsonl"),
    )

    command = [
        sys.executable,
        str(PROJECT_ROOT / "main.py"),
        "--config",
        str(runtime_config_path),
    ]
    metadata: Dict[str, Any] = {
        "config_path": str(runtime_config_path),
        "base_config_path": str(config_path),
        "paper_log_path": _paper_log_path_for_strategy(scoped_strategy_id),
        "diagnostics_path": str(_resolve_path(str(diagnostics_path))),
        "exceptions_path": str(_resolve_path(str(exceptions_path))),
    }
    metadata["strategy_id"] = requested_strategy_id
    metadata["owner"] = _record_owner_key()
    try:
        status = runner.start(
            command=command,
            cwd=PROJECT_ROOT,
            metadata=metadata,
        )
    except RuntimeError as exc:
        fallback_status = runner.status()
        if bool(fallback_status.get("running", False)):
            fallback_status["name"] = f"strategy:{requested_strategy_id}"
            fallback_meta = runner.metadata()
            fallback_status["metadata"] = dict(fallback_meta or {})
            fallback_status["metadata"]["strategy_id"] = requested_strategy_id
            _sync_strategy_store_statuses()
            _persist_current_user_strategies()
            _audit_event(
                "strategy.start.already_running",
                entity="strategy",
                entity_id=requested_strategy_id,
                detail={"external": False, "config_path": str(config_path), "runtime_error": str(exc)},
            )
            return {
                "ok": True,
                "status": fallback_status,
                "strategy_id": requested_strategy_id,
                "already_running": True,
            }
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    _ensure_default_strategy()
    _sync_strategy_store_statuses()
    _persist_current_user_strategies()
    _audit_event(
        "strategy.start",
        entity="strategy",
        entity_id=requested_strategy_id,
        detail={
            "config_path": str(config_path),
            "runtime_config_path": str(runtime_config_path),
            "pid": status.get("pid"),
        },
    )
    return {"ok": True, "status": status, "strategy_id": requested_strategy_id}


@app.post("/api/strategy/start")
def start_strategy(payload: StrategyStartRequest, request: Request) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        _require_permission("strategy.execute")
        return _start_strategy_impl(payload)


@app.get("/api/strategies")
def list_strategies(request: Request) -> List[Dict[str, Any]]:
    with _auth_user_context(_request_auth_username(request)):
        _ensure_preset_strategies()
        strategies: List[Dict[str, Any]] = []
        _sync_strategy_store_statuses()

        if _is_admin_username():
            for strategy_id in _PRESET_STRATEGIES:
                strategy = _strategy_store_get(strategy_id)
                if strategy is None:
                    continue
                strategies.append(strategy)

        current_user = _current_auth_username()
        for strategy_key, strategy in _STRATEGY_STORE.items():
            if not _strategy_record_visible_to_user(strategy, current_user):
                continue
            strategy_id = str(strategy.get("id") or _unscoped_strategy_id(strategy_key, current_user))
            if strategy_id in _PRESET_STRATEGIES or strategy_id == _DEFAULT_STRATEGY_ID:
                continue
            strategies.append(strategy)

        return strategies


@app.post("/api/strategies")
def create_strategy(payload: StrategyCreateRequest, request: Request) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        now = _now_iso()
        strategy_id = (
            f"strategy_{int(time.time() * 1000)}"
            if payload.name
            else f"strategy_{int(time.time() * 1000)}"
        )
        strategy = {
            "id": strategy_id,
            "name": payload.name,
            "type": payload.type if payload.type in {"mean_reversion", "trend_following", "market_making", "custom"} else "custom",
            "status": "stopped",
            "config": {
                "symbols": payload.config.get("symbols", ["BTC/USDT:USDT"]),
                "timeframe": _normalize_timeframe(str(payload.config.get("timeframe", "1h"))),
                "params": payload.config.get("params", {}),
            },
            "createdAt": now,
            "updatedAt": now,
            "owner": _record_owner_key(),
        }
        strategy_key = _strategy_store_key(strategy_id)
        _STRATEGY_STORE[strategy_key] = strategy
        _persist_strategy_record(strategy_key, strategy)
        compile_job: Optional[Dict[str, Any]] = None
        try:
            compile_job = _enqueue_strategy_compile(strategy_key, str(strategy.get("owner") or _record_owner_key()))
        except Exception as exc:
            _audit_event(
                "strategy.compile.enqueue_failed",
                entity="strategy",
                entity_id=strategy_id,
                detail={"error": str(exc)},
            )
        _audit_event(
            "strategy.create",
            entity="strategy",
            entity_id=strategy_id,
            detail={
                "name": payload.name,
                "type": strategy.get("type", "custom"),
                "compile_job_id": int((compile_job or {}).get("id") or 0),
            },
        )
        if isinstance(compile_job, dict):
            strategy["compileJob"] = compile_job
        return strategy


@app.get("/api/strategies/{id}")
def get_strategy(id: str, request: Request) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        if not _is_admin_username() and (id == _DEFAULT_STRATEGY_ID or id in _PRESET_STRATEGIES):
            raise HTTPException(status_code=404, detail=f"strategy not found: {id}")
        if id == _DEFAULT_STRATEGY_ID:
            return _ensure_default_strategy()
        if id in _PRESET_STRATEGIES:
            _ensure_preset_strategies()
        strategy = _strategy_store_get(id)
        if not strategy:
            raise HTTPException(status_code=404, detail=f"strategy not found: {id}")
        _sync_strategy_store_statuses()
        return strategy


@app.put("/api/strategies/{id}")
def update_strategy(id: str, payload: StrategyCreateRequest, request: Request) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        if not _is_admin_username() and (id == _DEFAULT_STRATEGY_ID or id in _PRESET_STRATEGIES):
            raise HTTPException(status_code=404, detail=f"strategy not found: {id}")
        if id == _DEFAULT_STRATEGY_ID:
            strategy = _ensure_default_strategy()
        else:
            strategy = _strategy_store_get(id)
            if not strategy:
                raise HTTPException(status_code=404, detail=f"strategy not found: {id}")

        strategy["name"] = payload.name
        strategy["type"] = payload.type if payload.type in {"mean_reversion", "trend_following", "market_making", "custom"} else strategy.get("type", "custom")
        strategy["config"] = {
            "symbols": payload.config.get("symbols", strategy["config"]["symbols"]),
            "timeframe": _normalize_timeframe(str(payload.config.get("timeframe", strategy["config"]["timeframe"]))),
            "params": payload.config.get("params", strategy["config"].get("params", {})),
        }
        strategy["updatedAt"] = _now_iso()
        strategy_key = _strategy_store_key(id)
        _STRATEGY_STORE[strategy_key] = strategy
        _persist_strategy_record(strategy_key, strategy)
        compile_job: Optional[Dict[str, Any]] = None
        try:
            compile_job = _enqueue_strategy_compile(strategy_key, str(strategy.get("owner") or _record_owner_key()))
        except Exception as exc:
            _audit_event(
                "strategy.compile.enqueue_failed",
                entity="strategy",
                entity_id=id,
                detail={"error": str(exc), "reason": "update"},
            )
        _audit_event(
            "strategy.update",
            entity="strategy",
            entity_id=id,
            detail={
                "name": payload.name,
                "type": strategy.get("type", "custom"),
                "compile_job_id": int((compile_job or {}).get("id") or 0),
            },
        )
        if isinstance(compile_job, dict):
            strategy["compileJob"] = compile_job
        return strategy


@app.post("/api/strategies/{id}/compile")
def compile_strategy(id: str, request: Request) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        if not _is_admin_username() and (id == _DEFAULT_STRATEGY_ID or id in _PRESET_STRATEGIES):
            raise HTTPException(status_code=404, detail=f"strategy not found: {id}")
        strategy = _strategy_store_get(id)
        if not strategy:
            raise HTTPException(status_code=404, detail=f"strategy not found: {id}")
        strategy_key = _strategy_store_key(id)
        owner_key = str(strategy.get("owner") or _record_owner_key()).strip() or _record_owner_key()
        job = _enqueue_strategy_compile(strategy_key, owner_key)
        _audit_event(
            "strategy.compile.enqueue",
            entity="strategy",
            entity_id=id,
            detail={"job_id": int(job.get("id") or 0)},
        )
        return {
            "ok": True,
            "strategy_id": id,
            "job": job,
        }


@app.get("/api/strategies/{id}/scripts")
def strategy_scripts(id: str, request: Request, limit: int = Query(default=20, ge=1, le=200)) -> List[Dict[str, Any]]:
    with _auth_user_context(_request_auth_username(request)):
        if not _is_admin_username() and (id == _DEFAULT_STRATEGY_ID or id in _PRESET_STRATEGIES):
            raise HTTPException(status_code=404, detail=f"strategy not found: {id}")
        strategy = _strategy_store_get(id)
        if not strategy:
            raise HTTPException(status_code=404, detail=f"strategy not found: {id}")
        strategy_key = _strategy_store_key(id)
        owner_key = str(strategy.get("owner") or _record_owner_key()).strip() or _record_owner_key()
        rows = _db_list_strategy_scripts(
            owner=_safe_user_key(owner_key),
            strategy_key=strategy_key,
            limit=limit,
        )
        if rows:
            return rows
        fallback_path = str(strategy.get("_compiled_script_path") or "").strip()
        if not fallback_path:
            return []
        return [
            {
                "id": 0,
                "strategyKey": strategy_key,
                "owner": owner_key,
                "version": int(strategy.get("_compiled_script_version") or 0),
                "scriptType": "yaml_config",
                "scriptPath": fallback_path,
                "scriptHash": "",
                "sourceConfig": {},
                "createdAt": str(strategy.get("updatedAt") or ""),
            }
        ]


@app.delete("/api/strategies/{id}")
def delete_strategy(id: str, request: Request) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        if not _is_admin_username() and (id == _DEFAULT_STRATEGY_ID or id in _PRESET_STRATEGIES):
            raise HTTPException(status_code=404, detail=f"strategy not found: {id}")
        if id == _DEFAULT_STRATEGY_ID or id in _PRESET_STRATEGIES:
            raise HTTPException(status_code=409, detail=f"built-in strategy cannot be deleted: {id}")

        strategy = _strategy_store_get(id)
        if not strategy:
            raise HTTPException(status_code=404, detail=f"strategy not found: {id}")

        owner = str(strategy.get("owner") or _record_owner_key())
        strategy_key = _strategy_store_key(id)

        runner = _get_strategy_runner(id, create=False)
        stopped_runner = runner.stop() if runner is not None else False
        stopped_external = _terminate_external_strategy_processes(
            strategy_id=id,
            username=_current_auth_username(),
        )
        external_pids = stopped_external.get(id, [])
        stopped = bool(stopped_runner or external_pids)

        with _STRATEGY_RUNNERS_LOCK:
            _STRATEGY_RUNNERS.pop(strategy_key, None)
            _STRATEGY_RUNNERS.pop(id, None)

        _STRATEGY_STORE.pop(strategy_key, None)
        _STRATEGY_STORE.pop(id, None)
        _delete_strategy_record(strategy_key)
        if strategy_key != id:
            _delete_strategy_record(id)

        risk_key = _scoped_strategy_id(id)
        _RISK_STATE_STORE.pop(risk_key, None)
        _delete_risk_state(risk_key, owner=owner)

        _audit_event(
            "strategy.delete",
            entity="strategy",
            entity_id=id,
            detail={"stopped": stopped, "external_stopped_pids": external_pids},
        )
        return {
            "ok": True,
            "deleted": True,
            "strategy_id": id,
            "stopped": stopped,
            "external_stopped_pids": external_pids,
        }


@app.post("/api/strategies/{id}/start")
def start_strategy_compat(id: str, request: Request) -> Dict[str, Any]:
    # Compatibility endpoint for legacy/standard frontend contract.
    with _auth_user_context(_request_auth_username(request)):
        _require_permission("strategy.execute")
        if not _is_admin_username() and (id == _DEFAULT_STRATEGY_ID or id in _PRESET_STRATEGIES):
            raise HTTPException(status_code=404, detail=f"strategy not found: {id}")
        _ensure_default_strategy()
        strategy = _strategy_store_get(id)
        if id != _DEFAULT_STRATEGY_ID and strategy is None:
            raise HTTPException(status_code=404, detail=f"strategy not found: {id}")
        if id != _DEFAULT_STRATEGY_ID and id not in _PRESET_STRATEGIES and isinstance(strategy, dict):
            owner_key = str(strategy.get("owner") or _record_owner_key()).strip() or _record_owner_key()
            strategy_key = _strategy_store_key(id)
            compiled_path = _latest_compiled_script_path(strategy_key, owner_key)
            if not compiled_path:
                try:
                    script = _compile_strategy_now(strategy_key, owner_key)
                    compiled_path = str(script.get("scriptPath") or "").strip()
                except Exception as exc:
                    _audit_event(
                        "strategy.compile.sync_failed",
                        entity="strategy",
                        entity_id=id,
                        detail={"error": str(exc)},
                    )
        config_path = _config_path_for_strategy_id(id)
        return _start_strategy_impl(StrategyStartRequest(config_path=config_path, strategy_id=id))


@app.post("/api/strategy/stop")
def stop_strategy(request: Request, strategy_id: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        _require_permission("strategy.execute")
        if strategy_id:
            runner = _get_strategy_runner(strategy_id, create=False)
            stopped_runner = runner.stop() if runner is not None else False
            stopped_external = _terminate_external_strategy_processes(
                strategy_id=strategy_id,
                username=_current_auth_username(),
            )
            external_pids = stopped_external.get(strategy_id, [])
            stopped = stopped_runner or bool(external_pids)
            status = _strategy_status(strategy_id)
            _sync_strategy_store_statuses()
            _persist_current_user_strategies()
            _audit_event(
                "strategy.stop",
                entity="strategy",
                entity_id=strategy_id,
                detail={"stopped": stopped, "external_stopped_pids": external_pids},
            )
            return {
                "stopped": stopped,
                "status": status,
                "strategy_id": strategy_id,
                "external_stopped_pids": external_pids,
            }

        stopped_ids: List[str] = []
        for sid, runner in _all_strategy_runners().items():
            if runner.stop():
                stopped_ids.append(sid)
        stopped_external = _terminate_external_strategy_processes(username=_current_auth_username())
        external_stopped_count = sum(len(rows) for rows in stopped_external.values())
        for sid, rows in stopped_external.items():
            if rows and sid not in stopped_ids:
                stopped_ids.append(sid)
        _sync_strategy_store_statuses()
        _persist_current_user_strategies()
        _audit_event(
            "strategy.stop.batch",
            entity="strategy",
            entity_id="*",
            detail={
                "stopped_count": len(stopped_ids),
                "strategy_ids": stopped_ids,
                "external_stopped": stopped_external,
            },
        )
        return {
            "stopped": len(stopped_ids) > 0,
            "stopped_count": len(stopped_ids),
            "strategy_ids": stopped_ids,
            "external_stopped_count": external_stopped_count,
            "external_stopped": stopped_external,
        }


@app.post("/api/strategies/{id}/stop")
def stop_strategy_compat(id: str, request: Request) -> Dict[str, Any]:
    # Compatibility endpoint for legacy/standard frontend contract.
    with _auth_user_context(_request_auth_username(request)):
        _require_permission("strategy.execute")
        if not _is_admin_username() and (id == _DEFAULT_STRATEGY_ID or id in _PRESET_STRATEGIES):
            raise HTTPException(status_code=404, detail=f"strategy not found: {id}")
        _ensure_default_strategy()
        if id != _DEFAULT_STRATEGY_ID and _strategy_store_get(id) is None:
            raise HTTPException(status_code=404, detail=f"strategy not found: {id}")
        runner = _get_strategy_runner(id, create=False)
        stopped_runner = runner.stop() if runner is not None else False
        stopped_external = _terminate_external_strategy_processes(
            strategy_id=id,
            username=_current_auth_username(),
        )
        external_pids = stopped_external.get(id, [])
        stopped = stopped_runner or bool(external_pids)
        if not stopped:
            _sync_strategy_store_statuses()
            _persist_current_user_strategies()
            _audit_event(
                "strategy.stop",
                entity="strategy",
                entity_id=id,
                detail={"stopped": False, "external_stopped_pids": []},
            )
            return {
                "stopped": False,
                "status": _strategy_status(id),
                "ok": False,
                "strategy_id": id,
                "detail": f"strategy {id} is not running",
                "external_stopped_pids": [],
            }
        _sync_strategy_store_statuses()
        _persist_current_user_strategies()
        _audit_event(
            "strategy.stop",
            entity="strategy",
            entity_id=id,
            detail={"stopped": True, "external_stopped_pids": external_pids},
        )
        return {
            "stopped": stopped,
            "status": _strategy_status(id),
            "ok": stopped,
            "strategy_id": id,
            "external_stopped_pids": external_pids,
        }


@app.get("/api/strategy/status")
def strategy_status(
    request: Request,
    log_limit: int = Query(default=80, ge=0, le=1000),
    strategy_id: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        if strategy_id:
            return _strategy_status(strategy_id, log_limit=log_limit)

        target = _current_running_strategy_id() or _DEFAULT_STRATEGY_ID
        status = _strategy_status(target, log_limit=log_limit)
        instances: List[Dict[str, Any]] = []
        runner_ids = set(_all_strategy_runners().keys())
        external_ids = set(_visible_external_strategy_processes(_current_auth_username()).keys())
        for sid in sorted(runner_ids | external_ids):
            item = _strategy_status(sid, log_limit=0)
            item["strategy_id"] = sid
            instances.append(item)
        status["instances"] = instances
        status["strategy_id"] = target
        return status


@app.get("/api/strategy/logs")
def strategy_logs(
    request: Request,
    limit: int = Query(default=200, ge=1, le=2000),
    strategy_id: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        if strategy_id:
            rows = []
            for row in _tail_strategy_logs(strategy_id, limit):
                item = dict(row)
                item["strategy_id"] = strategy_id
                rows.append(item)
            return {"logs": rows}
        return {"logs": _tail_strategy_logs_all(limit_per_strategy=limit)[:limit]}


@app.get("/api/strategy/diagnostics")
def strategy_diagnostics(
    request: Request,
    strategy_id: Optional[str] = Query(default=None),
    path: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        diag_path = _resolve_strategy_diagnostics_path(strategy_id=strategy_id, path_override=path)
        resolved_strategy_id = str(strategy_id or _current_running_strategy_id() or _DEFAULT_STRATEGY_ID)
        diag_key = f"{_safe_strategy_id(resolved_strategy_id)}:{_project_relative_path_text(diag_path)}"

        payload_text: Optional[str] = None
        stat_size = 0
        updated_at = _now_iso()
        if diag_path.exists():
            payload_text = _sync_text_file_to_db(
                owner=_record_owner_key(),
                scope="strategy_diagnostics_json",
                file_key=diag_key,
                path=diag_path,
                content_type="application/json",
                meta={"kind": "strategy_diagnostics", "strategy_id": resolved_strategy_id, "path": str(diag_path)},
            )
            stat = diag_path.stat()
            stat_size = int(stat.st_size)
            updated_at = datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat()
        else:
            diag_row = _db_get_data_file(
                owner=_record_owner_key(),
                scope="strategy_diagnostics_json",
                file_key=diag_key,
            )
            payload_text = _decode_data_file_text(diag_row)
            if payload_text is not None:
                stat_size = len(payload_text.encode("utf-8"))
                updated_at = str((diag_row or {}).get("updatedAt") or _now_iso())
        if payload_text is None:
            raise HTTPException(status_code=404, detail=f"diagnostics snapshot not found: {diag_path}")

        try:
            payload = json.loads(payload_text)
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"failed to parse diagnostics snapshot: {exc}") from exc
        _db_append_strategy_diagnostics_snapshot(
            owner=_record_owner_key(),
            strategy_id=resolved_strategy_id,
            source_path=str(diag_path),
            snapshot=payload if isinstance(payload, dict) else {"raw": payload},
            ts_utc=updated_at,
        )
        return {
            "strategy_id": resolved_strategy_id,
            "path": str(diag_path),
            "size_bytes": stat_size,
            "updated_at": updated_at,
            "snapshot": payload,
        }


@app.get("/api/strategy/diagnostics/history")
def strategy_diagnostics_history(
    request: Request,
    strategy_id: Optional[str] = Query(default=None),
    owner: Optional[str] = Query(default=None),
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    cursor: Optional[int] = Query(default=None),
    include_snapshot: bool = Query(default=False),
    limit: int = Query(default=200, ge=1, le=2000),
) -> List[Dict[str, Any]]:
    with _auth_user_context(_request_auth_username(request)):
        if not _db_is_enabled():
            return []

        owner_filter: Optional[str]
        if _is_admin_username():
            owner_filter = _safe_user_key(owner) if isinstance(owner, str) and owner.strip() else None
        else:
            owner_filter = _record_owner_key()

        strategy_filter = strategy_id.strip() if isinstance(strategy_id, str) and strategy_id.strip() else None
        start_ts = _parse_iso_datetime_param(start, "start")
        end_ts = _parse_iso_datetime_param(end, "end")
        if start_ts and end_ts and start_ts > end_ts:
            raise HTTPException(status_code=422, detail="start must be <= end")

        safe_cursor: Optional[int] = None
        if isinstance(cursor, (int, float, str)):
            try:
                parsed_cursor = int(cursor)
            except Exception as exc:
                raise HTTPException(status_code=422, detail="cursor must be > 0") from exc
            if parsed_cursor <= 0:
                raise HTTPException(status_code=422, detail="cursor must be > 0")
            safe_cursor = parsed_cursor

        include_snapshot_flag = False
        if isinstance(include_snapshot, bool):
            include_snapshot_flag = include_snapshot
        elif isinstance(include_snapshot, (int, float)):
            include_snapshot_flag = bool(int(include_snapshot))
        elif isinstance(include_snapshot, str):
            include_snapshot_flag = include_snapshot.strip().lower() in {"1", "true", "yes", "y", "on"}

        return _db_list_strategy_diagnostics_snapshots(
            owner=owner_filter,
            strategy_id=strategy_filter,
            start_ts=start_ts,
            end_ts=end_ts,
            cursor_id=safe_cursor,
            limit=limit,
            include_snapshot=include_snapshot_flag,
        )


def _start_backtest_impl(
    payload: BacktestStartRequest,
    *,
    request_fingerprint: Optional[str] = None,
) -> Dict[str, Any]:
    start_dt = _parse_yyyy_mm_dd(payload.start, "start")
    end_dt = _parse_yyyy_mm_dd(payload.end, "end")
    if end_dt <= start_dt:
        raise HTTPException(status_code=422, detail="end must be later than start")

    config_path = _resolve_config_path(payload.config_path)
    if not _config_available(config_path):
        raise HTTPException(status_code=404, detail=f"config file not found: {config_path}")

    runner = _get_backtest_runner(create=True)
    if runner is None:
        raise HTTPException(status_code=500, detail="failed to allocate backtest runner")
    running_status = runner.status()
    running_metadata = runner.metadata()
    if bool(running_status.get("running", False)) and _backtest_metadata_matches_request(
        running_metadata,
        start=payload.start,
        end=payload.end,
        config_path=config_path,
        request_fingerprint=request_fingerprint,
    ):
        _audit_event(
            "backtest.start.already_running",
            entity="backtest",
            entity_id=str(running_metadata.get("run_id") or ""),
            detail={
                "start": payload.start,
                "end": payload.end,
                "config_path": str(config_path),
            },
        )
        return _build_running_backtest_response(runner, running_metadata)

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    out_prefix = LOG_DIR / f"api_backtest_{run_id}"
    artifacts = {
        "equity_csv": str(out_prefix.with_name(out_prefix.name + "_equity.csv")),
        "trades_csv": str(out_prefix.with_name(out_prefix.name + "_trades.csv")),
        "metrics_txt": str(out_prefix.with_name(out_prefix.name + "_metrics.txt")),
        "equity_plot": str(out_prefix.with_name(out_prefix.name + "_equity.png")),
    }

    command = [
        sys.executable,
        "-m",
        "statarb.backtest",
        "--start",
        payload.start,
        "--end",
        payload.end,
        "--config",
        str(config_path),
        "--out",
        artifacts["equity_csv"],
        "--trades",
        artifacts["trades_csv"],
        "--metrics",
        artifacts["metrics_txt"],
        "--plot",
        artifacts["equity_plot"],
    ]
    metadata = {
        "run_id": run_id,
        "start": payload.start,
        "end": payload.end,
        "config_path": str(config_path),
        "artifacts": artifacts,
        "owner": _record_owner_key(),
    }
    if request_fingerprint:
        metadata["request_fingerprint"] = str(request_fingerprint)
    try:
        status = runner.start(
            command=command,
            cwd=PROJECT_ROOT,
            metadata=metadata,
        )
    except RuntimeError as exc:
        fallback_status = runner.status()
        fallback_metadata = runner.metadata()
        if bool(fallback_status.get("running", False)) and _backtest_metadata_matches_request(
            fallback_metadata,
            start=payload.start,
            end=payload.end,
            config_path=config_path,
            request_fingerprint=request_fingerprint,
        ):
            _audit_event(
                "backtest.start.already_running",
                entity="backtest",
                entity_id=str(fallback_metadata.get("run_id") or ""),
                detail={
                    "start": payload.start,
                    "end": payload.end,
                    "config_path": str(config_path),
                    "runtime_error": str(exc),
                },
            )
            return _build_running_backtest_response(runner, fallback_metadata)
        raise HTTPException(status_code=409, detail=str(exc)) from exc

    status["run_id"] = run_id
    status["artifacts"] = artifacts
    default_cfg = _load_config_with_db_fallback(config_path)
    default_capital = _to_float_or_default(default_cfg.raw.get("paper_equity_usdt"), 0.0)
    _BACKTEST_STORE[run_id] = _build_backtest_record(
        run_id=run_id,
        payload={"strategyId": _ensure_default_strategy().get("id"), "symbol": default_cfg.symbols[0] if default_cfg.symbols else "UNKNOWN"},
        start=payload.start,
        end=payload.end,
        initial_capital=default_capital,
        fee_rate=_to_float_or_default(default_cfg.raw.get("portfolio", {}).get("fee_bps"), 0.0) / 10000.0,
        slippage=_to_float_or_default(default_cfg.raw.get("portfolio", {}).get("slippage_bps"), 0.0) / 10000.0,
    )
    _BACKTEST_STORE[run_id]["status"] = _normalize_backtest_status(
        running=status.get("running", False),
        return_code=status.get("return_code"),
    )
    _BACKTEST_STORE[run_id]["updatedAt"] = _now_iso()
    _persist_backtest_record(run_id, _BACKTEST_STORE[run_id])
    _audit_event(
        "backtest.start",
        entity="backtest",
        entity_id=run_id,
        detail={
            "start": payload.start,
            "end": payload.end,
            "config_path": str(config_path),
            "artifacts": artifacts,
        },
    )
    return status


@app.post("/api/backtest/start")
def start_backtest(payload: BacktestStartRequest, request: Request) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        return _start_backtest_impl(payload)


@app.get("/api/backtests")
def list_backtests(request: Request) -> List[Dict[str, Any]]:
    with _auth_user_context(_request_auth_username(request)):
        _sync_backtest_status_everywhere()
        _ensure_default_strategy()
        running_records = [*_collect_backtest_records()]
        return running_records


@app.post("/api/backtests")
def create_backtest(payload: BacktestCreateRequest, request: Request) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        # Convert modern client payload (camelCase + fee/slip rates) to backtest CLI args.
        strategy_id = payload.strategyId or _DEFAULT_STRATEGY_ID
        config_path = _resolve_config_path(_config_path_for_strategy_id(strategy_id))
        if not _config_available(config_path):
            raise HTTPException(status_code=404, detail=f"config file not found: {config_path}")

        strategy_cfg = _load_config_with_db_fallback(config_path)
        allowed_symbols = strategy_cfg.symbols or []
        symbol = payload.symbol or (allowed_symbols[0] if allowed_symbols else "UNKNOWN")
        symbol = _canonicalize_symbol(symbol, allowed_symbols)
        if allowed_symbols and symbol not in allowed_symbols:
            raise HTTPException(status_code=422, detail=f"symbol must be one of: {', '.join(allowed_symbols)}")

        start = _to_iso_date_only(payload.startAt)
        end = _to_iso_date_only(payload.endAt)
        if start is None or end is None:
            raise HTTPException(status_code=422, detail="startAt/endAt must use YYYY-MM-DD")

        owner = _record_owner_key()
        initial_capital = float(payload.initialCapital)
        fee_rate = float(payload.feeRate)
        slippage = float(payload.slippage)
        request_fingerprint = _backtest_create_fingerprint(
            owner=owner,
            strategy_id=str(strategy_id),
            symbol=symbol,
            start=start,
            end=end,
            initial_capital=initial_capital,
            fee_rate=fee_rate,
            slippage=slippage,
        )
        dedup_record = _resolve_backtest_by_fingerprint(request_fingerprint)
        if isinstance(dedup_record, dict):
            dedup_payload = deepcopy(dedup_record)
            dedup_payload["idempotent"] = True
            dedup_payload["idempotentKey"] = request_fingerprint
            return dedup_payload

        override_config_path = _create_backtest_override_config(
            config_path,
            initial_capital=initial_capital,
            fee_rate=fee_rate,
            slippage=slippage,
        )

        start_payload = BacktestStartRequest(start=start, end=end, config_path=str(override_config_path))

        # Reuse the runner to keep one source of truth and consistent artifact layout.
        response = _start_backtest_impl(start_payload, request_fingerprint=request_fingerprint)

        run_id = response.get("run_id")
        if not isinstance(run_id, str):
            raise HTTPException(status_code=500, detail="failed to create backtest run id")

        if bool(response.get("already_running", False)):
            running_record = _resolve_backtest_record(run_id)
            if isinstance(running_record, dict):
                _remember_backtest_create_fingerprint(request_fingerprint, run_id)
                running_payload = deepcopy(running_record)
                running_payload["idempotent"] = True
                running_payload["idempotentKey"] = request_fingerprint
                return running_payload

        strategy = _ensure_default_strategy()
        _BACKTEST_STORE[run_id] = _build_backtest_record(
            run_id=run_id,
            payload={"strategyId": strategy_id or strategy["id"], "symbol": symbol},
            start=start,
            end=end,
            initial_capital=initial_capital,
            fee_rate=fee_rate,
            slippage=slippage,
        )
        _BACKTEST_STORE[run_id]["status"] = "running"
        _BACKTEST_STORE[run_id]["updatedAt"] = _now_iso()
        _persist_backtest_record(run_id, _BACKTEST_STORE[run_id])
        _remember_backtest_create_fingerprint(request_fingerprint, run_id)
        _audit_event(
            "backtest.create",
            entity="backtest",
            entity_id=run_id,
            detail={
                "strategy_id": strategy_id,
                "symbol": symbol,
                "start": start,
                "end": end,
                "initial_capital": initial_capital,
                "fee_rate": fee_rate,
                "slippage": slippage,
                "idempotent_key": request_fingerprint,
            },
        )
        result = deepcopy(_BACKTEST_STORE[run_id])
        result["idempotentKey"] = request_fingerprint
        return result


@app.get("/api/backtests/{run_id}")
def get_backtest(run_id: str, request: Request) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        record = _resolve_backtest_record(run_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"backtest not found: {run_id}")

        runner = _get_backtest_runner(create=False)
        metadata = runner.metadata() if runner is not None else {}
        artifacts_raw = metadata.get("artifacts", {})
        if not isinstance(artifacts_raw, dict) or not artifacts_raw:
            artifacts_raw = record.get("artifacts", {})
        artifacts = artifacts_raw if isinstance(artifacts_raw, dict) else {}

        owner_key = _safe_user_key(str(record.get("owner") or _record_owner_key()))
        running_same_run = False
        if runner is not None and str(metadata.get("run_id") or "") == str(run_id):
            status = runner.status()
            running_same_run = bool(status.get("running", False))

        equity_curve, trades = _load_backtest_detail_rows(
            run_id=run_id,
            owner=owner_key,
            artifacts=artifacts,
            default_symbol=str(record.get("symbol") or ""),
            prefer_db=not running_same_run,
        )
        drawdown_curve: List[Dict[str, float]] = [
            {"ts": str(item.get("ts") or ""), "dd": _to_finite_float_or_default(item.get("dd"), 0.0)}
            for item in equity_curve
        ]

        if not equity_curve:
            portfolio = _build_portfolio_response()
            if portfolio.get("equityCurve"):
                equity_curve = [
                    {
                        "ts": item["ts"],
                        "equity": float(item["equity"]),
                        "pnl": 0.0,
                        "dd": 0.0,
                    }
                    for item in portfolio["equityCurve"]
                ]

        if not drawdown_curve and equity_curve:
            current_peak = equity_curve[0]["equity"] if equity_curve else 0.0
            for item in equity_curve:
                current_peak = max(current_peak, item["equity"])
                dd = 0.0 if current_peak <= 0 else (current_peak - item["equity"]) / current_peak
                drawdown_curve.append({"ts": item["ts"], "dd": dd})

        final_equity = equity_curve[-1]["equity"] if equity_curve else record["initialCapital"]
        base_equity = equity_curve[0]["equity"] if equity_curve else record["initialCapital"]
        pnl_total = final_equity - base_equity
        max_drawdown = 0.0
        if equity_curve:
            max_dd = max((item["dd"] for item in drawdown_curve), default=0.0)
            max_drawdown = float(max_dd)

        win_rate = 0.0
        if trades:
            wins = len([t for t in trades if t["pnl"] > 0])
            win_rate = wins / len(trades)

        cagr = 0.0
        if record["startAt"] and record["endAt"] and base_equity > 0 and final_equity > 0:
            try:
                start_ms = datetime.fromisoformat(record["startAt"]).timestamp()
                end_ms = datetime.fromisoformat(record["endAt"]).timestamp()
                day_span = max((end_ms - start_ms) / 86400.0, 0.0)
                if day_span > 0:
                    cagr = (final_equity / base_equity) ** (365.0 / day_span) - 1
            except Exception:
                cagr = 0.0
        calmar = 0.0 if max_drawdown <= 0 else cagr / max_drawdown

        return {
            **record,
            "metrics": {
                "cagr": cagr,
                "sharpe": 0.0,
                "maxDrawdown": max_drawdown,
                "calmar": calmar,
                "winRate": win_rate,
                "trades": len(trades),
                "pnlTotal": pnl_total,
            },
            "equityCurve": equity_curve,
            "drawdownCurve": drawdown_curve,
            "trades": trades,
        }


@app.get("/api/backtests/{run_id}/logs")
def get_backtest_logs(run_id: str, request: Request) -> List[Dict[str, Any]]:
    with _auth_user_context(_request_auth_username(request)):
        record = _resolve_backtest_record(run_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"backtest not found: {run_id}")

        runner = _get_backtest_runner(create=False)
        if runner is None:
            logs = []
        else:
            metadata = runner.metadata()
            logs = runner.tail_logs(500) if str(run_id) == str(metadata.get("run_id")) else []
        if logs:
            return _collect_process_log_entries("backtest", logs, id_prefix=f"bt_{run_id}", extra="log")
        return [
            {
                "id": f"bt_{run_id}_done",
                "ts": record.get("updatedAt", _now_iso()),
                "level": "info",
                "source": "backtest",
                "message": f"Backtest {run_id} status={record.get('status')}",
            }
        ]


@app.post("/api/backtest/stop")
def stop_backtest(request: Request) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        runner = _get_backtest_runner(create=False)
        if runner is None:
            return {"stopped": False, "status": {"running": False, "pid": None}}
        stopped = runner.stop()
        status = runner.status()
        metadata = runner.metadata()
        run_id = str(metadata.get("run_id") or "")
        if run_id and run_id in _BACKTEST_STORE:
            _sync_backtest_record_status(_BACKTEST_STORE[run_id])
            _persist_backtest_record(run_id, _BACKTEST_STORE[run_id])
        _audit_event(
            "backtest.stop",
            entity="backtest",
            entity_id=run_id,
            detail={"stopped": stopped, "status": status},
        )
        return {"stopped": stopped, "status": status}


@app.get("/api/backtest/status")
def backtest_status(request: Request, log_limit: int = Query(default=120, ge=0, le=2000)) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        runner = _get_backtest_runner(create=False)
        if runner is None:
            return {
                "running": False,
                "pid": None,
                "return_code": None,
                "started_at": None,
                "ended_at": None,
                "command": [],
                "logs": [],
                "metadata": {},
            }
        status = runner.status()
        status["logs"] = runner.tail_logs(log_limit)
        status["metadata"] = runner.metadata()
        return status


@app.get("/api/backtest/artifacts")
def backtest_artifacts(request: Request) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        runner = _get_backtest_runner(create=False)
        metadata = runner.metadata() if runner is not None else {}
        artifacts = metadata.get("artifacts", {})
        run_id = str(metadata.get("run_id") or "")
        owner_key = _safe_user_key(str(metadata.get("owner") or _record_owner_key()))
        if run_id and isinstance(artifacts, dict):
            _sync_backtest_artifacts_to_db(run_id=run_id, owner=owner_key, artifacts=artifacts)
        payload: Dict[str, Any] = {}
        if isinstance(artifacts, dict):
            for name, path_text in artifacts.items():
                artifact_name = str(name or "").strip().lower()
                path = Path(path_text)
                exists = path.exists()
                db_row = None
                if run_id and owner_key and artifact_name:
                    db_row = _db_get_data_file(
                        owner=owner_key,
                        scope="backtest_artifact",
                        file_key=_backtest_artifact_file_key(run_id, artifact_name),
                    )
                size_bytes = path.stat().st_size if exists else 0
                if (not exists) and isinstance(db_row, dict):
                    raw = _decode_data_file_bytes(db_row)
                    size_bytes = len(raw) if raw is not None else 0
                payload[name] = {
                    "path": str(path),
                    "exists_disk": exists,
                    "exists_db": isinstance(db_row, dict),
                    "size_bytes": size_bytes,
                }

        metrics_preview = ""
        metrics_path = artifacts.get("metrics_txt") if isinstance(artifacts, dict) else None
        if metrics_path and Path(metrics_path).exists():
            metrics_preview = Path(metrics_path).read_text(encoding="utf-8", errors="replace")[:4000]
        elif run_id and owner_key:
            metrics_row = _db_get_data_file(
                owner=owner_key,
                scope="backtest_artifact",
                file_key=_backtest_artifact_file_key(run_id, "metrics_txt"),
            )
            metrics_text = _decode_data_file_text(metrics_row)
            if metrics_text is not None:
                metrics_preview = metrics_text[:4000]

        return {
            "metadata": metadata,
            "artifacts": payload,
            "metrics_preview": metrics_preview,
        }


@app.get("/api/backtest/file/{artifact_name}")
def backtest_file(artifact_name: str, request: Request):
    with _auth_user_context(_request_auth_username(request)):
        runner = _get_backtest_runner(create=False)
        metadata = runner.metadata() if runner is not None else {}
        artifacts = metadata.get("artifacts", {})
        if artifact_name not in artifacts:
            raise HTTPException(
                status_code=404,
                detail=f"artifact not found: {artifact_name}",
            )
        run_id = str(metadata.get("run_id") or "")
        owner_key = _safe_user_key(str(metadata.get("owner") or _record_owner_key()))
        if run_id:
            _sync_backtest_artifacts_to_db(run_id=run_id, owner=owner_key, artifacts=artifacts)
        path = Path(artifacts[artifact_name])
        if not path.exists():
            db_row = _db_get_data_file(
                owner=owner_key,
                scope="backtest_artifact",
                file_key=_backtest_artifact_file_key(run_id, artifact_name),
            )
            binary_payload = _decode_data_file_bytes(db_row)
            if binary_payload is None:
                raise HTTPException(status_code=404, detail=f"file not found: {path}")
            content_type = str((db_row or {}).get("contentType") or _guess_file_content_type(file_name=artifact_name))
            file_name = str((db_row or {}).get("fileName") or f"{run_id}_{artifact_name}")
            headers = {"Content-Disposition": f'attachment; filename="{file_name}"'}
            return Response(content=binary_payload, media_type=content_type, headers=headers)
        return FileResponse(path=str(path), filename=path.name)


@app.get("/api/paper/equity")
def paper_equity(
    request: Request,
    path: Optional[str] = Query(default=None),
    strategy_id: Optional[str] = Query(default=None),
    limit: int = Query(default=300, ge=1, le=5000),
) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        resolved_path = path or _resolve_strategy_paper_log_path(strategy_id)
        csv_path = _resolve_log_csv_path(resolved_path)
        owner_key = _record_owner_key()
        paper_file_key = _safe_strategy_id(strategy_id or _project_relative_path_text(csv_path))
        tail_rows = _tail_paper_equity_rows_from_file(csv_path, limit)
        if tail_rows is not None:
            return {"path": str(csv_path), "count": len(tail_rows), "rows": tail_rows}

        df = _load_csv_with_db_fallback(
            path=csv_path,
            owner=owner_key,
            scope="paper_equity_csv",
            file_key=paper_file_key,
        )
        if df is None:
            return {"path": str(csv_path), "count": 0, "rows": []}

        required_cols = {"ts_utc", "equity", "cash"}
        if not required_cols.issubset(set(df.columns)):
            raise HTTPException(status_code=500, detail="paper equity csv columns are invalid")

        rows_df = df.tail(limit).copy()
        rows_df["equity"] = pd.to_numeric(rows_df["equity"], errors="coerce").fillna(0.0)
        rows_df["cash"] = pd.to_numeric(rows_df["cash"], errors="coerce").fillna(0.0)
        rows = rows_df.to_dict(orient="records")
        return {"path": str(csv_path), "count": len(rows), "rows": rows}


@app.get("/api/portfolio")
def portfolio(request: Request, strategy_id: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        return _build_portfolio_response(
            path=_resolve_strategy_paper_log_path(strategy_id),
            strategy_id=strategy_id,
        )


@app.get("/api/positions")
def positions(request: Request, strategy_id: Optional[str] = Query(default=None)) -> List[Dict[str, Any]]:
    with _auth_user_context(_request_auth_username(request)):
        return _build_live_positions_payload(strategy_id=strategy_id)


@app.get("/api/orders")
def orders(request: Request, strategy_id: Optional[str] = Query(default=None)) -> List[Dict[str, Any]]:
    with _auth_user_context(_request_auth_username(request)):
        return _build_live_orders_payload(strategy_id=strategy_id)


@app.get("/api/fills")
def fills(request: Request, strategy_id: Optional[str] = Query(default=None)) -> List[Dict[str, Any]]:
    with _auth_user_context(_request_auth_username(request)):
        return _build_live_fills_payload(strategy_id=strategy_id)


@app.get("/api/logs")
def logs(
    request: Request,
    type: str = Query(default="system"),
    level: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    strategy_id: Optional[str] = Query(default=None),
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    cursor: Optional[int] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=1000),
) -> List[Dict[str, Any]]:
    with _auth_user_context(_request_auth_username(request)):
        requested_level = level.lower().strip() if isinstance(level, str) and level.strip() else None
        if type not in {"system", "strategy"}:
            raise HTTPException(status_code=400, detail="type must be system or strategy")
        if requested_level and requested_level not in {"info", "warn", "error"}:
            raise HTTPException(status_code=422, detail="level must be one of: info, warn, error")
        keyword_filter = q.strip() if isinstance(q, str) and q.strip() else None
        strategy_id_filter = strategy_id.strip() if isinstance(strategy_id, str) and strategy_id.strip() else None

        start_ts = _parse_iso_datetime_param(start, "start")
        end_ts = _parse_iso_datetime_param(end, "end")
        if start_ts and end_ts and start_ts > end_ts:
            raise HTTPException(status_code=422, detail="start must be <= end")
        safe_cursor: Optional[int] = None
        if isinstance(cursor, (int, float, str)):
            try:
                parsed_cursor = int(cursor)
            except Exception as exc:
                raise HTTPException(status_code=422, detail="cursor must be > 0") from exc
            if parsed_cursor <= 0:
                raise HTTPException(status_code=422, detail="cursor must be > 0")
            safe_cursor = parsed_cursor

        if _db_is_enabled():
            return _db_list_runtime_logs(
                owner=_record_owner_key(),
                log_type=type,
                level=requested_level,
                q=keyword_filter,
                strategy_id=strategy_id_filter,
                start_ts=start_ts,
                end_ts=end_ts,
                cursor_id=safe_cursor,
                limit=limit,
            )

        logs_raw: List[Dict[str, str]] = []
        if type == "system":
            runner = _get_backtest_runner(create=False)
            if runner is not None:
                logs_raw.extend(runner.tail_logs(limit))
            logs_raw.extend(_tail_strategy_logs_all(limit_per_strategy=limit))
        else:
            logs_raw.extend(_tail_strategy_logs_all(limit_per_strategy=limit))

        entries = _collect_process_log_entries("system", logs_raw[:limit], id_prefix=f"sys_{int(time.time())}", extra=type)
        if strategy_id_filter:
            entries = [entry for entry in entries if str(entry.get("strategyId") or "") == strategy_id_filter]
        if requested_level:
            entries = [entry for entry in entries if entry.get("level") == requested_level]
        if keyword_filter:
            lowered = keyword_filter.lower()
            entries = [entry for entry in entries if lowered in entry.get("message", "").lower()]
        entries.sort(key=lambda item: str(item.get("ts")), reverse=True)
        return entries[:limit]


@app.get("/api/audit/logs")
def audit_logs(
    request: Request,
    limit: int = Query(default=200, ge=1, le=2000),
    action: Optional[str] = Query(default=None),
    entity: Optional[str] = Query(default=None),
    owner: Optional[str] = Query(default=None),
    start: Optional[str] = None,
    end: Optional[str] = None,
    cursor: Optional[int] = None,
) -> List[Dict[str, Any]]:
    with _auth_user_context(_request_auth_username(request)):
        if not _db_is_enabled():
            return []

        owner_filter: Optional[str]
        if _has_permission("audit.read.all"):
            owner_filter = _safe_user_key(owner) if owner else None
        else:
            owner_filter = _record_owner_key()

        start_ts = _parse_iso_datetime_param(start, "start")
        end_ts = _parse_iso_datetime_param(end, "end")
        if start_ts and end_ts and start_ts > end_ts:
            raise HTTPException(status_code=422, detail="start must be <= end")
        if cursor is not None and int(cursor) <= 0:
            raise HTTPException(status_code=422, detail="cursor must be > 0")

        started = time.perf_counter()
        try:
            rows = _DB_SERVICE.list_audit_logs(
                owner=owner_filter,
                action=action.strip() if isinstance(action, str) and action.strip() else None,
                entity=entity.strip() if isinstance(entity, str) and entity.strip() else None,
                start_ts=start_ts,
                end_ts=end_ts,
                cursor_id=cursor,
                limit=limit,
            )
            _record_db_read_success("audit_read", (time.perf_counter() - started) * 1000.0)
            return rows
        except Exception as exc:
            _record_db_runtime_failure("audit_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
            return []


@app.get("/api/alerts/deliveries")
def alert_deliveries(
    request: Request,
    owner: Optional[str] = Query(default=None),
    event: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    cursor: Optional[int] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
) -> List[Dict[str, Any]]:
    with _auth_user_context(_request_auth_username(request)):
        if not _db_is_enabled():
            return []

        owner_filter: Optional[str]
        if _is_admin_username():
            owner_filter = _safe_user_key(owner) if owner else None
        else:
            owner_filter = _record_owner_key()
        event_filter = event.strip() if isinstance(event, str) and event.strip() else None
        status_filter = status.strip() if isinstance(status, str) and status.strip() else None
        if status_filter and status_filter not in {"sent", "failed"}:
            raise HTTPException(status_code=422, detail="status must be one of: sent, failed")

        start_ts = _parse_iso_datetime_param(start, "start")
        end_ts = _parse_iso_datetime_param(end, "end")
        if start_ts and end_ts and start_ts > end_ts:
            raise HTTPException(status_code=422, detail="start must be <= end")
        safe_cursor: Optional[int] = None
        if cursor is not None:
            try:
                parsed_cursor = int(cursor)
            except Exception as exc:
                raise HTTPException(status_code=422, detail="cursor must be > 0") from exc
            if parsed_cursor <= 0:
                raise HTTPException(status_code=422, detail="cursor must be > 0")
            safe_cursor = parsed_cursor

        return _db_list_alert_deliveries(
            owner=owner_filter,
            event=event_filter,
            status=status_filter,
            start_ts=start_ts,
            end_ts=end_ts,
            cursor_id=safe_cursor,
            limit=limit,
        )


@app.get("/api/ws/connection-events")
def ws_connection_events(
    request: Request,
    owner: Optional[str] = Query(default=None),
    event_type: Optional[str] = Query(default=None),
    strategy_id: Optional[str] = Query(default=None),
    start: Optional[str] = Query(default=None),
    end: Optional[str] = Query(default=None),
    cursor: Optional[int] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
) -> List[Dict[str, Any]]:
    with _auth_user_context(_request_auth_username(request)):
        if not _db_is_enabled():
            return []

        owner_filter: Optional[str]
        if _is_admin_username():
            owner_filter = _safe_user_key(owner) if owner else None
        else:
            owner_filter = _record_owner_key()
        event_filter = event_type.strip() if isinstance(event_type, str) and event_type.strip() else None
        strategy_filter = strategy_id.strip() if isinstance(strategy_id, str) and strategy_id.strip() else None
        start_ts = _parse_iso_datetime_param(start, "start")
        end_ts = _parse_iso_datetime_param(end, "end")
        if start_ts and end_ts and start_ts > end_ts:
            raise HTTPException(status_code=422, detail="start must be <= end")
        safe_cursor: Optional[int] = None
        if cursor is not None:
            try:
                parsed_cursor = int(cursor)
            except Exception as exc:
                raise HTTPException(status_code=422, detail="cursor must be > 0") from exc
            if parsed_cursor <= 0:
                raise HTTPException(status_code=422, detail="cursor must be > 0")
            safe_cursor = parsed_cursor

        return _db_list_ws_connection_events(
            owner=owner_filter,
            event_type=event_filter,
            strategy_id=strategy_filter,
            start_ts=start_ts,
            end_ts=end_ts,
            cursor_id=safe_cursor,
            limit=limit,
        )


@app.get("/api/audit/verify")
def audit_verify(
    request: Request,
    owner: Optional[str] = Query(default=None),
    start_id: Optional[int] = Query(default=None, ge=1),
    end_id: Optional[int] = Query(default=None, ge=1),
    limit: int = Query(default=5000, ge=1, le=100000),
) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        if not _db_is_enabled():
            return {"ok": False, "checked": 0, "mismatchedRows": [], "brokenLinks": [], "detail": "db disabled"}
        owner_filter: Optional[str]
        if _has_permission("audit.read.all"):
            owner_filter = _safe_user_key(owner) if owner else None
        else:
            owner_filter = _record_owner_key()

        started = time.perf_counter()
        try:
            result = _DB_SERVICE.verify_audit_hash_chain(
                owner=owner_filter,
                start_id=start_id,
                end_id=end_id,
                limit=limit,
            )
            _record_db_read_success("audit_verify_read", (time.perf_counter() - started) * 1000.0)
            return result
        except Exception as exc:
            _record_db_runtime_failure("audit_verify_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
            return {"ok": False, "checked": 0, "mismatchedRows": [], "brokenLinks": [], "detail": str(exc)}


@app.get("/api/reports/db/summary")
def db_report_summary(
    request: Request,
    owner: Optional[str] = Query(default=None),
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit_top: int = Query(default=10, ge=1, le=100),
) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        if not _db_is_enabled():
            return {}
        owner_filter: Optional[str]
        if _is_admin_username():
            owner_filter = _safe_user_key(owner) if owner else None
        else:
            owner_filter = _record_owner_key()
        start_ts = _parse_iso_datetime_param(start, "start")
        end_ts = _parse_iso_datetime_param(end, "end")
        if start_ts and end_ts and start_ts > end_ts:
            raise HTTPException(status_code=422, detail="start must be <= end")

        started = time.perf_counter()
        try:
            summary = _DB_SERVICE.build_db_report_summary(
                owner=owner_filter,
                start_ts=start_ts,
                end_ts=end_ts,
                limit_top=limit_top,
            )
            _record_db_read_success("report_read", (time.perf_counter() - started) * 1000.0)
            return summary
        except Exception as exc:
            _record_db_runtime_failure("report_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
            return {}


@app.get("/api/risk")
def risk(request: Request, strategy_id: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        target = strategy_id or _DEFAULT_STRATEGY_ID
        cfg_path = _config_path_for_strategy_id(target) if strategy_id else _DEFAULT_CONFIG_PATH
        return _risk_from_config(config_path=cfg_path, strategy_id=target)


@app.put("/api/risk")
def set_risk(payload: RiskUpdateRequest, request: Request, strategy_id: Optional[str] = Query(default=None)) -> Dict[str, Any]:
    with _auth_user_context(_request_auth_username(request)):
        _require_permission("risk.write")
        if hasattr(payload, "model_dump"):
            payload_data = payload.model_dump(exclude_unset=True)  # pydantic v2
        else:
            payload_data = payload.dict(exclude_unset=True)  # pragma: no cover - pydantic v1 fallback
        return _update_risk_state(payload_data, strategy_id=strategy_id or _DEFAULT_STRATEGY_ID)


@app.get("/api/risk/history")
def risk_history(
    request: Request,
    strategy_id: Optional[str] = Query(default=None),
    owner: Optional[str] = Query(default=None),
    cursor: Optional[int] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
) -> List[Dict[str, Any]]:
    with _auth_user_context(_request_auth_username(request)):
        if not _db_is_enabled():
            return []
        owner_filter: Optional[str]
        if _is_admin_username():
            owner_filter = _safe_user_key(owner) if owner else None
        else:
            owner_filter = _record_owner_key()

        strategy_key = _strategy_store_key(strategy_id) if isinstance(strategy_id, str) and strategy_id.strip() else None
        if cursor is not None and int(cursor) <= 0:
            raise HTTPException(status_code=422, detail="cursor must be > 0")

        started = time.perf_counter()
        try:
            rows = _DB_SERVICE.list_risk_state_history(
                owner=owner_filter,
                strategy_key=strategy_key,
                cursor_id=cursor,
                limit=limit,
            )
            _record_db_read_success("risk_history_read", (time.perf_counter() - started) * 1000.0)
            return rows
        except Exception as exc:
            _record_db_runtime_failure("risk_history_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
            return []


@app.get("/api/risk/events")
def risk_events(
    request: Request,
    strategy_id: Optional[str] = Query(default=None),
    event_type: Optional[str] = Query(default=None),
    owner: Optional[str] = Query(default=None),
    start: Optional[str] = None,
    end: Optional[str] = None,
    cursor: Optional[int] = None,
    limit: int = Query(default=200, ge=1, le=2000),
) -> List[Dict[str, Any]]:
    with _auth_user_context(_request_auth_username(request)):
        if not _db_is_enabled():
            return []

        owner_filter: Optional[str]
        if _is_admin_username():
            owner_filter = _safe_user_key(owner) if owner else None
        else:
            owner_filter = _record_owner_key()

        strategy_key = _strategy_store_key(strategy_id) if isinstance(strategy_id, str) and strategy_id.strip() else None
        event_filter = event_type.strip() if isinstance(event_type, str) and event_type.strip() else None
        start_ts = _parse_iso_datetime_param(start, "start")
        end_ts = _parse_iso_datetime_param(end, "end")
        if start_ts and end_ts and start_ts > end_ts:
            raise HTTPException(status_code=422, detail="start must be <= end")
        if cursor is not None and int(cursor) <= 0:
            raise HTTPException(status_code=422, detail="cursor must be > 0")

        started = time.perf_counter()
        try:
            rows = _DB_SERVICE.list_risk_events(
                owner=owner_filter,
                strategy_key=strategy_key,
                event_type=event_filter,
                start_ts=start_ts,
                end_ts=end_ts,
                cursor_id=cursor,
                limit=limit,
            )
            _record_db_read_success("risk_event_read", (time.perf_counter() - started) * 1000.0)
            return rows
        except Exception as exc:
            _record_db_runtime_failure("risk_event_read", exc, elapsed_ms=(time.perf_counter() - started) * 1000.0)
            return []
