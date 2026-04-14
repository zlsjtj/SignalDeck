import json
import math
import os
import re
import shutil
import subprocess
import threading
import time
import traceback
from collections import Counter, deque
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return None


def _sanitize_name(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", text)


def _sanitize_json_value(value: Any) -> Any:
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    if isinstance(value, dict):
        return {str(k): _sanitize_json_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_json_value(v) for v in value]
    if isinstance(value, tuple):
        return [_sanitize_json_value(v) for v in value]
    return value


class RuntimeDiagnostics:
    """
    Thread-safe runtime diagnostics collector.
    - Maintains an in-memory snapshot for live debugging.
    - Emits heartbeat logs periodically.
    - Persists snapshot JSON and exception history JSONL.
    """

    def __init__(
        self,
        *,
        project_root: Path,
        config_path: str,
        cfg_raw: Dict[str, Any],
        logger: Any,
    ) -> None:
        self.project_root = Path(project_root).resolve()
        self.config_path = Path(config_path).resolve()
        self.cfg_raw = deepcopy(cfg_raw if isinstance(cfg_raw, dict) else {})
        self.logger = logger
        self.pid = os.getpid()
        self.started_epoch = time.time()
        self.started_at = _iso_now()
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self.last_persisted_at = ""

        diag_cfg = self.cfg_raw.get("diagnostics")
        if not isinstance(diag_cfg, dict):
            diag_cfg = {}

        heartbeat_minutes = int(_safe_float(diag_cfg.get("heartbeat_minutes"), 1.0))
        self.heartbeat_minutes = max(1, min(5, heartbeat_minutes))
        self.heartbeat_seconds = self.heartbeat_minutes * 60

        default_name = _sanitize_name(self.config_path.stem) or f"runtime_{self.pid}"
        self.log_dir = self.project_root / "logs" / "diagnostics"
        self.log_dir.mkdir(parents=True, exist_ok=True)

        snapshot_path = diag_cfg.get("snapshot_path")
        if isinstance(snapshot_path, str) and snapshot_path.strip():
            resolved_snapshot = Path(snapshot_path.strip())
            if not resolved_snapshot.is_absolute():
                resolved_snapshot = self.project_root / resolved_snapshot
            self.snapshot_path = resolved_snapshot.resolve()
        else:
            self.snapshot_path = (self.log_dir / f"{default_name}.json").resolve()
        self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)

        exceptions_path = diag_cfg.get("exceptions_path")
        if isinstance(exceptions_path, str) and exceptions_path.strip():
            resolved_exceptions = Path(exceptions_path.strip())
            if not resolved_exceptions.is_absolute():
                resolved_exceptions = self.project_root / resolved_exceptions
            self.exceptions_path = resolved_exceptions.resolve()
        else:
            self.exceptions_path = (self.log_dir / f"{default_name}_exceptions.jsonl").resolve()
        self.exceptions_path.parent.mkdir(parents=True, exist_ok=True)

        self.commit_id = self._detect_commit_id()
        self.version = str(self.cfg_raw.get("version") or os.getenv("APP_VERSION") or "unknown")

        self.state = "RUNNING"
        self.state_reason = "startup"
        self.state_changed_at = self.started_at

        self.last_tick_time = ""
        self.last_bar_time = ""
        self.data_source_status = "unknown"
        self.data_source_detail = ""
        self.data_lag_seconds = None

        self.exchange_status = {
            "fetch_balance": {"ok": False, "ts": "", "detail": ""},
            "fetch_positions": {"ok": False, "ts": "", "detail": ""},
            "fetch_open_orders": {"ok": False, "ts": "", "detail": ""},
        }
        self.last_api_error = {"api": "", "message": "", "ts": ""}

        self.positions_snapshot: List[Dict[str, Any]] = []
        self.open_orders_snapshot: List[Dict[str, Any]] = []

        self.signal_evaluation = {
            "at": "",
            "conditions": [],
            "entry_signal": False,
            "filter_reasons": [],
            "details": {},
        }

        self.stop_levels = {
            "sl": None,
            "tp": None,
            "ts": None,
            "price_source": "",
            "last_updated": "",
            "note": "",
        }

        self.last_order_attempt = {
            "ts": "",
            "status": "",
            "symbol": "",
            "side": "",
            "qty": 0.0,
            "price": 0.0,
            "failure_reason": "",
            "error": "",
            "exchange_response": {},
            "params": {},
        }

        self._order_attempts: Deque[Dict[str, Any]] = deque(maxlen=200)
        self._exceptions: Deque[Dict[str, Any]] = deque(maxlen=2000)
        self._load_exception_history()

        self._config_summary = self._build_config_summary(self.cfg_raw)

    def _detect_commit_id(self) -> str:
        env_commit = (
            os.getenv("COMMIT_ID")
            or os.getenv("GIT_COMMIT")
            or os.getenv("CI_COMMIT_SHA")
            or ""
        ).strip()
        if env_commit:
            return env_commit[:12]

        try:
            value = subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"],
                cwd=str(self.project_root),
                stderr=subprocess.DEVNULL,
                timeout=1.5,
            )
            text = value.decode("utf-8", errors="replace").strip()
            return text or "unknown"
        except Exception:
            return "unknown"

    def _build_config_summary(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        strategy_cfg = raw.get("strategy")
        if not isinstance(strategy_cfg, dict):
            strategy_cfg = {}
        portfolio_cfg = raw.get("portfolio")
        if not isinstance(portfolio_cfg, dict):
            portfolio_cfg = {}
        risk_cfg = raw.get("risk")
        if not isinstance(risk_cfg, dict):
            risk_cfg = {}
        execution_cfg = raw.get("execution")
        if not isinstance(execution_cfg, dict):
            execution_cfg = {}

        symbols = raw.get("symbols")
        if not isinstance(symbols, list):
            symbols = []

        return {
            "exchange": raw.get("exchange", ""),
            "paper": bool(raw.get("paper", True)),
            "symbol_count": len(symbols),
            "symbols": symbols[:20],
            "timeframe": raw.get("timeframe", ""),
            "lookback_hours": raw.get("lookback_hours"),
            "rebalance_every_minutes": raw.get("rebalance_every_minutes"),
            "strategy": {
                "long_quantile": strategy_cfg.get("long_quantile"),
                "short_quantile": strategy_cfg.get("short_quantile"),
                "score_threshold": strategy_cfg.get("score_threshold"),
                "weight_mode": strategy_cfg.get("weight_mode"),
            },
            "portfolio": {
                "gross_leverage": portfolio_cfg.get("gross_leverage"),
                "max_weight_per_symbol": portfolio_cfg.get("max_weight_per_symbol"),
                "min_order_usdt": portfolio_cfg.get("min_order_usdt"),
                "drift_threshold": portfolio_cfg.get("drift_threshold"),
            },
            "risk": {
                "max_daily_loss": risk_cfg.get("max_daily_loss"),
                "max_strategy_dd": risk_cfg.get("max_strategy_dd"),
                "stop_out_dd": risk_cfg.get("stop_out_dd"),
                "cool_off_hours": risk_cfg.get("cool_off_hours"),
            },
            "execution": {
                "order_type": execution_cfg.get("order_type"),
                "limit_price_offset_bps": execution_cfg.get("limit_price_offset_bps"),
            },
            "diagnostics": {
                "heartbeat_minutes": self.heartbeat_minutes,
                "snapshot_path": str(self.snapshot_path),
                "exceptions_path": str(self.exceptions_path),
            },
        }

    def _load_exception_history(self) -> None:
        if not self.exceptions_path.exists():
            return
        try:
            # Keep memory bounded by loading only latest lines.
            tail_lines: Deque[str] = deque(maxlen=4000)
            with self.exceptions_path.open("r", encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    line = line.strip()
                    if line:
                        tail_lines.append(line)
            for line in tail_lines:
                try:
                    payload = json.loads(line)
                    if isinstance(payload, dict):
                        self._exceptions.append(payload)
                except Exception:
                    continue
        except Exception:
            return

    def _append_exception_persisted(self, payload: Dict[str, Any]) -> None:
        try:
            with self.exceptions_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception:
            return

    def start_heartbeat(self) -> None:
        with self._lock:
            if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
                return
            self._stop_event.clear()
            self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
            self._heartbeat_thread.start()

    def stop_heartbeat(self) -> None:
        self._stop_event.set()
        thread = self._heartbeat_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=1.0)

    def _heartbeat_loop(self) -> None:
        # Emit one immediate heartbeat, then periodic ones.
        self.write_snapshot(emit_heartbeat=True)
        while not self._stop_event.wait(self.heartbeat_seconds):
            self.write_snapshot(emit_heartbeat=True)

    def set_state(self, state: str, reason: str) -> None:
        normalized = str(state or "RUNNING").upper()
        if normalized not in {"RUNNING", "PAUSED", "SAFE_MODE", "ERROR"}:
            normalized = "RUNNING"
        now = _iso_now()
        with self._lock:
            if self.state != normalized:
                self.state = normalized
                self.state_changed_at = now
            self.state_reason = str(reason or "")

    def note_api_error(self, api_name: str, error: Exception | str) -> None:
        with self._lock:
            self.last_api_error = {
                "api": str(api_name),
                "message": str(error),
                "ts": _iso_now(),
            }

    def record_data_source_status(self, status: str, detail: str = "") -> None:
        with self._lock:
            self.data_source_status = str(status or "unknown")
            self.data_source_detail = str(detail or "")

    def record_tick_time(self, ts_iso: Optional[str] = None) -> None:
        with self._lock:
            self.last_tick_time = ts_iso or _iso_now()

    def record_data_snapshot(self, universe_data: Dict[str, Any]) -> None:
        now_dt = datetime.now(timezone.utc)
        latest_bar_dt: Optional[datetime] = None
        for df in (universe_data or {}).values():
            if df is None or getattr(df, "empty", True):
                continue
            try:
                idx = df.index.max()
                if idx is None:
                    continue
                if hasattr(idx, "to_pydatetime"):
                    idx_dt = idx.to_pydatetime()
                else:
                    idx_dt = idx
                if idx_dt.tzinfo is None:
                    idx_dt = idx_dt.replace(tzinfo=timezone.utc)
                if latest_bar_dt is None or idx_dt > latest_bar_dt:
                    latest_bar_dt = idx_dt
            except Exception:
                continue

        with self._lock:
            if latest_bar_dt is not None:
                self.last_bar_time = latest_bar_dt.isoformat()
                self.data_lag_seconds = max(0.0, (now_dt - latest_bar_dt).total_seconds())
            self.last_tick_time = now_dt.isoformat()
            if self.data_source_status in {"unknown", "error"}:
                self.data_source_status = "ok"
                self.data_source_detail = "market data refreshed"

    def record_exchange_probe(
        self,
        *,
        balance: Dict[str, Any],
        positions: Dict[str, Any],
        open_orders: Dict[str, Any],
    ) -> None:
        with self._lock:
            self.exchange_status["fetch_balance"] = balance
            self.exchange_status["fetch_positions"] = positions
            self.exchange_status["fetch_open_orders"] = open_orders

    def record_positions_and_orders(
        self,
        positions: List[Dict[str, Any]],
        open_orders: List[Dict[str, Any]],
    ) -> None:
        with self._lock:
            self.positions_snapshot = deepcopy(positions)
            self.open_orders_snapshot = deepcopy(open_orders)

    def record_signal_evaluation(
        self,
        *,
        conditions: List[Dict[str, Any]],
        entry_signal: bool,
        filter_reasons: List[str],
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._lock:
            self.signal_evaluation = {
                "at": _iso_now(),
                "conditions": deepcopy(conditions),
                "entry_signal": bool(entry_signal),
                "filter_reasons": [str(x) for x in filter_reasons],
                "details": deepcopy(details or {}),
            }

    def record_stop_levels(
        self,
        *,
        sl: Any,
        tp: Any,
        ts: Any,
        price_source: str,
        note: str = "",
    ) -> None:
        with self._lock:
            self.stop_levels = {
                "sl": sl,
                "tp": tp,
                "ts": ts,
                "price_source": str(price_source or ""),
                "last_updated": _iso_now(),
                "note": str(note or ""),
            }

    def record_order_attempt(self, payload: Dict[str, Any]) -> None:
        entry = deepcopy(payload if isinstance(payload, dict) else {})
        entry["ts"] = entry.get("ts") or _iso_now()
        if "qty" not in entry and "amount" in entry:
            entry["qty"] = _safe_float(entry.get("amount"))
        with self._lock:
            self._order_attempts.append(entry)
            self.last_order_attempt = {
                "ts": str(entry.get("ts", "")),
                "status": str(entry.get("status", "")),
                "symbol": str(entry.get("symbol", "")),
                "side": str(entry.get("side", "")),
                "qty": _safe_float(entry.get("qty", entry.get("amount", 0.0))),
                "price": _safe_float(entry.get("price", 0.0)),
                "failure_reason": str(entry.get("failure_reason", "")),
                "error": str(entry.get("error", "")),
                "exchange_response": deepcopy(entry.get("exchange_response", {})),
                "params": deepcopy(entry.get("params", {})),
            }

    def record_exception(self, where: str, err: Exception, stack_text: Optional[str] = None) -> None:
        payload = {
            "ts": _iso_now(),
            "where": str(where),
            "type": err.__class__.__name__,
            "message": str(err),
            "stack": stack_text or traceback.format_exc(),
        }
        with self._lock:
            self._exceptions.append(payload)
            self.last_api_error = {
                "api": str(where),
                "message": str(err),
                "ts": payload["ts"],
            }
        self._append_exception_persisted(payload)

    def _exception_window(self, days: int = 10) -> List[Dict[str, Any]]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        out: List[Dict[str, Any]] = []
        with self._lock:
            events = list(self._exceptions)
        for event in events:
            ts = _parse_iso(str(event.get("ts", "")))
            if ts is None:
                continue
            if ts >= cutoff:
                out.append(event)
        return out

    def _build_logging_payload(self) -> Dict[str, Any]:
        targets: List[str] = []
        level = ""
        try:
            level = str(getattr(self.logger, "level", "") or "")
            for handler in getattr(self.logger, "handlers", []):
                if hasattr(handler, "baseFilename"):
                    targets.append(f"file:{getattr(handler, 'baseFilename')}")
                elif hasattr(handler, "stream"):
                    stream_name = getattr(getattr(handler, "stream"), "name", repr(getattr(handler, "stream")))
                    targets.append(f"stream:{stream_name}")
                else:
                    targets.append(handler.__class__.__name__)
        except Exception:
            targets = []

        disk_free_bytes = 0
        try:
            usage = shutil.disk_usage(str(self.log_dir))
            disk_free_bytes = int(usage.free)
        except Exception:
            disk_free_bytes = 0

        writable = os.access(str(self.log_dir), os.W_OK)
        recent_write = ""
        try:
            if self.snapshot_path.exists():
                recent_write = datetime.fromtimestamp(
                    self.snapshot_path.stat().st_mtime, tz=timezone.utc
                ).isoformat()
        except Exception:
            recent_write = ""

        return {
            "targets": targets,
            "level": level,
            "recent_write_time": recent_write,
            "disk_free_bytes": disk_free_bytes,
            "log_dir": str(self.log_dir),
            "writable": writable,
        }

    def _build_process_payload(self) -> Dict[str, Any]:
        now = time.time()
        uptime = max(0, int(now - self.started_epoch))
        return {
            "pid": self.pid,
            "started_at": self.started_at,
            "uptime_seconds": uptime,
            "version": self.version,
            "commit_id": self.commit_id,
            "config_path": str(self.config_path),
        }

    def build_snapshot(self) -> Dict[str, Any]:
        exceptions_10d = self._exception_window(days=10)
        day_counter: Counter[str] = Counter()
        for event in exceptions_10d:
            ts = _parse_iso(str(event.get("ts", "")))
            if ts is None:
                continue
            day_counter[ts.strftime("%Y-%m-%d")] += 1

        with self._lock:
            snapshot = {
                "generated_at": _iso_now(),
                "schema_version": 1,
                "process": self._build_process_payload(),
                "config": {
                    "path": str(self.config_path),
                    "summary": deepcopy(self._config_summary),
                },
                "market_data": {
                    "last_tick_time": self.last_tick_time,
                    "last_bar_time": self.last_bar_time,
                    "data_lag_seconds": self.data_lag_seconds,
                    "data_source_status": self.data_source_status,
                    "data_source_detail": self.data_source_detail,
                },
                "exchange_connection": {
                    "fetch_balance": deepcopy(self.exchange_status.get("fetch_balance", {})),
                    "fetch_positions": deepcopy(self.exchange_status.get("fetch_positions", {})),
                    "fetch_open_orders": deepcopy(self.exchange_status.get("fetch_open_orders", {})),
                    "last_api_error": deepcopy(self.last_api_error),
                },
                "strategy_state": {
                    "state": self.state,
                    "last_switch_reason": self.state_reason,
                    "last_switch_time": self.state_changed_at,
                },
                "positions_and_orders": {
                    "positions": deepcopy(self.positions_snapshot),
                    "open_orders": deepcopy(self.open_orders_snapshot),
                },
                "signal_evaluation": deepcopy(self.signal_evaluation),
                "stop_take_trailing": deepcopy(self.stop_levels),
                "last_order_attempt": deepcopy(self.last_order_attempt),
                "logging": self._build_logging_payload(),
                "exceptions": {
                    "window_days": 10,
                    "total_count": len(exceptions_10d),
                    "counts_by_day": dict(sorted(day_counter.items())),
                    "last_20": exceptions_10d[-20:],
                },
                "recent_order_attempts": list(self._order_attempts)[-20:],
            }
        return snapshot

    def write_snapshot(self, emit_heartbeat: bool = False) -> Dict[str, Any]:
        snapshot = _sanitize_json_value(self.build_snapshot())
        tmp_path = self.snapshot_path.with_suffix(self.snapshot_path.suffix + ".tmp")
        try:
            with tmp_path.open("w", encoding="utf-8") as fh:
                json.dump(snapshot, fh, ensure_ascii=False, indent=2, allow_nan=False)
            tmp_path.replace(self.snapshot_path)
            self.last_persisted_at = _iso_now()
        except Exception as exc:
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass
            if self.logger:
                self.logger.warning(f"Failed to persist diagnostics snapshot: {exc}")

        if emit_heartbeat and self.logger:
            state = snapshot.get("strategy_state", {}).get("state", "")
            data_lag = snapshot.get("market_data", {}).get("data_lag_seconds")
            order_status = snapshot.get("last_order_attempt", {}).get("status", "")
            entry_signal = snapshot.get("signal_evaluation", {}).get("entry_signal", False)
            self.logger.info(
                "[HEARTBEAT] "
                f"state={state} "
                f"entry_signal={entry_signal} "
                f"data_lag_s={data_lag} "
                f"last_order_status={order_status} "
                f"snapshot={self.snapshot_path}"
            )
        return snapshot
