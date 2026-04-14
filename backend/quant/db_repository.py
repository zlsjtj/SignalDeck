from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol


class DBRepository(Protocol):
    backend: str

    def close(self) -> None:
        ...

    def initialize(self) -> None:
        ...

    def upsert_strategy(self, strategy_key: str, owner: str, record: Dict[str, Any]) -> None:
        ...

    def enqueue_strategy_compile_job(self, strategy_key: str, owner: str) -> Dict[str, Any]:
        ...

    def update_strategy_compile_job(
        self,
        job_id: int,
        *,
        status: str,
        error_message: str = "",
        started_at: Optional[str] = None,
        finished_at: Optional[str] = None,
    ) -> None:
        ...

    def list_strategy_compile_jobs(
        self,
        *,
        owner: Optional[str] = None,
        strategy_key: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        ...

    def add_strategy_script(
        self,
        *,
        strategy_key: str,
        owner: str,
        script_type: str,
        script_path: str,
        script_hash: str,
        source_config: Dict[str, Any],
    ) -> Dict[str, Any]:
        ...

    def list_strategy_scripts(
        self,
        *,
        owner: Optional[str] = None,
        strategy_key: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        ...

    def get_latest_strategy_script(
        self,
        *,
        owner: str,
        strategy_key: str,
    ) -> Optional[Dict[str, Any]]:
        ...

    def delete_strategy(self, strategy_key: str) -> None:
        ...

    def load_strategies(self) -> List[Dict[str, Any]]:
        ...

    def upsert_backtest(self, run_id: str, owner: str, record: Dict[str, Any]) -> None:
        ...

    def load_backtests(self) -> List[Dict[str, Any]]:
        ...

    def upsert_risk_state(self, owner: str, strategy_key: str, state: Dict[str, Any]) -> None:
        ...

    def delete_risk_state(self, owner: str, strategy_key: str) -> None:
        ...

    def load_risk_states(self) -> List[Dict[str, Any]]:
        ...

    def list_risk_state_history(
        self,
        *,
        owner: Optional[str] = None,
        strategy_key: Optional[str] = None,
        cursor_id: Optional[int] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        ...

    def append_audit_log(
        self,
        owner: str,
        action: str,
        entity: str,
        entity_id: str,
        detail: Optional[Dict[str, Any]] = None,
    ) -> None:
        ...

    def list_audit_logs(
        self,
        *,
        owner: Optional[str] = None,
        action: Optional[str] = None,
        entity: Optional[str] = None,
        start_ts: Optional[str] = None,
        end_ts: Optional[str] = None,
        cursor_id: Optional[int] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        ...

    def verify_audit_hash_chain(
        self,
        *,
        owner: Optional[str] = None,
        start_id: Optional[int] = None,
        end_id: Optional[int] = None,
        limit: int = 5000,
    ) -> Dict[str, Any]:
        ...

    def append_risk_event(
        self,
        *,
        owner: str,
        strategy_key: str,
        event_type: str,
        rule: str,
        message: str,
        detail: Optional[Dict[str, Any]] = None,
        ts_utc: Optional[str] = None,
    ) -> None:
        ...

    def list_risk_events(
        self,
        *,
        owner: Optional[str] = None,
        strategy_key: Optional[str] = None,
        event_type: Optional[str] = None,
        start_ts: Optional[str] = None,
        end_ts: Optional[str] = None,
        cursor_id: Optional[int] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        ...

    def upsert_market_ticks(
        self,
        rows: List[Dict[str, Any]],
        *,
        source_config_path: str = "",
    ) -> int:
        ...

    def upsert_market_klines(
        self,
        rows: List[Dict[str, Any]],
        *,
        timeframe: str,
        source_config_path: str = "",
    ) -> int:
        ...

    def build_db_report_summary(
        self,
        *,
        owner: Optional[str] = None,
        start_ts: Optional[str] = None,
        end_ts: Optional[str] = None,
        limit_top: int = 10,
    ) -> Dict[str, Any]:
        ...

    def ensure_user(
        self,
        username: str,
        *,
        role: str = "user",
        display_name: str = "",
    ) -> Dict[str, Any]:
        ...

    def upsert_user_credential(
        self,
        *,
        username: str,
        password_hash: str,
        algorithm: str,
        password_updated_at: Optional[str] = None,
    ) -> None:
        ...

    def create_auth_session(
        self,
        *,
        session_id: str,
        username: str,
        issued_at: Optional[str] = None,
        expires_at: str,
        client_ip: str = "",
        user_agent: str = "",
    ) -> None:
        ...

    def get_auth_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        ...

    def revoke_auth_session(self, session_id: str, revoked_at: Optional[str] = None) -> None:
        ...

    def record_login_attempt(
        self,
        *,
        username: str,
        client_ip: str,
        success: bool,
        reason: str = "",
        ts_utc: Optional[str] = None,
    ) -> None:
        ...

    def set_lockout(
        self,
        *,
        lock_key: str,
        locked_until: str,
        updated_at: Optional[str] = None,
    ) -> None:
        ...

    def get_active_lockouts(
        self,
        *,
        lock_keys: List[str],
        now_ts: str,
    ) -> Dict[str, str]:
        ...

    def clear_lockouts(self, lock_keys: List[str]) -> None:
        ...

    def upsert_user_preferences(self, owner: str, preferences: Dict[str, Any]) -> None:
        ...

    def get_user_preferences(self, owner: str) -> Optional[Dict[str, Any]]:
        ...

    def list_roles(self) -> List[Dict[str, Any]]:
        ...

    def list_permissions(self) -> List[Dict[str, Any]]:
        ...

    def list_user_roles(self, username: str) -> List[str]:
        ...

    def replace_user_roles(self, username: str, roles: List[str]) -> List[str]:
        ...

    def user_has_permission(self, username: str, permission_code: str) -> bool:
        ...

    def append_account_security_event(
        self,
        *,
        owner: str,
        event_type: str,
        severity: str,
        message: str,
        detail: Optional[Dict[str, Any]] = None,
        ts_utc: Optional[str] = None,
    ) -> None:
        ...

    def list_account_security_events(
        self,
        *,
        owner: Optional[str] = None,
        event_type: Optional[str] = None,
        start_ts: Optional[str] = None,
        end_ts: Optional[str] = None,
        cursor_id: Optional[int] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        ...

    def create_api_token(
        self,
        *,
        owner: str,
        token_name: str,
        token_prefix: str,
        token_hash: str,
        scopes: List[str],
        expires_at: str = "",
        created_by: str = "",
        created_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        ...

    def list_api_tokens(
        self,
        *,
        owner: Optional[str] = None,
        include_revoked: bool = False,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        ...

    def get_active_api_token_by_hash(
        self,
        *,
        token_hash: str,
        now_ts: str,
    ) -> Optional[Dict[str, Any]]:
        ...

    def touch_api_token_last_used(
        self,
        token_id: int,
        *,
        last_used_at: Optional[str] = None,
    ) -> None:
        ...

    def revoke_api_token(
        self,
        token_id: int,
        *,
        revoked_at: Optional[str] = None,
        revoked_by: str = "",
    ) -> None:
        ...

    def append_runtime_log(
        self,
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
        ...

    def list_runtime_logs(
        self,
        *,
        owner: Optional[str] = None,
        log_type: Optional[str] = None,
        level: Optional[str] = None,
        q: Optional[str] = None,
        strategy_id: Optional[str] = None,
        start_ts: Optional[str] = None,
        end_ts: Optional[str] = None,
        cursor_id: Optional[int] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        ...

    def append_alert_delivery(
        self,
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
        ...

    def list_alert_deliveries(
        self,
        *,
        owner: Optional[str] = None,
        event: Optional[str] = None,
        status: Optional[str] = None,
        start_ts: Optional[str] = None,
        end_ts: Optional[str] = None,
        cursor_id: Optional[int] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        ...

    def enqueue_alert_outbox(
        self,
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
        ...

    def list_due_alert_outbox(
        self,
        *,
        now_ts: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        ...

    def finalize_alert_outbox(
        self,
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
        ...

    def append_ws_connection_event(
        self,
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
        ...

    def list_ws_connection_events(
        self,
        *,
        owner: Optional[str] = None,
        event_type: Optional[str] = None,
        strategy_id: Optional[str] = None,
        start_ts: Optional[str] = None,
        end_ts: Optional[str] = None,
        cursor_id: Optional[int] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        ...

    def append_strategy_diagnostics_snapshot(
        self,
        *,
        owner: str,
        strategy_id: str,
        source_path: str,
        snapshot: Dict[str, Any],
        ts_utc: Optional[str] = None,
    ) -> None:
        ...

    def list_strategy_diagnostics_snapshots(
        self,
        *,
        owner: Optional[str] = None,
        strategy_id: Optional[str] = None,
        start_ts: Optional[str] = None,
        end_ts: Optional[str] = None,
        cursor_id: Optional[int] = None,
        limit: int = 200,
        include_snapshot: bool = False,
    ) -> List[Dict[str, Any]]:
        ...

    def replace_backtest_trades(
        self,
        *,
        run_id: str,
        owner: str,
        rows: List[Dict[str, Any]],
    ) -> int:
        ...

    def replace_backtest_equity_points(
        self,
        *,
        run_id: str,
        owner: str,
        rows: List[Dict[str, Any]],
    ) -> int:
        ...

    def list_backtest_trades(
        self,
        *,
        run_id: str,
        owner: str,
        limit: int = 10000,
    ) -> List[Dict[str, Any]]:
        ...

    def list_backtest_equity_points(
        self,
        *,
        run_id: str,
        owner: str,
        limit: int = 10000,
    ) -> List[Dict[str, Any]]:
        ...

    def upsert_data_file(
        self,
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
    ) -> Dict[str, Any]:
        ...

    def get_data_file(
        self,
        *,
        owner: str,
        scope: str,
        file_key: str,
    ) -> Optional[Dict[str, Any]]:
        ...
