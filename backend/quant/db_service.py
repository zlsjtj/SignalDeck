from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional

from db_repository import DBRepository


class PersistenceService:
    """
    Service layer that centralizes DB operations behind a repository contract.
    The repository can be switched at runtime (sqlite/postgres) while callers
    keep using a stable service API.
    """

    def __init__(self, repository_getter: Callable[[], DBRepository]) -> None:
        self._repository_getter = repository_getter

    def _repo(self) -> DBRepository:
        repo = self._repository_getter()
        if repo is None:
            raise RuntimeError("db repository is not ready")
        return repo

    @property
    def backend(self) -> str:
        return str(getattr(self._repo(), "backend", "unknown"))

    def initialize(self) -> None:
        self._repo().initialize()

    def upsert_strategy(self, strategy_key: str, owner: str, record: Dict[str, Any]) -> None:
        self._repo().upsert_strategy(strategy_key, owner, record)

    def enqueue_strategy_compile_job(self, strategy_key: str, owner: str) -> Dict[str, Any]:
        return self._repo().enqueue_strategy_compile_job(strategy_key, owner)

    def update_strategy_compile_job(
        self,
        job_id: int,
        *,
        status: str,
        error_message: str = "",
        started_at: Optional[str] = None,
        finished_at: Optional[str] = None,
    ) -> None:
        self._repo().update_strategy_compile_job(
            job_id,
            status=status,
            error_message=error_message,
            started_at=started_at,
            finished_at=finished_at,
        )

    def list_strategy_compile_jobs(
        self,
        *,
        owner: Optional[str] = None,
        strategy_key: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        return self._repo().list_strategy_compile_jobs(
            owner=owner,
            strategy_key=strategy_key,
            limit=limit,
        )

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
        return self._repo().add_strategy_script(
            strategy_key=strategy_key,
            owner=owner,
            script_type=script_type,
            script_path=script_path,
            script_hash=script_hash,
            source_config=source_config,
        )

    def list_strategy_scripts(
        self,
        *,
        owner: Optional[str] = None,
        strategy_key: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        return self._repo().list_strategy_scripts(
            owner=owner,
            strategy_key=strategy_key,
            limit=limit,
        )

    def get_latest_strategy_script(
        self,
        *,
        owner: str,
        strategy_key: str,
    ) -> Optional[Dict[str, Any]]:
        return self._repo().get_latest_strategy_script(
            owner=owner,
            strategy_key=strategy_key,
        )

    def delete_strategy(self, strategy_key: str) -> None:
        self._repo().delete_strategy(strategy_key)

    def load_strategies(self) -> List[Dict[str, Any]]:
        return self._repo().load_strategies()

    def upsert_backtest(self, run_id: str, owner: str, record: Dict[str, Any]) -> None:
        self._repo().upsert_backtest(run_id, owner, record)

    def load_backtests(self) -> List[Dict[str, Any]]:
        return self._repo().load_backtests()

    def upsert_risk_state(self, owner: str, strategy_key: str, state: Dict[str, Any]) -> None:
        self._repo().upsert_risk_state(owner, strategy_key, state)

    def delete_risk_state(self, owner: str, strategy_key: str) -> None:
        self._repo().delete_risk_state(owner, strategy_key)

    def load_risk_states(self) -> List[Dict[str, Any]]:
        return self._repo().load_risk_states()

    def list_risk_state_history(
        self,
        *,
        owner: Optional[str] = None,
        strategy_key: Optional[str] = None,
        cursor_id: Optional[int] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        return self._repo().list_risk_state_history(
            owner=owner,
            strategy_key=strategy_key,
            cursor_id=cursor_id,
            limit=limit,
        )

    def append_audit_log(
        self,
        owner: str,
        action: str,
        entity: str,
        entity_id: str,
        detail: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._repo().append_audit_log(owner, action, entity, entity_id, detail)

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
        return self._repo().list_audit_logs(
            owner=owner,
            action=action,
            entity=entity,
            start_ts=start_ts,
            end_ts=end_ts,
            cursor_id=cursor_id,
            limit=limit,
        )

    def verify_audit_hash_chain(
        self,
        *,
        owner: Optional[str] = None,
        start_id: Optional[int] = None,
        end_id: Optional[int] = None,
        limit: int = 5000,
    ) -> Dict[str, Any]:
        return self._repo().verify_audit_hash_chain(
            owner=owner,
            start_id=start_id,
            end_id=end_id,
            limit=limit,
        )

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
        self._repo().append_risk_event(
            owner=owner,
            strategy_key=strategy_key,
            event_type=event_type,
            rule=rule,
            message=message,
            detail=detail,
            ts_utc=ts_utc,
        )

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
        return self._repo().list_risk_events(
            owner=owner,
            strategy_key=strategy_key,
            event_type=event_type,
            start_ts=start_ts,
            end_ts=end_ts,
            cursor_id=cursor_id,
            limit=limit,
        )

    def upsert_market_ticks(
        self,
        rows: List[Dict[str, Any]],
        *,
        source_config_path: str = "",
    ) -> int:
        return self._repo().upsert_market_ticks(rows, source_config_path=source_config_path)

    def upsert_market_klines(
        self,
        rows: List[Dict[str, Any]],
        *,
        timeframe: str,
        source_config_path: str = "",
    ) -> int:
        return self._repo().upsert_market_klines(
            rows,
            timeframe=timeframe,
            source_config_path=source_config_path,
        )

    def build_db_report_summary(
        self,
        *,
        owner: Optional[str] = None,
        start_ts: Optional[str] = None,
        end_ts: Optional[str] = None,
        limit_top: int = 10,
    ) -> Dict[str, Any]:
        return self._repo().build_db_report_summary(
            owner=owner,
            start_ts=start_ts,
            end_ts=end_ts,
            limit_top=limit_top,
        )

    def ensure_user(
        self,
        username: str,
        *,
        role: str = "user",
        display_name: str = "",
    ) -> Dict[str, Any]:
        return self._repo().ensure_user(
            username,
            role=role,
            display_name=display_name,
        )

    def upsert_user_credential(
        self,
        *,
        username: str,
        password_hash: str,
        algorithm: str,
        password_updated_at: Optional[str] = None,
    ) -> None:
        self._repo().upsert_user_credential(
            username=username,
            password_hash=password_hash,
            algorithm=algorithm,
            password_updated_at=password_updated_at,
        )

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
        self._repo().create_auth_session(
            session_id=session_id,
            username=username,
            issued_at=issued_at,
            expires_at=expires_at,
            client_ip=client_ip,
            user_agent=user_agent,
        )

    def get_auth_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        return self._repo().get_auth_session(session_id)

    def revoke_auth_session(self, session_id: str, revoked_at: Optional[str] = None) -> None:
        self._repo().revoke_auth_session(session_id, revoked_at=revoked_at)

    def record_login_attempt(
        self,
        *,
        username: str,
        client_ip: str,
        success: bool,
        reason: str = "",
        ts_utc: Optional[str] = None,
    ) -> None:
        self._repo().record_login_attempt(
            username=username,
            client_ip=client_ip,
            success=success,
            reason=reason,
            ts_utc=ts_utc,
        )

    def set_lockout(
        self,
        *,
        lock_key: str,
        locked_until: str,
        updated_at: Optional[str] = None,
    ) -> None:
        self._repo().set_lockout(
            lock_key=lock_key,
            locked_until=locked_until,
            updated_at=updated_at,
        )

    def get_active_lockouts(
        self,
        *,
        lock_keys: List[str],
        now_ts: str,
    ) -> Dict[str, str]:
        return self._repo().get_active_lockouts(lock_keys=lock_keys, now_ts=now_ts)

    def clear_lockouts(self, lock_keys: List[str]) -> None:
        self._repo().clear_lockouts(lock_keys)

    def upsert_user_preferences(self, owner: str, preferences: Dict[str, Any]) -> None:
        self._repo().upsert_user_preferences(owner, preferences)

    def get_user_preferences(self, owner: str) -> Optional[Dict[str, Any]]:
        return self._repo().get_user_preferences(owner)

    def list_roles(self) -> List[Dict[str, Any]]:
        return self._repo().list_roles()

    def list_permissions(self) -> List[Dict[str, Any]]:
        return self._repo().list_permissions()

    def list_user_roles(self, username: str) -> List[str]:
        return self._repo().list_user_roles(username)

    def replace_user_roles(self, username: str, roles: List[str]) -> List[str]:
        return self._repo().replace_user_roles(username, roles)

    def user_has_permission(self, username: str, permission_code: str) -> bool:
        return bool(self._repo().user_has_permission(username, permission_code))

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
        self._repo().append_account_security_event(
            owner=owner,
            event_type=event_type,
            severity=severity,
            message=message,
            detail=detail,
            ts_utc=ts_utc,
        )

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
        return self._repo().list_account_security_events(
            owner=owner,
            event_type=event_type,
            start_ts=start_ts,
            end_ts=end_ts,
            cursor_id=cursor_id,
            limit=limit,
        )

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
        return self._repo().create_api_token(
            owner=owner,
            token_name=token_name,
            token_prefix=token_prefix,
            token_hash=token_hash,
            scopes=scopes,
            expires_at=expires_at,
            created_by=created_by,
            created_at=created_at,
        )

    def list_api_tokens(
        self,
        *,
        owner: Optional[str] = None,
        include_revoked: bool = False,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        return self._repo().list_api_tokens(
            owner=owner,
            include_revoked=include_revoked,
            limit=limit,
        )

    def get_active_api_token_by_hash(
        self,
        *,
        token_hash: str,
        now_ts: str,
    ) -> Optional[Dict[str, Any]]:
        return self._repo().get_active_api_token_by_hash(
            token_hash=token_hash,
            now_ts=now_ts,
        )

    def touch_api_token_last_used(
        self,
        token_id: int,
        *,
        last_used_at: Optional[str] = None,
    ) -> None:
        self._repo().touch_api_token_last_used(
            token_id,
            last_used_at=last_used_at,
        )

    def revoke_api_token(
        self,
        token_id: int,
        *,
        revoked_at: Optional[str] = None,
        revoked_by: str = "",
    ) -> None:
        self._repo().revoke_api_token(
            token_id,
            revoked_at=revoked_at,
            revoked_by=revoked_by,
        )

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
        self._repo().append_runtime_log(
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
        return self._repo().list_runtime_logs(
            owner=owner,
            log_type=log_type,
            level=level,
            q=q,
            strategy_id=strategy_id,
            start_ts=start_ts,
            end_ts=end_ts,
            cursor_id=cursor_id,
            limit=limit,
        )

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
        self._repo().append_alert_delivery(
            owner=owner,
            event=event,
            severity=severity,
            message=message,
            webhook_url=webhook_url,
            status=status,
            retry_count=retry_count,
            http_status=http_status,
            error_message=error_message,
            payload=payload,
            response_body=response_body,
            ts_utc=ts_utc,
            duration_ms=duration_ms,
        )

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
        return self._repo().list_alert_deliveries(
            owner=owner,
            event=event,
            status=status,
            start_ts=start_ts,
            end_ts=end_ts,
            cursor_id=cursor_id,
            limit=limit,
        )

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
        return int(
            self._repo().enqueue_alert_outbox(
                owner=owner,
                event=event,
                severity=severity,
                message=message,
                webhook_url=webhook_url,
                payload=payload,
                max_retries=max_retries,
                available_at=available_at,
                created_at=created_at,
            )
            or 0
        )

    def list_due_alert_outbox(
        self,
        *,
        now_ts: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        return self._repo().list_due_alert_outbox(
            now_ts=now_ts,
            limit=limit,
        )

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
        self._repo().finalize_alert_outbox(
            int(outbox_id),
            status=status,
            retry_count=retry_count,
            available_at=available_at,
            http_status=http_status,
            error_message=error_message,
            response_body=response_body,
            dispatched_at=dispatched_at,
        )

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
        self._repo().append_ws_connection_event(
            owner=owner,
            event_type=event_type,
            connection_id=connection_id,
            strategy_id=strategy_id,
            config_path=config_path,
            refresh_ms=refresh_ms,
            client_ip=client_ip,
            user_agent=user_agent,
            detail=detail,
            ts_utc=ts_utc,
        )

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
        return self._repo().list_ws_connection_events(
            owner=owner,
            event_type=event_type,
            strategy_id=strategy_id,
            start_ts=start_ts,
            end_ts=end_ts,
            cursor_id=cursor_id,
            limit=limit,
        )

    def append_strategy_diagnostics_snapshot(
        self,
        *,
        owner: str,
        strategy_id: str,
        source_path: str,
        snapshot: Dict[str, Any],
        ts_utc: Optional[str] = None,
    ) -> None:
        self._repo().append_strategy_diagnostics_snapshot(
            owner=owner,
            strategy_id=strategy_id,
            source_path=source_path,
            snapshot=snapshot,
            ts_utc=ts_utc,
        )

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
        return self._repo().list_strategy_diagnostics_snapshots(
            owner=owner,
            strategy_id=strategy_id,
            start_ts=start_ts,
            end_ts=end_ts,
            cursor_id=cursor_id,
            limit=limit,
            include_snapshot=include_snapshot,
        )

    def replace_backtest_trades(
        self,
        *,
        run_id: str,
        owner: str,
        rows: List[Dict[str, Any]],
    ) -> int:
        return self._repo().replace_backtest_trades(
            run_id=run_id,
            owner=owner,
            rows=rows,
        )

    def replace_backtest_equity_points(
        self,
        *,
        run_id: str,
        owner: str,
        rows: List[Dict[str, Any]],
    ) -> int:
        return self._repo().replace_backtest_equity_points(
            run_id=run_id,
            owner=owner,
            rows=rows,
        )

    def list_backtest_trades(
        self,
        *,
        run_id: str,
        owner: str,
        limit: int = 10000,
    ) -> List[Dict[str, Any]]:
        return self._repo().list_backtest_trades(
            run_id=run_id,
            owner=owner,
            limit=limit,
        )

    def list_backtest_equity_points(
        self,
        *,
        run_id: str,
        owner: str,
        limit: int = 10000,
    ) -> List[Dict[str, Any]]:
        return self._repo().list_backtest_equity_points(
            run_id=run_id,
            owner=owner,
            limit=limit,
        )

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
        return self._repo().upsert_data_file(
            owner=owner,
            scope=scope,
            file_key=file_key,
            file_name=file_name,
            source_path=source_path,
            content_type=content_type,
            content_encoding=content_encoding,
            content_text=content_text,
            meta=meta,
        )

    def get_data_file(
        self,
        *,
        owner: str,
        scope: str,
        file_key: str,
    ) -> Optional[Dict[str, Any]]:
        return self._repo().get_data_file(
            owner=owner,
            scope=scope,
            file_key=file_key,
        )
