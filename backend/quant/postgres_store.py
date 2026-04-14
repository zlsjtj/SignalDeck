from __future__ import annotations

import hashlib
import json
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from db_store import (
    _DEFAULT_RBAC_PERMISSIONS,
    _DEFAULT_RBAC_ROLE_CODES,
    _DEFAULT_RBAC_ROLE_PERMISSIONS,
    _DEFAULT_RBAC_ROLES,
    _as_float_or_none,
    _now_iso,
)

try:  # pragma: no cover - exercised only when postgres backend is enabled
    import psycopg
    from psycopg.rows import dict_row
except Exception:  # pragma: no cover
    psycopg = None
    dict_row = None

try:  # pragma: no cover - exercised only when postgres backend is enabled
    from psycopg_pool import ConnectionPool
except Exception:  # pragma: no cover
    ConnectionPool = None


class PostgresStore:
    backend = "postgres"

    def __init__(
        self,
        dsn: str,
        *,
        pool_enabled: bool = True,
        pool_min_size: int = 1,
        pool_max_size: int = 10,
        pool_timeout_seconds: float = 5.0,
    ) -> None:
        self.dsn = str(dsn or "").strip()
        self.db_path = Path("/dev/null")
        self._lock = threading.RLock()
        self._initialized = False
        self.pool_enabled = bool(pool_enabled)
        self.pool_min_size = max(1, int(pool_min_size))
        self.pool_max_size = max(self.pool_min_size, int(pool_max_size))
        self.pool_timeout_seconds = max(0.1, float(pool_timeout_seconds))
        self.pool_supported = bool(ConnectionPool is not None)
        self._pool = None

    def _connect(self):
        if psycopg is None or dict_row is None:
            raise RuntimeError("postgres backend requires 'psycopg' package, install with: pip install psycopg[binary]")
        if not self.dsn:
            raise RuntimeError("postgres backend requires non-empty API_DB_POSTGRES_DSN")
        if self.pool_enabled and ConnectionPool is not None:
            pool = self._ensure_pool()
            if pool is not None:
                return pool.connection(timeout=self.pool_timeout_seconds)
        conn = psycopg.connect(self.dsn, autocommit=False, row_factory=dict_row)
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout TO '5s'")
        conn.commit()
        return conn

    def _ensure_pool(self):
        if (not self.pool_enabled) or ConnectionPool is None:
            return None
        with self._lock:
            if self._pool is not None:
                return self._pool
            self._pool = ConnectionPool(
                conninfo=self.dsn,
                min_size=int(self.pool_min_size),
                max_size=int(self.pool_max_size),
                timeout=float(self.pool_timeout_seconds),
                kwargs={
                    "autocommit": False,
                    "row_factory": dict_row,
                },
                configure=self._configure_pool_connection,
                open=True,
            )
            return self._pool

    def _configure_pool_connection(self, conn) -> None:
        with conn.cursor() as cur:
            cur.execute("SET lock_timeout TO '5s'")
        conn.commit()

    def close(self) -> None:
        with self._lock:
            pool = self._pool
            self._pool = None
        if pool is not None:
            try:
                pool.close()
            except Exception:
                pass

    def initialize(self) -> None:
        with self._lock:
            if self._initialized:
                return
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS schema_version (
                            version INTEGER PRIMARY KEY,
                            applied_at TEXT NOT NULL,
                            description TEXT NOT NULL
                        )
                        """
                    )
                    self._apply_schema_migrations(cur)
                conn.commit()
            self._initialized = True

    def _apply_schema_migrations(self, cur) -> None:
        cur.execute("SELECT version FROM schema_version ORDER BY version ASC")
        applied_versions = {int(row["version"]) for row in cur.fetchall()}
        migrations = [
            (1, "bootstrap_core_tables", self._migration_v1_bootstrap_core_tables),
            (2, "add_idempotency_records_table", self._migration_v2_add_idempotency_records_table),
            (3, "add_structured_query_columns_and_risk_events", self._migration_v3_add_structured_query_columns_and_risk_events),
            (4, "add_strategy_params_and_risk_state_history", self._migration_v4_add_strategy_params_and_risk_state_history),
            (5, "add_audit_hash_chain", self._migration_v5_add_audit_hash_chain),
            (6, "add_market_timeseries_tables", self._migration_v6_add_market_timeseries_tables),
            (7, "add_strategy_compiler_and_auth_tables", self._migration_v7_add_strategy_compiler_and_auth_tables),
            (8, "add_user_preferences_table", self._migration_v8_add_user_preferences_table),
            (9, "add_runtime_logs_table", self._migration_v9_add_runtime_logs_table),
            (10, "add_strategy_diagnostics_snapshots_table", self._migration_v10_add_strategy_diagnostics_snapshots_table),
            (11, "add_backtest_detail_tables", self._migration_v11_add_backtest_detail_tables),
            (12, "add_alert_deliveries_table", self._migration_v12_add_alert_deliveries_table),
            (13, "add_ws_connection_events_table", self._migration_v13_add_ws_connection_events_table),
            (14, "add_account_security_and_api_tokens_tables", self._migration_v14_add_account_security_and_api_tokens_tables),
            (15, "add_rbac_tables", self._migration_v15_add_rbac_tables),
            (16, "add_owner_foreign_key_columns", self._migration_v16_add_owner_foreign_key_columns),
            (17, "strengthen_data_constraints", self._migration_v17_strengthen_data_constraints),
            (18, "add_postgres_typed_mirror_columns", self._migration_v18_add_postgres_typed_mirror_columns),
            (19, "add_alert_outbox_table", self._migration_v19_add_alert_outbox_table),
            (20, "add_monthly_time_partitions", self._migration_v20_add_monthly_time_partitions),
            (21, "add_data_files_table", self._migration_v21_add_data_files_table),
        ]
        for version, description, migration in migrations:
            if version in applied_versions:
                continue
            migration(cur)
            cur.execute(
                "INSERT INTO schema_version(version, applied_at, description) VALUES (%s, %s, %s)",
                (int(version), _now_iso(), str(description)),
            )

    def _migration_v1_bootstrap_core_tables(self, cur) -> None:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS strategies (
                strategy_key TEXT PRIMARY KEY,
                strategy_id TEXT NOT NULL,
                owner TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                record_json TEXT NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_strategies_owner ON strategies(owner)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_strategies_status ON strategies(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_strategies_updated_at ON strategies(updated_at DESC)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS backtests (
                run_id TEXT PRIMARY KEY,
                owner TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                record_json TEXT NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_backtests_owner ON backtests(owner)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_backtests_status ON backtests(status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_backtests_created_at ON backtests(created_at DESC)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS risk_states (
                owner TEXT NOT NULL,
                strategy_key TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                state_json TEXT NOT NULL,
                PRIMARY KEY (owner, strategy_key)
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_risk_owner ON risk_states(owner)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_logs (
                id BIGSERIAL PRIMARY KEY,
                ts_utc TEXT NOT NULL,
                owner TEXT NOT NULL,
                action TEXT NOT NULL,
                entity TEXT NOT NULL,
                entity_id TEXT NOT NULL,
                detail_json TEXT NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_owner_ts ON audit_logs(owner, ts_utc DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_logs(action)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_entity ON audit_logs(entity)")

    def _migration_v2_add_idempotency_records_table(self, cur) -> None:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS idempotency_records (
                owner TEXT NOT NULL,
                scope TEXT NOT NULL,
                idem_key TEXT NOT NULL,
                request_hash TEXT NOT NULL,
                response_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (owner, scope, idem_key)
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_idempotency_updated_at ON idempotency_records(updated_at DESC)")

    def _migration_v3_add_structured_query_columns_and_risk_events(self, cur) -> None:
        cur.execute("ALTER TABLE strategies ADD COLUMN IF NOT EXISTS strategy_name TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE strategies ADD COLUMN IF NOT EXISTS primary_symbol TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE strategies ADD COLUMN IF NOT EXISTS timeframe TEXT NOT NULL DEFAULT ''")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_strategies_name ON strategies(strategy_name)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_strategies_symbol ON strategies(primary_symbol)")

        cur.execute("ALTER TABLE backtests ADD COLUMN IF NOT EXISTS strategy_id TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE backtests ADD COLUMN IF NOT EXISTS strategy_name TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE backtests ADD COLUMN IF NOT EXISTS symbol TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE backtests ADD COLUMN IF NOT EXISTS start_at TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE backtests ADD COLUMN IF NOT EXISTS end_at TEXT NOT NULL DEFAULT ''")
        cur.execute("ALTER TABLE backtests ADD COLUMN IF NOT EXISTS metric_return DOUBLE PRECISION")
        cur.execute("ALTER TABLE backtests ADD COLUMN IF NOT EXISTS metric_sharpe DOUBLE PRECISION")
        cur.execute("ALTER TABLE backtests ADD COLUMN IF NOT EXISTS metric_calmar DOUBLE PRECISION")
        cur.execute("ALTER TABLE backtests ADD COLUMN IF NOT EXISTS metric_max_drawdown DOUBLE PRECISION")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_backtests_strategy_id ON backtests(strategy_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_backtests_symbol_period ON backtests(symbol, start_at, end_at)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_backtests_metric_return ON backtests(metric_return DESC)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS risk_events (
                id BIGSERIAL PRIMARY KEY,
                ts_utc TEXT NOT NULL,
                owner TEXT NOT NULL,
                strategy_key TEXT NOT NULL,
                event_type TEXT NOT NULL,
                rule TEXT NOT NULL,
                message TEXT NOT NULL,
                detail_json TEXT NOT NULL
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_risk_events_owner_ts ON risk_events(owner, ts_utc DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_risk_events_strategy_ts ON risk_events(strategy_key, ts_utc DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_risk_events_type ON risk_events(event_type)")

        cur.execute("SELECT strategy_key, record_json FROM strategies")
        for row in cur.fetchall():
            strategy_key = str(row["strategy_key"] or "")
            raw = str(row["record_json"] or "")
            if not strategy_key or not raw:
                continue
            try:
                record = json.loads(raw)
            except Exception:
                continue
            if not isinstance(record, dict):
                continue
            structured = self._extract_strategy_structured(strategy_key, record)
            cur.execute(
                """
                UPDATE strategies
                SET strategy_name = %s, primary_symbol = %s, timeframe = %s
                WHERE strategy_key = %s
                """,
                (
                    structured["strategy_name"],
                    structured["primary_symbol"],
                    structured["timeframe"],
                    strategy_key,
                ),
            )

        cur.execute("SELECT run_id, record_json FROM backtests")
        for row in cur.fetchall():
            run_id = str(row["run_id"] or "")
            raw = str(row["record_json"] or "")
            if not run_id or not raw:
                continue
            try:
                record = json.loads(raw)
            except Exception:
                continue
            if not isinstance(record, dict):
                continue
            structured = self._extract_backtest_structured(record)
            cur.execute(
                """
                UPDATE backtests
                SET strategy_id = %s,
                    strategy_name = %s,
                    symbol = %s,
                    start_at = %s,
                    end_at = %s,
                    metric_return = %s,
                    metric_sharpe = %s,
                    metric_calmar = %s,
                    metric_max_drawdown = %s
                WHERE run_id = %s
                """,
                (
                    structured["strategy_id"],
                    structured["strategy_name"],
                    structured["symbol"],
                    structured["start_at"],
                    structured["end_at"],
                    structured["metric_return"],
                    structured["metric_sharpe"],
                    structured["metric_calmar"],
                    structured["metric_max_drawdown"],
                    run_id,
                ),
            )

    def _table_columns(self, cur, table_name: str) -> List[str]:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema() AND table_name = %s
            """,
            (table_name,),
        )
        return [str(row["column_name"] or "") for row in cur.fetchall()]

    def _migration_v4_add_strategy_params_and_risk_state_history(self, cur) -> None:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_params (
                strategy_key TEXT NOT NULL,
                owner TEXT NOT NULL,
                param_key TEXT NOT NULL,
                param_value_text TEXT NOT NULL,
                param_value_num DOUBLE PRECISION,
                value_type TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (strategy_key, param_key)
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_strategy_params_owner ON strategy_params(owner)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_strategy_params_key ON strategy_params(param_key)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS risk_state_history (
                id BIGSERIAL PRIMARY KEY,
                owner TEXT NOT NULL,
                strategy_key TEXT NOT NULL,
                version INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                change_type TEXT NOT NULL,
                state_json TEXT NOT NULL
            )
            """
        )
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_risk_state_history_uk ON risk_state_history(owner, strategy_key, version)")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_risk_state_history_owner_strategy ON risk_state_history(owner, strategy_key, id DESC)"
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_risk_state_history_updated_at ON risk_state_history(updated_at DESC)")

        cur.execute("SELECT strategy_key, owner, record_json FROM strategies")
        for row in cur.fetchall():
            strategy_key = str(row["strategy_key"] or "")
            owner = str(row["owner"] or "")
            raw = str(row["record_json"] or "")
            if not strategy_key or not owner or not raw:
                continue
            try:
                record = json.loads(raw)
            except Exception:
                continue
            if not isinstance(record, dict):
                continue
            param_rows = self._extract_strategy_param_rows(strategy_key, owner, record, updated_at=_now_iso())
            if not param_rows:
                continue
            for item in param_rows:
                cur.execute(
                    """
                    INSERT INTO strategy_params (
                        strategy_key, owner, param_key, param_value_text, param_value_num, value_type, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(strategy_key, param_key) DO UPDATE SET
                        owner=EXCLUDED.owner,
                        param_value_text=EXCLUDED.param_value_text,
                        param_value_num=EXCLUDED.param_value_num,
                        value_type=EXCLUDED.value_type,
                        updated_at=EXCLUDED.updated_at
                    """,
                    item,
                )

        cur.execute("SELECT owner, strategy_key, updated_at, state_json FROM risk_states")
        for row in cur.fetchall():
            owner = str(row["owner"] or "")
            strategy_key = str(row["strategy_key"] or "")
            updated_at = str(row["updated_at"] or _now_iso())
            state_json = str(row["state_json"] or "{}")
            if not owner or not strategy_key:
                continue
            cur.execute(
                """
                INSERT INTO risk_state_history (
                    owner, strategy_key, version, updated_at, change_type, state_json
                ) VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT(owner, strategy_key, version) DO NOTHING
                """,
                (owner, strategy_key, 1, updated_at, "upsert", state_json),
            )

    def _audit_row_hash(
        self,
        *,
        owner: str,
        ts_utc: str,
        action: str,
        entity: str,
        entity_id: str,
        detail_json: str,
        prev_hash: str,
    ) -> str:
        basis = "\n".join(
            [
                str(owner or ""),
                str(ts_utc or ""),
                str(action or ""),
                str(entity or ""),
                str(entity_id or ""),
                str(detail_json or ""),
                str(prev_hash or ""),
            ]
        )
        return hashlib.sha256(basis.encode("utf-8")).hexdigest()

    def _migration_v5_add_audit_hash_chain(self, cur) -> None:
        columns = set(self._table_columns(cur, "audit_logs"))
        if "prev_hash" not in columns:
            cur.execute("ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS prev_hash TEXT NOT NULL DEFAULT ''")
        if "row_hash" not in columns:
            cur.execute("ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS row_hash TEXT NOT NULL DEFAULT ''")
        if "chain_version" not in columns:
            cur.execute("ALTER TABLE audit_logs ADD COLUMN IF NOT EXISTS chain_version INTEGER NOT NULL DEFAULT 1")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_owner_id ON audit_logs(owner, id DESC)")

        cur.execute(
            """
            SELECT id, ts_utc, owner, action, entity, entity_id, detail_json
            FROM audit_logs
            ORDER BY owner ASC, id ASC
            """
        )
        last_hash_by_owner: Dict[str, str] = {}
        for row in cur.fetchall():
            owner = str(row["owner"] or "")
            prev_hash = last_hash_by_owner.get(owner) or ("0" * 64)
            detail_json = str(row["detail_json"] or "{}")
            row_hash = self._audit_row_hash(
                owner=owner,
                ts_utc=str(row["ts_utc"] or ""),
                action=str(row["action"] or ""),
                entity=str(row["entity"] or ""),
                entity_id=str(row["entity_id"] or ""),
                detail_json=detail_json,
                prev_hash=prev_hash,
            )
            cur.execute(
                "UPDATE audit_logs SET prev_hash = %s, row_hash = %s, chain_version = 1 WHERE id = %s",
                (prev_hash, row_hash, int(row["id"])),
            )
            last_hash_by_owner[owner] = row_hash

    def _migration_v6_add_market_timeseries_tables(self, cur) -> None:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS market_ticks (
                symbol TEXT NOT NULL,
                ts_utc TEXT NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                bid DOUBLE PRECISION NOT NULL,
                ask DOUBLE PRECISION NOT NULL,
                volume DOUBLE PRECISION NOT NULL,
                source_config_path TEXT NOT NULL,
                ingested_at TEXT NOT NULL,
                PRIMARY KEY (symbol, ts_utc)
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_market_ticks_ts ON market_ticks(ts_utc DESC)")

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS market_klines (
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                ts_utc TEXT NOT NULL,
                time_sec BIGINT NOT NULL,
                open DOUBLE PRECISION NOT NULL,
                high DOUBLE PRECISION NOT NULL,
                low DOUBLE PRECISION NOT NULL,
                close DOUBLE PRECISION NOT NULL,
                volume DOUBLE PRECISION NOT NULL,
                source_config_path TEXT NOT NULL,
                ingested_at TEXT NOT NULL,
                PRIMARY KEY (symbol, timeframe, ts_utc)
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_market_klines_ts ON market_klines(ts_utc DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_market_klines_symbol_tf_time ON market_klines(symbol, timeframe, time_sec DESC)")

    def _migration_v7_add_strategy_compiler_and_auth_tables(self, cur) -> None:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_compiler_jobs (
                id BIGSERIAL PRIMARY KEY,
                strategy_key TEXT NOT NULL,
                owner TEXT NOT NULL,
                status TEXT NOT NULL,
                error_message TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                started_at TEXT NOT NULL DEFAULT '',
                finished_at TEXT NOT NULL DEFAULT ''
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_compiler_jobs_owner_created_at ON strategy_compiler_jobs(owner, created_at DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_compiler_jobs_strategy_created_at ON strategy_compiler_jobs(strategy_key, created_at DESC)"
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_scripts (
                id BIGSERIAL PRIMARY KEY,
                strategy_key TEXT NOT NULL,
                owner TEXT NOT NULL,
                version INTEGER NOT NULL,
                script_type TEXT NOT NULL,
                script_path TEXT NOT NULL,
                script_hash TEXT NOT NULL,
                source_config_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_strategy_scripts_uk ON strategy_scripts(strategy_key, version)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_strategy_scripts_owner_strategy_version ON strategy_scripts(owner, strategy_key, version DESC)"
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id BIGSERIAL PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                status TEXT NOT NULL DEFAULT 'active',
                display_name TEXT NOT NULL DEFAULT '',
                role TEXT NOT NULL DEFAULT 'user',
                created_at TEXT NOT NULL,
                last_login_at TEXT NOT NULL DEFAULT ''
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_credentials (
                user_id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                password_hash TEXT NOT NULL,
                algorithm TEXT NOT NULL,
                password_updated_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_sessions (
                session_id TEXT PRIMARY KEY,
                user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                username TEXT NOT NULL,
                issued_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                revoked_at TEXT NOT NULL DEFAULT '',
                client_ip TEXT NOT NULL DEFAULT '',
                user_agent TEXT NOT NULL DEFAULT ''
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_auth_sessions_user_expires ON auth_sessions(user_id, expires_at DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_auth_sessions_username_expires ON auth_sessions(username, expires_at DESC)"
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_login_attempts (
                id BIGSERIAL PRIMARY KEY,
                username TEXT NOT NULL,
                client_ip TEXT NOT NULL,
                success BOOLEAN NOT NULL,
                reason TEXT NOT NULL DEFAULT '',
                ts_utc TEXT NOT NULL
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_auth_attempts_user_ts ON auth_login_attempts(username, ts_utc DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_auth_attempts_ip_ts ON auth_login_attempts(client_ip, ts_utc DESC)"
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_lockouts (
                lock_key TEXT PRIMARY KEY,
                locked_until TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )

        owners = set()
        for table_name in ("strategies", "backtests", "risk_states", "audit_logs", "risk_events"):
            try:
                cur.execute(f"SELECT DISTINCT owner FROM {table_name}")
                rows = cur.fetchall()
            except Exception:
                continue
            for row in rows:
                owner = str(row.get("owner") or "").strip()
                if owner:
                    owners.add(owner)
        now_iso = _now_iso()
        for owner in owners:
            role = "admin" if owner == "admin" else ("guest" if owner == "guest" else "user")
            cur.execute(
                """
                INSERT INTO users (username, status, display_name, role, created_at, last_login_at)
                VALUES (%s, 'active', %s, %s, %s, '')
                ON CONFLICT(username) DO UPDATE SET
                    status='active',
                    display_name=EXCLUDED.display_name,
                    role=EXCLUDED.role
                """,
                (owner, owner, role, now_iso),
            )

    def _migration_v8_add_user_preferences_table(self, cur) -> None:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_preferences (
                owner TEXT PRIMARY KEY,
                preferences_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_user_preferences_updated_at ON user_preferences(updated_at DESC)"
        )

    def _migration_v9_add_runtime_logs_table(self, cur) -> None:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS runtime_logs (
                id BIGSERIAL PRIMARY KEY,
                ts_utc TEXT NOT NULL,
                owner TEXT NOT NULL,
                log_type TEXT NOT NULL,
                level TEXT NOT NULL,
                source TEXT NOT NULL,
                message TEXT NOT NULL,
                strategy_id TEXT NOT NULL DEFAULT '',
                backtest_id TEXT NOT NULL DEFAULT '',
                detail_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_runtime_logs_owner_ts ON runtime_logs(owner, ts_utc DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_runtime_logs_type_ts ON runtime_logs(log_type, ts_utc DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_runtime_logs_strategy_ts ON runtime_logs(strategy_id, ts_utc DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_runtime_logs_backtest_ts ON runtime_logs(backtest_id, ts_utc DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_runtime_logs_level_ts ON runtime_logs(level, ts_utc DESC)"
        )

    def _migration_v10_add_strategy_diagnostics_snapshots_table(self, cur) -> None:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS strategy_diagnostics_snapshots (
                id BIGSERIAL PRIMARY KEY,
                ts_utc TEXT NOT NULL,
                owner TEXT NOT NULL,
                strategy_id TEXT NOT NULL,
                source_path TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                strategy_state TEXT NOT NULL,
                data_source_status TEXT NOT NULL,
                entry_signal BOOLEAN NOT NULL,
                exception_total_count INTEGER NOT NULL,
                filter_reasons_json TEXT NOT NULL,
                snapshot_json TEXT NOT NULL
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_diag_snapshots_owner_ts ON strategy_diagnostics_snapshots(owner, ts_utc DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_diag_snapshots_strategy_ts ON strategy_diagnostics_snapshots(strategy_id, ts_utc DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_diag_snapshots_generated_at ON strategy_diagnostics_snapshots(generated_at DESC)"
        )

    def _migration_v11_add_backtest_detail_tables(self, cur) -> None:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_trades (
                id BIGSERIAL PRIMARY KEY,
                run_id TEXT NOT NULL,
                owner TEXT NOT NULL,
                seq INTEGER NOT NULL,
                trade_id TEXT NOT NULL DEFAULT '',
                ts_utc TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                qty DOUBLE PRECISION NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                fee DOUBLE PRECISION NOT NULL,
                pnl DOUBLE PRECISION NOT NULL,
                order_id TEXT NOT NULL DEFAULT '',
                extra_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_backtest_trades_owner_run_seq ON backtest_trades(owner, run_id, seq ASC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_backtest_trades_run_ts ON backtest_trades(run_id, ts_utc DESC, id DESC)"
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_equity_points (
                id BIGSERIAL PRIMARY KEY,
                run_id TEXT NOT NULL,
                owner TEXT NOT NULL,
                seq INTEGER NOT NULL,
                ts_utc TEXT NOT NULL,
                equity DOUBLE PRECISION NOT NULL,
                pnl DOUBLE PRECISION NOT NULL,
                dd DOUBLE PRECISION NOT NULL
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_backtest_equity_owner_run_seq ON backtest_equity_points(owner, run_id, seq ASC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_backtest_equity_run_ts ON backtest_equity_points(run_id, ts_utc DESC, id DESC)"
        )

    def _migration_v12_add_alert_deliveries_table(self, cur) -> None:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_deliveries (
                id BIGSERIAL PRIMARY KEY,
                ts_utc TEXT NOT NULL,
                owner TEXT NOT NULL,
                event TEXT NOT NULL,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                webhook_url TEXT NOT NULL,
                status TEXT NOT NULL,
                retry_count INTEGER NOT NULL,
                http_status INTEGER,
                error_message TEXT NOT NULL DEFAULT '',
                payload_json TEXT NOT NULL DEFAULT '{}',
                response_body TEXT NOT NULL DEFAULT '',
                duration_ms DOUBLE PRECISION NOT NULL DEFAULT 0
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_alert_deliveries_owner_ts ON alert_deliveries(owner, ts_utc DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_alert_deliveries_event_ts ON alert_deliveries(event, ts_utc DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_alert_deliveries_status_ts ON alert_deliveries(status, ts_utc DESC)"
        )

    def _migration_v13_add_ws_connection_events_table(self, cur) -> None:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ws_connection_events (
                id BIGSERIAL PRIMARY KEY,
                ts_utc TEXT NOT NULL,
                owner TEXT NOT NULL,
                event_type TEXT NOT NULL,
                connection_id TEXT NOT NULL,
                strategy_id TEXT NOT NULL DEFAULT '',
                config_path TEXT NOT NULL DEFAULT '',
                refresh_ms INTEGER NOT NULL DEFAULT 0,
                client_ip TEXT NOT NULL DEFAULT '',
                user_agent TEXT NOT NULL DEFAULT '',
                detail_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_ws_events_owner_ts ON ws_connection_events(owner, ts_utc DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_ws_events_type_ts ON ws_connection_events(event_type, ts_utc DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_ws_events_strategy_ts ON ws_connection_events(strategy_id, ts_utc DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_ws_events_conn_ts ON ws_connection_events(connection_id, ts_utc DESC)"
        )

    def _migration_v14_add_account_security_and_api_tokens_tables(self, cur) -> None:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS account_security_events (
                id BIGSERIAL PRIMARY KEY,
                ts_utc TEXT NOT NULL,
                owner TEXT NOT NULL,
                event_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                detail_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_account_security_owner_ts ON account_security_events(owner, ts_utc DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_account_security_event_ts ON account_security_events(event_type, ts_utc DESC)"
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS api_tokens (
                id BIGSERIAL PRIMARY KEY,
                owner TEXT NOT NULL,
                token_name TEXT NOT NULL DEFAULT '',
                token_prefix TEXT NOT NULL,
                token_hash TEXT NOT NULL UNIQUE,
                scopes_json TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL DEFAULT '',
                last_used_at TEXT NOT NULL DEFAULT '',
                revoked_at TEXT NOT NULL DEFAULT '',
                created_by TEXT NOT NULL DEFAULT '',
                revoked_by TEXT NOT NULL DEFAULT ''
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_api_tokens_owner_created ON api_tokens(owner, created_at DESC)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_api_tokens_active ON api_tokens(revoked_at, expires_at, id DESC)"
        )

    def _migration_v15_add_rbac_tables(self, cur) -> None:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS roles (
                role_code TEXT PRIMARY KEY,
                description TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS permissions (
                permission_code TEXT PRIMARY KEY,
                description TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS role_permissions (
                role_code TEXT NOT NULL REFERENCES roles(role_code) ON DELETE CASCADE,
                permission_code TEXT NOT NULL REFERENCES permissions(permission_code) ON DELETE CASCADE,
                created_at TEXT NOT NULL,
                PRIMARY KEY (role_code, permission_code)
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_role_permissions_permission ON role_permissions(permission_code, role_code)"
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_roles (
                username TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
                role_code TEXT NOT NULL REFERENCES roles(role_code) ON DELETE CASCADE,
                bound_at TEXT NOT NULL,
                PRIMARY KEY (username, role_code)
            )
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_user_roles_role_user ON user_roles(role_code, username)"
        )

        now_iso = _now_iso()
        for role_code, description in _DEFAULT_RBAC_ROLES:
            cur.execute(
                """
                INSERT INTO roles (role_code, description, created_at)
                VALUES (%s, %s, %s)
                ON CONFLICT(role_code) DO NOTHING
                """,
                (str(role_code), str(description), now_iso),
            )
        for permission_code, description in _DEFAULT_RBAC_PERMISSIONS:
            cur.execute(
                """
                INSERT INTO permissions (permission_code, description, created_at)
                VALUES (%s, %s, %s)
                ON CONFLICT(permission_code) DO NOTHING
                """,
                (str(permission_code), str(description), now_iso),
            )
        for role_code, permission_codes in _DEFAULT_RBAC_ROLE_PERMISSIONS.items():
            for permission_code in permission_codes:
                cur.execute(
                    """
                    INSERT INTO role_permissions (role_code, permission_code, created_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT(role_code, permission_code) DO NOTHING
                    """,
                    (str(role_code), str(permission_code), now_iso),
                )

        cur.execute("SELECT username, role FROM users")
        for row in cur.fetchall():
            username = str(row.get("username") or "").strip().lower()
            if not username:
                continue
            role_code = str(row.get("role") or "user").strip().lower() or "user"
            if role_code not in _DEFAULT_RBAC_ROLE_CODES:
                role_code = "user"
            cur.execute(
                """
                INSERT INTO user_roles (username, role_code, bound_at)
                VALUES (%s, %s, %s)
                ON CONFLICT(username, role_code) DO NOTHING
                """,
                (username, role_code, now_iso),
            )

    def _migration_v16_add_owner_foreign_key_columns(self, cur) -> None:
        owner_tables = [
            "strategies",
            "backtests",
            "risk_states",
            "risk_state_history",
            "audit_logs",
            "risk_events",
            "strategy_compiler_jobs",
            "strategy_scripts",
            "user_preferences",
            "runtime_logs",
            "strategy_diagnostics_snapshots",
            "backtest_trades",
            "backtest_equity_points",
            "alert_deliveries",
            "ws_connection_events",
            "account_security_events",
            "api_tokens",
        ]
        now_iso = _now_iso()

        owners = set()
        for table_name in owner_tables:
            try:
                cur.execute(f"SELECT DISTINCT owner FROM {table_name}")
                rows = cur.fetchall()
            except Exception:
                continue
            for row in rows:
                owner = str(row.get("owner") or "").strip().lower()
                if owner:
                    owners.add(owner)
        for owner in owners:
            role = "admin" if owner == "admin" else ("guest" if owner == "guest" else "user")
            cur.execute(
                """
                INSERT INTO users (username, status, display_name, role, created_at, last_login_at)
                VALUES (%s, 'active', %s, %s, %s, '')
                ON CONFLICT(username) DO UPDATE SET
                    status='active',
                    display_name=EXCLUDED.display_name,
                    role=EXCLUDED.role
                """,
                (owner, owner, role, now_iso),
            )

        for table_name in owner_tables:
            cur.execute(
                f"""
                ALTER TABLE {table_name}
                ADD COLUMN IF NOT EXISTS owner_user_id BIGINT REFERENCES users(id)
                """
            )
            cur.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{table_name}_owner_user_id ON {table_name}(owner_user_id)"
            )
            cur.execute(
                f"""
                UPDATE {table_name}
                SET owner = LOWER(BTRIM(owner))
                WHERE owner IS NOT NULL AND owner <> LOWER(BTRIM(owner))
                """
            )
            cur.execute(
                f"""
                UPDATE {table_name}
                SET owner_user_id = u.id
                FROM users u
                WHERE COALESCE({table_name}.owner, '') <> ''
                  AND u.username = LOWER(BTRIM({table_name}.owner))
                  AND ({table_name}.owner_user_id IS NULL OR {table_name}.owner_user_id <= 0)
                """
            )

        cur.execute(
            """
            CREATE OR REPLACE FUNCTION sync_owner_user_id()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $$
            DECLARE
                owner_text TEXT;
                role_text TEXT;
                resolved_id BIGINT;
            BEGIN
                owner_text := LOWER(BTRIM(COALESCE(NEW.owner, '')));
                NEW.owner := owner_text;
                IF owner_text = '' THEN
                    NEW.owner_user_id := NULL;
                    RETURN NEW;
                END IF;

                role_text := CASE
                    WHEN owner_text = 'admin' THEN 'admin'
                    WHEN owner_text = 'guest' THEN 'guest'
                    ELSE 'user'
                END;
                INSERT INTO users (username, status, display_name, role, created_at, last_login_at)
                VALUES (owner_text, 'active', owner_text, role_text, CURRENT_TIMESTAMP::TEXT, '')
                ON CONFLICT(username) DO NOTHING;

                SELECT id INTO resolved_id FROM users WHERE username = owner_text LIMIT 1;
                NEW.owner_user_id := resolved_id;
                RETURN NEW;
            END;
            $$;
            """
        )
        for table_name in owner_tables:
            cur.execute(f"DROP TRIGGER IF EXISTS trg_{table_name}_owner_user_id ON {table_name}")
            cur.execute(
                f"""
                CREATE TRIGGER trg_{table_name}_owner_user_id
                BEFORE INSERT OR UPDATE OF owner ON {table_name}
                FOR EACH ROW
                EXECUTE FUNCTION sync_owner_user_id()
                """
            )

    def _migration_v17_strengthen_data_constraints(self, cur) -> None:
        cur.execute(
            """
            UPDATE strategy_compiler_jobs
            SET status = CASE
                WHEN LOWER(BTRIM(status)) IN ('pending', 'running', 'success', 'failed')
                    THEN LOWER(BTRIM(status))
                ELSE 'failed'
            END
            """
        )
        cur.execute(
            """
            UPDATE risk_events
            SET event_type = CASE
                WHEN LOWER(BTRIM(event_type)) IN ('triggered', 'recovered', 'manual_update')
                    THEN LOWER(BTRIM(event_type))
                ELSE 'manual_update'
            END
            """
        )
        cur.execute(
            """
            UPDATE runtime_logs
            SET level = CASE
                WHEN LOWER(BTRIM(level)) IN ('debug', 'info', 'warn', 'error', 'critical')
                    THEN LOWER(BTRIM(level))
                ELSE 'info'
            END
            """
        )
        cur.execute(
            """
            UPDATE alert_deliveries
            SET status = CASE
                WHEN LOWER(BTRIM(status)) IN ('sent', 'failed')
                    THEN LOWER(BTRIM(status))
                ELSE 'failed'
            END,
                severity = CASE
                    WHEN LOWER(BTRIM(severity)) IN ('info', 'warn', 'error', 'critical')
                        THEN LOWER(BTRIM(severity))
                    ELSE 'info'
                END,
                retry_count = CASE WHEN retry_count >= 0 THEN retry_count ELSE 0 END,
                duration_ms = CASE WHEN duration_ms >= 0 THEN duration_ms ELSE 0 END
            """
        )
        cur.execute(
            """
            UPDATE ws_connection_events
            SET refresh_ms = CASE WHEN refresh_ms >= 0 THEN refresh_ms ELSE 0 END
            """
        )
        cur.execute(
            """
            UPDATE account_security_events
            SET severity = CASE
                WHEN LOWER(BTRIM(severity)) IN ('info', 'warn', 'error', 'critical')
                    THEN LOWER(BTRIM(severity))
                ELSE 'info'
            END
            """
        )
        cur.execute(
            """
            UPDATE api_tokens
            SET owner = LOWER(BTRIM(owner)),
                token_name = BTRIM(token_name)
            """
        )

        # Keep newest active token name, rename older duplicates to unblock unique index creation.
        seen_active_names = set()
        cur.execute(
            """
            SELECT id, owner, token_name
            FROM api_tokens
            WHERE token_name <> '' AND revoked_at = ''
            ORDER BY owner ASC, token_name ASC, id DESC
            """
        )
        for row in cur.fetchall():
            token_id = int(row["id"] or 0)
            owner = str(row["owner"] or "")
            token_name = str(row["token_name"] or "")
            key = (owner, token_name)
            if key not in seen_active_names:
                seen_active_names.add(key)
                continue
            suffix = max(1, token_id)
            candidate = f"{token_name}#{suffix}"
            while (owner, candidate) in seen_active_names:
                suffix += 1
                candidate = f"{token_name}#{suffix}"
            cur.execute(
                "UPDATE api_tokens SET token_name = %s WHERE id = %s",
                (candidate, token_id),
            )
            seen_active_names.add((owner, candidate))

        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_api_tokens_owner_name_active_uk
            ON api_tokens(owner, token_name)
            WHERE token_name <> '' AND revoked_at = ''
            """
        )

        constraints = [
            (
                "strategy_compiler_jobs",
                "chk_compiler_jobs_status_enum",
                "status <> '' AND LOWER(BTRIM(status)) IN ('pending', 'running', 'success', 'failed')",
            ),
            (
                "risk_events",
                "chk_risk_events_event_type_enum",
                "event_type <> '' AND LOWER(BTRIM(event_type)) IN ('triggered', 'recovered', 'manual_update')",
            ),
            (
                "runtime_logs",
                "chk_runtime_logs_level_enum",
                "level <> '' AND LOWER(BTRIM(level)) IN ('debug', 'info', 'warn', 'error', 'critical')",
            ),
            (
                "alert_deliveries",
                "chk_alert_deliveries_status_enum",
                "status <> '' AND LOWER(BTRIM(status)) IN ('sent', 'failed')",
            ),
            (
                "alert_deliveries",
                "chk_alert_deliveries_severity_enum",
                "severity <> '' AND LOWER(BTRIM(severity)) IN ('info', 'warn', 'error', 'critical')",
            ),
            (
                "alert_deliveries",
                "chk_alert_deliveries_non_negative",
                "retry_count >= 0 AND duration_ms >= 0",
            ),
            (
                "ws_connection_events",
                "chk_ws_connection_events_refresh_non_negative",
                "refresh_ms >= 0",
            ),
            (
                "account_security_events",
                "chk_account_security_severity_enum",
                "severity <> '' AND LOWER(BTRIM(severity)) IN ('info', 'warn', 'error', 'critical')",
            ),
        ]
        for table_name, constraint_name, expression in constraints:
            cur.execute(
                """
                SELECT 1
                FROM pg_constraint
                WHERE conname = %s AND conrelid = %s::regclass
                LIMIT 1
                """,
                (constraint_name, table_name),
            )
            exists = cur.fetchone() is not None
            if not exists:
                cur.execute(
                    f"""
                    ALTER TABLE {table_name}
                    ADD CONSTRAINT {constraint_name}
                    CHECK ({expression}) NOT VALID
                    """
                )
            cur.execute(f"ALTER TABLE {table_name} VALIDATE CONSTRAINT {constraint_name}")

    def _migration_v18_add_postgres_typed_mirror_columns(self, cur) -> None:
        # Defensive cast helpers allow generated columns to tolerate historical malformed payloads.
        cur.execute(
            """
            CREATE OR REPLACE FUNCTION quant_safe_jsonb(payload TEXT)
            RETURNS JSONB
            LANGUAGE plpgsql
            IMMUTABLE
            AS $$
            BEGIN
                IF payload IS NULL OR BTRIM(payload) = '' THEN
                    RETURN '{}'::jsonb;
                END IF;
                RETURN payload::jsonb;
            EXCEPTION
                WHEN OTHERS THEN
                    RETURN '{}'::jsonb;
            END;
            $$;
            """
        )
        cur.execute(
            """
            CREATE OR REPLACE FUNCTION quant_safe_timestamptz(payload TEXT)
            RETURNS TIMESTAMPTZ
            LANGUAGE plpgsql
            IMMUTABLE
            AS $$
            BEGIN
                IF payload IS NULL OR BTRIM(payload) = '' THEN
                    RETURN NULL;
                END IF;
                RETURN payload::timestamptz;
            EXCEPTION
                WHEN OTHERS THEN
                    RETURN NULL;
            END;
            $$;
            """
        )

        # JSONB mirror columns + GIN indexes (keeps existing TEXT fields for backward compatibility).
        cur.execute(
            """
            ALTER TABLE strategies
            ADD COLUMN IF NOT EXISTS record_jsonb JSONB
            GENERATED ALWAYS AS (quant_safe_jsonb(record_json)) STORED
            """
        )
        cur.execute(
            """
            ALTER TABLE backtests
            ADD COLUMN IF NOT EXISTS record_jsonb JSONB
            GENERATED ALWAYS AS (quant_safe_jsonb(record_json)) STORED
            """
        )
        cur.execute(
            """
            ALTER TABLE risk_states
            ADD COLUMN IF NOT EXISTS state_jsonb JSONB
            GENERATED ALWAYS AS (quant_safe_jsonb(state_json)) STORED
            """
        )
        cur.execute(
            """
            ALTER TABLE risk_state_history
            ADD COLUMN IF NOT EXISTS state_jsonb JSONB
            GENERATED ALWAYS AS (quant_safe_jsonb(state_json)) STORED
            """
        )
        cur.execute(
            """
            ALTER TABLE audit_logs
            ADD COLUMN IF NOT EXISTS detail_jsonb JSONB
            GENERATED ALWAYS AS (quant_safe_jsonb(detail_json)) STORED
            """
        )
        cur.execute(
            """
            ALTER TABLE risk_events
            ADD COLUMN IF NOT EXISTS detail_jsonb JSONB
            GENERATED ALWAYS AS (quant_safe_jsonb(detail_json)) STORED
            """
        )
        cur.execute(
            """
            ALTER TABLE runtime_logs
            ADD COLUMN IF NOT EXISTS detail_jsonb JSONB
            GENERATED ALWAYS AS (quant_safe_jsonb(detail_json)) STORED
            """
        )
        cur.execute(
            """
            ALTER TABLE ws_connection_events
            ADD COLUMN IF NOT EXISTS detail_jsonb JSONB
            GENERATED ALWAYS AS (quant_safe_jsonb(detail_json)) STORED
            """
        )
        cur.execute(
            """
            ALTER TABLE account_security_events
            ADD COLUMN IF NOT EXISTS detail_jsonb JSONB
            GENERATED ALWAYS AS (quant_safe_jsonb(detail_json)) STORED
            """
        )

        cur.execute("CREATE INDEX IF NOT EXISTS idx_strategies_record_jsonb_gin ON strategies USING GIN (record_jsonb)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_backtests_record_jsonb_gin ON backtests USING GIN (record_jsonb)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_risk_states_state_jsonb_gin ON risk_states USING GIN (state_jsonb)")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_risk_state_history_state_jsonb_gin ON risk_state_history USING GIN (state_jsonb)"
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_logs_detail_jsonb_gin ON audit_logs USING GIN (detail_jsonb)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_risk_events_detail_jsonb_gin ON risk_events USING GIN (detail_jsonb)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_runtime_logs_detail_jsonb_gin ON runtime_logs USING GIN (detail_jsonb)")
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_ws_connection_events_detail_jsonb_gin ON ws_connection_events USING GIN (detail_jsonb)"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_account_security_events_detail_jsonb_gin ON account_security_events USING GIN (detail_jsonb)"
        )

        # TIMESTAMPTZ mirror columns for incremental transition from text timestamps.
        cur.execute(
            """
            ALTER TABLE strategies
            ADD COLUMN IF NOT EXISTS created_at_tz TIMESTAMPTZ
            GENERATED ALWAYS AS (quant_safe_timestamptz(created_at)) STORED
            """
        )
        cur.execute(
            """
            ALTER TABLE strategies
            ADD COLUMN IF NOT EXISTS updated_at_tz TIMESTAMPTZ
            GENERATED ALWAYS AS (quant_safe_timestamptz(updated_at)) STORED
            """
        )
        cur.execute(
            """
            ALTER TABLE backtests
            ADD COLUMN IF NOT EXISTS created_at_tz TIMESTAMPTZ
            GENERATED ALWAYS AS (quant_safe_timestamptz(created_at)) STORED
            """
        )
        cur.execute(
            """
            ALTER TABLE backtests
            ADD COLUMN IF NOT EXISTS updated_at_tz TIMESTAMPTZ
            GENERATED ALWAYS AS (quant_safe_timestamptz(updated_at)) STORED
            """
        )
        cur.execute(
            """
            ALTER TABLE audit_logs
            ADD COLUMN IF NOT EXISTS ts_utc_tz TIMESTAMPTZ
            GENERATED ALWAYS AS (quant_safe_timestamptz(ts_utc)) STORED
            """
        )
        cur.execute(
            """
            ALTER TABLE risk_events
            ADD COLUMN IF NOT EXISTS ts_utc_tz TIMESTAMPTZ
            GENERATED ALWAYS AS (quant_safe_timestamptz(ts_utc)) STORED
            """
        )
        cur.execute(
            """
            ALTER TABLE market_ticks
            ADD COLUMN IF NOT EXISTS ts_utc_tz TIMESTAMPTZ
            GENERATED ALWAYS AS (quant_safe_timestamptz(ts_utc)) STORED
            """
        )
        cur.execute(
            """
            ALTER TABLE market_klines
            ADD COLUMN IF NOT EXISTS ts_utc_tz TIMESTAMPTZ
            GENERATED ALWAYS AS (quant_safe_timestamptz(ts_utc)) STORED
            """
        )
        cur.execute(
            """
            ALTER TABLE runtime_logs
            ADD COLUMN IF NOT EXISTS ts_utc_tz TIMESTAMPTZ
            GENERATED ALWAYS AS (quant_safe_timestamptz(ts_utc)) STORED
            """
        )
        cur.execute(
            """
            ALTER TABLE strategy_diagnostics_snapshots
            ADD COLUMN IF NOT EXISTS ts_utc_tz TIMESTAMPTZ
            GENERATED ALWAYS AS (quant_safe_timestamptz(ts_utc)) STORED
            """
        )
        cur.execute(
            """
            ALTER TABLE ws_connection_events
            ADD COLUMN IF NOT EXISTS ts_utc_tz TIMESTAMPTZ
            GENERATED ALWAYS AS (quant_safe_timestamptz(ts_utc)) STORED
            """
        )
        cur.execute(
            """
            ALTER TABLE account_security_events
            ADD COLUMN IF NOT EXISTS ts_utc_tz TIMESTAMPTZ
            GENERATED ALWAYS AS (quant_safe_timestamptz(ts_utc)) STORED
            """
        )
        cur.execute(
            """
            ALTER TABLE alert_deliveries
            ADD COLUMN IF NOT EXISTS ts_utc_tz TIMESTAMPTZ
            GENERATED ALWAYS AS (quant_safe_timestamptz(ts_utc)) STORED
            """
        )

        cur.execute("CREATE INDEX IF NOT EXISTS idx_strategies_updated_at_tz ON strategies(updated_at_tz DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_backtests_created_at_tz ON backtests(created_at_tz DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_owner_ts_tz ON audit_logs(owner, ts_utc_tz DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_risk_events_owner_ts_tz ON risk_events(owner, ts_utc_tz DESC)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_runtime_logs_owner_ts_tz ON runtime_logs(owner, ts_utc_tz DESC)")

    def _migration_v19_add_alert_outbox_table(self, cur) -> None:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS alert_outbox (
                id BIGSERIAL PRIMARY KEY,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                available_at TEXT NOT NULL,
                owner TEXT NOT NULL,
                event TEXT NOT NULL,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                webhook_url TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                status TEXT NOT NULL DEFAULT 'pending',
                retry_count INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 0,
                http_status INTEGER,
                error_message TEXT NOT NULL DEFAULT '',
                response_body TEXT NOT NULL DEFAULT '',
                dispatched_at TEXT NOT NULL DEFAULT ''
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_alert_outbox_status_available
            ON alert_outbox(status, available_at ASC, id ASC)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_alert_outbox_owner_created
            ON alert_outbox(owner, created_at DESC)
            """
        )
        constraints = [
            (
                "chk_alert_outbox_status_enum",
                "status <> '' AND LOWER(BTRIM(status)) IN ('pending', 'sent', 'failed')",
            ),
            (
                "chk_alert_outbox_severity_enum",
                "severity <> '' AND LOWER(BTRIM(severity)) IN ('info', 'warn', 'error', 'critical')",
            ),
            (
                "chk_alert_outbox_non_negative",
                "retry_count >= 0 AND max_retries >= 0",
            ),
        ]
        for constraint_name, expression in constraints:
            cur.execute(
                """
                SELECT 1
                FROM pg_constraint
                WHERE conname = %s AND conrelid = 'alert_outbox'::regclass
                LIMIT 1
                """,
                (constraint_name,),
            )
            exists = cur.fetchone() is not None
            if not exists:
                cur.execute(
                    f"""
                    ALTER TABLE alert_outbox
                    ADD CONSTRAINT {constraint_name}
                    CHECK ({expression}) NOT VALID
                    """
                )
            cur.execute(f"ALTER TABLE alert_outbox VALIDATE CONSTRAINT {constraint_name}")

    def _migration_v20_add_monthly_time_partitions(self, cur) -> None:
        cur.execute(
            """
            CREATE OR REPLACE FUNCTION quant_ensure_monthly_partition(base_table TEXT, ts_value TIMESTAMPTZ)
            RETURNS TEXT
            LANGUAGE plpgsql
            AS $$
            DECLARE
                tbl TEXT := LOWER(BTRIM(COALESCE(base_table, '')));
                event_ts TIMESTAMPTZ := COALESCE(ts_value, NOW());
                month_start TIMESTAMPTZ := date_trunc('month', event_ts);
                month_end TIMESTAMPTZ := month_start + INTERVAL '1 month';
                suffix TEXT := to_char(month_start AT TIME ZONE 'UTC', 'YYYYMM');
                part_table TEXT;
                check_name TEXT;
                part_oid REGCLASS;
            BEGIN
                IF tbl NOT IN ('audit_logs', 'risk_events', 'market_ticks', 'market_klines') THEN
                    RAISE EXCEPTION 'unsupported partition base table: %', base_table;
                END IF;
                part_table := format('%s_p%s', tbl, suffix);
                part_oid := to_regclass(part_table);

                IF part_oid IS NULL THEN
                    EXECUTE format('CREATE TABLE IF NOT EXISTS %I () INHERITS (%I)', part_table, tbl);
                    part_oid := to_regclass(part_table);
                END IF;

                check_name := format('%s_ts_chk', part_table);
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname = check_name AND conrelid = part_oid
                    LIMIT 1
                ) THEN
                    EXECUTE format(
                        'ALTER TABLE %I ADD CONSTRAINT %I CHECK (ts_utc_tz >= %L::timestamptz AND ts_utc_tz < %L::timestamptz) NOT VALID',
                        part_table,
                        check_name,
                        month_start,
                        month_end
                    );
                    EXECUTE format('ALTER TABLE %I VALIDATE CONSTRAINT %I', part_table, check_name);
                END IF;

                IF tbl = 'audit_logs' THEN
                    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I(owner, ts_utc DESC)', format('idx_%s_owner_ts', part_table), part_table);
                    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I(action)', format('idx_%s_action', part_table), part_table);
                    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I(entity)', format('idx_%s_entity', part_table), part_table);
                    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I(owner, id DESC)', format('idx_%s_owner_id', part_table), part_table);
                    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I(owner, ts_utc_tz DESC)', format('idx_%s_owner_ts_tz', part_table), part_table);
                ELSIF tbl = 'risk_events' THEN
                    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I(owner, ts_utc DESC)', format('idx_%s_owner_ts', part_table), part_table);
                    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I(strategy_key, ts_utc DESC)', format('idx_%s_strategy_ts', part_table), part_table);
                    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I(event_type)', format('idx_%s_type', part_table), part_table);
                    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I(owner, ts_utc_tz DESC)', format('idx_%s_owner_ts_tz', part_table), part_table);
                ELSIF tbl = 'market_ticks' THEN
                    EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I(symbol, ts_utc)', format('uidx_%s_symbol_ts', part_table), part_table);
                    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I(ts_utc DESC)', format('idx_%s_ts', part_table), part_table);
                    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I(ts_utc_tz DESC)', format('idx_%s_ts_tz', part_table), part_table);
                ELSIF tbl = 'market_klines' THEN
                    EXECUTE format('CREATE UNIQUE INDEX IF NOT EXISTS %I ON %I(symbol, timeframe, ts_utc)', format('uidx_%s_symbol_tf_ts', part_table), part_table);
                    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I(ts_utc DESC)', format('idx_%s_ts', part_table), part_table);
                    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I(symbol, timeframe, time_sec DESC)', format('idx_%s_symbol_tf_time', part_table), part_table);
                    EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I(ts_utc_tz DESC)', format('idx_%s_ts_tz', part_table), part_table);
                END IF;

                RETURN part_table;
            END;
            $$;
            """
        )
        cur.execute(
            """
            CREATE OR REPLACE FUNCTION quant_route_audit_logs_monthly_partition()
            RETURNS TRIGGER
            LANGUAGE plpgsql
            AS $$
            DECLARE
                event_ts TIMESTAMPTZ := COALESCE(quant_safe_timestamptz(NEW.ts_utc), NOW());
                part_table TEXT;
            BEGIN
                part_table := quant_ensure_monthly_partition('audit_logs', event_ts);
                EXECUTE format(
                    'INSERT INTO %I (id, ts_utc, owner, action, entity, entity_id, detail_json, prev_hash, row_hash, chain_version)
                     VALUES ($1.id, $1.ts_utc, $1.owner, $1.action, $1.entity, $1.entity_id, $1.detail_json, $1.prev_hash, $1.row_hash, $1.chain_version)',
                    part_table
                )
                USING NEW;
                RETURN NULL;
            END;
            $$;
            """
        )
        cur.execute(
            """
            CREATE OR REPLACE FUNCTION quant_route_risk_events_monthly_partition()
            RETURNS TRIGGER
            LANGUAGE plpgsql
            AS $$
            DECLARE
                event_ts TIMESTAMPTZ := COALESCE(quant_safe_timestamptz(NEW.ts_utc), NOW());
                part_table TEXT;
            BEGIN
                part_table := quant_ensure_monthly_partition('risk_events', event_ts);
                EXECUTE format(
                    'INSERT INTO %I (id, ts_utc, owner, strategy_key, event_type, rule, message, detail_json)
                     VALUES ($1.id, $1.ts_utc, $1.owner, $1.strategy_key, $1.event_type, $1.rule, $1.message, $1.detail_json)',
                    part_table
                )
                USING NEW;
                RETURN NULL;
            END;
            $$;
            """
        )
        cur.execute(
            """
            CREATE OR REPLACE FUNCTION quant_route_market_ticks_monthly_partition()
            RETURNS TRIGGER
            LANGUAGE plpgsql
            AS $$
            DECLARE
                event_ts TIMESTAMPTZ := COALESCE(quant_safe_timestamptz(NEW.ts_utc), NOW());
                part_table TEXT;
            BEGIN
                part_table := quant_ensure_monthly_partition('market_ticks', event_ts);
                EXECUTE format(
                    'INSERT INTO %I (symbol, ts_utc, price, bid, ask, volume, source_config_path, ingested_at)
                     VALUES ($1.symbol, $1.ts_utc, $1.price, $1.bid, $1.ask, $1.volume, $1.source_config_path, $1.ingested_at)
                     ON CONFLICT(symbol, ts_utc) DO UPDATE SET
                        price = EXCLUDED.price,
                        bid = EXCLUDED.bid,
                        ask = EXCLUDED.ask,
                        volume = EXCLUDED.volume,
                        source_config_path = EXCLUDED.source_config_path,
                        ingested_at = EXCLUDED.ingested_at',
                    part_table
                )
                USING NEW;
                RETURN NULL;
            END;
            $$;
            """
        )
        cur.execute(
            """
            CREATE OR REPLACE FUNCTION quant_route_market_klines_monthly_partition()
            RETURNS TRIGGER
            LANGUAGE plpgsql
            AS $$
            DECLARE
                event_ts TIMESTAMPTZ := COALESCE(quant_safe_timestamptz(NEW.ts_utc), NOW());
                part_table TEXT;
            BEGIN
                part_table := quant_ensure_monthly_partition('market_klines', event_ts);
                EXECUTE format(
                    'INSERT INTO %I (symbol, timeframe, ts_utc, time_sec, open, high, low, close, volume, source_config_path, ingested_at)
                     VALUES ($1.symbol, $1.timeframe, $1.ts_utc, $1.time_sec, $1.open, $1.high, $1.low, $1.close, $1.volume, $1.source_config_path, $1.ingested_at)
                     ON CONFLICT(symbol, timeframe, ts_utc) DO UPDATE SET
                        time_sec = EXCLUDED.time_sec,
                        open = EXCLUDED.open,
                        high = EXCLUDED.high,
                        low = EXCLUDED.low,
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume,
                        source_config_path = EXCLUDED.source_config_path,
                        ingested_at = EXCLUDED.ingested_at',
                    part_table
                )
                USING NEW;
                RETURN NULL;
            END;
            $$;
            """
        )
        cur.execute("DROP TRIGGER IF EXISTS trg_route_audit_logs_monthly_partition ON audit_logs")
        cur.execute(
            """
            CREATE TRIGGER trg_route_audit_logs_monthly_partition
            BEFORE INSERT ON audit_logs
            FOR EACH ROW
            EXECUTE FUNCTION quant_route_audit_logs_monthly_partition()
            """
        )
        cur.execute("DROP TRIGGER IF EXISTS trg_route_risk_events_monthly_partition ON risk_events")
        cur.execute(
            """
            CREATE TRIGGER trg_route_risk_events_monthly_partition
            BEFORE INSERT ON risk_events
            FOR EACH ROW
            EXECUTE FUNCTION quant_route_risk_events_monthly_partition()
            """
        )
        cur.execute("DROP TRIGGER IF EXISTS trg_route_market_ticks_monthly_partition ON market_ticks")
        cur.execute(
            """
            CREATE TRIGGER trg_route_market_ticks_monthly_partition
            BEFORE INSERT ON market_ticks
            FOR EACH ROW
            EXECUTE FUNCTION quant_route_market_ticks_monthly_partition()
            """
        )
        cur.execute("DROP TRIGGER IF EXISTS trg_route_market_klines_monthly_partition ON market_klines")
        cur.execute(
            """
            CREATE TRIGGER trg_route_market_klines_monthly_partition
            BEFORE INSERT ON market_klines
            FOR EACH ROW
            EXECUTE FUNCTION quant_route_market_klines_monthly_partition()
            """
        )
        cur.execute(
            """
            DO $$
            DECLARE
                base_table TEXT;
            BEGIN
                FOREACH base_table IN ARRAY ARRAY['audit_logs', 'risk_events', 'market_ticks', 'market_klines']
                LOOP
                    PERFORM quant_ensure_monthly_partition(base_table, NOW() - INTERVAL '1 month');
                    PERFORM quant_ensure_monthly_partition(base_table, NOW());
                    PERFORM quant_ensure_monthly_partition(base_table, NOW() + INTERVAL '1 month');
                END LOOP;
            END;
            $$;
            """
        )

    def _migration_v21_add_data_files_table(self, cur) -> None:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS data_files (
                id BIGSERIAL PRIMARY KEY,
                owner TEXT NOT NULL,
                scope TEXT NOT NULL,
                file_key TEXT NOT NULL,
                file_name TEXT NOT NULL DEFAULT '',
                source_path TEXT NOT NULL DEFAULT '',
                content_type TEXT NOT NULL DEFAULT 'text/plain',
                content_encoding TEXT NOT NULL DEFAULT 'utf-8',
                content_text TEXT NOT NULL DEFAULT '',
                content_sha256 TEXT NOT NULL DEFAULT '',
                meta_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(owner, scope, file_key)
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_data_files_scope_owner_updated
            ON data_files(scope, owner, updated_at DESC)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_data_files_scope_key
            ON data_files(scope, file_key)
            """
        )
        cur.execute(
            """
            ALTER TABLE data_files
            ADD COLUMN IF NOT EXISTS content_sha256 TEXT NOT NULL DEFAULT ''
            """
        )
        cur.execute(
            """
            ALTER TABLE data_files
            ADD COLUMN IF NOT EXISTS meta_json TEXT NOT NULL DEFAULT '{}'
            """
        )

    def _extract_strategy_structured(self, strategy_key: str, record: Dict[str, Any]) -> Dict[str, str]:
        strategy_name = str(record.get("name") or record.get("sourceName") or strategy_key).strip()
        if not strategy_name:
            strategy_name = strategy_key
        config = record.get("config")
        primary_symbol = ""
        timeframe = ""
        if isinstance(config, dict):
            symbols = config.get("symbols")
            if isinstance(symbols, list):
                for item in symbols:
                    text = str(item or "").strip()
                    if text:
                        primary_symbol = text
                        break
            timeframe = str(config.get("timeframe") or "").strip()
        if not primary_symbol:
            primary_symbol = str(record.get("symbol") or "").strip()
        return {
            "strategy_name": strategy_name,
            "primary_symbol": primary_symbol,
            "timeframe": timeframe,
        }

    def _extract_backtest_structured(self, record: Dict[str, Any]) -> Dict[str, Any]:
        strategy_id = str(record.get("strategyId") or record.get("strategy_id") or "").strip()
        strategy_name = str(record.get("strategyName") or record.get("strategy_name") or "").strip()
        symbol = str(record.get("symbol") or "").strip()
        start_at = str(record.get("startAt") or record.get("start_at") or "").strip()
        end_at = str(record.get("endAt") or record.get("end_at") or "").strip()

        metrics = record.get("metrics")
        metric_return = None
        metric_sharpe = None
        metric_calmar = None
        metric_max_drawdown = None
        if isinstance(metrics, dict):
            metric_return = _as_float_or_none(metrics.get("returnPct"))
            metric_sharpe = _as_float_or_none(metrics.get("sharpe"))
            metric_calmar = _as_float_or_none(metrics.get("calmar"))
            metric_max_drawdown = _as_float_or_none(metrics.get("maxDrawdown"))
            if metric_return is None:
                pnl_total = _as_float_or_none(metrics.get("pnlTotal"))
                initial_capital = _as_float_or_none(record.get("initialCapital"))
                if pnl_total is not None and initial_capital is not None and initial_capital > 0:
                    metric_return = pnl_total / initial_capital

        return {
            "strategy_id": strategy_id,
            "strategy_name": strategy_name,
            "symbol": symbol,
            "start_at": start_at,
            "end_at": end_at,
            "metric_return": metric_return,
            "metric_sharpe": metric_sharpe,
            "metric_calmar": metric_calmar,
            "metric_max_drawdown": metric_max_drawdown,
        }

    def _extract_strategy_param_rows(
        self,
        strategy_key: str,
        owner: str,
        record: Dict[str, Any],
        *,
        updated_at: str,
    ) -> List[tuple]:
        config = record.get("config")
        if not isinstance(config, dict):
            return []
        params = config.get("params")
        if not isinstance(params, dict):
            return []

        rows: List[tuple] = []
        for key in sorted(params.keys()):
            param_key = str(key or "").strip()
            if not param_key:
                continue
            value = params.get(key)
            value_type = "json"
            value_num = None
            value_text = ""
            if isinstance(value, bool):
                value_type = "bool"
                value_text = "true" if value else "false"
                value_num = 1.0 if value else 0.0
            elif isinstance(value, (int, float)):
                value_type = "number"
                value_num = _as_float_or_none(value)
                value_text = str(value)
            elif isinstance(value, str):
                value_type = "string"
                value_text = value
                value_num = _as_float_or_none(value)
            else:
                value_type = "json"
                try:
                    value_text = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
                except Exception:
                    value_text = str(value)
                value_num = _as_float_or_none(value)
            rows.append((strategy_key, owner, param_key, value_text, value_num, value_type, updated_at))
        return rows

    def _ensure_ready(self) -> None:
        if not self._initialized:
            raise RuntimeError("postgres store is not initialized")

    def upsert_strategy(self, strategy_key: str, owner: str, record: Dict[str, Any]) -> None:
        self._ensure_ready()
        payload = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        strategy_id = str(record.get("id") or strategy_key)
        status = str(record.get("status") or "stopped")
        created_at = str(record.get("createdAt") or _now_iso())
        updated_at = str(record.get("updatedAt") or _now_iso())
        structured = self._extract_strategy_structured(strategy_key, record)
        param_rows = self._extract_strategy_param_rows(strategy_key, owner, record, updated_at=updated_at)
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO strategies (
                            strategy_key,
                            strategy_id,
                            strategy_name,
                            primary_symbol,
                            timeframe,
                            owner,
                            status,
                            created_at,
                            updated_at,
                            record_json
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT(strategy_key) DO UPDATE SET
                            strategy_id=EXCLUDED.strategy_id,
                            strategy_name=EXCLUDED.strategy_name,
                            primary_symbol=EXCLUDED.primary_symbol,
                            timeframe=EXCLUDED.timeframe,
                            owner=EXCLUDED.owner,
                            status=EXCLUDED.status,
                            updated_at=EXCLUDED.updated_at,
                            record_json=EXCLUDED.record_json
                        """,
                        (
                            strategy_key,
                            strategy_id,
                            structured["strategy_name"],
                            structured["primary_symbol"],
                            structured["timeframe"],
                            owner,
                            status,
                            created_at,
                            updated_at,
                            payload,
                        ),
                    )
                    cur.execute("DELETE FROM strategy_params WHERE strategy_key = %s", (strategy_key,))
                    for item in param_rows:
                        cur.execute(
                            """
                            INSERT INTO strategy_params (
                                strategy_key, owner, param_key, param_value_text, param_value_num, value_type, updated_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            item,
                        )
                conn.commit()

    def enqueue_strategy_compile_job(self, strategy_key: str, owner: str) -> Dict[str, Any]:
        self._ensure_ready()
        now_iso = _now_iso()
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO strategy_compiler_jobs (
                            strategy_key, owner, status, error_message, created_at, updated_at, started_at, finished_at
                        ) VALUES (%s, %s, 'pending', '', %s, %s, '', '')
                        RETURNING id, strategy_key, owner, status, error_message, created_at, updated_at, started_at, finished_at
                        """,
                        (str(strategy_key), str(owner), now_iso, now_iso),
                    )
                    row = cur.fetchone()
                conn.commit()
        if row is None:
            raise RuntimeError("failed to create compiler job")
        return {
            "id": int(row["id"]),
            "strategyKey": str(row["strategy_key"] or ""),
            "owner": str(row["owner"] or ""),
            "status": str(row["status"] or ""),
            "errorMessage": str(row["error_message"] or ""),
            "createdAt": str(row["created_at"] or ""),
            "updatedAt": str(row["updated_at"] or ""),
            "startedAt": str(row["started_at"] or ""),
            "finishedAt": str(row["finished_at"] or ""),
        }

    def update_strategy_compile_job(
        self,
        job_id: int,
        *,
        status: str,
        error_message: str = "",
        started_at: Optional[str] = None,
        finished_at: Optional[str] = None,
    ) -> None:
        self._ensure_ready()
        status_text = str(status or "").strip().lower()
        if not status_text:
            status_text = "failed"
        sets = ["status = %s", "error_message = %s", "updated_at = %s"]
        params: List[Any] = [status_text, str(error_message or ""), _now_iso()]
        if started_at is not None:
            sets.append("started_at = %s")
            params.append(str(started_at or ""))
        if finished_at is not None:
            sets.append("finished_at = %s")
            params.append(str(finished_at or ""))
        params.append(int(job_id))
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"UPDATE strategy_compiler_jobs SET {', '.join(sets)} WHERE id = %s",
                        tuple(params),
                    )
                conn.commit()

    def list_strategy_compile_jobs(
        self,
        *,
        owner: Optional[str] = None,
        strategy_key: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        self._ensure_ready()
        clauses: List[str] = []
        params: List[Any] = []
        if owner:
            clauses.append("owner = %s")
            params.append(str(owner))
        if strategy_key:
            clauses.append("strategy_key = %s")
            params.append(str(strategy_key))
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        safe_limit = max(1, min(int(limit), 2000))
        params.append(safe_limit)
        rows: List[Dict[str, Any]] = []
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, strategy_key, owner, status, error_message, created_at, updated_at, started_at, finished_at
                        FROM strategy_compiler_jobs
                        {where_sql}
                        ORDER BY id DESC
                        LIMIT %s
                        """,
                        tuple(params),
                    )
                    for row in cur.fetchall():
                        rows.append(
                            {
                                "id": int(row["id"]),
                                "strategyKey": str(row["strategy_key"] or ""),
                                "owner": str(row["owner"] or ""),
                                "status": str(row["status"] or ""),
                                "errorMessage": str(row["error_message"] or ""),
                                "createdAt": str(row["created_at"] or ""),
                                "updatedAt": str(row["updated_at"] or ""),
                                "startedAt": str(row["started_at"] or ""),
                                "finishedAt": str(row["finished_at"] or ""),
                            }
                        )
        return rows

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
        self._ensure_ready()
        source_payload = json.dumps(source_config or {}, ensure_ascii=False, separators=(",", ":"))
        created_at = _now_iso()
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT COALESCE(MAX(version), 0) AS max_version
                        FROM strategy_scripts
                        WHERE strategy_key = %s AND owner = %s
                        """,
                        (str(strategy_key), str(owner)),
                    )
                    version_row = cur.fetchone()
                    next_version = int(version_row["max_version"] or 0) + 1 if version_row is not None else 1
                    cur.execute(
                        """
                        INSERT INTO strategy_scripts (
                            strategy_key, owner, version, script_type, script_path, script_hash, source_config_json, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id, strategy_key, owner, version, script_type, script_path, script_hash, source_config_json, created_at
                        """,
                        (
                            str(strategy_key),
                            str(owner),
                            int(next_version),
                            str(script_type or ""),
                            str(script_path or ""),
                            str(script_hash or ""),
                            source_payload,
                            created_at,
                        ),
                    )
                    row = cur.fetchone()
                conn.commit()
        if row is None:
            raise RuntimeError("failed to add strategy script")
        try:
            source_cfg = json.loads(str(row["source_config_json"] or "{}"))
        except Exception:
            source_cfg = {}
        if not isinstance(source_cfg, dict):
            source_cfg = {"raw": source_cfg}
        return {
            "id": int(row["id"]),
            "strategyKey": str(row["strategy_key"] or ""),
            "owner": str(row["owner"] or ""),
            "version": int(row["version"] or 0),
            "scriptType": str(row["script_type"] or ""),
            "scriptPath": str(row["script_path"] or ""),
            "scriptHash": str(row["script_hash"] or ""),
            "sourceConfig": source_cfg,
            "createdAt": str(row["created_at"] or ""),
        }

    def list_strategy_scripts(
        self,
        *,
        owner: Optional[str] = None,
        strategy_key: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        self._ensure_ready()
        clauses: List[str] = []
        params: List[Any] = []
        if owner:
            clauses.append("owner = %s")
            params.append(str(owner))
        if strategy_key:
            clauses.append("strategy_key = %s")
            params.append(str(strategy_key))
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        safe_limit = max(1, min(int(limit), 2000))
        params.append(safe_limit)
        rows: List[Dict[str, Any]] = []
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, strategy_key, owner, version, script_type, script_path, script_hash, source_config_json, created_at
                        FROM strategy_scripts
                        {where_sql}
                        ORDER BY version DESC, id DESC
                        LIMIT %s
                        """,
                        tuple(params),
                    )
                    for row in cur.fetchall():
                        try:
                            source_cfg = json.loads(str(row["source_config_json"] or "{}"))
                        except Exception:
                            source_cfg = {}
                        if not isinstance(source_cfg, dict):
                            source_cfg = {"raw": source_cfg}
                        rows.append(
                            {
                                "id": int(row["id"]),
                                "strategyKey": str(row["strategy_key"] or ""),
                                "owner": str(row["owner"] or ""),
                                "version": int(row["version"] or 0),
                                "scriptType": str(row["script_type"] or ""),
                                "scriptPath": str(row["script_path"] or ""),
                                "scriptHash": str(row["script_hash"] or ""),
                                "sourceConfig": source_cfg,
                                "createdAt": str(row["created_at"] or ""),
                            }
                        )
        return rows

    def get_latest_strategy_script(
        self,
        *,
        owner: str,
        strategy_key: str,
    ) -> Optional[Dict[str, Any]]:
        rows = self.list_strategy_scripts(owner=owner, strategy_key=strategy_key, limit=1)
        return rows[0] if rows else None

    def _ensure_user_row(
        self,
        cur,
        *,
        username: str,
        role: str = "user",
        display_name: str = "",
    ) -> Dict[str, Any]:
        normalized_username = str(username or "").strip().lower()
        if not normalized_username:
            raise ValueError("username is required")
        role_text = str(role or "user").strip().lower() or "user"
        if role_text not in {"admin", "user", "guest"}:
            role_text = "user"
        display_text = str(display_name or normalized_username).strip() or normalized_username
        now_iso = _now_iso()
        cur.execute(
            """
            INSERT INTO users (username, status, display_name, role, created_at, last_login_at)
            VALUES (%s, 'active', %s, %s, %s, '')
            ON CONFLICT(username) DO UPDATE SET
                status='active',
                display_name=EXCLUDED.display_name,
                role=EXCLUDED.role
            """,
            (normalized_username, display_text, role_text, now_iso),
        )
        cur.execute(
            """
            SELECT id, username, status, display_name, role, created_at, last_login_at
            FROM users
            WHERE username = %s
            LIMIT 1
            """,
            (normalized_username,),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("failed to upsert user")
        try:
            cur.execute("SELECT 1 FROM user_roles WHERE username = %s LIMIT 1", (normalized_username,))
            has_binding = cur.fetchone()
            if has_binding is None:
                role_for_bind = str(row.get("role") or "user").strip().lower() or "user"
                if role_for_bind not in _DEFAULT_RBAC_ROLE_CODES:
                    role_for_bind = "user"
                cur.execute(
                    """
                    INSERT INTO user_roles (username, role_code, bound_at)
                    VALUES (%s, %s, %s)
                    ON CONFLICT(username, role_code) DO NOTHING
                    """,
                    (normalized_username, role_for_bind, now_iso),
                )
        except Exception:
            pass
        return {
            "id": int(row["id"]),
            "username": str(row["username"] or ""),
            "status": str(row["status"] or ""),
            "displayName": str(row["display_name"] or ""),
            "role": str(row["role"] or "user"),
            "createdAt": str(row["created_at"] or ""),
            "lastLoginAt": str(row["last_login_at"] or ""),
        }

    def ensure_user(
        self,
        username: str,
        *,
        role: str = "user",
        display_name: str = "",
    ) -> Dict[str, Any]:
        self._ensure_ready()
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    row = self._ensure_user_row(
                        cur,
                        username=username,
                        role=role,
                        display_name=display_name,
                    )
                conn.commit()
        return row

    def upsert_user_credential(
        self,
        *,
        username: str,
        password_hash: str,
        algorithm: str,
        password_updated_at: Optional[str] = None,
    ) -> None:
        self._ensure_ready()
        now_iso = str(password_updated_at or _now_iso())
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    user = self._ensure_user_row(cur, username=username)
                    cur.execute(
                        """
                        INSERT INTO user_credentials (user_id, password_hash, algorithm, password_updated_at)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT(user_id) DO UPDATE SET
                            password_hash=EXCLUDED.password_hash,
                            algorithm=EXCLUDED.algorithm,
                            password_updated_at=EXCLUDED.password_updated_at
                        """,
                        (
                            int(user["id"]),
                            str(password_hash or ""),
                            str(algorithm or ""),
                            now_iso,
                        ),
                    )
                conn.commit()

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
        self._ensure_ready()
        sid = str(session_id or "").strip()
        if not sid:
            raise ValueError("session_id is required")
        issued = str(issued_at or _now_iso())
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    role = "admin" if str(username or "").strip().lower() == "admin" else (
                        "guest" if str(username or "").strip().lower() == "guest" else "user"
                    )
                    user = self._ensure_user_row(cur, username=username, role=role)
                    cur.execute(
                        """
                        INSERT INTO auth_sessions (
                            session_id, user_id, username, issued_at, expires_at, revoked_at, client_ip, user_agent
                        ) VALUES (%s, %s, %s, %s, %s, '', %s, %s)
                        ON CONFLICT(session_id) DO UPDATE SET
                            user_id=EXCLUDED.user_id,
                            username=EXCLUDED.username,
                            issued_at=EXCLUDED.issued_at,
                            expires_at=EXCLUDED.expires_at,
                            revoked_at='',
                            client_ip=EXCLUDED.client_ip,
                            user_agent=EXCLUDED.user_agent
                        """,
                        (
                            sid,
                            int(user["id"]),
                            str(user["username"]),
                            issued,
                            str(expires_at or ""),
                            str(client_ip or ""),
                            str(user_agent or ""),
                        ),
                    )
                    cur.execute("UPDATE users SET last_login_at = %s WHERE id = %s", (issued, int(user["id"])))
                conn.commit()

    def get_auth_session(self, session_id: str) -> Optional[Dict[str, Any]]:
        self._ensure_ready()
        sid = str(session_id or "").strip()
        if not sid:
            return None
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            s.session_id,
                            s.user_id,
                            s.username,
                            s.issued_at,
                            s.expires_at,
                            s.revoked_at,
                            s.client_ip,
                            s.user_agent,
                            u.status AS user_status,
                            u.role AS user_role
                        FROM auth_sessions s
                        LEFT JOIN users u ON u.id = s.user_id
                        WHERE s.session_id = %s
                        LIMIT 1
                        """,
                        (sid,),
                    )
                    row = cur.fetchone()
        if row is None:
            return None
        return {
            "sessionId": str(row["session_id"] or ""),
            "userId": int(row["user_id"] or 0),
            "username": str(row["username"] or ""),
            "issuedAt": str(row["issued_at"] or ""),
            "expiresAt": str(row["expires_at"] or ""),
            "revokedAt": str(row["revoked_at"] or ""),
            "clientIp": str(row["client_ip"] or ""),
            "userAgent": str(row["user_agent"] or ""),
            "userStatus": str(row["user_status"] or ""),
            "userRole": str(row["user_role"] or ""),
        }

    def revoke_auth_session(self, session_id: str, revoked_at: Optional[str] = None) -> None:
        self._ensure_ready()
        sid = str(session_id or "").strip()
        if not sid:
            return
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE auth_sessions SET revoked_at = %s WHERE session_id = %s",
                        (str(revoked_at or _now_iso()), sid),
                    )
                conn.commit()

    def record_login_attempt(
        self,
        *,
        username: str,
        client_ip: str,
        success: bool,
        reason: str = "",
        ts_utc: Optional[str] = None,
    ) -> None:
        self._ensure_ready()
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO auth_login_attempts (username, client_ip, success, reason, ts_utc)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            str(username or "").strip().lower(),
                            str(client_ip or "").strip() or "unknown",
                            bool(success),
                            str(reason or ""),
                            str(ts_utc or _now_iso()),
                        ),
                    )
                conn.commit()

    def set_lockout(
        self,
        *,
        lock_key: str,
        locked_until: str,
        updated_at: Optional[str] = None,
    ) -> None:
        self._ensure_ready()
        key = str(lock_key or "").strip()
        if not key:
            return
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO auth_lockouts (lock_key, locked_until, updated_at)
                        VALUES (%s, %s, %s)
                        ON CONFLICT(lock_key) DO UPDATE SET
                            locked_until=EXCLUDED.locked_until,
                            updated_at=EXCLUDED.updated_at
                        """,
                        (key, str(locked_until or ""), str(updated_at or _now_iso())),
                    )
                conn.commit()

    def get_active_lockouts(
        self,
        *,
        lock_keys: List[str],
        now_ts: str,
    ) -> Dict[str, str]:
        self._ensure_ready()
        keys = [str(item or "").strip() for item in lock_keys if str(item or "").strip()]
        if not keys:
            return {}
        out: Dict[str, str] = {}
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT lock_key, locked_until
                        FROM auth_lockouts
                        WHERE lock_key = ANY(%s) AND locked_until > %s
                        """,
                        (keys, str(now_ts or _now_iso())),
                    )
                    for row in cur.fetchall():
                        key = str(row["lock_key"] or "").strip()
                        until = str(row["locked_until"] or "").strip()
                        if key and until:
                            out[key] = until
        return out

    def clear_lockouts(self, lock_keys: List[str]) -> None:
        self._ensure_ready()
        keys = [str(item or "").strip() for item in lock_keys if str(item or "").strip()]
        if not keys:
            return
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM auth_lockouts WHERE lock_key = ANY(%s)", (keys,))
                conn.commit()

    def upsert_user_preferences(self, owner: str, preferences: Dict[str, Any]) -> None:
        self._ensure_ready()
        owner_key = str(owner or "").strip()
        if not owner_key:
            return
        payload = json.dumps(preferences or {}, ensure_ascii=False, separators=(",", ":"))
        updated_at = _now_iso()
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO user_preferences (owner, preferences_json, updated_at)
                        VALUES (%s, %s, %s)
                        ON CONFLICT(owner) DO UPDATE SET
                            preferences_json=EXCLUDED.preferences_json,
                            updated_at=EXCLUDED.updated_at
                        """,
                        (owner_key, payload, updated_at),
                    )
                conn.commit()

    def get_user_preferences(self, owner: str) -> Optional[Dict[str, Any]]:
        self._ensure_ready()
        owner_key = str(owner or "").strip()
        if not owner_key:
            return None
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT owner, preferences_json, updated_at
                        FROM user_preferences
                        WHERE owner = %s
                        LIMIT 1
                        """,
                        (owner_key,),
                    )
                    row = cur.fetchone()
        if row is None:
            return None
        try:
            preferences = json.loads(str(row["preferences_json"] or "{}"))
        except Exception:
            preferences = {}
        if not isinstance(preferences, dict):
            preferences = {"raw": preferences}
        return {
            "owner": str(row["owner"] or ""),
            "preferences": preferences,
            "updatedAt": str(row["updated_at"] or ""),
        }

    def _normalize_role_codes(self, roles: List[str]) -> List[str]:
        normalized: List[str] = []
        seen = set()
        for role in roles or []:
            role_code = str(role or "").strip().lower()
            if not role_code or role_code in seen:
                continue
            seen.add(role_code)
            normalized.append(role_code)
        return normalized

    def _pick_legacy_role(self, roles: List[str]) -> str:
        for candidate in ("admin", "guest", "user"):
            if candidate in roles:
                return candidate
        return str((roles or ["user"])[0] or "user")

    def list_roles(self) -> List[Dict[str, Any]]:
        self._ensure_ready()
        rows: List[Dict[str, Any]] = []
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT role_code, description
                        FROM roles
                        ORDER BY role_code ASC
                        """
                    )
                    for row in cur.fetchall():
                        rows.append(
                            {
                                "roleCode": str(row["role_code"] or ""),
                                "description": str(row["description"] or ""),
                            }
                        )
        return rows

    def list_permissions(self) -> List[Dict[str, Any]]:
        self._ensure_ready()
        rows: List[Dict[str, Any]] = []
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT permission_code, description
                        FROM permissions
                        ORDER BY permission_code ASC
                        """
                    )
                    for row in cur.fetchall():
                        rows.append(
                            {
                                "permissionCode": str(row["permission_code"] or ""),
                                "description": str(row["description"] or ""),
                            }
                        )
        return rows

    def list_user_roles(self, username: str) -> List[str]:
        self._ensure_ready()
        username_key = str(username or "").strip().lower()
        if not username_key:
            return []
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT role_code
                        FROM user_roles
                        WHERE username = %s
                        ORDER BY role_code ASC
                        """,
                        (username_key,),
                    )
                    roles = [str(row["role_code"] or "") for row in cur.fetchall() if str(row["role_code"] or "").strip()]
                    if roles:
                        return roles
                    cur.execute(
                        """
                        SELECT role
                        FROM users
                        WHERE username = %s
                        LIMIT 1
                        """,
                        (username_key,),
                    )
                    fallback = cur.fetchone()
                    role_code = str((fallback or {}).get("role") or "").strip().lower()
                    if role_code and role_code in _DEFAULT_RBAC_ROLE_CODES:
                        return [role_code]
                    return []

    def replace_user_roles(self, username: str, roles: List[str]) -> List[str]:
        self._ensure_ready()
        username_key = str(username or "").strip().lower()
        if not username_key:
            raise ValueError("username is required")
        requested = self._normalize_role_codes(roles)
        if not requested:
            raise ValueError("roles must contain at least one item")

        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    self._ensure_user_row(cur, username=username_key, role="user", display_name=username_key)
                    accepted: List[str] = []
                    for role_code in requested:
                        cur.execute("SELECT 1 FROM roles WHERE role_code = %s LIMIT 1", (role_code,))
                        if cur.fetchone() is not None:
                            accepted.append(role_code)
                    if not accepted:
                        raise ValueError("no valid roles found")
                    cur.execute("DELETE FROM user_roles WHERE username = %s", (username_key,))
                    now_iso = _now_iso()
                    for role_code in accepted:
                        cur.execute(
                            """
                            INSERT INTO user_roles (username, role_code, bound_at)
                            VALUES (%s, %s, %s)
                            ON CONFLICT(username, role_code) DO NOTHING
                            """,
                            (username_key, role_code, now_iso),
                        )
                    cur.execute(
                        "UPDATE users SET role = %s WHERE username = %s",
                        (self._pick_legacy_role(accepted), username_key),
                    )
                conn.commit()
                return accepted

    def user_has_permission(self, username: str, permission_code: str) -> bool:
        self._ensure_ready()
        username_key = str(username or "").strip().lower()
        perm_code = str(permission_code or "").strip()
        if not username_key or not perm_code:
            return False
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT 1
                        FROM user_roles ur
                        JOIN role_permissions rp ON rp.role_code = ur.role_code
                        WHERE ur.username = %s AND rp.permission_code = %s
                        LIMIT 1
                        """,
                        (username_key, perm_code),
                    )
                    if cur.fetchone() is not None:
                        return True
                    cur.execute(
                        """
                        SELECT 1
                        FROM users u
                        JOIN role_permissions rp ON rp.role_code = u.role
                        WHERE u.username = %s AND rp.permission_code = %s
                        LIMIT 1
                        """,
                        (username_key, perm_code),
                    )
                    return cur.fetchone() is not None

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
        self._ensure_ready()
        payload = json.dumps(detail or {}, ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO account_security_events (
                            ts_utc, owner, event_type, severity, message, detail_json
                        ) VALUES (%s, %s, %s, %s, %s, %s)
                        """,
                        (
                            str(ts_utc or _now_iso()),
                            str(owner or "").strip().lower(),
                            str(event_type or "").strip(),
                            str(severity or "info").strip().lower() or "info",
                            str(message or ""),
                            payload,
                        ),
                    )
                conn.commit()

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
        self._ensure_ready()
        clauses: List[str] = []
        params: List[Any] = []
        if owner:
            clauses.append("owner = %s")
            params.append(str(owner).strip().lower())
        if event_type:
            clauses.append("event_type = %s")
            params.append(str(event_type))
        if start_ts:
            clauses.append("ts_utc >= %s")
            params.append(str(start_ts))
        if end_ts:
            clauses.append("ts_utc <= %s")
            params.append(str(end_ts))
        if cursor_id is not None:
            try:
                safe_cursor = int(cursor_id)
            except Exception:
                safe_cursor = 0
            if safe_cursor > 0:
                clauses.append("id < %s")
                params.append(safe_cursor)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        safe_limit = max(1, min(int(limit), 2000))
        params.append(safe_limit)

        rows: List[Dict[str, Any]] = []
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, ts_utc, owner, event_type, severity, message, detail_json
                        FROM account_security_events
                        {where_sql}
                        ORDER BY id DESC
                        LIMIT %s
                        """,
                        tuple(params),
                    )
                    for row in cur.fetchall():
                        try:
                            detail_payload = json.loads(str(row["detail_json"] or "{}"))
                        except Exception:
                            detail_payload = {}
                        if not isinstance(detail_payload, dict):
                            detail_payload = {"raw": detail_payload}
                        rows.append(
                            {
                                "id": int(row["id"] or 0),
                                "ts": str(row["ts_utc"] or ""),
                                "owner": str(row["owner"] or ""),
                                "eventType": str(row["event_type"] or ""),
                                "severity": str(row["severity"] or ""),
                                "message": str(row["message"] or ""),
                                "detail": detail_payload,
                                "cursorId": int(row["id"] or 0),
                            }
                        )
        return rows

    def _normalize_token_scopes(self, scopes: List[str]) -> List[str]:
        normalized: List[str] = []
        seen = set()
        for scope in scopes or []:
            text = str(scope or "").strip()
            if not text or text in seen:
                continue
            seen.add(text)
            normalized.append(text)
        return normalized

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
        self._ensure_ready()
        owner_key = str(owner or "").strip().lower()
        if not owner_key:
            raise ValueError("owner is required")
        token_name_text = str(token_name or "").strip()
        prefix = str(token_prefix or "").strip()
        token_hash_text = str(token_hash or "").strip()
        if not prefix or not token_hash_text:
            raise ValueError("token_prefix and token_hash are required")
        scopes_payload = json.dumps(self._normalize_token_scopes(scopes), ensure_ascii=False, separators=(",", ":"))
        created_at_text = str(created_at or _now_iso())

        row: Optional[Dict[str, Any]] = None
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO api_tokens (
                            owner, token_name, token_prefix, token_hash, scopes_json, created_at, expires_at,
                            last_used_at, revoked_at, created_by, revoked_by
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, '', '', %s, '')
                        RETURNING id, owner, token_name, token_prefix, scopes_json, created_at, expires_at,
                                  last_used_at, revoked_at, created_by, revoked_by
                        """,
                        (
                            owner_key,
                            token_name_text,
                            prefix,
                            token_hash_text,
                            scopes_payload,
                            created_at_text,
                            str(expires_at or ""),
                            str(created_by or "").strip().lower(),
                        ),
                    )
                    row = cur.fetchone()
                conn.commit()
        if row is None:
            raise RuntimeError("failed to create api token")
        try:
            scopes_data = json.loads(str(row["scopes_json"] or "[]"))
        except Exception:
            scopes_data = []
        if not isinstance(scopes_data, list):
            scopes_data = []
        return {
            "id": int(row["id"] or 0),
            "owner": str(row["owner"] or ""),
            "tokenName": str(row["token_name"] or ""),
            "tokenPrefix": str(row["token_prefix"] or ""),
            "scopes": [str(item) for item in scopes_data if str(item or "").strip()],
            "createdAt": str(row["created_at"] or ""),
            "expiresAt": str(row["expires_at"] or ""),
            "lastUsedAt": str(row["last_used_at"] or ""),
            "revokedAt": str(row["revoked_at"] or ""),
            "createdBy": str(row["created_by"] or ""),
            "revokedBy": str(row["revoked_by"] or ""),
            "active": not bool(str(row["revoked_at"] or "").strip()),
        }

    def list_api_tokens(
        self,
        *,
        owner: Optional[str] = None,
        include_revoked: bool = False,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        self._ensure_ready()
        clauses: List[str] = []
        params: List[Any] = []
        if owner:
            clauses.append("owner = %s")
            params.append(str(owner).strip().lower())
        if not include_revoked:
            clauses.append("revoked_at = ''")
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        safe_limit = max(1, min(int(limit), 2000))
        params.append(safe_limit)

        rows: List[Dict[str, Any]] = []
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, owner, token_name, token_prefix, scopes_json, created_at, expires_at,
                               last_used_at, revoked_at, created_by, revoked_by
                        FROM api_tokens
                        {where_sql}
                        ORDER BY id DESC
                        LIMIT %s
                        """,
                        tuple(params),
                    )
                    for row in cur.fetchall():
                        try:
                            scopes_data = json.loads(str(row["scopes_json"] or "[]"))
                        except Exception:
                            scopes_data = []
                        if not isinstance(scopes_data, list):
                            scopes_data = []
                        rows.append(
                            {
                                "id": int(row["id"] or 0),
                                "owner": str(row["owner"] or ""),
                                "tokenName": str(row["token_name"] or ""),
                                "tokenPrefix": str(row["token_prefix"] or ""),
                                "scopes": [str(item) for item in scopes_data if str(item or "").strip()],
                                "createdAt": str(row["created_at"] or ""),
                                "expiresAt": str(row["expires_at"] or ""),
                                "lastUsedAt": str(row["last_used_at"] or ""),
                                "revokedAt": str(row["revoked_at"] or ""),
                                "createdBy": str(row["created_by"] or ""),
                                "revokedBy": str(row["revoked_by"] or ""),
                                "active": not bool(str(row["revoked_at"] or "").strip()),
                            }
                        )
        return rows

    def get_active_api_token_by_hash(
        self,
        *,
        token_hash: str,
        now_ts: str,
    ) -> Optional[Dict[str, Any]]:
        self._ensure_ready()
        token_hash_text = str(token_hash or "").strip()
        if not token_hash_text:
            return None
        row: Optional[Dict[str, Any]] = None
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, owner, token_name, token_prefix, scopes_json, created_at, expires_at,
                               last_used_at, revoked_at, created_by, revoked_by
                        FROM api_tokens
                        WHERE token_hash = %s
                          AND revoked_at = ''
                          AND (expires_at = '' OR expires_at > %s)
                        LIMIT 1
                        """,
                        (token_hash_text, str(now_ts or _now_iso())),
                    )
                    row = cur.fetchone()
        if row is None:
            return None
        try:
            scopes_data = json.loads(str(row["scopes_json"] or "[]"))
        except Exception:
            scopes_data = []
        if not isinstance(scopes_data, list):
            scopes_data = []
        return {
            "id": int(row["id"] or 0),
            "owner": str(row["owner"] or ""),
            "tokenName": str(row["token_name"] or ""),
            "tokenPrefix": str(row["token_prefix"] or ""),
            "scopes": [str(item) for item in scopes_data if str(item or "").strip()],
            "createdAt": str(row["created_at"] or ""),
            "expiresAt": str(row["expires_at"] or ""),
            "lastUsedAt": str(row["last_used_at"] or ""),
            "revokedAt": str(row["revoked_at"] or ""),
            "createdBy": str(row["created_by"] or ""),
            "revokedBy": str(row["revoked_by"] or ""),
            "active": True,
        }

    def touch_api_token_last_used(
        self,
        token_id: int,
        *,
        last_used_at: Optional[str] = None,
    ) -> None:
        self._ensure_ready()
        try:
            token_id_int = int(token_id)
        except Exception:
            return
        if token_id_int <= 0:
            return
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "UPDATE api_tokens SET last_used_at = %s WHERE id = %s",
                        (str(last_used_at or _now_iso()), token_id_int),
                    )
                conn.commit()

    def revoke_api_token(
        self,
        token_id: int,
        *,
        revoked_at: Optional[str] = None,
        revoked_by: str = "",
    ) -> None:
        self._ensure_ready()
        try:
            token_id_int = int(token_id)
        except Exception:
            return
        if token_id_int <= 0:
            return
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE api_tokens
                        SET revoked_at = %s, revoked_by = %s
                        WHERE id = %s
                        """,
                        (
                            str(revoked_at or _now_iso()),
                            str(revoked_by or "").strip().lower(),
                            token_id_int,
                        ),
                    )
                conn.commit()

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
        self._ensure_ready()
        payload = json.dumps(detail or {}, ensure_ascii=False, separators=(",", ":"))
        level_text = str(level or "info").strip().lower() or "info"
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO runtime_logs (
                            ts_utc, owner, log_type, level, source, message, strategy_id, backtest_id, detail_json
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            str(ts_utc or _now_iso()),
                            str(owner or ""),
                            str(log_type or "system"),
                            level_text,
                            str(source or "system"),
                            str(message or ""),
                            str(strategy_id or ""),
                            str(backtest_id or ""),
                            payload,
                        ),
                    )
                conn.commit()

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
        self._ensure_ready()
        clauses: List[str] = []
        params: List[Any] = []
        if owner:
            clauses.append("owner = %s")
            params.append(str(owner))
        if log_type:
            clauses.append("log_type = %s")
            params.append(str(log_type))
        if level:
            clauses.append("level = %s")
            params.append(str(level).strip().lower())
        if strategy_id:
            clauses.append("strategy_id = %s")
            params.append(str(strategy_id))
        if start_ts:
            clauses.append("ts_utc >= %s")
            params.append(str(start_ts))
        if end_ts:
            clauses.append("ts_utc <= %s")
            params.append(str(end_ts))
        if q:
            clauses.append("LOWER(message) LIKE %s")
            params.append(f"%{str(q).lower()}%")
        if cursor_id is not None:
            try:
                safe_cursor = int(cursor_id)
            except Exception:
                safe_cursor = 0
            if safe_cursor > 0:
                clauses.append("id < %s")
                params.append(safe_cursor)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        safe_limit = max(1, min(int(limit), 2000))
        params.append(safe_limit)

        rows: List[Dict[str, Any]] = []
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, ts_utc, owner, log_type, level, source, message, strategy_id, backtest_id, detail_json
                        FROM runtime_logs
                        {where_sql}
                        ORDER BY id DESC
                        LIMIT %s
                        """,
                        tuple(params),
                    )
                    for row in cur.fetchall():
                        try:
                            detail_payload = json.loads(str(row["detail_json"] or "{}"))
                        except Exception:
                            detail_payload = {}
                        if not isinstance(detail_payload, dict):
                            detail_payload = {"raw": detail_payload}
                        rows.append(
                            {
                                "id": f"db_rt_{int(row['id'])}",
                                "ts": str(row["ts_utc"] or ""),
                                "owner": str(row["owner"] or ""),
                                "type": str(row["log_type"] or ""),
                                "level": str(row["level"] or ""),
                                "source": str(row["source"] or ""),
                                "message": str(row["message"] or ""),
                                "strategyId": str(row["strategy_id"] or ""),
                                "backtestId": str(row["backtest_id"] or ""),
                                "detail": detail_payload,
                                "cursorId": int(row["id"]),
                            }
                        )
        return rows

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
        self._ensure_ready()
        payload_json = json.dumps(payload or {}, ensure_ascii=False, separators=(",", ":"))
        severity_text = str(severity or "info").strip().lower() or "info"
        status_text = str(status or "failed").strip().lower() or "failed"
        retry = max(0, int(retry_count))
        http_status_safe: Optional[int]
        if http_status is None:
            http_status_safe = None
        else:
            try:
                http_status_safe = int(http_status)
            except Exception:
                http_status_safe = None
        duration = float(_as_float_or_none(duration_ms) or 0.0)
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO alert_deliveries (
                            ts_utc, owner, event, severity, message, webhook_url, status, retry_count,
                            http_status, error_message, payload_json, response_body, duration_ms
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            str(ts_utc or _now_iso()),
                            str(owner or ""),
                            str(event or ""),
                            severity_text,
                            str(message or ""),
                            str(webhook_url or ""),
                            status_text,
                            retry,
                            http_status_safe,
                            str(error_message or ""),
                            payload_json,
                            str(response_body or ""),
                            duration,
                        ),
                    )
                conn.commit()

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
        self._ensure_ready()
        clauses: List[str] = []
        params: List[Any] = []
        if owner:
            clauses.append("owner = %s")
            params.append(str(owner))
        if event:
            clauses.append("event = %s")
            params.append(str(event))
        if status:
            clauses.append("status = %s")
            params.append(str(status).strip().lower())
        if start_ts:
            clauses.append("ts_utc >= %s")
            params.append(str(start_ts))
        if end_ts:
            clauses.append("ts_utc <= %s")
            params.append(str(end_ts))
        if cursor_id is not None:
            try:
                safe_cursor = int(cursor_id)
            except Exception:
                safe_cursor = 0
            if safe_cursor > 0:
                clauses.append("id < %s")
                params.append(safe_cursor)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        safe_limit = max(1, min(int(limit), 2000))
        params.append(safe_limit)

        rows: List[Dict[str, Any]] = []
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, ts_utc, owner, event, severity, message, webhook_url, status, retry_count,
                               http_status, error_message, payload_json, response_body, duration_ms
                        FROM alert_deliveries
                        {where_sql}
                        ORDER BY id DESC
                        LIMIT %s
                        """,
                        tuple(params),
                    )
                    for row in cur.fetchall():
                        try:
                            payload_data = json.loads(str(row["payload_json"] or "{}"))
                        except Exception:
                            payload_data = {}
                        if not isinstance(payload_data, dict):
                            payload_data = {"raw": payload_data}
                        rows.append(
                            {
                                "id": int(row["id"] or 0),
                                "ts": str(row["ts_utc"] or ""),
                                "owner": str(row["owner"] or ""),
                                "event": str(row["event"] or ""),
                                "severity": str(row["severity"] or ""),
                                "message": str(row["message"] or ""),
                                "webhookUrl": str(row["webhook_url"] or ""),
                                "status": str(row["status"] or ""),
                                "retryCount": int(row["retry_count"] or 0),
                                "httpStatus": int(row["http_status"]) if row["http_status"] is not None else None,
                                "errorMessage": str(row["error_message"] or ""),
                                "payload": payload_data,
                                "responseBody": str(row["response_body"] or ""),
                                "durationMs": float(row["duration_ms"] or 0.0),
                                "cursorId": int(row["id"] or 0),
                            }
                        )
        return rows

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
        self._ensure_ready()
        now_iso = str(created_at or _now_iso())
        severity_text = str(severity or "info").strip().lower() or "info"
        if severity_text not in {"info", "warn", "error", "critical"}:
            severity_text = "info"
        payload_json = json.dumps(payload or {}, ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO alert_outbox (
                            created_at, updated_at, available_at, owner, event, severity, message,
                            webhook_url, payload_json, status, retry_count, max_retries,
                            http_status, error_message, response_body, dispatched_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                        """,
                        (
                            now_iso,
                            now_iso,
                            str(available_at or now_iso),
                            str(owner or ""),
                            str(event or ""),
                            severity_text,
                            str(message or ""),
                            str(webhook_url or ""),
                            payload_json,
                            "pending",
                            0,
                            max(0, int(max_retries)),
                            None,
                            "",
                            "",
                            "",
                        ),
                    )
                    row = cur.fetchone()
                conn.commit()
                return int((row or {}).get("id") or 0)

    def list_due_alert_outbox(
        self,
        *,
        now_ts: Optional[str] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        self._ensure_ready()
        now_iso = str(now_ts or _now_iso())
        safe_limit = max(1, min(int(limit), 1000))
        rows: List[Dict[str, Any]] = []
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT id, created_at, updated_at, available_at, owner, event, severity, message,
                               webhook_url, payload_json, status, retry_count, max_retries, http_status,
                               error_message, response_body, dispatched_at
                        FROM alert_outbox
                        WHERE status = 'pending' AND available_at <= %s
                        ORDER BY id ASC
                        LIMIT %s
                        """,
                        (now_iso, safe_limit),
                    )
                    for row in cur.fetchall():
                        try:
                            payload_data = json.loads(str(row["payload_json"] or "{}"))
                        except Exception:
                            payload_data = {}
                        if not isinstance(payload_data, dict):
                            payload_data = {"raw": payload_data}
                        rows.append(
                            {
                                "id": int(row["id"] or 0),
                                "createdAt": str(row["created_at"] or ""),
                                "updatedAt": str(row["updated_at"] or ""),
                                "availableAt": str(row["available_at"] or ""),
                                "owner": str(row["owner"] or ""),
                                "event": str(row["event"] or ""),
                                "severity": str(row["severity"] or ""),
                                "message": str(row["message"] or ""),
                                "webhookUrl": str(row["webhook_url"] or ""),
                                "payload": payload_data,
                                "status": str(row["status"] or ""),
                                "retryCount": int(row["retry_count"] or 0),
                                "maxRetries": int(row["max_retries"] or 0),
                                "httpStatus": int(row["http_status"]) if row["http_status"] is not None else None,
                                "errorMessage": str(row["error_message"] or ""),
                                "responseBody": str(row["response_body"] or ""),
                                "dispatchedAt": str(row["dispatched_at"] or ""),
                            }
                        )
        return rows

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
        self._ensure_ready()
        status_text = str(status or "failed").strip().lower() or "failed"
        if status_text not in {"pending", "sent", "failed"}:
            status_text = "failed"
        now_iso = _now_iso()
        available_text = str(available_at or now_iso)
        dispatched_text = str(dispatched_at or (now_iso if status_text in {"sent", "failed"} else ""))
        http_status_safe: Optional[int]
        if http_status is None:
            http_status_safe = None
        else:
            try:
                http_status_safe = int(http_status)
            except Exception:
                http_status_safe = None
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE alert_outbox
                        SET status = %s,
                            retry_count = %s,
                            available_at = %s,
                            http_status = %s,
                            error_message = %s,
                            response_body = %s,
                            dispatched_at = %s,
                            updated_at = %s
                        WHERE id = %s
                        """,
                        (
                            status_text,
                            max(0, int(retry_count)),
                            available_text,
                            http_status_safe,
                            str(error_message or ""),
                            str(response_body or ""),
                            dispatched_text,
                            now_iso,
                            int(outbox_id),
                        ),
                    )
                conn.commit()

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
        self._ensure_ready()
        payload = json.dumps(detail or {}, ensure_ascii=False, separators=(",", ":"))
        refresh = max(0, int(refresh_ms))
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO ws_connection_events (
                            ts_utc, owner, event_type, connection_id, strategy_id, config_path,
                            refresh_ms, client_ip, user_agent, detail_json
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            str(ts_utc or _now_iso()),
                            str(owner or ""),
                            str(event_type or ""),
                            str(connection_id or ""),
                            str(strategy_id or ""),
                            str(config_path or ""),
                            refresh,
                            str(client_ip or ""),
                            str(user_agent or ""),
                            payload,
                        ),
                    )
                conn.commit()

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
        self._ensure_ready()
        clauses: List[str] = []
        params: List[Any] = []
        if owner:
            clauses.append("owner = %s")
            params.append(str(owner))
        if event_type:
            clauses.append("event_type = %s")
            params.append(str(event_type))
        if strategy_id:
            clauses.append("strategy_id = %s")
            params.append(str(strategy_id))
        if start_ts:
            clauses.append("ts_utc >= %s")
            params.append(str(start_ts))
        if end_ts:
            clauses.append("ts_utc <= %s")
            params.append(str(end_ts))
        if cursor_id is not None:
            try:
                safe_cursor = int(cursor_id)
            except Exception:
                safe_cursor = 0
            if safe_cursor > 0:
                clauses.append("id < %s")
                params.append(safe_cursor)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        safe_limit = max(1, min(int(limit), 2000))
        params.append(safe_limit)

        rows: List[Dict[str, Any]] = []
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, ts_utc, owner, event_type, connection_id, strategy_id, config_path,
                               refresh_ms, client_ip, user_agent, detail_json
                        FROM ws_connection_events
                        {where_sql}
                        ORDER BY id DESC
                        LIMIT %s
                        """,
                        tuple(params),
                    )
                    for row in cur.fetchall():
                        try:
                            detail_payload = json.loads(str(row["detail_json"] or "{}"))
                        except Exception:
                            detail_payload = {}
                        if not isinstance(detail_payload, dict):
                            detail_payload = {"raw": detail_payload}
                        rows.append(
                            {
                                "id": int(row["id"] or 0),
                                "ts": str(row["ts_utc"] or ""),
                                "owner": str(row["owner"] or ""),
                                "eventType": str(row["event_type"] or ""),
                                "connectionId": str(row["connection_id"] or ""),
                                "strategyId": str(row["strategy_id"] or ""),
                                "configPath": str(row["config_path"] or ""),
                                "refreshMs": int(row["refresh_ms"] or 0),
                                "clientIp": str(row["client_ip"] or ""),
                                "userAgent": str(row["user_agent"] or ""),
                                "detail": detail_payload,
                                "cursorId": int(row["id"] or 0),
                            }
                        )
        return rows

    def _extract_diagnostics_snapshot_summary(
        self,
        snapshot: Dict[str, Any],
    ) -> Dict[str, Any]:
        if not isinstance(snapshot, dict):
            snapshot = {}
        signal = snapshot.get("signal_evaluation")
        if not isinstance(signal, dict):
            signal = {}
        strategy_state = snapshot.get("strategy_state")
        if not isinstance(strategy_state, dict):
            strategy_state = {}
        market_data = snapshot.get("market_data")
        if not isinstance(market_data, dict):
            market_data = {}
        exceptions = snapshot.get("exceptions")
        if not isinstance(exceptions, dict):
            exceptions = {}

        filter_reasons = signal.get("filter_reasons")
        if not isinstance(filter_reasons, list):
            filter_reasons = []
        entry_signal = bool(signal.get("entry_signal"))
        try:
            exception_total_count = max(0, int(exceptions.get("total_count") or 0))
        except Exception:
            exception_total_count = 0

        return {
            "generated_at": str(snapshot.get("generated_at") or _now_iso()),
            "strategy_state": str(strategy_state.get("state") or ""),
            "data_source_status": str(market_data.get("data_source_status") or ""),
            "entry_signal": entry_signal,
            "exception_total_count": exception_total_count,
            "filter_reasons": [str(item) for item in filter_reasons if str(item or "").strip()],
        }

    def append_strategy_diagnostics_snapshot(
        self,
        *,
        owner: str,
        strategy_id: str,
        source_path: str,
        snapshot: Dict[str, Any],
        ts_utc: Optional[str] = None,
    ) -> None:
        self._ensure_ready()
        summary = self._extract_diagnostics_snapshot_summary(snapshot)
        filter_reasons_payload = json.dumps(summary["filter_reasons"], ensure_ascii=False, separators=(",", ":"))
        snapshot_payload = json.dumps(snapshot or {}, ensure_ascii=False, separators=(",", ":"))
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO strategy_diagnostics_snapshots (
                            ts_utc, owner, strategy_id, source_path, generated_at, strategy_state, data_source_status,
                            entry_signal, exception_total_count, filter_reasons_json, snapshot_json
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            str(ts_utc or _now_iso()),
                            str(owner or ""),
                            str(strategy_id or ""),
                            str(source_path or ""),
                            str(summary["generated_at"] or _now_iso()),
                            str(summary["strategy_state"] or ""),
                            str(summary["data_source_status"] or ""),
                            bool(summary["entry_signal"]),
                            int(summary["exception_total_count"] or 0),
                            filter_reasons_payload,
                            snapshot_payload,
                        ),
                    )
                conn.commit()

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
        self._ensure_ready()
        clauses: List[str] = []
        params: List[Any] = []
        if owner:
            clauses.append("owner = %s")
            params.append(str(owner))
        if strategy_id:
            clauses.append("strategy_id = %s")
            params.append(str(strategy_id))
        if start_ts:
            clauses.append("ts_utc >= %s")
            params.append(str(start_ts))
        if end_ts:
            clauses.append("ts_utc <= %s")
            params.append(str(end_ts))
        if cursor_id is not None:
            try:
                safe_cursor = int(cursor_id)
            except Exception:
                safe_cursor = 0
            if safe_cursor > 0:
                clauses.append("id < %s")
                params.append(safe_cursor)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        safe_limit = max(1, min(int(limit), 2000))
        params.append(safe_limit)

        rows: List[Dict[str, Any]] = []
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT
                            id, ts_utc, owner, strategy_id, source_path, generated_at,
                            strategy_state, data_source_status, entry_signal, exception_total_count,
                            filter_reasons_json, snapshot_json
                        FROM strategy_diagnostics_snapshots
                        {where_sql}
                        ORDER BY id DESC
                        LIMIT %s
                        """,
                        tuple(params),
                    )
                    for row in cur.fetchall():
                        try:
                            filter_reasons = json.loads(str(row["filter_reasons_json"] or "[]"))
                        except Exception:
                            filter_reasons = []
                        if not isinstance(filter_reasons, list):
                            filter_reasons = []
                        out: Dict[str, Any] = {
                            "id": int(row["id"]),
                            "ts": str(row["ts_utc"] or ""),
                            "owner": str(row["owner"] or ""),
                            "strategyId": str(row["strategy_id"] or ""),
                            "sourcePath": str(row["source_path"] or ""),
                            "generatedAt": str(row["generated_at"] or ""),
                            "strategyState": str(row["strategy_state"] or ""),
                            "dataSourceStatus": str(row["data_source_status"] or ""),
                            "entrySignal": bool(row["entry_signal"]),
                            "exceptionTotalCount": int(row["exception_total_count"] or 0),
                            "filterReasons": [str(item) for item in filter_reasons],
                            "cursorId": int(row["id"]),
                        }
                        if include_snapshot:
                            try:
                                snapshot_payload = json.loads(str(row["snapshot_json"] or "{}"))
                            except Exception:
                                snapshot_payload = {}
                            if not isinstance(snapshot_payload, dict):
                                snapshot_payload = {"raw": snapshot_payload}
                            out["snapshot"] = snapshot_payload
                        rows.append(out)
        return rows

    def replace_backtest_trades(
        self,
        *,
        run_id: str,
        owner: str,
        rows: List[Dict[str, Any]],
    ) -> int:
        self._ensure_ready()
        run_key = str(run_id or "").strip()
        owner_key = str(owner or "").strip()
        if not run_key or not owner_key:
            return 0

        payload: List[tuple] = []
        for idx, item in enumerate(rows):
            if not isinstance(item, dict):
                continue
            seq = idx + 1
            ts_utc = str(item.get("ts") or item.get("ts_utc") or _now_iso())
            side_raw = str(item.get("side") or "buy").strip().lower()
            side = "sell" if side_raw == "sell" else "buy"
            extra = item.get("extra")
            if not isinstance(extra, dict):
                extra = {}
            payload.append(
                (
                    run_key,
                    owner_key,
                    int(seq),
                    str(item.get("id") or item.get("trade_id") or ""),
                    ts_utc,
                    str(item.get("symbol") or ""),
                    side,
                    float(_as_float_or_none(item.get("qty")) or 0.0),
                    float(_as_float_or_none(item.get("price")) or 0.0),
                    float(_as_float_or_none(item.get("fee")) or 0.0),
                    float(_as_float_or_none(item.get("pnl")) or 0.0),
                    str(item.get("orderId") or item.get("order_id") or ""),
                    json.dumps(extra, ensure_ascii=False, separators=(",", ":")),
                )
            )

        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM backtest_trades WHERE run_id = %s AND owner = %s",
                        (run_key, owner_key),
                    )
                    if payload:
                        for item in payload:
                            cur.execute(
                                """
                                INSERT INTO backtest_trades (
                                    run_id, owner, seq, trade_id, ts_utc, symbol, side, qty, price, fee, pnl, order_id, extra_json
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                """,
                                item,
                            )
                conn.commit()
        return len(payload)

    def replace_backtest_equity_points(
        self,
        *,
        run_id: str,
        owner: str,
        rows: List[Dict[str, Any]],
    ) -> int:
        self._ensure_ready()
        run_key = str(run_id or "").strip()
        owner_key = str(owner or "").strip()
        if not run_key or not owner_key:
            return 0

        payload: List[tuple] = []
        for idx, item in enumerate(rows):
            if not isinstance(item, dict):
                continue
            seq = idx + 1
            ts_utc = str(item.get("ts") or item.get("ts_utc") or _now_iso())
            payload.append(
                (
                    run_key,
                    owner_key,
                    int(seq),
                    ts_utc,
                    float(_as_float_or_none(item.get("equity")) or 0.0),
                    float(_as_float_or_none(item.get("pnl")) or 0.0),
                    float(_as_float_or_none(item.get("dd")) or 0.0),
                )
            )

        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "DELETE FROM backtest_equity_points WHERE run_id = %s AND owner = %s",
                        (run_key, owner_key),
                    )
                    if payload:
                        for item in payload:
                            cur.execute(
                                """
                                INSERT INTO backtest_equity_points (
                                    run_id, owner, seq, ts_utc, equity, pnl, dd
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                                """,
                                item,
                            )
                conn.commit()
        return len(payload)

    def list_backtest_trades(
        self,
        *,
        run_id: str,
        owner: str,
        limit: int = 10000,
    ) -> List[Dict[str, Any]]:
        self._ensure_ready()
        run_key = str(run_id or "").strip()
        owner_key = str(owner or "").strip()
        if not run_key or not owner_key:
            return []
        safe_limit = max(1, min(int(limit), 50000))
        rows: List[Dict[str, Any]] = []
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT seq, trade_id, ts_utc, symbol, side, qty, price, fee, pnl, order_id, extra_json
                        FROM backtest_trades
                        WHERE run_id = %s AND owner = %s
                        ORDER BY seq ASC
                        LIMIT %s
                        """,
                        (run_key, owner_key, safe_limit),
                    )
                    for row in cur.fetchall():
                        try:
                            extra = json.loads(str(row["extra_json"] or "{}"))
                        except Exception:
                            extra = {}
                        if not isinstance(extra, dict):
                            extra = {"raw": extra}
                        rows.append(
                            {
                                "id": str(row["trade_id"] or ""),
                                "seq": int(row["seq"] or 0),
                                "ts": str(row["ts_utc"] or ""),
                                "symbol": str(row["symbol"] or ""),
                                "side": str(row["side"] or "buy"),
                                "qty": float(row["qty"] or 0.0),
                                "price": float(row["price"] or 0.0),
                                "fee": float(row["fee"] or 0.0),
                                "pnl": float(row["pnl"] or 0.0),
                                "orderId": str(row["order_id"] or ""),
                                "extra": extra,
                            }
                        )
        return rows

    def list_backtest_equity_points(
        self,
        *,
        run_id: str,
        owner: str,
        limit: int = 10000,
    ) -> List[Dict[str, Any]]:
        self._ensure_ready()
        run_key = str(run_id or "").strip()
        owner_key = str(owner or "").strip()
        if not run_key or not owner_key:
            return []
        safe_limit = max(1, min(int(limit), 50000))
        rows: List[Dict[str, Any]] = []
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT seq, ts_utc, equity, pnl, dd
                        FROM backtest_equity_points
                        WHERE run_id = %s AND owner = %s
                        ORDER BY seq ASC
                        LIMIT %s
                        """,
                        (run_key, owner_key, safe_limit),
                    )
                    for row in cur.fetchall():
                        rows.append(
                            {
                                "seq": int(row["seq"] or 0),
                                "ts": str(row["ts_utc"] or ""),
                                "equity": float(row["equity"] or 0.0),
                                "pnl": float(row["pnl"] or 0.0),
                                "dd": float(row["dd"] or 0.0),
                            }
                        )
        return rows

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
        self._ensure_ready()
        owner_key = str(owner or "").strip()
        scope_key = str(scope or "").strip().lower()
        file_key_text = str(file_key or "").strip()
        if not owner_key or not scope_key or not file_key_text:
            raise ValueError("owner/scope/file_key are required")
        payload_text = str(content_text or "")
        meta_payload = json.dumps(meta or {}, ensure_ascii=False, separators=(",", ":"))
        now_iso = _now_iso()
        content_hash = hashlib.sha256(payload_text.encode("utf-8")).hexdigest()
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO data_files (
                            owner, scope, file_key, file_name, source_path,
                            content_type, content_encoding, content_text, content_sha256,
                            meta_json, created_at, updated_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT(owner, scope, file_key) DO UPDATE SET
                            file_name=EXCLUDED.file_name,
                            source_path=EXCLUDED.source_path,
                            content_type=EXCLUDED.content_type,
                            content_encoding=EXCLUDED.content_encoding,
                            content_text=EXCLUDED.content_text,
                            content_sha256=EXCLUDED.content_sha256,
                            meta_json=EXCLUDED.meta_json,
                            updated_at=EXCLUDED.updated_at
                        RETURNING
                            id, owner, scope, file_key, file_name, source_path,
                            content_type, content_encoding, content_text, content_sha256,
                            meta_json, created_at, updated_at
                        """,
                        (
                            owner_key,
                            scope_key,
                            file_key_text,
                            str(file_name or ""),
                            str(source_path or ""),
                            str(content_type or "text/plain"),
                            str(content_encoding or "utf-8"),
                            payload_text,
                            content_hash,
                            meta_payload,
                            now_iso,
                            now_iso,
                        ),
                    )
                    row = cur.fetchone()
                conn.commit()
        if row is None:
            raise RuntimeError("failed to upsert data file")
        try:
            meta_value = json.loads(str(row["meta_json"] or "{}"))
        except Exception:
            meta_value = {}
        if not isinstance(meta_value, dict):
            meta_value = {"raw": meta_value}
        return {
            "id": int(row["id"]),
            "owner": str(row["owner"] or ""),
            "scope": str(row["scope"] or ""),
            "fileKey": str(row["file_key"] or ""),
            "fileName": str(row["file_name"] or ""),
            "sourcePath": str(row["source_path"] or ""),
            "contentType": str(row["content_type"] or "text/plain"),
            "contentEncoding": str(row["content_encoding"] or "utf-8"),
            "contentText": str(row["content_text"] or ""),
            "contentSha256": str(row["content_sha256"] or ""),
            "meta": meta_value,
            "createdAt": str(row["created_at"] or ""),
            "updatedAt": str(row["updated_at"] or ""),
        }

    def get_data_file(
        self,
        *,
        owner: str,
        scope: str,
        file_key: str,
    ) -> Optional[Dict[str, Any]]:
        self._ensure_ready()
        owner_key = str(owner or "").strip()
        scope_key = str(scope or "").strip().lower()
        file_key_text = str(file_key or "").strip()
        if not owner_key or not scope_key or not file_key_text:
            return None
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT
                            id, owner, scope, file_key, file_name, source_path,
                            content_type, content_encoding, content_text, content_sha256,
                            meta_json, created_at, updated_at
                        FROM data_files
                        WHERE owner = %s AND scope = %s AND file_key = %s
                        LIMIT 1
                        """,
                        (owner_key, scope_key, file_key_text),
                    )
                    row = cur.fetchone()
        if row is None:
            return None
        try:
            meta_value = json.loads(str(row["meta_json"] or "{}"))
        except Exception:
            meta_value = {}
        if not isinstance(meta_value, dict):
            meta_value = {"raw": meta_value}
        return {
            "id": int(row["id"]),
            "owner": str(row["owner"] or ""),
            "scope": str(row["scope"] or ""),
            "fileKey": str(row["file_key"] or ""),
            "fileName": str(row["file_name"] or ""),
            "sourcePath": str(row["source_path"] or ""),
            "contentType": str(row["content_type"] or "text/plain"),
            "contentEncoding": str(row["content_encoding"] or "utf-8"),
            "contentText": str(row["content_text"] or ""),
            "contentSha256": str(row["content_sha256"] or ""),
            "meta": meta_value,
            "createdAt": str(row["created_at"] or ""),
            "updatedAt": str(row["updated_at"] or ""),
        }

    def delete_strategy(self, strategy_key: str) -> None:
        self._ensure_ready()
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM strategies WHERE strategy_key = %s", (strategy_key,))
                    cur.execute("DELETE FROM strategy_params WHERE strategy_key = %s", (strategy_key,))
                conn.commit()

    def load_strategies(self) -> List[Dict[str, Any]]:
        self._ensure_ready()
        rows: List[Dict[str, Any]] = []
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT strategy_key, owner, record_json FROM strategies ORDER BY updated_at DESC, strategy_key ASC")
                    for row in cur.fetchall():
                        raw = str(row["record_json"] or "")
                        if not raw:
                            continue
                        try:
                            payload = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(payload, dict):
                            continue
                        rows.append(
                            {
                                "strategy_key": str(row["strategy_key"] or ""),
                                "owner": str(row["owner"] or ""),
                                "record": payload,
                            }
                        )
        return rows

    def upsert_backtest(self, run_id: str, owner: str, record: Dict[str, Any]) -> None:
        self._ensure_ready()
        payload = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        status = str(record.get("status") or "running")
        created_at = str(record.get("createdAt") or _now_iso())
        updated_at = str(record.get("updatedAt") or _now_iso())
        structured = self._extract_backtest_structured(record)
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO backtests (
                            run_id,
                            owner,
                            strategy_id,
                            strategy_name,
                            symbol,
                            start_at,
                            end_at,
                            status,
                            metric_return,
                            metric_sharpe,
                            metric_calmar,
                            metric_max_drawdown,
                            created_at,
                            updated_at,
                            record_json
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT(run_id) DO UPDATE SET
                            owner=EXCLUDED.owner,
                            strategy_id=EXCLUDED.strategy_id,
                            strategy_name=EXCLUDED.strategy_name,
                            symbol=EXCLUDED.symbol,
                            start_at=EXCLUDED.start_at,
                            end_at=EXCLUDED.end_at,
                            status=EXCLUDED.status,
                            metric_return=EXCLUDED.metric_return,
                            metric_sharpe=EXCLUDED.metric_sharpe,
                            metric_calmar=EXCLUDED.metric_calmar,
                            metric_max_drawdown=EXCLUDED.metric_max_drawdown,
                            updated_at=EXCLUDED.updated_at,
                            record_json=EXCLUDED.record_json
                        """,
                        (
                            run_id,
                            owner,
                            structured["strategy_id"],
                            structured["strategy_name"],
                            structured["symbol"],
                            structured["start_at"],
                            structured["end_at"],
                            status,
                            structured["metric_return"],
                            structured["metric_sharpe"],
                            structured["metric_calmar"],
                            structured["metric_max_drawdown"],
                            created_at,
                            updated_at,
                            payload,
                        ),
                    )
                conn.commit()

    def load_backtests(self) -> List[Dict[str, Any]]:
        self._ensure_ready()
        rows: List[Dict[str, Any]] = []
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT run_id, owner, record_json FROM backtests ORDER BY created_at DESC, run_id DESC")
                    for row in cur.fetchall():
                        raw = str(row["record_json"] or "")
                        if not raw:
                            continue
                        try:
                            payload = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(payload, dict):
                            continue
                        rows.append(
                            {
                                "run_id": str(row["run_id"] or ""),
                                "owner": str(row["owner"] or ""),
                                "record": payload,
                            }
                        )
        return rows

    def upsert_risk_state(self, owner: str, strategy_key: str, state: Dict[str, Any]) -> None:
        self._ensure_ready()
        payload = json.dumps(state, ensure_ascii=False, separators=(",", ":"))
        updated_at = str(state.get("updatedAt") or _now_iso())
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT state_json FROM risk_states WHERE owner = %s AND strategy_key = %s",
                        (owner, strategy_key),
                    )
                    previous_row = cur.fetchone()
                    previous_json = str(previous_row["state_json"] or "") if previous_row is not None else ""
                    cur.execute(
                        """
                        INSERT INTO risk_states (owner, strategy_key, updated_at, state_json)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT(owner, strategy_key) DO UPDATE SET
                            updated_at=EXCLUDED.updated_at,
                            state_json=EXCLUDED.state_json
                        """,
                        (owner, strategy_key, updated_at, payload),
                    )
                    if previous_json != payload:
                        cur.execute(
                            "SELECT COALESCE(MAX(version), 0) AS max_version FROM risk_state_history WHERE owner = %s AND strategy_key = %s",
                            (owner, strategy_key),
                        )
                        version_row = cur.fetchone()
                        next_version = int(version_row["max_version"] or 0) + 1 if version_row is not None else 1
                        cur.execute(
                            """
                            INSERT INTO risk_state_history (
                                owner, strategy_key, version, updated_at, change_type, state_json
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                            (owner, strategy_key, next_version, updated_at, "upsert", payload),
                        )
                conn.commit()

    def delete_risk_state(self, owner: str, strategy_key: str) -> None:
        self._ensure_ready()
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT updated_at, state_json FROM risk_states WHERE owner = %s AND strategy_key = %s",
                        (owner, strategy_key),
                    )
                    previous_row = cur.fetchone()
                    cur.execute("DELETE FROM risk_states WHERE owner = %s AND strategy_key = %s", (owner, strategy_key))
                    if previous_row is not None:
                        cur.execute(
                            "SELECT COALESCE(MAX(version), 0) AS max_version FROM risk_state_history WHERE owner = %s AND strategy_key = %s",
                            (owner, strategy_key),
                        )
                        version_row = cur.fetchone()
                        next_version = int(version_row["max_version"] or 0) + 1 if version_row is not None else 1
                        cur.execute(
                            """
                            INSERT INTO risk_state_history (
                                owner, strategy_key, version, updated_at, change_type, state_json
                            ) VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                            (
                                owner,
                                strategy_key,
                                next_version,
                                str(previous_row["updated_at"] or _now_iso()),
                                "delete",
                                str(previous_row["state_json"] or "{}"),
                            ),
                        )
                conn.commit()

    def load_risk_states(self) -> List[Dict[str, Any]]:
        self._ensure_ready()
        rows: List[Dict[str, Any]] = []
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT owner, strategy_key, state_json FROM risk_states ORDER BY updated_at DESC, strategy_key ASC")
                    for row in cur.fetchall():
                        raw = str(row["state_json"] or "")
                        if not raw:
                            continue
                        try:
                            payload = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(payload, dict):
                            continue
                        rows.append(
                            {
                                "owner": str(row["owner"] or ""),
                                "strategy_key": str(row["strategy_key"] or ""),
                                "state": payload,
                            }
                        )
        return rows

    def list_risk_state_history(
        self,
        *,
        owner: Optional[str] = None,
        strategy_key: Optional[str] = None,
        cursor_id: Optional[int] = None,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        self._ensure_ready()
        clauses: List[str] = []
        params: List[Any] = []
        if owner:
            clauses.append("owner = %s")
            params.append(owner)
        if strategy_key:
            clauses.append("strategy_key = %s")
            params.append(strategy_key)
        if cursor_id is not None:
            try:
                safe_cursor = int(cursor_id)
            except Exception:
                safe_cursor = 0
            if safe_cursor > 0:
                clauses.append("id < %s")
                params.append(safe_cursor)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        safe_limit = max(1, min(int(limit), 2000))
        params.append(safe_limit)

        rows: List[Dict[str, Any]] = []
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, owner, strategy_key, version, updated_at, change_type, state_json
                        FROM risk_state_history
                        {where_sql}
                        ORDER BY id DESC
                        LIMIT %s
                        """,
                        tuple(params),
                    )
                    for row in cur.fetchall():
                        raw = str(row["state_json"] or "{}")
                        try:
                            state = json.loads(raw)
                        except Exception:
                            state = {}
                        if not isinstance(state, dict):
                            state = {"raw": state}
                        rows.append(
                            {
                                "id": int(row["id"]),
                                "owner": str(row["owner"] or ""),
                                "strategyKey": str(row["strategy_key"] or ""),
                                "version": int(row["version"] or 0),
                                "updatedAt": str(row["updated_at"] or ""),
                                "changeType": str(row["change_type"] or ""),
                                "state": state,
                            }
                        )
        return rows

    def append_audit_log(
        self,
        owner: str,
        action: str,
        entity: str,
        entity_id: str,
        detail: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._ensure_ready()
        payload = json.dumps(detail or {}, ensure_ascii=False, separators=(",", ":"))
        ts_utc = _now_iso()
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT row_hash FROM audit_logs WHERE owner = %s ORDER BY id DESC LIMIT 1", (owner,))
                    previous_row = cur.fetchone()
                    prev_hash = str(previous_row["row_hash"] or "") if previous_row is not None else ""
                    if not prev_hash:
                        prev_hash = "0" * 64
                    row_hash = self._audit_row_hash(
                        owner=owner,
                        ts_utc=ts_utc,
                        action=action,
                        entity=entity,
                        entity_id=entity_id,
                        detail_json=payload,
                        prev_hash=prev_hash,
                    )
                    cur.execute(
                        """
                        INSERT INTO audit_logs (
                            ts_utc, owner, action, entity, entity_id, detail_json, prev_hash, row_hash, chain_version
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 1)
                        """,
                        (ts_utc, owner, action, entity, entity_id, payload, prev_hash, row_hash),
                    )
                conn.commit()

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
        self._ensure_ready()
        clauses: List[str] = []
        params: List[Any] = []
        if owner:
            clauses.append("owner = %s")
            params.append(owner)
        if action:
            clauses.append("action = %s")
            params.append(action)
        if entity:
            clauses.append("entity = %s")
            params.append(entity)
        if start_ts:
            clauses.append("ts_utc >= %s")
            params.append(start_ts)
        if end_ts:
            clauses.append("ts_utc <= %s")
            params.append(end_ts)
        if cursor_id is not None:
            try:
                safe_cursor = int(cursor_id)
            except Exception:
                safe_cursor = 0
            if safe_cursor > 0:
                clauses.append("id < %s")
                params.append(safe_cursor)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        safe_limit = max(1, min(int(limit), 2000))
        params.append(safe_limit)

        rows: List[Dict[str, Any]] = []
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, ts_utc, owner, action, entity, entity_id, detail_json, prev_hash, row_hash, chain_version
                        FROM audit_logs
                        {where_sql}
                        ORDER BY id DESC
                        LIMIT %s
                        """,
                        tuple(params),
                    )
                    for row in cur.fetchall():
                        try:
                            detail = json.loads(str(row["detail_json"] or "{}"))
                        except Exception:
                            detail = {}
                        if not isinstance(detail, dict):
                            detail = {"raw": detail}
                        rows.append(
                            {
                                "id": int(row["id"]),
                                "ts": str(row["ts_utc"] or ""),
                                "owner": str(row["owner"] or ""),
                                "action": str(row["action"] or ""),
                                "entity": str(row["entity"] or ""),
                                "entityId": str(row["entity_id"] or ""),
                                "prevHash": str(row["prev_hash"] or ""),
                                "rowHash": str(row["row_hash"] or ""),
                                "chainVersion": int(row["chain_version"] or 1),
                                "detail": detail,
                            }
                        )
        return rows

    def verify_audit_hash_chain(
        self,
        *,
        owner: Optional[str] = None,
        start_id: Optional[int] = None,
        end_id: Optional[int] = None,
        limit: int = 5000,
    ) -> Dict[str, Any]:
        self._ensure_ready()
        clauses: List[str] = []
        params: List[Any] = []
        if owner:
            clauses.append("owner = %s")
            params.append(owner)
        if start_id is not None:
            clauses.append("id >= %s")
            params.append(int(start_id))
        if end_id is not None:
            clauses.append("id <= %s")
            params.append(int(end_id))
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        safe_limit = max(1, min(int(limit), 100000))
        params.append(safe_limit)

        checked = 0
        mismatched_rows: List[int] = []
        broken_links: List[int] = []
        previous_hash_by_owner: Dict[str, str] = {}
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, ts_utc, owner, action, entity, entity_id, detail_json, prev_hash, row_hash
                        FROM audit_logs
                        {where_sql}
                        ORDER BY owner ASC, id ASC
                        LIMIT %s
                        """,
                        tuple(params),
                    )
                    for row in cur.fetchall():
                        checked += 1
                        row_id = int(row["id"])
                        owner_key = str(row["owner"] or "")
                        prev_hash = str(row["prev_hash"] or "")
                        row_hash = str(row["row_hash"] or "")
                        recomputed = self._audit_row_hash(
                            owner=owner_key,
                            ts_utc=str(row["ts_utc"] or ""),
                            action=str(row["action"] or ""),
                            entity=str(row["entity"] or ""),
                            entity_id=str(row["entity_id"] or ""),
                            detail_json=str(row["detail_json"] or "{}"),
                            prev_hash=prev_hash,
                        )
                        if row_hash != recomputed:
                            mismatched_rows.append(row_id)
                        expected_prev = previous_hash_by_owner.get(owner_key)
                        if expected_prev is not None and prev_hash != expected_prev:
                            broken_links.append(row_id)
                        previous_hash_by_owner[owner_key] = row_hash
        return {
            "checked": checked,
            "mismatchedRows": mismatched_rows,
            "brokenLinks": broken_links,
            "ok": not mismatched_rows and not broken_links,
        }

    def upsert_market_ticks(
        self,
        rows: List[Dict[str, Any]],
        *,
        source_config_path: str = "",
    ) -> int:
        self._ensure_ready()
        payload: List[tuple] = []
        now_iso = _now_iso()
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or "").strip()
            ts_utc = str(row.get("ts_utc") or row.get("ts") or "").strip()
            if not symbol or not ts_utc:
                continue
            payload.append(
                (
                    symbol,
                    ts_utc,
                    float(_as_float_or_none(row.get("price")) or 0.0),
                    float(_as_float_or_none(row.get("bid")) or 0.0),
                    float(_as_float_or_none(row.get("ask")) or 0.0),
                    float(_as_float_or_none(row.get("volume")) or 0.0),
                    str(source_config_path or ""),
                    now_iso,
                )
            )
        if not payload:
            return 0
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    for item in payload:
                        cur.execute(
                            """
                            INSERT INTO market_ticks (
                                symbol, ts_utc, price, bid, ask, volume, source_config_path, ingested_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT(symbol, ts_utc) DO UPDATE SET
                                price=EXCLUDED.price,
                                bid=EXCLUDED.bid,
                                ask=EXCLUDED.ask,
                                volume=EXCLUDED.volume,
                                source_config_path=EXCLUDED.source_config_path,
                                ingested_at=EXCLUDED.ingested_at
                            """,
                            item,
                        )
                conn.commit()
        return len(payload)

    def upsert_market_klines(
        self,
        rows: List[Dict[str, Any]],
        *,
        timeframe: str,
        source_config_path: str = "",
    ) -> int:
        self._ensure_ready()
        payload: List[tuple] = []
        now_iso = _now_iso()
        tf = str(timeframe or "").strip()
        for row in rows:
            if not isinstance(row, dict):
                continue
            symbol = str(row.get("symbol") or "").strip()
            ts_utc = str(row.get("ts_utc") or "").strip()
            if not symbol or not ts_utc or not tf:
                continue
            try:
                time_sec = int(row.get("time") or 0)
            except Exception:
                time_sec = 0
            if time_sec <= 0:
                continue
            payload.append(
                (
                    symbol,
                    tf,
                    ts_utc,
                    time_sec,
                    float(_as_float_or_none(row.get("open")) or 0.0),
                    float(_as_float_or_none(row.get("high")) or 0.0),
                    float(_as_float_or_none(row.get("low")) or 0.0),
                    float(_as_float_or_none(row.get("close")) or 0.0),
                    float(_as_float_or_none(row.get("volume")) or 0.0),
                    str(source_config_path or ""),
                    now_iso,
                )
            )
        if not payload:
            return 0
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    for item in payload:
                        cur.execute(
                            """
                            INSERT INTO market_klines (
                                symbol, timeframe, ts_utc, time_sec, open, high, low, close, volume, source_config_path, ingested_at
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT(symbol, timeframe, ts_utc) DO UPDATE SET
                                time_sec=EXCLUDED.time_sec,
                                open=EXCLUDED.open,
                                high=EXCLUDED.high,
                                low=EXCLUDED.low,
                                close=EXCLUDED.close,
                                volume=EXCLUDED.volume,
                                source_config_path=EXCLUDED.source_config_path,
                                ingested_at=EXCLUDED.ingested_at
                            """,
                            item,
                        )
                conn.commit()
        return len(payload)

    def build_db_report_summary(
        self,
        *,
        owner: Optional[str] = None,
        start_ts: Optional[str] = None,
        end_ts: Optional[str] = None,
        limit_top: int = 10,
    ) -> Dict[str, Any]:
        self._ensure_ready()
        safe_top = max(1, min(int(limit_top), 100))

        def _where(prefix: str = "") -> tuple[str, List[Any]]:
            clauses: List[str] = []
            params: List[Any] = []
            if owner:
                clauses.append(f"{prefix}owner = %s")
                params.append(owner)
            if start_ts:
                clauses.append(f"{prefix}ts_utc >= %s")
                params.append(start_ts)
            if end_ts:
                clauses.append(f"{prefix}ts_utc <= %s")
                params.append(end_ts)
            where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
            return where_sql, params

        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    audit_where, audit_params = _where()
                    cur.execute(f"SELECT COUNT(1) AS cnt FROM audit_logs {audit_where}", tuple(audit_params))
                    audit_total_row = cur.fetchone()
                    audit_total = int(audit_total_row["cnt"]) if audit_total_row is not None else 0

                    cur.execute(
                        f"""
                        SELECT action, COUNT(1) AS cnt
                        FROM audit_logs
                        {audit_where}
                        GROUP BY action
                        ORDER BY cnt DESC, action ASC
                        LIMIT %s
                        """,
                        tuple([*audit_params, safe_top]),
                    )
                    top_actions = [
                        {"action": str(row["action"] or ""), "count": int(row["cnt"] or 0)}
                        for row in cur.fetchall()
                    ]

                    cur.execute(
                        f"""
                        SELECT entity, COUNT(1) AS cnt
                        FROM audit_logs
                        {audit_where}
                        GROUP BY entity
                        ORDER BY cnt DESC, entity ASC
                        LIMIT %s
                        """,
                        tuple([*audit_params, safe_top]),
                    )
                    top_entities = [
                        {"entity": str(row["entity"] or ""), "count": int(row["cnt"] or 0)}
                        for row in cur.fetchall()
                    ]

                    risk_where, risk_params = _where()
                    cur.execute(f"SELECT COUNT(1) AS cnt FROM risk_events {risk_where}", tuple(risk_params))
                    risk_total_row = cur.fetchone()
                    risk_total = int(risk_total_row["cnt"]) if risk_total_row is not None else 0

                    cur.execute(
                        f"""
                        SELECT event_type, COUNT(1) AS cnt
                        FROM risk_events
                        {risk_where}
                        GROUP BY event_type
                        ORDER BY cnt DESC, event_type ASC
                        LIMIT %s
                        """,
                        tuple([*risk_params, safe_top]),
                    )
                    risk_by_type = [
                        {"eventType": str(row["event_type"] or ""), "count": int(row["cnt"] or 0)}
                        for row in cur.fetchall()
                    ]

                    cur.execute(
                        """
                        SELECT COUNT(1) AS cnt
                        FROM risk_state_history
                        WHERE (%s::text IS NULL OR owner = %s::text)
                        """,
                        (owner, owner),
                    )
                    risk_history_total_row = cur.fetchone()
                    risk_history_total = int(risk_history_total_row["cnt"]) if risk_history_total_row is not None else 0

                    alert_where, alert_params = _where()
                    cur.execute(f"SELECT COUNT(1) AS cnt FROM alert_deliveries {alert_where}", tuple(alert_params))
                    alert_total_row = cur.fetchone()
                    alert_total = int(alert_total_row["cnt"]) if alert_total_row is not None else 0

                    cur.execute(
                        f"SELECT COUNT(1) AS cnt FROM alert_deliveries {alert_where} {'AND' if alert_where else 'WHERE'} status = %s",
                        tuple([*alert_params, "failed"]),
                    )
                    alert_failed_row = cur.fetchone()
                    alert_failed_total = int(alert_failed_row["cnt"]) if alert_failed_row is not None else 0

                    cur.execute(
                        f"""
                        SELECT event, COUNT(1) AS cnt
                        FROM alert_deliveries
                        {alert_where}
                        GROUP BY event
                        ORDER BY cnt DESC, event ASC
                        LIMIT %s
                        """,
                        tuple([*alert_params, safe_top]),
                    )
                    alert_by_event = [
                        {"event": str(row["event"] or ""), "count": int(row["cnt"] or 0)}
                        for row in cur.fetchall()
                    ]

                    ws_where, ws_params = _where()
                    cur.execute(f"SELECT COUNT(1) AS cnt FROM ws_connection_events {ws_where}", tuple(ws_params))
                    ws_total_row = cur.fetchone()
                    ws_total = int(ws_total_row["cnt"]) if ws_total_row is not None else 0

                    cur.execute(
                        f"""
                        SELECT event_type, COUNT(1) AS cnt
                        FROM ws_connection_events
                        {ws_where}
                        GROUP BY event_type
                        ORDER BY cnt DESC, event_type ASC
                        LIMIT %s
                        """,
                        tuple([*ws_params, safe_top]),
                    )
                    ws_by_type = [
                        {"eventType": str(row["event_type"] or ""), "count": int(row["cnt"] or 0)}
                        for row in cur.fetchall()
                    ]

                    cur.execute("SELECT COUNT(1) AS cnt FROM market_ticks")
                    tick_total_row = cur.fetchone()
                    cur.execute("SELECT COUNT(1) AS cnt FROM market_klines")
                    kline_total_row = cur.fetchone()
                    market_counts = {
                        "ticks": int(tick_total_row["cnt"]) if tick_total_row is not None else 0,
                        "klines": int(kline_total_row["cnt"]) if kline_total_row is not None else 0,
                    }

        return {
            "auditTotal": audit_total,
            "topActions": top_actions,
            "topEntities": top_entities,
            "riskEventTotal": risk_total,
            "riskEventsByType": risk_by_type,
            "riskStateHistoryTotal": risk_history_total,
            "alertDeliveryTotal": alert_total,
            "alertDeliveryFailedTotal": alert_failed_total,
            "alertDeliveriesByEvent": alert_by_event,
            "wsConnectionEventTotal": ws_total,
            "wsEventsByType": ws_by_type,
            "marketTimeseries": market_counts,
        }

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
        self._ensure_ready()
        payload = json.dumps(detail or {}, ensure_ascii=False, separators=(",", ":"))
        event_type_text = str(event_type or "").strip().lower()
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO risk_events (
                            ts_utc, owner, strategy_key, event_type, rule, message, detail_json
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            str(ts_utc or _now_iso()),
                            str(owner or ""),
                            str(strategy_key or ""),
                            event_type_text,
                            str(rule or ""),
                            str(message or ""),
                            payload,
                        ),
                    )
                conn.commit()

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
        self._ensure_ready()
        clauses: List[str] = []
        params: List[Any] = []
        if owner:
            clauses.append("owner = %s")
            params.append(owner)
        if strategy_key:
            clauses.append("strategy_key = %s")
            params.append(strategy_key)
        if event_type:
            clauses.append("event_type = %s")
            params.append(str(event_type).strip().lower())
        if start_ts:
            clauses.append("ts_utc >= %s")
            params.append(start_ts)
        if end_ts:
            clauses.append("ts_utc <= %s")
            params.append(end_ts)
        if cursor_id is not None:
            try:
                safe_cursor = int(cursor_id)
            except Exception:
                safe_cursor = 0
            if safe_cursor > 0:
                clauses.append("id < %s")
                params.append(safe_cursor)
        where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        safe_limit = max(1, min(int(limit), 2000))
        params.append(safe_limit)

        rows: List[Dict[str, Any]] = []
        with self._lock:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f"""
                        SELECT id, ts_utc, owner, strategy_key, event_type, rule, message, detail_json
                        FROM risk_events
                        {where_sql}
                        ORDER BY id DESC
                        LIMIT %s
                        """,
                        tuple(params),
                    )
                    for row in cur.fetchall():
                        try:
                            detail = json.loads(str(row["detail_json"] or "{}"))
                        except Exception:
                            detail = {}
                        if not isinstance(detail, dict):
                            detail = {"raw": detail}
                        rows.append(
                            {
                                "id": int(row["id"]),
                                "ts": str(row["ts_utc"] or ""),
                                "owner": str(row["owner"] or ""),
                                "strategyKey": str(row["strategy_key"] or ""),
                                "eventType": str(row["event_type"] or ""),
                                "rule": str(row["rule"] or ""),
                                "message": str(row["message"] or ""),
                                "detail": detail,
                            }
                        )
        return rows
