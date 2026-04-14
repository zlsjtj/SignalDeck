import os
import tempfile
import unittest
import uuid
from pathlib import Path

from db_store import SQLiteStore
from postgres_store import PostgresStore


class PostgresSqliteFeatureParityFor133Tests(unittest.TestCase):
    def _exercise_store(self, store, prefix: str):
        store.initialize()
        owner = f"{prefix}_guest"
        strategy_key = f"usr__{owner}__strategy_{prefix}"
        session_id = f"{prefix}_session_{uuid.uuid4().hex}"
        lock_key = f"ip:127.0.0.1|{owner}"

        user = store.ensure_user(owner, role="guest", display_name=owner)
        store.upsert_user_credential(
            username=owner,
            password_hash=f"hash_{prefix}",
            algorithm="pbkdf2_sha256",
        )
        store.create_auth_session(
            session_id=session_id,
            username=owner,
            issued_at="2026-03-05T00:00:00+00:00",
            expires_at="2099-01-01T00:00:00+00:00",
            client_ip="127.0.0.1",
            user_agent="unit-test",
        )
        session = store.get_auth_session(session_id)

        store.record_login_attempt(
            username=owner,
            client_ip="127.0.0.1",
            success=False,
            reason="invalid",
            ts_utc="2026-03-05T00:00:00+00:00",
        )
        store.set_lockout(
            lock_key=lock_key,
            locked_until="2099-01-01T00:00:00+00:00",
            updated_at="2026-03-05T00:00:00+00:00",
        )
        active_lockouts = store.get_active_lockouts(
            lock_keys=[lock_key],
            now_ts="2026-03-05T00:00:01+00:00",
        )

        job = store.enqueue_strategy_compile_job(strategy_key, owner)
        store.update_strategy_compile_job(
            int(job["id"]),
            status="running",
            started_at="2026-03-05T00:01:00+00:00",
        )
        store.update_strategy_compile_job(
            int(job["id"]),
            status="success",
            finished_at="2026-03-05T00:01:05+00:00",
        )
        jobs = store.list_strategy_compile_jobs(owner=owner, strategy_key=strategy_key, limit=10)

        script = store.add_strategy_script(
            strategy_key=strategy_key,
            owner=owner,
            script_type="yaml_config",
            script_path=f"/tmp/{prefix}_strategy_v1.yaml",
            script_hash=f"hash_{prefix}",
            source_config={"symbols": ["BTC/USDT:USDT"], "timeframe": "1h"},
        )
        latest = store.get_latest_strategy_script(owner=owner, strategy_key=strategy_key)
        scripts = store.list_strategy_scripts(owner=owner, strategy_key=strategy_key, limit=10)

        store.revoke_auth_session(session_id, revoked_at="2026-03-05T00:02:00+00:00")
        session_revoked = store.get_auth_session(session_id)
        store.clear_lockouts([lock_key])
        active_lockouts_after_clear = store.get_active_lockouts(
            lock_keys=[lock_key],
            now_ts="2026-03-05T00:02:01+00:00",
        )
        store.upsert_user_preferences(
            owner,
            {"theme": "light", "language": "en", "selectedLiveStrategyId": "strategy_x"},
        )
        user_pref = store.get_user_preferences(owner)
        store.append_runtime_log(
            owner=owner,
            log_type="strategy",
            level="error",
            source="stderr",
            message=f"{prefix} compile failed",
            strategy_id=strategy_key,
            detail={"kind": "unit"},
            ts_utc="2026-03-05T00:03:00+00:00",
        )
        runtime_rows = store.list_runtime_logs(owner=owner, log_type="strategy", q="failed", limit=10)
        store.append_strategy_diagnostics_snapshot(
            owner=owner,
            strategy_id=strategy_key,
            source_path=f"/tmp/{prefix}_diag.json",
            snapshot={
                "generated_at": "2026-03-05T00:04:00+00:00",
                "strategy_state": {"state": "RUNNING"},
                "market_data": {"data_source_status": "ok"},
                "signal_evaluation": {"entry_signal": True, "filter_reasons": ["lag"]},
                "exceptions": {"total_count": 1},
            },
            ts_utc="2026-03-05T00:04:01+00:00",
        )
        diag_rows = store.list_strategy_diagnostics_snapshots(
            owner=owner,
            strategy_id=strategy_key,
            include_snapshot=False,
            limit=10,
        )
        backtest_run_id = f"{prefix}_run"
        store.replace_backtest_trades(
            run_id=backtest_run_id,
            owner=owner,
            rows=[
                {
                    "id": f"{prefix}_trade_1",
                    "ts": "2026-03-05T00:05:00+00:00",
                    "symbol": "BTC/USDT:USDT",
                    "side": "buy",
                    "qty": 1.0,
                    "price": 100.0,
                    "fee": 0.1,
                    "pnl": 0.0,
                    "orderId": f"{prefix}_order_1",
                }
            ],
        )
        store.replace_backtest_equity_points(
            run_id=backtest_run_id,
            owner=owner,
            rows=[
                {
                    "ts": "2026-03-05T00:00:00+00:00",
                    "equity": 1000.0,
                    "pnl": 0.0,
                    "dd": 0.0,
                },
                {
                    "ts": "2026-03-05T00:05:00+00:00",
                    "equity": 1002.0,
                    "pnl": 2.0,
                    "dd": 0.0,
                },
            ],
        )
        backtest_trade_rows = store.list_backtest_trades(run_id=backtest_run_id, owner=owner, limit=10)
        backtest_equity_rows = store.list_backtest_equity_points(run_id=backtest_run_id, owner=owner, limit=10)
        store.append_alert_delivery(
            owner=owner,
            event="db_runtime_persistence_failure",
            severity="critical",
            message=f"{prefix} alert",
            webhook_url="https://example.invalid/hook",
            status="failed",
            retry_count=1,
            http_status=500,
            error_message="http_status=500",
            payload={"kind": "unit"},
            ts_utc="2026-03-05T00:05:00+00:00",
            duration_ms=123.4,
        )
        alert_rows = store.list_alert_deliveries(owner=owner, status="failed", limit=10)
        store.append_ws_connection_event(
            owner=owner,
            event_type="connected",
            connection_id=f"{prefix}_ws_1",
            strategy_id=strategy_key,
            config_path=f"/tmp/{prefix}.yaml",
            refresh_ms=1000,
            client_ip="127.0.0.1",
            user_agent="unit-test",
            detail={"phase": "open"},
            ts_utc="2026-03-05T00:06:00+00:00",
        )
        ws_rows = store.list_ws_connection_events(owner=owner, event_type="connected", limit=10)
        store.append_account_security_event(
            owner=owner,
            event_type="login_success",
            severity="info",
            message=f"{prefix} login",
            detail={"client_ip": "127.0.0.1"},
            ts_utc="2026-03-05T00:07:00+00:00",
        )
        security_rows = store.list_account_security_events(owner=owner, event_type="login_success", limit=10)
        token_row = store.create_api_token(
            owner=owner,
            token_name=f"{prefix}-token",
            token_prefix=f"{prefix[:4]}_tok",
            token_hash=f"hash_{prefix}_token",
            scopes=["read", "trade"],
            expires_at="2099-01-01T00:00:00+00:00",
            created_by="admin",
        )
        token_id = int(token_row.get("id") or 0)
        active_token = store.get_active_api_token_by_hash(
            token_hash=f"hash_{prefix}_token",
            now_ts="2026-03-05T00:08:00+00:00",
        )
        store.touch_api_token_last_used(token_id, last_used_at="2026-03-05T00:08:30+00:00")
        store.revoke_api_token(token_id, revoked_at="2026-03-05T00:09:00+00:00", revoked_by="admin")
        active_after_revoke = store.get_active_api_token_by_hash(
            token_hash=f"hash_{prefix}_token",
            now_ts="2026-03-05T00:09:01+00:00",
        )
        token_rows_all = store.list_api_tokens(owner=owner, include_revoked=True, limit=10)
        available_roles = store.list_roles()
        available_permissions = store.list_permissions()
        initial_user_roles = store.list_user_roles(owner)
        replaced_roles = store.replace_user_roles(owner, ["auditor"])
        final_user_roles = store.list_user_roles(owner)
        can_read_audit_all = store.user_has_permission(owner, "audit.read.all")
        can_write_risk = store.user_has_permission(owner, "risk.write")

        return {
            "backend": str(getattr(store, "backend", "")),
            "user_role": str(user.get("role") or ""),
            "session_username": str((session or {}).get("username") or ""),
            "session_revoked_at": str((session_revoked or {}).get("revokedAt") or ""),
            "lockout_active": bool(active_lockouts),
            "lockout_after_clear": bool(active_lockouts_after_clear),
            "job_status": str((jobs[0] if jobs else {}).get("status") or ""),
            "script_version": int(script.get("version") or 0),
            "latest_script_version": int((latest or {}).get("version") or 0),
            "script_count": len(scripts),
            "pref_theme": str((((user_pref or {}).get("preferences") or {}).get("theme"))),
            "runtime_log_count": len(runtime_rows),
            "runtime_log_level": str((runtime_rows[0] if runtime_rows else {}).get("level") or ""),
            "diag_count": len(diag_rows),
            "diag_state": str((diag_rows[0] if diag_rows else {}).get("strategyState") or ""),
            "diag_exception_total": int((diag_rows[0] if diag_rows else {}).get("exceptionTotalCount") or 0),
            "backtest_trade_count": len(backtest_trade_rows),
            "backtest_trade_symbol": str((backtest_trade_rows[0] if backtest_trade_rows else {}).get("symbol") or ""),
            "backtest_equity_count": len(backtest_equity_rows),
            "backtest_equity_last": float((backtest_equity_rows[-1] if backtest_equity_rows else {}).get("equity") or 0.0),
            "alert_delivery_count": len(alert_rows),
            "alert_delivery_event": str((alert_rows[0] if alert_rows else {}).get("event") or ""),
            "alert_delivery_retry": int((alert_rows[0] if alert_rows else {}).get("retryCount") or 0),
            "ws_event_count": len(ws_rows),
            "ws_event_type": str((ws_rows[0] if ws_rows else {}).get("eventType") or ""),
            "ws_event_connection": str((ws_rows[0] if ws_rows else {}).get("connectionId") or ""),
            "security_event_count": len(security_rows),
            "security_event_type": str((security_rows[0] if security_rows else {}).get("eventType") or ""),
            "token_active_before_revoke": bool(active_token),
            "token_active_after_revoke": bool(active_after_revoke),
            "token_rows_all": len(token_rows_all),
            "token_revoked_by": str((token_rows_all[0] if token_rows_all else {}).get("revokedBy") or ""),
            "rbac_roles_count": len(available_roles),
            "rbac_permissions_count": len(available_permissions),
            "rbac_initial_roles_count": len(initial_user_roles),
            "rbac_replaced_roles_count": len(replaced_roles),
            "rbac_final_roles_count": len(final_user_roles),
            "rbac_can_read_audit_all": bool(can_read_audit_all),
            "rbac_can_write_risk": bool(can_write_risk),
        }

    def test_sqlite_store_supports_133_feature_set(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            sqlite_path = Path(tmp_dir) / "quant_api.db"
            sqlite_store = SQLiteStore(sqlite_path)
            summary = self._exercise_store(sqlite_store, prefix="sqlite")

        self.assertEqual(summary["backend"], "sqlite")
        self.assertEqual(summary["user_role"], "guest")
        self.assertEqual(summary["session_username"], "sqlite_guest")
        self.assertTrue(summary["session_revoked_at"])
        self.assertTrue(summary["lockout_active"])
        self.assertFalse(summary["lockout_after_clear"])
        self.assertEqual(summary["job_status"], "success")
        self.assertEqual(summary["script_version"], 1)
        self.assertEqual(summary["latest_script_version"], 1)
        self.assertEqual(summary["script_count"], 1)
        self.assertEqual(summary["pref_theme"], "light")
        self.assertEqual(summary["runtime_log_count"], 1)
        self.assertEqual(summary["runtime_log_level"], "error")
        self.assertEqual(summary["diag_count"], 1)
        self.assertEqual(summary["diag_state"], "RUNNING")
        self.assertEqual(summary["diag_exception_total"], 1)
        self.assertEqual(summary["backtest_trade_count"], 1)
        self.assertEqual(summary["backtest_trade_symbol"], "BTC/USDT:USDT")
        self.assertEqual(summary["backtest_equity_count"], 2)
        self.assertAlmostEqual(float(summary["backtest_equity_last"]), 1002.0, places=6)
        self.assertEqual(summary["alert_delivery_count"], 1)
        self.assertEqual(summary["alert_delivery_event"], "db_runtime_persistence_failure")
        self.assertEqual(summary["alert_delivery_retry"], 1)
        self.assertEqual(summary["ws_event_count"], 1)
        self.assertEqual(summary["ws_event_type"], "connected")
        self.assertTrue(summary["ws_event_connection"])
        self.assertEqual(summary["security_event_count"], 1)
        self.assertEqual(summary["security_event_type"], "login_success")
        self.assertTrue(summary["token_active_before_revoke"])
        self.assertFalse(summary["token_active_after_revoke"])
        self.assertEqual(summary["token_rows_all"], 1)
        self.assertEqual(summary["token_revoked_by"], "admin")
        self.assertGreaterEqual(summary["rbac_roles_count"], 4)
        self.assertGreaterEqual(summary["rbac_permissions_count"], 4)
        self.assertGreaterEqual(summary["rbac_initial_roles_count"], 1)
        self.assertEqual(summary["rbac_replaced_roles_count"], 1)
        self.assertEqual(summary["rbac_final_roles_count"], 1)
        self.assertTrue(summary["rbac_can_read_audit_all"])
        self.assertFalse(summary["rbac_can_write_risk"])

    @unittest.skipUnless(
        bool(str(os.getenv("QUANT_E2E_POSTGRES_DSN", "")).strip()),
        "requires QUANT_E2E_POSTGRES_DSN for PostgreSQL parity test",
    )
    def test_postgres_sqlite_parity_for_133_features(self):
        dsn = str(os.getenv("QUANT_E2E_POSTGRES_DSN", "")).strip()
        with tempfile.TemporaryDirectory() as tmp_dir:
            sqlite_path = Path(tmp_dir) / "quant_api.db"
            sqlite_store = SQLiteStore(sqlite_path)
            sqlite_summary = self._exercise_store(sqlite_store, prefix=f"sqlite_{uuid.uuid4().hex[:8]}")

        postgres_store = PostgresStore(dsn)
        postgres_summary = self._exercise_store(postgres_store, prefix=f"pg_{uuid.uuid4().hex[:8]}")

        for key in (
            "user_role",
            "lockout_active",
            "lockout_after_clear",
            "job_status",
            "script_version",
            "latest_script_version",
            "script_count",
            "pref_theme",
            "runtime_log_count",
            "runtime_log_level",
            "diag_count",
            "diag_state",
            "diag_exception_total",
            "backtest_trade_count",
            "backtest_trade_symbol",
            "backtest_equity_count",
            "backtest_equity_last",
            "alert_delivery_count",
            "alert_delivery_event",
            "alert_delivery_retry",
            "ws_event_count",
            "ws_event_type",
            "ws_event_connection",
            "security_event_count",
            "security_event_type",
            "token_active_before_revoke",
            "token_active_after_revoke",
            "token_rows_all",
            "token_revoked_by",
            "rbac_roles_count",
            "rbac_permissions_count",
            "rbac_initial_roles_count",
            "rbac_replaced_roles_count",
            "rbac_final_roles_count",
            "rbac_can_read_audit_all",
            "rbac_can_write_risk",
        ):
            self.assertEqual(postgres_summary[key], sqlite_summary[key], msg=f"mismatch on {key}")
        self.assertEqual(postgres_summary["backend"], "postgres")
        self.assertTrue(postgres_summary["session_username"].endswith("_guest"))
        self.assertTrue(postgres_summary["session_revoked_at"])


if __name__ == "__main__":
    unittest.main()
