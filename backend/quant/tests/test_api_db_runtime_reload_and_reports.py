import os
import tempfile
import types
import unittest
from copy import deepcopy
from pathlib import Path

from fastapi import HTTPException

import api_server
from db_store import SQLiteStore
from postgres_store import PostgresStore


def _fake_request(username: str):
    state = types.SimpleNamespace(auth_username=username)
    return types.SimpleNamespace(state=state)


class ApiDbRuntimeReloadAndReportsTests(unittest.TestCase):
    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory()
        self._db_path = Path(self._tmp_dir.name) / "quant_api.db"
        self._store = SQLiteStore(self._db_path)
        self._store.initialize()

        self._orig_db = api_server._DB
        self._orig_db_ready = api_server._DB_READY
        self._orig_db_enabled = api_server._DB_ENABLED
        self._orig_db_init_error = api_server._DB_INIT_ERROR
        self._orig_db_path = api_server._DB_PATH
        self._orig_db_path_text = api_server._DB_PATH_TEXT
        self._orig_db_backend = api_server._DB_BACKEND
        self._orig_db_postgres_dsn = api_server._DB_POSTGRES_DSN
        self._orig_build_db_store = api_server._build_db_store
        self._orig_strategy_store = deepcopy(api_server._STRATEGY_STORE)
        self._orig_backtest_store = deepcopy(api_server._BACKTEST_STORE)
        self._orig_risk_state_store = deepcopy(api_server._RISK_STATE_STORE)
        self._orig_db_health_cache = deepcopy(api_server._DB_HEALTH_STATS_CACHE)
        self._orig_db_runtime_stats = deepcopy(api_server._DB_RUNTIME_STATS)
        self._orig_db_runtime_alert_state = deepcopy(api_server._DB_RUNTIME_ALERT_STATE)

        api_server._DB = self._store
        api_server._DB_READY = True
        api_server._DB_ENABLED = True
        api_server._DB_INIT_ERROR = ""
        api_server._DB_PATH = self._db_path
        api_server._DB_PATH_TEXT = str(self._db_path)
        api_server._DB_BACKEND = "sqlite"
        api_server._DB_POSTGRES_DSN = ""
        api_server._STRATEGY_STORE = {}
        api_server._BACKTEST_STORE = {}
        api_server._RISK_STATE_STORE = {}
        api_server._DB_HEALTH_STATS_CACHE = {"ts_epoch": 0.0, "stats": {}, "error": ""}
        api_server._DB_RUNTIME_STATS = deepcopy(self._orig_db_runtime_stats)
        api_server._DB_RUNTIME_ALERT_STATE = deepcopy(self._orig_db_runtime_alert_state)

    def tearDown(self):
        api_server._DB = self._orig_db
        api_server._DB_READY = self._orig_db_ready
        api_server._DB_ENABLED = self._orig_db_enabled
        api_server._DB_INIT_ERROR = self._orig_db_init_error
        api_server._DB_PATH = self._orig_db_path
        api_server._DB_PATH_TEXT = self._orig_db_path_text
        api_server._DB_BACKEND = self._orig_db_backend
        api_server._DB_POSTGRES_DSN = self._orig_db_postgres_dsn
        api_server._build_db_store = self._orig_build_db_store
        api_server._STRATEGY_STORE = self._orig_strategy_store
        api_server._BACKTEST_STORE = self._orig_backtest_store
        api_server._RISK_STATE_STORE = self._orig_risk_state_store
        api_server._DB_HEALTH_STATS_CACHE = self._orig_db_health_cache
        api_server._DB_RUNTIME_STATS = self._orig_db_runtime_stats
        api_server._DB_RUNTIME_ALERT_STATE = self._orig_db_runtime_alert_state
        self._tmp_dir.cleanup()

    def test_admin_db_reload_preserves_memory_state(self):
        strategy_key = api_server._scoped_strategy_id("strategy_1", "alice")
        strategy_record = {
            "id": "strategy_1",
            "name": "alpha",
            "status": "running",
            "createdAt": "2026-01-01T00:00:00+00:00",
            "updatedAt": "2026-01-01T00:00:00+00:00",
            "owner": "alice",
            "config": {
                "symbols": ["BTC/USDT:USDT"],
                "timeframe": "1h",
                "params": {"lookback": 20},
            },
        }
        backtest_record = {
            "id": "run_1",
            "owner": "alice",
            "strategyId": "strategy_1",
            "status": "finished",
            "createdAt": "2026-01-01T00:00:00+00:00",
            "updatedAt": "2026-01-01T00:00:00+00:00",
        }
        risk_state = {
            "enabled": True,
            "maxDrawdownPct": 0.2,
            "updatedAt": "2026-01-01T00:00:00+00:00",
            "triggered": [],
        }

        api_server._STRATEGY_STORE[strategy_key] = deepcopy(strategy_record)
        api_server._BACKTEST_STORE["run_1"] = deepcopy(backtest_record)
        api_server._RISK_STATE_STORE[strategy_key] = deepcopy(risk_state)

        reloaded_db = (Path(self._tmp_dir.name) / "reloaded.db").resolve()
        result = api_server.admin_db_reload(
            payload=api_server.DbReloadRequest(enabled=True, dbPath=str(reloaded_db), preserveState=True),
            request=_fake_request("admin"),
        )

        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(str(reloaded_db), str(result.get("current", {}).get("db_path")))
        self.assertEqual(str(reloaded_db), str(api_server._DB_PATH))

        loaded_strategies = api_server._DB.load_strategies()
        loaded_backtests = api_server._DB.load_backtests()
        loaded_risks = api_server._DB.load_risk_states()
        self.assertEqual(len(loaded_strategies), 1)
        self.assertEqual(len(loaded_backtests), 1)
        self.assertEqual(len(loaded_risks), 1)

    def test_admin_db_config_and_disable_reload_rejected(self):
        config = api_server.admin_db_config(request=_fake_request("admin"))
        self.assertTrue(bool(config.get("enabled")))
        self.assertTrue(bool(config.get("ready")))
        self.assertEqual(str(self._db_path), str(config.get("db_path")))

        with self.assertRaises(HTTPException) as ctx:
            api_server.admin_db_reload(
                payload=api_server.DbReloadRequest(enabled=False, preserveState=False),
                request=_fake_request("admin"),
            )
        self.assertEqual(getattr(ctx.exception, "status_code", None), 500)
        self.assertIn("database disable is not allowed", str(getattr(ctx.exception, "detail", "")))
        self.assertTrue(api_server._DB_ENABLED)
        self.assertTrue(api_server._DB_READY)

        health = api_server.health()
        self.assertEqual(str(health.get("db")), "degraded")

    def test_non_admin_db_admin_endpoints_forbidden(self):
        with self.assertRaises(HTTPException) as cfg_ctx:
            api_server.admin_db_config(request=_fake_request("alice"))
        self.assertEqual(getattr(cfg_ctx.exception, "status_code", None), 403)

        with self.assertRaises(HTTPException) as reload_ctx:
            api_server.admin_db_reload(
                payload=api_server.DbReloadRequest(enabled=True, preserveState=False),
                request=_fake_request("alice"),
            )
        self.assertEqual(getattr(reload_ctx.exception, "status_code", None), 403)

    def test_admin_db_reload_can_switch_backend_to_postgres(self):
        postgres_dsn = "postgresql://alice:secret@127.0.0.1:5432/quant"
        fake_postgres_db = (Path(self._tmp_dir.name) / "fake_postgres_backed.db").resolve()

        def _fake_build_db_store(*, backend: str, db_path_text: str, postgres_dsn: str):
            self.assertEqual(str(backend), "postgres")
            self.assertTrue(bool(postgres_dsn))
            store = SQLiteStore(fake_postgres_db)
            return store, "postgres", fake_postgres_db, postgres_dsn

        api_server._build_db_store = _fake_build_db_store

        result = api_server.admin_db_reload(
            payload=api_server.DbReloadRequest(
                enabled=True,
                backend="postgres",
                postgresDsn=postgres_dsn,
                preserveState=False,
            ),
            request=_fake_request("admin"),
        )
        self.assertTrue(bool(result.get("ok")))
        self.assertEqual(str(result.get("current", {}).get("backend")), "postgres")
        self.assertIn("***@", str(result.get("current", {}).get("postgres_dsn")))
        self.assertEqual(str(api_server._DB_BACKEND), "postgres")
        self.assertEqual(str(api_server._DB_POSTGRES_DSN), postgres_dsn)

        health = api_server.health()
        self.assertEqual(str(health.get("db_backend")), "postgres")
        self.assertIn("***@", str(health.get("db_postgres_dsn")))

        metrics_resp = api_server.metrics()
        body = metrics_resp.body.decode("utf-8")
        self.assertIn('quant_db_backend{backend="postgres"} 1', body)

    def test_build_db_store_reads_postgres_pool_env(self):
        keys = [
            "API_DB_POSTGRES_POOL_ENABLED",
            "API_DB_POSTGRES_POOL_MIN_SIZE",
            "API_DB_POSTGRES_POOL_MAX_SIZE",
            "API_DB_POSTGRES_POOL_TIMEOUT_SECONDS",
        ]
        env_backup = {key: os.getenv(key) for key in keys}
        try:
            os.environ["API_DB_POSTGRES_POOL_ENABLED"] = "true"
            os.environ["API_DB_POSTGRES_POOL_MIN_SIZE"] = "2"
            os.environ["API_DB_POSTGRES_POOL_MAX_SIZE"] = "7"
            os.environ["API_DB_POSTGRES_POOL_TIMEOUT_SECONDS"] = "4.5"
            store, backend, _, dsn = api_server._build_db_store(
                backend="postgres",
                db_path_text="logs/quant_api.db",
                postgres_dsn="postgresql://alice:secret@127.0.0.1:5432/quant",
            )
            self.assertIsInstance(store, PostgresStore)
            self.assertEqual(str(backend), "postgres")
            self.assertEqual(str(dsn), "postgresql://alice:secret@127.0.0.1:5432/quant")
            self.assertTrue(bool(store.pool_enabled))
            self.assertEqual(int(store.pool_min_size), 2)
            self.assertEqual(int(store.pool_max_size), 7)
            self.assertAlmostEqual(float(store.pool_timeout_seconds), 4.5, places=6)
        finally:
            for key, value in env_backup.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

    def test_admin_db_config_includes_postgres_pool_fields(self):
        api_server._DB_BACKEND = "postgres"
        api_server._DB_POSTGRES_DSN = "postgresql://alice:secret@127.0.0.1:5432/quant"
        api_server._DB = types.SimpleNamespace(
            pool_enabled=True,
            pool_supported=True,
            pool_min_size=2,
            pool_max_size=8,
            pool_timeout_seconds=4.0,
            _pool=object(),
        )
        config = api_server.admin_db_config(request=_fake_request("admin"))
        pool = config.get("postgres_pool") or {}
        self.assertTrue(bool(pool.get("enabled")))
        self.assertTrue(bool(pool.get("supported")))
        self.assertTrue(bool(pool.get("active")))
        self.assertEqual(int(pool.get("min_size", 0)), 2)
        self.assertEqual(int(pool.get("max_size", 0)), 8)
        self.assertAlmostEqual(float(pool.get("timeout_seconds", 0.0)), 4.0, places=6)

    def test_reload_disable_rejected_without_closing_previous_db_repository(self):
        class _ClosableRepo:
            backend = "sqlite"

            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        old_repo = _ClosableRepo()
        api_server._DB = old_repo
        api_server._DB_ENABLED = True
        api_server._DB_READY = True
        api_server._DB_INIT_ERROR = ""
        api_server._DB_BACKEND = "sqlite"
        with self.assertRaises(RuntimeError):
            api_server._reload_db_runtime(enabled=False, preserve_state=False)
        self.assertFalse(bool(old_repo.closed))
        self.assertIs(api_server._DB, old_repo)

    def test_market_helpers_persist_timeseries_rows(self):
        api_server._persist_market_ticks(
            "config_market.yaml",
            [
                {
                    "symbol": "BTC/USDT:USDT",
                    "ts_utc": "2026-01-01T00:00:00+00:00",
                    "price": 100.0,
                    "bid": 99.5,
                    "ask": 100.5,
                    "volume": 10.0,
                }
            ],
        )
        api_server._persist_market_ticks(
            "config_market.yaml",
            [
                {
                    "symbol": "BTC/USDT:USDT",
                    "ts_utc": "2026-01-01T00:00:00+00:00",
                    "price": 101.0,
                    "bid": 100.5,
                    "ask": 101.5,
                    "volume": 11.0,
                }
            ],
        )
        api_server._persist_market_klines(
            "config_market.yaml",
            "15m",
            [
                {
                    "symbol": "BTC/USDT:USDT",
                    "ts_utc": "2026-01-01T00:15:00+00:00",
                    "time": 1767226500,
                    "open": 100.0,
                    "high": 102.0,
                    "low": 99.0,
                    "close": 101.0,
                    "volume": 55.0,
                }
            ],
        )

        summary = self._store.build_db_report_summary()
        market = summary.get("marketTimeseries") or {}
        self.assertEqual(int(market.get("ticks", 0)), 1)
        self.assertEqual(int(market.get("klines", 0)), 1)

    def test_audit_verify_report_and_risk_history_owner_scope(self):
        self._store.append_audit_log(
            owner="alice",
            action="strategy.create",
            entity="strategy",
            entity_id="a1",
            detail={},
        )
        self._store.append_audit_log(
            owner="bob",
            action="strategy.create",
            entity="strategy",
            entity_id="b1",
            detail={},
        )

        strategy_alice = api_server._scoped_strategy_id("strategy_a", "alice")
        strategy_bob = api_server._scoped_strategy_id("strategy_b", "bob")
        self._store.upsert_risk_state(
            "alice",
            strategy_alice,
            {"enabled": True, "maxDrawdownPct": 0.2, "updatedAt": "2026-01-01T00:00:00+00:00", "triggered": []},
        )
        self._store.upsert_risk_state(
            "alice",
            strategy_alice,
            {"enabled": True, "maxDrawdownPct": 0.3, "updatedAt": "2026-01-01T00:01:00+00:00", "triggered": []},
        )
        self._store.upsert_risk_state(
            "bob",
            strategy_bob,
            {"enabled": True, "maxDrawdownPct": 0.1, "updatedAt": "2026-01-01T00:00:00+00:00", "triggered": []},
        )
        self._store.append_risk_event(
            owner="alice",
            strategy_key=strategy_alice,
            event_type="triggered",
            rule="max_drawdown",
            message="alice-triggered",
            detail={},
        )
        self._store.append_risk_event(
            owner="bob",
            strategy_key=strategy_bob,
            event_type="triggered",
            rule="max_drawdown",
            message="bob-triggered",
            detail={},
        )

        verify_alice = api_server.audit_verify(
            request=_fake_request("alice"),
            owner="bob",
            start_id=None,
            end_id=None,
            limit=5000,
        )
        verify_admin_bob = api_server.audit_verify(
            request=_fake_request("admin"),
            owner="bob",
            start_id=None,
            end_id=None,
            limit=5000,
        )
        self.assertTrue(bool(verify_alice.get("ok")))
        self.assertTrue(bool(verify_admin_bob.get("ok")))
        self.assertEqual(int(verify_alice.get("checked", 0)), 1)
        self.assertEqual(int(verify_admin_bob.get("checked", 0)), 1)

        summary_alice = api_server.db_report_summary(
            request=_fake_request("alice"),
            owner="bob",
            start=None,
            end=None,
            limit_top=10,
        )
        summary_admin_bob = api_server.db_report_summary(
            request=_fake_request("admin"),
            owner="bob",
            start=None,
            end=None,
            limit_top=10,
        )
        self.assertEqual(int(summary_alice.get("auditTotal", 0)), 1)
        self.assertEqual(int(summary_alice.get("riskEventTotal", 0)), 1)
        self.assertEqual(int(summary_alice.get("riskStateHistoryTotal", 0)), 2)
        self.assertEqual(int(summary_admin_bob.get("auditTotal", 0)), 1)
        self.assertEqual(int(summary_admin_bob.get("riskEventTotal", 0)), 1)
        self.assertEqual(int(summary_admin_bob.get("riskStateHistoryTotal", 0)), 1)

        history_alice = api_server.risk_history(
            request=_fake_request("alice"),
            strategy_id=None,
            owner="bob",
            cursor=None,
            limit=50,
        )
        history_admin_bob = api_server.risk_history(
            request=_fake_request("admin"),
            strategy_id=None,
            owner="bob",
            cursor=None,
            limit=50,
        )
        self.assertTrue(bool(history_alice))
        self.assertTrue(bool(history_admin_bob))
        self.assertTrue(all(str(row.get("owner")) == "alice" for row in history_alice))
        self.assertTrue(all(str(row.get("owner")) == "bob" for row in history_admin_bob))


if __name__ == "__main__":
    unittest.main()
