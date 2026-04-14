import tempfile
import types
import unittest
from copy import deepcopy
from pathlib import Path

import api_server
from db_store import SQLiteStore


def _fake_request(username: str):
    state = types.SimpleNamespace(auth_username=username)
    return types.SimpleNamespace(state=state)


class StrategyDeleteAndReadAuditTests(unittest.TestCase):
    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory()
        db_path = Path(self._tmp_dir.name) / "quant_api.db"
        self._store = SQLiteStore(db_path)
        self._store.initialize()

        self._orig_db = api_server._DB
        self._orig_db_ready = api_server._DB_READY
        self._orig_db_enabled = api_server._DB_ENABLED
        self._orig_db_init_error = api_server._DB_INIT_ERROR
        self._orig_strategy_store = deepcopy(api_server._STRATEGY_STORE)
        self._orig_risk_store = deepcopy(api_server._RISK_STATE_STORE)
        self._orig_get_strategy_runner = api_server._get_strategy_runner
        self._orig_terminate_external = api_server._terminate_external_strategy_processes

        api_server._DB = self._store
        api_server._DB_READY = True
        api_server._DB_ENABLED = True
        api_server._DB_INIT_ERROR = ""
        api_server._STRATEGY_STORE = {}
        api_server._RISK_STATE_STORE = {}
        api_server._get_strategy_runner = lambda strategy_id, create=False: None
        api_server._terminate_external_strategy_processes = lambda strategy_id=None, username=None: {}

        with api_server._auth_user_context("alice"):
            strategy_id = "strategy_delete_1"
            strategy_key = api_server._strategy_store_key(strategy_id)
            strategy = {
                "id": strategy_id,
                "name": "to delete",
                "type": "custom",
                "status": "stopped",
                "config": {"symbols": ["BTC/USDT:USDT"], "timeframe": "1h", "params": {}},
                "createdAt": "2026-01-01T00:00:00+00:00",
                "updatedAt": "2026-01-01T00:00:00+00:00",
                "owner": "alice",
            }
            api_server._STRATEGY_STORE[strategy_key] = strategy
            self._store.upsert_strategy(strategy_key, "alice", strategy)

            risk_key = api_server._scoped_strategy_id(strategy_id)
            risk_state = {
                "enabled": True,
                "maxDrawdownPct": 0.2,
                "updatedAt": "2026-01-01T00:00:00+00:00",
            }
            api_server._RISK_STATE_STORE[risk_key] = deepcopy(risk_state)
            self._store.upsert_risk_state("alice", risk_key, risk_state)

    def tearDown(self):
        api_server._DB = self._orig_db
        api_server._DB_READY = self._orig_db_ready
        api_server._DB_ENABLED = self._orig_db_enabled
        api_server._DB_INIT_ERROR = self._orig_db_init_error
        api_server._STRATEGY_STORE = self._orig_strategy_store
        api_server._RISK_STATE_STORE = self._orig_risk_store
        api_server._get_strategy_runner = self._orig_get_strategy_runner
        api_server._terminate_external_strategy_processes = self._orig_terminate_external
        self._tmp_dir.cleanup()

    def test_delete_strategy_removes_db_and_memory_records(self):
        response = api_server.delete_strategy("strategy_delete_1", request=_fake_request("alice"))
        self.assertTrue(bool(response.get("deleted")))
        self.assertEqual(response.get("strategy_id"), "strategy_delete_1")

        with api_server._auth_user_context("alice"):
            self.assertIsNone(api_server._strategy_store_get("strategy_delete_1"))
            risk_key = api_server._scoped_strategy_id("strategy_delete_1")
            self.assertIsNone(api_server._RISK_STATE_STORE.get(risk_key))

        with self._store._connect() as conn:
            strategy_cnt = int(
                conn.execute("SELECT COUNT(1) FROM strategies WHERE strategy_key LIKE ?", ("%strategy_delete_1",)).fetchone()[0]
            )
            risk_cnt = int(
                conn.execute("SELECT COUNT(1) FROM risk_states WHERE strategy_key LIKE ?", ("%strategy_delete_1",)).fetchone()[0]
            )
        self.assertEqual(strategy_cnt, 0)
        self.assertEqual(risk_cnt, 0)

        rows = self._store.list_audit_logs(owner="alice", action="strategy.delete", entity="strategy", limit=10)
        self.assertGreaterEqual(len(rows), 1)

    def test_get_config_writes_read_audit(self):
        with api_server._auth_user_context("alice"):
            payload = api_server.get_config(config_path="config.yaml")
        self.assertIn("config", payload)

        rows = self._store.list_audit_logs(owner="alice", action="config.read", entity="config", limit=10)
        self.assertGreaterEqual(len(rows), 1)


if __name__ == "__main__":
    unittest.main()
