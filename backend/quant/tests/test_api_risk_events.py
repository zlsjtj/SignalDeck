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


class ApiRiskEventsTests(unittest.TestCase):
    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory()
        db_path = Path(self._tmp_dir.name) / "quant_api.db"
        self._store = SQLiteStore(db_path)
        self._store.initialize()

        self._orig_db = api_server._DB
        self._orig_db_ready = api_server._DB_READY
        self._orig_db_enabled = api_server._DB_ENABLED
        self._orig_db_init_error = api_server._DB_INIT_ERROR
        self._orig_risk_state_store = deepcopy(api_server._RISK_STATE_STORE)
        self._orig_risk_from_config = api_server._risk_from_config
        self._orig_config_path_for_strategy_id = api_server._config_path_for_strategy_id

        api_server._DB = self._store
        api_server._DB_READY = True
        api_server._DB_ENABLED = True
        api_server._DB_INIT_ERROR = ""
        api_server._RISK_STATE_STORE = {}
        api_server._config_path_for_strategy_id = lambda strategy_id: api_server._DEFAULT_CONFIG_PATH

        def _fake_risk_from_config(config_path=api_server._DEFAULT_CONFIG_PATH, strategy_id=None):
            key = api_server._scoped_strategy_id(strategy_id or api_server._DEFAULT_STRATEGY_ID)
            current = api_server._RISK_STATE_STORE.get(key)
            if isinstance(current, dict):
                return deepcopy(current)
            state = {
                "enabled": True,
                "maxDrawdownPct": 0.2,
                "maxPositionPct": 0.5,
                "maxRiskPerTradePct": 0.02,
                "maxLeverage": 1.0,
                "dailyLossLimitPct": 0.1,
                "updatedAt": "2026-01-01T00:00:00+00:00",
                "triggered": [],
            }
            api_server._RISK_STATE_STORE[key] = deepcopy(state)
            return deepcopy(state)

        api_server._risk_from_config = _fake_risk_from_config

    def tearDown(self):
        api_server._DB = self._orig_db
        api_server._DB_READY = self._orig_db_ready
        api_server._DB_ENABLED = self._orig_db_enabled
        api_server._DB_INIT_ERROR = self._orig_db_init_error
        api_server._RISK_STATE_STORE = self._orig_risk_state_store
        api_server._risk_from_config = self._orig_risk_from_config
        api_server._config_path_for_strategy_id = self._orig_config_path_for_strategy_id
        self._tmp_dir.cleanup()

    def test_set_risk_records_manual_update_event(self):
        with api_server._auth_user_context("alice"):
            api_server.set_risk(
                payload=api_server.RiskUpdateRequest(maxDrawdownPct=0.25),
                request=_fake_request("alice"),
                strategy_id="strategy_1",
            )

        rows = self._store.list_risk_events(
            owner="alice",
            strategy_key=api_server._scoped_strategy_id("strategy_1", "alice"),
            event_type="manual_update",
            limit=10,
        )
        self.assertEqual(len(rows), 1)
        changed_fields = rows[0]["detail"].get("changed_fields") or []
        self.assertIn("maxDrawdownPct", changed_fields)

    def test_triggered_and_recovered_events_are_recorded(self):
        with api_server._auth_user_context("alice"):
            api_server.set_risk(
                payload=api_server.RiskUpdateRequest(
                    triggered=[
                        {
                            "rule": "max_drawdown",
                            "ts": "2026-01-01T00:00:00+00:00",
                            "message": "threshold breached",
                        }
                    ]
                ),
                request=_fake_request("alice"),
                strategy_id="strategy_1",
            )
            api_server.set_risk(
                payload=api_server.RiskUpdateRequest(triggered=[]),
                request=_fake_request("alice"),
                strategy_id="strategy_1",
            )

        strategy_key = api_server._scoped_strategy_id("strategy_1", "alice")
        triggered_rows = self._store.list_risk_events(
            owner="alice",
            strategy_key=strategy_key,
            event_type="triggered",
            limit=10,
        )
        recovered_rows = self._store.list_risk_events(
            owner="alice",
            strategy_key=strategy_key,
            event_type="recovered",
            limit=10,
        )
        self.assertEqual(len(triggered_rows), 1)
        self.assertEqual(len(recovered_rows), 1)
        self.assertEqual(triggered_rows[0]["rule"], "max_drawdown")
        self.assertEqual(recovered_rows[0]["rule"], "max_drawdown")

    def test_risk_events_endpoint_enforces_owner_scope(self):
        with api_server._auth_user_context("alice"):
            api_server._append_risk_event(
                api_server._scoped_strategy_id("strategy_a", "alice"),
                "manual_update",
                rule="manual_update",
                message="alice change",
                detail={"by": "alice"},
            )
        with api_server._auth_user_context("bob"):
            api_server._append_risk_event(
                api_server._scoped_strategy_id("strategy_b", "bob"),
                "manual_update",
                rule="manual_update",
                message="bob change",
                detail={"by": "bob"},
            )

        rows_alice = api_server.risk_events(request=_fake_request("alice"), limit=10)
        self.assertEqual(len(rows_alice), 1)
        self.assertEqual(rows_alice[0]["owner"], "alice")

        rows_admin_bob = api_server.risk_events(request=_fake_request("admin"), owner="bob", limit=10)
        self.assertEqual(len(rows_admin_bob), 1)
        self.assertEqual(rows_admin_bob[0]["owner"], "bob")


if __name__ == "__main__":
    unittest.main()
