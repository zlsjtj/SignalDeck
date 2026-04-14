import tempfile
import types
import unittest
from copy import deepcopy
from pathlib import Path

from fastapi import HTTPException

import api_server
from db_store import SQLiteStore


def _fake_request(username: str):
    state = types.SimpleNamespace(auth_username=username)
    return types.SimpleNamespace(state=state)


class ApiMultiAccountIsolationTests(unittest.TestCase):
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
        self._orig_backtest_store = deepcopy(api_server._BACKTEST_STORE)
        self._orig_risk_state_store = deepcopy(api_server._RISK_STATE_STORE)
        self._orig_enqueue_strategy_compile = api_server._enqueue_strategy_compile

        api_server._DB = self._store
        api_server._DB_READY = True
        api_server._DB_ENABLED = True
        api_server._DB_INIT_ERROR = ""
        api_server._STRATEGY_STORE = {}
        api_server._BACKTEST_STORE = {}
        api_server._RISK_STATE_STORE = {}
        api_server._enqueue_strategy_compile = lambda strategy_key, owner: {
            "id": 0,
            "strategyKey": strategy_key,
            "owner": owner,
            "status": "pending",
        }

        self._users = ["guest", "admin", "lsm_test"]
        self._strategies = {}
        self._backtests = {}
        for username in self._users:
            with api_server._auth_user_context(username):
                created = api_server.create_strategy(
                    payload=api_server.StrategyCreateRequest(
                        name=f"{username}_strategy",
                        type="custom",
                        config={
                            "symbols": ["BTC/USDT:USDT"],
                            "timeframe": "1h",
                            "params": {"strategy.long_quantile": 0.8},
                        },
                    ),
                    request=_fake_request(username),
                )
                strategy_id = str(created.get("id"))
                self._strategies[username] = strategy_id

                run_id = f"run_{username}"
                self._backtests[username] = run_id
                backtest_record = {
                    "id": run_id,
                    "owner": api_server._safe_user_key(username),
                    "strategyId": strategy_id,
                    "strategyName": f"{username}_strategy",
                    "symbol": "BTC/USDT:USDT",
                    "startAt": "2026-01-01T00:00:00+00:00",
                    "endAt": "2026-01-02T00:00:00+00:00",
                    "initialCapital": 1000.0,
                    "feeRate": 0.001,
                    "slippage": 0.0005,
                    "status": "success",
                    "createdAt": "2026-01-01T00:00:00+00:00",
                    "updatedAt": "2026-01-01T00:01:00+00:00",
                }
                api_server._BACKTEST_STORE[run_id] = deepcopy(backtest_record)
                api_server._persist_backtest_record(run_id, backtest_record)

                strategy_key = api_server._scoped_strategy_id(strategy_id, username)
                risk_state = {
                    "enabled": True,
                    "maxDrawdownPct": 0.2,
                    "maxPositionPct": 0.5,
                    "maxRiskPerTradePct": 0.02,
                    "maxLeverage": 1.0,
                    "dailyLossLimitPct": 0.1,
                    "updatedAt": "2026-01-01T00:00:00+00:00",
                    "triggered": [],
                }
                api_server._RISK_STATE_STORE[strategy_key] = deepcopy(risk_state)
                api_server._persist_risk_state(strategy_key, risk_state, owner=username)
                api_server._append_risk_event(
                    strategy_key,
                    "manual_update",
                    rule="manual_update",
                    message=f"{username} manual update",
                    detail={"owner": username},
                    owner=username,
                )

    def tearDown(self):
        api_server._DB = self._orig_db
        api_server._DB_READY = self._orig_db_ready
        api_server._DB_ENABLED = self._orig_db_enabled
        api_server._DB_INIT_ERROR = self._orig_db_init_error
        api_server._STRATEGY_STORE = self._orig_strategy_store
        api_server._BACKTEST_STORE = self._orig_backtest_store
        api_server._RISK_STATE_STORE = self._orig_risk_state_store
        api_server._enqueue_strategy_compile = self._orig_enqueue_strategy_compile
        self._tmp_dir.cleanup()

    def test_strategies_and_backtests_are_isolated_by_owner(self):
        for username in self._users:
            rows = api_server.list_strategies(request=_fake_request(username))
            owner_key = api_server._safe_user_key(username)
            self.assertTrue(rows)
            self.assertTrue(all(str(item.get("owner") or "") == owner_key for item in rows))
            ids = {str(item.get("id") or "") for item in rows}
            self.assertIn(self._strategies[username], ids)

            backtests = api_server.list_backtests(request=_fake_request(username))
            self.assertTrue(backtests)
            self.assertTrue(all(str(item.get("owner") or "") == owner_key for item in backtests))
            run_ids = {str(item.get("id") or "") for item in backtests}
            self.assertIn(self._backtests[username], run_ids)

        for viewer in self._users:
            for owner, strategy_id in self._strategies.items():
                if viewer == owner:
                    continue
                with self.assertRaises(HTTPException) as ctx:
                    api_server.get_strategy(strategy_id, request=_fake_request(viewer))
                self.assertEqual(getattr(ctx.exception, "status_code", None), 404)

            for owner, run_id in self._backtests.items():
                if viewer == owner:
                    continue
                with self.assertRaises(HTTPException) as ctx:
                    api_server.get_backtest(run_id, request=_fake_request(viewer))
                self.assertEqual(getattr(ctx.exception, "status_code", None), 404)

    def test_audit_risk_endpoints_enforce_owner_scope(self):
        for username in ("guest", "lsm_test"):
            owner_key = api_server._safe_user_key(username)
            audit_rows = api_server.audit_logs(
                request=_fake_request(username),
                limit=200,
                action=None,
                entity=None,
                owner="admin",
                start=None,
                end=None,
                cursor=None,
            )
            self.assertTrue(audit_rows)
            self.assertTrue(all(str(row.get("owner") or "") == owner_key for row in audit_rows))

            risk_rows = api_server.risk_events(
                request=_fake_request(username),
                limit=200,
                strategy_id=None,
                event_type=None,
                owner="admin",
                start=None,
                end=None,
                cursor=None,
            )
            self.assertTrue(risk_rows)
            self.assertTrue(all(str(row.get("owner") or "") == owner_key for row in risk_rows))

            risk_history_rows = api_server.risk_history(
                request=_fake_request(username),
                limit=200,
                strategy_id=None,
                owner="admin",
                cursor=None,
            )
            self.assertTrue(risk_history_rows)
            self.assertTrue(all(str(row.get("owner") or "") == owner_key for row in risk_history_rows))

        admin_guest_audit = api_server.audit_logs(
            request=_fake_request("admin"),
            limit=200,
            action=None,
            entity=None,
            owner="guest",
            start=None,
            end=None,
            cursor=None,
        )
        self.assertTrue(admin_guest_audit)
        self.assertTrue(all(str(row.get("owner") or "") == "guest" for row in admin_guest_audit))

        admin_lsm_risk = api_server.risk_events(
            request=_fake_request("admin"),
            limit=200,
            strategy_id=None,
            event_type=None,
            owner="lsm_test",
            start=None,
            end=None,
            cursor=None,
        )
        self.assertTrue(admin_lsm_risk)
        self.assertTrue(all(str(row.get("owner") or "") == "lsm_test" for row in admin_lsm_risk))


if __name__ == "__main__":
    unittest.main()
