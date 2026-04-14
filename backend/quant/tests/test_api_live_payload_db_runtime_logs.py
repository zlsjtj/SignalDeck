import tempfile
import types
import unittest
import json
from pathlib import Path

import api_server
from db_store import SQLiteStore


def _fake_request(username: str):
    state = types.SimpleNamespace(auth_username=username)
    return types.SimpleNamespace(state=state)


class ApiLivePayloadDbRuntimeLogsTests(unittest.TestCase):
    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory()
        db_path = Path(self._tmp_dir.name) / "quant_api.db"
        self._store = SQLiteStore(db_path)
        self._store.initialize()

        self._orig_db = api_server._DB
        self._orig_db_ready = api_server._DB_READY
        self._orig_db_enabled = api_server._DB_ENABLED
        self._orig_db_init_error = api_server._DB_INIT_ERROR
        self._orig_latest_tick_prices = api_server._latest_tick_prices
        self._orig_tail_strategy_logs = api_server._tail_strategy_logs
        self._orig_resolve_strategy_diagnostics_path = api_server._resolve_strategy_diagnostics_path

        api_server._DB = self._store
        api_server._DB_READY = True
        api_server._DB_ENABLED = True
        api_server._DB_INIT_ERROR = ""

    def tearDown(self):
        api_server._DB = self._orig_db
        api_server._DB_READY = self._orig_db_ready
        api_server._DB_ENABLED = self._orig_db_enabled
        api_server._DB_INIT_ERROR = self._orig_db_init_error
        api_server._latest_tick_prices = self._orig_latest_tick_prices
        api_server._tail_strategy_logs = self._orig_tail_strategy_logs
        api_server._resolve_strategy_diagnostics_path = self._orig_resolve_strategy_diagnostics_path
        self._tmp_dir.cleanup()

    def _emit_strategy_log(self, owner: str, strategy_id: str, ts_utc: str, message: str, source: str = "stdout") -> None:
        api_server._on_runtime_process_log(
            {
                "name": f"strategy:usr__{owner}__{strategy_id}",
                "source": source,
                "message": message,
                "ts_utc": ts_utc,
                "metadata": {
                    "owner": owner,
                    "strategy_id": strategy_id,
                },
            }
        )

    def test_live_payload_prefers_db_runtime_logs(self):
        strategy_id = "s1"
        with api_server._auth_user_context("guest"):
            self._emit_strategy_log(
                "guest",
                strategy_id,
                "2026-03-05T00:00:00+00:00",
                "[PAPER] buy BTC/USDT:USDT amount=0.5 price=1000 notion=500",
            )
            self._emit_strategy_log(
                "guest",
                strategy_id,
                "2026-03-05T00:00:05+00:00",
                "[PAPER] positions=BTC/USDT:USDT qty=0.5 notion=500",
            )
            self._emit_strategy_log(
                "guest",
                strategy_id,
                "2026-03-05T00:01:00+00:00",
                "[PAPER] sell BTC/USDT:USDT amount=0.2 price=1200 notion=240",
            )
            self._emit_strategy_log(
                "guest",
                strategy_id,
                "2026-03-05T00:01:05+00:00",
                "[PAPER] positions=BTC/USDT:USDT qty=0.3 notion=330",
            )

            api_server._latest_tick_prices = lambda _config_path, refresh_ms=1000: {"BTC/USDT:USDT": 1250.0}

            def _should_not_be_called(_strategy_id, _limit):
                raise AssertionError("tail logs should not be called when db runtime logs are available")

            api_server._tail_strategy_logs = _should_not_be_called

            positions = api_server.positions(request=_fake_request("guest"), strategy_id=strategy_id)
            orders = api_server.orders(request=_fake_request("guest"), strategy_id=strategy_id)
            fills = api_server.fills(request=_fake_request("guest"), strategy_id=strategy_id)

        self.assertEqual(len(positions), 1)
        self.assertEqual(str(positions[0]["symbol"]), "BTC/USDT:USDT")
        self.assertAlmostEqual(float(positions[0]["qty"]), 0.3, places=6)
        self.assertAlmostEqual(float(positions[0]["avgPrice"]), 1100.0, places=6)
        self.assertAlmostEqual(float(positions[0]["lastPrice"]), 1250.0, places=6)
        self.assertAlmostEqual(float(positions[0]["unrealizedPnl"]), 45.0, places=6)

        self.assertEqual(len(orders), 2)
        self.assertEqual(str(orders[0]["side"]), "sell")
        self.assertEqual(str(orders[1]["side"]), "buy")

        self.assertEqual(len(fills), 2)
        self.assertEqual(str(fills[0]["side"]), "sell")
        self.assertEqual(str(fills[1]["side"]), "buy")

    def test_live_payload_falls_back_to_diagnostics_when_logs_empty(self):
        strategy_id = "diag_s1"
        diagnostics_path = Path(self._tmp_dir.name) / f"{strategy_id}.json"
        diagnostics_payload = {
            "generated_at": "2026-03-05T00:03:00+00:00",
            "positions_and_orders": {
                "positions": [
                    {"symbol": "BTC/USDT:USDT", "side": "long", "qty": 0.6, "mark_price": 120.0, "notional": 72.0},
                    {"symbol": "ETH/USDT:USDT", "side": "long", "qty": 2.0, "mark_price": 50.0, "notional": 100.0},
                ],
                "open_orders": [],
            },
            "recent_order_attempts": [
                {
                    "ts": "2026-03-05T00:00:00+00:00",
                    "status": "filled_paper",
                    "symbol": "BTC/USDT:USDT",
                    "side": "buy",
                    "amount": 1.0,
                    "qty": 1.0,
                    "price": 100.0,
                    "notional": 100.0,
                },
                {
                    "ts": "2026-03-05T00:01:00+00:00",
                    "status": "filled_paper",
                    "symbol": "ETH/USDT:USDT",
                    "side": "buy",
                    "amount": 2.0,
                    "qty": 2.0,
                    "price": 50.0,
                    "notional": 100.0,
                },
                {
                    "ts": "2026-03-05T00:02:00+00:00",
                    "status": "filled_paper",
                    "symbol": "BTC/USDT:USDT",
                    "side": "sell",
                    "amount": 0.4,
                    "qty": 0.4,
                    "price": 120.0,
                    "notional": 48.0,
                },
            ],
        }
        diagnostics_path.write_text(json.dumps(diagnostics_payload), encoding="utf-8")

        with api_server._auth_user_context("guest"):
            api_server._tail_strategy_logs = lambda _strategy_id, _limit: []
            api_server._latest_tick_prices = lambda _config_path, refresh_ms=1000: {}
            api_server._resolve_strategy_diagnostics_path = lambda strategy_id=None, path_override=None: diagnostics_path

            positions = api_server.positions(request=_fake_request("guest"), strategy_id=strategy_id)
            orders = api_server.orders(request=_fake_request("guest"), strategy_id=strategy_id)
            fills = api_server.fills(request=_fake_request("guest"), strategy_id=strategy_id)

        self.assertEqual(len(positions), 2)
        self.assertEqual(str(positions[0]["symbol"]), "BTC/USDT:USDT")
        self.assertAlmostEqual(float(positions[0]["qty"]), 0.6, places=6)
        self.assertAlmostEqual(float(positions[0]["avgPrice"]), 100.0, places=6)
        self.assertEqual(str(positions[1]["symbol"]), "ETH/USDT:USDT")
        self.assertAlmostEqual(float(positions[1]["qty"]), 2.0, places=6)

        self.assertEqual(len(orders), 3)
        self.assertEqual(str(orders[0]["side"]), "sell")
        self.assertEqual(str(orders[2]["side"]), "buy")

        self.assertEqual(len(fills), 3)
        self.assertEqual(str(fills[0]["side"]), "sell")
        self.assertEqual(str(fills[2]["side"]), "buy")


if __name__ == "__main__":
    unittest.main()
