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


class ApiBacktestDetailPersistenceTests(unittest.TestCase):
    def setUp(self):
        self._tmp_db_dir = tempfile.TemporaryDirectory()
        db_path = Path(self._tmp_db_dir.name) / "quant_api.db"
        self._store = SQLiteStore(db_path)
        self._store.initialize()

        self._orig_db = api_server._DB
        self._orig_db_ready = api_server._DB_READY
        self._orig_db_enabled = api_server._DB_ENABLED
        self._orig_db_init_error = api_server._DB_INIT_ERROR
        self._orig_backtest_store = deepcopy(api_server._BACKTEST_STORE)
        self._orig_get_backtest_runner = api_server._get_backtest_runner

        api_server._DB = self._store
        api_server._DB_READY = True
        api_server._DB_ENABLED = True
        api_server._DB_INIT_ERROR = ""
        api_server._BACKTEST_STORE = {}
        api_server._get_backtest_runner = lambda create=False: None

    def tearDown(self):
        api_server._DB = self._orig_db
        api_server._DB_READY = self._orig_db_ready
        api_server._DB_ENABLED = self._orig_db_enabled
        api_server._DB_INIT_ERROR = self._orig_db_init_error
        api_server._BACKTEST_STORE = self._orig_backtest_store
        api_server._get_backtest_runner = self._orig_get_backtest_runner
        self._tmp_db_dir.cleanup()

    def test_get_backtest_reads_csv_and_persists_detail_tables(self):
        with tempfile.TemporaryDirectory(dir=str(api_server.LOG_DIR)) as tmp_dir:
            tmp_path = Path(tmp_dir)
            equity_csv = tmp_path / "equity.csv"
            trades_csv = tmp_path / "trades.csv"
            equity_csv.write_text(
                "\n".join(
                    [
                        "ts_utc,equity",
                        "2026-03-05T00:00:00+00:00,1000",
                        "2026-03-05T00:05:00+00:00,1010",
                    ]
                ),
                encoding="utf-8",
            )
            trades_csv.write_text(
                "\n".join(
                    [
                        "ts_exec_utc,symbol,side,amount,price,fee,order_id",
                        "2026-03-05T00:05:00+00:00,BTC/USDT:USDT,buy,1,100,0.1,t1",
                    ]
                ),
                encoding="utf-8",
            )

            run_id = "run_csv_1"
            api_server._BACKTEST_STORE[run_id] = {
                "id": run_id,
                "owner": "guest",
                "strategyId": "strategy_1",
                "strategyName": "strategy_1",
                "symbol": "BTC/USDT:USDT",
                "startAt": "2026-03-01T00:00:00+00:00",
                "endAt": "2026-03-05T00:10:00+00:00",
                "initialCapital": 1000.0,
                "feeRate": 0.0,
                "slippage": 0.0,
                "status": "success",
                "createdAt": "2026-03-05T00:00:00+00:00",
                "updatedAt": "2026-03-05T00:10:00+00:00",
                "artifacts": {
                    "equity_csv": str(equity_csv),
                    "trades_csv": str(trades_csv),
                },
            }

            with api_server._auth_user_context("guest"):
                payload = api_server.get_backtest(run_id, request=_fake_request("guest"))
            self.assertEqual(len(payload.get("equityCurve") or []), 2)
            self.assertEqual(len(payload.get("trades") or []), 1)
            self.assertEqual(str((payload.get("trades") or [])[0].get("id")), "t1")

            trade_rows = self._store.list_backtest_trades(run_id=run_id, owner="guest", limit=50)
            equity_rows = self._store.list_backtest_equity_points(run_id=run_id, owner="guest", limit=50)
            self.assertEqual(len(trade_rows), 1)
            self.assertEqual(len(equity_rows), 2)
            self.assertAlmostEqual(float(equity_rows[-1].get("equity") or 0.0), 1010.0, places=6)

    def test_get_backtest_prefers_db_when_csv_missing(self):
        run_id = "run_db_1"
        self._store.replace_backtest_equity_points(
            run_id=run_id,
            owner="guest",
            rows=[
                {
                    "ts": "2026-03-05T00:00:00+00:00",
                    "equity": 1000.0,
                    "pnl": 0.0,
                    "dd": 0.0,
                },
                {
                    "ts": "2026-03-05T00:05:00+00:00",
                    "equity": 980.0,
                    "pnl": -20.0,
                    "dd": 0.02,
                },
            ],
        )
        self._store.replace_backtest_trades(
            run_id=run_id,
            owner="guest",
            rows=[
                {
                    "id": "trade_1",
                    "ts": "2026-03-05T00:05:00+00:00",
                    "symbol": "BTC/USDT:USDT",
                    "side": "sell",
                    "qty": 1.0,
                    "price": 98.0,
                    "fee": 0.1,
                    "pnl": -20.0,
                    "orderId": "ord_1",
                }
            ],
        )
        api_server._BACKTEST_STORE[run_id] = {
            "id": run_id,
            "owner": "guest",
            "strategyId": "strategy_1",
            "strategyName": "strategy_1",
            "symbol": "BTC/USDT:USDT",
            "startAt": "2026-03-01T00:00:00+00:00",
            "endAt": "2026-03-05T00:10:00+00:00",
            "initialCapital": 1000.0,
            "feeRate": 0.0,
            "slippage": 0.0,
            "status": "success",
            "createdAt": "2026-03-05T00:00:00+00:00",
            "updatedAt": "2026-03-05T00:10:00+00:00",
            "artifacts": {
                "equity_csv": "logs/not_found_equity.csv",
                "trades_csv": "logs/not_found_trades.csv",
            },
        }

        with api_server._auth_user_context("guest"):
            payload = api_server.get_backtest(run_id, request=_fake_request("guest"))
        self.assertEqual(len(payload.get("equityCurve") or []), 2)
        self.assertEqual(len(payload.get("trades") or []), 1)
        self.assertAlmostEqual(float((payload.get("metrics") or {}).get("maxDrawdown") or 0.0), 0.02, places=6)
        self.assertEqual(str((payload.get("trades") or [])[0].get("id")), "trade_1")


if __name__ == "__main__":
    unittest.main()
