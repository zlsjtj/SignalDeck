import tempfile
import types
import unittest
from copy import deepcopy
from pathlib import Path

import api_server


def _fake_request(username: str):
    state = types.SimpleNamespace(auth_username=username)
    return types.SimpleNamespace(state=state)


class _FakeConfig:
    def __init__(self) -> None:
        self.symbols = ["BTC/USDT:USDT"]
        self.raw = {
            "paper_equity_usdt": 1000.0,
            "portfolio": {"fee_bps": 10.0, "slippage_bps": 5.0},
        }


class ApiIdempotencyTests(unittest.TestCase):
    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory()
        self._config_path = Path(self._tmp_dir.name) / "config.yaml"
        self._config_path.write_text("symbols: ['BTC/USDT:USDT']\n", encoding="utf-8")

        self._orig_values = {
            "_config_path_for_strategy_id": api_server._config_path_for_strategy_id,
            "_resolve_config_path": api_server._resolve_config_path,
            "_create_backtest_override_config": api_server._create_backtest_override_config,
            "_start_backtest_impl": api_server._start_backtest_impl,
            "_ensure_default_strategy": api_server._ensure_default_strategy,
            "_audit_event": api_server._audit_event,
            "_persist_backtest_record": api_server._persist_backtest_record,
            "_get_backtest_runner": api_server._get_backtest_runner,
            "load_config": api_server.load_config,
            "_BACKTEST_STORE": deepcopy(api_server._BACKTEST_STORE),
            "_BACKTEST_CREATE_RECENT": deepcopy(api_server._BACKTEST_CREATE_RECENT),
        }

        api_server._BACKTEST_STORE = {}
        api_server._BACKTEST_CREATE_RECENT = {}
        api_server._config_path_for_strategy_id = lambda strategy_id: str(self._config_path)
        api_server._resolve_config_path = lambda path: Path(path)
        api_server._create_backtest_override_config = (
            lambda config_path, initial_capital, fee_rate, slippage: Path(config_path)
        )
        api_server._ensure_default_strategy = lambda: {
            "id": "quant-default",
            "name": "default",
            "config": {"symbols": ["BTC/USDT:USDT"]},
        }
        api_server._audit_event = lambda *args, **kwargs: None
        api_server._persist_backtest_record = lambda *args, **kwargs: None
        api_server.load_config = lambda path: _FakeConfig()

    def tearDown(self):
        for key, value in self._orig_values.items():
            setattr(api_server, key, value)
        self._tmp_dir.cleanup()

    def test_create_backtest_duplicate_payload_returns_existing_record(self):
        start_calls = {"count": 0}

        def _fake_start_backtest_impl(payload, request_fingerprint=None):
            start_calls["count"] += 1
            return {"run_id": "run_001", "running": True, "return_code": None}

        api_server._start_backtest_impl = _fake_start_backtest_impl

        payload = api_server.BacktestCreateRequest(
            strategyId="strategy_demo",
            symbol="BTC/USDT:USDT",
            startAt="2025-01-01",
            endAt="2025-01-31",
            initialCapital=1000.0,
            feeRate=0.001,
            slippage=0.0005,
        )

        first = api_server.create_backtest(payload=payload, request=_fake_request("alice"))
        second = api_server.create_backtest(payload=payload, request=_fake_request("alice"))

        self.assertEqual(start_calls["count"], 1)
        self.assertEqual(first["id"], "run_001")
        self.assertEqual(second["id"], "run_001")
        self.assertTrue(bool(second.get("idempotent")))
        self.assertEqual(second.get("idempotentKey"), first.get("idempotentKey"))

    def test_start_backtest_returns_running_record_when_same_request_inflight(self):
        class _FakeRunner:
            def __init__(self) -> None:
                self.start_called = False

            def status(self):
                return {"running": True, "return_code": None, "pid": 123, "started_at": "t1", "ended_at": None}

            def start(self, command, cwd, metadata=None):
                self.start_called = True
                raise RuntimeError("should not start when same request is inflight")

        runner = _FakeRunner()
        runner.metadata = lambda: {
            "run_id": "run_inflight",
            "start": "2025-01-01",
            "end": "2025-01-31",
            "config_path": str(self._config_path),
            "artifacts": {"metrics_txt": "logs/run_inflight_metrics.txt"},
            "owner": "alice",
            "request_fingerprint": "fingerprint_1",
        }
        api_server._get_backtest_runner = lambda create=True: runner
        api_server._BACKTEST_STORE["run_inflight"] = {
            "id": "run_inflight",
            "owner": "alice",
            "status": "running",
            "updatedAt": "2025-01-02T00:00:00+00:00",
        }

        with api_server._auth_user_context("alice"):
            response = api_server._start_backtest_impl(
                api_server.BacktestStartRequest(
                    start="2025-01-01",
                    end="2025-01-31",
                    config_path=str(self._config_path),
                ),
                request_fingerprint="fingerprint_1",
            )

        self.assertTrue(bool(response.get("already_running")))
        self.assertEqual(response.get("run_id"), "run_inflight")
        self.assertFalse(bool(runner.start_called))


if __name__ == "__main__":
    unittest.main()
