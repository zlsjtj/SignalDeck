import tempfile
import time
import threading
import types
import unittest
from copy import deepcopy
from collections import deque
from pathlib import Path

import yaml

import api_server
from db_store import SQLiteStore


def _fake_request(username: str):
    state = types.SimpleNamespace(auth_username=username)
    return types.SimpleNamespace(state=state)


class ApiStrategyAutoCompileTests(unittest.TestCase):
    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory(dir=str(api_server.LOG_DIR))
        tmp_root = Path(self._tmp_dir.name)
        self._base_config = tmp_root / "base_config.yaml"
        self._base_config.write_text(
            "\n".join(
                [
                    "symbols: ['BTC/USDT:USDT']",
                    "timeframe: '1h'",
                    "paper_equity_usdt: 1000",
                    "portfolio:",
                    "  fee_bps: 10",
                    "  slippage_bps: 5",
                    "strategy:",
                    "  mode: custom",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        self._db_path = tmp_root / "quant_api.db"
        self._store = SQLiteStore(self._db_path)
        self._store.initialize()

        self._orig_db = api_server._DB
        self._orig_db_ready = api_server._DB_READY
        self._orig_db_enabled = api_server._DB_ENABLED
        self._orig_db_init_error = api_server._DB_INIT_ERROR
        self._orig_default_strategy_config = api_server._DEFAULT_CONFIG_PATH
        self._orig_strategy_store = deepcopy(api_server._STRATEGY_STORE)
        self._orig_backtest_store = deepcopy(api_server._BACKTEST_STORE)
        self._orig_risk_store = deepcopy(api_server._RISK_STATE_STORE)
        self._orig_compile_queue = deepcopy(api_server._STRATEGY_COMPILE_QUEUE)
        self._orig_compile_worker = api_server._STRATEGY_COMPILE_WORKER
        self._orig_compile_stop = api_server._STRATEGY_COMPILE_STOP
        self._orig_compile_event = api_server._STRATEGY_COMPILE_EVENT
        self._orig_strategy_runners = deepcopy(api_server._STRATEGY_RUNNERS)
        self._orig_start_impl = api_server._start_strategy_impl
        self._orig_ensure_default_strategy = api_server._ensure_default_strategy

        api_server._DB = self._store
        api_server._DB_READY = True
        api_server._DB_ENABLED = True
        api_server._DB_INIT_ERROR = ""
        api_server._DEFAULT_CONFIG_PATH = str(self._base_config)
        api_server._STRATEGY_STORE = {}
        api_server._BACKTEST_STORE = {}
        api_server._RISK_STATE_STORE = {}
        api_server._STRATEGY_RUNNERS = {}
        api_server._STRATEGY_COMPILE_QUEUE = deque()
        api_server._STRATEGY_COMPILE_EVENT = threading.Event()
        api_server._STRATEGY_COMPILE_STOP = threading.Event()
        api_server._STRATEGY_COMPILE_WORKER = None

    def tearDown(self):
        api_server._STRATEGY_COMPILE_STOP.set()
        api_server._STRATEGY_COMPILE_EVENT.set()
        worker = api_server._STRATEGY_COMPILE_WORKER
        if worker is not None and worker.is_alive():
            worker.join(timeout=2.0)

        api_server._DB = self._orig_db
        api_server._DB_READY = self._orig_db_ready
        api_server._DB_ENABLED = self._orig_db_enabled
        api_server._DB_INIT_ERROR = self._orig_db_init_error
        api_server._DEFAULT_CONFIG_PATH = self._orig_default_strategy_config
        api_server._STRATEGY_STORE = self._orig_strategy_store
        api_server._BACKTEST_STORE = self._orig_backtest_store
        api_server._RISK_STATE_STORE = self._orig_risk_store
        api_server._STRATEGY_COMPILE_QUEUE = self._orig_compile_queue
        api_server._STRATEGY_COMPILE_WORKER = self._orig_compile_worker
        api_server._STRATEGY_COMPILE_STOP = self._orig_compile_stop
        api_server._STRATEGY_COMPILE_EVENT = self._orig_compile_event
        api_server._STRATEGY_RUNNERS = self._orig_strategy_runners
        api_server._start_strategy_impl = self._orig_start_impl
        api_server._ensure_default_strategy = self._orig_ensure_default_strategy
        self._tmp_dir.cleanup()

    def _wait_scripts(self, strategy_id: str, username: str, at_least: int, timeout_seconds: float = 5.0):
        deadline = time.time() + timeout_seconds
        rows = []
        while time.time() < deadline:
            rows = api_server.strategy_scripts(strategy_id, request=_fake_request(username), limit=20)
            if len(rows) >= at_least:
                return rows
            time.sleep(0.05)
        return rows

    def test_create_strategy_auto_compile_and_start_uses_compiled_script(self):
        with api_server._auth_user_context("guest"):
            created = api_server.create_strategy(
                payload=api_server.StrategyCreateRequest(
                    name="guest alpha",
                    type="custom",
                    config={
                        "symbols": ["BTC/USDT:USDT"],
                        "timeframe": "15m",
                        "params": {
                            "portfolio.fee_bps": 8,
                            "portfolio.slippage_bps": 2,
                            "strategy.long_quantile": 0.8,
                        },
                    },
                ),
                request=_fake_request("guest"),
            )

        strategy_id = str(created.get("id"))
        self.assertTrue(strategy_id)
        scripts = self._wait_scripts(strategy_id, "guest", at_least=1)
        self.assertGreaterEqual(len(scripts), 1)

        latest = scripts[0]
        script_path = Path(str(latest.get("scriptPath") or ""))
        self.assertTrue(script_path.exists())
        compiled_raw = yaml.safe_load(script_path.read_text(encoding="utf-8")) or {}
        self.assertEqual(compiled_raw.get("timeframe"), "15m")
        self.assertEqual((compiled_raw.get("symbols") or [None])[0], "BTC/USDT:USDT")
        self.assertEqual(
            (((compiled_raw.get("portfolio") or {}).get("fee_bps"))),
            8,
        )
        self.assertEqual(
            (((compiled_raw.get("strategy") or {}).get("long_quantile"))),
            0.8,
        )

        with api_server._auth_user_context("guest"):
            enqueue_resp = api_server.compile_strategy(strategy_id, request=_fake_request("guest"))
        self.assertTrue(bool(enqueue_resp.get("ok")))
        scripts_after_recompile = self._wait_scripts(strategy_id, "guest", at_least=2)
        self.assertGreaterEqual(len(scripts_after_recompile), 2)

        captured = {}

        def _fake_start(payload):
            captured["config_path"] = payload.config_path
            captured["strategy_id"] = payload.strategy_id
            return {"ok": True, "status": {"running": False}, "strategy_id": payload.strategy_id}

        api_server._start_strategy_impl = _fake_start
        api_server._ensure_default_strategy = lambda: {"id": "quant-default"}
        api_server._db_replace_user_roles("guest", ["trader"])

        with api_server._auth_user_context("guest"):
            start_resp = api_server.start_strategy_compat(strategy_id, request=_fake_request("guest"))
        self.assertTrue(bool(start_resp.get("ok")))
        self.assertEqual(str(captured.get("strategy_id")), strategy_id)
        self.assertEqual(
            str(captured.get("config_path")),
            str(scripts_after_recompile[0].get("scriptPath")),
        )

    def test_recover_pending_compile_jobs_requeues_without_duplication(self):
        strategy_id = "recovery_strategy"
        strategy_key = "usr__guest__recovery_strategy"
        strategy_record = {
            "id": strategy_id,
            "name": "guest recovery",
            "type": "custom",
            "status": "stopped",
            "createdAt": "2026-03-05T00:00:00+00:00",
            "updatedAt": "2026-03-05T00:00:00+00:00",
            "owner": "guest",
            "config": {
                "symbols": ["BTC/USDT:USDT"],
                "timeframe": "15m",
                "params": {
                    "portfolio.fee_bps": 8,
                    "portfolio.slippage_bps": 2,
                },
            },
            "_source_config_path": str(self._base_config),
        }
        api_server._STRATEGY_STORE[strategy_key] = deepcopy(strategy_record)
        self._store.upsert_strategy(strategy_key, "guest", deepcopy(strategy_record))

        job = self._store.enqueue_strategy_compile_job(strategy_key, "guest")
        job_id = int(job.get("id") or 0)
        self.assertGreater(job_id, 0)
        self._store.update_strategy_compile_job(
            job_id,
            status="running",
            error_message="",
            started_at="2026-03-05T00:00:10+00:00",
        )

        orig_ensure_worker = api_server._ensure_strategy_compile_worker
        api_server._ensure_strategy_compile_worker = lambda: None
        try:
            queued_first = api_server._recover_pending_strategy_compile_jobs(limit=20)
            queued_second = api_server._recover_pending_strategy_compile_jobs(limit=20)
        finally:
            api_server._ensure_strategy_compile_worker = orig_ensure_worker

        self.assertEqual(queued_first, 1)
        self.assertEqual(queued_second, 0)
        self.assertEqual(len(api_server._STRATEGY_COMPILE_QUEUE), 1)
        queued_payload = api_server._STRATEGY_COMPILE_QUEUE[0]
        self.assertEqual(int(queued_payload.get("job_id") or 0), job_id)
        self.assertEqual(str(queued_payload.get("strategy_key") or ""), strategy_key)
        self.assertEqual(str(queued_payload.get("owner") or ""), "guest")

        job_rows = self._store.list_strategy_compile_jobs(owner="guest", strategy_key=strategy_key, limit=5)
        self.assertEqual(len(job_rows), 1)
        self.assertEqual(str(job_rows[0].get("status")), "pending")
        self.assertEqual(str(job_rows[0].get("startedAt") or ""), "")
        self.assertEqual(str(job_rows[0].get("finishedAt") or ""), "")

        result = api_server._run_strategy_compile_job(strategy_key, "guest", job_id=job_id)
        self.assertTrue(bool(result.get("ok")))
        script_rows = self._store.list_strategy_scripts(owner="guest", strategy_key=strategy_key, limit=5)
        self.assertGreaterEqual(len(script_rows), 1)

        refreshed_job_rows = self._store.list_strategy_compile_jobs(owner="guest", strategy_key=strategy_key, limit=5)
        self.assertEqual(str(refreshed_job_rows[0].get("status")), "success")


if __name__ == "__main__":
    unittest.main()
