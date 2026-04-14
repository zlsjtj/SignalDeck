import time
import unittest
from copy import deepcopy

import api_server


class _FakeRunner:
    def status(self):
        return {
            "running": True,
            "return_code": None,
            "started_at": "2026-01-01T00:00:00+00:00",
            "ended_at": None,
        }

    def metadata(self):
        return {"run_id": "run_sync"}

    def tail_logs(self, limit: int):
        _ = limit
        return [
            {"message": "random line"},
            {"message": "BACKTEST_PROGRESS pct=27 done=27 total=100 ts=2026-01-01T00:00:00+00:00"},
        ]


class BacktestProgressPersistenceTests(unittest.TestCase):
    def setUp(self):
        self._orig_backtest_store = deepcopy(api_server._BACKTEST_STORE)
        self._orig_progress_state = deepcopy(api_server._BACKTEST_PROGRESS_PERSIST_STATE)
        self._orig_progress_interval = api_server._BACKTEST_PROGRESS_PERSIST_INTERVAL_SECONDS
        self._orig_progress_delta = api_server._BACKTEST_PROGRESS_MIN_DELTA_PCT
        self._orig_persist_backtest_record = api_server._persist_backtest_record
        self._orig_get_backtest_runner = api_server._get_backtest_runner

        api_server._BACKTEST_STORE = {}
        api_server._BACKTEST_PROGRESS_PERSIST_STATE = {}
        api_server._BACKTEST_PROGRESS_PERSIST_INTERVAL_SECONDS = 300
        api_server._BACKTEST_PROGRESS_MIN_DELTA_PCT = 1.0
        self._persist_calls = []

        def _fake_persist(run_id, record):
            self._persist_calls.append((str(run_id), deepcopy(record)))

        api_server._persist_backtest_record = _fake_persist

    def tearDown(self):
        api_server._BACKTEST_STORE = self._orig_backtest_store
        api_server._BACKTEST_PROGRESS_PERSIST_STATE = self._orig_progress_state
        api_server._BACKTEST_PROGRESS_PERSIST_INTERVAL_SECONDS = self._orig_progress_interval
        api_server._BACKTEST_PROGRESS_MIN_DELTA_PCT = self._orig_progress_delta
        api_server._persist_backtest_record = self._orig_persist_backtest_record
        api_server._get_backtest_runner = self._orig_get_backtest_runner

    def test_extract_backtest_progress_from_message(self):
        self.assertEqual(
            api_server._extract_backtest_progress_from_message(
                "BACKTEST_PROGRESS pct=12 done=3 total=25 ts=2026-01-01T00:00:00+00:00"
            ),
            12,
        )
        self.assertEqual(
            api_server._extract_backtest_progress_from_message(
                "BACKTEST_PROGRESS pct=150 done=3 total=25 ts=2026-01-01T00:00:00+00:00"
            ),
            100,
        )
        self.assertIsNone(api_server._extract_backtest_progress_from_message("not a progress line"))

    def test_on_backtest_process_log_updates_progress_and_persists(self):
        api_server._BACKTEST_STORE["run_1"] = {
            "id": "run_1",
            "owner": "alice",
            "status": "running",
            "progress": 0,
            "updatedAt": "2026-01-01T00:00:00+00:00",
        }
        event = {
            "metadata": {"run_id": "run_1", "owner": "alice"},
            "message": "BACKTEST_PROGRESS pct=3 done=3 total=100 ts=2026-01-01T00:00:03+00:00",
            "ts_utc": "2026-01-01T00:00:03+00:00",
        }
        api_server._on_backtest_process_log(event)
        self.assertEqual(int(api_server._BACKTEST_STORE["run_1"].get("progress", 0)), 3)
        self.assertEqual(len(self._persist_calls), 1)

        api_server._on_backtest_process_log(event)
        self.assertEqual(len(self._persist_calls), 1)

        api_server._on_backtest_process_log(
            {
                "metadata": {"run_id": "run_1", "owner": "alice"},
                "message": "BACKTEST_PROGRESS pct=4 done=4 total=100 ts=2026-01-01T00:00:04+00:00",
                "ts_utc": "2026-01-01T00:00:04+00:00",
            }
        )
        self.assertEqual(int(api_server._BACKTEST_STORE["run_1"].get("progress", 0)), 4)
        self.assertEqual(len(self._persist_calls), 2)

        api_server._on_backtest_process_log(
            {
                "metadata": {"run_id": "run_1", "owner": "alice"},
                "message": "BACKTEST_PROGRESS pct=2 done=2 total=100 ts=2026-01-01T00:00:05+00:00",
                "ts_utc": "2026-01-01T00:00:05+00:00",
            }
        )
        self.assertEqual(len(self._persist_calls), 2)

    def test_sync_backtest_record_status_reads_progress_from_logs(self):
        api_server._get_backtest_runner = lambda create=False: _FakeRunner()
        record = {
            "id": "run_sync",
            "owner": "alice",
            "status": "running",
            "progress": 0,
            "updatedAt": "2026-01-01T00:00:00+00:00",
        }
        with api_server._auth_user_context("alice"):
            api_server._sync_backtest_record_status(record)
        self.assertEqual(int(record.get("progress", 0)), 27)
        self.assertEqual(len(self._persist_calls), 1)

    def test_on_backtest_process_exit_sets_success_progress_to_100_and_cleans_state(self):
        api_server._BACKTEST_STORE["run_exit"] = {
            "id": "run_exit",
            "owner": "alice",
            "status": "running",
            "progress": 67,
            "updatedAt": "2026-01-01T00:00:00+00:00",
        }
        api_server._BACKTEST_PROGRESS_PERSIST_STATE["run_exit"] = {
            "last_progress": 67.0,
            "last_persist_epoch": time.time(),
        }
        api_server._on_backtest_process_exit(
            {
                "metadata": {"run_id": "run_exit", "owner": "alice"},
                "return_code": 0,
                "ended_at": "2026-01-01T00:10:00+00:00",
            }
        )
        self.assertEqual(api_server._BACKTEST_STORE["run_exit"]["status"], "success")
        self.assertEqual(int(api_server._BACKTEST_STORE["run_exit"].get("progress", 0)), 100)
        self.assertNotIn("run_exit", api_server._BACKTEST_PROGRESS_PERSIST_STATE)
        self.assertEqual(len(self._persist_calls), 1)


if __name__ == "__main__":
    unittest.main()
