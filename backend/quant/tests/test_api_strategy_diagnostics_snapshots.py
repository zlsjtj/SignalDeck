import json
import tempfile
import types
import unittest
from pathlib import Path

import api_server
from db_store import SQLiteStore


def _fake_request(username: str):
    state = types.SimpleNamespace(auth_username=username)
    return types.SimpleNamespace(state=state)


class ApiStrategyDiagnosticsSnapshotsTests(unittest.TestCase):
    def setUp(self):
        self._tmp_db_dir = tempfile.TemporaryDirectory()
        db_path = Path(self._tmp_db_dir.name) / "quant_api.db"
        self._store = SQLiteStore(db_path)
        self._store.initialize()

        self._tmp_diag_dir = tempfile.TemporaryDirectory(dir=str(api_server.LOG_DIR))
        self._diag_root = Path(self._tmp_diag_dir.name)
        self._diag_file = self._diag_root / "usr__guest__diag_guest.json"
        self._diag_file.write_text(
            json.dumps(
                {
                    "generated_at": "2026-03-05T00:00:00+00:00",
                    "strategy_state": {"state": "RUNNING"},
                    "market_data": {"data_source_status": "ok"},
                    "signal_evaluation": {"entry_signal": True, "filter_reasons": ["f1"]},
                    "exceptions": {"total_count": 2},
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        self._orig_db = api_server._DB
        self._orig_db_ready = api_server._DB_READY
        self._orig_db_enabled = api_server._DB_ENABLED
        self._orig_db_init_error = api_server._DB_INIT_ERROR

        api_server._DB = self._store
        api_server._DB_READY = True
        api_server._DB_ENABLED = True
        api_server._DB_INIT_ERROR = ""

    def tearDown(self):
        api_server._DB = self._orig_db
        api_server._DB_READY = self._orig_db_ready
        api_server._DB_ENABLED = self._orig_db_enabled
        api_server._DB_INIT_ERROR = self._orig_db_init_error
        self._tmp_diag_dir.cleanup()
        self._tmp_db_dir.cleanup()

    def test_read_diagnostics_persists_snapshot_and_history_query(self):
        with api_server._auth_user_context("guest"):
            row = api_server.strategy_diagnostics(
                request=_fake_request("guest"),
                strategy_id="s1",
                path=str(self._diag_file),
            )
            self.assertEqual(str(row.get("strategy_id")), "s1")
            history = api_server.strategy_diagnostics_history(
                request=_fake_request("guest"),
                strategy_id="s1",
                owner=None,
                start=None,
                end=None,
                cursor=None,
                include_snapshot=True,
                limit=20,
            )
            self.assertEqual(len(history), 1)
            self.assertEqual(str(history[0].get("strategyState")), "RUNNING")
            self.assertEqual(int(history[0].get("exceptionTotalCount") or 0), 2)
            self.assertEqual(history[0].get("filterReasons"), ["f1"])
            self.assertTrue(isinstance(history[0].get("snapshot"), dict))

    def test_history_owner_isolation(self):
        admin_diag = self._diag_root / "diag_admin.json"
        admin_diag.write_text(
            json.dumps(
                {
                    "generated_at": "2026-03-05T00:01:00+00:00",
                    "strategy_state": {"state": "STOPPED"},
                    "market_data": {"data_source_status": "stale"},
                    "signal_evaluation": {"entry_signal": False, "filter_reasons": []},
                    "exceptions": {"total_count": 0},
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        with api_server._auth_user_context("guest"):
            api_server.strategy_diagnostics(
                request=_fake_request("guest"),
                strategy_id="s_guest",
                path=str(self._diag_file),
            )
        with api_server._auth_user_context("admin"):
            api_server.strategy_diagnostics(
                request=_fake_request("admin"),
                strategy_id="s_admin",
                path=str(admin_diag),
            )

        with api_server._auth_user_context("guest"):
            guest_rows = api_server.strategy_diagnostics_history(
                request=_fake_request("guest"),
                strategy_id=None,
                owner=None,
                start=None,
                end=None,
                cursor=None,
                include_snapshot=False,
                limit=20,
            )
            self.assertEqual(len(guest_rows), 1)
            self.assertEqual(str(guest_rows[0].get("strategyId")), "s_guest")

        with api_server._auth_user_context("admin"):
            admin_rows = api_server.strategy_diagnostics_history(
                request=_fake_request("admin"),
                strategy_id=None,
                owner="admin",
                start=None,
                end=None,
                cursor=None,
                include_snapshot=False,
                limit=20,
            )
            self.assertEqual(len(admin_rows), 1)
            self.assertEqual(str(admin_rows[0].get("strategyId")), "s_admin")


if __name__ == "__main__":
    unittest.main()
