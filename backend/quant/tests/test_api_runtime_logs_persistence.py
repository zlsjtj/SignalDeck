import tempfile
import types
import unittest
from pathlib import Path

import api_server
from db_store import SQLiteStore


def _fake_request(username: str):
    state = types.SimpleNamespace(auth_username=username)
    return types.SimpleNamespace(state=state)


class ApiRuntimeLogsPersistenceTests(unittest.TestCase):
    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory()
        db_path = Path(self._tmp_dir.name) / "quant_api.db"
        self._store = SQLiteStore(db_path)
        self._store.initialize()

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

    def test_runtime_logs_persist_and_support_filters(self):
        with api_server._auth_user_context("guest"):
            self._emit_strategy_log("guest", "s1", "2026-03-05T00:00:00+00:00", "strategy warmup done")
            self._emit_strategy_log("guest", "s1", "2026-03-05T00:01:00+00:00", "strategy panic error", source="stderr")

            all_rows = api_server.logs(
                request=_fake_request("guest"),
                type="strategy",
                limit=20,
            )
            self.assertEqual(len(all_rows), 2)
            self.assertEqual(str(all_rows[0]["level"]), "error")
            self.assertEqual(str(all_rows[1]["level"]), "info")

            error_rows = api_server.logs(
                request=_fake_request("guest"),
                type="strategy",
                level="error",
                limit=20,
            )
            self.assertEqual(len(error_rows), 1)
            self.assertIn("panic", str(error_rows[0]["message"]))

            keyword_rows = api_server.logs(
                request=_fake_request("guest"),
                type="strategy",
                q="warmup",
                limit=20,
            )
            self.assertEqual(len(keyword_rows), 1)
            self.assertIn("warmup", str(keyword_rows[0]["message"]))

            range_rows = api_server.logs(
                request=_fake_request("guest"),
                type="strategy",
                start="2026-03-05T00:00:30+00:00",
                end="2026-03-05T00:01:30+00:00",
                limit=20,
            )
            self.assertEqual(len(range_rows), 1)
            self.assertIn("panic", str(range_rows[0]["message"]))

            first_page = api_server.logs(
                request=_fake_request("guest"),
                type="strategy",
                limit=1,
            )
            self.assertEqual(len(first_page), 1)
            cursor = int(first_page[0]["cursorId"])
            second_page = api_server.logs(
                request=_fake_request("guest"),
                type="strategy",
                cursor=cursor,
                limit=20,
            )
            self.assertEqual(len(second_page), 1)
            self.assertIn("warmup", str(second_page[0]["message"]))

    def test_runtime_logs_isolated_by_owner(self):
        with api_server._auth_user_context("guest"):
            self._emit_strategy_log("guest", "guest_s1", "2026-03-05T00:00:00+00:00", "guest message")
        with api_server._auth_user_context("admin"):
            self._emit_strategy_log("admin", "admin_s1", "2026-03-05T00:00:01+00:00", "admin message")

        with api_server._auth_user_context("guest"):
            guest_rows = api_server.logs(request=_fake_request("guest"), type="strategy", limit=20)
            self.assertEqual(len(guest_rows), 1)
            self.assertIn("guest message", str(guest_rows[0]["message"]))

        with api_server._auth_user_context("admin"):
            admin_rows = api_server.logs(request=_fake_request("admin"), type="strategy", limit=20)
            self.assertEqual(len(admin_rows), 1)
            self.assertIn("admin message", str(admin_rows[0]["message"]))


if __name__ == "__main__":
    unittest.main()
