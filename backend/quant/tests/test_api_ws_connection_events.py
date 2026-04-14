import tempfile
import types
import unittest
from pathlib import Path

import api_server
from db_store import SQLiteStore


def _fake_request(username: str):
    state = types.SimpleNamespace(auth_username=username)
    return types.SimpleNamespace(state=state)


class ApiWsConnectionEventsTests(unittest.TestCase):
    def setUp(self):
        self._tmp_db_dir = tempfile.TemporaryDirectory()
        db_path = Path(self._tmp_db_dir.name) / "quant_api.db"
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
        self._tmp_db_dir.cleanup()

    def test_ws_connection_events_endpoint_owner_scope(self):
        api_server._db_append_ws_connection_event(
            owner="guest",
            event_type="connected",
            connection_id="ws_guest_1",
            strategy_id="strategy_guest",
            config_path="config.yaml",
            refresh_ms=1000,
            client_ip="127.0.0.1",
            user_agent="guest-agent",
            detail={"phase": "open"},
            ts_utc="2026-03-05T00:00:00+00:00",
        )
        api_server._db_append_ws_connection_event(
            owner="admin",
            event_type="connected",
            connection_id="ws_admin_1",
            strategy_id="strategy_admin",
            config_path="config.yaml",
            refresh_ms=1000,
            client_ip="127.0.0.1",
            user_agent="admin-agent",
            detail={"phase": "open"},
            ts_utc="2026-03-05T00:00:01+00:00",
        )

        with api_server._auth_user_context("guest"):
            rows_guest = api_server.ws_connection_events(
                request=_fake_request("guest"),
                owner="admin",
                event_type=None,
                strategy_id=None,
                start=None,
                end=None,
                cursor=None,
                limit=20,
            )
            self.assertEqual(len(rows_guest), 1)
            self.assertEqual(str(rows_guest[0]["owner"]), "guest")

        with api_server._auth_user_context("admin"):
            rows_admin_filtered = api_server.ws_connection_events(
                request=_fake_request("admin"),
                owner="admin",
                event_type="connected",
                strategy_id="strategy_admin",
                start=None,
                end=None,
                cursor=None,
                limit=20,
            )
            self.assertEqual(len(rows_admin_filtered), 1)
            self.assertEqual(str(rows_admin_filtered[0]["owner"]), "admin")
            self.assertEqual(str(rows_admin_filtered[0]["connectionId"]), "ws_admin_1")


if __name__ == "__main__":
    unittest.main()
