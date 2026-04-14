import tempfile
import types
import unittest
from pathlib import Path
from datetime import datetime, timezone, timedelta

import api_server
from db_store import SQLiteStore


def _fake_request(username: str):
    state = types.SimpleNamespace(auth_username=username)
    return types.SimpleNamespace(state=state)


class AuditLogPermissionTests(unittest.TestCase):
    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory()
        db_path = Path(self._tmp_dir.name) / "quant_api.db"
        store = SQLiteStore(db_path)
        store.initialize()
        store.append_audit_log(
            owner="alice",
            action="strategy.create",
            entity="strategy",
            entity_id="strategy_alice_1",
            detail={"name": "alice_s1"},
        )
        store.append_audit_log(
            owner="bob",
            action="risk.update",
            entity="risk",
            entity_id="strategy_bob_1",
            detail={"maxDrawdownPct": 0.1},
        )
        store.append_audit_log(
            owner="admin",
            action="auth.login.success",
            entity="auth",
            entity_id="admin",
            detail={},
        )
        rows = store.list_audit_logs(limit=10)
        by_owner = {row["owner"]: int(row["id"]) for row in rows}
        t0 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        with store._connect() as conn:
            conn.execute("UPDATE audit_logs SET ts_utc = ? WHERE id = ?", (t0.isoformat(), by_owner["alice"]))
            conn.execute(
                "UPDATE audit_logs SET ts_utc = ? WHERE id = ?",
                ((t0 + timedelta(minutes=1)).isoformat(), by_owner["bob"]),
            )
            conn.execute(
                "UPDATE audit_logs SET ts_utc = ? WHERE id = ?",
                ((t0 + timedelta(minutes=2)).isoformat(), by_owner["admin"]),
            )
            conn.commit()

        self._orig_db = api_server._DB
        self._orig_db_ready = api_server._DB_READY
        self._orig_db_enabled = api_server._DB_ENABLED
        self._orig_db_init_error = api_server._DB_INIT_ERROR

        api_server._DB = store
        api_server._DB_READY = True
        api_server._DB_ENABLED = True
        api_server._DB_INIT_ERROR = ""

    def tearDown(self):
        api_server._DB = self._orig_db
        api_server._DB_READY = self._orig_db_ready
        api_server._DB_ENABLED = self._orig_db_enabled
        api_server._DB_INIT_ERROR = self._orig_db_init_error
        self._tmp_dir.cleanup()

    def test_admin_can_filter_owner(self):
        rows = api_server.audit_logs(
            request=_fake_request("admin"),
            limit=200,
            action=None,
            entity=None,
            owner="alice",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["owner"], "alice")

    def test_non_admin_cannot_read_other_owner_even_with_owner_param(self):
        rows = api_server.audit_logs(
            request=_fake_request("alice"),
            limit=200,
            action=None,
            entity=None,
            owner="bob",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["owner"], "alice")
        self.assertEqual(rows[0]["entityId"], "strategy_alice_1")

    def test_non_admin_without_owner_param_only_sees_self(self):
        rows = api_server.audit_logs(
            request=_fake_request("bob"),
            limit=200,
            action=None,
            entity=None,
            owner=None,
            start=None,
            end=None,
            cursor=None,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["owner"], "bob")
        self.assertEqual(rows[0]["entityId"], "strategy_bob_1")

    def test_admin_supports_start_end_filter(self):
        rows = api_server.audit_logs(
            request=_fake_request("admin"),
            limit=200,
            action=None,
            entity=None,
            owner=None,
            start="2026-01-01T00:00:30+00:00",
            end="2026-01-01T00:01:30+00:00",
            cursor=None,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["owner"], "bob")

    def test_admin_supports_cursor_pagination(self):
        page1 = api_server.audit_logs(
            request=_fake_request("admin"),
            limit=2,
            action=None,
            entity=None,
            owner=None,
            start=None,
            end=None,
            cursor=None,
        )
        self.assertEqual(len(page1), 2)
        cursor = int(page1[-1]["id"])
        page2 = api_server.audit_logs(
            request=_fake_request("admin"),
            limit=2,
            action=None,
            entity=None,
            owner=None,
            start=None,
            end=None,
            cursor=cursor,
        )
        self.assertEqual(len(page2), 1)
        self.assertTrue(all(int(row["id"]) < cursor for row in page2))


if __name__ == "__main__":
    unittest.main()
