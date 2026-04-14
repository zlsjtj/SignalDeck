import tempfile
import time
import types
import unittest
from pathlib import Path
from urllib import error as urllib_error

import api_server
from db_store import SQLiteStore


def _fake_request(username: str):
    state = types.SimpleNamespace(auth_username=username)
    return types.SimpleNamespace(state=state)


class _FakeResponse:
    def __init__(self, status: int = 200, body: str = "ok") -> None:
        self.status = status
        self._body = body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        _ = (exc_type, exc, tb)
        return False

    def getcode(self) -> int:
        return int(self.status)

    def read(self) -> bytes:
        return str(self._body).encode("utf-8")


class ApiAlertDeliveriesPersistenceTests(unittest.TestCase):
    def setUp(self):
        self._tmp_db_dir = tempfile.TemporaryDirectory()
        db_path = Path(self._tmp_db_dir.name) / "quant_api.db"
        self._store = SQLiteStore(db_path)
        self._store.initialize()

        self._orig_db = api_server._DB
        self._orig_db_ready = api_server._DB_READY
        self._orig_db_enabled = api_server._DB_ENABLED
        self._orig_db_init_error = api_server._DB_INIT_ERROR
        self._orig_alert_webhook = api_server._DB_ALERT_WEBHOOK_URL
        self._orig_alert_max_retries = api_server._DB_ALERT_MAX_RETRIES
        self._orig_alert_retry_backoff = api_server._DB_ALERT_RETRY_BACKOFF_MS
        self._orig_alert_timeout = api_server._DB_ALERT_TIMEOUT_SECONDS
        self._orig_alert_outbox_enabled = api_server._DB_ALERT_OUTBOX_ENABLED
        self._orig_alert_outbox_poll = api_server._DB_ALERT_OUTBOX_POLL_SECONDS
        self._orig_alert_outbox_batch = api_server._DB_ALERT_OUTBOX_BATCH_SIZE
        self._orig_urlopen = api_server.urllib_request.urlopen

        api_server._DB = self._store
        api_server._DB_READY = True
        api_server._DB_ENABLED = True
        api_server._DB_INIT_ERROR = ""
        api_server._DB_ALERT_WEBHOOK_URL = "https://example.invalid/hook"
        api_server._DB_ALERT_MAX_RETRIES = 1
        api_server._DB_ALERT_RETRY_BACKOFF_MS = 0
        api_server._DB_ALERT_TIMEOUT_SECONDS = 1.0
        api_server._DB_ALERT_OUTBOX_ENABLED = True
        api_server._DB_ALERT_OUTBOX_POLL_SECONDS = 0.01
        api_server._DB_ALERT_OUTBOX_BATCH_SIZE = 20
        api_server._stop_db_alert_outbox_worker(join_timeout=0.2)

    def tearDown(self):
        api_server._stop_db_alert_outbox_worker(join_timeout=0.5)
        api_server._DB = self._orig_db
        api_server._DB_READY = self._orig_db_ready
        api_server._DB_ENABLED = self._orig_db_enabled
        api_server._DB_INIT_ERROR = self._orig_db_init_error
        api_server._DB_ALERT_WEBHOOK_URL = self._orig_alert_webhook
        api_server._DB_ALERT_MAX_RETRIES = self._orig_alert_max_retries
        api_server._DB_ALERT_RETRY_BACKOFF_MS = self._orig_alert_retry_backoff
        api_server._DB_ALERT_TIMEOUT_SECONDS = self._orig_alert_timeout
        api_server._DB_ALERT_OUTBOX_ENABLED = self._orig_alert_outbox_enabled
        api_server._DB_ALERT_OUTBOX_POLL_SECONDS = self._orig_alert_outbox_poll
        api_server._DB_ALERT_OUTBOX_BATCH_SIZE = self._orig_alert_outbox_batch
        api_server.urllib_request.urlopen = self._orig_urlopen
        self._tmp_db_dir.cleanup()

    def test_emit_db_alert_persists_sent_delivery_with_retry(self):
        calls = {"n": 0}

        def _fake_urlopen(req, timeout=0):
            _ = (req, timeout)
            calls["n"] += 1
            if calls["n"] == 1:
                raise urllib_error.URLError("temporary error")
            return _FakeResponse(status=200, body="ok")

        api_server.urllib_request.urlopen = _fake_urlopen

        with api_server._auth_user_context("guest"):
            sent, err = api_server._emit_db_alert(
                event="db_runtime_persistence_failure",
                severity="critical",
                message="db write failed",
                detail={"kind": "unit"},
            )
        self.assertTrue(sent)
        self.assertIn(str(err), {"", "queued"})

        deadline = time.time() + 2.0
        rows = []
        while time.time() < deadline:
            rows = self._store.list_alert_deliveries(owner="guest", limit=10)
            if rows:
                break
            time.sleep(0.02)

        self.assertEqual(int(calls["n"]), 2)
        self.assertEqual(len(rows), 1)
        self.assertEqual(str(rows[0]["status"]), "sent")
        self.assertEqual(int(rows[0]["retryCount"]), 1)
        self.assertEqual(str(rows[0]["event"]), "db_runtime_persistence_failure")

    def test_alert_deliveries_endpoint_owner_scope(self):
        self._store.append_alert_delivery(
            owner="guest",
            event="db_runtime_persistence_failure",
            severity="critical",
            message="guest alert",
            webhook_url="https://example.invalid/hook",
            status="failed",
        )
        self._store.append_alert_delivery(
            owner="admin",
            event="db_init_failure",
            severity="critical",
            message="admin alert",
            webhook_url="https://example.invalid/hook",
            status="sent",
        )

        with api_server._auth_user_context("guest"):
            guest_rows = api_server.alert_deliveries(
                request=_fake_request("guest"),
                owner="admin",
                event=None,
                status=None,
                start=None,
                end=None,
                cursor=None,
                limit=20,
            )
            self.assertEqual(len(guest_rows), 1)
            self.assertEqual(str(guest_rows[0]["owner"]), "guest")

        with api_server._auth_user_context("admin"):
            admin_filtered = api_server.alert_deliveries(
                request=_fake_request("admin"),
                owner="guest",
                event=None,
                status=None,
                start=None,
                end=None,
                cursor=None,
                limit=20,
            )
            self.assertEqual(len(admin_filtered), 1)
            self.assertEqual(str(admin_filtered[0]["owner"]), "guest")


if __name__ == "__main__":
    unittest.main()
