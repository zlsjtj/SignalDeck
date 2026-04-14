import tempfile
import types
import unittest
from pathlib import Path

from fastapi import Response

import api_server
from db_store import SQLiteStore


def _fake_http_request(
    *,
    client_ip: str = "127.0.0.1",
    user_agent: str = "pytest-agent",
    cookies=None,
):
    return types.SimpleNamespace(
        headers={
            "x-real-ip": client_ip,
            "user-agent": user_agent,
        },
        client=types.SimpleNamespace(host=client_ip),
        cookies=cookies or {},
    )


def _extract_cookie_token(set_cookie_header: str, cookie_name: str) -> str:
    prefix = f"{cookie_name}="
    for part in str(set_cookie_header or "").split(";"):
        text = part.strip()
        if text.startswith(prefix):
            return text[len(prefix):]
    return ""


class AuthSessionPersistenceTests(unittest.TestCase):
    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory()
        db_path = Path(self._tmp_dir.name) / "quant_api.db"
        self._store = SQLiteStore(db_path)
        self._store.initialize()

        self._orig_db = api_server._DB
        self._orig_db_ready = api_server._DB_READY
        self._orig_db_enabled = api_server._DB_ENABLED
        self._orig_db_init_error = api_server._DB_INIT_ERROR
        self._orig_dashboard_credentials = dict(api_server._DASHBOARD_CREDENTIALS)
        self._orig_session_secret = api_server._SESSION_SECRET
        self._orig_login_attempts = dict(api_server._LOGIN_ATTEMPTS)
        self._orig_login_locked = dict(api_server._LOGIN_LOCKED_UNTIL)
        self._orig_login_max_attempts = api_server._LOGIN_RATE_LIMIT_MAX_ATTEMPTS
        self._orig_login_lockout_seconds = api_server._LOGIN_LOCKOUT_SECONDS
        self._orig_login_window_seconds = api_server._LOGIN_RATE_LIMIT_WINDOW_SECONDS

        api_server._DB = self._store
        api_server._DB_READY = True
        api_server._DB_ENABLED = True
        api_server._DB_INIT_ERROR = ""
        api_server._DASHBOARD_CREDENTIALS = {
            "admin": "admin-pass",
            "lsm_test": "lsm-pass",
        }
        api_server._SESSION_SECRET = "unit-test-session-secret"
        api_server._LOGIN_ATTEMPTS = {}
        api_server._LOGIN_LOCKED_UNTIL = {}
        api_server._LOGIN_RATE_LIMIT_MAX_ATTEMPTS = 2
        api_server._LOGIN_LOCKOUT_SECONDS = 120
        api_server._LOGIN_RATE_LIMIT_WINDOW_SECONDS = 300

    def tearDown(self):
        api_server._DB = self._orig_db
        api_server._DB_READY = self._orig_db_ready
        api_server._DB_ENABLED = self._orig_db_enabled
        api_server._DB_INIT_ERROR = self._orig_db_init_error
        api_server._DASHBOARD_CREDENTIALS = self._orig_dashboard_credentials
        api_server._SESSION_SECRET = self._orig_session_secret
        api_server._LOGIN_ATTEMPTS = self._orig_login_attempts
        api_server._LOGIN_LOCKED_UNTIL = self._orig_login_locked
        api_server._LOGIN_RATE_LIMIT_MAX_ATTEMPTS = self._orig_login_max_attempts
        api_server._LOGIN_LOCKOUT_SECONDS = self._orig_login_lockout_seconds
        api_server._LOGIN_RATE_LIMIT_WINDOW_SECONDS = self._orig_login_window_seconds
        self._tmp_dir.cleanup()

    def test_login_persists_session_and_logout_revokes_it(self):
        request = _fake_http_request(client_ip="10.0.0.8")
        response = Response()
        login_resp = api_server.auth_login(
            payload=api_server.AuthLoginRequest(username="lsm_test", password="lsm-pass"),
            response=response,
            request=request,
        )
        self.assertTrue(bool(login_resp.get("authenticated")))
        self.assertEqual(str(login_resp.get("username")), "lsm_test")

        token = _extract_cookie_token(
            response.headers.get("set-cookie", ""),
            api_server._SESSION_COOKIE_NAME,
        )
        self.assertTrue(token)
        payload = api_server._validate_session_token_payload(token)
        self.assertIsInstance(payload, dict)
        session_id = str((payload or {}).get("session_id") or "")
        self.assertTrue(session_id)

        persisted = self._store.get_auth_session(session_id)
        self.assertIsInstance(persisted, dict)
        self.assertEqual(str((persisted or {}).get("username")), "lsm_test")
        self.assertEqual(str((persisted or {}).get("revokedAt") or ""), "")
        login_events = self._store.list_account_security_events(owner="lsm_test", event_type="login_success", limit=10)
        self.assertEqual(len(login_events), 1)

        self.assertEqual(api_server._validate_session_token(token), "lsm_test")

        logout_resp = Response()
        api_server.auth_logout(
            response=logout_resp,
            request=_fake_http_request(cookies={api_server._SESSION_COOKIE_NAME: token}),
        )
        persisted_after = self._store.get_auth_session(session_id)
        self.assertIsInstance(persisted_after, dict)
        self.assertTrue(str((persisted_after or {}).get("revokedAt") or ""))
        self.assertIsNone(api_server._validate_session_token(token))
        logout_events = self._store.list_account_security_events(owner="lsm_test", event_type="logout", limit=10)
        self.assertEqual(len(logout_events), 1)

    def test_login_lockout_persists_to_database(self):
        username = "lsm_test"
        client_ip = "10.0.0.8"
        api_server._login_rate_limit_record_failure(username=username, client_ip=client_ip)
        api_server._login_rate_limit_record_failure(username=username, client_ip=client_ip)

        # Simulate process restart by clearing in-memory lock state.
        api_server._LOGIN_ATTEMPTS = {}
        api_server._LOGIN_LOCKED_UNTIL = {}

        retry_after = api_server._login_rate_limit_check(username=username, client_ip=client_ip)
        self.assertGreater(retry_after, 0)

        api_server._login_rate_limit_reset(username=username, client_ip=client_ip)
        api_server._LOGIN_ATTEMPTS = {}
        api_server._LOGIN_LOCKED_UNTIL = {}
        retry_after_after_reset = api_server._login_rate_limit_check(username=username, client_ip=client_ip)
        self.assertEqual(retry_after_after_reset, 0)


if __name__ == "__main__":
    unittest.main()
