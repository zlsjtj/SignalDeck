import tempfile
import types
import unittest
from pathlib import Path

import api_server
from db_store import SQLiteStore
from fastapi import HTTPException


def _fake_request(username: str):
    state = types.SimpleNamespace(auth_username=username)
    return types.SimpleNamespace(state=state)


class ApiAuthTokensAndSecurityEventsTests(unittest.TestCase):
    def setUp(self):
        self._tmp_dir = tempfile.TemporaryDirectory()
        db_path = Path(self._tmp_dir.name) / "quant_api.db"
        self._store = SQLiteStore(db_path)
        self._store.initialize()

        self._orig_db = api_server._DB
        self._orig_db_ready = api_server._DB_READY
        self._orig_db_enabled = api_server._DB_ENABLED
        self._orig_db_init_error = api_server._DB_INIT_ERROR
        self._orig_auth_token = api_server._AUTH_TOKEN

        api_server._DB = self._store
        api_server._DB_READY = True
        api_server._DB_ENABLED = True
        api_server._DB_INIT_ERROR = ""
        api_server._AUTH_TOKEN = ""

    def tearDown(self):
        api_server._DB = self._orig_db
        api_server._DB_READY = self._orig_db_ready
        api_server._DB_ENABLED = self._orig_db_enabled
        api_server._DB_INIT_ERROR = self._orig_db_init_error
        api_server._AUTH_TOKEN = self._orig_auth_token
        self._tmp_dir.cleanup()

    def test_auth_token_create_list_revoke_and_authenticate(self):
        with api_server._auth_user_context("admin"):
            created = api_server.auth_tokens_create(
                payload=api_server.ApiTokenCreateRequest(
                    owner="lsm_test",
                    tokenName="robot",
                    scopes=["read", "trade"],
                    expiresAt="2099-01-01T00:00:00+00:00",
                ),
                request=_fake_request("admin"),
            )
            raw_token = str(created.get("token") or "")
            self.assertTrue(raw_token.startswith("qat_"))

            meta = created.get("meta") or {}
            self.assertEqual(str(meta.get("owner")), "lsm_test")
            self.assertEqual(meta.get("scopes"), ["read", "trade"])
            token_id = int(meta.get("id") or 0)
            self.assertGreater(token_id, 0)

            resolved_user = api_server._resolve_auth_username(
                authorization=None,
                x_api_key=raw_token,
                session_token=None,
            )
            self.assertEqual(str(resolved_user), "lsm_test")

            listed = api_server.auth_tokens_list(
                request=_fake_request("admin"),
                owner="lsm_test",
                include_revoked=False,
                limit=10,
            )
            self.assertEqual(len(listed), 1)
            self.assertEqual(int(listed[0]["id"]), token_id)
            self.assertTrue(str(listed[0]["lastUsedAt"] or ""))

            api_server.auth_token_revoke(token_id=token_id, request=_fake_request("admin"))
            resolved_after_revoke = api_server._resolve_auth_username(
                authorization=None,
                x_api_key=raw_token,
                session_token=None,
            )
            self.assertIsNone(resolved_after_revoke)

            listed_active = api_server.auth_tokens_list(
                request=_fake_request("admin"),
                owner="lsm_test",
                include_revoked=False,
                limit=10,
            )
            self.assertEqual(len(listed_active), 0)

            listed_all = api_server.auth_tokens_list(
                request=_fake_request("admin"),
                owner="lsm_test",
                include_revoked=True,
                limit=10,
            )
            self.assertEqual(len(listed_all), 1)
            self.assertTrue(str(listed_all[0]["revokedAt"] or ""))

    def test_security_events_endpoint_respects_owner_scope(self):
        api_server._db_append_account_security_event(
            owner="guest",
            event_type="login_success",
            severity="info",
            message="guest login",
            detail={"client_ip": "127.0.0.1"},
            ts_utc="2026-03-05T00:00:00+00:00",
        )
        api_server._db_append_account_security_event(
            owner="admin",
            event_type="login_success",
            severity="info",
            message="admin login",
            detail={"client_ip": "127.0.0.2"},
            ts_utc="2026-03-05T00:01:00+00:00",
        )

        with api_server._auth_user_context("guest"):
            guest_rows = api_server.auth_security_events(
                request=_fake_request("guest"),
                owner=None,
                event_type=None,
                start=None,
                end=None,
                cursor=None,
                limit=20,
            )
        self.assertEqual(len(guest_rows), 1)
        self.assertEqual(str(guest_rows[0]["owner"]), "guest")

        with api_server._auth_user_context("admin"):
            admin_rows = api_server.auth_security_events(
                request=_fake_request("admin"),
                owner="guest",
                event_type=None,
                start=None,
                end=None,
                cursor=None,
                limit=20,
            )
        self.assertEqual(len(admin_rows), 1)
        self.assertEqual(str(admin_rows[0]["owner"]), "guest")

    def test_rbac_role_binding_controls_token_management_permission(self):
        with api_server._auth_user_context("admin"):
            updated = api_server.auth_user_roles_update(
                payload=api_server.UserRolesUpdateRequest(username="lsm_test", roles=["auditor"]),
                request=_fake_request("admin"),
            )
            self.assertEqual(updated.get("roles"), ["auditor"])

        with api_server._auth_user_context("lsm_test"):
            with self.assertRaises(HTTPException) as denied:
                api_server.auth_tokens_create(
                    payload=api_server.ApiTokenCreateRequest(
                        owner="lsm_test",
                        tokenName="blocked",
                        scopes=["read"],
                    ),
                    request=_fake_request("lsm_test"),
                )
            self.assertEqual(int(denied.exception.status_code), 403)

        with api_server._auth_user_context("admin"):
            updated_admin_role = api_server.auth_user_roles_update(
                payload=api_server.UserRolesUpdateRequest(username="lsm_test", roles=["admin"]),
                request=_fake_request("admin"),
            )
            self.assertEqual(updated_admin_role.get("roles"), ["admin"])

        with api_server._auth_user_context("lsm_test"):
            created = api_server.auth_tokens_create(
                payload=api_server.ApiTokenCreateRequest(
                    owner="lsm_test",
                    tokenName="allowed",
                    scopes=["read"],
                ),
                request=_fake_request("lsm_test"),
            )
            self.assertTrue(str(created.get("token") or "").startswith("qat_"))

    def test_non_admin_with_auditor_role_can_read_cross_owner_security_events(self):
        api_server._db_append_account_security_event(
            owner="guest",
            event_type="login_success",
            severity="info",
            message="guest login",
            detail={"client_ip": "127.0.0.1"},
            ts_utc="2026-03-05T00:00:00+00:00",
        )
        api_server._db_append_account_security_event(
            owner="admin",
            event_type="login_success",
            severity="info",
            message="admin login",
            detail={"client_ip": "127.0.0.2"},
            ts_utc="2026-03-05T00:01:00+00:00",
        )

        with api_server._auth_user_context("admin"):
            api_server.auth_user_roles_update(
                payload=api_server.UserRolesUpdateRequest(username="auditor_user", roles=["auditor"]),
                request=_fake_request("admin"),
            )

        with api_server._auth_user_context("auditor_user"):
            guest_rows = api_server.auth_security_events(
                request=_fake_request("auditor_user"),
                owner="guest",
                event_type=None,
                start=None,
                end=None,
                cursor=None,
                limit=20,
            )
            self.assertEqual(len(guest_rows), 1)
            self.assertEqual(str(guest_rows[0]["owner"]), "guest")


if __name__ == "__main__":
    unittest.main()
