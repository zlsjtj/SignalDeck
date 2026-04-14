import tempfile
import types
import unittest
from pathlib import Path

import api_server
from db_store import SQLiteStore


def _fake_request(username: str):
    state = types.SimpleNamespace(auth_username=username)
    return types.SimpleNamespace(state=state)


class ApiUserPreferencesTests(unittest.TestCase):
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

    def test_preferences_default_and_update(self):
        with api_server._auth_user_context("guest"):
            defaults = api_server.user_preferences_get(request=_fake_request("guest"))
            self.assertEqual(str(defaults.get("theme")), "dark")
            self.assertEqual(str(defaults.get("language")), "zh")

            updated = api_server.user_preferences_put(
                payload=api_server.UserPreferencesUpdateRequest(
                    theme="light",
                    language="en",
                    selectedLiveStrategyId="strategy_guest",
                    logsFilters={
                        "view": "audit",
                        "runtimePageSize": 50,
                        "auditPageSize": 20,
                        "riskEventsPageSize": 100,
                    },
                    backtestsFilters={
                        "listStrategyId": "strategy_guest",
                        "listPageSize": 20,
                        "createSymbol": "BTCUSDT",
                        "createStartAt": "2026-03-01T00:00:00+00:00",
                        "createEndAt": "2026-03-05T00:00:00+00:00",
                        "createInitialCapital": 120000,
                        "createFeeRate": 0.0008,
                        "createSlippage": 0.0003,
                    },
                    liveFilters={"diagRefreshMs": 120000},
                ),
                request=_fake_request("guest"),
            )
            self.assertEqual(str(updated.get("theme")), "light")
            self.assertEqual(str(updated.get("language")), "en")
            self.assertEqual(str(updated.get("selectedLiveStrategyId")), "strategy_guest")
            self.assertEqual(
                str(((updated.get("backtestsFilters") or {}).get("listStrategyId"))),
                "strategy_guest",
            )
            self.assertEqual(
                str(((updated.get("backtestsFilters") or {}).get("createSymbol"))),
                "BTCUSDT",
            )
            self.assertEqual(
                int(((updated.get("backtestsFilters") or {}).get("listPageSize"))),
                20,
            )
            self.assertEqual(
                int(((updated.get("backtestsFilters") or {}).get("createInitialCapital"))),
                120000,
            )
            self.assertEqual(
                int(((updated.get("logsFilters") or {}).get("runtimePageSize"))),
                50,
            )
            self.assertEqual(
                int(((updated.get("liveFilters") or {}).get("diagRefreshMs"))),
                120000,
            )

            reloaded = api_server.user_preferences_get(request=_fake_request("guest"))
            self.assertEqual(str(reloaded.get("theme")), "light")
            self.assertEqual(str(reloaded.get("language")), "en")
            self.assertEqual(str(reloaded.get("selectedLiveStrategyId")), "strategy_guest")
            self.assertEqual(str(((reloaded.get("logsFilters") or {}).get("view"))), "audit")
            self.assertEqual(
                int(((reloaded.get("logsFilters") or {}).get("riskEventsPageSize"))),
                100,
            )
            self.assertEqual(
                str(((reloaded.get("backtestsFilters") or {}).get("listStrategyId"))),
                "strategy_guest",
            )
            self.assertEqual(
                str(((reloaded.get("backtestsFilters") or {}).get("createStartAt"))),
                "2026-03-01T00:00:00+00:00",
            )
            self.assertEqual(
                str(((reloaded.get("backtestsFilters") or {}).get("createEndAt"))),
                "2026-03-05T00:00:00+00:00",
            )
            self.assertEqual(
                int(((reloaded.get("liveFilters") or {}).get("diagRefreshMs"))),
                120000,
            )

            audit_rows = self._store.list_audit_logs(owner="guest", action="user.preferences.update", limit=10)
            self.assertEqual(len(audit_rows), 1)

    def test_preferences_isolated_between_users(self):
        with api_server._auth_user_context("guest"):
            api_server.user_preferences_put(
                payload=api_server.UserPreferencesUpdateRequest(theme="light", language="en"),
                request=_fake_request("guest"),
            )

        with api_server._auth_user_context("admin"):
            admin_defaults = api_server.user_preferences_get(request=_fake_request("admin"))
            self.assertEqual(str(admin_defaults.get("theme")), "dark")
            self.assertEqual(str(admin_defaults.get("language")), "zh")
            api_server.user_preferences_put(
                payload=api_server.UserPreferencesUpdateRequest(theme="dark", language="zh"),
                request=_fake_request("admin"),
            )

        with api_server._auth_user_context("guest"):
            guest_prefs = api_server.user_preferences_get(request=_fake_request("guest"))
            self.assertEqual(str(guest_prefs.get("theme")), "light")
            self.assertEqual(str(guest_prefs.get("language")), "en")

        with api_server._auth_user_context("admin"):
            admin_prefs = api_server.user_preferences_get(request=_fake_request("admin"))
            self.assertEqual(str(admin_prefs.get("theme")), "dark")
            self.assertEqual(str(admin_prefs.get("language")), "zh")


if __name__ == "__main__":
    unittest.main()
