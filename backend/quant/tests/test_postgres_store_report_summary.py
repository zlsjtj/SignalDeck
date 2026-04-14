import os
import unittest
import uuid
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

try:
    import psycopg
except Exception:  # pragma: no cover - environment dependent
    psycopg = None

from postgres_store import PostgresStore


def _dsn_with_search_path(base_dsn: str, schema: str) -> str:
    text = str(base_dsn or "").strip()
    if "://" not in text:
        return f"{text} options='-csearch_path={schema}'"
    parts = urlsplit(text)
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    params = {k: v for k, v in pairs}
    search_opt = f"-csearch_path={schema}"
    existing = str(params.get("options") or "").strip()
    params["options"] = f"{existing} {search_opt}".strip() if existing else search_opt
    query = urlencode(params)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))


@unittest.skipUnless(bool(str(os.getenv("QUANT_E2E_POSTGRES_DSN", "")).strip()), "requires QUANT_E2E_POSTGRES_DSN")
@unittest.skipUnless(psycopg is not None, "requires psycopg")
class PostgresStoreReportSummaryRegressionTests(unittest.TestCase):
    def setUp(self):
        self._base_dsn = str(os.getenv("QUANT_E2E_POSTGRES_DSN", "")).strip()
        self._schema = f"quant_test_{uuid.uuid4().hex[:12]}"
        with psycopg.connect(self._base_dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA "{self._schema}"')
        self._store = PostgresStore(_dsn_with_search_path(self._base_dsn, self._schema))
        self._store.initialize()

    def tearDown(self):
        with psycopg.connect(self._base_dsn, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP SCHEMA IF EXISTS "{self._schema}" CASCADE')

    def test_build_db_report_summary_all_owner_does_not_raise(self):
        self._store.append_audit_log(
            owner="alice",
            action="strategy.create",
            entity="strategy",
            entity_id="s1",
            detail={},
        )
        self._store.upsert_risk_state(
            "alice",
            "strategy_a",
            {"enabled": True, "maxDrawdownPct": 0.2, "updatedAt": "2026-01-01T00:00:00+00:00", "triggered": []},
        )
        self._store.upsert_risk_state(
            "alice",
            "strategy_a",
            {"enabled": True, "maxDrawdownPct": 0.3, "updatedAt": "2026-01-01T00:01:00+00:00", "triggered": []},
        )

        summary_all = self._store.build_db_report_summary(owner=None, start_ts=None, end_ts=None, limit_top=5)
        self.assertIn("auditTotal", summary_all)
        self.assertIn("topActions", summary_all)
        self.assertIn("topEntities", summary_all)
        self.assertIn("riskEventTotal", summary_all)
        self.assertIn("riskEventsByType", summary_all)
        self.assertIn("riskStateHistoryTotal", summary_all)
        self.assertGreaterEqual(int(summary_all.get("auditTotal") or 0), 1)
        self.assertGreaterEqual(int(summary_all.get("riskStateHistoryTotal") or 0), 1)

        summary_owner = self._store.build_db_report_summary(owner="alice", start_ts=None, end_ts=None, limit_top=5)
        self.assertGreaterEqual(int(summary_owner.get("auditTotal") or 0), 1)
        self.assertGreaterEqual(int(summary_owner.get("riskStateHistoryTotal") or 0), 1)


if __name__ == "__main__":
    unittest.main()
