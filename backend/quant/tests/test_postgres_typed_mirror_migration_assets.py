import unittest
from pathlib import Path


class PostgresTypedMirrorMigrationAssetsTests(unittest.TestCase):
    def test_v18_migration_registered(self):
        src = (Path(__file__).resolve().parents[1] / "postgres_store.py").read_text(encoding="utf-8")
        self.assertIn(
            '(18, "add_postgres_typed_mirror_columns", self._migration_v18_add_postgres_typed_mirror_columns)',
            src,
        )

    def test_v18_contains_jsonb_gin_and_timestamptz_assets(self):
        src = (Path(__file__).resolve().parents[1] / "postgres_store.py").read_text(encoding="utf-8")
        self.assertIn("def _migration_v18_add_postgres_typed_mirror_columns", src)
        self.assertIn("CREATE OR REPLACE FUNCTION quant_safe_jsonb", src)
        self.assertIn("CREATE OR REPLACE FUNCTION quant_safe_timestamptz", src)
        self.assertIn("record_jsonb JSONB", src)
        self.assertIn("detail_jsonb JSONB", src)
        self.assertIn("state_jsonb JSONB", src)
        self.assertIn("USING GIN (record_jsonb)", src)
        self.assertIn("USING GIN (detail_jsonb)", src)
        self.assertIn("USING GIN (state_jsonb)", src)
        self.assertIn("ts_utc_tz TIMESTAMPTZ", src)


if __name__ == "__main__":
    unittest.main()
