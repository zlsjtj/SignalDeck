import unittest
from pathlib import Path


class PostgresPermissionModelAssetsTests(unittest.TestCase):
    def test_sql_template_contains_required_privilege_controls(self):
        sql_path = Path(__file__).resolve().parents[1] / "ops" / "postgres" / "postgres_permission_model_template.sql"
        self.assertTrue(sql_path.exists())
        text = sql_path.read_text(encoding="utf-8")

        required_tokens = [
            "quant_app_rw",
            "quant_report_ro",
            "quant_app_login",
            "quant_report_login",
            "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES",
            "GRANT SELECT ON ALL TABLES",
            "ALTER DEFAULT PRIVILEGES",
            "ALTER ROLE quant_app_login WITH PASSWORD",
        ]
        for token in required_tokens:
            self.assertIn(token, text)

    def test_doc_references_template_and_rotation(self):
        doc_path = Path(__file__).resolve().parents[1] / "docs" / "postgres_permission_model.md"
        self.assertTrue(doc_path.exists())
        text = doc_path.read_text(encoding="utf-8")

        self.assertIn("postgres_permission_model_template.sql", text)
        self.assertIn("凭据轮换", text)
        self.assertIn("最小权限", text)


if __name__ == "__main__":
    unittest.main()
