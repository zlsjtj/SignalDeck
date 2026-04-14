import json
import subprocess
import sys
import unittest
from pathlib import Path


class E2EFrontendBackendSQLiteToolTests(unittest.TestCase):
    def test_backend_sqlite_smoke_path(self):
        script = Path(__file__).resolve().parents[1] / "tools" / "e2e_frontend_backend_sqlite.py"
        completed = subprocess.run(
            [
                sys.executable,
                str(script),
                "--skip-frontend-check",
                "--startup-timeout-seconds",
                "60",
                "--http-timeout-seconds",
                "15",
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        payload = json.loads(completed.stdout)
        self.assertTrue(bool(payload.get("ok")))

        backend = payload.get("backend", {})
        self.assertTrue(backend.get("steps"))
        self.assertIn("strategy.create", backend.get("steps", []))
        self.assertIn("risk.update", backend.get("steps", []))
        self.assertIn("risk.events.read", backend.get("steps", []))
        self.assertIn("audit.read", backend.get("steps", []))
        self.assertIn("strategy.delete", backend.get("steps", []))

        db_counts = backend.get("db_counts", {})
        self.assertGreaterEqual(int(db_counts.get("strategies", 0)), 0)
        self.assertGreaterEqual(int(db_counts.get("risk_events", 0)), 1)
        self.assertGreaterEqual(int(db_counts.get("audit_logs", 0)), 1)

        frontend = payload.get("frontend", {})
        self.assertTrue(bool(frontend.get("skipped")))


if __name__ == "__main__":
    unittest.main()
