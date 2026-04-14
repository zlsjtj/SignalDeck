import json
import os
import subprocess
import sys
import unittest
from pathlib import Path


class E2EBackendPostgresToolTests(unittest.TestCase):
    def test_requires_postgres_dsn_or_docker_mode(self):
        script = Path(__file__).resolve().parents[1] / "tools" / "e2e_backend_postgres.py"
        env = os.environ.copy()
        env.pop("QUANT_E2E_POSTGRES_DSN", None)
        env.pop("API_DB_POSTGRES_DSN", None)

        completed = subprocess.run(
            [
                sys.executable,
                str(script),
                "--startup-timeout-seconds",
                "5",
                "--http-timeout-seconds",
                "2",
            ],
            capture_output=True,
            text=True,
            check=False,
            env=env,
        )
        self.assertNotEqual(completed.returncode, 0)
        payload = json.loads(completed.stdout)
        self.assertFalse(bool(payload.get("ok")))
        self.assertIn("postgres dsn", str(payload.get("error", "")).lower())

    @unittest.skipUnless(
        bool(str(os.getenv("QUANT_E2E_POSTGRES_DSN", "")).strip()),
        "requires QUANT_E2E_POSTGRES_DSN for real PostgreSQL integration test",
    )
    def test_real_postgres_smoke_and_runtime_reload(self):
        script = Path(__file__).resolve().parents[1] / "tools" / "e2e_backend_postgres.py"
        dsn = str(os.getenv("QUANT_E2E_POSTGRES_DSN", "")).strip()
        completed = subprocess.run(
            [
                sys.executable,
                str(script),
                "--postgres-dsn",
                dsn,
                "--startup-timeout-seconds",
                "90",
                "--http-timeout-seconds",
                "20",
                "--postgres-ready-timeout-seconds",
                "60",
                "--min-schema-version",
                "13",
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        payload = json.loads(completed.stdout)
        self.assertTrue(bool(payload.get("ok")))
        backend = payload.get("backend", {})
        steps = backend.get("steps", [])
        required_steps = [
            "postgres.ready",
            "health.postgres.ok",
            "postgres.migration.ok",
            "strategy.create",
            "risk.update",
            "risk.events.read",
            "audit.read",
            "db.reload.sqlite",
            "health.sqlite.ok",
            "strategy.read.after.sqlite_reload",
            "db.reload.postgres",
            "health.postgres.reloaded",
            "strategy.read.after.postgres_reload",
            "reports.summary.read",
            "strategy.delete",
        ]
        for step in required_steps:
            self.assertIn(step, steps)

        postgres = payload.get("postgres", {})
        self.assertGreaterEqual(int(postgres.get("schema_version_max", 0)), 13)


if __name__ == "__main__":
    unittest.main()
