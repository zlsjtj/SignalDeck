import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class PostgresPerformanceBaselineToolTests(unittest.TestCase):
    def test_requires_postgres_dsn(self):
        script = Path(__file__).resolve().parents[1] / "tools" / "postgres_performance_baseline.py"
        env = os.environ.copy()
        env.pop("QUANT_E2E_POSTGRES_DSN", None)
        env.pop("API_DB_POSTGRES_DSN", None)

        completed = subprocess.run(
            [sys.executable, str(script)],
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
        "requires QUANT_E2E_POSTGRES_DSN for real postgres baseline",
    )
    def test_real_postgres_perf_baseline_smoke(self):
        script = Path(__file__).resolve().parents[1] / "tools" / "postgres_performance_baseline.py"
        dsn = str(os.getenv("QUANT_E2E_POSTGRES_DSN", "")).strip()
        with tempfile.TemporaryDirectory() as _:
            completed = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--postgres-dsn",
                    dsn,
                    "--seed-rows",
                    "300",
                    "--write-ops",
                    "240",
                    "--commit-every",
                    "20",
                    "--pagination-queries",
                    "40",
                    "--report-queries",
                    "20",
                    "--min-write-tps",
                    "1",
                    "--max-pagination-p95-ms",
                    "2000",
                    "--max-report-p95-ms",
                    "3000",
                ],
                capture_output=True,
                text=True,
                check=True,
            )

        payload = json.loads(completed.stdout)
        self.assertTrue(bool(payload.get("ok")))
        metrics = payload.get("metrics") or {}
        self.assertIn("write", metrics)
        self.assertIn("pagination", metrics)
        self.assertIn("report", metrics)


if __name__ == "__main__":
    unittest.main()
