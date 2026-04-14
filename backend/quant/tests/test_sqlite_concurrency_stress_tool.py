import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class SQLiteConcurrencyStressToolTests(unittest.TestCase):
    def test_stress_tool_runs_and_emits_report(self):
        script = Path(__file__).resolve().parents[1] / "tools" / "sqlite_concurrency_stress.py"
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            db_path = root / "stress.db"
            report_path = root / "stress_report.md"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--db-path",
                    str(db_path),
                    "--threads",
                    "2",
                    "--ops-per-thread",
                    "20",
                    "--users",
                    "4",
                    "--max-error-rate",
                    "0.5",
                    "--max-p95-ms",
                    "2000",
                    "--report-md",
                    str(report_path),
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            payload = json.loads(completed.stdout)
            self.assertTrue(bool(payload.get("pass")))
            self.assertEqual(int(payload.get("total_ops", 0)), 40)
            self.assertTrue(report_path.exists())


if __name__ == "__main__":
    unittest.main()
