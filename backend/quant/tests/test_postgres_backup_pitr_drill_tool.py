import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class PostgresBackupPitrDrillToolTests(unittest.TestCase):
    def test_requires_postgres_dsn(self):
        script = Path(__file__).resolve().parents[1] / "tools" / "postgres_backup_pitr_drill.py"
        env = os.environ.copy()
        env.pop("QUANT_E2E_POSTGRES_DSN", None)
        env.pop("API_DB_POSTGRES_DSN", None)

        completed = subprocess.run(
            [
                sys.executable,
                str(script),
                "--skip-pg-dump",
                "--skip-pitr-drill",
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
        "requires QUANT_E2E_POSTGRES_DSN for real PostgreSQL drill",
    )
    def test_real_postgres_wal_and_pitr_probe(self):
        script = Path(__file__).resolve().parents[1] / "tools" / "postgres_backup_pitr_drill.py"
        dsn = str(os.getenv("QUANT_E2E_POSTGRES_DSN", "")).strip()
        with tempfile.TemporaryDirectory() as tmp_dir:
            backup_dir = Path(tmp_dir) / "pg_backups"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--postgres-dsn",
                    dsn,
                    "--backup-dir",
                    str(backup_dir),
                    "--skip-pg-dump",
                    "--allow-wal-unconfigured",
                    "--allow-pitr-fail",
                ],
                capture_output=True,
                text=True,
                check=True,
            )

        payload = json.loads(completed.stdout)
        self.assertTrue(bool(payload.get("ok")))
        self.assertIn("wal", payload)
        self.assertIn("pitr", payload)
        self.assertTrue(bool((payload.get("pitr") or {}).get("enabled")))


if __name__ == "__main__":
    unittest.main()
