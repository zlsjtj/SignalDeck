import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class SQLiteColdBackupToolTests(unittest.TestCase):
    def test_cold_backup_and_retention(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            db_path = root / "quant_api.db"
            backup_dir = root / "cold_backups"
            backup_dir.mkdir(parents=True, exist_ok=True)

            with sqlite3.connect(str(db_path)) as conn:
                conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT NOT NULL)")
                conn.execute("INSERT INTO t (v) VALUES ('x')")
                conn.commit()

            script = Path(__file__).resolve().parents[1] / "tools" / "sqlite_cold_backup.py"
            for _ in range(3):
                completed = subprocess.run(
                    [
                        sys.executable,
                        str(script),
                        "--db-path",
                        str(db_path),
                        "--backup-dir",
                        str(backup_dir),
                        "--prefix",
                        "quant_api_cold",
                        "--retain",
                        "2",
                        "--verify",
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                payload = json.loads(completed.stdout)
                self.assertTrue(bool(payload.get("backup_path")))
                self.assertTrue(bool(payload.get("verify", {}).get("ok")))

            backups = sorted(backup_dir.glob("quant_api_cold_*.db"))
            self.assertLessEqual(len(backups), 2)
            self.assertGreaterEqual(len(backups), 1)


if __name__ == "__main__":
    unittest.main()
