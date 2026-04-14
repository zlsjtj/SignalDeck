import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from db_store import SQLiteStore


class SQLiteRestoreDrillToolTests(unittest.TestCase):
    def test_restore_drill_passes_on_valid_backup(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            db_path = root / "quant_api.db"
            backup_path = root / "quant_api_backup.db"
            drill_dir = root / "drill"
            drill_dir.mkdir(parents=True, exist_ok=True)

            store = SQLiteStore(db_path)
            store.initialize()
            store.append_audit_log(
                owner="alice",
                action="strategy.create",
                entity="strategy",
                entity_id="s1",
                detail={"name": "s1"},
            )
            with sqlite3.connect(str(db_path)) as src, sqlite3.connect(str(backup_path)) as dst:
                src.backup(dst)
                dst.commit()

            script = Path(__file__).resolve().parents[1] / "tools" / "sqlite_restore_drill.py"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--backup-file",
                    str(backup_path),
                    "--output-dir",
                    str(drill_dir),
                    "--cleanup",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            payload = json.loads(completed.stdout)
            self.assertTrue(bool(payload.get("ok")))
            self.assertTrue(bool(payload.get("checks", {}).get("integrity", {}).get("ok")))
            self.assertTrue(bool(payload.get("checks", {}).get("required_tables", {}).get("ok")))
            self.assertTrue(bool(payload.get("checks", {}).get("read_write_probe", {}).get("ok")))
            self.assertEqual(str(payload.get("cleanup")), "done")


if __name__ == "__main__":
    unittest.main()
