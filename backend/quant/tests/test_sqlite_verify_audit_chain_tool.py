import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from db_store import SQLiteStore


class SQLiteVerifyAuditChainToolTests(unittest.TestCase):
    def test_verify_audit_chain_tool_detects_tamper(self):
        script = Path(__file__).resolve().parents[1] / "tools" / "sqlite_verify_audit_chain.py"
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()
            store.append_audit_log(
                owner="alice",
                action="strategy.create",
                entity="strategy",
                entity_id="s1",
                detail={"name": "s1"},
            )
            store.append_audit_log(
                owner="alice",
                action="strategy.start",
                entity="strategy",
                entity_id="s1",
                detail={},
            )

            ok_completed = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--db-path",
                    str(db_path),
                    "--owner",
                    "alice",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            ok_payload = json.loads(ok_completed.stdout)
            self.assertTrue(bool(ok_payload.get("ok")))
            self.assertEqual(int(ok_payload.get("checked", 0)), 2)

            with store._connect() as conn:
                conn.execute("UPDATE audit_logs SET detail_json = ? WHERE owner = ? AND id = 2", ('{"tampered":true}', "alice"))
                conn.commit()

            bad_completed = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--db-path",
                    str(db_path),
                    "--owner",
                    "alice",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertNotEqual(int(bad_completed.returncode), 0)
            bad_payload = json.loads(bad_completed.stdout)
            self.assertFalse(bool(bad_payload.get("ok")))
            self.assertTrue(bool(bad_payload.get("mismatchedRows")))


if __name__ == "__main__":
    unittest.main()
