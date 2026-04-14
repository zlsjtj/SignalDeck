import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from db_store import SQLiteStore


class SQLiteRetentionCleanupToolTests(unittest.TestCase):
    def test_cleanup_audit_and_backtests(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            db_path = root / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            old_ts = datetime(2020, 1, 1, tzinfo=timezone.utc).isoformat()
            now_ts = datetime.now(timezone.utc).isoformat()

            store.append_audit_log(owner="alice", action="a_old", entity="strategy", entity_id="s_old", detail={})
            store.append_audit_log(owner="alice", action="a_new", entity="strategy", entity_id="s_new", detail={})
            rows = store.list_audit_logs(limit=10)
            by_action = {row["action"]: int(row["id"]) for row in rows}
            with store._connect() as conn:
                conn.execute("UPDATE audit_logs SET ts_utc = ? WHERE id = ?", (old_ts, by_action["a_old"]))
                conn.execute("UPDATE audit_logs SET ts_utc = ? WHERE id = ?", (now_ts, by_action["a_new"]))
                conn.commit()

            store.append_runtime_log(
                owner="alice",
                log_type="strategy",
                level="info",
                source="stdout",
                message="old runtime",
                strategy_id="s_old",
                ts_utc=old_ts,
            )
            store.append_runtime_log(
                owner="alice",
                log_type="strategy",
                level="info",
                source="stdout",
                message="new runtime",
                strategy_id="s_new",
                ts_utc=now_ts,
            )

            store.upsert_backtest(
                "bt_old_finished",
                "alice",
                {
                    "id": "bt_old_finished",
                    "owner": "alice",
                    "status": "finished",
                    "createdAt": old_ts,
                    "updatedAt": old_ts,
                },
            )
            store.upsert_backtest(
                "bt_old_running",
                "alice",
                {
                    "id": "bt_old_running",
                    "owner": "alice",
                    "status": "running",
                    "createdAt": old_ts,
                    "updatedAt": old_ts,
                },
            )
            store.upsert_backtest(
                "bt_new_finished",
                "alice",
                {
                    "id": "bt_new_finished",
                    "owner": "alice",
                    "status": "finished",
                    "createdAt": now_ts,
                    "updatedAt": now_ts,
                },
            )

            script = Path(__file__).resolve().parents[1] / "tools" / "sqlite_retention_cleanup.py"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--db-path",
                    str(db_path),
                    "--audit-ttl-days",
                    "30",
                    "--runtime-log-ttl-days",
                    "30",
                    "--backtest-ttl-days",
                    "30",
                    "--backtest-final-statuses",
                    "finished,failed,stopped",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            payload = json.loads(completed.stdout)
            self.assertEqual(int(payload["audit"]["deleted"]), 1)
            self.assertEqual(int(payload["runtime_logs"]["deleted"]), 1)
            self.assertEqual(int(payload["backtests"]["deleted"]), 1)
            self.assertEqual(int(payload["deleted_total"]), 3)

            with sqlite3.connect(str(db_path)) as conn:
                audit_count = int(conn.execute("SELECT COUNT(1) FROM audit_logs").fetchone()[0])
                self.assertEqual(audit_count, 1)
                runtime_count = int(conn.execute("SELECT COUNT(1) FROM runtime_logs").fetchone()[0])
                self.assertEqual(runtime_count, 1)
                run_ids = {
                    str(row[0])
                    for row in conn.execute("SELECT run_id FROM backtests ORDER BY run_id ASC").fetchall()
                }
            self.assertSetEqual(run_ids, {"bt_old_running", "bt_new_finished"})


if __name__ == "__main__":
    unittest.main()
