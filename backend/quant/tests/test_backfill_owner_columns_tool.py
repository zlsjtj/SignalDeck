import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from db_store import SQLiteStore


class BackfillOwnerColumnsToolTests(unittest.TestCase):
    def test_sqlite_backfill_owner_columns_and_users(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            db_path = root / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            store.upsert_strategy(
                "usr__guest__strategy_1",
                "",
                {
                    "id": "strategy_1",
                    "name": "guest strat",
                    "status": "stopped",
                    "createdAt": "2026-03-05T00:00:00+00:00",
                    "updatedAt": "2026-03-05T00:00:00+00:00",
                    "owner": "",
                    "config": {"symbols": ["BTC/USDT:USDT"], "timeframe": "1h"},
                },
            )
            store.upsert_backtest(
                "bt_1",
                " Admin ",
                {
                    "id": "bt_1",
                    "owner": " Admin ",
                    "status": "finished",
                    "createdAt": "2026-03-05T00:00:00+00:00",
                    "updatedAt": "2026-03-05T00:00:00+00:00",
                },
            )
            store.append_audit_log(owner="", action="a1", entity="strategy", entity_id="s1", detail={})

            script = Path(__file__).resolve().parents[1] / "tools" / "backfill_owner_columns.py"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--backend",
                    "sqlite",
                    "--db-path",
                    str(db_path),
                    "--default-owner",
                    "admin",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            payload = json.loads(completed.stdout)
            self.assertTrue(bool(payload.get("ok")))
            self.assertGreaterEqual(int(payload.get("updated_total", 0)), 2)
            self.assertGreaterEqual(int(payload.get("users_upserted", 0)), 0)

            with sqlite3.connect(str(db_path)) as conn:
                strategy_owner = str(
                    conn.execute("SELECT owner FROM strategies WHERE strategy_key = ?", ("usr__guest__strategy_1",)).fetchone()[0]
                )
                backtest_owner = str(conn.execute("SELECT owner FROM backtests WHERE run_id = ?", ("bt_1",)).fetchone()[0])
                audit_owner = str(
                    conn.execute(
                        "SELECT owner FROM audit_logs WHERE action = ? ORDER BY id DESC LIMIT 1",
                        ("a1",),
                    ).fetchone()[0]
                )
                users = {
                    str(row[0])
                    for row in conn.execute(
                        "SELECT username FROM users WHERE username IN ('admin', 'guest') ORDER BY username ASC"
                    ).fetchall()
                }

            self.assertEqual(strategy_owner, "guest")
            self.assertEqual(backtest_owner, "admin")
            self.assertEqual(audit_owner, "admin")
            self.assertSetEqual(users, {"admin", "guest"})

    def test_sqlite_backfill_dry_run_does_not_mutate(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            db_path = root / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()
            store.append_audit_log(owner="", action="dryrun", entity="strategy", entity_id="s1", detail={})

            script = Path(__file__).resolve().parents[1] / "tools" / "backfill_owner_columns.py"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--backend",
                    "sqlite",
                    "--db-path",
                    str(db_path),
                    "--dry-run",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            payload = json.loads(completed.stdout)
            self.assertTrue(bool(payload.get("ok")))
            self.assertGreaterEqual(int(payload.get("updated_total", 0)), 1)

            with sqlite3.connect(str(db_path)) as conn:
                owner = str(
                    conn.execute(
                        "SELECT owner FROM audit_logs WHERE action = ? ORDER BY id DESC LIMIT 1",
                        ("dryrun",),
                    ).fetchone()[0]
                )
            self.assertEqual(owner, "")


if __name__ == "__main__":
    unittest.main()
