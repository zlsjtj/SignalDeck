import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from db_store import SQLiteStore


class ValidateDbConstraintsReplayToolTests(unittest.TestCase):
    def test_sqlite_constraints_replay_passes_on_clean_db(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            script = Path(__file__).resolve().parents[1] / "tools" / "validate_db_constraints_replay.py"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--backend",
                    "sqlite",
                    "--db-path",
                    str(db_path),
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            payload = json.loads(completed.stdout)
            self.assertTrue(bool(payload.get("ok")))
            self.assertEqual(int(payload.get("violations_total", 0)), 0)

    def test_sqlite_constraints_replay_detects_runtime_log_level_violation(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "quant_api.db"
            store = SQLiteStore(db_path)
            store.initialize()

            with sqlite3.connect(str(db_path), timeout=30.0) as conn:
                conn.execute("DROP TRIGGER IF EXISTS trg_constraint_runtime_logs_level_ins")
                conn.execute("DROP TRIGGER IF EXISTS trg_constraint_runtime_logs_level_upd")
                conn.execute(
                    """
                    INSERT INTO runtime_logs (
                        ts_utc, owner, log_type, level, source, message, strategy_id, backtest_id, detail_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        "2026-03-05T00:00:00+00:00",
                        "guest",
                        "system",
                        "fatal",
                        "unit",
                        "invalid level",
                        "",
                        "",
                        "{}",
                    ),
                )
                conn.commit()

            script = Path(__file__).resolve().parents[1] / "tools" / "validate_db_constraints_replay.py"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--backend",
                    "sqlite",
                    "--db-path",
                    str(db_path),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertNotEqual(completed.returncode, 0)
            payload = json.loads(completed.stdout)
            self.assertFalse(bool(payload.get("ok")))
            self.assertGreaterEqual(int(payload.get("violations_total", 0)), 1)
            runtime_rule = next(
                item for item in (payload.get("rules") or []) if str(item.get("name")) == "runtime_logs.level.enum"
            )
            self.assertGreaterEqual(int(runtime_rule.get("violations", 0)), 1)


if __name__ == "__main__":
    unittest.main()
