import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from db_store import SQLiteStore


class ReconcileDbReportSummaryToolTests(unittest.TestCase):
    def _seed_basic_data(self, db_path: Path, *, owner: str, event_suffix: str = "") -> None:
        store = SQLiteStore(db_path)
        store.initialize()
        store.append_audit_log(
            owner=owner,
            action=f"strategy.create{event_suffix}",
            entity="strategy",
            entity_id=f"s_{event_suffix or '0'}",
            detail={},
        )
        store.upsert_risk_state(
            owner,
            f"usr__{owner}__strategy_{event_suffix or '0'}",
            {
                "enabled": True,
                "maxDrawdownPct": 0.2,
                "updatedAt": "2026-03-05T00:01:00+00:00",
                "triggered": [],
            },
        )
        store.append_risk_event(
            owner=owner,
            strategy_key=f"usr__{owner}__strategy_{event_suffix or '0'}",
            event_type="triggered",
            rule="max_drawdown",
            message="triggered",
            detail={},
            ts_utc="2026-03-05T00:02:00+00:00",
        )

    def test_sqlite_vs_sqlite_no_diff(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            left_db = root / "left.db"
            right_db = root / "right.db"
            self._seed_basic_data(left_db, owner="alice")
            self._seed_basic_data(right_db, owner="alice")

            script = Path(__file__).resolve().parents[1] / "tools" / "reconcile_db_report_summary.py"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--left-backend",
                    "sqlite",
                    "--left-sqlite-path",
                    str(left_db),
                    "--right-backend",
                    "sqlite",
                    "--right-sqlite-path",
                    str(right_db),
                    "--owner",
                    "alice",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            payload = json.loads(completed.stdout)
            self.assertTrue(bool(payload.get("ok")))
            self.assertEqual(int(payload.get("diffCount", 0)), 0)

    def test_sqlite_vs_sqlite_detects_diff(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            left_db = root / "left.db"
            right_db = root / "right.db"
            self._seed_basic_data(left_db, owner="alice")
            SQLiteStore(right_db).initialize()

            script = Path(__file__).resolve().parents[1] / "tools" / "reconcile_db_report_summary.py"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--left-backend",
                    "sqlite",
                    "--left-sqlite-path",
                    str(left_db),
                    "--right-backend",
                    "sqlite",
                    "--right-sqlite-path",
                    str(right_db),
                    "--owner",
                    "alice",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertNotEqual(completed.returncode, 0)
            payload = json.loads(completed.stdout)
            self.assertFalse(bool(payload.get("ok")))
            self.assertGreater(int(payload.get("diffCount", 0)), 0)

    def test_allow_diff_returns_zero(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            left_db = root / "left.db"
            right_db = root / "right.db"
            self._seed_basic_data(left_db, owner="alice")
            SQLiteStore(right_db).initialize()

            script = Path(__file__).resolve().parents[1] / "tools" / "reconcile_db_report_summary.py"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    "--left-backend",
                    "sqlite",
                    "--left-sqlite-path",
                    str(left_db),
                    "--right-backend",
                    "sqlite",
                    "--right-sqlite-path",
                    str(right_db),
                    "--owner",
                    "alice",
                    "--allow-diff",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            payload = json.loads(completed.stdout)
            self.assertFalse(bool(payload.get("ok")))
            self.assertGreater(int(payload.get("diffCount", 0)), 0)


if __name__ == "__main__":
    unittest.main()
