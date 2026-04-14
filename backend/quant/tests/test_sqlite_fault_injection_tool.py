import json
import subprocess
import sys
import unittest
from pathlib import Path


class SQLiteFaultInjectionToolTests(unittest.TestCase):
    def test_fault_injection_tool_runs_all_scenarios(self):
        script = Path(__file__).resolve().parents[1] / "tools" / "sqlite_fault_injection.py"
        completed = subprocess.run(
            [
                sys.executable,
                str(script),
                "--jitter-writes",
                "5",
                "--jitter-sleep-ms",
                "5",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        payload = json.loads(completed.stdout)
        self.assertTrue(bool(payload.get("ok")))
        scenarios = payload.get("scenarios") or []
        self.assertEqual(len(scenarios), 3)
        by_name = {str(item.get("name")): item for item in scenarios}
        self.assertSetEqual(set(by_name.keys()), {"lock_conflict", "disk_full_simulated", "io_jitter"})
        self.assertTrue(bool(by_name["lock_conflict"].get("ok")))
        self.assertTrue(bool(by_name["disk_full_simulated"].get("ok")))
        self.assertTrue(bool(by_name["io_jitter"].get("ok")))


if __name__ == "__main__":
    unittest.main()
