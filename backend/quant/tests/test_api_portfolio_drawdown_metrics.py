import tempfile
import unittest
from pathlib import Path

import api_server


class ApiPortfolioDrawdownMetricsTests(unittest.TestCase):
    def setUp(self):
        self._orig_strategy_status = api_server._strategy_status
        self._orig_build_live_fills_payload = api_server._build_live_fills_payload
        api_server._strategy_status = lambda strategy_id, log_limit=0: {"running": False}
        api_server._build_live_fills_payload = lambda strategy_id=None: []

    def tearDown(self):
        api_server._strategy_status = self._orig_strategy_status
        api_server._build_live_fills_payload = self._orig_build_live_fills_payload

    def test_portfolio_max_drawdown_comes_from_paper_equity_csv_even_when_stopped(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = Path(tmp_dir) / "paper_equity.csv"
            csv_path.write_text(
                "\n".join(
                    [
                        "ts_utc,equity,cash",
                        "2026-01-01T00:00:00+00:00,100,100",
                        "2026-01-01T00:01:00+00:00,120,120",
                        "2026-01-01T00:02:00+00:00,90,90",
                        "2026-01-01T00:03:00+00:00,95,95",
                    ]
                ),
                encoding="utf-8",
            )

            payload = api_server._build_portfolio_response(path=str(csv_path), strategy_id="strategy_1")
            self.assertAlmostEqual(float(payload.get("maxDrawdown", 0.0)), 0.25, places=6)
            self.assertEqual(bool(payload.get("running")), False)
            self.assertEqual(bool(payload.get("stale")), True)
            self.assertEqual(len(payload.get("equityCurve") or []), 4)

    def test_portfolio_max_drawdown_ignores_invalid_equity_rows(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = Path(tmp_dir) / "paper_equity.csv"
            csv_path.write_text(
                "\n".join(
                    [
                        "ts_utc,equity,cash",
                        "2026-01-01T00:00:00+00:00,100,100",
                        "2026-01-01T00:01:00+00:00,120,120",
                        "2026-01-01T00:01:30+00:00,not-a-number,95",
                        "2026-01-01T00:02:00+00:00,90,90",
                        "2026-01-01T00:03:00+00:00,95,95",
                    ]
                ),
                encoding="utf-8",
            )

            payload = api_server._build_portfolio_response(path=str(csv_path), strategy_id="strategy_2")
            self.assertAlmostEqual(float(payload.get("maxDrawdown", 0.0)), 0.25, places=6)
            self.assertEqual(len(payload.get("equityCurve") or []), 4)

    def test_portfolio_max_drawdown_ignores_transient_single_tick_drop_spike(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = Path(tmp_dir) / "paper_equity.csv"
            csv_path.write_text(
                "\n".join(
                    [
                        "ts_utc,equity,cash",
                        "2026-01-01T00:00:00+00:00,100,100",
                        "2026-01-01T00:01:00+00:00,110,110",
                        "2026-01-01T00:02:00+00:00,108,108",
                        "2026-01-01T00:03:00+00:00,30,30",
                        "2026-01-01T00:04:00+00:00,109,109",
                        "2026-01-01T00:05:00+00:00,112,112",
                    ]
                ),
                encoding="utf-8",
            )

            payload = api_server._build_portfolio_response(path=str(csv_path), strategy_id="strategy_3")
            self.assertAlmostEqual(float(payload.get("maxDrawdown", 0.0)), 0.01818181818181818, places=6)

    def test_portfolio_max_drawdown_ignores_transient_single_tick_rise_spike(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = Path(tmp_dir) / "paper_equity.csv"
            csv_path.write_text(
                "\n".join(
                    [
                        "ts_utc,equity,cash",
                        "2026-01-01T00:00:00+00:00,100,100",
                        "2026-01-01T00:01:00+00:00,111,111",
                        "2026-01-01T00:02:00+00:00,112,112",
                        "2026-01-01T00:03:00+00:00,170,170",
                        "2026-01-01T00:04:00+00:00,113,113",
                        "2026-01-01T00:05:00+00:00,111,111",
                    ]
                ),
                encoding="utf-8",
            )

            payload = api_server._build_portfolio_response(path=str(csv_path), strategy_id="strategy_4")
            self.assertAlmostEqual(float(payload.get("maxDrawdown", 0.0)), 0.017699115044247787, places=6)

    def test_portfolio_max_drawdown_keeps_sustained_losses(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            csv_path = Path(tmp_dir) / "paper_equity.csv"
            csv_path.write_text(
                "\n".join(
                    [
                        "ts_utc,equity,cash",
                        "2026-01-01T00:00:00+00:00,100,100",
                        "2026-01-01T00:01:00+00:00,120,120",
                        "2026-01-01T00:02:00+00:00,90,90",
                        "2026-01-01T00:03:00+00:00,89,89",
                        "2026-01-01T00:04:00+00:00,88,88",
                        "2026-01-01T00:05:00+00:00,87,87",
                    ]
                ),
                encoding="utf-8",
            )

            payload = api_server._build_portfolio_response(path=str(csv_path), strategy_id="strategy_5")
            self.assertAlmostEqual(float(payload.get("maxDrawdown", 0.0)), 0.275, places=6)


if __name__ == "__main__":
    unittest.main()
