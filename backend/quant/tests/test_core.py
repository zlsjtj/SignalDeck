import unittest

import pandas as pd

from main import build_order_intents
from statarb.factors import compute_scores
from statarb.paper import PaperAccount


class CoreLogicTests(unittest.TestCase):
    def test_paper_account_applies_fee_and_positions(self):
        acct = PaperAccount(cash=1000.0, fee_bps=10.0)
        acct.apply_fills([{"symbol": "BTC/USDT:USDT", "side": "buy", "amount": 1.0, "price": 100.0}])
        self.assertAlmostEqual(acct.cash, 899.9, places=8)
        self.assertAlmostEqual(acct.positions["BTC/USDT:USDT"], 1.0, places=8)

        acct.apply_fills([{"symbol": "BTC/USDT:USDT", "side": "sell", "amount": 0.5, "price": 120.0}])
        self.assertAlmostEqual(acct.cash, 959.84, places=8)
        self.assertAlmostEqual(acct.positions["BTC/USDT:USDT"], 0.5, places=8)

    def test_build_order_intents_handles_flip(self):
        symbols = ["BTC/USDT:USDT"]
        current_w = {"BTC/USDT:USDT": 0.2}
        target_w = {"BTC/USDT:USDT": -0.1}
        intents = build_order_intents(symbols, current_w, target_w, drift_threshold=0.0, force_rebalance=False)
        self.assertEqual(len(intents), 2)
        self.assertTrue(intents[0]["reduce_only"])
        self.assertEqual(intents[0]["position_side"], "LONG")
        self.assertAlmostEqual(float(intents[0]["delta_w"]), -0.2, places=8)
        self.assertFalse(intents[1]["reduce_only"])
        self.assertEqual(intents[1]["position_side"], "SHORT")
        self.assertAlmostEqual(float(intents[1]["delta_w"]), -0.1, places=8)

    def test_factor_weights_change_scores(self):
        idx = pd.date_range("2024-01-01", periods=80, freq="h", tz="UTC")
        data = {
            "A": pd.DataFrame({"close": [100 + i * 0.4 for i in range(80)], "volume": [1000 + i * 2 for i in range(80)]}, index=idx),
            "B": pd.DataFrame({"close": [100 - i * 0.2 for i in range(80)], "volume": [900 + i for i in range(80)]}, index=idx),
            "C": pd.DataFrame({"close": [100 + (i % 5) * 0.1 for i in range(80)], "volume": [1200 - i for i in range(80)]}, index=idx),
        }

        s_mom = compute_scores(
            data,
            w_reversal=0.0,
            w_momentum=1.0,
            w_trend=0.0,
            w_flow=0.0,
            w_volz=0.0,
            w_volume=0.0,
            lookback=24,
            mom_lookback=12,
            trend_lookback=24,
            flow_lookback=24,
            vol_lookback=12,
            volume_lookback=24,
        )
        s_rev = compute_scores(
            data,
            w_reversal=1.0,
            w_momentum=0.0,
            w_trend=0.0,
            w_flow=0.0,
            w_volz=0.0,
            w_volume=0.0,
            lookback=24,
            mom_lookback=12,
            trend_lookback=24,
            flow_lookback=24,
            vol_lookback=12,
            volume_lookback=24,
        )
        self.assertFalse(s_mom.equals(s_rev))
        self.assertNotEqual(list(s_mom.sort_values().index), list(s_rev.sort_values().index))


if __name__ == "__main__":
    unittest.main()
