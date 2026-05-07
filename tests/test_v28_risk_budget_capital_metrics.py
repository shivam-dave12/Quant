import unittest
from types import SimpleNamespace

import config
from risk.risk_manager import RiskManager


class V28RiskBudgetCapitalMetricsTest(unittest.TestCase):
    def test_portfolio_max_entries_is_six(self):
        self.assertEqual(int(config.PORTFOLIO_MAX_OPEN_POSITIONS), 6)
        self.assertGreaterEqual(int(config.PORTFOLIO_MAX_OPEN_PER_ASSET_CLASS), 6)

    def test_count_win_rate_does_not_hide_capital_loss(self):
        rm = RiskManager(shared_api=None)
        inst = SimpleNamespace(asset_id="TEST", display_symbol="TESTUSD", asset_class="equity", primary_exchange=SimpleNamespace(value="delta"))
        # $5 margin loser: entry 100 qty 1 leverage 20 => margin 5, pnl -2
        rm.record_trade("long", 100, 98, 1, "large_margin_loss", pnl_override=-2.0, instrument=inst, leverage=20)
        # $1 margin winner: entry 100 qty 0.2 leverage 20 => margin 1, pnl +1
        rm.record_trade("long", 100, 105, 0.2, "small_margin_win", pnl_override=1.0, instrument=inst, leverage=20)
        stats = rm.get_statistics()
        self.assertEqual(stats["count_win_rate"], 50.0)
        self.assertEqual(stats["capital_result"], "LOSS")
        self.assertLess(stats["net_margin_return_pct"], 0)
        self.assertAlmostEqual(stats["total_margin_used"], 6.0, places=6)
        self.assertAlmostEqual(stats["margin_weighted_win_rate"], (1.0 / 6.0) * 100.0, places=2)

    def test_trade_record_stores_margin_and_units(self):
        rm = RiskManager(shared_api=None)
        inst = SimpleNamespace(asset_id="NVDA", display_symbol="NVDAXUSD", asset_class="equity", primary_exchange=SimpleNamespace(value="delta"))
        rm.record_trade("short", 200, 198, 0.5, "unit", pnl_override=1.0, instrument=inst, leverage=25)
        t = rm.trade_history[-1]
        self.assertEqual(t.asset_id, "NVDA")
        self.assertEqual(t.symbol, "NVDAXUSD")
        self.assertEqual(t.qty_unit, "contracts")
        self.assertAlmostEqual(t.margin_used, 4.0)
        self.assertAlmostEqual(t.return_on_margin, 25.0)


if __name__ == "__main__":
    unittest.main()
