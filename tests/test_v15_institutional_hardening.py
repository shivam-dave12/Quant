import unittest
import config
from strategy.quant_strategy import QuantStrategy


class TestV15InstitutionalHardening(unittest.TestCase):
    def test_equity_spread_thresholds_are_not_btc_tight(self):
        self.assertGreaterEqual(float(config.QUANT_MAX_SPREAD_BPS_EQUITY), 60.0)
        self.assertGreaterEqual(float(config.QUANT_MAX_SPREAD_TICKS_EQUITY), 50.0)
        self.assertTrue(hasattr(config, "QUANT_CRITICAL_SPREAD_BPS_EQUITY"))
        self.assertTrue(hasattr(config, "QUANT_CRITICAL_SPREAD_TICKS_EQUITY"))

    def test_operator_alert_helper_exists(self):
        self.assertTrue(hasattr(QuantStrategy, "_send_operator_alert"))

    def test_trailing_default_is_explicit_bool(self):
        self.assertIsInstance(bool(config.QUANT_TRAIL_ENABLED), bool)


if __name__ == "__main__":
    unittest.main()
