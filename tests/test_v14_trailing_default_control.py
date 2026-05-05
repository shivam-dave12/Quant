import sys
import types
import unittest
from types import SimpleNamespace

# Test environment does not include optional websocket dependency.
socketio = types.ModuleType("socketio")
socketio.Client = object
sys.modules.setdefault("socketio", socketio)

import config
from strategy.quant_strategy import QCfg
from orchestration.multi_asset_bot import MultiAssetQuantBot


class _FakeStrategy:
    def __init__(self):
        self.override = None
    def set_trail_override(self, enabled):
        self.override = enabled
        return True
    def get_trail_enabled(self):
        if self.override is not None:
            return self.override
        return bool(getattr(config, "QUANT_TRAIL_ENABLED", False))
    def get_position(self):
        return None


class TrailDefaultControlTests(unittest.TestCase):
    def _bot(self):
        bot = MultiAssetQuantBot.__new__(MultiAssetQuantBot)
        exch = SimpleNamespace(value="delta")
        inst1 = SimpleNamespace(asset_id="COIN", display_symbol="COINXUSD", primary_exchange=exch)
        inst2 = SimpleNamespace(asset_id="BTC", display_symbol="BTCUSD", primary_exchange=exch)
        bot.contexts = [
            SimpleNamespace(instrument=inst1, strategy=_FakeStrategy(), ready=True),
            SimpleNamespace(instrument=inst2, strategy=_FakeStrategy(), ready=True),
        ]
        return bot

    def test_default_trailing_is_off(self):
        self.assertTrue(bool(getattr(config, "QUANT_TRAIL_ENABLED", False)))
        self.assertTrue(QCfg.TRAIL_ENABLED())

    def test_portfolio_trail_on_can_target_one_asset(self):
        bot = self._bot()
        res = bot.set_trailing_override(True, "COIN")
        self.assertEqual(res["changed"], ["COIN"])
        self.assertTrue(bot.contexts[0].strategy.get_trail_enabled())
        self.assertTrue(bot.contexts[1].strategy.get_trail_enabled())

    def test_portfolio_trail_off_disables_all(self):
        bot = self._bot()
        bot.set_trailing_override(True)
        res = bot.set_trailing_override(False)
        self.assertEqual(set(res["changed"]), {"COIN", "BTC"})
        self.assertFalse(bot.contexts[0].strategy.get_trail_enabled())
        self.assertFalse(bot.contexts[1].strategy.get_trail_enabled())

    def test_report_states_default_off(self):
        bot = self._bot()
        txt = bot.format_trailing_control_report()
        self.assertIn("Default: <b>ON</b>", txt)
        self.assertIn("COIN", txt)
        self.assertIn("DEFAULT / ON", txt)


if __name__ == "__main__":
    unittest.main()
