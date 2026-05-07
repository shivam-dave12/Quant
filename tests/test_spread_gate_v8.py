import os
import sys
import types
import unittest
from types import SimpleNamespace

os.environ.setdefault("DELTA_API_KEY", "test")
os.environ.setdefault("DELTA_SECRET_KEY", "test")
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "")
os.environ.setdefault("TELEGRAM_CHAT_ID", "")

socketio_stub = types.SimpleNamespace(Client=lambda *a, **kw: SimpleNamespace())
sys.modules.setdefault("socketio", socketio_stub)

from core.instruments import AssetClass
from strategy.quant_strategy import QuantStrategy


class _DM:
    def __init__(self, bid, ask):
        self.bid = bid
        self.ask = ask

    def get_orderbook(self):
        return {"bids": [[self.bid, 1]], "asks": [[self.ask, 1]]}

    def get_last_price(self):
        return (self.bid + self.ask) / 2.0


class SpreadGateV8Tests(unittest.TestCase):
    def _strategy(self, asset_class, tick_size=0.1, asset_id="TEST", atr=0.2):
        qs = object.__new__(QuantStrategy)
        qs._atr_5m = SimpleNamespace(atr=atr)
        qs._instrument = SimpleNamespace(
            asset_class=asset_class,
            asset_id=asset_id,
            tick_size=tick_size,
        )
        qs._asset_id = asset_id
        qs._active_spread_cost_mult = 1.0
        qs._last_spread_gate_context = {}
        return qs

    def test_xstock_normal_coarse_tick_spread_is_haircut_not_block(self):
        qs = self._strategy(AssetClass.EQUITY, tick_size=0.1, asset_id="AAPL", atr=0.20)
        ok, ratio = qs._spread_atr_gate(_DM(278.80, 279.20))  # $0.40 spread, 4 ticks

        self.assertTrue(ok)
        self.assertGreater(ratio, 0.5)
        self.assertLess(qs._active_spread_cost_mult, 1.0)
        self.assertFalse(qs._last_spread_gate_context["hard_fail"])

    def test_crypto_same_spread_atr_remains_hard_blocked(self):
        qs = self._strategy(AssetClass.CRYPTO, tick_size=0.1, asset_id="BTC", atr=0.20)
        ok, ratio = qs._spread_atr_gate(_DM(100.00, 100.40))

        self.assertFalse(ok)
        self.assertGreater(ratio, 0.5)

    def test_xstock_extreme_spread_is_still_hard_blocked(self):
        qs = self._strategy(AssetClass.EQUITY, tick_size=0.1, asset_id="META", atr=0.20)
        ok, ratio = qs._spread_atr_gate(_DM(100.00, 101.00))  # 100 bps, 10 ticks, 5 ATR

        self.assertFalse(ok)
        self.assertGreater(ratio, 4.0)
        self.assertTrue(qs._last_spread_gate_context["hard_fail"])


if __name__ == "__main__":
    unittest.main()
