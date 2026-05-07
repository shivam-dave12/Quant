
import inspect
import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

os.environ.setdefault("DELTA_API_KEY", "dummy")
os.environ.setdefault("DELTA_SECRET_KEY", "dummy")


class InstitutionalPatchV58Tests(unittest.TestCase):
    def test_config_import_does_not_require_live_credentials(self):
        import config
        self.assertTrue(hasattr(config, "validate_exchange_credentials"))
        self.assertEqual(config.PORTFOLIO_MAX_OPEN_POSITIONS, 6)

    def test_delta_bracket_is_post_only_and_missing_trigger_fails(self):
        from execution.order_manager import OrderManager
        src = inspect.getsource(OrderManager.place_bracket_limit_entry)
        self.assertIn('default="maker"', src)
        self.assertIn('return False\n                    return abs(actual - expected)', src)
        from execution.order_manager import _DeltaAdapter
        src2 = inspect.getsource(_DeltaAdapter.place_bracket_limit_entry)
        self.assertIn('post_only                 = True', src2)

    def test_strategy_no_btc_contract_step_leak_in_sizing(self):
        from strategy.quant_strategy import QuantStrategy
        src = inspect.getsource(QuantStrategy._compute_quantity)
        self.assertIn("Instrument-native lot step only", src)
        self.assertNotIn("DELTA_CONTRACT_VALUE_BTC", src.split("Instrument-native lot step only", 1)[1].split("min_qty", 1)[0])

    def test_position_state_keeps_full_entry_signal(self):
        from strategy.quant_strategy import PositionState
        pos = PositionState(entry_signal_full=SimpleNamespace(entry_price=123.0))
        self.assertEqual(pos.entry_signal_full.entry_price, 123.0)

    def test_entry_engine_push_filters_consumed_pools(self):
        from strategy.entry_engine import EntryEngine
        e = EntryEngine()
        consumed = SimpleNamespace(pool=SimpleNamespace(price=99.5, status="CONSUMED", significance=9.0))
        live = SimpleNamespace(pool=SimpleNamespace(price=99.4, status="ACTIVE", significance=4.0))
        e._last_liq_snapshot = SimpleNamespace(ssl_pools=[consumed, live], bsl_pools=[])
        sl = e._push_sl_behind_pools(99.0, "long", 100.0, 2.0)
        # consumed 99.5 must be ignored; live 99.4 is valid and pushes SL behind it by 0.25ATR.
        self.assertLess(sl, 99.4 - 0.30)


if __name__ == "__main__":
    unittest.main()
