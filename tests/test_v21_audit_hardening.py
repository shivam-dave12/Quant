import inspect
import sys
import types
import unittest
from types import SimpleNamespace

_socketio = types.ModuleType("socketio")
_socketio.Client = object
sys.modules.setdefault("socketio", _socketio)


class V21AuditHardeningTests(unittest.TestCase):
    def test_delta_position_without_identity_is_not_adopted_by_every_asset(self):
        from execution.order_manager import _DeltaAdapter

        class Api:
            def get_product_id(self, symbol):
                return 123 if symbol == "COINXUSD" else 999

        inst = SimpleNamespace(
            symbol="COINXUSD",
            display_symbol="COINXUSD",
            tick_size=0.01,
            lot_step=1,
            min_qty=1,
            max_qty=1000,
            contract_value_btc=1,
            product_id=123,
            asset_class="equity",
        )
        adapter = _DeltaAdapter(Api(), exchange_instrument=inst)
        adopted = adapter.normalise_position({"size": 1, "side": "long", "entry_price": 200.0})
        self.assertEqual(adopted["side"], None)
        self.assertEqual(adopted["size"], 0.0)

    def test_delta_position_product_id_match_is_adopted_without_symbol(self):
        from execution.order_manager import _DeltaAdapter

        class Api:
            def get_product_id(self, symbol):
                return 123

        inst = SimpleNamespace(
            symbol="COINXUSD",
            display_symbol="COINXUSD",
            tick_size=0.01,
            lot_step=1,
            min_qty=1,
            max_qty=1000,
            contract_value_btc=1,
            product_id=123,
            asset_class="equity",
        )
        adapter = _DeltaAdapter(Api(), exchange_instrument=inst)
        adopted = adapter.normalise_position({"product_id": 123, "size": 2, "side": "long", "entry_price": 200.0})
        self.assertEqual(adopted["side"], "LONG")
        self.assertEqual(adopted["size"], 2.0)

    def test_defer_signal_emits_dashboard_event(self):
        from strategy.quant_strategy import QuantStrategy

        src = inspect.getsource(QuantStrategy._defer_entry_signal)
        self.assertIn('"type": "candidate_deferred"', src)
        self.assertIn('cooldown_sec', src)

    def test_multi_asset_initialise_emits_universe_after_context_build(self):
        from orchestration.multi_asset_bot import MultiAssetQuantBot

        src = inspect.getsource(MultiAssetQuantBot.initialize)
        self.assertIn("self._dashboard_emit_universe()", src)

    def test_user_facing_quantity_label_is_not_hardcoded_btc(self):
        from strategy.quant_strategy import QuantStrategy
        from pathlib import Path

        src = inspect.getsource(QuantStrategy._enter_trade)
        self.assertIn("_contract_qty_label", src)
        controller = Path("telegram/controller.py").read_text()
        self.assertNotIn("Qty: {qty:.4f} BTC", controller)


if __name__ == "__main__":
    unittest.main()
