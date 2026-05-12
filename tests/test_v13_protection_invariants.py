import inspect
import unittest
from types import SimpleNamespace


class V13ProtectionInvariantTests(unittest.TestCase):
    def test_open_orders_are_product_strict_for_multi_asset_delta(self):
        from execution.order_manager import OrderManager
        from core.instruments import ExchangeName

        class API:
            def get_product_id(self, symbol):
                return {"COINXUSD": 125551, "BTCUSD": 27}.get(symbol)
            def get_open_orders(self, symbol=None):
                return {"success": True, "result": [
                    {"order_id": "btc-sl", "type": "STOP_MARKET", "trigger_price": 79134.5, "side": "BUY", "product_id": 27, "product_symbol": "BTCUSD"},
                    {"order_id": "coin-sl", "type": "STOP_MARKET", "trigger_price": 199.61, "side": "BUY", "product_id": 125551, "product_symbol": "COINXUSD"},
                ]}

        ei = SimpleNamespace(symbol="COINXUSD", display_symbol="COINXUSD", tick_size=0.01,
                             lot_step=0.01, min_qty=0.01, max_qty=1000,
                             contract_value_btc=1, product_id=125551, asset_class="equity")
        inst = SimpleNamespace(by_exchange={ExchangeName.DELTA: ei})
        om = OrderManager(API(), exchange_name="delta", instrument=inst)
        orders = om.get_open_orders(symbol="COINXUSD")
        ids = {o["order_id"] for o in orders}
        self.assertIn("coin-sl", ids)
        self.assertNotIn("btc-sl", ids)

    def test_bracket_child_parser_has_price_and_product_guards(self):
        from execution.order_manager import OrderManager
        src = inspect.getsource(OrderManager.place_bracket_limit_entry)
        self.assertIn("product_mismatch", src)
        self.assertIn("sl_price_mismatch", src)
        self.assertIn("tp_price_mismatch", src)
        self.assertIn("_bracket_children_missing", src)

    def test_strategy_flattens_and_alerts_when_bracket_children_missing(self):
        from strategy.quant_strategy import QuantStrategy
        src = inspect.getsource(QuantStrategy._enter_trade)
        self.assertIn("PROTECTION FAILURE — POSITION FLATTENED", src)
        self.assertIn("_bracket_children_missing", src)
        self.assertIn("place_market_order", src)
        self.assertIn("event_type=\"protection_failure\"", src)


if __name__ == "__main__":
    unittest.main()
