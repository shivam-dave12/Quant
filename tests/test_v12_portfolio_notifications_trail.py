
import unittest
from types import SimpleNamespace

class V12PortfolioNotificationTrailTests(unittest.TestCase):
    def test_delta_edit_order_passes_product_id(self):
        from execution.order_manager import _DeltaAdapter
        class API:
            def __init__(self): self.kw = None
            def edit_order(self, **kw):
                self.kw = kw
                return {"success": True, "result": {"id": kw["order_id"], "state": "open"}}
        api = API()
        inst = SimpleNamespace(symbol="PAXGUSD", display_symbol="PAXGUSD", tick_size=0.01, lot_step=1, min_qty=1, max_qty=100, contract_value_btc=1, product_id=123006, asset_class="commodity")
        ad = _DeltaAdapter(api, exchange_instrument=inst)
        res = ad.edit_order("999", 4554.5, 4555.0)
        self.assertEqual(api.kw.get("product_id"), 123006)
        self.assertFalse(res.get("_error"))

    def test_order_manager_uses_instrument_tick(self):
        from execution.order_manager import OrderManager
        from core.instruments import ExchangeName
        class API: pass
        ei = SimpleNamespace(symbol="AAPLXUSD", display_symbol="AAPLXUSD", tick_size=0.01, lot_step=1, min_qty=1, max_qty=100, contract_value_btc=1, product_id=1, asset_class="equity")
        inst = SimpleNamespace(by_exchange={ExchangeName.DELTA: ei})
        om = OrderManager(API(), exchange_name="delta", instrument=inst)
        self.assertEqual(om._active_tick_size(), 0.01)

    def test_multi_asset_has_command_center_reports(self):
        from pathlib import Path
        src = Path("orchestration/multi_asset_bot.py").read_text()
        self.assertIn("def format_portfolio_pnl_report", src)
        self.assertIn("def format_portfolio_position_report", src)
        self.assertIn("def format_portfolio_equity_report", src)
        self.assertIn("PORTFOLIO", src)

if __name__ == "__main__":
    unittest.main()
