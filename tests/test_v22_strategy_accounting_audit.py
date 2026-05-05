import inspect
import os
import unittest

os.environ.setdefault("DELTA_API_KEY", "test")
os.environ.setdefault("DELTA_SECRET_KEY", "test")
from types import SimpleNamespace


class TestV22StrategyAccountingAudit(unittest.TestCase):
    def test_core_pnl_uses_generic_quantity_units(self):
        from core.pnl import gross_pnl_usd
        self.assertEqual(gross_pnl_usd('LONG', 100, 105, quantity_units=2), 10)
        # Backward-compatible old kw still works, but new code is not BTC-specific.
        self.assertEqual(gross_pnl_usd('SHORT', 100, 95, quantity_btc=3), 15)

    def test_risk_manager_record_trade_accepts_instrument_and_leverage(self):
        from risk.risk_manager import RiskManager
        sig = inspect.signature(RiskManager.record_trade)
        self.assertIn('instrument', sig.parameters)
        self.assertIn('leverage', sig.parameters)
        src = inspect.getsource(RiskManager.record_trade)
        self.assertIn('qty_unit', src)
        self.assertNotIn('Qty: {quantity:.4f} BTC', src)

    def test_strategy_passes_instrument_and_leverage_to_risk_manager(self):
        from strategy.quant_strategy import QuantStrategy
        src = inspect.getsource(QuantStrategy._record_exchange_exit)
        self.assertIn('instrument   = getattr(self, "_instrument", None)', src)
        self.assertIn('leverage     = QCfg.LEVERAGE()', src)

    def test_delta_step_is_btc_only_not_global(self):
        from strategy.quant_strategy import QuantStrategy
        src = inspect.getsource(QuantStrategy._compute_quantity)
        self.assertIn('Only BTC inverse products should inherit DELTA_CONTRACT_VALUE_BTC', src)
        self.assertIn('ac == "crypto"', src)
        self.assertIn('asset_id == "BTC" or "BTC" in sym', src)

    def test_dead_v9_display_shim_removed(self):
        import pathlib
        self.assertFalse(pathlib.Path('strategy/v9_display.py').exists())


class TestV22ProtectionAudit(unittest.TestCase):
    def test_reconcile_unprotected_adoption_alerts_operator(self):
        from strategy.quant_strategy import QuantStrategy
        src = inspect.getsource(QuantStrategy._reconcile_apply)
        self.assertIn('ADOPTED POSITION HAS NO EXCHANGE SL', src)
        self.assertIn('emergency reduce-only flatten submitted', src)
        self.assertIn('ADOPTED POSITION MISSING TP', src)

    def test_incomplete_bracket_child_ids_alert_operator(self):
        from strategy.quant_strategy import QuantStrategy
        src = inspect.getsource(QuantStrategy._enter_trade)
        self.assertIn('BRACKET CHILD IDS INCOMPLETE', src)
        self.assertIn('strategy will not assume full exchange protection', src)

if __name__ == '__main__':
    unittest.main()
