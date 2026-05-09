import unittest

import config
from strategy.quant_strategy import PositionState, QuantStrategy


class _DummyInstrument:
    def __init__(self, asset_id: str, symbol: str) -> None:
        self.asset_id = asset_id
        self.display_symbol = symbol
        self.canonical_symbol = symbol


def _strategy(asset_id: str, symbol: str) -> QuantStrategy:
    qs = object.__new__(QuantStrategy)
    qs._asset_id = asset_id
    qs._instrument = _DummyInstrument(asset_id, symbol)
    return qs


class InstitutionalRegressionTests(unittest.TestCase):
    def setUp(self):
        self._old_exchange = getattr(config, "EXECUTION_EXCHANGE", "")
        self._old_delta_symbol = getattr(config, "DELTA_SYMBOL", "")
        self._old_delta_fee = getattr(config, "DELTA_COMMISSION_RATE", 0.00050)
        self._old_delta_maker = getattr(config, "DELTA_COMMISSION_RATE_MAKER", -0.00020)

    def tearDown(self):
        config.EXECUTION_EXCHANGE = self._old_exchange
        config.DELTA_SYMBOL = self._old_delta_symbol
        config.DELTA_COMMISSION_RATE = self._old_delta_fee
        config.DELTA_COMMISSION_RATE_MAKER = self._old_delta_maker

    def test_config_import_is_safe_without_live_credentials(self):
        self.assertTrue(hasattr(config, "REQUIRE_EXCHANGE_CREDENTIALS"))

    def test_delta_xstock_is_linear_pnl_even_when_global_delta_symbol_is_btc(self):
        config.EXECUTION_EXCHANGE = "delta"
        config.DELTA_SYMBOL = "BTCUSD"
        config.DELTA_COMMISSION_RATE = 0.00050
        qs = _strategy("AAPL", "AAPLXUSD")
        self.assertTrue(qs._is_delta_execution())
        self.assertFalse(qs._is_inverse_pnl_contract())

        pos = PositionState(side="long", quantity=2.0, entry_price=100.0)
        pnl = qs._estimate_pnl(pos, 110.0, entry_fill_type="taker")
        expected = (110.0 - 100.0) * 2.0 - (100.0 * 2.0 * 0.00050) - (110.0 * 2.0 * 0.00050)
        self.assertAlmostEqual(pnl, expected, places=8)

    def test_delta_btc_contract_keeps_inverse_pnl_geometry(self):
        config.EXECUTION_EXCHANGE = "delta"
        config.DELTA_SYMBOL = "BTCUSD"
        qs = _strategy("BTC", "BTCUSD")
        self.assertTrue(qs._is_delta_execution())
        self.assertTrue(qs._is_inverse_pnl_contract())


if __name__ == "__main__":
    unittest.main()

class DashboardTelemetryRegressionTests(unittest.TestCase):
    def test_log_tail_parses_tp_audit_and_quant_wait(self):
        from dashboard.agents.log_tail_agent import parse

        tp_line = '{"log":"10:03:25.611 | INFO | [BTC|DELTA:BTCUSD] RAW_TP_AUDIT: TP/SHORT: no eligible TP pool; best visible pool rejected"}'
        tp = parse(tp_line)
        self.assertEqual(tp["type"], "tp_audit")
        self.assertEqual(tp["asset"], "BTC")

        wait_line = '{"log":"10:23:33.536 | INFO | [AMZN|DELTA:AMZNXUSD] POST-SWEEP QUANT WAIT: REVERSAL LONG | p=0.746 min=0.734 EV=0.598 LLR=1.08 U=0.49 REJECT quant posterior auction"}'
        wait = parse(wait_line)
        self.assertEqual(wait["type"], "candidate_deferred")
        self.assertEqual(wait["phase"], "POST_SWEEP_QUANT_WAIT")
        self.assertEqual(wait["asset"], "AMZN")
        self.assertAlmostEqual(wait["posterior"], 0.746)
