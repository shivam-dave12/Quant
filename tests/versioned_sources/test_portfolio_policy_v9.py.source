import unittest
from core.instruments import AssetClass, ExchangeName, ExchangeInstrument, TradableInstrument, instrument_scope
from core.market_policy import active_policy
from orchestration.portfolio_manager import PortfolioManager
from strategy.quant_strategy import QCfg


def inst(asset_id, ac, symbol, max_lev=0, tick=0.01, lot=1.0):
    ei = ExchangeInstrument(
        exchange=ExchangeName.DELTA, symbol=symbol, ws_symbol=symbol, display_symbol=symbol,
        asset_id=asset_id, asset_class=ac, max_leverage=max_lev, tick_size=tick, lot_step=lot, min_qty=lot,
    )
    return TradableInstrument(asset_id, asset_id, ac, ExchangeName.DELTA, {ExchangeName.DELTA: ei})


class PortfolioPolicyV9Test(unittest.TestCase):
    def test_xstock_policy_not_btc(self):
        aapl = inst('AAPL', AssetClass.EQUITY, 'AAPLXUSD', max_lev=25, tick=0.1, lot=1)
        with instrument_scope(aapl):
            pol = active_policy()
            self.assertEqual(QCfg.LEVERAGE(), 8)
            self.assertLess(QCfg.MARGIN_PCT(), 0.20)
            self.assertGreaterEqual(QCfg.MIN_RR_RATIO(), 1.45)
            self.assertEqual(QCfg.LOT_STEP(), 1)
            self.assertEqual(QCfg.TICK_SIZE(), 0.1)
            self.assertLess(pol.risk_multiplier, 1.0)

    def test_crypto_policy_keeps_btc_speed(self):
        btc = inst('BTC', AssetClass.CRYPTO, 'BTCUSD', max_lev=40, tick=0.5, lot=0.001)
        with instrument_scope(btc):
            pol = active_policy()
            self.assertGreaterEqual(pol.risk_multiplier, 1.0)
            self.assertLessEqual(pol.loop_interval_sec, 0.25)

    def test_portfolio_allocation_carries_policy(self):
        mgr = PortfolioManager()
        aapl = inst('AAPL', AssetClass.EQUITY, 'AAPLXUSD', max_lev=25, tick=0.1, lot=1)
        class C: pass
        c=C(); c.instrument=aapl; c.has_position=False
        b = mgr.allocate_balance(c, [c], {'available': 100.0, 'total': 100.0})
        self.assertTrue(b['portfolio_scoped'])
        self.assertIn('instrument_policy', b)
        self.assertEqual(b['instrument_policy']['asset_class'], 'equity')
        self.assertLess(b['instrument_risk_multiplier'], 1.0)

if __name__ == '__main__':
    unittest.main()
