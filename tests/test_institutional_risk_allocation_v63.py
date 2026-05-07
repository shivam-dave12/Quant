import unittest
from types import SimpleNamespace

from core.instruments import AssetClass, ExchangeName, ExchangeInstrument, TradableInstrument, instrument_scope
from core.market_policy import active_policy
from orchestration.portfolio_manager import PortfolioManager


def inst(asset_id, ac, symbol, max_lev=25, tick=0.1, lot=1.0):
    ei = ExchangeInstrument(
        exchange=ExchangeName.DELTA, symbol=symbol, ws_symbol=symbol, display_symbol=symbol,
        asset_id=asset_id, asset_class=ac, max_leverage=max_lev, tick_size=tick, lot_step=lot, min_qty=lot,
    )
    return TradableInstrument(asset_id, asset_id, ac, ExchangeName.DELTA, {ExchangeName.DELTA: ei})


class Ctx:
    def __init__(self, instrument, has_position=False, pos=None):
        self.instrument = instrument
        self._has_position = has_position
        self.position = pos
        self.phase_name = 'ACTIVE' if has_position else 'FLAT'

    @property
    def has_position(self):
        return self._has_position


class InstitutionalRiskAllocationV63Test(unittest.TestCase):
    def test_ticker_policy_is_not_asset_class_flat(self):
        btc = inst('BTC', AssetClass.CRYPTO, 'BTCUSD', max_lev=40, tick=0.5, lot=0.001)
        gold = inst('GOLD', AssetClass.COMMODITY, 'PAXGUSD')
        silver = inst('SILVER', AssetClass.COMMODITY, 'SLVONUSD')
        nvda = inst('NVDA', AssetClass.EQUITY, 'NVDAXUSD')
        crcl = inst('CRCL', AssetClass.EQUITY, 'CRCLXUSD')

        with instrument_scope(btc): btc_pol = active_policy()
        with instrument_scope(gold): gold_pol = active_policy()
        with instrument_scope(silver): silver_pol = active_policy()
        with instrument_scope(nvda): nvda_pol = active_policy()
        with instrument_scope(crcl): crcl_pol = active_policy()

        self.assertGreater(btc_pol.portfolio_weight, gold_pol.portfolio_weight)
        self.assertNotEqual(gold_pol.risk_multiplier, silver_pol.risk_multiplier)
        self.assertGreater(nvda_pol.risk_multiplier, crcl_pol.risk_multiplier)
        self.assertGreater(nvda_pol.margin_pct, crcl_pol.margin_pct)
        self.assertLess(crcl_pol.max_trade_risk_pct, nvda_pol.max_trade_risk_pct)

    def test_institutional_portfolio_allocation_is_weighted_not_equal_split(self):
        btc = Ctx(inst('BTC', AssetClass.CRYPTO, 'BTCUSD', max_lev=40, tick=0.5, lot=0.001))
        nvda = Ctx(inst('NVDA', AssetClass.EQUITY, 'NVDAXUSD'))
        crcl = Ctx(inst('CRCL', AssetClass.EQUITY, 'CRCLXUSD'))
        ctxs = [btc, nvda, crcl]
        mgr = PortfolioManager()
        mgr.budget_mode = 'institutional_risk_parity'
        mgr.max_open_positions = 6

        raw = {'available': 1000.0, 'total': 1000.0}
        b_btc = mgr.allocate_balance(btc, ctxs, raw)
        b_nvda = mgr.allocate_balance(nvda, ctxs, raw)
        b_crcl = mgr.allocate_balance(crcl, ctxs, raw)

        self.assertGreater(b_btc['available'], b_nvda['available'])
        self.assertGreater(b_nvda['available'], b_crcl['available'])
        self.assertGreater(b_btc['portfolio_cash_share'], b_crcl['portfolio_cash_share'])
        self.assertEqual(b_nvda['instrument_policy']['asset_id'], 'NVDA')
        self.assertGreater(b_nvda['portfolio_elastic_cash_budget_usd'], b_nvda['portfolio_normal_cash_budget_usd'])

    def test_equal_slots_mode_remains_backward_compatible(self):
        btc = Ctx(inst('BTC', AssetClass.CRYPTO, 'BTCUSD', max_lev=40, tick=0.5, lot=0.001))
        mgr = PortfolioManager()
        mgr.budget_mode = 'equal_slots'
        mgr.max_open_positions = 4
        b = mgr.allocate_balance(btc, [btc], {'available': 100.0, 'total': 100.0})
        self.assertAlmostEqual(b['available'], 25.0)
        self.assertAlmostEqual(b['risk_available'], 100.0)

    def test_open_margin_and_risk_reduce_remaining_portfolio_budgets(self):
        btc_i = inst('BTC', AssetClass.CRYPTO, 'BTCUSD', max_lev=40, tick=0.5, lot=0.001)
        nvda_i = inst('NVDA', AssetClass.EQUITY, 'NVDAXUSD')
        pos = SimpleNamespace(quantity=0.01, entry_price=80000.0, sl_price=79500.0, initial_risk=5.0)
        btc = Ctx(btc_i, has_position=True, pos=pos)
        nvda = Ctx(nvda_i)
        mgr = PortfolioManager()
        mgr.budget_mode = 'institutional_risk_parity'
        raw = {'available': 1000.0, 'total': 1000.0}
        b = mgr.allocate_balance(nvda, [btc, nvda], raw)
        self.assertAlmostEqual(b['portfolio_open_risk_usd'], 5.0)
        self.assertLess(b['portfolio_remaining_risk_budget_usd'], b['portfolio_max_risk_budget_usd'])
        self.assertGreater(b['portfolio_open_margin_usd'], 0.0)


if __name__ == '__main__':
    unittest.main()
