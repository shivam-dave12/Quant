import time
import unittest
from aggregator.market_aggregator import MarketAggregator
from execution.instrument_registry import InstrumentRegistry
from core.instruments import ExchangeName
class _DM:
    is_ready=True
    def __init__(self,trades): self.trades=trades
    def get_recent_trades_raw(self): return list(self.trades)
    def get_last_price(self): return 100.0
    def get_orderbook(self): return {"bids":[[99,1]],"asks":[[101,1]]}
    def register_strategy(self,*_): pass
    def start(self): return True
    def stop(self): pass
    def wait_until_ready(self,*_): return True
class _API:
    def get_meta(self,dex=None):
        if dex=="xyz": return {"universe":[{"name":"CL","szDecimals":2,"maxLeverage":25},{"name":"NVDA","szDecimals":2,"maxLeverage":20}]}
        return {"universe":[{"name":"BTC","szDecimals":5,"maxLeverage":40}]}
    def get_all_mids(self): return {"BTC":"100"}
class V20HyperliquidInstitutionalTests(unittest.TestCase):
    def test_dual_feed_does_not_double_count_volume(self):
        now=time.time(); primary=_DM([{"price":100,"quantity":100,"side":"buy","timestamp":now-1}]); secondary=_DM([])
        agg=MarketAggregator(primary,secondary); agg._secondary_alive=True
        with agg._lock: agg._merged_trades.append({"price":100,"quantity":1000,"side":"buy","timestamp":now,"source":"secondary"})
        vd=agg.get_volume_delta(60); self.assertTrue(vd["venue_normalized"]); self.assertLess(vd["buy_volume"],110); self.assertGreater(vd["raw_by_source"]["secondary"]["buy"],900)
    def test_hyper_only_market_is_not_traded_when_execution_disabled(self):
        reg=InstrumentRegistry(execution_preference="delta")
        report=reg.discover(delta_api=None, hyperliquid_api=_API(), requested=[{"asset_id":"OIL","display_name":"WTI","asset_class":"commodity","aliases":["CL","xyz:CL"],"priority":1}], max_active=5)
        self.assertEqual(report.matched, []); self.assertIn("Hyperliquid", report.unavailable["OIL"])
    def test_delta_plus_hyper_stays_delta_primary_with_hyper_secondary(self):
        class _Delta:
            def get_products(self,*_,**__): return {"result":[{"id":27,"symbol":"BTCUSD","underlying_asset":{"symbol":"BTC"},"quoting_asset":{"symbol":"USD"},"contract_type":"perpetual_futures","tick_size":"0.5","contract_value":"0.001","max_leverage":"40"}]}
        reg=InstrumentRegistry(execution_preference="delta")
        report=reg.discover(delta_api=_Delta(), hyperliquid_api=_API(), requested=[{"asset_id":"BTC","display_name":"Bitcoin","asset_class":"crypto","aliases":["BTCUSD","BTC"],"priority":1}], max_active=5)
        self.assertEqual(len(report.matched),1); inst=report.matched[0]; self.assertEqual(inst.primary_exchange,ExchangeName.DELTA); self.assertIn(ExchangeName.HYPERLIQUID, inst.by_exchange)
if __name__ == "__main__": unittest.main()
