
import unittest

from execution.instrument_registry import InstrumentRegistry
from core.instruments import AssetClass, AssetIntent, ExchangeName


class FakeDelta:
    def get_products(self, contract_types=None):
        return {"success": True, "result": [
            {"symbol": "BTCUSD", "id": 27, "underlying_asset": {"symbol": "BTC"}, "quoting_asset": {"symbol": "USD"}, "max_leverage": 200},
            {"symbol": "SPXUSD", "id": 55661, "underlying_asset": {"symbol": "SPX"}, "quoting_asset": {"symbol": "USD"}, "max_leverage": 20},
            {"symbol": "AAPLXUSD", "id": 1001, "underlying_asset": {"symbol": "AAPLX"}, "quoting_asset": {"symbol": "USD"}, "max_leverage": 25},
        ]}


class FakeCoinSwitch:
    def get_instrument_info(self, exchange="EXCHANGE_2"):
        # Simulate incomplete all-instrument response. BTC should still be validated live.
        return {"data": []}

    def get_futures_ticker(self, symbol, exchange="EXCHANGE_2"):
        if symbol.upper() == "BTCUSDT":
            return {"data": {"symbol": "BTCUSDT", "last_price": "79000", "funding_rate": "0.001"}}
        return {"error": "not found"}


class InstrumentRegistryV4Tests(unittest.TestCase):
    def test_coinswitch_secondary_is_added_by_live_symbol_validation(self):
        intents = [AssetIntent("BTC", "Bitcoin", AssetClass.CRYPTO, ("BTCUSD", "BTCUSDT"), priority=0)]
        reg = InstrumentRegistry(execution_preference="delta")
        report = reg.discover(FakeDelta(), FakeCoinSwitch(), requested=[{
            "asset_id": "BTC", "display_name": "Bitcoin", "asset_class": "crypto", "aliases": ["BTCUSD", "BTCUSDT"], "priority": 0
        }])
        btc = report.matched[0]
        self.assertIn(ExchangeName.DELTA, btc.by_exchange)
        self.assertIn(ExchangeName.COINSWITCH, btc.by_exchange)
        self.assertEqual(btc.by_exchange[ExchangeName.COINSWITCH].symbol, "BTCUSDT")

    def test_spxusd_is_not_used_for_s_and_p_500_index(self):
        reg = InstrumentRegistry(execution_preference="delta")
        report = reg.discover(FakeDelta(), None, requested=[{
            "asset_id": "SPX_INDEX", "display_name": "S&P 500 index", "asset_class": "index", "aliases": ["SPX500USD", "US500", "SP500"], "priority": 0
        }])
        self.assertEqual(report.matched, [])
        self.assertIn("SPX_INDEX", report.unavailable)

    def test_xstock_exact_alias_matches_delta_symbol(self):
        reg = InstrumentRegistry(execution_preference="delta")
        report = reg.discover(FakeDelta(), None, requested=[{
            "asset_id": "AAPL", "display_name": "Apple xStock token derivative", "asset_class": "equity", "aliases": ["AAPLXUSD", "AAPL"], "priority": 0
        }])
        self.assertEqual(report.matched[0].primary.symbol, "AAPLXUSD")
        self.assertEqual(report.matched[0].max_leverage, 25)


if __name__ == "__main__":
    unittest.main()
