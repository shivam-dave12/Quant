
import unittest

from execution.instrument_registry import InstrumentRegistry
from core.instruments import AssetClass, AssetIntent, ExchangeName


class FakeDelta:
    def get_products(self, contract_types=None):
        return {"success": True, "result": [
            {"symbol": "BTCUSD", "id": 27, "underlying_asset": {"symbol": "BTC"}, "quoting_asset": {"symbol": "USD"}, "max_leverage": 200},
            {"symbol": "SPXUSD", "id": 55661, "underlying_asset": {"symbol": "SPX"}, "quoting_asset": {"symbol": "USD"}, "max_leverage": 20},
            {"symbol": "AAPLXUSD", "id": 1001, "underlying_asset": {"symbol": "AAPLX"}, "quoting_asset": {"symbol": "USD"}, "product_specs": {"max_leverage": 25}},
            {"symbol": "SPYXUSD", "id": 1002, "underlying_asset": {"symbol": "SPYX"}, "quoting_asset": {"symbol": "USD"}, "product_specs": {"max_leverage": 25}},
            {"symbol": "QQQXUSD", "id": 1003, "underlying_asset": {"symbol": "QQQX"}, "quoting_asset": {"symbol": "USD"}, "product_specs": {"max_leverage": 25}},
            {"symbol": "COINXUSD", "id": 1004, "underlying_asset": {"symbol": "COINX"}, "quoting_asset": {"symbol": "USD"}, "product_specs": {"max_leverage": 25}},
            {"symbol": "CRCLXUSD", "id": 1005, "underlying_asset": {"symbol": "CRCLX"}, "quoting_asset": {"symbol": "USD"}, "product_specs": {"max_leverage": 25}},
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

    def test_coinswitch_ticker_field_names_are_not_symbols(self):
        reg = InstrumentRegistry(execution_preference="delta")
        rows = reg._augment_coinswitch_from_requested({}, FakeCoinSwitch(), [
            AssetIntent("BTC", "Bitcoin", AssetClass.CRYPTO, ("BTCUSDT",), priority=0)
        ])
        self.assertIn("BTCUSDT", rows)
        self.assertNotIn("LOWPRICE24H", rows)

    def test_xstock_screenshot_symbols_match(self):
        reg = InstrumentRegistry(execution_preference="delta")
        report = reg.discover(FakeDelta(), None, requested=[
            {"asset_id": "SPY", "display_name": "SP500 xStock token derivative", "asset_class": "equity", "aliases": ["SPYXUSD"], "priority": 0},
            {"asset_id": "QQQ", "display_name": "Nasdaq xStock token derivative", "asset_class": "equity", "aliases": ["QQQXUSD"], "priority": 1},
            {"asset_id": "COIN", "display_name": "Coinbase xStock token derivative", "asset_class": "equity", "aliases": ["COINXUSD"], "priority": 2},
            {"asset_id": "CRCL", "display_name": "Circle xStock token derivative", "asset_class": "equity", "aliases": ["CRCLXUSD"], "priority": 3},
        ], max_active=10)
        syms = {x.primary.symbol for x in report.matched}
        self.assertEqual({"SPYXUSD", "QQQXUSD", "COINXUSD", "CRCLXUSD"}, syms)
        self.assertTrue(all(x.max_leverage == 25 for x in report.matched))


if __name__ == "__main__":
    unittest.main()
