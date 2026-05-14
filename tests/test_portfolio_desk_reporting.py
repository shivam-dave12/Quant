import unittest
from types import SimpleNamespace


def _instrument(asset_id, symbol, asset_class):
    from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument

    ei = ExchangeInstrument(
        exchange=ExchangeName.DELTA,
        symbol=symbol,
        ws_symbol=symbol,
        display_symbol=symbol,
        asset_id=asset_id,
        asset_class=asset_class,
        tick_size=0.01,
        lot_step=1.0,
        min_qty=1.0,
        max_qty=1000.0,
    )
    return TradableInstrument(
        asset_id=asset_id,
        display_name=asset_id,
        asset_class=asset_class,
        primary_exchange=ExchangeName.DELTA,
        by_exchange={ExchangeName.DELTA: ei},
    )


class _Strategy:
    def __init__(self, history):
        self._trade_history = list(history)

    def get_position(self):
        return None


class _Data:
    def get_last_price(self):
        return 0.0


class PortfolioDeskReportingTests(unittest.TestCase):
    def _bot(self):
        from core.instruments import AssetClass
        from orchestration.multi_asset_bot import MultiAssetQuantBot

        bot = object.__new__(MultiAssetQuantBot)
        bot.guard = SimpleNamespace(max_open_positions=4, budget_mode="balanced")
        bot.contexts = [
            SimpleNamespace(
                instrument=_instrument("BTC", "BTCUSD", AssetClass.CRYPTO),
                strategy=_Strategy([
                    {
                        "timestamp": 1000.0,
                        "side": "long",
                        "entry": 100.0,
                        "exit": 110.0,
                        "pnl": 10.0,
                        "gross_pnl": 12.0,
                        "total_fees": 2.0,
                        "r": 1.25,
                        "reason": "target_fill",
                    }
                ]),
                data_manager=_Data(),
                phase_name="FLAT",
            ),
            SimpleNamespace(
                instrument=_instrument("PAXG", "PAXGUSD", AssetClass.COMMODITY),
                strategy=_Strategy([
                    {
                        "timestamp": 2000.0,
                        "side": "short",
                        "entry": 200.0,
                        "exit": 205.0,
                        "pnl": -5.0,
                        "gross_pnl": -4.0,
                        "total_fees": 1.0,
                        "achieved_r": -0.75,
                        "reason": "stop_fill",
                    }
                ]),
                data_manager=_Data(),
                phase_name="FLAT",
            ),
            SimpleNamespace(
                instrument=_instrument("AAPL", "AAPLXUSD", AssetClass.EQUITY),
                strategy=_Strategy([]),
                data_manager=_Data(),
                phase_name="FLAT",
            ),
        ]
        return bot

    def test_trade_records_are_normalised_by_desk(self):
        bot = self._bot()
        records = sorted(bot._all_trade_records(), key=lambda r: r["asset"])

        self.assertEqual(records[0]["desk"], "BTC")
        self.assertEqual(records[0]["timestamp"], 1000.0)
        self.assertEqual(records[0]["pnl"], 10.0)
        self.assertEqual(records[1]["desk"], "COMMODITIES")
        self.assertEqual(records[1]["pnl"], -5.0)
        for record in records:
            self.assertEqual(1, sum(1 for key in record if key == "pnl"))
            self.assertEqual(1, sum(1 for key in record if key == "timestamp"))

    def test_pnl_report_groups_by_desk_and_asset(self):
        report = self._bot().format_portfolio_pnl_report()

        self.assertIn("Desk PnL", report)
        self.assertIn("BTC Desk", report)
        self.assertIn("Commodities", report)
        self.assertIn("Stocks Desk", report)
        self.assertIn("Asset PnL", report)
        self.assertIn("BTC", report)
        self.assertIn("PAXG", report)
        self.assertIn("AAPL", report)
        self.assertIn("$+5.00", report)
        self.assertIn("WR  50.0%", report)

    def test_trades_report_shows_all_recent_newest_first(self):
        report = self._bot().format_portfolio_trades_report()

        self.assertIn("Showing 2/2", report)
        self.assertIn("PAXG", report)
        self.assertIn("BTC", report)
        self.assertLess(report.index("PAXG"), report.index("BTC"))
        self.assertIn("net     $-5.00", report)
        self.assertIn("fees $    1.00", report)


if __name__ == "__main__":
    unittest.main()
