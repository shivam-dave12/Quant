
import unittest
from core.instruments import AssetClass, ExchangeName, ExchangeInstrument, TradableInstrument, instrument_scope
from telegram.notifier import _tg_enrich_asset_message, format_periodic_report


def _inst(asset="AAPL", sym="AAPLXUSD", cls=AssetClass.EQUITY):
    ei = ExchangeInstrument(
        exchange=ExchangeName.DELTA,
        symbol=sym,
        ws_symbol=sym,
        display_symbol=sym,
        asset_id=asset,
        asset_class=cls,
        max_leverage=25,
    )
    return TradableInstrument(
        asset_id=asset,
        display_name=f"{asset} xStock",
        asset_class=cls,
        primary_exchange=ExchangeName.DELTA,
        by_exchange={ExchangeName.DELTA: ei},
    )


class AssetNotificationTests(unittest.TestCase):
    def test_asset_header_is_added(self):
        inst = _inst()
        msg = _tg_enrich_asset_message("🧠 <b>POSTERIOR ACCEPTED</b>", instrument=inst, event_type="posterior", context={"state":"SCANNING", "price":279.0})
        self.assertIn("AAPL", msg)
        self.assertIn("DELTA:AAPLXUSD", msg)
        self.assertIn("POSTERIOR", msg)
        self.assertIn("SCANNING", msg)

    def test_periodic_report_uses_asset_not_btc(self):
        inst = _inst("NVDA", "NVDAXUSD")
        with instrument_scope(inst):
            msg = format_periodic_report(current_price=198.0, atr=1.2, instrument=inst)
        self.assertIn("NVDA", msg)
        self.assertIn("NVDAXUSD", msg)
        self.assertNotIn("<code>BTC", msg)

    def test_already_scoped_message_not_double_wrapped(self):
        inst = _inst()
        raw = "🏛 <b>POSTERIOR</b>  <code>AAPL</code>\nbody"
        msg = _tg_enrich_asset_message(raw, instrument=inst, event_type="posterior")
        self.assertEqual(raw, msg)

if __name__ == "__main__":
    unittest.main()


class StartupMessagePolicyAliasTest(unittest.TestCase):
    def test_instrument_policy_exposes_evaluation_interval_alias(self):
        from core.market_policy import build_instrument_policy
        pol = build_instrument_policy(None)
        self.assertTrue(hasattr(pol, "evaluation_interval_sec"))
        self.assertEqual(pol.evaluation_interval_sec, pol.loop_interval_sec)
        self.assertIn("evaluation_interval_sec", pol.asdict())
