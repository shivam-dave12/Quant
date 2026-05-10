
import sys
import types
import unittest
from types import SimpleNamespace

socketio = types.ModuleType("socketio")
socketio.Client = object
sys.modules.setdefault("socketio", socketio)


class TestV26OpsIntegrity(unittest.TestCase):
    def test_log_tail_does_not_mark_hold_as_exchange_trail_update(self):
        from dashboard.agents.log_tail_agent import parse
        line = '{"log":"22:03:25.584 | INFO | 🧠 POSTERIOR    | [SILVER|DELTA:SLVONUSD] InstitutionalTrail PAYOFF_HOLD [AGGRESSIVE] SL=$66.6 current=$67.2 | net_lock=0.0pts"}'
        ev = parse(line)
        self.assertEqual(ev["type"], "trail_hold")

    def test_log_tail_dispatch_is_not_exchange_trail_update(self):
        from dashboard.agents.log_tail_agent import parse
        line = '{"log":"21:55:44.655 | INFO | 🧠 POSTERIOR    | [GOLD|DELTA:PAXGUSD] 🏦 InstitutionalTrail SL dispatch [STOP-LIMIT] trigger=$4,554.0 side=buy qty=0.029 phase=BE_LOCK"}'
        ev = parse(line)
        self.assertEqual(ev["type"], "trail_dispatch")

    def test_log_tail_exchange_applied_is_trail_update(self):
        from dashboard.agents.log_tail_agent import parse
        line = '{"log":"22:03:25.584 | INFO | 🧠 POSTERIOR    | [SILVER|DELTA:SLVONUSD] ✅ InstitutionalTrail EXCHANGE_APPLIED old=$67.20 new=$66.60 order=abc phase=AGGRESSIVE"}'
        ev = parse(line)
        self.assertEqual(ev["type"], "trail_update")
        self.assertAlmostEqual(ev["sl"], 66.60)

    def test_risk_record_trade_non_btc_quantity_unit(self):
        from risk.risk_manager import RiskManager
        rm = RiskManager(shared_api=None)
        inst = SimpleNamespace(asset_id="NVDA", asset_class="equity")
        with self.assertLogs("risk.risk_manager", level="INFO") as cm:
            rm.record_trade("long", 100.0, 101.0, 2.0, "unit", pnl_override=1.0, instrument=inst, leverage=25)
        text = "\n".join(cm.output)
        self.assertIn("contracts", text)
        self.assertNotIn("BTC |", text)


    def test_risk_record_trade_without_override_uses_generic_quantity_units(self):
        from risk.risk_manager import RiskManager
        rm = RiskManager(shared_api=None)
        inst = SimpleNamespace(asset_id="NVDA", asset_class="equity")
        rm.record_trade("long", 100.0, 101.0, 2.0, "unit_no_override", instrument=inst, leverage=25)
        self.assertAlmostEqual(rm.realized_pnl, 2.0 - ((100.0 + 101.0) * 2.0 * 0.00055), places=6)

    def test_multiasset_has_portfolio_command_reports(self):
        from orchestration.multi_asset_bot import MultiAssetQuantBot
        for name in ["format_portfolio_status_report", "format_portfolio_market_report", "format_portfolio_risk_report", "format_portfolio_sl_tp_report"]:
            self.assertTrue(hasattr(MultiAssetQuantBot, name), name)


if __name__ == "__main__":
    unittest.main()
