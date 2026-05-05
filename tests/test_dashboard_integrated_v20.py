
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dashboard" / "backend"))

from state_store import DashboardState
from telemetry.dashboard_emitter import DashboardEmitter, instrument_fields, policy_fields, position_fields


class DashboardIntegratedV20Tests(unittest.TestCase):
    def test_direct_event_populates_dashboard_state(self):
        st = DashboardState()
        st.apply({"type":"catalog_asset", "asset":"COIN", "venue":"DELTA", "symbol":"COINXUSD", "price":200, "policy":"equity"})
        st.apply({"type":"candidate_approved", "asset":"COIN", "venue":"DELTA", "symbol":"COINXUSD", "side":"SHORT", "entry":198, "sl":202, "tp":190, "rr":2.0, "posterior":0.81})
        st.apply({"type":"position_opened", "asset":"COIN", "venue":"DELTA", "symbol":"COINXUSD", "side":"SHORT", "entry":198, "price":198, "sl":202, "tp":190, "qty":1, "bracket":"VERIFIED"})
        snap = st.snapshot()
        self.assertEqual(snap["system"]["assets"], 1)
        self.assertEqual(len(snap["positions"]), 1)
        self.assertEqual(snap["positions"][0]["bracket"], "VERIFIED")
        self.assertEqual(snap["decisions"][0]["kind"], "candidate_approved")

    def test_emitter_is_non_blocking_when_disabled(self):
        em = DashboardEmitter(enabled=False)
        self.assertFalse(em.emit({"type":"heartbeat"}))

    def test_position_fields_from_context(self):
        inst = SimpleNamespace(asset_id="BTC", display_symbol="BTCUSD", primary_exchange=SimpleNamespace(value="delta"))
        pos = SimpleNamespace(side="long", entry_price=100.0, quantity=2.0, sl_price=95.0, tp_price=115.0, initial_sl_dist=5.0, peak_profit=10.0, trail_active=True, sl_order_id="s", tp_order_id="t")
        strat = SimpleNamespace(get_position=lambda: pos)
        dm = SimpleNamespace(get_last_price=lambda: 105.0)
        ctx = SimpleNamespace(instrument=inst, strategy=strat, data_manager=dm, phase_name="ACTIVE")
        payload = position_fields(ctx)
        self.assertEqual(payload["type"], "position_update")
        self.assertEqual(payload["asset"], "BTC")
        self.assertAlmostEqual(payload["upnl"], 10.0)
        self.assertAlmostEqual(payload["r"], 1.0)
        self.assertEqual(payload["bracket"], "VERIFIED")
    def test_from_config_returns_single_shared_emitter(self):
        from telemetry.dashboard_emitter import DashboardEmitter
        old = DashboardEmitter._shared
        try:
            DashboardEmitter._shared = None
            a = DashboardEmitter.from_config()
            b = DashboardEmitter.from_config()
            self.assertIs(a, b)
        finally:
            try:
                if DashboardEmitter._shared is not None:
                    DashboardEmitter._shared.stop()
            finally:
                DashboardEmitter._shared = old

