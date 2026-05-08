import unittest
from types import SimpleNamespace

from core.redaction import redact_sensitive
from dashboard.agents.log_tail_agent import parse
from dashboard.backend.state_store import DashboardState


class TelemetryIntegrityV27Tests(unittest.TestCase):
    def test_dashboard_redacts_sensitive_payloads(self):
        st = DashboardState()
        st.apply({
            "type": "alert",
            "severity": "critical",
            "title": "leak test",
            "message": "api_key=abc123 secret_key=xyz private_key=0x" + "a"*64,
        })
        snap = st.snapshot()
        msg = snap["alerts"][0]["message"]
        self.assertIn("<REDACTED>", msg)
        self.assertNotIn("abc123", msg)
        self.assertNotIn("xyz", msg)
        self.assertNotIn("a"*64, msg)

    def test_log_tail_adopted_position_becomes_position_update(self):
        line = '{"log":"20:11:02.753 | WARN | 🧠 POSTERIOR    | [SILVER|DELTA:SLVONUSD] ⚡ RECONCILE: adopted SHORT @ $66.74"}'
        ev = parse(line)
        self.assertEqual(ev["type"], "position_update")
        self.assertEqual(ev["asset"], "SILVER")
        self.assertEqual(ev["side"], "SHORT")
        self.assertEqual(ev["entry"], 66.74)
        self.assertEqual(ev["bracket"], "ADOPTED")

    def test_trail_hold_does_not_mutate_position_sl(self):
        st = DashboardState()
        st.apply({"type": "position_update", "asset": "SILVER", "venue": "DELTA", "symbol": "SLVONUSD", "side": "SHORT", "entry": 66.74, "sl": 67.2, "tp": 65.5, "price": 66.7})
        st.apply({"type": "trail_hold", "asset": "SILVER", "venue": "DELTA", "symbol": "SLVONUSD", "sl": 66.6, "message": "PAYOFF_HOLD"})
        snap = st.snapshot()
        self.assertEqual(snap["positions"][0]["sl"], 67.2)
        self.assertEqual(snap["decisions"][0]["kind"], "trail_hold")

    def test_trail_update_mutates_position_sl_only_when_confirmed(self):
        st = DashboardState()
        st.apply({"type": "position_update", "asset": "GOLD", "venue": "DELTA", "symbol": "PAXGUSD", "side": "SHORT", "entry": 4570, "sl": 4580, "tp": 4550, "price": 4560})
        st.apply({"type": "trail_update", "asset": "GOLD", "venue": "DELTA", "symbol": "PAXGUSD", "sl": 4554, "trailing": "ON"})
        snap = st.snapshot()
        self.assertEqual(snap["positions"][0]["sl"], 4554)
        self.assertEqual(snap["positions"][0]["trailing"], "ON")

    def test_dashboard_emitter_is_process_shared(self):
        from telemetry.dashboard_emitter import DashboardEmitter
        DashboardEmitter._shared = None
        a = DashboardEmitter.from_config()
        b = DashboardEmitter.from_config()
        self.assertIs(a, b)


if __name__ == "__main__":
    unittest.main()
