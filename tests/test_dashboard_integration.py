import os
import sys
import types
import unittest

_socketio = types.ModuleType("socketio")
_socketio.Client = lambda *a, **k: object()
sys.modules.setdefault("socketio", _socketio)


class DashboardIntegrationTests(unittest.TestCase):
    def test_dashboard_emitter_imports_and_disabled_send_is_safe(self):
        from telemetry.dashboard_emitter import DashboardEmitter
        emitter = DashboardEmitter(base_url="http://127.0.0.1:9", enabled=False)
        self.assertFalse(emitter.heartbeat(mode="test"))

    def test_multi_asset_bot_has_dashboard_hooks(self):
        from orchestration.multi_asset_bot import MultiAssetQuantBot
        self.assertTrue(hasattr(MultiAssetQuantBot, "_dashboard_heartbeat"))
        self.assertTrue(hasattr(MultiAssetQuantBot, "_maybe_dashboard_context_event"))
        self.assertTrue(hasattr(MultiAssetQuantBot, "_dashboard_universe_event"))


if __name__ == "__main__":
    unittest.main()
