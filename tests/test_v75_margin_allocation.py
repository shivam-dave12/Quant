
import sys
import types
import unittest
from types import SimpleNamespace

socketio_stub = types.SimpleNamespace(Client=lambda *a, **kw: SimpleNamespace())
sys.modules.setdefault("socketio", socketio_stub)

import config
from core.instruments import AssetClass
from orchestration.multi_asset_bot import PortfolioGuard


class _Strategy:
    def __init__(self, pos=None):
        self._pos = pos
    def get_position(self):
        return self._pos


class _Ctx:
    def __init__(self, asset_id, asset_class, pos=None):
        self.instrument = SimpleNamespace(asset_id=asset_id, asset_class=asset_class)
        self.strategy = _Strategy(pos)
        self.phase_name = "ACTIVE" if pos else "FLAT"
    @property
    def has_position(self):
        return self.strategy.get_position() is not None


class V75MarginAllocationTests(unittest.TestCase):
    def test_allocator_reports_open_risk_and_remaining_aggregate_cap(self):
        guard = PortfolioGuard()
        guard.max_open_positions = 6
        raw = {"available": 200.0, "total": 200.0}
        open_ctx = _Ctx("GOLD", AssetClass.COMMODITY, {
            "entry_price": 4700.0,
            "sl_price": 4710.0,
            "quantity": 0.05,
        })
        candidate = _Ctx("SILVER", AssetClass.COMMODITY)
        scoped = guard.allocate_balance(candidate, [open_ctx, candidate], raw)
        self.assertAlmostEqual(scoped["portfolio_open_risk_usd"], 0.5, places=6)
        self.assertAlmostEqual(scoped["portfolio_aggregate_risk_cap"], 6.0, places=6)
        self.assertAlmostEqual(scoped["portfolio_remaining_risk_cap"], 5.5, places=6)
        self.assertAlmostEqual(scoped["available"], 200.0 / 6.0, places=6)
        self.assertEqual(scoped["portfolio_max_slots"], 6)


if __name__ == "__main__":
    unittest.main()
