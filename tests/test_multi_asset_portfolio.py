import sys
import types
import unittest
from types import SimpleNamespace

# The portfolio guard tests do not exercise network/websocket clients.
# Some minimal production images used for CI do not install python-socketio.
socketio_stub = types.SimpleNamespace(Client=lambda *a, **kw: SimpleNamespace())
sys.modules.setdefault("socketio", socketio_stub)

from core.instruments import AssetClass
from orchestration.multi_asset_bot import PortfolioGuard


class _Ctx:
    def __init__(self, asset_id, asset_class, has_position=False, phase="FLAT"):
        self.instrument = SimpleNamespace(asset_id=asset_id, asset_class=asset_class)
        self._has_position = has_position
        self.phase_name = phase

    @property
    def has_position(self):
        return self._has_position


class MultiAssetPortfolioTests(unittest.TestCase):
    def test_one_position_per_contract_but_multiple_contracts_allowed(self):
        guard = PortfolioGuard()
        guard.max_open_positions = 4
        guard.max_same_class = 4
        guard.max_per_contract = 1

        btc = _Ctx("BTC", AssetClass.CRYPTO, has_position=True, phase="ACTIVE")
        eth = _Ctx("ETH", AssetClass.CRYPTO, has_position=False)
        btc_second = _Ctx("BTC", AssetClass.CRYPTO, has_position=False)
        contexts = [btc, eth, btc_second]

        allowed_eth, reason_eth = guard.can_evaluate_entry(eth, contexts)
        allowed_btc2, reason_btc2 = guard.can_evaluate_entry(btc_second, contexts)

        self.assertTrue(allowed_eth, reason_eth)
        self.assertFalse(allowed_btc2)
        self.assertIn("contract slot occupied", reason_btc2)

    def test_equal_slot_balance_allocation_preserves_portfolio_budget(self):
        guard = PortfolioGuard()
        guard.max_open_positions = 4
        guard.budget_mode = "equal_slots"
        ctx = _Ctx("BTC", AssetClass.CRYPTO)
        raw = {"available": 100.0, "total": 100.0}

        scoped = guard.allocate_balance(ctx, [ctx], raw)

        self.assertTrue(scoped["portfolio_scoped"])
        self.assertEqual(scoped["portfolio_slot_count"], 4)
        self.assertAlmostEqual(scoped["available"], 25.0)
        self.assertAlmostEqual(scoped["total"], 25.0)
        self.assertAlmostEqual(scoped["available_raw"], 100.0)
        self.assertAlmostEqual(scoped["risk_available"], 100.0)
        self.assertAlmostEqual(scoped["risk_total"], 100.0)


if __name__ == "__main__":
    unittest.main()
