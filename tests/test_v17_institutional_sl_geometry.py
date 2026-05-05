
import unittest
from types import SimpleNamespace


class TestV17InstitutionalSLGeometry(unittest.TestCase):
    def _target(self, price, significance=8.0, touches=4):
        return SimpleNamespace(
            pool=SimpleNamespace(
                price=price,
                status="DETECTED",
                timeframe="5m",
                touches=touches,
                ob_aligned=False,
                fvg_aligned=False,
            ),
            significance=significance,
            distance_atr=abs(100.0 - price),
            tf_sources=["5m"],
        )

    def test_sl_selector_does_not_place_stop_at_liquidity_pool(self):
        from strategy.liquidity_pool_selector import score_sl_pool

        snap = SimpleNamespace(ssl_pools=[self._target(99.0, 8.0, 4)], bsl_pools=[])
        pick = score_sl_pool(
            snap,
            side="long",
            entry=100.0,
            atr=1.0,
            invalidation_price=99.5,
            min_risk=0.0,
        )
        self.assertIsNotNone(pick)
        # Institutional stop must be meaningfully behind the pool, not 0.20-0.30 ATR away.
        self.assertLessEqual(pick.sl_price, 98.25)

    def test_final_pool_push_clears_nearby_liquidity_band(self):
        from strategy.entry_engine import EntryEngine

        engine = EntryEngine()
        engine._last_liq_snapshot = SimpleNamespace(
            ssl_pools=[self._target(99.0, 7.0, 3)],
            bsl_pools=[],
        )
        # Old behavior left 98.76 effectively at the pool.  New behavior pushes to <=98.25.
        sl = engine._push_sl_behind_pools(98.76, "long", 100.0, 1.0)
        self.assertLessEqual(sl, 98.25)


if __name__ == "__main__":
    unittest.main()
