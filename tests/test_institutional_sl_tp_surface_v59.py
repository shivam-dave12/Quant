import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("DELTA_API_KEY", "dummy")
os.environ.setdefault("DELTA_SECRET_KEY", "dummy")


class InstitutionalSLTPSurfaceTests(unittest.TestCase):
    def _target(self, price, sig=6.0, tf="15m", status="DETECTED"):
        return SimpleNamespace(
            pool=SimpleNamespace(
                price=price,
                status=status,
                timeframe=tf,
                touches=1,
                ob_aligned=False,
                fvg_aligned=False,
                significance=sig,
            ),
            significance=sig,
            distance_atr=abs(100.0 - price),
            direction="short" if price < 100 else "long",
            tf_sources=[tf],
        )

    def test_sl_selector_places_stop_outside_cluster_not_middle(self):
        from strategy.liquidity_pool_selector import score_sl_pool
        snap = SimpleNamespace(
            ssl_pools=[
                self._target(99.35, 5.0),
                self._target(99.10, 7.0),
                self._target(98.92, 6.0),
            ],
            bsl_pools=[],
        )
        pick = score_sl_pool(
            snap=snap,
            side="long",
            entry=100.0,
            atr=1.0,
            invalidation_price=99.55,
        )
        self.assertIsNotNone(pick)
        self.assertLess(pick.sl_price, 98.92 - 0.25)
        self.assertGreaterEqual(pick.zone_size, 3)
        self.assertIn("cluster-envelope stop", " ".join(pick.reasons))

    def test_sl_push_uses_zone_buffer_not_fixed_tick_behind_one_pool(self):
        from strategy.entry_engine import EntryEngine
        e = EntryEngine()
        e._last_liq_snapshot = SimpleNamespace(
            ssl_pools=[
                self._target(99.40, 6.0),
                self._target(99.15, 6.0),
                self._target(98.95, 6.0),
            ],
            bsl_pools=[],
        )
        sl = e._push_sl_behind_pools(99.20, "long", 100.0, 1.0)
        self.assertLess(sl, 98.95 - 0.25)


if __name__ == "__main__":
    unittest.main()
