
import unittest
from types import SimpleNamespace


class TestV18StructuralStopInvariant(unittest.TestCase):
    def _target(self, price, significance=7.4, status="DETECTED"):
        return SimpleNamespace(
            pool=SimpleNamespace(
                price=price,
                status=status,
                timeframe="5m",
                touches=4,
                ob_aligned=False,
                fvg_aligned=False,
            ),
            significance=significance,
            distance_atr=0.0,
            tf_sources=["5m"],
        )

    def test_short_sl_in_front_of_bsl_is_rejected(self):
        from strategy.entry_engine import EntryEngine

        engine = EntryEngine()
        snap = SimpleNamespace(
            bsl_pools=[self._target(4544.7, 7.4)],
            ssl_pools=[],
        )
        sl, reason = engine._apply_institutional_sl_envelope(
            snap=snap,
            side="short",
            price=4535.1,
            atr=4.0,
            structural_sl=4538.5,
            invalidation_price=4538.5,
            label="unit",
            min_risk=0.0,
        )
        self.assertIsNone(sl)
        self.assertTrue("protective liquidity" in reason and ("reprice/abstain" in reason or "trade must abstain" in reason), reason)

    def test_long_sl_in_front_of_ssl_is_rejected(self):
        from strategy.entry_engine import EntryEngine

        engine = EntryEngine()
        snap = SimpleNamespace(
            ssl_pools=[self._target(99.0, 6.0)],
            bsl_pools=[],
        )
        sl, reason = engine._apply_institutional_sl_envelope(
            snap=snap,
            side="long",
            price=100.0,
            atr=1.0,
            structural_sl=99.4,
            invalidation_price=99.4,
            label="unit",
            min_risk=0.0,
        )
        self.assertIsNone(sl)
        self.assertTrue("protective liquidity" in reason and ("reprice/abstain" in reason or "trade must abstain" in reason), reason)


if __name__ == "__main__":
    unittest.main()
