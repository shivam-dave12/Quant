from types import SimpleNamespace
import unittest

from strategy.liquidity_pool_selector import select_tp_with_report


class InstitutionalTPFrontierTests(unittest.TestCase):
    def _target(self, *, price, entry, atr, timeframe="1d", side="BSL", significance=10.0,
                status="ACTIVE", ob=True, fvg=True):
        pool = SimpleNamespace(
            price=price,
            timeframe=timeframe,
            side=side,
            significance=significance,
            htf_count=3,
            touches=1,
            status=status,
            created_at=1_700_000_000,
            ob_aligned=ob,
            fvg_aligned=fvg,
        )
        return SimpleNamespace(
            pool=pool,
            distance_atr=abs(price - entry) / atr,
            significance=significance,
            direction="long" if side == "BSL" else "short",
            tf_sources=[timeframe, "4h"],
        )

    def test_reasonable_far_frontier_tp_can_be_executable(self):
        entry = 100.0
        sl = 98.0
        atr = 1.0
        far = self._target(price=126.0, entry=entry, atr=atr, timeframe="1d", side="BSL")
        snap = SimpleNamespace(bsl_pools=[far], ssl_pools=[], feed_reliability=0.90)

        tp, target, score, report = select_tp_with_report(
            snap=snap,
            side="long",
            entry=entry,
            sl=sl,
            atr=atr,
            min_rr=2.20,
            posterior_prob=0.78,
            now=1_700_000_000,
        )

        self.assertIsNotNone(tp)
        self.assertIs(target, far)
        self.assertIsNotNone(score)
        self.assertEqual(score.components.get("delivery_model"), "frontier")
        self.assertGreater(score.components.get("delivery_prob", 0.0), score.sweep_prob)
        self.assertGreater(score.rr, 10.0)
        payload = report.as_dict()
        self.assertIn("deliveryP", payload["summary"])
        self.assertTrue(payload["selected"]["selected"])

    def test_far_frontier_can_beat_shallow_near_pool_when_path_ev_is_higher(self):
        entry = 100.0
        sl = 98.0
        atr = 1.0
        near = self._target(price=104.2, entry=entry, atr=atr, timeframe="15m", side="BSL", significance=7.0, ob=False, fvg=False)
        far = self._target(price=126.0, entry=entry, atr=atr, timeframe="1d", side="BSL", significance=10.0)
        snap = SimpleNamespace(bsl_pools=[near, far], ssl_pools=[], feed_reliability=0.90)

        tp, target, score, _report = select_tp_with_report(
            snap=snap,
            side="long",
            entry=entry,
            sl=sl,
            atr=atr,
            min_rr=2.20,
            posterior_prob=0.80,
            now=1_700_000_000,
        )

        self.assertIsNotNone(tp)
        self.assertIs(target, far)
        self.assertGreater(score.components.get("selection_ev", 0.0), 0.0)
        self.assertIn("frontier", " ".join(score.reasons))

    def test_lottery_far_tp_still_rejected(self):
        entry = 100.0
        sl = 98.0
        atr = 1.0
        lottery = self._target(price=190.0, entry=entry, atr=atr, timeframe="1d", side="BSL", significance=12.0)
        snap = SimpleNamespace(bsl_pools=[lottery], ssl_pools=[], feed_reliability=0.90)

        tp, target, score, report = select_tp_with_report(
            snap=snap,
            side="long",
            entry=entry,
            sl=sl,
            atr=atr,
            min_rr=2.20,
            posterior_prob=0.90,
            now=1_700_000_000,
        )

        self.assertIsNone(tp)
        self.assertIsNone(target)
        self.assertIsNone(score)
        self.assertIn("beyond executable frontier", report.as_dict()["summary"])


if __name__ == "__main__":
    unittest.main()
