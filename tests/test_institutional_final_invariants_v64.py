import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("DELTA_API_KEY", "dummy")
os.environ.setdefault("DELTA_SECRET_KEY", "dummy")


class _QD:
    def __init__(self, accept=True, posterior=0.82, ev=0.72, llr=2.2):
        self.accept = accept
        self.posterior = posterior
        self.min_posterior = 0.60
        self.expected_value = ev
        self.llr = llr
        self.uncertainty = 0.20
        self.reason = "ACCEPT test posterior"
        self.components = {"test": 1.0}

    def compact(self):
        return f"p={self.posterior:.3f} EV={self.expected_value:.3f} LLR={self.llr:.2f}"


class InstitutionalFinalInvariantsV64Tests(unittest.TestCase):
    def test_wrong_side_sl_is_repaired_not_deferred(self):
        from strategy.entry_engine import EntryEngine
        e = EntryEngine()
        snap = SimpleNamespace(ssl_pools=[], bsl_pools=[])
        sl, reason = e._apply_institutional_sl_envelope(
            snap=snap,
            side="long",
            price=100.0,
            atr=1.0,
            structural_sl=101.25,     # invalid for a long
            invalidation_price=99.40,
            label="unit-test",
            min_risk=0.75,
        )
        self.assertIsNotNone(sl, reason)
        self.assertLess(sl, 100.0)
        self.assertTrue(e._sl_before_liquidation("long", sl, 100.0))

        sl2, reason2 = e._apply_institutional_sl_envelope(
            snap=snap,
            side="short",
            price=100.0,
            atr=1.0,
            structural_sl=98.75,      # invalid for a short
            invalidation_price=100.65,
            label="unit-test",
            min_risk=0.75,
        )
        self.assertIsNotNone(sl2, reason2)
        self.assertGreater(sl2, 100.0)
        self.assertTrue(e._sl_before_liquidation("short", sl2, 100.0))

    def test_no_legacy_post_sweep_threshold_fallback_left(self):
        import inspect
        import strategy.entry_engine as ee
        src = inspect.getsource(ee.EntryEngine._evaluate_evidence)
        self.assertIn("No legacy score-threshold fallback", src)
        self.assertNotIn("if rev_total >= threshold and gap >= gap_min", src)
        self.assertNotIn("elif cont_total >= threshold", src)

    def test_adverse_selection_is_continuous_utility_not_hard_veto(self):
        import strategy.entry_engine as ee
        from strategy.liquidity_map import LiquidityPool, PoolSide, SweepResult

        old_eval = ee.evaluate_post_sweep_quant
        try:
            ee.evaluate_post_sweep_quant = lambda **kwargs: _QD(
                accept=(kwargs.get("action") == "reverse"), posterior=0.86, ev=0.82, llr=2.4)
            engine = ee.EntryEngine()
            engine._institutional_entry_quality_gate = lambda *a, **k: (True, "ok")
            pool = LiquidityPool(price=100.0, side=PoolSide.SSL, timeframe="1m")
            sweep = SweepResult(
                pool=pool,
                sweep_candle_idx=1,
                wick_extreme=99.5,
                rejection_pct=0.60,
                volume_ratio=1.5,
                quality=0.72,
                direction="long",
                detected_at=1_700_000_000,
            )
            now = 1_700_000_300.0
            ps = ee._PostSweepState(
                sweep=sweep,
                entered_at=now - 240.0,
                highest_since=118.0,
                lowest_since=99.5,
                static_scored=True,
                static_rev_base=2.0,
                static_cont_base=1.0,
                max_displacement=18.0,
            )
            d = engine._evaluate_evidence(
                ps,
                snap=SimpleNamespace(bsl_pools=[], ssl_pools=[]),
                flow=ee.OrderFlowState(tick_flow=0.0, cvd_trend=0.0),
                ict=ee.ICTContext(),
                price=118.0,
                atr=1.0,
                now=now,
            )
            self.assertEqual(d.action, "reverse")
            self.assertIn("adverse_selection_mult", engine._last_sweep_analysis)
            self.assertLessEqual(engine._last_sweep_analysis["adverse_selection_mult"], 1.0)
        finally:
            ee.evaluate_post_sweep_quant = old_eval

    def test_frontier_tp_gets_path_credit_and_scale_fraction(self):
        from strategy.liquidity_pool_selector import select_tp_with_report
        entry = 100.0
        sl = 98.0
        atr = 1.0
        pool = SimpleNamespace(
            price=126.0,
            timeframe="1d",
            side="BSL",
            significance=10.0,
            htf_count=3,
            touches=1,
            status="ACTIVE",
            created_at=1_700_000_000,
            ob_aligned=True,
            fvg_aligned=True,
        )
        target = SimpleNamespace(
            pool=pool,
            distance_atr=26.0,
            significance=10.0,
            direction="long",
            tf_sources=["1d", "4h"],
        )
        snap = SimpleNamespace(bsl_pools=[target], ssl_pools=[], feed_reliability=0.90)
        tp, chosen, score, report = select_tp_with_report(
            snap=snap,
            side="long",
            entry=entry,
            sl=sl,
            atr=atr,
            min_rr=2.2,
            posterior_prob=0.78,
            now=1_700_000_000,
        )
        self.assertIsNotNone(tp, report.as_dict()["summary"])
        self.assertIs(chosen, target)
        self.assertGreater(score.components.get("path_credit", 0.0), 0.0)
        self.assertGreaterEqual(score.components.get("scale_fraction", 0.0), 0.28)
        self.assertIn("scale≈", " ".join(score.reasons))


if __name__ == "__main__":
    unittest.main()
