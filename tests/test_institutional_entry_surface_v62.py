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


class InstitutionalEntrySurfaceV62Tests(unittest.TestCase):
    def test_entry_liquidation_guard_uses_active_instrument_policy(self):
        import strategy.entry_engine as ee
        old = ee.active_policy
        try:
            ee.active_policy = lambda: SimpleNamespace(leverage=20)
            _liq, guard, room = ee.EntryEngine._liquidation_guard("short", 100.0)
            self.assertGreater(room, 3.5)
            self.assertGreater(guard, 103.5)
        finally:
            ee.active_policy = old

    def test_statistical_sl_surface_when_structural_stop_beyond_leverage_room(self):
        import strategy.entry_engine as ee
        old = ee.active_policy
        try:
            ee.active_policy = lambda: SimpleNamespace(leverage=10)
            engine = ee.EntryEngine()
            snap = SimpleNamespace(bsl_pools=[], ssl_pools=[])
            sl, reason = engine._apply_institutional_sl_envelope(
                snap=snap,
                side="long",
                price=100.0,
                atr=1.0,
                structural_sl=80.0,
                invalidation_price=79.5,
                label="unit-test",
                min_risk=1.25,
            )
            self.assertIsNotNone(sl, reason)
            _liq, guard, _room = engine._liquidation_guard("long", 100.0)
            self.assertLess(sl, 100.0)
            self.assertGreater(sl, guard)
            self.assertGreaterEqual(abs(100.0 - sl), 1.25)
        finally:
            ee.active_policy = old

    def test_continuous_posterior_resolver_can_accept_without_cisd_ote_threshold(self):
        import strategy.entry_engine as ee
        from strategy.liquidity_map import LiquidityPool, PoolSide, SweepResult

        old_eval = ee.evaluate_post_sweep_quant
        try:
            def fake_eval(**kwargs):
                return _QD(accept=(kwargs.get("action") == "reverse"), posterior=0.84, ev=0.81, llr=2.35)
            ee.evaluate_post_sweep_quant = fake_eval

            engine = ee.EntryEngine()
            engine._institutional_entry_quality_gate = lambda *a, **k: (True, "ok")
            pool = LiquidityPool(price=99.0, side=PoolSide.SSL, timeframe="1m")
            sweep = SweepResult(
                pool=pool,
                sweep_candle_idx=1,
                wick_extreme=98.8,
                rejection_pct=0.60,
                volume_ratio=1.5,
                quality=0.72,
                direction="long",
                detected_at=1_700_000_000,
            )
            now = 1_700_000_030.0
            ps = ee._PostSweepState(
                sweep=sweep,
                entered_at=now - 20.0,
                highest_since=100.1,
                lowest_since=98.8,
                static_scored=True,
                static_rev_base=2.0,
                static_cont_base=1.0,
            )
            decision = engine._evaluate_evidence(
                ps,
                snap=SimpleNamespace(bsl_pools=[], ssl_pools=[]),
                flow=ee.OrderFlowState(tick_flow=0.0, cvd_trend=0.0),
                ict=ee.ICTContext(),
                price=100.0,
                atr=2.0,
                now=now,
            )
            self.assertEqual(decision.action, "reverse")
            self.assertEqual(decision.direction, "long")
            self.assertIn("CONTINUOUS_POSTERIOR", decision.reason)
            self.assertEqual(engine._last_sweep_analysis.get("auction_resolver"), "continuous_posterior_ev")
        finally:
            ee.evaluate_post_sweep_quant = old_eval

    def test_same_value_area_flip_requires_dominance(self):
        from strategy.entry_engine import EntryEngine
        engine = EntryEngine()
        now = 1_700_000_000.0
        engine._record_auction_memory("long", 100.0, _QD(accept=True, posterior=0.83, ev=0.70, llr=2.2), now)
        allowed, reason = engine._auction_memory_allows(
            "short", 100.25, 1.0, _QD(accept=True, posterior=0.84, ev=0.75, llr=2.3), now + 80.0)
        self.assertFalse(allowed)
        self.assertIn("same-auction flip dominance", reason)

        allowed2, _ = engine._auction_memory_allows(
            "short", 100.25, 1.0, _QD(accept=True, posterior=0.91, ev=1.05, llr=2.8), now + 80.0)
        self.assertTrue(allowed2)

    def test_delta_bracket_path_reprices_and_retries_without_naked_fallback(self):
        import inspect
        from strategy.quant_strategy import QuantStrategy
        src = inspect.getsource(QuantStrategy._enter_trade)
        self.assertIn("_maker_safe_delta_limit", src)
        self.assertIn("retrying once with deeper post-only maker limit", src)
        self.assertIn("failed after maker-safe retry", src)
        self.assertNotIn("fallback_to_market=True", src)


if __name__ == "__main__":
    unittest.main()
