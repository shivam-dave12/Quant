import os
import sys
import time
import unittest
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

os.environ.setdefault("DELTA_API_KEY", "test")
os.environ.setdefault("DELTA_SECRET_KEY", "test")
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "")
os.environ.setdefault("TELEGRAM_CHAT_ID", "")


class _Pool:
    price = 100.0
    timeframe = "15m"
    htf_count = 2
    significance = 12.0


def _high_conviction_filter():
    from strategy.conviction_filter import ConvictionFilter

    cf = ConvictionFilter()
    cf._score_displacement = lambda *a, **kw: 1.0
    cf._score_cisd = lambda *a, **kw: 1.0
    cf._score_ote = lambda *a, **kw: 1.0
    cf._score_amd = lambda *a, **kw: 1.0
    cf._get_dealing_range_pd = lambda *a, **kw: (0.20, True)
    return cf


class HardeningTests(unittest.TestCase):
    def test_watchdog_freeze_log_is_suppressed_for_telegram(self):
        from telegram import notifier

        msg = (
            "strategy.quant_strategy: Entries paused: "
            "watchdog circuit breaker is engaged"
        )
        self.assertTrue(notifier._is_suppressed_for_telegram(msg))

    def test_execution_router_delegates_emergency_flatten(self):
        from execution.router import ExecutionRouter

        class FakeOrderManager:
            def __init__(self):
                self.calls = []

            def emergency_flatten(self, *args, **kwargs):
                self.calls.append((args, kwargs))
                return {"ok": True}

        om = FakeOrderManager()
        router = ExecutionRouter(coinswitch_om=None, delta_om=om, default="delta")

        self.assertEqual(router.emergency_flatten(reason="unit_test"), {"ok": True})
        self.assertEqual(om.calls[0][1]["reason"], "unit_test")

    def test_htf_veto_is_not_unconditionally_disabled(self):
        from strategy.quant_strategy import HTFTrendFilter

        htf = object.__new__(HTFTrendFilter)
        htf._trend_15m = -0.50
        htf._trend_4h = 0.00

        self.assertTrue(htf.vetoes_trade("long"))
        self.assertFalse(htf.vetoes_trade("short"))

    def test_conviction_advises_low_rr_without_alpha_veto(self):
        cf = _high_conviction_filter()

        result = cf.evaluate(
            trade_side="long",
            sweep_pool=_Pool(),
            entry_price=100.0,
            sl_price=99.0,
            tp_price=101.0,
            price=100.0,
            atr=2.0,
            now=time.time(),
            session="LONDON",
            entry_type="sweep_reversal",
            sweep_wick_price=99.0,
            measured_displacement_atr=2.0,
        )

        self.assertTrue(result.allowed)
        self.assertTrue(any("RR_LOW_EXPECTANCY" in r or "RR_LOW" in r for r in result.reject_reasons))

    def test_conviction_advises_approach_entries_without_alpha_veto(self):
        cf = _high_conviction_filter()

        result = cf.evaluate(
            trade_side="long",
            sweep_pool=_Pool(),
            entry_price=100.0,
            sl_price=99.0,
            tp_price=103.0,
            price=100.0,
            atr=2.0,
            now=time.time(),
            session="LONDON",
            entry_type="approach",
            sweep_wick_price=99.0,
            measured_displacement_atr=2.0,
        )

        self.assertTrue(result.allowed)
        self.assertTrue(any("APPROACH_PRE_SWEEP" in r or "APPROACH" in r for r in result.reject_reasons))

    def test_conviction_advises_missing_sweep_displacement_without_alpha_veto(self):
        cf = _high_conviction_filter()

        result = cf.evaluate(
            trade_side="long",
            sweep_pool=_Pool(),
            entry_price=100.0,
            sl_price=99.0,
            tp_price=103.0,
            price=100.0,
            atr=2.0,
            now=time.time(),
            session="LONDON",
            entry_type="sweep_reversal",
            sweep_wick_price=99.0,
            measured_displacement_atr=0.0,
        )

        self.assertTrue(result.allowed)
        self.assertTrue(any("DISPLACEMENT_MISSING" in r for r in result.reject_reasons))

    def test_reconcile_defers_stale_position_right_after_exit(self):
        from strategy.quant_strategy import PositionPhase, QuantStrategy

        class FakeOrderManager:
            def __init__(self):
                self.flatten_calls = 0

            def emergency_flatten(self, *args, **kwargs):
                self.flatten_calls += 1

        strategy = object.__new__(QuantStrategy)
        strategy._pos = SimpleNamespace(phase=PositionPhase.FLAT)
        strategy._exit_completed = True
        strategy._last_exit_time = time.time()

        om = FakeOrderManager()
        strategy._reconcile_apply(
            om,
            {
                "ex_pos": {
                    "size": 0.001,
                    "size_signed": 0.001,
                    "side": "LONG",
                    "entry_price": 100.0,
                    "unrealized_pnl": 0.0,
                },
                "open_orders": [],
            },
        )

        self.assertEqual(om.flatten_calls, 0)

    def test_quant_strategy_blocks_only_account_safety_conviction(self):
        from strategy.conviction_filter import ConvictionResult
        from strategy.quant_strategy import QuantStrategy

        safe_quality_result = ConvictionResult(
            allowed=False,
            score=0.512,
            reject_reasons=["PRODUCT_CORE: 0.11 < 0.60"],
        )
        safety_result = ConvictionResult(
            allowed=False,
            score=0.100,
            reject_reasons=["DRAWDOWN_CIRCUIT_BREAKER: session loss limit hit"],
        )

        self.assertFalse(QuantStrategy._conviction_reject_is_account_safety(safe_quality_result))
        self.assertTrue(QuantStrategy._conviction_reject_is_account_safety(safety_result))

    def test_target_surface_uses_posterior_without_lottery_tp(self):
        from strategy.expected_utility import build_target_surface

        target = SimpleNamespace(
            price=104.0,
            timeframe="15m",
            significance=12.0,
            side="BSL",
        )
        snap = SimpleNamespace(
            bsl_pools=[target],
            ssl_pools=[],
            feed_reliability=0.90,
        )
        flow = SimpleNamespace(tick_flow=0.30, cvd_trend=0.35)
        ict = SimpleNamespace(
            structure_15m="bullish",
            structure_4h="bullish",
            dealing_range_pd=0.30,
        )

        low = build_target_surface(
            side="long", entry=100.0, stop=98.0, atr=1.0,
            snapshot=snap, flow=flow, ict=ict, posterior_prob=0.0,
        )
        high = build_target_surface(
            side="long", entry=100.0, stop=98.0, atr=1.0,
            snapshot=snap, flow=flow, ict=ict, posterior_prob=0.90,
        )

        self.assertIsNotNone(low.best)
        self.assertIsNotNone(high.best)
        self.assertGreater(high.best.probability, low.best.probability)
        self.assertGreater(high.best.expected_value_r, low.best.expected_value_r)
        self.assertEqual(high.best.role, "external")

    def test_expected_utility_surface_does_not_rewrite_institutional_levels(self):
        from strategy.quant_strategy import QuantStrategy

        strategy = object.__new__(QuantStrategy)
        strategy._fee_engine = None
        strategy._entry_engine = SimpleNamespace(
            _last_sweep_analysis={"quant_posterior": 0.869}
        )

        entry = 78143.0
        raw_sl = 78554.9
        raw_tp = 76491.3
        raw_rr = abs(raw_tp - entry) / abs(entry - raw_sl)
        atr = 159.6
        signal = SimpleNamespace(
            side="short",
            entry_price=entry,
            sl_price=raw_sl,
            tp_price=raw_tp,
            rr_ratio=raw_rr,
            posterior_prob=0.869,
        )

        def pool_target(price, side, timeframe, significance):
            pool = SimpleNamespace(
                price=price,
                side=side,
                timeframe=timeframe,
                significance=significance,
                htf_count=3,
                touches=4,
                status="ACTIVE",
            )
            return SimpleNamespace(
                pool=pool,
                distance_atr=abs(price - entry) / atr,
            )

        snap = SimpleNamespace(
            bsl_pools=[
                pool_target(78145.0, "BSL", "5m", 18.0),
                pool_target(78232.0, "BSL", "1h", 14.0),
            ],
            ssl_pools=[
                pool_target(76833.5, "SSL", "1h", 20.0),
                pool_target(raw_tp, "SSL", "15m", 14.0),
            ],
            feed_reliability=0.90,
        )
        flow = SimpleNamespace(tick_flow=-0.30, cvd_trend=-0.36)
        ict = SimpleNamespace(
            structure_15m="bearish",
            structure_4h="bearish",
            dealing_range_pd=0.60,
        )

        strategy._apply_expected_utility_target_surface(
            signal, snap, flow, ict, entry, atr
        )

        self.assertAlmostEqual(signal.sl_price, raw_sl)
        self.assertAlmostEqual(signal.tp_price, raw_tp)
        self.assertAlmostEqual(signal.rr_ratio, raw_rr)
        self.assertIsNone(signal.stop_surface)
        self.assertIsNotNone(signal.target_surface)

    def test_quant_posterior_learns_from_closed_trade_outcomes(self):
        from strategy import quantitative_models as qm

        old_calibrator = qm.GLOBAL_QUANT_CALIBRATOR
        qm.GLOBAL_QUANT_CALIBRATOR = qm.AdaptiveQuantCalibrator()
        try:
            def pool(price, side):
                return SimpleNamespace(
                    pool=SimpleNamespace(
                        price=price,
                        side=side,
                        timeframe="15m",
                        significance=18.0,
                    )
                )

            snap = SimpleNamespace(
                bsl_pools=[pool(106.0, "BSL")],
                ssl_pools=[pool(94.0, "SSL")],
                feed_reliability=0.90,
            )
            flow = SimpleNamespace(
                direction="short",
                conviction=0.62,
                cvd_trend=-0.55,
            )
            ict = SimpleNamespace(
                structure_15m="bearish",
                structure_4h="bearish",
                dealing_range_pd=0.72,
            )

            base = qm.evaluate_post_sweep_quant(
                action="reverse",
                side="short",
                rev_score=100.0,
                cont_score=12.0,
                displacement_atr=1.80,
                cisd=True,
                ote=True,
                phase="CISD",
                price=100.0,
                atr=1.0,
                snap=snap,
                flow=flow,
                ict=ict,
            )
            for _ in range(40):
                qm.GLOBAL_QUANT_CALIBRATOR.record_trade_outcome(
                    base.components,
                    pnl=-1.0,
                    achieved_r=-1.0,
                )
            learned = qm.evaluate_post_sweep_quant(
                action="reverse",
                side="short",
                rev_score=100.0,
                cont_score=12.0,
                displacement_atr=1.80,
                cisd=True,
                ote=True,
                phase="CISD",
                price=100.0,
                atr=1.0,
                snap=snap,
                flow=flow,
                ict=ict,
            )

            self.assertGreaterEqual(learned.components["outcome_n"], 40.0)
            self.assertLess(learned.posterior, base.posterior)
        finally:
            qm.GLOBAL_QUANT_CALIBRATOR = old_calibrator


if __name__ == "__main__":
    unittest.main()
