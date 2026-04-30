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

    def test_initial_sl_rounding_moves_loss_side_only(self):
        from execution.order_manager import OrderManager
        from strategy.quant_strategy import _round_initial_sl_to_tick

        om = object.__new__(OrderManager)
        tick = om._active_tick_size()
        raw = 100.26

        sell_stop = om._round_stop_trigger_to_tick("SELL", raw)
        buy_stop = om._round_stop_trigger_to_tick("BUY", raw)

        self.assertLessEqual(sell_stop, raw)
        self.assertGreaterEqual(buy_stop, raw)
        self.assertAlmostEqual(sell_stop / tick, round(sell_stop / tick))
        self.assertAlmostEqual(buy_stop / tick, round(buy_stop / tick))
        self.assertLessEqual(_round_initial_sl_to_tick("long", raw), raw)
        self.assertGreaterEqual(_round_initial_sl_to_tick("short", raw), raw)

    def test_sl_selector_ignores_swept_or_consumed_protective_pools(self):
        from strategy.liquidity_pool_selector import select_sl_with_report

        swept_pool = SimpleNamespace(
            price=98.0, side="SSL", timeframe="15m", status="SWEPT",
            touches=1, ob_aligned=True, fvg_aligned=True,
        )
        active_pool = SimpleNamespace(
            price=97.5, side="SSL", timeframe="15m", status="ACTIVE",
            touches=1, ob_aligned=False, fvg_aligned=False,
        )
        swept_target = SimpleNamespace(
            pool=swept_pool, significance=50.0, distance_atr=2.0, tf_sources=["15m"],
        )
        active_target = SimpleNamespace(
            pool=active_pool, significance=2.0, distance_atr=2.5, tf_sources=["15m"],
        )
        snap = SimpleNamespace(ssl_pools=[swept_target, active_target], bsl_pools=[])

        sl_price, target, pick, report = select_sl_with_report(
            snap=snap, side="long", entry=100.0, atr=1.0,
            invalidation_price=98.8,
        )

        self.assertIs(target, active_target)
        self.assertIsNotNone(pick)
        self.assertLess(sl_price, active_pool.price)
        self.assertIn("selected", report.summary)

    def test_tp_selector_ignores_swept_pool_in_actual_scoring(self):
        from strategy.liquidity_pool_selector import select_tp_with_report

        swept_pool = SimpleNamespace(
            price=110.0, side="BSL", timeframe="15m", status="SWEPT",
            touches=1, ob_aligned=True, fvg_aligned=True,
        )
        active_pool = SimpleNamespace(
            price=106.0, side="BSL", timeframe="15m", status="ACTIVE",
            touches=1, ob_aligned=False, fvg_aligned=False,
        )
        swept_target = SimpleNamespace(
            pool=swept_pool, significance=50.0, distance_atr=10.0,
            direction="long", tf_sources=["15m"],
        )
        active_target = SimpleNamespace(
            pool=active_pool, significance=3.0, distance_atr=6.0,
            direction="long", tf_sources=["15m"],
        )
        snap = SimpleNamespace(bsl_pools=[swept_target, active_target], ssl_pools=[])

        tp_price, target, score, report = select_tp_with_report(
            snap=snap, side="long", entry=100.0, sl=98.0, atr=1.0,
            min_rr=1.5,
        )

        self.assertIs(target, active_target)
        self.assertIsNotNone(score)
        self.assertLess(tp_price, active_pool.price)
        self.assertIn("selected", report.summary)

    def test_expected_utility_tp_is_buffered_before_liquidity_pool(self):
        from strategy.expected_utility import build_target_surface

        pool = SimpleNamespace(
            price=108.0, side="BSL", timeframe="15m", status="ACTIVE",
            significance=8.0,
        )
        target = SimpleNamespace(pool=pool, distance_atr=8.0, direction="long")
        snap = SimpleNamespace(bsl_pools=[target], ssl_pools=[], feed_reliability=0.95)

        surface = build_target_surface(
            side="long", entry=100.0, stop=98.0, atr=1.0,
            snapshot=snap, posterior_prob=0.90, tick_size=0.5,
        )

        self.assertIsNotNone(surface.best)
        self.assertLess(surface.best.price, pool.price)
        self.assertGreater(surface.best.price, 100.0)
        self.assertTrue(any("tp_buffer" in n for n in surface.best.notes))

    def test_expected_utility_ignores_archived_targets(self):
        from strategy.expected_utility import build_target_surface
        from strategy.liquidity_map import PoolStatus

        swept_pool = SimpleNamespace(
            price=108.0, side="BSL", timeframe="15m", status=PoolStatus.CONSUMED,
            significance=50.0,
        )
        active_pool = SimpleNamespace(
            price=106.0, side="BSL", timeframe="15m", status="ACTIVE",
            significance=5.0,
        )
        snap = SimpleNamespace(
            bsl_pools=[
                SimpleNamespace(pool=swept_pool, distance_atr=8.0, direction="long"),
                SimpleNamespace(pool=active_pool, distance_atr=6.0, direction="long"),
            ],
            ssl_pools=[],
            feed_reliability=0.95,
        )

        surface = build_target_surface(
            side="long", entry=100.0, stop=98.0, atr=1.0,
            snapshot=snap, posterior_prob=0.90, tick_size=0.5,
        )

        self.assertIsNotNone(surface.best)
        self.assertIs(surface.best.pool_ref.pool, active_pool)


if __name__ == "__main__":
    unittest.main()
