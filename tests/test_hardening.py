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
        self.assertIsNotNone(signal.selected_target_utility)

    def test_selected_tp_must_have_positive_executable_utility(self):
        from strategy.quant_strategy import QuantStrategy

        strategy = object.__new__(QuantStrategy)
        strategy._fee_engine = None
        strategy._entry_engine = SimpleNamespace(
            _last_sweep_analysis={"quant_posterior": 0.55}
        )

        signal = SimpleNamespace(
            side="long",
            entry_price=100.0,
            sl_price=99.0,
            tp_price=140.0,
            rr_ratio=40.0,
            posterior_prob=0.55,
            target_pool=SimpleNamespace(pool=SimpleNamespace(price=140.0)),
        )
        snap = SimpleNamespace(
            bsl_pools=[
                SimpleNamespace(price=104.0, side="BSL", timeframe="15m", significance=16.0),
                SimpleNamespace(price=140.0, side="BSL", timeframe="15m", significance=1.0),
            ],
            ssl_pools=[],
            feed_reliability=0.80,
        )
        flow = SimpleNamespace(tick_flow=0.05, cvd_trend=0.02)
        ict = SimpleNamespace(
            structure_15m="mixed",
            structure_4h="mixed",
            dealing_range_pd=0.50,
        )

        strategy._apply_expected_utility_target_surface(
            signal, snap, flow, ict, 100.0, 1.0
        )

        self.assertIsNotNone(signal.selected_target_utility)
        self.assertLessEqual(signal.selected_target_utility.full_position_utility, 0.0)

    def test_fee_to_risk_can_return_no_allocation(self):
        from strategy.quant_strategy import QuantStrategy, SignalBreakdown

        strategy = object.__new__(QuantStrategy)
        strategy._post_trade_agent = None
        strategy._active_institutional_size_mult = 1.0
        strategy._active_ic_size_mult = 1.0
        strategy._active_post_exit_size_mult = 1.0

        class FakeRisk:
            def get_available_balance(self):
                return {"available": 250.0, "total": 250.0}

        qty = strategy._compute_quantity(
            FakeRisk(),
            price=78212.8,
            sig=SignalBreakdown(composite=0.80, amd_conf=0.80),
            ict_tier="S",
            sl_price=78158.3,
            prefetched_bal_info={"available": 250.0, "total": 250.0},
        )

        self.assertIsNone(qty)
        ctx = strategy._last_execution_viability
        self.assertGreater(ctx["min_viable_sl_dist"], ctx["current_sl_dist"])
        self.assertGreater(ctx["geometry_gap_pts"], 0.0)
        self.assertFalse(bool(ctx["allocation_allowed"]))

    def test_positive_net_utility_cannot_override_no_alloc_geometry(self):
        from strategy.quant_strategy import QuantStrategy, SignalBreakdown

        strategy = object.__new__(QuantStrategy)
        strategy._fee_engine = None
        strategy._post_trade_agent = None
        strategy._active_institutional_size_mult = 1.0
        strategy._active_ic_size_mult = 1.0
        strategy._active_post_exit_size_mult = 1.0

        class FakeRisk:
            def get_available_balance(self):
                return {"available": 250.0, "total": 250.0}

        qty = strategy._compute_quantity(
            FakeRisk(),
            price=78212.8,
            sig=SignalBreakdown(composite=0.80, amd_conf=0.80),
            ict_tier="S",
            sl_price=78158.3,
            tp_price=79212.8,
            side="long",
            use_maker_entry=False,
            posterior_prob=0.75,
            prefetched_bal_info={"available": 250.0, "total": 250.0},
        )

        self.assertIsNone(qty)
        self.assertGreater(strategy._last_execution_viability["expected_net_utility_r"], 0.0)
        self.assertFalse(bool(strategy._last_execution_viability["allocation_allowed"]))

    def test_log_case_maker_high_fee_geometry_is_refine_only(self):
        from strategy.quant_strategy import QuantStrategy, SignalBreakdown

        strategy = object.__new__(QuantStrategy)
        strategy._post_trade_agent = None
        strategy._active_institutional_size_mult = 1.0
        strategy._active_ic_size_mult = 1.0
        strategy._active_post_exit_size_mult = 1.0

        class FakeFeeEngine:
            def effective_roundtrip_cost_bps(self, use_maker_entry=True):
                return 9.3 if use_maker_entry else 12.0

        class FakeRisk:
            def get_available_balance(self):
                return {"available": 250.0, "total": 250.0}

        strategy._fee_engine = FakeFeeEngine()

        qty = strategy._compute_quantity(
            FakeRisk(),
            price=78599.8,
            sig=SignalBreakdown(composite=0.80, amd_conf=0.80),
            ict_tier="S",
            sl_price=78553.0,
            tp_price=78858.5,
            side="long",
            use_maker_entry=True,
            posterior_prob=0.75,
            prefetched_bal_info={"available": 250.0, "total": 250.0},
        )

        ctx = strategy._last_execution_viability
        self.assertIsNone(qty)
        self.assertEqual(ctx["route"], "maker")
        self.assertGreater(ctx["fee_to_risk"], 1.0)
        self.assertGreater(ctx["expected_net_utility_r"], 0.0)
        self.assertGreater(ctx["min_viable_sl_dist"], ctx["current_sl_dist"])
        self.assertFalse(bool(ctx["allocation_allowed"]))

    def test_position_sizing_floors_lot_without_overrisking_budget(self):
        from strategy.quant_strategy import QuantStrategy

        strategy = object.__new__(QuantStrategy)
        strategy._fee_engine = None
        strategy._post_trade_agent = None
        strategy._active_institutional_size_mult = 1.0
        strategy._active_ic_size_mult = 1.0
        strategy._active_post_exit_size_mult = 1.0

        class FakeRisk:
            def get_available_balance(self):
                return {"available": 1000.0, "total": 1000.0}

        price = 10000.0
        sl_dist = 5.0 / 0.00175
        qty = strategy._compute_quantity(
            FakeRisk(),
            price=price,
            sig=None,
            ict_tier="S",
            sl_price=price - sl_dist,
            tp_price=price + 6000.0,
            side="long",
            use_maker_entry=True,
            posterior_prob=0.75,
            prefetched_bal_info={"available": 1000.0, "total": 1000.0},
        )

        self.assertEqual(qty, 0.001)
        self.assertLessEqual(qty * sl_dist, 1000.0 * 0.005 + 1e-9)

    def test_position_sizing_interprets_legacy_percent_style_risk(self):
        import config
        from strategy.quant_strategy import QuantStrategy

        old_risk = config.RISK_PER_TRADE
        config.RISK_PER_TRADE = 0.5
        try:
            strategy = object.__new__(QuantStrategy)
            strategy._fee_engine = None
            strategy._post_trade_agent = None
            strategy._active_institutional_size_mult = 1.0
            strategy._active_ic_size_mult = 1.0
            strategy._active_post_exit_size_mult = 1.0

            class FakeRisk:
                def get_available_balance(self):
                    return {"available": 1000.0, "total": 1000.0}

            with self.assertLogs("strategy.quant_strategy", level="WARNING") as logs:
                qty = strategy._compute_quantity(
                    FakeRisk(),
                    price=10000.0,
                    sig=None,
                    ict_tier="S",
                    sl_price=9000.0,
                    tp_price=12000.0,
                    side="long",
                    use_maker_entry=True,
                    posterior_prob=0.75,
                    prefetched_bal_info={"available": 1000.0, "total": 1000.0},
                )

            self.assertEqual(qty, 0.005)
            self.assertLessEqual(qty * 1000.0, 5.0 + 1e-9)
            self.assertIn("looks percent-style", "\n".join(logs.output))
        finally:
            config.RISK_PER_TRADE = old_risk

    def test_execution_geometry_repair_uses_structural_sl(self):
        from strategy.quant_strategy import QuantStrategy

        strategy = object.__new__(QuantStrategy)
        strategy._liq_map = None
        strategy._last_entry_signal = SimpleNamespace(sweep_result=SimpleNamespace(wick_extreme=78552.8))

        class FakeFeeEngine:
            def effective_roundtrip_cost_bps(self, use_maker_entry=True):
                return 9.3

        class FakeEntryEngine:
            _last_liq_snapshot = SimpleNamespace()

            def _apply_institutional_sl_envelope(self, snap, side, price, atr, structural_sl,
                                                 invalidation_price, label, min_risk=0.0):
                return price - 120.0, "ok"

            def _last_selected_tp_rr_floor(self, floor):
                return floor

            def _find_tp(self, *args, **kwargs):
                return None, None

        strategy._fee_engine = FakeFeeEngine()
        strategy._entry_engine = FakeEntryEngine()

        sl, tp, repaired = strategy._repair_execution_geometry(
            side="long",
            entry_price=78599.8,
            sl_price=78553.0,
            tp_price=78858.5,
            atr=67.6,
            use_maker_entry=True,
            posterior_prob=0.75,
        )

        self.assertTrue(repaired)
        self.assertEqual(sl, 78479.8)
        self.assertEqual(tp, 78858.5)
        self.assertTrue(bool(strategy._last_execution_viability["allocation_allowed"]))

    def test_sl_pool_selector_honors_execution_min_risk(self):
        from strategy.liquidity_pool_selector import score_sl_pool

        def target(price, significance):
            return SimpleNamespace(
                pool=SimpleNamespace(
                    price=price,
                    status="DETECTED",
                    timeframe="5m",
                    touches=1,
                    ob_aligned=False,
                    fvg_aligned=False,
                ),
                significance=significance,
                distance_atr=abs(100.0 - price),
                tf_sources=["5m"],
            )

        snap = SimpleNamespace(
            ssl_pools=[target(99.5, 7.0), target(97.0, 4.0)],
            bsl_pools=[],
        )

        pick = score_sl_pool(
            snap,
            side="long",
            entry=100.0,
            atr=1.0,
            invalidation_price=99.8,
            min_risk=2.0,
        )

        self.assertIsNotNone(pick)
        self.assertEqual(pick.target.pool.price, 97.0)

    def test_execution_viability_context_arms_refine_watch_math(self):
        from strategy.entry_engine import EntryEngine, EntrySignal, EntryType

        engine = EntryEngine()
        sweep = SimpleNamespace(
            pool=SimpleNamespace(price=100.0, side=SimpleNamespace(value="SSL")),
            detected_at=123.0,
        )
        signal = EntrySignal(
            side="long",
            entry_type=EntryType.SWEEP_REVERSAL,
            entry_price=100.0,
            sl_price=99.0,
            tp_price=103.0,
            rr_ratio=3.0,
            target_pool=SimpleNamespace(),
            sweep_result=sweep,
            conviction=0.70,
        )
        ctx = {
            "min_viable_sl_dist": 2.4,
            "geometry_gap_pts": 1.4,
            "required_entry_price": 101.4,
            "required_sl_price": 97.6,
            "fee_to_risk": 1.80,
            "fee_no_alloc": 0.75,
            "expected_net_utility_r": -0.25,
        }

        engine.mark_pre_order_rejected(signal, execution_context=ctx)
        pending = engine._pending_refined

        self.assertIsNotNone(pending)
        self.assertEqual(pending.min_viable_risk, 2.4)
        self.assertIn("execution geometry invalid", pending.last_reason)

    def test_fee_refine_does_not_wait_for_chase_to_expand_risk(self):
        from strategy.entry_engine import EntryEngine, EntrySignal, EntryType

        engine = EntryEngine()
        sweep = SimpleNamespace(
            pool=SimpleNamespace(price=99.8, side=SimpleNamespace(value="SSL")),
            wick_extreme=99.6,
            detected_at=123.0,
        )
        signal = EntrySignal(
            side="long",
            entry_type=EntryType.SWEEP_REVERSAL,
            entry_price=101.0,
            sl_price=99.8,
            tp_price=105.0,
            rr_ratio=3.3,
            target_pool=SimpleNamespace(pool=SimpleNamespace(price=105.0)),
            sweep_result=sweep,
            conviction=0.75,
            reason="unit",
        )
        engine.mark_pre_order_rejected(
            signal,
            execution_context={
                "min_viable_sl_dist": 2.4,
                "geometry_gap_pts": 1.2,
                "required_entry_price": 102.2,
                "required_sl_price": 98.6,
                "fee_to_risk": 1.2,
                "fee_no_alloc": 0.75,
            },
        )

        engine._regime_sl_mult = lambda: 1.0
        engine._push_sl_behind_pools = lambda sl, side, price, atr: sl
        engine._sl_structural_bounds = lambda *args, **kwargs: (0.0, 100.0)
        engine._sl_before_liquidation = lambda *args, **kwargs: True
        engine._apply_institutional_sl_envelope = (
            lambda snap, side, price, atr, sl, inval, label, min_risk=0.0:
                (price - min_risk - 0.2, "ok")
        )
        engine._find_tp = lambda *args, **kwargs: (107.0, SimpleNamespace(pool=SimpleNamespace(price=107.0)))
        engine._last_selected_tp_rr_floor = lambda floor: floor
        engine._ict_summary = lambda *args, **kwargs: "unit"

        engine._evaluate_pending_refined_entry(
            SimpleNamespace(), SimpleNamespace(), SimpleNamespace(),
            price=101.0, atr=1.0, now=time.time() + 2.0)

        self.assertIsNotNone(engine._signal)
        self.assertLess(engine._signal.sl_price, 99.0)
        self.assertIsNone(engine._pending_refined)

    def test_conviction_entry_cap_becomes_allocation_haircut(self):
        from strategy.conviction_filter import ConvictionFactors, ConvictionResult
        from strategy.quant_strategy import QuantStrategy

        strategy = object.__new__(QuantStrategy)
        result = ConvictionResult(
            allowed=True,
            score=0.82,
            factors=ConvictionFactors(
                pool_sig_score=0.80,
                displacement_score=0.76,
                cisd_score=0.72,
                ote_score=0.60,
                session_score=1.00,
                amd_score=0.70,
            ),
            reject_reasons=["ENTRY_CAP: 3/3 entries exhausted this session."],
        )

        mult = strategy._conviction_allocation_multiplier(result)

        self.assertGreater(mult, 0.0)
        self.assertLess(mult, 0.50)

    def test_structural_trail_rejects_fee_dragged_micro_winner(self):
        from strategy.quant_strategy import QuantStrategy

        strategy = object.__new__(QuantStrategy)
        pos = SimpleNamespace(
            side="long",
            entry_price=100.0,
            sl_price=98.0,
            initial_sl_dist=2.0,
            quantity=1.0,
            entry_fee_paid=0.0,
        )

        ok, reason = strategy._trail_payoff_lock_ok(
            pos, new_sl=100.6, atr=1.0, phase="DELIVERY_LOCK")

        self.assertFalse(ok)
        self.assertIn("net_lock", reason)

    def test_breakeven_trail_remains_allowed_for_loss_reduction(self):
        from strategy.quant_strategy import QuantStrategy

        strategy = object.__new__(QuantStrategy)
        pos = SimpleNamespace(
            side="long",
            entry_price=100.0,
            sl_price=98.0,
            initial_sl_dist=2.0,
            quantity=1.0,
            entry_fee_paid=0.0,
        )

        ok, reason = strategy._trail_payoff_lock_ok(
            pos, new_sl=100.5, atr=1.0, phase="BE_LOCK")

        self.assertTrue(ok)
        self.assertIn("risk-defense", reason)

    def test_structural_trail_accepts_meaningful_net_payoff_lock(self):
        from strategy.quant_strategy import QuantStrategy

        strategy = object.__new__(QuantStrategy)
        pos = SimpleNamespace(
            side="long",
            entry_price=100.0,
            sl_price=98.0,
            initial_sl_dist=2.0,
            quantity=1.0,
            entry_fee_paid=0.0,
        )

        ok, reason = strategy._trail_payoff_lock_ok(
            pos, new_sl=101.6, atr=1.0, phase="STRUCTURAL")

        self.assertTrue(ok, reason)

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

    def test_tp_selector_rejects_sub_loss_final_tp_geometry(self):
        from strategy.liquidity_pool_selector import select_tp_with_report

        entry = 78645.0
        sl = 78005.1
        atr = 196.8
        pool = SimpleNamespace(
            price=79443.2,
            timeframe="1d",
            side="BSL",
            significance=14.0,
            htf_count=3,
            touches=2,
            status="ACTIVE",
        )
        target = SimpleNamespace(
            pool=pool,
            distance_atr=abs(pool.price - entry) / atr,
            significance=14.0,
        )
        snap = SimpleNamespace(bsl_pools=[target], ssl_pools=[], feed_reliability=0.90)

        tp, selected_target, score, report = select_tp_with_report(
            snap=snap,
            side="long",
            entry=entry,
            sl=sl,
            atr=atr,
            min_rr=2.20,
            posterior_prob=0.862,
            now=0,
        )

        self.assertIsNone(tp)
        self.assertIsNone(selected_target)
        self.assertIsNone(score)
        payload = report.as_dict()
        self.assertIsNone(payload["selected"])
        self.assertGreaterEqual(payload["candidates"][0]["required_rr"], 1.35)
        self.assertIn("payoff floor", payload["candidates"][0]["reason"])

    def test_tp_selector_prefers_durable_liquidity_over_near_payoff(self):
        from strategy.liquidity_pool_selector import select_tp_with_report

        now = 1_700_000_000

        def target(price, timeframe, significance):
            pool = SimpleNamespace(
                price=price,
                timeframe=timeframe,
                side="BSL",
                significance=significance,
                htf_count=3,
                touches=1,
                status="DETECTED",
                created_at=now,
                ob_aligned=False,
                fvg_aligned=False,
            )
            return SimpleNamespace(
                pool=pool,
                distance_atr=abs(price - 100.0),
                significance=significance,
                direction="long",
                tf_sources=[timeframe],
            )

        near = target(103.4, "15m", 22.0)
        far = target(108.0, "4h", 14.0)
        snap = SimpleNamespace(
            bsl_pools=[near, far],
            ssl_pools=[],
            feed_reliability=0.90,
        )

        tp, selected_target, score, report = select_tp_with_report(
            snap=snap,
            side="long",
            entry=100.0,
            sl=98.0,
            atr=1.0,
            min_rr=2.20,
            posterior_prob=0.88,
            now=now,
        )

        self.assertIsNotNone(tp)
        self.assertIs(selected_target, far)
        self.assertGreater(score.rr, 3.0)
        self.assertGreater(score.components["selection_ev"], score.ev)
        payload = report.as_dict()
        self.assertEqual(payload["selected"]["pool_price"], far.pool.price)
        self.assertIn("payoff-adjusted EV", payload["selected"]["reason"])

    def test_tp_selector_rejects_zero_probability_terminal_lottery(self):
        from strategy.liquidity_pool_selector import select_tp_with_report

        entry = 78127.9
        sl = 78236.6
        atr = 34.3
        pool = SimpleNamespace(
            price=74886.7,
            timeframe="1d",
            side="SSL",
            significance=14.0,
            htf_count=3,
            touches=1,
            status="ACTIVE",
            created_at=0,
            ob_aligned=False,
            fvg_aligned=False,
        )
        target = SimpleNamespace(
            pool=pool,
            distance_atr=abs(pool.price - entry) / atr,
            significance=14.0,
            direction="short",
            tf_sources=["1d"],
        )
        snap = SimpleNamespace(
            bsl_pools=[],
            ssl_pools=[target],
            feed_reliability=0.90,
        )

        tp, selected_target, score, report = select_tp_with_report(
            snap=snap,
            side="short",
            entry=entry,
            sl=sl,
            atr=atr,
            min_rr=2.20,
            posterior_prob=0.916,
            now=0,
        )

        self.assertIsNone(tp)
        self.assertIsNone(selected_target)
        self.assertIsNone(score)
        payload = report.as_dict()
        self.assertIsNone(payload["selected"])
        candidate = payload["candidates"][0]
        self.assertGreater(candidate["required_rr"], 100.0)
        self.assertIn("delivery probability", candidate["reason"])
        self.assertGreater(candidate["required_delivery_prob"], candidate["delivery_prob"])

    def test_tp_audit_prioritizes_executable_near_reject_over_lottery_rr(self):
        from strategy.liquidity_pool_selector import select_tp_with_report

        now = 1_700_000_000
        entry = 78340.0
        sl = 78506.8
        atr = 51.3

        def target(price, timeframe, significance):
            pool = SimpleNamespace(
                price=price,
                timeframe=timeframe,
                side="SSL",
                significance=significance,
                htf_count=3,
                touches=1,
                status="DETECTED",
                created_at=now,
                ob_aligned=False,
                fvg_aligned=False,
            )
            return SimpleNamespace(
                pool=pool,
                distance_atr=abs(price - entry) / atr,
                significance=significance,
                direction="short",
                tf_sources=[timeframe],
            )

        near = target(78078.75, "15m", 12.3)
        far = target(73675.0, "1d", 16.0)
        snap = SimpleNamespace(
            bsl_pools=[],
            ssl_pools=[far, near],
            feed_reliability=0.90,
        )

        tp, selected_target, score, report = select_tp_with_report(
            snap=snap,
            side="short",
            entry=entry,
            sl=sl,
            atr=atr,
            min_rr=2.20,
            posterior_prob=0.834,
            now=now,
        )

        self.assertIsNone(tp)
        self.assertIsNone(selected_target)
        self.assertIsNone(score)
        payload = report.as_dict()
        self.assertEqual(payload["candidates"][0]["pool_price"], near.pool.price)
        self.assertIn("delivery probability", payload["summary"])
        self.assertIn("RR 1.46", payload["summary"])
        self.assertLess(payload["candidates"][0]["required_delivery_prob"], 1.0)

    def test_low_quality_sl_pool_cannot_destroy_real_tp_geometry(self):
        from strategy.entry_engine import EntryEngine

        ok, reason = EntryEngine._accepts_pool_stop_geometry(
            current_risk=142.0,
            pool_risk=639.9,
            target_reward=738.6,
            posterior_prob=0.862,
            pool_quality=0.47,
        )
        self.assertFalse(ok)
        self.assertIn("risk expansion", reason)

        ok, _reason = EntryEngine._accepts_pool_stop_geometry(
            current_risk=142.0,
            pool_risk=639.9,
            target_reward=738.6,
            posterior_prob=0.862,
            pool_quality=0.95,
        )
        self.assertTrue(ok)


if __name__ == "__main__":
    unittest.main()
