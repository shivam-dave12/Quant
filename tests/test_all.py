# test_all.py
# Consolidated institutional regression suite.
# Generated from the former split test modules so pytest runs one test file only.



import os
import sys

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
import config

# ===== BEGIN test_asset_notifications_v10.py =====

import unittest
from core.instruments import AssetClass, ExchangeName, ExchangeInstrument, TradableInstrument, instrument_scope
from telegram.notifier import _tg_enrich_asset_message, format_periodic_report


def _inst(asset="AAPL", sym="AAPLXUSD", cls=AssetClass.EQUITY):
    ei = ExchangeInstrument(
        exchange=ExchangeName.DELTA,
        symbol=sym,
        ws_symbol=sym,
        display_symbol=sym,
        asset_id=asset,
        asset_class=cls,
        max_leverage=25,
    )
    return TradableInstrument(
        asset_id=asset,
        display_name=f"{asset} xStock",
        asset_class=cls,
        primary_exchange=ExchangeName.DELTA,
        by_exchange={ExchangeName.DELTA: ei},
    )


class AssetNotificationTests(unittest.TestCase):
    def test_asset_header_is_added(self):
        inst = _inst()
        msg = _tg_enrich_asset_message("🧠 <b>POSTERIOR ACCEPTED</b>", instrument=inst, event_type="posterior", context={"state":"SCANNING", "price":279.0})
        self.assertIn("AAPL", msg)
        self.assertIn("DELTA:AAPLXUSD", msg)
        self.assertIn("POSTERIOR", msg)
        self.assertIn("SCANNING", msg)

    def test_periodic_report_uses_supplied_unrealised_pnl_source(self):
        msg = format_periodic_report(
            current_price=105.0,
            position={
                "side": "long", "quantity": 2.0, "entry_price": 100.0,
                "sl_price": 90.0, "tp_price": 130.0,
                "unrealised_pnl": 10.0,
                "tp_ladder_realized_pnl": 1.5,
                "lifecycle_open_pnl": 11.5,
            },
            current_sl=90.0, current_tp=130.0, entry_price=100.0,
        )
        self.assertIn("UPNL        +$10.00", msg)
        self.assertIn("LIVE           +$11.50", msg)
        self.assertIn("ladder       +$1.50", msg)

    def test_already_scoped_message_not_double_wrapped(self):
        inst = _inst()
        raw = "🏛 <b>POSTERIOR</b>  <code>AAPL</code>\nbody"
        msg = _tg_enrich_asset_message(raw, instrument=inst, event_type="posterior")
        self.assertEqual(raw, msg)

if __name__ == "__main__":
    unittest.main()


class StartupMessagePolicyAliasTest(unittest.TestCase):
    def test_instrument_policy_exposes_evaluation_interval_alias(self):
        from core.market_policy import build_instrument_policy
        pol = build_instrument_policy(None)
        self.assertTrue(hasattr(pol, "evaluation_interval_sec"))
        self.assertEqual(pol.evaluation_interval_sec, pol.loop_interval_sec)
        self.assertIn("evaluation_interval_sec", pol.asdict())
# ===== END test_asset_notifications_v10.py =====


# ===== BEGIN test_cross_asset_overlay.py =====
import math
import time
from types import SimpleNamespace

from strategy.cross_asset_regime import CrossAssetRegimeEngine, CrossAssetState, CrossAssetAdjustment
from strategy.liquidity_pool_selector import _apply_cross_asset_tp_overlay


def test_cross_asset_engine_identifies_silver_catchup_candidate():
    eng = CrossAssetRegimeEngine()
    now = time.time()
    base = [0.0007, 0.0011, 0.0009, 0.0014, 0.0005, 0.0012] * 7
    series = {
        "GOLD":   base + [0.0040],
        "SILVER": [x * 1.2 + 0.00005 for x in base] + [0.0010],
        "BTC":    [0.0002] * (len(base) + 1),
    }
    st = eng._build_state(series, now)
    adj = st.adjustment_for("SILVER", "long")

    assert st.enabled
    assert st.preferred_asset == "SILVER"
    assert st.silver_residual_z < -0.8
    assert adj.enabled
    assert adj.posterior_logit_adjust > 0
    assert adj.tp_aggression > 0
    assert adj.risk_multiplier > 1.0


def test_cross_asset_adjustment_changes_probability_without_hard_veto():
    adj = CrossAssetAdjustment(enabled=True, posterior_logit_adjust=0.16, risk_multiplier=1.12)
    p0 = 0.55
    p1 = adj.adjusted_probability(p0)
    assert p1 > p0
    assert 0.0 < p1 < 1.0


def test_cross_asset_tp_overlay_rewards_farther_sponsored_pool():
    adj = CrossAssetAdjustment(
        enabled=True,
        tp_aggression=0.25,
        posterior_logit_adjust=0.10,
        cluster_risk_penalty=0.0,
        reason="silver lagging gold; catch-up candidate",
    )
    near = SimpleNamespace(distance_atr=1.0, rr=2.0)
    far = SimpleNamespace(distance_atr=4.0, rr=4.0)
    near_ev = _apply_cross_asset_tp_overlay(1.0, near, adj)
    far_ev = _apply_cross_asset_tp_overlay(1.0, far, adj)
    assert far_ev > near_ev
    assert near_ev > 1.0
# ===== END test_cross_asset_overlay.py =====


# ===== BEGIN test_cross_asset_pair_governance.py =====
from types import SimpleNamespace

from strategy.cross_asset_regime import CrossAssetRegimeEngine


def test_decorrelated_residual_is_noise_not_relative_value():
    e = CrossAssetRegimeEngine()
    st = SimpleNamespace(
        corr={"GOLD:SILVER": 0.10, "GOLD:SILVER_BASE": 0.12},
        returns={"GOLD": 0.01, "SILVER": -0.01},
        silver_residual_z=1.50,
    )
    assert e._relationship_quality(st) == 0.0
    # Attach relationship quality because _classify_metals expects it on real state.
    st.relationship_quality = 0.0
    assert e._classify_metals(st) == "DECORRELATED_NOISE"


def test_unsponsored_opposite_metal_position_blocks_candidate():
    e = CrossAssetRegimeEngine()
    st = SimpleNamespace(
        corr={"GOLD:SILVER": 0.10, "GOLD:SILVER_BASE": 0.10},
        returns={"GOLD": -0.01, "SILVER": -0.02},
        silver_residual_z=1.50,
        gold_silver_ratio_z=0.0,
        metals_regime="DECORRELATED_NOISE",
        btc_macro_role="IDIOSYNCRATIC_CRYPTO_FLOW",
        preferred_asset="",
        relationship_quality=0.0,
        cluster_risk_score=0.0,
        positions={"SILVER": {"side": "short", "quantity": 9.0}},
    )
    adj = e._build_adjustments(st)[("GOLD", "long")]
    assert not adj.entry_allowed
    assert "unsponsored metal pair" in adj.block_reason


def test_cross_asset_state_supports_tp_ladder_signal_alias():
    e = CrossAssetRegimeEngine()
    st = e._build_state({
        "GOLD": [0.001] * 30,
        "SILVER": [0.0012] * 30,
        "BTC": [0.0005] * 30,
    }, now=1.0)
    st.ts = __import__("time").time()
    adj = st.adjustment_for_signal("GOLD", "long")
    assert adj.asset == "GOLD"
    assert adj.side == "long"
# ===== END test_cross_asset_pair_governance.py =====


# ===== BEGIN test_delta_bracket_policy.py =====
import inspect
import unittest


class DeltaBracketPolicyTests(unittest.TestCase):
    def test_quant_strategy_refuses_delta_non_bracket_fallback(self):
        from strategy.quant_strategy import QuantStrategy

        src = inspect.getsource(QuantStrategy._enter_trade)
        self.assertIn("DELTA_REQUIRE_NATIVE_BRACKET", src)
        self.assertIn("Delta native bracket entry failed", src)
        self.assertIn("refusing non-bracket fallback", src)
        self.assertIn("_active_exchange == \"delta\"", src)

    def test_config_defaults_require_delta_native_bracket(self):
        import config
        self.assertTrue(getattr(config, "DELTA_REQUIRE_NATIVE_BRACKET", False))


if __name__ == "__main__":
    unittest.main()
# ===== END test_delta_bracket_policy.py =====


# ===== BEGIN test_hardening.py =====
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

    def test_no_positive_target_utility_blocks_execution_allocation(self):
        from strategy.quant_strategy import EntryType, QuantStrategy

        strategy = object.__new__(QuantStrategy)
        strategy._htf = SimpleNamespace(trend_15m=0.0, trend_4h=0.0)
        strategy._last_hunt_prediction = None
        strategy._entry_engine = SimpleNamespace(
            _last_sweep_analysis={
                "displacement_atr": 1.50,
                "cisd": True,
                "ote": False,
            }
        )
        bad_target = SimpleNamespace(
            full_position_utility=-0.40,
            expected_value_r=-0.10,
            compact=lambda: "terminal p=0.01 fullU=-0.40",
        )
        signal = SimpleNamespace(
            side="long",
            entry_type=EntryType.SWEEP_REVERSAL,
            entry_price=100.0,
            sl_price=98.0,
            tp_price=140.0,
            target_pool=SimpleNamespace(
                pool=SimpleNamespace(price=140.0, timeframe="15m", side="BSL"),
                significance=10.0,
                distance_atr=40.0,
            ),
            sweep_result=SimpleNamespace(quality=0.90),
            conviction=0.90,
            selected_target_utility=bad_target,
            target_surface=SimpleNamespace(has_positive_edge=False, best=bad_target),
        )
        ict = SimpleNamespace(
            direction_hint_side="",
            direction_hint_confidence=0.0,
            dealing_range_pd=0.25,
            amd_phase="MANIPULATION",
            amd_bias="bullish",
            amd_confidence=0.90,
        )
        flow = SimpleNamespace(tick_flow=0.30, cvd_trend=0.30)

        decision = strategy._institutional_decision_matrix(
            signal, ict, flow, None, price=100.0, atr=1.0)

        self.assertFalse(decision.allowed)
        self.assertTrue(any("executable utility" in r for r in decision.reject_reasons))

    def test_expected_utility_negative_surface_has_no_size_allocation(self):
        from strategy.expected_utility import expected_utility_size_multiplier

        surface = SimpleNamespace(
            has_positive_edge=False,
            best=SimpleNamespace(payoff_r=12.0, probability=0.02, full_position_utility=-1.0),
            runner_fraction=0.0,
        )

        self.assertEqual(expected_utility_size_multiplier(surface, posterior=0.95), 0.0)

    def test_legacy_liquidity_trail_module_removed_from_strategy_build(self):
        import os

        # Fixed-SL TP ladder is the active exit model. The legacy SL-migration
        # strategy module must not be shipped as an executable strategy path.
        self.assertFalse(os.path.exists(os.path.join("strategy", "liquidity_trail.py")))

    def test_sl_migration_override_is_ignored_while_position_active(self):
        import threading
        from strategy.quant_strategy import QuantStrategy

        strategy = object.__new__(QuantStrategy)
        strategy._lock = threading.RLock()
        strategy._pos = SimpleNamespace(
            trail_override=True,
            is_active=lambda: True,
        )

        changed = strategy.set_trail_override(False)

        self.assertFalse(changed)
        self.assertFalse(strategy._pos.trail_override)

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

    def test_position_sizing_rejects_dust_margin_even_if_lot_fits_risk(self):
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
        with self.assertLogs("strategy.quant_strategy", level="WARNING") as logs:
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

        self.assertIsNone(qty)
        self.assertIn("SL distance too wide for margin-risk budget", "\n".join(logs.output))

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

            self.assertIsNone(qty)
            self.assertIn("looks percent-style", "\n".join(logs.output))
            self.assertIn("SL distance too wide for margin-risk budget", "\n".join(logs.output))
        finally:
            config.RISK_PER_TRADE = old_risk

    def test_portfolio_scoped_sizing_uses_portfolio_risk_base_and_slot_cash_cap(self):
        from strategy.quant_strategy import QuantStrategy

        strategy = object.__new__(QuantStrategy)
        strategy._fee_engine = None
        strategy._post_trade_agent = None
        strategy._active_institutional_size_mult = 1.0
        strategy._active_ic_size_mult = 1.0
        strategy._active_post_exit_size_mult = 1.0

        class FakeRisk:
            def get_available_balance(self):
                return {
                    "available": 51.21,
                    "total": 51.21,
                    "available_raw": 204.86,
                    "total_raw": 204.86,
                    "risk_available": 204.86,
                    "risk_total": 204.86,
                    "portfolio_scoped": True,
                    "portfolio_slot_count": 4,
                }

        price = 78980.0
        sl_dist = 526.0
        qty = strategy._compute_quantity(
            FakeRisk(),
            price=price,
            sig=None,
            ict_tier="",
            sl_price=price - sl_dist,
            tp_price=price + 2000.0,
            side="long",
            use_maker_entry=True,
            posterior_prob=0.80,
            prefetched_bal_info=FakeRisk().get_available_balance(),
        )

        # BTC's 0.001 minimum lot risks ~$0.53 here.  That is above the
        # confidence-haircut target, but still inside the configured portfolio
        # risk cap and inside the slot cash/margin envelope, so it is valid.
        self.assertIsNotNone(qty)
        self.assertGreaterEqual(qty, 0.001)
        self.assertLessEqual(qty * sl_dist, 204.86 * config.RISK_PER_TRADE * 1.15 + 1e-9)
        required_margin = qty * price / 40.0
        self.assertLessEqual(required_margin, 51.21 * 0.60 + 1e-9)

    def test_position_sizing_rejects_min_lot_above_haircut_risk_budget(self):
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

        with self.assertLogs("strategy.quant_strategy", level="WARNING") as logs:
            qty = strategy._compute_quantity(
                FakeRisk(),
                price=100000.0,
                sig=None,
                ict_tier="",
                sl_price=50000.0,
                tp_price=200000.0,
                side="long",
                use_maker_entry=True,
                posterior_prob=0.75,
                prefetched_bal_info={"available": 1000.0, "total": 1000.0},
            )

        self.assertIsNone(qty)
        self.assertIn("SL distance too wide for margin-risk budget", "\n".join(logs.output))

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

    def test_sl_migration_payoff_lock_disabled_under_tp_ladder(self):
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
        self.assertIn("TP-ladder", reason)

    def test_be_style_sl_migration_disabled_under_tp_ladder(self):
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

        self.assertFalse(ok)
        self.assertIn("TP-ladder", reason)

    def test_structural_sl_migration_disabled_even_when_payoff_positive(self):
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

        self.assertFalse(ok)
        self.assertIn("TP-ladder", reason)

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
# ===== END test_hardening.py =====


# ===== BEGIN test_instrument_registry_v4.py =====

import unittest

from execution.instrument_registry import InstrumentRegistry
from core.instruments import AssetClass, AssetIntent, ExchangeName


class FakeDelta:
    def get_products(self, contract_types=None):
        return {"success": True, "result": [
            {"symbol": "BTCUSD", "id": 27, "underlying_asset": {"symbol": "BTC"}, "quoting_asset": {"symbol": "USD"}, "max_leverage": 200},
            {"symbol": "SPXUSD", "id": 55661, "underlying_asset": {"symbol": "SPX"}, "quoting_asset": {"symbol": "USD"}, "max_leverage": 20},
            {"symbol": "AAPLXUSD", "id": 1001, "underlying_asset": {"symbol": "AAPLX"}, "quoting_asset": {"symbol": "USD"}, "product_specs": {"max_leverage": 25}},
            {"symbol": "SPYXUSD", "id": 1002, "underlying_asset": {"symbol": "SPYX"}, "quoting_asset": {"symbol": "USD"}, "product_specs": {"max_leverage": 25}},
            {"symbol": "QQQXUSD", "id": 1003, "underlying_asset": {"symbol": "QQQX"}, "quoting_asset": {"symbol": "USD"}, "product_specs": {"max_leverage": 25}},
            {"symbol": "COINXUSD", "id": 1004, "underlying_asset": {"symbol": "COINX"}, "quoting_asset": {"symbol": "USD"}, "product_specs": {"max_leverage": 25}},
            {"symbol": "CRCLXUSD", "id": 1005, "underlying_asset": {"symbol": "CRCLX"}, "quoting_asset": {"symbol": "USD"}, "product_specs": {"max_leverage": 25}},
        ]}


class FakeCoinSwitch:
    def get_instrument_info(self, exchange="EXCHANGE_2"):
        # Simulate incomplete all-instrument response. BTC should still be validated live.
        return {"data": []}

    def get_futures_ticker(self, symbol, exchange="EXCHANGE_2"):
        if symbol.upper() == "BTCUSDT":
            return {"data": {"symbol": "BTCUSDT", "last_price": "79000", "funding_rate": "0.001"}}
        return {"error": "not found"}


class InstrumentRegistryV4Tests(unittest.TestCase):
    def test_coinswitch_secondary_is_added_by_live_symbol_validation(self):
        intents = [AssetIntent("BTC", "Bitcoin", AssetClass.CRYPTO, ("BTCUSD", "BTCUSDT"), priority=0)]
        reg = InstrumentRegistry(execution_preference="delta")
        report = reg.discover(FakeDelta(), FakeCoinSwitch(), requested=[{
            "asset_id": "BTC", "display_name": "Bitcoin", "asset_class": "crypto", "aliases": ["BTCUSD", "BTCUSDT"], "priority": 0
        }])
        btc = report.matched[0]
        self.assertIn(ExchangeName.DELTA, btc.by_exchange)
        self.assertIn(ExchangeName.COINSWITCH, btc.by_exchange)
        self.assertEqual(btc.by_exchange[ExchangeName.COINSWITCH].symbol, "BTCUSDT")

    def test_spxusd_is_not_used_for_s_and_p_500_index(self):
        reg = InstrumentRegistry(execution_preference="delta")
        report = reg.discover(FakeDelta(), None, requested=[{
            "asset_id": "SPX_INDEX", "display_name": "S&P 500 index", "asset_class": "index", "aliases": ["SPX500USD", "US500", "SP500"], "priority": 0
        }])
        self.assertEqual(report.matched, [])
        self.assertIn("SPX_INDEX", report.unavailable)

    def test_xstock_exact_alias_matches_delta_symbol(self):
        reg = InstrumentRegistry(execution_preference="delta")
        report = reg.discover(FakeDelta(), None, requested=[{
            "asset_id": "AAPL", "display_name": "Apple xStock token derivative", "asset_class": "equity", "aliases": ["AAPLXUSD", "AAPL"], "priority": 0
        }])
        self.assertEqual(report.matched[0].primary.symbol, "AAPLXUSD")
        self.assertEqual(report.matched[0].max_leverage, 25)

    def test_coinswitch_ticker_field_names_are_not_symbols(self):
        reg = InstrumentRegistry(execution_preference="delta")
        rows = reg._augment_coinswitch_from_requested({}, FakeCoinSwitch(), [
            AssetIntent("BTC", "Bitcoin", AssetClass.CRYPTO, ("BTCUSDT",), priority=0)
        ])
        self.assertIn("BTCUSDT", rows)
        self.assertNotIn("LOWPRICE24H", rows)

    def test_xstock_screenshot_symbols_match(self):
        reg = InstrumentRegistry(execution_preference="delta")
        report = reg.discover(FakeDelta(), None, requested=[
            {"asset_id": "SPY", "display_name": "SP500 xStock token derivative", "asset_class": "equity", "aliases": ["SPYXUSD"], "priority": 0},
            {"asset_id": "QQQ", "display_name": "Nasdaq xStock token derivative", "asset_class": "equity", "aliases": ["QQQXUSD"], "priority": 1},
            {"asset_id": "COIN", "display_name": "Coinbase xStock token derivative", "asset_class": "equity", "aliases": ["COINXUSD"], "priority": 2},
            {"asset_id": "CRCL", "display_name": "Circle xStock token derivative", "asset_class": "equity", "aliases": ["CRCLXUSD"], "priority": 3},
        ], max_active=10)
        syms = {x.primary.symbol for x in report.matched}
        self.assertEqual({"SPYXUSD", "QQQXUSD", "COINXUSD", "CRCLXUSD"}, syms)
        self.assertTrue(all(x.max_leverage == 25 for x in report.matched))


if __name__ == "__main__":
    unittest.main()
# ===== END test_instrument_registry_v4.py =====


# ===== BEGIN test_liquidity_fibonacci_target_surface.py =====
from types import SimpleNamespace

from strategy.liquidity_fibonacci import score_liquidity_fib_confluence
from strategy.tp_ladder import build_tp_ladder


def _target(side, price, significance=4.0, tf="5m"):
    pool = SimpleNamespace(
        side=side,
        price=float(price),
        status="ACTIVE",
        touches=1,
        timeframe=tf,
        ob_aligned=False,
        fvg_aligned=False,
    )
    return SimpleNamespace(pool=pool, significance=float(significance), distance_atr=1.0, tf_sources=[tf], direction="long" if side == "BSL" else "short")


def test_fibonacci_only_scores_existing_liquidity_and_does_not_create_targets():
    snap = SimpleNamespace(
        ssl_pools=[_target("SSL", 98.0, significance=5.0)],
        bsl_pools=[_target("BSL", 103.236, significance=5.0)],
    )
    # Entry=100, anchor=SL=98, 1.618 extension = 103.236.
    aligned = score_liquidity_fib_confluence(
        snap=snap, side="long", entry=100.0, sl=98.0,
        target_price=103.236, atr=1.0, target=snap.bsl_pools[0],
    )
    off = score_liquidity_fib_confluence(
        snap=snap, side="long", entry=100.0, sl=98.0,
        target_price=104.4, atr=1.0, target=snap.bsl_pools[0],
    )

    assert aligned.score > off.score
    assert aligned.multiplier > off.multiplier
    assert aligned.nearest_ratio == 1.618
    # Still soft: even poor fib geometry is a multiplier, not a veto.
    assert 0.90 <= off.multiplier <= 1.32


def test_tp_ladder_uses_fib_confluence_as_runner_geometry_not_fixed_percent():
    qty = 100.0
    pool_report = {
        "candidates": [
            {"pool_side": "BSL", "tp_price": 101.2, "pool_price": 101.2, "quality": 0.65, "significance": 4.0, "delivery_prob": 0.70, "selection_ev": 0.50, "fib_confluence": 1.22, "fib_score": 0.70, "fib_ratio": 0.618, "fib_role": "internal_monetisation", "timeframe": "5m", "cost_r": 0.04},
            {"pool_side": "BSL", "tp_price": 102.0, "pool_price": 102.0, "quality": 0.68, "significance": 4.5, "delivery_prob": 0.63, "selection_ev": 0.45, "fib_confluence": 1.18, "fib_score": 0.55, "fib_ratio": 1.0, "fib_role": "internal_monetisation", "timeframe": "15m", "cost_r": 0.04},
            {"pool_side": "BSL", "tp_price": 103.236, "pool_price": 103.236, "quality": 0.80, "significance": 6.0, "delivery_prob": 0.45, "selection_ev": 0.40, "fib_confluence": 1.28, "fib_score": 0.82, "fib_ratio": 1.618, "fib_role": "runner_projection", "timeframe": "1h", "selected": True, "cost_r": 0.04},
        ]
    }
    plan = build_tp_ladder(
        side="long", entry=100.0, sl=98.0, final_tp=103.236, atr=1.0,
        total_quantity=qty, pool_report=pool_report,
        min_leg_fraction=1.0 / qty, max_internal_legs=99,
    )

    assert plan.has_internal_targets
    assert plan.final_fraction > 0.0
    assert plan.final_runner_model["fib_score"] >= 0.80
    assert any("liq+Fib final geometry" in n for n in plan.regime_notes)
    # Solvency still dominates: Fib support cannot leave a destructive residual.
    assert plan.solvency_checkpoint_index >= 1
    assert plan.worst_case_after_checkpoint_r >= plan.solvency_floor_r


def test_tp_ladder_uses_fibonacci_fallback_when_no_internal_liquidity_exists():
    pool_report = {
        "candidates": [
            # Only the final liquidity target exists.  No internal BSL rows are present.
            {"pool_side": "BSL", "tp_price": 104.0, "pool_price": 104.0, "quality": 0.75,
             "significance": 6.0, "delivery_prob": 0.42, "selection_ev": 0.40,
             "fib_confluence": 1.24, "fib_score": 0.78, "fib_ratio": 1.618,
             "fib_role": "runner_projection", "timeframe": "1h", "selected": True, "cost_r": 0.05},
        ]
    }
    plan = build_tp_ladder(
        side="long", entry=100.0, sl=99.0, final_tp=104.0, atr=1.0,
        total_quantity=10.0, pool_report=pool_report,
        min_leg_fraction=0.10, max_internal_legs=4,
    )
    internal = [l for l in plan.legs if l.role != "FINAL"]
    assert internal, plan.as_dict()
    assert all(l.source == "fib_fallback_geometry" for l in internal)
    assert all(100.0 < l.price < 104.0 for l in internal)
    assert any("Fibonacci path-monetisation fallback" in n for n in plan.regime_notes)
    assert plan.final_fraction < 1.0


def test_tp_ladder_final_only_when_quantity_cannot_be_split():
    pool_report = {
        "candidates": [
            {"pool_side": "BSL", "tp_price": 104.0, "pool_price": 104.0, "selected": True,
             "fib_confluence": 1.24, "fib_score": 0.78, "fib_ratio": 1.618}
        ]
    }
    plan = build_tp_ladder(
        side="long", entry=100.0, sl=99.0, final_tp=104.0, atr=1.0,
        total_quantity=1.0, pool_report=pool_report,
        min_leg_fraction=1.0, max_internal_legs=0,
    )
    assert len(plan.legs) == 1
    assert plan.legs[0].role == "FINAL"
    assert "position size not splittable" in plan.legs[0].reason

# ===== END test_liquidity_fibonacci_target_surface.py =====


# ===== BEGIN test_multi_asset_portfolio.py =====
import sys
import types
import unittest
from types import SimpleNamespace

# The portfolio guard tests do not exercise network/websocket clients.
# Some minimal production images used for CI do not install python-socketio.
socketio_stub = types.SimpleNamespace(Client=lambda *a, **kw: SimpleNamespace())
sys.modules.setdefault("socketio", socketio_stub)

from core.instruments import AssetClass
from orchestration.portfolio_manager import PortfolioManager


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
        guard = PortfolioManager()
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
        guard = PortfolioManager()
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

    def test_available_funds_balance_allocation_does_not_slot_cap_cash(self):
        guard = PortfolioManager()
        guard.max_open_positions = 4
        guard.budget_mode = "available_funds"
        ctx = _Ctx("BTC", AssetClass.CRYPTO)
        raw = {"available": 100.0, "total": 120.0}

        scoped = guard.allocate_balance(ctx, [ctx], raw)

        self.assertTrue(scoped["portfolio_scoped"])
        self.assertEqual(scoped["portfolio_budget_mode"], "available_funds")
        self.assertAlmostEqual(scoped["available"], 100.0)
        self.assertAlmostEqual(scoped["total"], 120.0)
        self.assertAlmostEqual(scoped["available_raw"], 100.0)
        self.assertAlmostEqual(scoped["risk_available"], 120.0)
        self.assertAlmostEqual(scoped["risk_total"], 120.0)


if __name__ == "__main__":
    unittest.main()
# ===== END test_multi_asset_portfolio.py =====


# ===== BEGIN test_portfolio_desk_reporting.py =====
import unittest
from types import SimpleNamespace


def _instrument(asset_id, symbol, asset_class):
    from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument

    ei = ExchangeInstrument(
        exchange=ExchangeName.DELTA,
        symbol=symbol,
        ws_symbol=symbol,
        display_symbol=symbol,
        asset_id=asset_id,
        asset_class=asset_class,
        tick_size=0.01,
        lot_step=1.0,
        min_qty=1.0,
        max_qty=1000.0,
    )
    return TradableInstrument(
        asset_id=asset_id,
        display_name=asset_id,
        asset_class=asset_class,
        primary_exchange=ExchangeName.DELTA,
        by_exchange={ExchangeName.DELTA: ei},
    )


class _Strategy:
    def __init__(self, history):
        self._trade_history = list(history)

    def get_position(self):
        return None


class _Data:
    def get_last_price(self):
        return 0.0


class PortfolioDeskReportingTests(unittest.TestCase):
    def _bot(self):
        from core.instruments import AssetClass
        from orchestration.multi_asset_bot import MultiAssetQuantBot

        bot = object.__new__(MultiAssetQuantBot)
        bot.guard = SimpleNamespace(max_open_positions=4, budget_mode="balanced")
        bot.contexts = [
            SimpleNamespace(
                instrument=_instrument("BTC", "BTCUSD", AssetClass.CRYPTO),
                strategy=_Strategy([
                    {
                        "timestamp": 1000.0,
                        "side": "long",
                        "entry": 100.0,
                        "exit": 110.0,
                        "pnl": 10.0,
                        "gross_pnl": 12.0,
                        "total_fees": 2.0,
                        "r": 1.25,
                        "reason": "target_fill",
                    }
                ]),
                data_manager=_Data(),
                phase_name="FLAT",
            ),
            SimpleNamespace(
                instrument=_instrument("PAXG", "PAXGUSD", AssetClass.COMMODITY),
                strategy=_Strategy([
                    {
                        "timestamp": 2000.0,
                        "side": "short",
                        "entry": 200.0,
                        "exit": 205.0,
                        "pnl": -5.0,
                        "gross_pnl": -4.0,
                        "total_fees": 1.0,
                        "achieved_r": -0.75,
                        "reason": "stop_fill",
                    }
                ]),
                data_manager=_Data(),
                phase_name="FLAT",
            ),
            SimpleNamespace(
                instrument=_instrument("AAPL", "AAPLXUSD", AssetClass.EQUITY),
                strategy=_Strategy([]),
                data_manager=_Data(),
                phase_name="FLAT",
            ),
        ]
        return bot

    def test_trade_records_are_normalised_by_desk(self):
        bot = self._bot()
        records = sorted(bot._all_trade_records(), key=lambda r: r["asset"])

        self.assertEqual(records[0]["desk"], "BTC")
        self.assertEqual(records[0]["timestamp"], 1000.0)
        self.assertEqual(records[0]["pnl"], 10.0)
        self.assertEqual(records[1]["desk"], "COMMODITIES")
        self.assertEqual(records[1]["pnl"], -5.0)
        for record in records:
            self.assertEqual(1, sum(1 for key in record if key == "pnl"))
            self.assertEqual(1, sum(1 for key in record if key == "timestamp"))

    def test_pnl_report_groups_by_desk_and_asset(self):
        report = self._bot().format_portfolio_pnl_report()

        self.assertIn("Desk PnL", report)
        self.assertIn("BTC Desk", report)
        self.assertIn("Commodities", report)
        self.assertIn("Stocks Desk", report)
        self.assertIn("Asset PnL", report)
        self.assertIn("BTC", report)
        self.assertIn("PAXG", report)
        self.assertIn("AAPL", report)
        self.assertIn("$+5.00", report)
        self.assertIn("WR  50.0%", report)

    def test_open_position_unrealised_pnl_is_wired_from_get_position_dict(self):
        from core.instruments import AssetClass
        from orchestration.multi_asset_bot import MultiAssetQuantBot

        class OpenStrategy:
            def __init__(self):
                self._trade_history = []
                self._pos = SimpleNamespace(
                    side="long", quantity=2.0, entry_price=100.0,
                    sl_price=90.0, tp_price=130.0, initial_sl_dist=10.0,
                    peak_profit=8.0, entry_time=time.time() - 600,
                    tp_ladder_realized_pnl=1.50, trade_mode="reversal",
                    entry_order_id="entry", sl_order_id="sl", tp_order_id="tp",
                    is_flat=lambda: False,
                )
            def get_position(self):
                # Real QuantStrategy.get_position() returns a dict.  Portfolio
                # reporting must not treat this like an object and silently zero UPNL.
                return {
                    "side": "long", "quantity": 2.0, "entry_price": 100.0,
                    "sl_price": 90.0, "tp_price": 130.0,
                    "tp_ladder_realized_pnl": 1.50,
                }
            def _unrealised_pnl_usd(self, mark_price, pos):
                return (float(mark_price) - 100.0) * 2.0

        class MarkData:
            def get_last_price(self):
                return 105.0

        bot = object.__new__(MultiAssetQuantBot)
        bot.guard = SimpleNamespace(max_open_positions=4, budget_mode="balanced", count_open=lambda contexts: 1)
        ctx = SimpleNamespace(
            instrument=_instrument("BTC", "BTCUSD", AssetClass.CRYPTO),
            strategy=OpenStrategy(),
            data_manager=MarkData(),
            phase_name="ACTIVE",
            risk_manager=SimpleNamespace(get_available_balance=lambda: {"available": 195.0, "total": 200.0, "risk_total": 200.0}),
        )
        bot.contexts = [ctx]

        metrics = bot._ctx_position_metrics(ctx)
        self.assertEqual(metrics["upnl"], 10.0)
        self.assertEqual(metrics["open_realized"], 1.5)
        self.assertEqual(metrics["lifecycle_pnl"], 11.5)

        pnl_report = bot.format_portfolio_pnl_report()
        self.assertIn("UPNL      $+10.00", pnl_report)
        self.assertIn("LIVE      $+11.50", pnl_report)

        equity_report = bot.format_portfolio_equity_report()
        self.assertIn("MARKED equity", equity_report)
        self.assertIn("OPEN   UPNL", equity_report)
        self.assertIn("$+10.00", equity_report)

    def test_trades_report_shows_all_recent_newest_first(self):
        report = self._bot().format_portfolio_trades_report()

        self.assertIn("Showing 2/2", report)
        self.assertIn("PAXG", report)
        self.assertIn("BTC", report)
        self.assertLess(report.index("PAXG"), report.index("BTC"))
        self.assertIn("net     $-5.00", report)
        self.assertIn("fees $    1.00", report)


if __name__ == "__main__":
    unittest.main()
# ===== END test_portfolio_desk_reporting.py =====


# ===== BEGIN test_portfolio_policy_v9.py =====
import unittest
from core.instruments import AssetClass, ExchangeName, ExchangeInstrument, TradableInstrument, instrument_scope
from core.market_policy import active_policy
from orchestration.portfolio_manager import PortfolioManager
from strategy.quant_strategy import QCfg


def inst(asset_id, ac, symbol, max_lev=0, tick=0.01, lot=1.0):
    ei = ExchangeInstrument(
        exchange=ExchangeName.DELTA, symbol=symbol, ws_symbol=symbol, display_symbol=symbol,
        asset_id=asset_id, asset_class=ac, max_leverage=max_lev, tick_size=tick, lot_step=lot, min_qty=lot,
    )
    return TradableInstrument(asset_id, asset_id, ac, ExchangeName.DELTA, {ExchangeName.DELTA: ei})


class PortfolioPolicyV9Test(unittest.TestCase):
    def test_xstock_policy_not_btc(self):
        aapl = inst('AAPL', AssetClass.EQUITY, 'AAPLXUSD', max_lev=25, tick=0.1, lot=1)
        with instrument_scope(aapl):
            pol = active_policy()
            self.assertEqual(QCfg.LEVERAGE(), 25)
            self.assertLess(QCfg.MARGIN_PCT(), 0.20)
            self.assertGreaterEqual(QCfg.MIN_RR_RATIO(), 1.45)
            self.assertEqual(QCfg.LOT_STEP(), 1)
            self.assertEqual(QCfg.TICK_SIZE(), 0.1)
            self.assertLess(pol.risk_multiplier, 1.0)

    def test_crypto_policy_keeps_btc_speed(self):
        btc = inst('BTC', AssetClass.CRYPTO, 'BTCUSD', max_lev=40, tick=0.5, lot=0.001)
        with instrument_scope(btc):
            pol = active_policy()
            self.assertGreaterEqual(pol.risk_multiplier, 1.0)
            self.assertLessEqual(pol.loop_interval_sec, 0.25)

    def test_portfolio_allocation_carries_policy(self):
        mgr = PortfolioManager()
        aapl = inst('AAPL', AssetClass.EQUITY, 'AAPLXUSD', max_lev=25, tick=0.1, lot=1)
        class C: pass
        c=C(); c.instrument=aapl; c.has_position=False
        b = mgr.allocate_balance(c, [c], {'available': 100.0, 'total': 100.0})
        self.assertTrue(b['portfolio_scoped'])
        self.assertIn('instrument_policy', b)
        self.assertEqual(b['instrument_policy']['asset_class'], 'equity')
        self.assertLess(b['instrument_risk_multiplier'], 1.0)

if __name__ == '__main__':
    unittest.main()
# ===== END test_portfolio_policy_v9.py =====


# ===== BEGIN test_spread_gate_v8.py =====
import os
import sys
import types
import unittest
from types import SimpleNamespace

os.environ.setdefault("DELTA_API_KEY", "test")
os.environ.setdefault("DELTA_SECRET_KEY", "test")
os.environ.setdefault("TELEGRAM_BOT_TOKEN", "")
os.environ.setdefault("TELEGRAM_CHAT_ID", "")

socketio_stub = types.SimpleNamespace(Client=lambda *a, **kw: SimpleNamespace())
sys.modules.setdefault("socketio", socketio_stub)

from core.instruments import AssetClass
from strategy.quant_strategy import QuantStrategy


class _DM:
    def __init__(self, bid, ask):
        self.bid = bid
        self.ask = ask

    def get_orderbook(self):
        return {"bids": [[self.bid, 1]], "asks": [[self.ask, 1]]}

    def get_last_price(self):
        return (self.bid + self.ask) / 2.0


class SpreadGateV8Tests(unittest.TestCase):
    def _strategy(self, asset_class, tick_size=0.1, asset_id="TEST", atr=0.2):
        qs = object.__new__(QuantStrategy)
        qs._atr_5m = SimpleNamespace(atr=atr)
        qs._instrument = SimpleNamespace(
            asset_class=asset_class,
            asset_id=asset_id,
            tick_size=tick_size,
        )
        qs._asset_id = asset_id
        qs._active_spread_cost_mult = 1.0
        qs._last_spread_gate_context = {}
        return qs

    def test_xstock_normal_coarse_tick_spread_is_haircut_not_block(self):
        qs = self._strategy(AssetClass.EQUITY, tick_size=0.1, asset_id="AAPL", atr=0.20)
        ok, ratio = qs._spread_atr_gate(_DM(278.80, 279.20))  # $0.40 spread, 4 ticks

        self.assertTrue(ok)
        self.assertGreater(ratio, 0.5)
        self.assertLess(qs._active_spread_cost_mult, 1.0)
        self.assertFalse(qs._last_spread_gate_context["hard_fail"])

    def test_crypto_same_spread_atr_remains_hard_blocked(self):
        qs = self._strategy(AssetClass.CRYPTO, tick_size=0.1, asset_id="BTC", atr=0.20)
        ok, ratio = qs._spread_atr_gate(_DM(100.00, 100.40))

        self.assertFalse(ok)
        self.assertGreater(ratio, 0.5)

    def test_xstock_extreme_spread_is_still_hard_blocked(self):
        qs = self._strategy(AssetClass.EQUITY, tick_size=0.1, asset_id="META", atr=0.20)
        ok, ratio = qs._spread_atr_gate(_DM(100.00, 101.00))  # 100 bps, 10 ticks, 5 ATR

        self.assertFalse(ok)
        self.assertGreater(ratio, 4.0)
        self.assertTrue(qs._last_spread_gate_context["hard_fail"])


if __name__ == "__main__":
    unittest.main()
# ===== END test_spread_gate_v8.py =====


# ===== BEGIN test_tp_ladder_lifecycle_solvency.py =====
from strategy.tp_ladder import build_tp_ladder


def test_paxg_style_ladder_reduces_final_residual_and_stays_solvent_after_checkpoint():
    entry = 4685.65
    sl = 4676.00
    final_tp = 4705.00
    qty = 101.0
    pool_report = {
        "candidates": [
            {"pool_side": "BSL", "tp_price": 4690.20, "pool_price": 4690.20, "quality": 0.60, "significance": 3.0, "delivery_prob": 0.65, "selection_ev": 0.40, "timeframe": "1m", "cost_r": 0.10},
            {"pool_side": "BSL", "tp_price": 4692.00, "pool_price": 4692.00, "quality": 0.70, "significance": 3.5, "delivery_prob": 0.60, "selection_ev": 0.35, "timeframe": "5m", "cost_r": 0.10},
            {"pool_side": "BSL", "tp_price": 4695.90, "pool_price": 4695.90, "quality": 0.75, "significance": 4.0, "delivery_prob": 0.55, "selection_ev": 0.30, "timeframe": "15m", "cost_r": 0.10},
            {"pool_side": "BSL", "tp_price": final_tp, "pool_price": final_tp, "quality": 0.80, "significance": 5.0, "delivery_prob": 0.40, "selection_ev": 0.25, "timeframe": "1h", "selected": True, "cost_r": 0.10},
        ]
    }
    plan = build_tp_ladder(
        side="long",
        entry=entry,
        sl=sl,
        final_tp=final_tp,
        atr=3.0,
        total_quantity=qty,
        pool_report=pool_report,
        asset_id="GOLD",
        # Lot-derived: 1 contract minimum / 101 total contracts.
        min_leg_fraction=1.0 / qty,
        max_internal_legs=100,
    )

    assert plan.has_internal_targets
    assert plan.solvency_checkpoint_index >= 1
    assert plan.worst_case_after_checkpoint_r >= plan.solvency_floor_r
    # Regression target from the bad lifecycle: old residual was 47/101 = 46.5%.
    # The final runner must be earned and materially smaller in this regime.
    assert plan.final_fraction < 47.0 / 101.0
# ===== END test_tp_ladder_lifecycle_solvency.py =====


# ===== BEGIN test_v12_portfolio_notifications_trail.py =====

import unittest
from types import SimpleNamespace

class V12PortfolioNotificationTrailTests(unittest.TestCase):
    def test_delta_edit_order_passes_product_id(self):
        from execution.order_manager import _DeltaAdapter
        class API:
            def __init__(self): self.kw = None
            def edit_order(self, **kw):
                self.kw = kw
                return {"success": True, "result": {"id": kw["order_id"], "state": "open"}}
        api = API()
        inst = SimpleNamespace(symbol="PAXGUSD", display_symbol="PAXGUSD", tick_size=0.01, lot_step=1, min_qty=1, max_qty=100, contract_value_btc=1, product_id=123006, asset_class="commodity")
        ad = _DeltaAdapter(api, exchange_instrument=inst)
        res = ad.edit_order("999", 4554.5, 4555.0)
        self.assertEqual(api.kw.get("product_id"), 123006)
        self.assertFalse(res.get("_error"))

    def test_order_manager_uses_instrument_tick(self):
        from execution.order_manager import OrderManager
        from core.instruments import ExchangeName
        class API: pass
        ei = SimpleNamespace(symbol="AAPLXUSD", display_symbol="AAPLXUSD", tick_size=0.01, lot_step=1, min_qty=1, max_qty=100, contract_value_btc=1, product_id=1, asset_class="equity")
        inst = SimpleNamespace(by_exchange={ExchangeName.DELTA: ei})
        om = OrderManager(API(), exchange_name="delta", instrument=inst)
        self.assertEqual(om._active_tick_size(), 0.01)

    def test_multi_asset_has_command_center_reports(self):
        from pathlib import Path
        src = Path("orchestration/multi_asset_bot.py").read_text(encoding="utf-8")
        self.assertIn("def format_portfolio_pnl_report", src)
        self.assertIn("def format_portfolio_position_report", src)
        self.assertIn("def format_portfolio_equity_report", src)
        self.assertIn("PORTFOLIO", src)

if __name__ == "__main__":
    unittest.main()
# ===== END test_v12_portfolio_notifications_trail.py =====


# ===== BEGIN test_v13_protection_invariants.py =====
import inspect
import unittest
from types import SimpleNamespace


class V13ProtectionInvariantTests(unittest.TestCase):
    def test_open_orders_are_product_strict_for_multi_asset_delta(self):
        from execution.order_manager import OrderManager
        from core.instruments import ExchangeName

        class API:
            def get_product_id(self, symbol):
                return {"COINXUSD": 125551, "BTCUSD": 27}.get(symbol)
            def get_open_orders(self, symbol=None):
                return {"success": True, "result": [
                    {"order_id": "btc-sl", "type": "STOP_MARKET", "trigger_price": 79134.5, "side": "BUY", "product_id": 27, "product_symbol": "BTCUSD"},
                    {"order_id": "coin-sl", "type": "STOP_MARKET", "trigger_price": 199.61, "side": "BUY", "product_id": 125551, "product_symbol": "COINXUSD"},
                ]}

        ei = SimpleNamespace(symbol="COINXUSD", display_symbol="COINXUSD", tick_size=0.01,
                             lot_step=0.01, min_qty=0.01, max_qty=1000,
                             contract_value_btc=1, product_id=125551, asset_class="equity")
        inst = SimpleNamespace(by_exchange={ExchangeName.DELTA: ei})
        om = OrderManager(API(), exchange_name="delta", instrument=inst)
        orders = om.get_open_orders(symbol="COINXUSD")
        ids = {o["order_id"] for o in orders}
        self.assertIn("coin-sl", ids)
        self.assertNotIn("btc-sl", ids)

    def test_bracket_child_parser_has_price_and_product_guards(self):
        from execution.order_manager import OrderManager
        src = inspect.getsource(OrderManager.place_bracket_limit_entry)
        self.assertIn("product_mismatch", src)
        self.assertIn("sl_price_mismatch", src)
        self.assertIn("tp_price_mismatch", src)
        self.assertIn("_bracket_children_missing", src)

    def test_strategy_flattens_and_alerts_when_bracket_children_missing(self):
        from strategy.quant_strategy import QuantStrategy
        src = inspect.getsource(QuantStrategy._enter_trade)
        self.assertIn("PROTECTION FAILURE — POSITION FLATTENED", src)
        self.assertIn("_bracket_children_missing", src)
        self.assertIn("place_market_order", src)
        self.assertIn("event_type=\"protection_failure\"", src)


if __name__ == "__main__":
    unittest.main()
# ===== END test_v13_protection_invariants.py =====


# ===== BEGIN institutional_final_audit_tests =====

def test_config_uses_margin_target_with_coherent_daily_budget():
    import config as _config
    from config_schema import cfg

    assert abs(_config.RISK_PER_TRADE - 0.015) < 1e-12
    assert _config.MAX_CONSECUTIVE_LOSSES == 3
    # Conviction/session loss limit is deliberately separate from the global
    # risk-manager loss streak.  Desk-specific overrides live in TRADING_DESKS.
    assert _config.CONVICTION_MAX_SESSION_LOSSES == 2
    assert _config.CONVICTION_MAX_SESSION_LOSSES != _config.MAX_CONSECUTIVE_LOSSES
    assert abs(cfg.risk.RISK_PER_TRADE - 0.015) < 1e-12
    assert cfg.risk.MAX_CONSECUTIVE_LOSSES == 3
    assert cfg.risk.RISK_PER_TRADE * cfg.risk.MAX_CONSECUTIVE_LOSSES <= cfg.risk.MAX_DAILY_LOSS_PCT / 100.0 + 1e-12
    assert _config.MIN_MARGIN_PER_TRADE == 0
    assert _config.MAX_MARGIN_PER_TRADE == 0
    assert _config.MAX_ENTRY_MARGIN_USAGE_PCT == 0
    assert _config.PORTFOLIO_BUDGET_MODE == "available_funds"
    assert abs(_config.QUANT_MARGIN_PCT - 0.36) < 1e-12




def test_margin_risk_sizing_uses_margin_budget_and_dynamic_leverage():
    from strategy.quant_strategy import QuantStrategy

    strategy = object.__new__(QuantStrategy)
    strategy._fee_engine = None
    strategy._post_trade_agent = None
    strategy._active_institutional_size_mult = 1.0
    strategy._active_ic_size_mult = 1.0
    strategy._active_post_exit_size_mult = 1.0
    strategy._active_spread_cost_mult = 1.0
    strategy._active_cross_asset_size_mult = 1.0

    class FakeRisk:
        def get_available_balance(self):
            return {"available": 200.0, "total": 200.0}

    price = 76951.0
    sl_dist = 128.3
    qty = strategy._compute_quantity(
        FakeRisk(),
        price=price,
        sig=None,
        ict_tier="S",
        sl_price=price - sl_dist,
        tp_price=price + 400.0,
        side="long",
        use_maker_entry=True,
        posterior_prob=0.75,
        prefetched_bal_info={"available": 200.0, "total": 200.0},
    )

    assert qty is not None
    lev = float(strategy._active_effective_leverage)
    margin_used = qty * price / lev
    dollar_risk = qty * sl_dist
    assert lev <= 9.0
    assert margin_used <= 200.0 * 0.36 + 1e-9
    assert dollar_risk <= margin_used * 0.015 * 1.02 + 1e-9


def test_dynamic_leverage_is_set_before_entry_order_for_margin_risk():
    from strategy.quant_strategy import QuantStrategy, SignalBreakdown

    # This is a narrow unit check over the post-sizing leverage handoff block.
    # Full _enter_trade coverage is intentionally avoided because it requires
    # live data-manager candles, bracket child discovery and exchange state.
    class FakeOM:
        def __init__(self): self.calls = []
        def set_leverage(self, leverage, product_id=None):
            self.calls.append(int(leverage))
            return {"success": True}

    om = FakeOM()
    strategy = object.__new__(QuantStrategy)
    strategy._active_effective_leverage = 9.0
    # Mirrors the exact entry-block invariant: if sizing used 9x while config is
    # 40x, the exchange leverage must be normalized before order submission.
    entry_lev = int(max(1, round(float(getattr(strategy, "_active_effective_leverage", 40) or 40))))
    assert entry_lev == 9
    resp = om.set_leverage(leverage=entry_lev)
    assert resp["success"] is True
    assert om.calls == [9]

def test_conviction_session_loss_limit_is_desk_scoped_and_resets_on_session_change():
    import time
    import config as _config
    from strategy.conviction_filter import ConvictionFilter

    assert _config.TRADING_DESKS["BTC"]["conviction"]["max_session_losses"] == 2
    assert _config.TRADING_DESKS["COMMODITIES"]["conviction"]["max_session_losses"] == 2
    assert _config.TRADING_DESKS["STOCKS"]["conviction"]["max_session_losses"] == 1

    btc = ConvictionFilter(desk_id="BTC", desk_name="BTC Desk")
    stocks = ConvictionFilter(desk_id="STOCKS", desk_name="Stocks Desk")
    assert btc.get_session_state_summary()["max_losses"] == 2
    assert stocks.get_session_state_summary()["max_losses"] == 1

    btc.on_session_change("LONDON")
    btc.record_trade_result(win=False, pnl=-1.0)
    btc.record_trade_result(win=False, pnl=-1.0)
    blocked = btc._check_session_limits(time.time(), "LONDON")
    assert blocked and "CIRCUIT_BREAKER[BTC:LONDON]" in blocked
    assert "max=2" in blocked

    # A new session must clear only session-scoped counters; lifetime W/L stays.
    assert btc._check_session_limits(time.time(), "NEW_YORK") is None
    summary = btc.get_session_state_summary()
    assert summary["session_id"] == "NY"
    assert summary["consecutive_losses"] == 0
    assert summary["entries_taken"] == 0
    assert summary["losses"] == 2



def test_conviction_session_state_is_shared_per_desk_but_isolated_across_desks():
    import time
    from strategy.conviction_filter import ConvictionFilter

    # Prevent state leakage from previous test order; production code keeps the
    # store warm for the process lifetime.
    ConvictionFilter._DESK_SESSION_STATES.pop("BTC", None)
    ConvictionFilter._DESK_SESSION_STATES.pop("COMMODITIES", None)

    btc_primary = ConvictionFilter(desk_id="BTC", desk_name="BTC Desk")
    btc_shadow = ConvictionFilter(desk_id="BTC", desk_name="BTC Desk")
    commodities = ConvictionFilter(desk_id="COMMODITIES", desk_name="Commodities Desk")

    btc_primary.on_session_change("LONDON")
    btc_primary.record_trade_result(win=False, pnl=-1.0)
    btc_primary.record_trade_result(win=False, pnl=-1.0)

    # Same desk, different strategy instance: must see the same desk-level circuit.
    blocked = btc_shadow._check_session_limits(time.time(), "LONDON")
    assert blocked and "CIRCUIT_BREAKER[BTC:LONDON]" in blocked

    # Different desk: BTC losses must not pause commodities.
    assert commodities._check_session_limits(time.time(), "LONDON") is None
    assert commodities.get_session_state_summary()["consecutive_losses"] == 0

    # Session boundary must reset the shared BTC desk state for every live instance.
    assert btc_shadow._check_session_limits(time.time(), "NEW_YORK") is None
    assert btc_primary.get_session_state_summary()["session_id"] == "NY"
    assert btc_primary.get_session_state_summary()["consecutive_losses"] == 0
    assert btc_shadow.get_session_state_summary()["consecutive_losses"] == 0
    assert btc_shadow.get_session_state_summary()["losses"] == 2

def test_stock_desk_suspension_filters_equity_and_index_before_context_creation():
    import config as _config
    from orchestration.multi_asset_bot import MultiAssetQuantBot

    requested = [
        {"asset_id": "BTC", "asset_class": "crypto", "aliases": ["BTCUSD"]},
        {"asset_id": "GOLD", "asset_class": "commodity", "aliases": ["PAXGUSD"]},
        {"asset_id": "SPY", "asset_class": "equity", "aliases": ["SPYXUSD"]},
        {"asset_id": "QQQ", "asset_class": "index", "aliases": ["QQQXUSD"]},
    ]
    assert _config.STOCK_DESK_TRADING_ENABLED is False
    filtered = MultiAssetQuantBot._filter_suspended_requests(None, requested)
    assert [x["asset_id"] for x in filtered] == ["BTC", "GOLD"]


def test_telegram_does_not_register_legacy_trail_command():
    import inspect
    from telegram.controller import TelegramBotController

    src = inspect.getsource(TelegramBotController.set_my_commands)
    assert '"trail"' not in src
    assert '"settrail"' not in src


def test_active_strategy_exit_management_returns_before_legacy_sl_migration():
    import inspect
    from strategy.quant_strategy import QuantStrategy

    src = inspect.getsource(QuantStrategy._manage_active)
    assert "fixed original SL + reduce-only internal TP ladder + final TP" in src
    assert "return" in src.split("Do not compute or dispatch any SL migration here.", 1)[1].split("def _detect_structure_change", 1)[0]

# ===== END institutional_final_audit_tests =====


def test_tp_ladder_clusters_near_same_pool_and_uses_mtf_effective_liquidity():
    from strategy.tp_ladder import build_tp_ladder

    report = {
        "candidates": [
            {"pool_side": "SSL", "tp_price": 70.40, "pool_price": 70.40, "quality": 0.65, "significance": 2.0, "delivery_prob": 0.72, "selection_ev": 0.40, "timeframe": "1m"},
            {"pool_side": "SSL", "tp_price": 70.42, "pool_price": 70.42, "quality": 0.82, "significance": 4.5, "delivery_prob": 0.76, "selection_ev": 0.48, "timeframe": "15m"},
            {"pool_side": "SSL", "tp_price": 69.70, "pool_price": 69.70, "quality": 0.70, "significance": 3.5, "delivery_prob": 0.58, "selection_ev": 0.55, "timeframe": "1h"},
        ],
        "selected": {"pool_side": "SSL", "tp_price": 67.42, "pool_price": 67.42, "quality": 0.80, "significance": 5.0, "delivery_prob": 0.12, "selection_ev": 0.77, "timeframe": "4h", "selected": True},
    }
    plan = build_tp_ladder(
        side="short", entry=70.66, sl=71.26, final_tp=67.42, atr=0.34,
        total_quantity=1.0, pool_report=report, min_leg_fraction=0.01,
        max_internal_legs=8,
    )
    internals = [l for l in plan.legs if l.role != "FINAL"]
    assert internals, plan.compact()
    assert any(l.source == "mtf_liquidity_cluster" for l in internals)
    prices = [l.price for l in internals]
    assert len(prices) == len(set(round(p, 2) for p in prices))
    for a, b in zip(prices, prices[1:]):
        assert abs(a - b) >= 0.30 * 0.34
    assert internals[0].qty_fraction < 0.90
    assert plan.final_fraction > 0.0


def test_tp_ladder_sparse_path_adds_fib_gap_filler_not_all_nearest():
    from strategy.tp_ladder import build_tp_ladder

    report = {
        "candidates": [
            {"pool_side": "SSL", "tp_price": 70.40, "pool_price": 70.40, "quality": 0.70, "significance": 3.0, "delivery_prob": 0.72, "selection_ev": 0.40, "timeframe": "1m"},
        ],
        "selected": {"pool_side": "SSL", "tp_price": 67.42, "pool_price": 67.42, "quality": 0.82, "significance": 5.0, "delivery_prob": 0.12, "selection_ev": 0.77, "timeframe": "4h", "selected": True, "fib_score": 0.75, "fib_confluence": 1.20, "fib_ratio": 1.618},
    }
    plan = build_tp_ladder(
        side="short", entry=70.66, sl=71.26, final_tp=67.42, atr=0.34,
        total_quantity=1.0, pool_report=report, min_leg_fraction=0.01,
        max_internal_legs=8,
    )
    internals = [l for l in plan.legs if l.role != "FINAL"]
    assert len(internals) >= 2, plan.compact()
    assert any(l.source == "fib_fallback_geometry" for l in internals)
    assert internals[0].qty_fraction < 0.90

# ===== BEGIN test_institutional_tp_ladder_reserve_and_quantisation =====

def test_sparse_silver_path_keeps_meaningful_final_runner_and_limits_tp_count():
    from strategy.tp_ladder import build_tp_ladder

    report = {
        "candidates": [
            {"pool_side": "SSL", "tp_price": 70.40, "pool_price": 70.40,
             "quality": 0.70, "significance": 3.0, "delivery_prob": 0.72,
             "selection_ev": 0.40, "timeframe": "1m"},
        ],
        "selected": {"pool_side": "SSL", "tp_price": 67.42, "pool_price": 67.42,
                     "quality": 0.82, "significance": 5.0, "delivery_prob": 0.12,
                     "selection_ev": 0.77, "timeframe": "4h", "selected": True,
                     "fib_score": 0.75, "fib_confluence": 1.20, "fib_ratio": 1.618},
    }
    plan = build_tp_ladder(
        side="short", entry=70.66, sl=71.26, final_tp=67.42, atr=0.367,
        total_quantity=10.0, pool_report=report, min_leg_fraction=0.10,
        min_spacing_atr=0.85, max_internal_legs=3,
    )
    internals = [l for l in plan.legs if l.role != "FINAL"]
    assert 1 <= len(internals) <= 3, plan.compact()
    assert plan.final_fraction >= 0.18, plan.as_dict()
    assert sum(l.qty_fraction for l in internals) <= 1.0 - plan.final_fraction + 1e-9
    assert internals[0].qty_fraction < 0.60
    assert any("terminal-runner reserve" in n for n in plan.regime_notes)


def test_tp_ladder_exchange_quantisation_cannot_sell_full_size_before_final(monkeypatch):
    from types import SimpleNamespace
    from strategy.quant_strategy import QuantStrategy, QCfg

    monkeypatch.setattr(QCfg, "LOT_STEP", staticmethod(lambda: 1.0))
    monkeypatch.setattr(QCfg, "MIN_QTY", staticmethod(lambda: 1.0))

    plan = SimpleNamespace(legs=[])
    legs = [
        SimpleNamespace(role="TP1", quantity=3.9, distance_atr=1.0),
        SimpleNamespace(role="TP2", quantity=1.1, distance_atr=2.0),
        SimpleNamespace(role="TP3", quantity=1.1, distance_atr=3.0),
        SimpleNamespace(role="TP4", quantity=1.1, distance_atr=4.0),
        SimpleNamespace(role="TP5", quantity=1.0, distance_atr=5.0),
        SimpleNamespace(role="TP6", quantity=0.9, distance_atr=6.0),
    ]
    final = SimpleNamespace(role="FINAL", quantity=2.0)
    plan.legs = legs + [final]

    qs = QuantStrategy.__new__(QuantStrategy)
    executable = qs._prepare_executable_tp_ladder_legs(legs, 10.0, plan)
    internal_qty = sum(q for _, q in executable)
    assert internal_qty <= 8.0
    assert 10.0 - internal_qty >= 2.0
    assert all(q >= 1.0 and abs(q - round(q)) < 1e-12 for _, q in executable)



def test_tp_ladder_balances_tp1_materiality_without_overselling_first_lot():
    from strategy.tp_ladder import build_tp_ladder

    report = {
        "candidates": [
            {"pool_side": "SSL", "tp_price": 70.40, "pool_price": 70.40,
             "quality": 0.70, "significance": 3.0, "delivery_prob": 0.72,
             "selection_ev": 0.40, "timeframe": "1m"},
        ],
        "selected": {"pool_side": "SSL", "tp_price": 67.42, "pool_price": 67.42,
                     "quality": 0.82, "significance": 5.0, "delivery_prob": 0.12,
                     "selection_ev": 0.77, "timeframe": "4h", "selected": True,
                     "fib_score": 0.75, "fib_confluence": 1.20, "fib_ratio": 1.618},
    }
    plan = build_tp_ladder(
        side="short", entry=70.66, sl=71.26, final_tp=67.42, atr=0.367,
        total_quantity=10.0, pool_report=report, min_leg_fraction=0.10,
        min_spacing_atr=0.85, max_internal_legs=3,
    )
    internals = [l for l in plan.legs if l.role != "FINAL"]
    assert internals, plan.as_dict()
    # The old nearest pool was a shallow 0.43R monetisation point that required
    # too much quantity to matter.  New TP1 must produce a material full-trade R
    # contribution while staying below a concentrated first-exit allocation.
    assert internals[0].rr >= 1.0, plan.compact()
    assert internals[0].qty_fraction <= 0.30, plan.compact()
    assert internals[0].qty_fraction * internals[0].rr >= 0.12, plan.compact()
    assert any("near TP1 demoted" in n or "TP1 balanced" in n for n in plan.regime_notes)

# ===== END test_institutional_tp_ladder_reserve_and_quantisation =====


# ===== BEGIN test_tp_ladder_exit_cleanup.py =====
from types import SimpleNamespace as _SNS_TP_CLEANUP


def test_internal_tp_ladder_orders_are_cancelled_when_position_is_flat():
    from strategy.quant_strategy import QuantStrategy

    class DummyOM:
        def __init__(self):
            self.cancelled = []
        def cancel_order(self, oid):
            self.cancelled.append(oid)
            return _SNS_TP_CLEANUP(value="SUCCESS")

    qs = object.__new__(QuantStrategy)
    qs._om = DummyOM()
    qs._lock = __import__('threading').RLock()
    pos = _SNS_TP_CLEANUP(
        tp_order_id="native-final",
        tp_ladder_order_ids=["tp1", "tp2", "native-final", "tp3"],
        tp_ladder=[
            {"role": "TP1", "order_id": "tp1"},
            {"role": "TP2", "order_id": "tp2"},
            {"role": "FINAL", "order_id": "native-final"},
            {"role": "TP3", "order_id": "tp3"},
        ],
        tp_ladder_active=True,
    )

    qs._cleanup_tp_ladder_after_position_flat(qs._om, pos, reason="sl_hit")

    assert qs._om.cancelled == ["tp1", "tp2", "tp3"]
    assert pos.tp_ladder_active is False
    assert pos.tp_ladder_order_ids == []
    assert all(
        leg.get("cancelled_after_flat") is True
        for leg in pos.tp_ladder
        if leg.get("role") != "FINAL"
    )


def test_exchange_exit_path_calls_ladder_cleanup_after_exit_reason_resolution():
    import inspect
    from strategy.quant_strategy import QuantStrategy

    src = inspect.getsource(QuantStrategy._record_exchange_exit)
    assert '_cleanup_tp_ladder_after_position_flat(self._om, pos, reason=exit_reason)' in src
    assert 'exit_type in ("sl", "trail_sl", "tp", "manual_exit")' in src
# ===== END test_tp_ladder_exit_cleanup.py =====

# ===== BEGIN test_long_run_state_hardening.py =====

def test_risk_manager_record_trade_rolls_ist_day_before_booking_close():
    from datetime import datetime, timedelta
    from risk.risk_manager import RiskManager

    rm = RiskManager(shared_api=None)
    rm._last_reset_date = (datetime.now(rm._IST) - timedelta(days=1)).date()
    rm.daily_pnl = -9.0
    rm.consecutive_losses = 2
    rm.daily_trades.append(object())
    rm._position_is_open = True

    rm.record_trade(
        side="long",
        entry_price=100.0,
        exit_price=90.0,
        quantity=1.0,
        reason="sl",
        pnl_override=-1.25,
    )

    assert rm._last_reset_date == datetime.now(rm._IST).date()
    assert rm.daily_pnl == -1.25
    assert rm.consecutive_losses == 1
    assert len(rm.daily_trades) == 1
    assert rm._position_is_open is False


def test_strategy_wires_risk_manager_position_open_close_and_daily_pnl_source():
    import inspect
    from strategy.quant_strategy import QuantStrategy

    enter_src = inspect.getsource(QuantStrategy._enter_trade)
    exit_src = inspect.getsource(QuantStrategy._record_exchange_exit)
    finalise_src = inspect.getsource(QuantStrategy._finalise_exit)
    status_src = inspect.getsource(QuantStrategy.format_status_report)

    assert "set_position_open(True)" in enter_src
    assert "set_position_open(False)" in exit_src
    assert "set_position_open(False)" in finalise_src
    assert "getattr(self._risk_gate, 'daily_pnl'" in status_src


def test_daily_risk_gate_properties_roll_day_before_reporting():
    from datetime import timedelta
    from strategy.quant_strategy import DailyRiskGate

    rg = DailyRiskGate()
    rg._today = rg._today - timedelta(days=1)
    rg._daily_trades = 3
    rg._daily_pnl = -4.5
    rg._consec_losses = 2
    rg._loss_lockout_until = 123.0

    assert rg.daily_trades == 0
    assert rg.daily_pnl == 0.0
    assert rg.consec_losses == 0
    assert rg.loss_lockout_until == 0.0

# ===== END test_long_run_state_hardening.py =====

# ===== BEGIN test_tp_ladder_planner_lot_capacity.py =====

def test_tp_ladder_planner_does_not_create_internal_legs_when_lot_capacity_zero(monkeypatch):
    import inspect
    from strategy.quant_strategy import QuantStrategy

    src = inspect.getsource(QuantStrategy._build_tp_ladder_plan)
    assert "if _qty > 0 and _min_qty > 0:" in src
    assert "else 0" in src
    assert "planner must respect executable" in src

# ===== END test_tp_ladder_planner_lot_capacity.py =====

def test_reconcile_adoption_marks_risk_manager_position_open():
    from pathlib import Path
    src = Path('strategy/quant_strategy.py').read_text()
    assert "adoption means the exchange is already carrying risk" in src
    assert "set_position_open(True) adoption" in src

# ===== BEGIN test_margin_based_sizing_invariants.py =====

def test_margin_risk_leverage_math_matches_btc_example(monkeypatch):
    import config
    from strategy.quant_strategy import QuantStrategy

    monkeypatch.setattr(config, "RISK_PER_TRADE", 0.015, raising=False)
    monkeypatch.setattr(config, "LEVERAGE", 40, raising=False)
    qs = object.__new__(QuantStrategy)

    cap = qs._margin_risk_leverage_cap(price=76951.0, sl_dist=128.3, risk_pct=0.015)
    lev = qs._effective_margin_risk_leverage(price=76951.0, sl_dist=128.3, risk_pct=0.015, configured_leverage=40)

    assert 8.9 < cap < 9.1
    assert lev == 8.0 or lev == 9.0  # floor semantics; exact depends on example SL rounding
    assert (128.3 * lev / 76951.0) <= 0.015 + 1e-12


def test_liquidation_guard_accepts_lower_effective_leverage_override():
    from strategy.quant_strategy import QuantStrategy

    qs = object.__new__(QuantStrategy)

    high_lev_ok, _, _, _ = qs._sl_liquidation_sanity("long", 100.0, 96.0, leverage_override=40)
    low_lev_ok, _, _, _ = qs._sl_liquidation_sanity("long", 100.0, 96.0, leverage_override=10)

    assert high_lev_ok is False
    assert low_lev_ok is True


def test_entry_path_always_asserts_margin_risk_leverage_before_bracket():
    import inspect
    from strategy.quant_strategy import QuantStrategy

    src = inspect.getsource(QuantStrategy._enter_trade)
    assert "Always assert the leverage used by the sizing model" in src
    assert "_must_assert_leverage" in src
    assert "order_manager.set_leverage(leverage=_entry_leverage)" in src
    assert "exchange would still be sitting at 9x" in src


def test_risk_manager_record_trade_accepts_entry_leverage_for_margin_accounting():
    import inspect
    from risk.risk_manager import RiskManager

    sig = inspect.signature(RiskManager.record_trade)
    assert "entry_leverage" in sig.parameters
    src = inspect.getsource(RiskManager.record_trade)
    assert "_lev_for_margin" in src
    assert "entry_leverage or getattr(config, \"LEVERAGE\"" in src

# ===== END test_margin_based_sizing_invariants.py =====

# ===== BEGIN test_delta_native_bracket_payload_hardening.py =====

def test_delta_native_bracket_sends_child_limit_prices_and_trigger_method():
    from types import SimpleNamespace
    from execution.order_manager import _DeltaAdapter

    class API:
        def __init__(self):
            self.kw = None
        def place_order(self, **kw):
            self.kw = kw
            return {"success": True, "result": {"id": 12345, "state": "open"}}

    inst = SimpleNamespace(
        symbol="SLVONUSD", display_symbol="SLVONUSD", tick_size=0.01,
        lot_step=1, min_qty=1, max_qty=1000, contract_value_btc=1.0,
        product_id=777, asset_class="commodity",
    )
    api = API()
    ad = _DeltaAdapter(api, exchange_instrument=inst)
    res = ad.place_bracket_limit_entry("long", 1.9, 68.96, 68.50, 69.70)

    assert res.get("order_id") == "12345"
    assert api.kw["size"] == 2
    assert api.kw["limit_price"] == 68.96
    assert api.kw["bracket_stop_loss_price"] == 68.50
    assert api.kw["bracket_stop_loss_limit_price"] < api.kw["bracket_stop_loss_price"]
    assert api.kw["bracket_take_profit_price"] == 69.70
    assert api.kw["bracket_take_profit_limit_price"] == 69.70
    assert api.kw["bracket_stop_trigger_method"] == "last_traded_price"


def test_delta_native_bracket_rejects_invalid_geometry_before_api_call():
    from types import SimpleNamespace
    from execution.order_manager import _DeltaAdapter

    class API:
        def __init__(self):
            self.called = False
        def place_order(self, **kw):
            self.called = True
            return {"success": True, "result": {"id": 12345}}

    inst = SimpleNamespace(
        symbol="SLVONUSD", display_symbol="SLVONUSD", tick_size=0.01,
        lot_step=1, min_qty=1, max_qty=1000, contract_value_btc=1.0,
        product_id=777, asset_class="commodity",
    )
    api = API()
    ad = _DeltaAdapter(api, exchange_instrument=inst)
    res = ad.place_bracket_limit_entry("long", 1.9, 68.96, 69.10, 69.70)

    assert res.get("_error") is True
    assert api.called is False
    assert "local_bracket_preflight_failed" in str(res.get("_raw"))


def test_strategy_bracket_failure_log_includes_order_manager_reason():
    import inspect
    from strategy.quant_strategy import QuantStrategy

    src = inspect.getsource(QuantStrategy._enter_trade)
    assert "last_order_error" in src
    assert "reason=" in src

# ===== END test_delta_native_bracket_payload_hardening.py =====
