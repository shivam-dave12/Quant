"""
Combined institutional pytest suite.

Historical version tests are stored here as direct source blocks and loaded into
isolated in-memory modules, so helpers/imports from one version cannot overwrite
another version's globals.  There are no sidecar .source files and no base64
packing layer.

Run:
    PYTHONPATH=. pytest -q tests/test_all_versions_combined.py
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

# Keep old conftest behavior for Path.read_text() compatibility.
_read_text = Path.read_text


def _read_text_utf8_default(self, encoding=None, errors=None, newline=None):
    if encoding is None:
        encoding = "utf-8"
    return _read_text(self, encoding=encoding, errors=errors, newline=newline)


Path.read_text = _read_text_utf8_default

_CONSOLIDATED_TEST_SOURCES = [
    ('test_asset_notifications_v10.py',
     '''
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

    def test_periodic_report_uses_asset_not_btc(self):
        inst = _inst("NVDA", "NVDAXUSD")
        with instrument_scope(inst):
            msg = format_periodic_report(current_price=198.0, atr=1.2, instrument=inst)
        self.assertIn("NVDA", msg)
        self.assertIn("NVDAXUSD", msg)
        self.assertNotIn("<code>BTC", msg)

    def test_already_scoped_message_not_double_wrapped(self):
        inst = _inst()
        raw = "🏛 <b>POSTERIOR</b>  <code>AAPL</code>\\nbody"
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
''',
    ),
    ('test_dashboard_v25_installer_and_parser.py',
     '''from pathlib import Path


def test_install_script_creates_tail_service():
    script = Path('scripts/install_dashboard_aws.sh').read_text()
    assert 'trading-dashboard-tail.service' in script
    assert 'log_tail_agent.py' in script
    assert '--from-start' in script
    assert '{{.LogPath}}' in script


def test_log_parser_extracts_catalog_and_scan():
    import importlib.util
    agent_path = Path('dashboard/agents/log_tail_agent.py')
    spec = importlib.util.spec_from_file_location('log_tail_agent', agent_path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)

    catalog = '19:47:45.989 | INFO | • SYSTEM |      ✅ BTC      primary=delta      delta:BTCUSD'
    ev = mod.parse(catalog)
    assert ev and ev['type'] == 'catalog_asset'
    assert ev['asset'] == 'BTC'
    assert ev['symbol'] == 'BTCUSD'

    scan = '[BTC|DELTA:BTCUSD] BTC DELTA BTCUSD | price 100000.00 | SCANNING | open=0/4'
    ev = mod.parse(scan)
    assert ev and ev['type'] == 'scan'
    assert ev['asset'] == 'BTC'
    assert ev['price'] == 100000.0
''',
    ),
    ('test_delta_bracket_policy.py',
     '''import inspect
import unittest


class DeltaBracketPolicyTests(unittest.TestCase):
    def test_quant_strategy_refuses_delta_non_bracket_fallback(self):
        from strategy.quant_strategy import QuantStrategy

        src = inspect.getsource(QuantStrategy._enter_trade)
        self.assertIn("DELTA_REQUIRE_NATIVE_BRACKET", src)
        self.assertIn("Delta native bracket entry failed", src)
        self.assertIn("refusing non-bracket fallback", src)
        self.assertIn("_active_exchange == \\"delta\\"", src)

    def test_config_defaults_require_delta_native_bracket(self):
        import config
        self.assertTrue(getattr(config, "DELTA_REQUIRE_NATIVE_BRACKET", False))


if __name__ == "__main__":
    unittest.main()
''',
    ),
    ('test_hardening.py',
     '''import os
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

    def test_target_surface_prices_executable_buffered_tp_not_raw_pool(self):
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

        surface = build_target_surface(
            side="long",
            entry=100.0,
            stop=98.0,
            atr=1.0,
            snapshot=snap,
            flow=SimpleNamespace(tick_flow=0.30, cvd_trend=0.35),
            ict=SimpleNamespace(
                structure_15m="bullish",
                structure_4h="bullish",
                dealing_range_pd=0.30,
            ),
            fee_bps=10.0,
            slippage_bps=0.0,
            posterior_prob=0.90,
        )

        self.assertIsNotNone(surface.best)
        self.assertAlmostEqual(surface.best.price, 103.70, places=6)
        self.assertAlmostEqual(surface.best.rr, 1.85, places=6)
        self.assertTrue(any("pool=$104.0" in note for note in surface.best.notes))

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

    def test_expected_utility_fee_snapshot_is_not_double_counted(self):
        from strategy.quant_strategy import QuantStrategy

        strategy = object.__new__(QuantStrategy)
        strategy._entry_engine = SimpleNamespace(_last_sweep_analysis={"quant_posterior": 0.90})

        class FakeFeeEngine:
            def diagnostic_snapshot(self):
                return {
                    "rt_cost_maker_bps": 10.0,
                    "rt_cost_taker_bps": 14.0,
                    "slippage_ewma_bps": 5.0,
                }

        strategy._fee_engine = FakeFeeEngine()
        signal = SimpleNamespace(
            side="long",
            entry_price=100.0,
            sl_price=98.0,
            tp_price=103.70,
            rr_ratio=1.85,
            posterior_prob=0.90,
            target_pool=SimpleNamespace(pool=SimpleNamespace(price=104.0)),
        )
        snap = SimpleNamespace(
            bsl_pools=[SimpleNamespace(price=104.0, side="BSL", timeframe="15m", significance=12.0)],
            ssl_pools=[],
            feed_reliability=0.90,
        )

        strategy._apply_expected_utility_target_surface(
            signal,
            snap,
            SimpleNamespace(tick_flow=0.30, cvd_trend=0.35),
            SimpleNamespace(structure_15m="bullish", structure_4h="bullish", dealing_range_pd=0.30),
            100.0,
            1.0,
        )

        self.assertIsNotNone(signal.selected_target_utility)
        self.assertAlmostEqual(signal.selected_target_utility.cost_r, 0.05, places=6)

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

    def test_positive_target_utility_prevents_low_rr_duplicate_block(self):
        from strategy.quant_strategy import EntryType, QuantStrategy

        strategy = object.__new__(QuantStrategy)
        strategy._htf = SimpleNamespace(trend_15m=0.0, trend_4h=0.0)
        strategy._last_hunt_prediction = None
        strategy._entry_engine = SimpleNamespace(
            _last_sweep_analysis={
                "posterior": 0.84,
                "displacement_atr": 1.20,
                "cisd": True,
                "ote": False,
            }
        )
        good_target = SimpleNamespace(
            full_position_utility=0.22,
            expected_value_r=0.09,
            compact=lambda: "near pool p=0.78 fullU=0.22",
        )
        signal = SimpleNamespace(
            side="long",
            entry_type=EntryType.SWEEP_REVERSAL,
            entry_price=100.0,
            sl_price=98.6,
            tp_price=101.5,
            target_pool=SimpleNamespace(
                pool=SimpleNamespace(price=101.7, timeframe="15m", side="BSL"),
                significance=16.0,
                distance_atr=1.7,
            ),
            sweep_result=SimpleNamespace(quality=0.90),
            conviction=0.90,
            selected_target_utility=good_target,
            target_surface=SimpleNamespace(has_positive_edge=True, best=good_target),
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

        self.assertTrue(decision.allowed)
        self.assertEqual(decision.reject_reasons, [])
        self.assertTrue(any("PAYOFF_DENSITY" in r for r in decision.allow_reasons))

    def test_expected_utility_negative_surface_has_no_size_allocation(self):
        from strategy.expected_utility import expected_utility_size_multiplier

        surface = SimpleNamespace(
            has_positive_edge=False,
            best=SimpleNamespace(payoff_r=12.0, probability=0.02, full_position_utility=-1.0),
            runner_fraction=0.0,
        )

        self.assertEqual(expected_utility_size_multiplier(surface, posterior=0.95), 0.0)

    def test_expected_utility_size_uses_target_probability_when_posterior_missing(self):
        from strategy.expected_utility import expected_utility_size_multiplier

        surface = SimpleNamespace(
            has_positive_edge=True,
            best=SimpleNamespace(
                payoff_r=2.0,
                probability=0.70,
                full_position_utility=0.40,
                loss_r=1.0,
                cost_r=0.0,
                role="external",
            ),
            runner_fraction=0.0,
        )

        self.assertGreater(expected_utility_size_multiplier(surface, posterior=0.0), 0.98)

    def test_expected_utility_size_penalizes_fat_left_tail_and_cost(self):
        from strategy.expected_utility import expected_utility_size_multiplier

        base_best = dict(
            payoff_r=2.0,
            probability=0.65,
            full_position_utility=0.30,
            role="external",
        )
        clean = SimpleNamespace(
            has_positive_edge=True,
            best=SimpleNamespace(**base_best, loss_r=1.0, cost_r=0.0),
            runner_fraction=0.0,
        )
        fat_tail = SimpleNamespace(
            has_positive_edge=True,
            best=SimpleNamespace(**base_best, loss_r=2.0, cost_r=0.25),
            runner_fraction=0.0,
        )

        self.assertLess(
            expected_utility_size_multiplier(fat_tail, posterior=0.80),
            expected_utility_size_multiplier(clean, posterior=0.80),
        )

    def test_structural_be_gate_uses_peak_delivery_not_retraced_mark(self):
        from strategy.liquidity_trail import LiquidityTrailEngine

        ok, reason = LiquidityTrailEngine._structural_be_gate(
            "short", price=100.40, entry_price=101.00, atr=1.0,
            r_multiple=0.0, momentum_gate="CVD", ict_engine=None,
            liq_snapshot=None, now=time.time(), peak_profit=1.00,
        )

        self.assertTrue(ok, reason)

    def test_trail_off_is_ignored_while_position_active(self):
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
        self.assertIsNone(strategy._pos.trail_override)

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

    def test_negative_route_utility_blocks_sizing_even_when_geometry_is_valid(self):
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

        qty = strategy._compute_quantity(
            FakeRisk(),
            price=10000.0,
            sig=None,
            ict_tier="S",
            sl_price=9900.0,
            tp_price=10050.0,
            side="long",
            use_maker_entry=True,
            posterior_prob=0.20,
            prefetched_bal_info={"available": 1000.0, "total": 1000.0},
        )

        self.assertIsNone(qty)
        ctx = strategy._last_execution_viability
        self.assertTrue(bool(ctx["allocation_allowed"]))
        self.assertLessEqual(ctx["expected_net_utility_r"], 0.0)

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
            self.assertIn("looks percent-style", "\\n".join(logs.output))
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
        # confidence-haircut target, but still inside the raw 0.5% portfolio
        # risk cap and inside the slot cash/margin envelope, so it is valid.
        self.assertEqual(qty, 0.001)
        self.assertLessEqual(qty * sl_dist, 204.86 * 0.005 * 1.15 + 1e-9)
        required_margin = qty * price / 40.0
        self.assertLessEqual(required_margin, 51.21 * 0.60 + 1e-9)

    def test_position_sizing_rejects_min_lot_above_haircut_risk_budget(self):
        from strategy.quant_strategy import QuantStrategy

        strategy = object.__new__(QuantStrategy)
        strategy._fee_engine = None
        strategy._post_trade_agent = None
        strategy._active_institutional_size_mult = 0.01
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
                ict_tier="",
                sl_price=9000.0,
                tp_price=12000.0,
                side="long",
                use_maker_entry=True,
                posterior_prob=0.75,
                prefetched_bal_info={"available": 1000.0, "total": 1000.0},
            )

        self.assertIsNone(qty)
        self.assertIn("no exchange lot fits risk/margin envelope", "\\n".join(logs.output))

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
        candidate = payload["candidates"][0]
        self.assertGreater(candidate["required_rr"], candidate["rr"])
        self.assertLess(candidate["required_rr"], 1.35)
        self.assertIn("delivery probability", candidate["reason"])

    def test_tp_selector_accepts_probability_compensated_positive_utility_target(self):
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
            direction="long",
            tf_sources=["1d"],
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

        self.assertIsNotNone(tp)
        self.assertIs(selected_target, target)
        self.assertLess(score.rr, 2.20)
        self.assertGreater(score.rr, score.components["rr_floor"])
        payload = report.as_dict()
        self.assertEqual(payload["selected"]["pool_price"], pool.price)
        self.assertGreater(payload["selected"]["delivery_prob"], payload["selected"]["required_delivery_prob"])
        self.assertTrue(any("dynamic payoff floor" in n for n in payload["selected"]["notes"]))

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
''',
    ),
    ('test_instrument_registry_v4.py',
     '''
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


class FakeICICI:
    def get_security_master_rows(self, url=None):
        return [
            {"ExchangeCode": "NFO", "ShortName": "NIFTY", "TradingSymbol": "NIFTY28MAY2625000CE", "ProductType": "Options", "OptionType": "Call", "StrikePrice": "25000", "ExpiryDate": "28-May-2026", "Token": "12345", "LotSize": "75"},
            {"ExchangeCode": "NFO", "ShortName": "CNXBAN", "TradingSymbol": "BANKNIFTY28MAY2655000PE", "ProductType": "Options", "OptionType": "Put", "StrikePrice": "55000", "ExpiryDate": "28-May-2026", "Token": "12346", "LotSize": "30"},
            {"ExchangeCode": "NSE", "ShortName": "RELIANCE", "TradingSymbol": "RELIANCE", "Series": "EQ", "Token": "2885", "LotSize": "1"},
        ]


class InstrumentRegistryV4Tests(unittest.TestCase):
    def test_coinswitch_secondary_is_added_by_live_symbol_validation(self):
        intents = [AssetIntent("BTC", "Bitcoin", AssetClass.CRYPTO, ("BTCUSD", "BTCUSDT"), priority=0)]
        reg = InstrumentRegistry(execution_preference="delta")
        report = reg.discover(FakeDelta(), FakeCoinSwitch(), requested=[{
            "asset_id": "BTC", "display_name": "Bitcoin", "asset_class": "crypto", "aliases": ["BTCUSD", "BTCUSDT"], "priority": 0
        }], discovery_mode="static")
        btc = report.matched[0]
        self.assertIn(ExchangeName.DELTA, btc.by_exchange)
        self.assertIn(ExchangeName.COINSWITCH, btc.by_exchange)
        self.assertEqual(btc.by_exchange[ExchangeName.COINSWITCH].symbol, "BTCUSDT")

    def test_spxusd_is_not_used_for_s_and_p_500_index(self):
        reg = InstrumentRegistry(execution_preference="delta")
        report = reg.discover(FakeDelta(), None, requested=[{
            "asset_id": "SPX_INDEX", "display_name": "S&P 500 index", "asset_class": "index", "aliases": ["SPX500USD", "US500", "SP500"], "priority": 0
        }], discovery_mode="static")
        self.assertEqual(report.matched, [])
        self.assertIn("SPX_INDEX", report.unavailable)

    def test_xstock_exact_alias_matches_delta_symbol(self):
        reg = InstrumentRegistry(execution_preference="delta")
        report = reg.discover(FakeDelta(), None, requested=[{
            "asset_id": "AAPL", "display_name": "Apple xStock token derivative", "asset_class": "equity", "aliases": ["AAPLXUSD", "AAPL"], "priority": 0
        }], discovery_mode="static")
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
        ], max_active=10, discovery_mode="static")
        syms = {x.primary.symbol for x in report.matched}
        self.assertEqual({"SPYXUSD", "QQQXUSD", "COINXUSD", "CRCLXUSD"}, syms)
        self.assertTrue(all(x.max_leverage == 25 for x in report.matched))

    def test_dynamic_discovery_activates_all_live_catalog_symbols_without_request_basket(self):
        reg = InstrumentRegistry(execution_preference="delta")
        report = reg.discover(FakeDelta(), FakeCoinSwitch(), requested=[], max_active=0, discovery_mode="dynamic")
        syms = {x.primary.symbol for x in report.matched}
        self.assertIn("BTCUSD", syms)
        self.assertIn("SPXUSD", syms)
        self.assertIn("AAPLXUSD", syms)
        self.assertGreaterEqual(len(report.matched), 7)

    def test_zero_max_active_means_no_registry_cap(self):
        reg = InstrumentRegistry(execution_preference="delta")
        uncapped = reg.discover(FakeDelta(), None, requested=[], max_active=0, discovery_mode="dynamic")
        capped = reg.discover(FakeDelta(), None, requested=[], max_active=2, discovery_mode="dynamic")
        self.assertGreater(len(uncapped.matched), len(capped.matched))
        self.assertEqual(len(capped.matched), 2)

    def test_icici_security_master_options_enter_dynamic_coverage(self):
        reg = InstrumentRegistry(execution_preference="delta")
        report = reg.discover(None, None, icici_api=FakeICICI(), requested=[], max_active=0, discovery_mode="dynamic")
        by_symbol = {x.primary.symbol: x for x in report.matched}
        self.assertEqual(by_symbol["NIFTY28MAY2625000CE"].asset_class, AssetClass.OPTION)
        self.assertEqual(by_symbol["BANKNIFTY28MAY2655000PE"].primary_exchange, ExchangeName.ICICI)
        self.assertNotIn("RELIANCE", by_symbol)  # v84 Indian mandate: options only


if __name__ == "__main__":
    unittest.main()
''',
    ),
    ('test_multi_asset_portfolio.py',
     '''import sys
import types
import unittest
from types import SimpleNamespace

# The portfolio guard tests do not exercise network/websocket clients.
# Some minimal production images used for CI do not install python-socketio.
socketio_stub = types.SimpleNamespace(Client=lambda *a, **kw: SimpleNamespace())
sys.modules.setdefault("socketio", socketio_stub)

from core.instruments import AssetClass
from orchestration.multi_asset_bot import PortfolioGuard


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
        guard = PortfolioGuard()
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
        guard = PortfolioGuard()
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


if __name__ == "__main__":
    unittest.main()
''',
    ),
    ('test_portfolio_policy_v9.py',
     '''import unittest
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
            self.assertEqual(QCfg.LEVERAGE(), 8)
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
''',
    ),
    ('test_spread_gate_v8.py',
     '''import os
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
''',
    ),
    ('test_v12_portfolio_notifications_trail.py',
     '''
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
        src = Path("orchestration/multi_asset_bot.py").read_text()
        self.assertIn("def format_portfolio_pnl_report", src)
        self.assertIn("def format_portfolio_position_report", src)
        self.assertIn("def format_portfolio_equity_report", src)
        self.assertIn("PORTFOLIO", src)

if __name__ == "__main__":
    unittest.main()
''',
    ),
    ('test_v13_protection_invariants.py',
     '''import inspect
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
        self.assertIn("event_type=\\"protection_failure\\"", src)


if __name__ == "__main__":
    unittest.main()
''',
    ),
    ('test_v24_restore_working_calculations.py',
     '''from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[1]


class V24RestoreWorkingCalculationsTest(unittest.TestCase):
    def test_no_v18_structural_stop_veto_left_in_entry_engine(self):
        src = (ROOT / 'strategy' / 'entry_engine.py').read_text()
        self.assertNotIn('_first_uncovered_protective_pool', src)
        self.assertNotIn('trade must reprice/abstain', src)
        self.assertNotIn('not place SL in front of liquidity', src)

    def test_sl_pool_selector_uses_v66_liquidity_exclusion_model(self):
        src = (ROOT / 'strategy' / 'liquidity_pool_selector.py').read_text()
        self.assertIn('_SL_BUFFER_BASE_ATR        = 0.30', src)
        self.assertIn('_SL_BUFFER_MAX_ATR         = 1.35', src)
        self.assertIn('stronger liquidity → wider buffer', src)
        self.assertIn('_SL_HARD_MAX_DISTANCE_ATR  = 18.0', src)

    def test_trailing_default_is_on_in_this_repaired_trailing_on_build(self):
        import config
        self.assertTrue(bool(getattr(config, 'QUANT_TRAIL_ENABLED', False)))


if __name__ == '__main__':
    unittest.main()
''',
    ),
    ('test_v26_ops_integrity.py',
     '''
import sys
import types
import unittest
from types import SimpleNamespace

socketio = types.ModuleType("socketio")
socketio.Client = object
sys.modules.setdefault("socketio", socketio)


class TestV26OpsIntegrity(unittest.TestCase):
    def test_log_tail_does_not_mark_hold_as_exchange_trail_update(self):
        from dashboard.agents.log_tail_agent import parse
        line = '{"log":"22:03:25.584 | INFO | 🧠 POSTERIOR    | [SILVER|DELTA:SLVONUSD] InstitutionalTrail PAYOFF_HOLD [AGGRESSIVE] SL=$66.6 current=$67.2 | net_lock=0.0pts"}'
        ev = parse(line)
        self.assertEqual(ev["type"], "trail_hold")

    def test_log_tail_dispatch_is_not_exchange_trail_update(self):
        from dashboard.agents.log_tail_agent import parse
        line = '{"log":"21:55:44.655 | INFO | 🧠 POSTERIOR    | [GOLD|DELTA:PAXGUSD] 🏦 InstitutionalTrail SL dispatch [STOP-LIMIT] trigger=$4,554.0 side=buy qty=0.029 phase=BE_LOCK"}'
        ev = parse(line)
        self.assertEqual(ev["type"], "trail_dispatch")

    def test_log_tail_exchange_applied_is_trail_update(self):
        from dashboard.agents.log_tail_agent import parse
        line = '{"log":"22:03:25.584 | INFO | 🧠 POSTERIOR    | [SILVER|DELTA:SLVONUSD] ✅ InstitutionalTrail EXCHANGE_APPLIED old=$67.20 new=$66.60 order=abc phase=AGGRESSIVE"}'
        ev = parse(line)
        self.assertEqual(ev["type"], "trail_update")
        self.assertAlmostEqual(ev["sl"], 66.60)

    def test_risk_record_trade_non_btc_quantity_unit(self):
        from risk.risk_manager import RiskManager
        rm = RiskManager(shared_api=None)
        inst = SimpleNamespace(asset_id="NVDA", asset_class="equity")
        with self.assertLogs("risk.risk_manager", level="INFO") as cm:
            rm.record_trade("long", 100.0, 101.0, 2.0, "unit", pnl_override=1.0, instrument=inst, leverage=25)
        text = "\\n".join(cm.output)
        self.assertIn("contracts", text)
        self.assertNotIn("BTC |", text)


    def test_risk_record_trade_without_override_uses_generic_quantity_units(self):
        from risk.risk_manager import RiskManager
        rm = RiskManager(shared_api=None)
        inst = SimpleNamespace(asset_id="NVDA", asset_class="equity")
        rm.record_trade("long", 100.0, 101.0, 2.0, "unit_no_override", instrument=inst, leverage=25)
        self.assertAlmostEqual(rm.realized_pnl, 2.0 - ((100.0 + 101.0) * 2.0 * 0.00055), places=6)

    def test_multiasset_has_portfolio_command_reports(self):
        from orchestration.multi_asset_bot import MultiAssetQuantBot
        for name in ["format_portfolio_status_report", "format_portfolio_market_report", "format_portfolio_risk_report", "format_portfolio_sl_tp_report"]:
            self.assertTrue(hasattr(MultiAssetQuantBot, name), name)


if __name__ == "__main__":
    unittest.main()
''',
    ),
    ('test_v27_telemetry_integrity.py',
     '''import unittest
from types import SimpleNamespace

from core.redaction import redact_sensitive
from dashboard.agents.log_tail_agent import parse
from dashboard.backend.state_store import DashboardState


class TelemetryIntegrityV27Tests(unittest.TestCase):
    def test_dashboard_redacts_sensitive_payloads(self):
        st = DashboardState()
        st.apply({
            "type": "alert",
            "severity": "critical",
            "title": "leak test",
            "message": "api_key=abc123 secret_key=xyz private_key=0x" + "a"*64,
        })
        snap = st.snapshot()
        msg = snap["alerts"][0]["message"]
        self.assertIn("<REDACTED>", msg)
        self.assertNotIn("abc123", msg)
        self.assertNotIn("xyz", msg)
        self.assertNotIn("a"*64, msg)

    def test_log_tail_adopted_position_becomes_position_update(self):
        line = '{"log":"20:11:02.753 | WARN | 🧠 POSTERIOR    | [SILVER|DELTA:SLVONUSD] ⚡ RECONCILE: adopted SHORT @ $66.74"}'
        ev = parse(line)
        self.assertEqual(ev["type"], "position_update")
        self.assertEqual(ev["asset"], "SILVER")
        self.assertEqual(ev["side"], "SHORT")
        self.assertEqual(ev["entry"], 66.74)
        self.assertEqual(ev["bracket"], "ADOPTED")

    def test_trail_hold_does_not_mutate_position_sl(self):
        st = DashboardState()
        st.apply({"type": "position_update", "asset": "SILVER", "venue": "DELTA", "symbol": "SLVONUSD", "side": "SHORT", "entry": 66.74, "sl": 67.2, "tp": 65.5, "price": 66.7})
        st.apply({"type": "trail_hold", "asset": "SILVER", "venue": "DELTA", "symbol": "SLVONUSD", "sl": 66.6, "message": "PAYOFF_HOLD"})
        snap = st.snapshot()
        self.assertEqual(snap["positions"][0]["sl"], 67.2)
        self.assertEqual(snap["decisions"][0]["kind"], "trail_hold")

    def test_trail_update_mutates_position_sl_only_when_confirmed(self):
        st = DashboardState()
        st.apply({"type": "position_update", "asset": "GOLD", "venue": "DELTA", "symbol": "PAXGUSD", "side": "SHORT", "entry": 4570, "sl": 4580, "tp": 4550, "price": 4560})
        st.apply({"type": "trail_update", "asset": "GOLD", "venue": "DELTA", "symbol": "PAXGUSD", "sl": 4554, "trailing": "ON"})
        snap = st.snapshot()
        self.assertEqual(snap["positions"][0]["sl"], 4554)
        self.assertEqual(snap["positions"][0]["trailing"], "ON")

    def test_dashboard_emitter_is_process_shared(self):
        from telemetry.dashboard_emitter import DashboardEmitter
        DashboardEmitter._shared = None
        a = DashboardEmitter.from_config()
        b = DashboardEmitter.from_config()
        self.assertIs(a, b)


if __name__ == "__main__":
    unittest.main()
''',
    ),
    ('test_v28_risk_budget_capital_metrics.py',
     '''import unittest
from types import SimpleNamespace

import config
from risk.risk_manager import RiskManager


class V28RiskBudgetCapitalMetricsTest(unittest.TestCase):
    def test_portfolio_max_entries_is_six(self):
        self.assertEqual(int(config.PORTFOLIO_MAX_OPEN_POSITIONS), 6)
        self.assertGreaterEqual(int(config.PORTFOLIO_MAX_OPEN_PER_ASSET_CLASS), 6)

    def test_count_win_rate_does_not_hide_capital_loss(self):
        rm = RiskManager(shared_api=None)
        inst = SimpleNamespace(asset_id="TEST", display_symbol="TESTUSD", asset_class="equity", primary_exchange=SimpleNamespace(value="delta"))
        # $5 margin loser: entry 100 qty 1 leverage 20 => margin 5, pnl -2
        rm.record_trade("long", 100, 98, 1, "large_margin_loss", pnl_override=-2.0, instrument=inst, leverage=20)
        # $1 margin winner: entry 100 qty 0.2 leverage 20 => margin 1, pnl +1
        rm.record_trade("long", 100, 105, 0.2, "small_margin_win", pnl_override=1.0, instrument=inst, leverage=20)
        stats = rm.get_statistics()
        self.assertEqual(stats["count_win_rate"], 50.0)
        self.assertEqual(stats["capital_result"], "LOSS")
        self.assertLess(stats["net_margin_return_pct"], 0)
        self.assertAlmostEqual(stats["total_margin_used"], 6.0, places=6)
        self.assertAlmostEqual(stats["margin_weighted_win_rate"], (1.0 / 6.0) * 100.0, places=2)

    def test_trade_record_stores_margin_and_units(self):
        rm = RiskManager(shared_api=None)
        inst = SimpleNamespace(asset_id="NVDA", display_symbol="NVDAXUSD", asset_class="equity", primary_exchange=SimpleNamespace(value="delta"))
        rm.record_trade("short", 200, 198, 0.5, "unit", pnl_override=1.0, instrument=inst, leverage=25)
        t = rm.trade_history[-1]
        self.assertEqual(t.asset_id, "NVDA")
        self.assertEqual(t.symbol, "NVDAXUSD")
        self.assertEqual(t.qty_unit, "contracts")
        self.assertAlmostEqual(t.margin_used, 4.0)
        self.assertAlmostEqual(t.return_on_margin, 25.0)


if __name__ == "__main__":
    unittest.main()
''',
    ),
    ('test_v64_institutional_regressions.py',
     '''import unittest

import config
from strategy.quant_strategy import PositionState, QuantStrategy


class _DummyInstrument:
    def __init__(self, asset_id: str, symbol: str, contract_type: str = "") -> None:
        self.asset_id = asset_id
        self.display_symbol = symbol
        self.canonical_symbol = symbol
        self.primary = type("Primary", (), {"contract_type": contract_type, "raw": {"contract_type": contract_type}, "base_asset": asset_id})()


def _strategy(asset_id: str, symbol: str, contract_type: str = "") -> QuantStrategy:
    qs = object.__new__(QuantStrategy)
    qs._asset_id = asset_id
    qs._instrument = _DummyInstrument(asset_id, symbol, contract_type)
    return qs


class InstitutionalRegressionTests(unittest.TestCase):
    def setUp(self):
        self._old_exchange = getattr(config, "EXECUTION_EXCHANGE", "")
        self._old_delta_symbol = getattr(config, "DELTA_SYMBOL", "")
        self._old_delta_fee = getattr(config, "DELTA_COMMISSION_RATE", 0.00050)
        self._old_delta_maker = getattr(config, "DELTA_COMMISSION_RATE_MAKER", -0.00020)

    def tearDown(self):
        config.EXECUTION_EXCHANGE = self._old_exchange
        config.DELTA_SYMBOL = self._old_delta_symbol
        config.DELTA_COMMISSION_RATE = self._old_delta_fee
        config.DELTA_COMMISSION_RATE_MAKER = self._old_delta_maker

    def test_config_import_is_safe_without_live_credentials(self):
        self.assertTrue(hasattr(config, "REQUIRE_EXCHANGE_CREDENTIALS"))

    def test_delta_xstock_is_linear_pnl_even_when_global_delta_symbol_is_btc(self):
        config.EXECUTION_EXCHANGE = "delta"
        config.DELTA_SYMBOL = "BTCUSD"
        config.DELTA_COMMISSION_RATE = 0.00050
        qs = _strategy("AAPL", "AAPLXUSD")
        self.assertTrue(qs._is_delta_execution())
        self.assertFalse(qs._is_inverse_pnl_contract())

        pos = PositionState(side="long", quantity=2.0, entry_price=100.0)
        pnl = qs._estimate_pnl(pos, 110.0, entry_fill_type="taker")
        expected = (110.0 - 100.0) * 2.0 - (100.0 * 2.0 * 0.00050) - (110.0 * 2.0 * 0.00050)
        self.assertAlmostEqual(pnl, expected, places=8)

    def test_delta_btc_contract_keeps_inverse_pnl_geometry(self):
        config.EXECUTION_EXCHANGE = "delta"
        config.DELTA_SYMBOL = "BTCUSD"
        qs = _strategy("BTC", "BTCUSD", contract_type="inverse_perpetual")
        self.assertTrue(qs._is_delta_execution())
        self.assertTrue(qs._is_inverse_pnl_contract())


if __name__ == "__main__":
    unittest.main()

class DashboardTelemetryRegressionTests(unittest.TestCase):
    def test_log_tail_parses_tp_audit_and_quant_wait(self):
        from dashboard.agents.log_tail_agent import parse

        tp_line = '{"log":"10:03:25.611 | INFO | [BTC|DELTA:BTCUSD] RAW_TP_AUDIT: TP/SHORT: no eligible TP pool; best visible pool rejected"}'
        tp = parse(tp_line)
        self.assertEqual(tp["type"], "tp_audit")
        self.assertEqual(tp["asset"], "BTC")

        wait_line = '{"log":"10:23:33.536 | INFO | [AMZN|DELTA:AMZNXUSD] POST-SWEEP QUANT WAIT: REVERSAL LONG | p=0.746 min=0.734 EV=0.598 LLR=1.08 U=0.49 REJECT quant posterior auction"}'
        wait = parse(wait_line)
        self.assertEqual(wait["type"], "candidate_deferred")
        self.assertEqual(wait["phase"], "POST_SWEEP_QUANT_WAIT")
        self.assertEqual(wait["asset"], "AMZN")
        self.assertAlmostEqual(wait["posterior"], 0.746)
''',
    ),
    ('test_v65_tp_sl_institutional_selectors.py',
     '''import time
import pytest

from config import MULTI_ASSET_REQUESTS
from strategy.entry_engine import EntryEngine
from strategy.liquidity_map import (
    LiquidityMapSnapshot,
    LiquidityPool,
    PoolSide,
    PoolStatus,
    PoolTarget,
)
from strategy.liquidity_pool_selector import select_sl_with_report, select_tp_with_report
from strategy.quant_strategy import InstitutionalLevels


def _target(price, pool_side, status=PoolStatus.CONFIRMED, *, entry=100.0, atr=2.0,
            sig=50.0, tf="5m"):
    now = time.time()
    pool = LiquidityPool(
        price=float(price),
        side=pool_side,
        timeframe=tf,
        status=status,
        touches=8,
        created_at=now,
        last_touch=now,
        ob_aligned=True,
        fvg_aligned=True,
        htf_count=2,
    )
    return PoolTarget(
        pool=pool,
        distance_atr=abs(float(price) - float(entry)) / max(float(atr), 1e-9),
        direction="above" if float(price) > float(entry) else "below",
        significance=float(sig),
        tf_sources=[tf],
    )


def _snapshot(entry=100.0, atr=2.0, bsl=None, ssl=None):
    return LiquidityMapSnapshot(
        bsl_pools=list(bsl or []),
        ssl_pools=list(ssl or []),
        primary_target=None,
        recent_sweeps=[],
        swept_bsl_levels=[],
        swept_ssl_levels=[],
        nearest_bsl_atr=1.0,
        nearest_ssl_atr=1.0,
        timestamp=time.time(),
    )


def test_tp_selector_never_selects_archived_swept_pool_even_if_closer_and_higher_sig():
    snap = _snapshot(
        bsl=[
            _target(101.50, PoolSide.BSL, PoolStatus.SWEPT, sig=500.0),
            _target(102.00, PoolSide.BSL, PoolStatus.CONFIRMED, sig=50.0),
        ],
        ssl=[_target(99.0, PoolSide.SSL, PoolStatus.CONFIRMED)],
    )

    tp, target, score, report = select_tp_with_report(
        snap, "long", entry=100.0, sl=99.5, atr=2.0, min_rr=0.5, posterior_prob=0.95
    )

    assert tp is not None
    assert target is not None
    assert target.pool.status is PoolStatus.CONFIRMED
    assert target.pool.price == pytest.approx(102.0)
    assert all(
        not (c["status"] == "SWEPT" and c["selected"])
        for c in report.as_dict()["candidates"]
    )


def test_sl_selector_never_anchors_stop_to_archived_pool():
    snap = _snapshot(
        ssl=[
            _target(94.0, PoolSide.SSL, PoolStatus.SWEPT, sig=500.0),
            _target(92.0, PoolSide.SSL, PoolStatus.CONFIRMED, sig=50.0),
        ],
        bsl=[_target(102.0, PoolSide.BSL, PoolStatus.CONFIRMED)],
    )

    sl, target, pick, report = select_sl_with_report(
        snap,
        "long",
        entry=100.0,
        atr=2.0,
        invalidation_price=96.0,
        max_buffer_atr=2.0,
        min_risk=1.0,
    )

    assert sl is not None
    assert target.pool.status is PoolStatus.CONFIRMED
    assert target.pool.price == pytest.approx(92.0)
    assert sl < target.pool.price < 100.0
    assert all(
        not (c["status"] == "SWEPT" and c["selected"])
        for c in report.as_dict()["candidates"]
    )


def test_entry_engine_tp_rr_floor_is_not_clamped_down_to_static_fallback():
    engine = EntryEngine.__new__(EntryEngine)
    engine._last_pool_plan = {
        "role": "TP",
        "selected": {"required_rr": 3.75},
    }

    assert engine._last_selected_tp_rr_floor(1.50) == pytest.approx(3.75)


def test_entry_engine_tp_rr_floor_keeps_dynamic_probability_compensated_floor():
    engine = EntryEngine.__new__(EntryEngine)
    engine._last_pool_plan = {
        "role": "TP",
        "selected": {"required_rr": 0.32},
    }

    assert engine._last_selected_tp_rr_floor(1.50) == pytest.approx(0.32)


_GEOMETRY_ASSETS = [r["asset_id"] for r in MULTI_ASSET_REQUESTS] or [
    "CRYPTO_DYNAMIC",
    "COMMODITY_DYNAMIC",
    "EQUITY_DYNAMIC",
    "INDEX_DYNAMIC",
    "OPTION_DYNAMIC",
    "FUTURE_DYNAMIC",
]


@pytest.mark.parametrize("asset", _GEOMETRY_ASSETS)
def test_every_configured_ticker_has_liquidity_aware_tp_and_sl_geometry(asset):
    # Asset-specific prices are normalized to a synthetic 100/2 ATR frame so the
    # same invariant is tested across the complete configured ticker universe:
    # long TP must be a live BSL above entry; long SL must be a live SSL below
    # invalidation and below entry.  The exchange/instrument layer maps these
    # normalized decisions onto each symbol's tick/lot policy elsewhere.
    snap = _snapshot(
        bsl=[_target(102.0, PoolSide.BSL, PoolStatus.CONFIRMED, sig=50.0)],
        ssl=[_target(96.0, PoolSide.SSL, PoolStatus.SWEPT, sig=500.0),
             _target(94.0, PoolSide.SSL, PoolStatus.CONFIRMED, sig=50.0)],
    )

    tp, tp_target, _, _ = select_tp_with_report(
        snap, "long", entry=100.0, sl=99.5, atr=2.0, min_rr=0.5, posterior_prob=0.95
    )
    sl, sl_target, _, _ = select_sl_with_report(
        snap,
        "long",
        entry=100.0,
        atr=2.0,
        invalidation_price=96.0,
        max_buffer_atr=2.0,
        min_risk=1.0,
    )

    assert asset
    assert tp is not None and sl is not None
    assert tp_target.pool.side is PoolSide.BSL
    assert tp_target.pool.status is PoolStatus.CONFIRMED
    assert sl_target.pool.side is PoolSide.SSL
    assert sl_target.pool.status is PoolStatus.CONFIRMED
    assert sl < 100.0 < tp


def test_legacy_compute_tp_refuses_vwap_or_swing_fallback_without_liquidity_map():
    tp = InstitutionalLevels.compute_tp(
        price=100.0,
        side="long",
        atr=2.0,
        sl_price=99.5,
        candles_1m=[],
        orderbook={},
        vwap=105.0,
        vwap_std=2.0,
        candles_5m=[],
        candles_15m=[{"h": 110.0, "l": 90.0}, {"h": 112.0, "l": 91.0}, {"h": 109.0, "l": 92.0}],
        liq_map=None,
    )
    assert tp is None

from strategy.liquidity_trail import LiquidityTrailEngine


def test_trailing_delivery_lock_ignores_archived_pools():
    snap = _snapshot(
        ssl=[
            _target(105.0, PoolSide.SSL, PoolStatus.SWEPT, sig=500.0, entry=108.0),
            _target(104.0, PoolSide.SSL, PoolStatus.CONFIRMED, sig=50.0, entry=108.0),
        ],
        bsl=[],
    )
    engine = LiquidityTrailEngine()

    new_sl, reason = engine._delivery_lock_from_pools(
        pos_side="long", price=108.0, true_be=101.0, atr=2.0, liq_snapshot=snap
    )

    assert new_sl is not None
    assert "$104.0" in reason
    assert 101.0 < new_sl < 108.0
''',
    ),
    ('test_v66_sl_mtf_liquidity_shield.py',
     '''import time
import pytest

from strategy.entry_engine import EntryEngine
from strategy.liquidity_map import LiquidityMapSnapshot, LiquidityPool, PoolSide, PoolStatus, PoolTarget
from strategy.liquidity_pool_selector import select_sl_with_report


def _target(price, pool_side, status=PoolStatus.CONFIRMED, *, entry=100.0, atr=2.0, sig=10.0, tf="5m", touches=5):
    now = time.time()
    pool = LiquidityPool(
        price=float(price), side=pool_side, timeframe=tf, status=status,
        touches=touches, created_at=now, last_touch=now, ob_aligned=True,
        fvg_aligned=False, htf_count=2,
    )
    return PoolTarget(
        pool=pool,
        distance_atr=abs(float(price) - float(entry)) / max(float(atr), 1e-9),
        direction="above" if float(price) > float(entry) else "below",
        significance=float(sig), tf_sources=[tf],
    )


def _snapshot(entry=100.0, atr=2.0, bsl=None, ssl=None):
    return LiquidityMapSnapshot(
        bsl_pools=list(bsl or []), ssl_pools=list(ssl or []), primary_target=None,
        recent_sweeps=[], swept_bsl_levels=[], swept_ssl_levels=[], nearest_bsl_atr=1.0,
        nearest_ssl_atr=1.0, timestamp=time.time(),
    )


def test_sl_selector_evaluates_htf_anchor_beyond_old_four_atr_window():
    snap = _snapshot(
        entry=100.0, atr=2.0,
        bsl=[_target(112.0, PoolSide.BSL, entry=100.0, atr=2.0, sig=35.0, tf="1d", touches=8)],
    )
    sl, target, pick, report = select_sl_with_report(
        snap, "short", entry=100.0, atr=2.0, invalidation_price=101.0,
        max_buffer_atr=2.0, min_risk=1.0,
    )
    assert sl is not None
    assert target.pool.timeframe == "1d"
    assert target.pool.price == pytest.approx(112.0)
    assert sl > target.pool.price > 100.0
    assert pick.buffer_atr >= 0.30
    assert "outside-liquidity-zone" in pick.reasons
    assert "outside SL search window" not in report.as_dict()["summary"]


def test_high_quality_stop_cluster_gets_wider_not_smaller_buffer():
    weak_snap = _snapshot(bsl=[_target(104.0, PoolSide.BSL, sig=2.0, tf="5m", touches=2)])
    strong_snap = _snapshot(bsl=[_target(104.0, PoolSide.BSL, sig=40.0, tf="4h", touches=9)])
    _, _, weak_pick, _ = select_sl_with_report(weak_snap, "short", entry=100.0, atr=2.0, invalidation_price=101.0)
    _, _, strong_pick, _ = select_sl_with_report(strong_snap, "short", entry=100.0, atr=2.0, invalidation_price=101.0)
    assert weak_pick is not None and strong_pick is not None
    assert strong_pick.buffer_atr > weak_pick.buffer_atr


def test_push_sl_behind_pools_handles_sl_equal_to_liquidity_pool():
    snap = _snapshot(
        bsl=[_target(105.0, PoolSide.BSL, sig=12.0, tf="15m", touches=6)],
    )
    engine = EntryEngine.__new__(EntryEngine)
    engine._last_liq_snapshot = snap
    pushed = engine._push_sl_behind_pools(sl=105.0, side="short", price=100.0, atr=2.0)
    assert pushed > 105.0
    assert pushed - 105.0 >= 0.30 * 2.0
''',
    ),
    ('test_v67_entry_integrity.py',
     '''import time
from types import SimpleNamespace

from strategy.entry_engine import EntryEngine, ICTContext, OrderFlowState
from strategy.liquidity_map import LiquidityMapSnapshot, LiquidityPool, PoolSide, PoolStatus, SweepResult
from strategy.quant_strategy import QuantStrategy


def _snapshot():
    return LiquidityMapSnapshot(
        bsl_pools=[], ssl_pools=[], primary_target=None,
        recent_sweeps=[], swept_bsl_levels=[], swept_ssl_levels=[],
        nearest_bsl_atr=1.0, nearest_ssl_atr=1.0, timestamp=time.time(),
    )


def _sweep(side=PoolSide.SSL, price=100.0, wick=99.5, direction="long"):
    pool = LiquidityPool(
        price=float(price), side=side, timeframe="15m", status=PoolStatus.SWEPT,
        touches=4, created_at=time.time(), last_touch=time.time(),
        ob_aligned=True, fvg_aligned=True, htf_count=1,
    )
    return SweepResult(
        pool=pool, sweep_candle_idx=0, wick_extreme=float(wick),
        rejection_pct=0.2, volume_ratio=1.0, quality=0.65,
        direction=direction, detected_at=time.time(),
    )


def test_entry_quality_gate_rejects_raw_posterior_without_delivery_acceptance():
    engine = EntryEngine()
    ps = SimpleNamespace(
        sweep=_sweep(), max_displacement=0.05,
        cisd_detected=False, ote_reached=False, ote_holding=False,
    )
    ok, reason = engine._institutional_entry_quality_gate(
        ps, "reverse", "long", _snapshot(), OrderFlowState(), ICTContext(),
        price=100.2, atr=2.0, now=time.time(), phase="DISPLACEMENT",
    )
    assert not ok
    assert "delivery_unaccepted" in reason
    assert engine._last_sweep_analysis["quality_score"] < engine._last_sweep_analysis["quality_critical_floor"]


def test_entry_quality_gate_allows_strong_delivery_without_retail_boolean_stack():
    engine = EntryEngine()
    ps = SimpleNamespace(
        sweep=_sweep(), max_displacement=1.40,
        cisd_detected=False, ote_reached=False, ote_holding=False,
    )
    ok, reason = engine._institutional_entry_quality_gate(
        ps, "reverse", "long", _snapshot(), OrderFlowState(tick_flow=0.15, cvd_trend=0.20), ICTContext(dealing_range_pd=0.32),
        price=102.4, atr=2.0, now=time.time(), phase="DISPLACEMENT",
    )
    assert ok
    assert "entry_quality_score" in reason
    assert engine._last_sweep_analysis["quality_score"] >= engine._last_sweep_analysis["quality_floor"]


def test_unified_gate_blocks_signal_if_entry_integrity_surface_is_broken():
    qs = QuantStrategy.__new__(QuantStrategy)
    qs._entry_engine = SimpleNamespace(_last_sweep_analysis={
        "displacement_atr": 0.10,
        "cisd": False,
        "ote": False,
        "quality_score": 0.40,
        "quality_floor": 0.52,
    })
    qs._dir_engine = None
    qs._ict = None
    signal = SimpleNamespace(side="long", entry_type="REVERSAL", rr_ratio=2.4)
    ok, reason = qs._unified_entry_gate(
        signal, ICTContext(), OrderFlowState(), _snapshot(), price=100.0, atr=2.0, now=time.time()
    )
    assert not ok
    assert "entry quality" in reason or "displacement" in reason


def test_legacy_ob_momentum_sl_builder_removed_from_entry_engine():
    assert not hasattr(EntryEngine, "_compute_sl")
''',
    ),
    ('test_v68_entry_architecture.py',
     '''import time
from types import SimpleNamespace

from strategy.entry_engine import EntryEngine, EntryType, ICTContext, OrderFlowState
from strategy.liquidity_map import LiquidityMapSnapshot, LiquidityPool, PoolSide, PoolStatus, SweepResult, PoolTarget
from strategy.quant_strategy import QuantStrategy


def _snapshot():
    return LiquidityMapSnapshot(
        bsl_pools=[], ssl_pools=[], primary_target=None,
        recent_sweeps=[], swept_bsl_levels=[], swept_ssl_levels=[],
        nearest_bsl_atr=1.0, nearest_ssl_atr=1.0, timestamp=time.time(),
    )


def _sweep(side=PoolSide.SSL, price=100.0, wick=99.5, direction="long"):
    pool = LiquidityPool(
        price=float(price), side=side, timeframe="15m", status=PoolStatus.SWEPT,
        touches=4, created_at=time.time(), last_touch=time.time(),
        ob_aligned=True, fvg_aligned=True, htf_count=1,
    )
    return SweepResult(
        pool=pool, sweep_candle_idx=0, wick_extreme=float(wick),
        rejection_pct=0.2, volume_ratio=1.0, quality=0.70,
        direction=direction, detected_at=time.time(),
    )


def _target():
    return PoolTarget(
        pool=LiquidityPool(
            price=110.0, side=PoolSide.BSL, timeframe="15m", status=PoolStatus.CONFIRMED,
            touches=3, created_at=time.time(), last_touch=time.time(),
        ),
        distance_atr=3.0,
        direction="long",
        significance=2.5,
        tf_sources=["15m"],
    )


def _signal(side="long", entry_type=EntryType.SWEEP_REVERSAL, sweep=None):
    sweep = sweep or _sweep(direction=side)
    return SimpleNamespace(
        side=side,
        entry_type=entry_type,
        entry_price=102.0,
        sl_price=98.0,
        tp_price=112.0,
        rr_ratio=2.5,
        conviction=0.80,
        sweep_result=sweep,
        target_pool=_target(),
    )


def _qs_with_analysis(analysis):
    qs = QuantStrategy.__new__(QuantStrategy)
    qs._entry_engine = SimpleNamespace(_last_sweep_analysis=analysis)
    qs._dir_engine = None
    qs._ict = None
    qs._last_hunt_prediction = None
    qs._active_institutional_size_mult = 1.0
    return qs


def test_v68_unified_gate_allows_fast_strong_sweep_without_waiting_for_cisd_or_ote():
    qs = _qs_with_analysis({
        "rev_score": 126.0,
        "cont_score": 22.0,
        "displacement_atr": 1.55,
        "cisd": False,
        "ote": False,
        "quality_score": 0.76,
        "quality_floor": 0.52,
        "quant_posterior": 0.72,
        "quant_ev": 0.42,
    })
    ok, reason = qs._unified_entry_gate(
        _signal(),
        ICTContext(dealing_range_pd=0.34, structure_15m="ranging", structure_4h="ranging"),
        OrderFlowState(tick_flow=0.20, cvd_trend=0.22),
        _snapshot(),
        price=102.0,
        atr=2.0,
        now=time.time(),
    )
    assert ok, reason
    assert qs._active_institutional_size_mult > 0


def test_v68_single_pd_or_htf_penalty_does_not_kill_strong_delivery_entry():
    qs = _qs_with_analysis({
        "rev_score": 118.0,
        "cont_score": 35.0,
        "displacement_atr": 1.70,
        "cisd": False,
        "ote": False,
        "quality_score": 0.73,
        "quality_floor": 0.52,
        "quant_posterior": 0.69,
        "quant_ev": 0.35,
    })
    ok, reason = qs._unified_entry_gate(
        _signal(side="long"),
        ICTContext(dealing_range_pd=0.68, structure_15m="bearish", structure_4h="bearish"),
        OrderFlowState(tick_flow=0.18, cvd_trend=0.12),
        _snapshot(),
        price=102.0,
        atr=2.0,
        now=time.time(),
    )
    assert ok, reason
    assert qs._last_entry_readiness.penalties
    assert qs._last_entry_readiness.size_mult < 1.0


def test_v68_unified_gate_blocks_deliveryless_static_score_candidate():
    qs = _qs_with_analysis({
        "rev_score": 80.0,
        "cont_score": 71.0,
        "displacement_atr": 0.05,
        "cisd": False,
        "ote": False,
        "quality_score": 0.44,
        "quality_floor": 0.52,
        "quant_posterior": 0.50,
        "quant_ev": 0.05,
    })
    ok, reason = qs._unified_entry_gate(
        _signal(),
        ICTContext(dealing_range_pd=0.50),
        OrderFlowState(tick_flow=0.02, cvd_trend=0.01),
        _snapshot(),
        price=102.0,
        atr=2.0,
        now=time.time(),
    )
    assert not ok
    assert "no accepted delivery" in reason or "readiness score" in reason


def test_v68_pre_order_rejection_replaces_expired_sweep_lock():
    engine = EntryEngine()
    sweep = _sweep()
    sig = _signal(sweep=sweep)
    key = engine._sweep_key(sweep)
    level_key = engine._sweep_level_key(sweep)
    engine._processed_sweeps[key] = time.time() - 222.0
    engine._processed_sweep_levels[level_key] = time.time() - 222.0
    engine.mark_pre_order_rejected(sig, cooldown_sec=30.0)
    assert engine._processed_sweeps[key] > time.time()
    assert engine._processed_sweep_levels[level_key] > time.time()


def test_v68_same_level_sweep_replay_is_suppressed_even_with_new_detected_at():
    engine = EntryEngine()
    now = time.time()
    first = _sweep(side=PoolSide.BSL, price=398.10, wick=399.0, direction="short")
    replay = _sweep(side=PoolSide.BSL, price=398.12, wick=399.1, direction="short")
    first.detected_at = now
    replay.detected_at = now + 12.0

    engine._lock_processed_sweep(first, now + 120.0, atr=0.38)

    assert engine._sweep_key(first) != engine._sweep_key(replay)
    assert engine._is_processed(replay, now + 12.0, atr=0.38)


def test_v68_dead_post_sweep_without_delivery_is_abandoned_before_full_timeout():
    engine = EntryEngine()
    ps = SimpleNamespace(
        sweep=_sweep(side=PoolSide.SSL, price=100.0, wick=99.5, direction="long"),
        max_displacement=0.0,
        cisd_detected=False,
        ote_reached=False,
        ote_holding=False,
    )

    invalid, reason = engine._post_sweep_invalidation_reason(
        ps, _snapshot(), OrderFlowState(), ICTContext(),
        price=100.1, atr=2.0, elapsed=120.0,
    )

    assert invalid
    assert "dead auction" in reason


def test_v68_post_sweep_abandons_when_price_crosses_structural_invalidation():
    engine = EntryEngine()
    ps = SimpleNamespace(
        sweep=_sweep(side=PoolSide.SSL, price=100.0, wick=99.5, direction="long"),
        max_displacement=0.6,
        cisd_detected=False,
        ote_reached=False,
        ote_holding=False,
    )

    invalid, reason = engine._post_sweep_invalidation_reason(
        ps, _snapshot(), OrderFlowState(), ICTContext(),
        price=98.7, atr=2.0, elapsed=20.0,
    )

    assert invalid
    assert "invalidated before entry" in reason
''',
    ),
    ('test_v70_flow_integrity.py',
     '''from types import SimpleNamespace


def test_entry_engine_liquidation_guard_uses_active_instrument_policy_leverage():
    import config
    from strategy.entry_engine import EntryEngine
    from core.instruments import (
        AssetClass,
        ExchangeInstrument,
        ExchangeName,
        TradableInstrument,
        instrument_scope,
    )

    # Global BTC leverage is intentionally high.  A non-BTC instrument with lower
    # exchange leverage must not inherit this inside SL/liquidation sanity.
    old_leverage = getattr(config, "LEVERAGE", 40)
    config.LEVERAGE = 40
    try:
        inst = TradableInstrument(
            asset_id="TESTX",
            display_name="Test xStock",
            asset_class=AssetClass.EQUITY,
            primary_exchange=ExchangeName.DELTA,
            by_exchange={
                ExchangeName.DELTA: ExchangeInstrument(
                    exchange=ExchangeName.DELTA,
                    symbol="TESTXUSD",
                    ws_symbol="TESTXUSD",
                    display_symbol="TESTXUSD",
                    asset_id="TESTX",
                    asset_class=AssetClass.EQUITY,
                    max_leverage=10,
                    status="active",
                )
            },
        )
        with instrument_scope(inst):
            _liq, guard, room = EntryEngine._liquidation_guard("long", 100.0)
        assert 67.0 < guard < 68.0
        assert room > 30.0
    finally:
        config.LEVERAGE = old_leverage


def test_unified_entry_gate_does_not_double_apply_readiness_sizing():
    from strategy.quant_strategy import QuantStrategy, EntryReadinessDecision

    qs = QuantStrategy.__new__(QuantStrategy)
    qs._active_institutional_size_mult = 0.50
    readiness = EntryReadinessDecision(
        allowed=True,
        score=0.80,
        floor=0.55,
        size_mult=0.60,
        reason="test pass",
        hard_rejects=[],
        penalties=[],
        allows=["test"],
    )
    qs._entry_readiness_surface = lambda *args, **kwargs: readiness
    ok, reason = qs._unified_entry_gate(
        SimpleNamespace(side="long", entry_type=SimpleNamespace(value="SWEEP_REVERSAL")),
        SimpleNamespace(),
        SimpleNamespace(),
        SimpleNamespace(),
        100.0,
        1.0,
        0.0,
    )
    assert ok is True
    assert reason == "ENTRY_READINESS_PASS"
    assert qs._active_institutional_size_mult == 0.50


def test_route_readiness_is_single_authority_for_same_signal():
    from strategy.quant_strategy import QuantStrategy, EntryReadinessDecision

    qs = QuantStrategy.__new__(QuantStrategy)
    calls = {"n": 0}
    readiness = EntryReadinessDecision(
        allowed=True,
        score=0.82,
        floor=0.55,
        size_mult=0.70,
        reason="cached pass",
        hard_rejects=[],
        penalties=[],
        allows=["cached"],
    )

    def surface(*args, **kwargs):
        calls["n"] += 1
        return readiness

    qs._entry_readiness_surface = surface
    signal = SimpleNamespace(
        side="long",
        entry_type=SimpleNamespace(value="SWEEP_REVERSAL"),
        entry_price=100.0,
    )

    first = qs._route_readiness_decision(
        signal, SimpleNamespace(), SimpleNamespace(), SimpleNamespace(), 100.0, 2.0
    )
    second = qs._route_readiness_decision(
        signal, SimpleNamespace(), SimpleNamespace(), SimpleNamespace(), 100.0, 2.0
    )

    assert first is readiness
    assert second is readiness
    assert calls["n"] == 1


def test_selector_utility_components_have_single_distance_quality_key():
    src = open("strategy/liquidity_pool_selector.py", "r", encoding="utf-8").read()
    assert src.count('"distance_quality": distance_quality') == 1
''',
    ),
    ('test_v71_liquidity_stop_invariant.py',
     '''from types import SimpleNamespace

from strategy.entry_engine import EntryEngine


def _engine_for_sl_envelope():
    e = EntryEngine.__new__(EntryEngine)
    e._atr_pctile = 0.5
    e._last_liq_snapshot = None
    e._current_quant_posterior = lambda: 0.50
    e._dominant_institutional_tp_reward = lambda snap, side, price, atr: 10.0
    # For short: guard above entry, plenty of liquidation room.
    e._liquidation_guard = lambda side, entry: (125.0, 130.0, 30.0)
    e._push_sl_behind_pools = lambda sl, side, price, atr: sl
    return e


def test_sl_envelope_refuses_to_execute_inside_required_protective_pool():
    e = _engine_for_sl_envelope()
    pick = SimpleNamespace(quality=0.10, reasons=["test protective BSL"])
    target = SimpleNamespace(pool=SimpleNamespace(price=108.0))
    e._find_sl_pool = lambda **kwargs: (110.0, target, pick)

    sl, reason = e._apply_institutional_sl_envelope(
        snap=SimpleNamespace(),
        side="short",
        price=100.0,
        atr=2.0,
        structural_sl=105.0,   # inside the BSL shield; should never route
        invalidation_price=104.0,
        label="reversal",
    )

    assert sl is None
    assert "refusing executable stop inside liquidity" in reason
    assert "protective SL pool" in reason


def test_sl_envelope_allows_stop_already_beyond_protective_pool():
    e = _engine_for_sl_envelope()
    pick = SimpleNamespace(quality=0.10, reasons=["test protective BSL"])
    target = SimpleNamespace(pool=SimpleNamespace(price=108.0))
    e._find_sl_pool = lambda **kwargs: (110.0, target, pick)

    sl, reason = e._apply_institutional_sl_envelope(
        snap=SimpleNamespace(),
        side="short",
        price=100.0,
        atr=2.0,
        structural_sl=112.0,   # already beyond pool SL
        invalidation_price=104.0,
        label="reversal",
    )

    assert sl == 112.0
    assert reason == "ok"


def test_sl_envelope_refuses_pool_beyond_liquidation_guard_instead_of_shrinking_inside():
    e = _engine_for_sl_envelope()
    e._liquidation_guard = lambda side, entry: (108.0, 109.0, 9.0)
    pick = SimpleNamespace(quality=0.95, reasons=["HTF protective BSL"])
    target = SimpleNamespace(pool=SimpleNamespace(price=118.0))
    e._find_sl_pool = lambda **kwargs: (120.0, target, pick)

    sl, reason = e._apply_institutional_sl_envelope(
        snap=SimpleNamespace(),
        side="short",
        price=100.0,
        atr=2.0,
        structural_sl=105.0,
        invalidation_price=104.0,
        label="reversal",
    )

    assert sl is None
    assert "beyond liquidation guard" in reason
    assert "inside liquidity" in reason
''',
    ),
    ('test_v73_native_bracket_fast_execution.py',
     '''import inspect

import config
from execution.order_manager import OrderManager
from strategy.quant_strategy import QuantStrategy
from exchanges.delta.api import DeltaAPI


def test_v73_runtime_fingerprint_present():
    src = inspect.getsource(QuantStrategy._log_init)
    # v74 supersedes the v73 runtime fingerprint while preserving v73 behavior.
    assert "v74-native-bracket-fast-fee-true-flow" in src
    assert "v70-flow-audit" not in src


def test_v73_protected_cross_uses_readiness_not_raw_confidence_hard_veto():
    src = inspect.getsource(QuantStrategy._enter_trade)
    assert "ENTRY_PROTECTED_CROSS_REQUIRE_SIGNAL_CONF" in src
    assert "_sig_conf_ok_for_cross" in src
    assert "readiness_score >= protected_cross_floor" in src
    assert "native_market_bracket" in src
    assert "NATIVE-BRACKET-MARKET" in src


def test_v73_delta_market_bracket_path_exists_and_no_naked_fallback():
    src = inspect.getsource(OrderManager.place_bracket_limit_entry)
    assert "market_entry" in src
    assert "place_bracket_market_entry" in src
    assert "not_filled_timeout" in src
    qsrc = inspect.getsource(QuantStrategy._enter_trade)
    assert "Delta native bracket rejected by exchange/API" in qsrc
    assert "no non-bracket fallback" in qsrc


def test_v73_delta_api_market_orders_do_not_send_limit_only_fields():
    src = inspect.getsource(DeltaAPI.place_order)
    assert '_LIMIT_LIKE_TYPES = {"limit_order", "stop_limit_order", "take_profit_limit_order"}' in src
    assert 'if _otype in _LIMIT_LIKE_TYPES:' in src


def test_v73_config_fast_bracket_policy():
    assert getattr(config, "ENTRY_PROTECTED_CROSS_REQUIRE_SIGNAL_CONF") is False
    assert getattr(config, "DELTA_NATIVE_BRACKET_MARKET_FOR_PROTECTED_CROSS") is True
    assert getattr(config, "PROTECTED_CROSS_FILL_TIMEOUT_SEC") <= 12.0
''',
    ),
    ('test_v74_execution_telemetry_truth.py',
     '''import inspect

from execution.order_manager import OrderManager
from strategy.liquidity_trail import LiquidityTrailEngine
from strategy.quant_strategy import QuantStrategy


def test_v74_runtime_fingerprint_present():
    src = inspect.getsource(QuantStrategy._log_init)
    assert "v74-native-bracket-fast-fee-true-flow" in src
    assert "v70-flow-audit" not in src


def test_v74_market_native_bracket_reports_taker_fill_type():
    src = inspect.getsource(OrderManager.place_bracket_limit_entry)
    assert 'data["fill_type"]    = "taker" if market_entry else "maker"' in src
    assert 'fill_type={data[\\'fill_type\\']}' in src


def test_v74_delivery_lock_preserves_r_multiple_in_trail_result():
    src = inspect.getsource(LiquidityTrailEngine._try_delivery_structure_lock)
    assert 'phase="DELIVERY_LOCK", r_multiple=r_multiple' in src
    assert 'phase="DELIVERY_LOCK", r_multiple=0.0' not in src
''',
    ),
    ('test_v78_institutional_agents.py',
     '''from types import SimpleNamespace

import pytest

from agents.portfolio_cio import PortfolioCIO
from agents.ticker_selection_agent import TickerSelectionAgent
from agents.universe_agent import UniverseAgent
from exchanges.icici.api import BreezeRestClient
from exchanges.icici.token_generator import extract_api_session
from fund.mandate import FundMandate


class _Data:
    def __init__(self, price=100.0, spread=0.02, warm=True):
        self.price = price
        self.spread = spread
        self.warm = warm

    def get_last_price(self):
        return self.price

    def get_orderbook(self):
        return {
            "bids": [[self.price - self.spread / 2, 1000]],
            "asks": [[self.price + self.spread / 2, 1000]],
            "timestamp": 1.0,
        }

    def get_candles(self, timeframe="5m", limit=100):
        n = 100 if self.warm else 8
        return [{"high": 101, "low": 99, "close": 100} for _ in range(n)]


class _Guard:
    max_open_positions = 1

    def can_evaluate_entry(self, ctx, contexts):
        return True, "ok"


def _ctx(asset_id, spread, ready=True):
    inst = SimpleNamespace(
        asset_id=asset_id,
        display_symbol=f"{asset_id}USD",
        primary_exchange=SimpleNamespace(value="delta"),
        asset_class=SimpleNamespace(value="crypto"),
    )
    strat = SimpleNamespace(
        _atr_5m=SimpleNamespace(atr=2.0, get_percentile=lambda: 0.5),
        _last_entry_signal=None,
        _last_entry_readiness=None,
        _last_institutional_decision=None,
        _entry_engine=None,
    )
    return SimpleNamespace(
        instrument=inst,
        data_manager=_Data(spread=spread),
        strategy=strat,
        ready=ready,
        has_position=False,
        phase_name="FLAT",
    )


def _mandate(audit_log_path="data/test_fund_audit.jsonl"):
    return FundMandate(
        enabled=True,
        paper_mode=True,
        top_n_execution_desks=1,
        top_n_depth_scan=2,
        min_ticker_score=0.1,
        min_execution_score=0.1,
        min_warmup_ratio=0.5,
        max_spread_bps_crypto=50.0,
        audit_log_path=str(audit_log_path),
    )


def test_extract_api_session_from_query_fragment_and_bare_fragment():
    assert extract_api_session("https://x/cb?apisession=abc123456789") == "abc123456789"
    assert extract_api_session("https://x/cb#apisession=xyz123456789") == "xyz123456789"
    assert extract_api_session("https://x/cb#baretoken12345") == "baretoken12345"


def test_ticker_selector_prefers_tighter_executable_spread():
    mandate = _mandate()
    universe = UniverseAgent(mandate)
    selector = TickerSelectionAgent(mandate)
    ranked = selector.rank(universe.diagnose_many([_ctx("TIGHT", 0.02), _ctx("WIDE", 0.40)]))
    assert ranked[0].asset_id == "TIGHT"
    assert ranked[0].score > ranked[1].score


def test_portfolio_cio_selects_only_top_execution_desk(tmp_path):
    mandate = _mandate(tmp_path / "audit.jsonl")
    report = PortfolioCIO(mandate).select_execution_queue([_ctx("A", 0.02), _ctx("B", 0.10)], _Guard())
    assert len(report.selected) == 1
    assert report.selected[0].asset_id == "A"
    assert "B" in {r.asset_id for r in report.rejected}


def test_portfolio_cio_zero_caps_scan_and_select_all_eligible_desks(tmp_path):
    mandate = FundMandate(
        enabled=True,
        paper_mode=True,
        top_n_execution_desks=0,
        top_n_depth_scan=0,
        min_ticker_score=0.1,
        min_execution_score=0.1,
        min_warmup_ratio=0.5,
        max_spread_bps_crypto=50.0,
        audit_log_path=str(tmp_path / "audit.jsonl"),
    )
    report = PortfolioCIO(mandate).select_execution_queue([_ctx("A", 0.02), _ctx("B", 0.03), _ctx("C", 0.04)], _Guard())
    assert {x.asset_id for x in report.selected} == {"A", "B", "C"}
    assert len(report.setup_candidates) == 3


def test_breeze_client_blocks_market_orders_before_network():
    client = BreezeRestClient(auth=SimpleNamespace())
    with pytest.raises(RuntimeError, match="market orders are not permitted"):
        client.place_order(
            stock_code="NIFTY",
            exchange_code="NFO",
            product="options",
            action="buy",
            order_type="market",
            quantity="25",
            price="0",
            validity="day",
        )
''',
    ),
    ('test_v80_dynamic_tradable_desk.py',
     '''from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument
from agents.tradable_ticker_desk import TradableTickerDesk


def _inst(asset_id, symbol, asset_class=AssetClass.CRYPTO):
    ei = ExchangeInstrument(
        exchange=ExchangeName.DELTA,
        symbol=symbol,
        ws_symbol=symbol,
        display_symbol=symbol,
        asset_id=asset_id,
        asset_class=asset_class,
        status="active",
        max_leverage=100,
        raw={"symbol": symbol},
    )
    return TradableInstrument(
        asset_id=asset_id,
        display_name=asset_id,
        asset_class=asset_class,
        primary_exchange=ExchangeName.DELTA,
        by_exchange={ExchangeName.DELTA: ei},
        priority=100,
    )


class _Delta:
    def get_tickers(self, contract_types=None):
        return {"success": True, "result": [
            {"symbol": "BTCUSD", "mark_price": "100", "best_bid": "99.9", "best_ask": "100.1", "volume": "100000", "high": "104", "low": "98", "open_interest": "10000"},
            {"symbol": "DEADUSD", "mark_price": "10", "best_bid": "9", "best_ask": "11", "volume": "1", "high": "10.1", "low": "9.9", "open_interest": "1"},
            {"symbol": "ETHUSD", "mark_price": "50", "best_bid": "49.95", "best_ask": "50.05", "volume": "80000", "high": "52", "low": "49", "open_interest": "9000"},
        ]}


def test_dynamic_desk_selects_shortlist_before_any_candle_stream(monkeypatch):
    monkeypatch.setattr("config.DYNAMIC_DESK_MAX_ACTIVE_CONTEXTS", 2, raising=False)
    monkeypatch.setattr("config.DYNAMIC_DESK_MIN_SCORE", 0.01, raising=False)
    desk = TradableTickerDesk()
    selection = desk.select([
        _inst("BTC", "BTCUSD"),
        _inst("DEAD", "DEADUSD"),
        _inst("ETH", "ETHUSD"),
    ], delta_api=_Delta())
    assert len(selection.selected) == 2
    assert {x.asset_id for x in selection.selected} == {"BTC", "ETH"}
    assert "stream load avoided" in ";".join(selection.notes)
''',
    ),
    ('test_v81_icici_breeze_compliance.py',
     '''import hashlib
import json
from types import SimpleNamespace

import pytest

from agents.tradable_ticker_desk import TradableTickerDesk
from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument
from exchanges.icici.api import BreezeRestClient
from exchanges.icici.breeze_auth import BreezeSession, BreezeTokenService
from exchanges.icici.token_generator import login_url


class FakeAuth:
    api_key = "app+key=with special"
    secret_key = "secret"

    def __init__(self):
        self.force_flags = []

    def get_session(self, force_refresh=False):
        self.force_flags.append(force_refresh)
        return BreezeSession(api_session="api-session", session_token="session-token", created_at=1.0, raw_customer_details={})

    def can_refresh_without_operator(self):
        return True


def test_breeze_login_url_encodes_app_key():
    assert login_url("abc+def=ghi!").endswith("abc%2Bdef%3Dghi%21")


def test_breeze_headers_follow_checksum_contract(monkeypatch):
    client = BreezeRestClient(auth=FakeAuth())
    monkeypatch.setattr(client, "_timestamp", lambda: "2026-05-09T10:00:00.000Z")
    payload = json.dumps({"exchange_code": "NSE"}, separators=(",", ":"))
    headers = client._headers(payload)
    expected = hashlib.sha256(("2026-05-09T10:00:00.000Z" + payload + "secret").encode("utf-8")).hexdigest()
    assert headers["X-Checksum"] == "token " + expected
    assert headers["X-AppKey"] == "app+key=with special"
    assert headers["X-SessionToken"] == "session-token"


def test_portfolio_positions_uses_official_path(monkeypatch):
    client = BreezeRestClient(auth=FakeAuth())
    seen = {}

    def fake_request(method, path, body=None, **kwargs):
        seen.update(method=method, path=path, body=body)
        return {"Success": []}

    monkeypatch.setattr(client, "request", fake_request)
    assert client.get_portfolio_positions() == {"Success": []}
    assert seen == {"method": "GET", "path": "/portfoliopositions", "body": {}}


def test_token_service_prefers_api_session_file(monkeypatch, tmp_path):
    p = tmp_path / "api_session.txt"
    p.write_text("login-session\\n", encoding="utf-8")
    svc = BreezeTokenService(api_key="app", secret_key="sec", api_session_path=p, cache_path=tmp_path / "cache.json")

    def fake_exchange(api_session):
        assert api_session == "login-session"
        return BreezeSession(api_session=api_session, session_token="header-session", created_at=1.0, raw_customer_details={})

    monkeypatch.setattr(svc, "exchange_api_session", fake_exchange)
    got = svc.get_session(force_refresh=True)
    assert got.session_token == "header-session"


def _icici_inst(asset_id="NIFTY"):
    ex = ExchangeInstrument(
        exchange=ExchangeName.ICICI,
        symbol=asset_id,
        ws_symbol=asset_id,
        display_symbol=asset_id,
        asset_id=asset_id,
        asset_class=AssetClass.OPTION,
        raw={"ExchangeCode": "NSE", "ShortName": asset_id, "Series": "EQ"},
    )
    return TradableInstrument(
        asset_id=asset_id,
        display_name=asset_id,
        asset_class=AssetClass.OPTION,
        primary_exchange=ExchangeName.ICICI,
        by_exchange={ExchangeName.ICICI: ex},
        priority=1,
    )


class FakeICICIQuotes:
    def __init__(self):
        self.calls = 0

    def get_quote_for_instrument(self, instrument):
        self.calls += 1
        return {"Success": {"ltp": "100", "bPrice": "99.95", "sPrice": "100.05", "ttv": "10000000"}}


def test_tradable_desk_icici_quote_probes_are_bounded(monkeypatch):
    monkeypatch.setattr("agents.tradable_ticker_desk._cfg", lambda name, default: 1 if name == "DYNAMIC_DESK_ICICI_QUOTE_PROBES" else default)
    api = FakeICICIQuotes()
    sel = TradableTickerDesk().select([_icici_inst("NIFTY"), _icici_inst("BANKNIFTY")], icici_api=api)
    assert api.calls == 1
    assert any("icici authenticated quote probes=1" in note for note in sel.notes)
''',
    ),
    ('test_v82_icici_telegram_token_generator.py',
     '''import threading

import config
from telegram.controller import TelegramBotController


def _controller_stub():
    c = object.__new__(TelegramBotController)
    c._icici_otp_cv = threading.Condition()
    c._icici_pending_otp = None
    c._icici_waiting_for_otp = False
    c._icici_refresh_thread = None
    c._icici_refresh_result = ""
    c.sent = []
    c.send_message = lambda msg, parse_mode="HTML": c.sent.append(msg) or True
    return c


def test_plain_six_digit_otp_is_accepted_when_icici_waiting():
    c = _controller_stub()
    with c._icici_otp_cv:
        c._icici_waiting_for_otp = True
    assert "OTP received" in c.handle_command("123456")
    assert c._icici_pending_otp == "123456"


def test_startup_auto_runs_icici_token_generator_with_telegram_otp(monkeypatch):
    c = _controller_stub()
    monkeypatch.setattr(config, "ICICI_AUTO_TOKEN_GENERATOR_ON_STARTUP", True, raising=False)
    monkeypatch.setattr(config, "DYNAMIC_DESK_ICICI_DETAILS_ENABLED", True, raising=False)
    monkeypatch.setattr(config, "ICICI_AUTH_REQUIRED_FOR_DETAILS", False, raising=False)
    monkeypatch.setattr(config, "ICICI_ENABLED", False, raising=False)
    c._telegram_icici_otp_getter = lambda: "654321"

    calls = []

    class FakeSession:
        def age_sec(self):
            return 1.2

        def masked(self):
            return {"session_token": "abc***1234"}

    class FakeService:
        def require_configured(self, *, for_login=False):
            calls.append(("require", for_login))

        def get_session(self, *, force_refresh=False):
            calls.append(("get", force_refresh))
            raise RuntimeError("missing api session")

        def refresh(self, *, otp_getter=None, otp_code=None):
            calls.append(("refresh", otp_getter()))
            return FakeSession()

    import exchanges.icici.breeze_auth as breeze_auth

    monkeypatch.setattr(breeze_auth, "BreezeTokenService", FakeService)
    c._ensure_icici_session_before_bot_start()

    assert ("refresh", "654321") in calls
    assert any("ICICI Breeze token ready" in msg for msg in c.sent)
''',
    ),
    ('test_v83_institutional_policy.py',
     '''from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument
from core.market_policy import active_policy


def _inst(exchange, ac, max_lev=0, symbol="TEST"):
    ei = ExchangeInstrument(
        exchange=exchange,
        symbol=symbol,
        ws_symbol=symbol,
        display_symbol=symbol,
        asset_id=symbol,
        asset_class=ac,
        max_leverage=max_lev,
    )
    return TradableInstrument(symbol, symbol, ac, exchange, {exchange: ei})


def test_icici_cash_and_options_are_one_x():
    cash = _inst(ExchangeName.ICICI, AssetClass.CASH, max_lev=99, symbol="RELIANCE")
    opt = _inst(ExchangeName.ICICI, AssetClass.OPTION, max_lev=99, symbol="NIFTYOPT")
    assert active_policy(cash).leverage == 1
    assert active_policy(opt).leverage == 1
    assert active_policy(cash).margin_pct <= 0.06


def test_delta_equity_uses_configured_venue_schedule_when_product_row_omits_cap():
    equity = _inst(ExchangeName.DELTA, AssetClass.EQUITY, max_lev=0, symbol="AAPLXUSD")
    assert active_policy(equity).leverage == 8  # 25x venue schedule * 32% utilisation


def test_delta_btc_uses_institutional_slice_of_venue_cap():
    crypto = _inst(ExchangeName.DELTA, AssetClass.CRYPTO, max_lev=0, symbol="BTCUSD")
    pol = active_policy(crypto)
    assert pol.leverage == 40  # 200x venue schedule * 20% utilisation, firm-capped at 40x


def test_confirmed_delta_leverage_is_used_with_symbol_utilisation():
    crypto = _inst(ExchangeName.DELTA, AssetClass.CRYPTO, max_lev=100, symbol="ETHUSD")
    pol = active_policy(crypto)
    assert pol.leverage == 28  # 100x confirmed venue cap * 28% ETH utilisation
''',
    ),
    ('test_v84_deskwise_options_architecture.py',
     '''from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument
from agents.desk_router import InstitutionalDeskRouter
from agents.indian_options_desk import BlackScholesModel, IndianOptionsDesk
from agents.tradable_ticker_desk import TradableTickerDesk


def _inst(asset_id, ex, ac, symbol=None, raw=None, max_leverage=0):
    ei = ExchangeInstrument(
        exchange=ex,
        symbol=symbol or asset_id,
        ws_symbol=symbol or asset_id,
        display_symbol=symbol or asset_id,
        asset_id=asset_id,
        asset_class=ac,
        max_leverage=max_leverage,
        raw=raw or {},
        status="active",
        tick_size=0.05 if ex == ExchangeName.ICICI else 0.1,
        lot_step=1,
        min_qty=1,
    )
    return TradableInstrument(asset_id=asset_id, display_name=asset_id, asset_class=ac, primary_exchange=ex, by_exchange={ex: ei})


def test_desk_router_separates_btc_crypto_us_stocks_commodities_and_options():
    r = InstitutionalDeskRouter()
    assert r.desk_id_for(_inst("BTCUSD", ExchangeName.DELTA, AssetClass.CRYPTO, "BTCUSD")) == "BTC_GLOBAL"
    assert r.desk_id_for(_inst("ETHUSD", ExchangeName.DELTA, AssetClass.CRYPTO, "ETHUSD")) == "CRYPTO_ALTS"
    assert r.desk_id_for(_inst("AAPL", ExchangeName.DELTA, AssetClass.EQUITY, "AAPLXUSD")) == "US_STOCK_DERIVATIVES"
    assert r.desk_id_for(_inst("PAXGUSD", ExchangeName.DELTA, AssetClass.COMMODITY, "PAXGUSD")) == "COMMODITIES_GLOBAL"
    opt = _inst("NIFTY_25000_C", ExchangeName.ICICI, AssetClass.OPTION, "NIFTY28MAY2625000CE", {"stock_code": "NIFTY", "right": "call", "strike_price": "25000", "expiry_date": "28-May-2026", "product_type": "OPTIDX"})
    assert r.desk_id_for(opt) == "ICICI_INDEX_OPTIONS"
    cash = _inst("RELIANCE", ExchangeName.ICICI, AssetClass.CASH, "RELIANCE", {"stock_code": "RELIANCE"})
    assert r.desk_id_for(cash) == "ICICI_REJECT_NON_OPTION"


def test_black_scholes_returns_greeks_and_theta_ratio():
    bs = BlackScholesModel.greeks("call", spot=25000, strike=25100, dte=7, rate=0.065, volatility=0.18, premium=180)
    assert bs is not None
    assert 0 < bs.delta < 1
    assert bs.gamma > 0
    assert bs.theta_to_premium >= 0


def test_icici_option_selector_scores_real_option_not_cash():
    desk = IndianOptionsDesk()
    opt = _inst("NIFTY_25000_C", ExchangeName.ICICI, AssetClass.OPTION, "NIFTY28MAY2625000CE", {"stock_code": "NIFTY", "right": "call", "strike_price": "25000", "expiry_date": "28-May-2026", "product_type": "OPTIDX"})
    score = desk.score_option(opt, {"ltp": "220", "best_bid": "219", "best_ask": "221", "underlying_spot_price": "25020", "iv": "18"})
    assert score.desk_id == "ICICI_INDEX_OPTIONS"
    assert score.bs is not None
    assert score.score > 0


def test_tradable_ticker_desk_rejects_icici_cash_before_runtime():
    cash = _inst("RELIANCE", ExchangeName.ICICI, AssetClass.CASH, "RELIANCE", {"stock_code": "RELIANCE", "exchange_code": "NSE", "product_type": "Cash"})
    sel = TradableTickerDesk().select([cash])
    assert not sel.selected
    assert any(r.reason == "icici_non_option_rejected" for r in sel.rows)


def test_icici_structural_underlying_is_rejected_without_allowlist():
    desk = IndianOptionsDesk()
    bad = _inst("BSE_26MAY2026_5200_P", ExchangeName.ICICI, AssetClass.OPTION, "BSE", {"stock_code": "BSE", "exchange_code": "BSE", "right": "put", "strike_price": "5200", "expiry_date": "26-May-2026", "product_type": "options"})
    score = desk.score_option(bad, {"ltp": "10", "best_bid": "9", "best_ask": "11", "underlying_spot_price": "5000", "iv": "20"})
    assert score.score == 0
    assert score.desk_id == "ICICI_REJECT_CORRUPT_OPTION"
    assert "structural_underlying" in score.reasons
''',
    ),
    ('test_v85_asset_desk_venue_router.py',
     '''from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument
from agents.desk_router import InstitutionalDeskRouter
from agents.tradable_ticker_desk import TradableTickerDesk


def _ei(asset_id, ex, symbol, ac=AssetClass.CRYPTO, max_leverage=0, raw=None):
    return ExchangeInstrument(
        exchange=ex,
        symbol=symbol,
        ws_symbol=symbol,
        display_symbol=symbol,
        asset_id=asset_id,
        asset_class=ac,
        status="active",
        max_leverage=max_leverage,
        raw=raw or {},
    )


def test_btc_is_one_asset_desk_even_when_multiple_venues_exist():
    inst = TradableInstrument(
        asset_id="BTC",
        display_name="BTC",
        asset_class=AssetClass.CRYPTO,
        primary_exchange=ExchangeName.DELTA,
        by_exchange={
            ExchangeName.DELTA: _ei("BTC", ExchangeName.DELTA, "BTCUSD", max_leverage=50),
            ExchangeName.COINSWITCH: _ei("BTC", ExchangeName.COINSWITCH, "BTCUSDT", max_leverage=0),
            ExchangeName.COINDCX: _ei("BTC", ExchangeName.COINDCX, "BTCINR", max_leverage=0),
        },
    )
    router = InstitutionalDeskRouter()
    assert router.desk_id_for(inst) == "BTC_GLOBAL"
    routes = router.venue_routes_for(inst, market_snapshots={
        ExchangeName.DELTA: {"BTCUSD": {"best_bid": 99990, "best_ask": 100000, "volume_24h": 10000, "last_price": 99995}},
        ExchangeName.COINSWITCH: {"BTCUSDT": {"best_bid": 99900, "best_ask": 100100, "volume_24h": 100, "last_price": 100000}},
    })
    assert len(routes) == 3
    assert routes[0].exchange == ExchangeName.DELTA


def test_selection_returns_single_btc_not_one_per_venue():
    inst = TradableInstrument(
        asset_id="BTC",
        display_name="BTC",
        asset_class=AssetClass.CRYPTO,
        primary_exchange=ExchangeName.DELTA,
        by_exchange={
            ExchangeName.DELTA: _ei("BTC", ExchangeName.DELTA, "BTCUSD", max_leverage=50),
            ExchangeName.COINSWITCH: _ei("BTC", ExchangeName.COINSWITCH, "BTCUSDT"),
        },
    )
    sel = TradableTickerDesk().select([inst])
    assert len(sel.selected) <= 1
    assert {r.desk_id for r in sel.rows} == {"BTC_GLOBAL"}
    assert sel.rows[0].route_exchange in {"delta", "coinswitch"}
''',
    ),
    ('test_institutional_desk_books.py',
     '''from types import SimpleNamespace

from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument
from observability.institutional import InstitutionalLogFilter
from orchestration.portfolio_manager import PortfolioManager


def _inst(asset_id: str, symbol: str, asset_class: AssetClass = AssetClass.CRYPTO) -> TradableInstrument:
    ei = ExchangeInstrument(
        exchange=ExchangeName.DELTA,
        symbol=symbol,
        ws_symbol=symbol,
        display_symbol=symbol,
        asset_id=asset_id,
        asset_class=asset_class,
        status="active",
        max_leverage=50,
        tick_size=0.1,
        lot_step=0.001 if asset_class == AssetClass.CRYPTO else 1.0,
        min_qty=0.001 if asset_class == AssetClass.CRYPTO else 1.0,
    )
    return TradableInstrument(asset_id, asset_id, asset_class, ExchangeName.DELTA, {ExchangeName.DELTA: ei})


def _ctx(inst: TradableInstrument, has_position: bool = False):
    return SimpleNamespace(instrument=inst, has_position=has_position, phase_name="ACTIVE" if has_position else "FLAT")


def test_desk_position_books_are_independent(monkeypatch):
    monkeypatch.setattr("config.PORTFOLIO_MAX_OPEN_POSITIONS", 4, raising=False)
    monkeypatch.setattr("config.DESK_POSITION_LIMITS_BY_ID", "BTC_GLOBAL:1,CRYPTO_ALTS:2", raising=False)
    monkeypatch.setattr("config.DESK_CAPITAL_WEIGHT_BY_ID", "BTC_GLOBAL:0.50,CRYPTO_ALTS:0.50", raising=False)
    monkeypatch.setattr("config.DESK_RISK_WEIGHT_BY_ID", "BTC_GLOBAL:0.50,CRYPTO_ALTS:0.50", raising=False)

    mgr = PortfolioManager()
    live_btc = _ctx(_inst("BTC", "BTCUSD"), has_position=True)
    second_btc_book = _ctx(_inst("BTCM", "BTCMUSD"), has_position=False)
    eth = _ctx(_inst("ETH", "ETHUSD"), has_position=False)

    ok, reason = mgr.can_evaluate_entry(second_btc_book, [live_btc, second_btc_book, eth])
    assert not ok
    assert "desk book exposure cap BTC_GLOBAL 1/1" in reason

    ok, reason = mgr.can_evaluate_entry(eth, [live_btc, second_btc_book, eth])
    assert ok, reason


def test_desk_weighted_allocation_sets_cash_and_risk_books(monkeypatch):
    monkeypatch.setattr("config.PORTFOLIO_MAX_OPEN_POSITIONS", 4, raising=False)
    monkeypatch.setattr("config.PORTFOLIO_BUDGET_MODE", "desk_weighted", raising=False)
    monkeypatch.setattr("config.PORTFOLIO_DESK_BUDGET_MODE", "desk_weighted", raising=False)
    monkeypatch.setattr("config.PORTFOLIO_RISK_BUDGET_MODE", "desk_weighted", raising=False)
    monkeypatch.setattr("config.PORTFOLIO_MAX_AGGREGATE_RISK_PCT", 3.0, raising=False)
    monkeypatch.setattr("config.RISK_PER_TRADE", 0.005, raising=False)
    monkeypatch.setattr("config.DESK_POSITION_LIMITS_BY_ID", "BTC_GLOBAL:1,CRYPTO_ALTS:2", raising=False)
    monkeypatch.setattr("config.DESK_CAPITAL_WEIGHT_BY_ID", "BTC_GLOBAL:0.60,CRYPTO_ALTS:0.40", raising=False)
    monkeypatch.setattr("config.DESK_RISK_WEIGHT_BY_ID", "BTC_GLOBAL:0.50,CRYPTO_ALTS:0.50", raising=False)

    mgr = PortfolioManager()
    btc = _ctx(_inst("BTC", "BTCUSD"))
    eth = _ctx(_inst("ETH", "ETHUSD"))
    raw = {"available": 1000.0, "total": 1000.0}

    btc_alloc = mgr.allocate_balance(btc, [btc, eth], raw)
    eth_alloc = mgr.allocate_balance(eth, [btc, eth], raw)

    assert btc_alloc["portfolio_desk_id"] == "BTC_GLOBAL"
    assert btc_alloc["available"] == 600.0
    assert btc_alloc["portfolio_desk_risk_per_slot"] == 15.0
    assert btc_alloc["risk_total"] == 3000.0

    assert eth_alloc["portfolio_desk_id"] == "CRYPTO_ALTS"
    assert eth_alloc["available"] == 200.0
    assert eth_alloc["portfolio_desk_risk_per_slot"] == 7.5
    assert eth_alloc["risk_total"] == 1500.0


def test_institutional_log_filter_dedupes_repeated_info(monkeypatch):
    import logging

    monkeypatch.setattr("config.INSTITUTIONAL_LOG_DEDUPE_SEC", 60.0, raising=False)
    flt = InstitutionalLogFilter()
    first = logging.LogRecord("desk", logging.INFO, __file__, 1, "Agentic CIO gate: parked", (), None)
    second = logging.LogRecord("desk", logging.INFO, __file__, 1, "Agentic CIO gate: parked", (), None)
    warning = logging.LogRecord("desk", logging.WARNING, __file__, 1, "Agentic CIO gate: parked", (), None)

    assert flt.filter(first)
    assert not flt.filter(second)
    assert flt.filter(warning)
''',
    ),
]


def _load_consolidated_tests():
    current_globals = globals()
    base_dir = Path(__file__).resolve().parent
    exported = 0
    for index, (filename, source) in enumerate(_CONSOLIDATED_TEST_SOURCES, start=1):
        stem = Path(filename).stem
        module_name = f"_combined_{index:03d}_{stem}"
        module = types.ModuleType(module_name)
        module.__file__ = str(base_dir / filename)
        module.__package__ = ""
        sys.modules[module_name] = module
        exec(compile(source, module.__file__, "exec"), module.__dict__)

        for name, obj in list(module.__dict__.items()):
            if name.startswith("test_") and callable(obj):
                public_name = f"test_{stem}__{name[5:]}"
                obj.__name__ = public_name
                obj.__qualname__ = public_name
                current_globals[public_name] = obj
                exported += 1
            elif name.startswith("Test") and isinstance(obj, type):
                public_name = f"Test_{stem}__{name}"
                obj.__name__ = public_name
                obj.__qualname__ = public_name
                current_globals[public_name] = obj
                exported += 1
    return exported


_EXPORTED_TEST_COUNT = _load_consolidated_tests()


def test_combined_suite_loaded_all_version_tests():
    assert _EXPORTED_TEST_COUNT >= 1


def test_v105_icici_runtime_is_underlying_first_not_strike_first():
    from agents.icici_chain_architect import build_underlying_payload, select_contract_for_thesis
    from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument

    def opt(strike, right):
        raw = {
            "stock_code": "NIFTY",
            "exchange_code": "NFO",
            "product_type": "OPTIDX",
            "right": right,
            "strike_price": str(strike),
            "expiry_date": "19-May-2026",
            "TradingSymbol": f"NIFTY_19MAY2026_{strike}_{right[:1].upper()}",
        }
        return ExchangeInstrument(ExchangeName.ICICI, raw["TradingSymbol"], raw["TradingSymbol"], raw["TradingSymbol"], "NIFTY", AssetClass.OPTION, raw=raw, tick_size=0.05, lot_step=75, min_qty=75)

    calls_puts = [opt(22800, "Call"), opt(23000, "Call"), opt(22800, "Put"), opt(23000, "Put")]
    payload = build_underlying_payload("NIFTY", "ICICI_INDEX_OPTIONS", calls_puts)
    ei = ExchangeInstrument(ExchangeName.ICICI, "NIFTY", "NIFTY", "NIFTY", "NIFTY", AssetClass.OPTION, raw=payload, tick_size=0.05, lot_step=75, min_qty=75)
    inst = TradableInstrument("NIFTY", "NIFTY", AssetClass.OPTION, ExchangeName.ICICI, {ExchangeName.ICICI: ei})

    assert payload["icici_underlying_desk"] is True
    assert payload["contract_selector_mode"] == "post_thesis"
    assert len(payload["chain_candidates"]) == 4
    assert select_contract_for_thesis(inst, "long", underlying_spot=22900).right == "call"
    assert select_contract_for_thesis(inst, "short", underlying_spot=22900).right == "put"


def test_v105_registry_selects_one_icici_context_per_underlying():
    from execution.instrument_registry import _select_icici_options_for_runtime
    from core.instruments import AssetClass, ExchangeInstrument, ExchangeName

    rows = []
    for underlying in ("NIFTY", "BANKNIFTY"):
        for strike in (22000, 22100, 22200, 22300):
            for right in ("Call", "Put"):
                raw = {"stock_code": underlying, "exchange_code": "NFO", "product_type": "OPTIDX", "right": right, "strike_price": str(strike), "expiry_date": "19-May-2026", "TradingSymbol": f"{underlying}_{strike}_{right[0]}"}
                rows.append(ExchangeInstrument(ExchangeName.ICICI, raw["TradingSymbol"], raw["TradingSymbol"], raw["TradingSymbol"], underlying, AssetClass.OPTION, raw=raw, tick_size=0.05, lot_step=75, min_qty=75))
    selected = _select_icici_options_for_runtime(rows)
    ids = {x.asset_id for x in selected}
    assert "NIFTY" in ids and "BANKNIFTY" in ids
    assert not any("22000" in x.asset_id or "22100" in x.asset_id for x in selected)
    assert all((x.raw or {}).get("icici_underlying_desk") for x in selected)



def test_v106_breeze_session_expires_at_midnight_rollover(monkeypatch, tmp_path):
    from datetime import datetime, timedelta, timezone

    from exchanges.icici import breeze_auth
    from exchanges.icici.breeze_auth import BreezeSession, BreezeTokenService

    tz = timezone(timedelta(minutes=330))
    now = datetime(2026, 5, 10, 8, 0, tzinfo=tz)
    monkeypatch.setattr(breeze_auth.time, "time", lambda: now.timestamp())
    monkeypatch.setattr(breeze_auth.config, "ICICI_SESSION_EXPIRES_DAILY", True, raising=False)
    svc = BreezeTokenService(
        api_key="app",
        secret_key="secret",
        api_session_path=tmp_path / "api_session.txt",
        cache_path=tmp_path / "cache.json",
        ttl_sec=18 * 60 * 60,
    )
    monkeypatch.setattr(svc, "_now_local", lambda: now)

    yesterday = datetime(2026, 5, 9, 23, 59, tzinfo=tz)
    stale = BreezeSession("api", "session-token", yesterday.timestamp(), {})
    assert svc.session_status(stale)["reason"] == "midnight_rollover"
    assert not svc.validate_session(stale)

    today = datetime(2026, 5, 10, 7, 59, tzinfo=tz)
    fresh = BreezeSession("api", "session-token", today.timestamp(), {})
    assert svc.session_status(fresh)["valid"] is True
    assert svc.validate_session(fresh)


def test_v106_breeze_refresh_falls_back_to_operator_login_when_api_session_is_stale(monkeypatch, tmp_path):
    from exchanges.icici import breeze_auth
    from exchanges.icici.breeze_auth import BreezeSession, BreezeTokenService

    api_path = tmp_path / "api_session.txt"
    api_path.write_text("stale-login-session\n", encoding="utf-8")
    svc = BreezeTokenService(
        api_key="app",
        secret_key="secret",
        client_id="client",
        password="password",
        api_session_path=api_path,
        cache_path=tmp_path / "cache.json",
    )
    calls = []

    def fake_exchange(api_session):
        calls.append(api_session)
        if api_session == "stale-login-session":
            raise RuntimeError("Breeze CustomerDetails failed: expired")
        return BreezeSession(api_session, "fresh-session-token", breeze_auth.time.time(), {})

    monkeypatch.setattr(svc, "exchange_api_session", fake_exchange)
    monkeypatch.setattr(breeze_auth, "generate_api_session", lambda **kwargs: "fresh-login-session")

    got = svc.refresh(otp_getter=lambda: "123456")
    assert got.session_token == "fresh-session-token"
    assert calls == ["stale-login-session", "fresh-login-session"]


def _telegram_controller_stub_for_icici():
    import threading

    from telegram.controller import TelegramBotController

    c = object.__new__(TelegramBotController)
    c._icici_otp_cv = threading.Condition()
    c._icici_pending_otp = None
    c._icici_waiting_for_otp = False
    c._icici_refresh_thread = None
    c._icici_refresh_result = ""
    c._icici_renewal_stop = threading.Event()
    c._icici_renewal_thread = None
    c._icici_renewal_last_attempt_ts = 0.0
    c._icici_renewal_last_attempt_date = ""
    c._icici_renewal_last_success_date = ""
    c.sent = []
    c.send_message = lambda msg, parse_mode="HTML": c.sent.append(msg) or True
    return c


def test_v106_telegram_icici_manual_token_aliases(monkeypatch):
    c = _telegram_controller_stub_for_icici()
    monkeypatch.setattr(c, "_cmd_icici_refresh", lambda: "refresh-started")
    monkeypatch.setattr(c, "_cmd_icici_status", lambda: "status-ready")

    assert c.handle_command("/icici_token") == "refresh-started"
    assert c.handle_command("icici_auth") == "refresh-started"
    assert c.handle_command("icici_status") == "status-ready"


def test_v106_icici_scheduler_retries_after_8_until_session_ready(monkeypatch):
    from datetime import datetime

    import config
    import telegram.controller as controller

    c = _telegram_controller_stub_for_icici()
    monkeypatch.setattr(config, "ICICI_TOKEN_RENEWAL_ENABLED", True, raising=False)
    monkeypatch.setattr(config, "ICICI_AUTO_TOKEN_GENERATOR_ON_STARTUP", True, raising=False)
    monkeypatch.setattr(config, "ICICI_ENABLED", True, raising=False)
    monkeypatch.setattr(config, "ICICI_TOKEN_RENEWAL_TIME", "08:00", raising=False)
    monkeypatch.setattr(config, "ICICI_TOKEN_RENEWAL_RETRY_SEC", 30 * 60, raising=False)
    monkeypatch.setattr(c, "_icici_session_ready_for_today", lambda: False)
    monkeypatch.setattr(controller.time, "time", lambda: 1_000.0)

    now = datetime(2026, 5, 10, 8, 0, tzinfo=c._icici_auth_tz())
    assert c._icici_renewal_due(now)

    c._icici_renewal_last_attempt_date = "2026-05-10"
    c._icici_renewal_last_attempt_ts = 1_000.0
    assert not c._icici_renewal_due(now)

    monkeypatch.setattr(controller.time, "time", lambda: 1_000.0 + 31 * 60)
    assert c._icici_renewal_due(now)


def test_v107_icici_scheduler_does_not_fire_stale_morning_job_at_night(monkeypatch):
    from datetime import datetime

    import config

    c = _telegram_controller_stub_for_icici()
    monkeypatch.setattr(config, "ICICI_TOKEN_RENEWAL_ENABLED", True, raising=False)
    monkeypatch.setattr(config, "ICICI_AUTO_TOKEN_GENERATOR_ON_STARTUP", True, raising=False)
    monkeypatch.setattr(config, "ICICI_ENABLED", True, raising=False)
    monkeypatch.setattr(config, "ICICI_TOKEN_RENEWAL_TIME", "08:00", raising=False)
    monkeypatch.setattr(config, "ICICI_TOKEN_RENEWAL_WINDOW_SEC", 4 * 60 * 60, raising=False)
    monkeypatch.setattr(c, "_icici_session_ready_for_today", lambda: False)

    morning = datetime(2026, 5, 10, 8, 30, tzinfo=c._icici_auth_tz())
    night = datetime(2026, 5, 10, 22, 20, tzinfo=c._icici_auth_tz())

    assert c._icici_renewal_due(morning)
    assert not c._icici_renewal_due(night)


def test_v107_icici_startup_waits_for_existing_token_refresh(monkeypatch):
    import threading
    import time as pytime

    import config

    c = _telegram_controller_stub_for_icici()
    c._icici_refresh_thread = threading.Thread(target=lambda: pytime.sleep(0.20), daemon=True)
    c._icici_refresh_thread.start()
    monkeypatch.setattr(config, "ICICI_AUTO_TOKEN_GENERATOR_ON_STARTUP", True, raising=False)
    monkeypatch.setattr(config, "ICICI_ENABLED", True, raising=False)
    monkeypatch.setattr(config, "ICICI_AUTH_REQUIRED_FOR_DETAILS", True, raising=False)
    monkeypatch.setattr(config, "ICICI_STARTUP_TOKEN_WAIT_SEC", 0.01, raising=False)
    monkeypatch.setattr(c, "_icici_otp_timeout_sec", lambda: -60.0)

    try:
        with pytest.raises(RuntimeError, match="already running"):
            c._ensure_icici_session_before_bot_start()
    finally:
        c._icici_refresh_thread.join(timeout=1.0)

    assert any("already running" in msg for msg in c.sent)


def test_v107_icici_underlying_chain_desk_is_selectable_without_quote(monkeypatch):
    from agents.desk_router import InstitutionalDeskRouter
    from agents.icici_chain_architect import build_underlying_payload
    from agents.tradable_ticker_desk import TradableTickerDesk
    from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument

    def opt(strike, right):
        raw = {
            "stock_code": "NIFTY",
            "exchange_code": "NFO",
            "product_type": "OPTIDX",
            "right": right,
            "strike_price": str(strike),
            "expiry_date": "19-May-2026",
            "TradingSymbol": f"NIFTY_19MAY2026_{strike}_{right[:1].upper()}",
        }
        return ExchangeInstrument(
            ExchangeName.ICICI,
            raw["TradingSymbol"],
            raw["TradingSymbol"],
            raw["TradingSymbol"],
            "NIFTY",
            AssetClass.OPTION,
            raw=raw,
            tick_size=0.05,
            lot_step=75,
            min_qty=75,
        )

    rows = [opt(22800, "Call"), opt(23000, "Call"), opt(22800, "Put"), opt(23000, "Put")]
    payload = build_underlying_payload("NIFTY", "ICICI_INDEX_OPTIONS", rows)
    ei = ExchangeInstrument(
        ExchangeName.ICICI,
        "NIFTY",
        "NIFTY",
        "NIFTY",
        "NIFTY",
        AssetClass.OPTION,
        raw=payload,
        tick_size=0.05,
        lot_step=75,
        min_qty=75,
    )
    inst = TradableInstrument("NIFTY", "NIFTY", AssetClass.OPTION, ExchangeName.ICICI, {ExchangeName.ICICI: ei})

    monkeypatch.setattr("config.ICICI_OPTION_REQUIRE_LIVE_QUOTE", True, raising=False)
    monkeypatch.setattr("config.DYNAMIC_DESK_MAX_ACTIVE_CONTEXTS", 10, raising=False)
    monkeypatch.setattr("config.DYNAMIC_DESK_MIN_SCORE", 0.38, raising=False)
    monkeypatch.setattr("config.DESK_ICICI_INDEX_OPTIONS_MAX_ACTIVE", 4, raising=False)
    monkeypatch.setattr("config.DESK_ENABLED_IDS", "", raising=False)

    assert InstitutionalDeskRouter().desk_id_for(inst) == "ICICI_INDEX_OPTIONS"
    selection = TradableTickerDesk().select([inst], icici_api=None)
    row = next(r for r in selection.rows if r.asset_id == "NIFTY")

    assert inst in selection.selected
    assert row.selected is True
    assert row.desk_id == "ICICI_INDEX_OPTIONS"
    assert row.score >= 0.38


def test_v107_multi_asset_operator_heartbeat_log_path_removed():
    import inspect

    from orchestration.multi_asset_bot import AssetContext, MultiAssetQuantBot

    assert "last_heartbeat_sec" not in getattr(AssetContext, "__dataclass_fields__", {})
    assert not hasattr(MultiAssetQuantBot, "_maybe_asset_heartbeat")
    assert "_maybe_asset_heartbeat" not in inspect.getsource(MultiAssetQuantBot.run)
    assert "DESK_HEARTBEAT" not in inspect.getsource(MultiAssetQuantBot)


def test_v108_runtime_signal_guard_ignores_protected_sigterm(monkeypatch, tmp_path):
    import signal

    import config
    import runtime.signal_guard as signal_guard

    captured = {}
    stopped = []
    notified = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(config, "RUNTIME_PROTECT_EXTERNAL_SIGTERM", True, raising=False)
    monkeypatch.setattr(signal_guard.signal, "signal", lambda sig, handler: captured.setdefault(sig, handler))

    signal_guard.install_signal_handlers("test_controller", shutdown=lambda: stopped.append(True), notify=notified.append)
    captured[signal.SIGTERM](signal.SIGTERM, None)

    assert stopped == []
    assert notified and "ignored" in notified[0]
    assert (tmp_path / "data" / "last_shutdown.json").exists()

    monkeypatch.setattr(config, "RUNTIME_PROTECT_EXTERNAL_SIGTERM", False, raising=False)
    with pytest.raises(SystemExit):
        captured[signal.SIGTERM](signal.SIGTERM, None)
    assert stopped == [True]


def test_v108_hyperliquid_is_primary_with_delta_secondary_in_registry():
    from execution.instrument_registry import InstrumentRegistry
    from core.instruments import ExchangeName

    class FakeHyperliquid:
        def get_products(self):
            return {"success": True, "result": [{
                "symbol": "BTC",
                "base": "BTC",
                "quote": "USDC",
                "status": "active",
                "szDecimals": 5,
                "maxLeverage": 40,
                "midPx": "80000",
                "dayNtlVlm": "1000000000",
            }]}

    class FakeDelta:
        def get_products(self, contract_types=None):
            return {"success": True, "result": [{
                "symbol": "BTCUSD",
                "underlying_asset": {"symbol": "BTC"},
                "quoting_asset": {"symbol": "USD"},
                "contract_type": "perpetual_futures",
                "state": "active",
                "max_leverage": "100",
                "tick_size": "0.5",
                "contract_value": "0.001",
            }]}

    reg = InstrumentRegistry(execution_preference="hyperliquid")
    report = reg.discover(
        hyperliquid_api=FakeHyperliquid(),
        delta_api=FakeDelta(),
        include_exchanges="hyperliquid,delta",
        include_asset_classes="crypto",
        discovery_mode="dynamic",
    )
    btc = next(x for x in report.matched if x.asset_id == "BTC")

    assert btc.primary_exchange == ExchangeName.HYPERLIQUID
    assert ExchangeName.HYPERLIQUID in btc.by_exchange
    assert ExchangeName.DELTA in btc.by_exchange


def test_v108_execution_router_accepts_hyperliquid_as_active_primary():
    from execution.router import ExecutionRouter

    class OM:
        def __init__(self, name):
            self.name = name

        def get_balance(self):
            return {"available": 100.0}

    router = ExecutionRouter(
        coinswitch_om=None,
        delta_om=OM("delta"),
        hyperliquid_om=OM("hyperliquid"),
        default="hyperliquid",
    )

    assert router.active_exchange == "hyperliquid"
    assert router.active.name == "hyperliquid"


def test_v108_hyperliquid_order_manager_normalises_sdk_order_response():
    from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument
    from execution.order_manager import OrderManager

    ei = ExchangeInstrument(
        exchange=ExchangeName.HYPERLIQUID,
        symbol="BTC",
        ws_symbol="BTC",
        display_symbol="BTC-PERP",
        asset_id="BTC",
        asset_class=AssetClass.CRYPTO,
        tick_size=0.5,
        lot_step=0.00001,
        min_qty=0.00001,
        max_leverage=40,
    )
    inst = TradableInstrument("BTC", "BTC", AssetClass.CRYPTO, ExchangeName.HYPERLIQUID, {ExchangeName.HYPERLIQUID: ei})

    class FakeHL:
        def place_order(self, **kwargs):
            assert kwargs["symbol"] == "BTC"
            return {"status": "ok", "response": {"data": {"statuses": [{"resting": {"oid": 12345}}]}}}

        def get_balance(self, currency="USDC"):
            return {"available": 100.0}

    om = OrderManager(FakeHL(), exchange_name="hyperliquid", instrument=inst)
    data = om.place_limit_order("BUY", 0.001, 80000.0)

    assert data["order_id"] == "12345"
    assert data["quantity"] == 0.001


def test_v109_hyperliquid_public_api_starts_without_sdk(monkeypatch):
    import builtins

    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if str(name).startswith("hyperliquid"):
            raise ModuleNotFoundError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    from exchanges.hyperliquid.api import HyperliquidAPI

    api = HyperliquidAPI(account_address="", secret_key="", base_url="https://example.invalid")

    assert api.sdk_available is False
    assert api.info is None
    assert api.can_trade is False


def test_v109_hyperliquid_missing_sdk_marks_live_gate_not_ready(monkeypatch):
    import config

    monkeypatch.setattr(config, "EXECUTION_EXCHANGE", "hyperliquid", raising=False)
    monkeypatch.setattr(config, "FUND_PAPER_MODE", False, raising=False)
    monkeypatch.setattr(config, "FUND_LIVE_ORDERING_ENABLED", True, raising=False)
    monkeypatch.setattr(config, "HYPERLIQUID_ACCOUNT_ADDRESS", "0xabc", raising=False)
    monkeypatch.setattr(config, "HYPERLIQUID_SECRET_KEY", "0xdef", raising=False)
    monkeypatch.setattr(config, "hyperliquid_sdk_available", lambda: False, raising=False)

    ready, reason = config.assert_live_ordering_ready()

    assert ready is False
    assert "hyperliquid-python-sdk" in reason


def test_v109_hyperliquid_client_failure_does_not_abort_api_build(monkeypatch):
    import config
    import orchestration.multi_asset_bot as mb

    class BrokenHyperliquid:
        def __init__(self):
            raise RuntimeError("sdk missing")

    monkeypatch.setattr(config, "HYPERLIQUID_ENABLED", True, raising=False)
    monkeypatch.setattr(config, "DELTA_API_KEY", "", raising=False)
    monkeypatch.setattr(config, "DELTA_SECRET_KEY", "", raising=False)
    monkeypatch.setattr(config, "COINSWITCH_API_KEY", "", raising=False)
    monkeypatch.setattr(config, "COINSWITCH_SECRET_KEY", "", raising=False)
    monkeypatch.setattr(config, "ICICI_DISCOVERY_ENABLED", False, raising=False)
    monkeypatch.setattr(config, "DYNAMIC_DESK_ICICI_DETAILS_ENABLED", False, raising=False)
    monkeypatch.setattr(config, "ICICI_ENABLED", False, raising=False)
    monkeypatch.setattr(mb, "HyperliquidAPI", BrokenHyperliquid)

    bot = object.__new__(mb.MultiAssetQuantBot)
    hl_api, delta_api, cs_api, icici_api = mb.MultiAssetQuantBot._build_api_clients(bot)

    assert hl_api is None
    assert delta_api is None
    assert cs_api is None
    assert icici_api is None


def test_v109_public_hyperliquid_data_uses_delta_execution_fallback(monkeypatch):
    from types import SimpleNamespace

    import orchestration.multi_asset_bot as mb
    from core.instruments import AssetClass, ExchangeInstrument, ExchangeName, TradableInstrument

    hl_ei = ExchangeInstrument(ExchangeName.HYPERLIQUID, "BTC", "BTC", "BTC-PERP", "BTC", AssetClass.CRYPTO, max_leverage=40)
    delta_ei = ExchangeInstrument(ExchangeName.DELTA, "BTCUSD", "BTCUSD", "BTCUSD", "BTC", AssetClass.CRYPTO, max_leverage=100)
    inst = TradableInstrument("BTC", "BTC", AssetClass.CRYPTO, ExchangeName.HYPERLIQUID, {
        ExchangeName.HYPERLIQUID: hl_ei,
        ExchangeName.DELTA: delta_ei,
    })

    class FakeOM:
        def __init__(self, api, exchange_name="delta", instrument=None):
            self.exchange_name = exchange_name

        def get_balance(self):
            return {"available": 100.0}

    class FakeDM:
        def __init__(self, instrument=None, api=None):
            self.instrument = instrument
            self.api = api

    class FakeAgg:
        def __init__(self, primary_dm, secondary_dm, instrument=None, analysis_dm=None):
            self.primary_dm = primary_dm
            self.secondary_dm = secondary_dm

        def register_strategy(self, strategy):
            self.strategy = strategy

    class FakeRisk:
        def __init__(self, *args, **kwargs):
            pass

    class FakeStrategy:
        def __init__(self, *args, **kwargs):
            pass

    monkeypatch.setattr(mb, "OrderManager", FakeOM)
    monkeypatch.setattr(mb, "HyperliquidDataManager", FakeDM)
    monkeypatch.setattr(mb, "DeltaDataManager", FakeDM)
    monkeypatch.setattr(mb, "MarketAggregator", FakeAgg)
    monkeypatch.setattr(mb, "PortfolioRiskManager", FakeRisk)
    monkeypatch.setattr(mb, "QuantStrategy", FakeStrategy)

    bot = object.__new__(mb.MultiAssetQuantBot)
    bot.icici_api = None
    bot.contexts = []
    bot.guard = SimpleNamespace(allocate_balance=lambda *a, **k: {})
    hl_api = SimpleNamespace(can_trade=False)

    ctx = mb.MultiAssetQuantBot._build_asset_context(bot, inst, hl_api, object(), None)

    assert ctx.execution_router.active_exchange == "delta"
    assert isinstance(ctx.data_manager.primary_dm, FakeDM)
    assert isinstance(ctx.data_manager.secondary_dm, FakeDM)
