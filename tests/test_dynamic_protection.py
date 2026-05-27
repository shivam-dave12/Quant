from __future__ import annotations

import math

from strategy.domain import Direction
from strategy.dynamic_protection import DynamicProtectionPlanBuilder
import strategy.dynamic_protection as dp


def _relax_warmup(monkeypatch):
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_MIN_SIGNAL_OBSERVATIONS", 8, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_MIN_KYLE_OBSERVATIONS", 8, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_MIN_VPIN_BUCKETS", 3, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_VPIN_WINDOW_BUCKETS", 5, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", True, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_FOR_DELTA", True, raising=False)


def test_ar1_signal_decay_produces_finite_cost_crossing_horizon(monkeypatch):
    _relax_warmup(monkeypatch)
    engine = DynamicProtectionPlanBuilder("GOLD")
    for i in range(20):
        engine.observe(signal_bps=12.0 * (0.88 ** i), timestamp_s=100.0 + i * 10.0)
    estimate = engine.signal_decay(current_edge_bps=8.0, total_exit_cost_bps=2.5)
    assert estimate.ready is True
    assert estimate.phi is not None and 0.0 < estimate.phi < 1.0
    assert estimate.half_life_sec is not None and estimate.half_life_sec > 0
    assert estimate.optimal_hold_sec is not None and estimate.optimal_hold_sec > 0


def test_kyle_impact_uses_primary_signed_ofi_price_impact(monkeypatch):
    _relax_warmup(monkeypatch)
    engine = DynamicProtectionPlanBuilder("GOLD")
    rows = []
    px = 4500.0
    for i in range(20):
        signed_ofi = 1000.0 if i % 2 == 0 else -800.0
        px *= 1.0 + (signed_ofi * 0.000002) / 10000.0
        rows.append({"timestamp_s": 100 + i, "microprice": px, "signed_ofi_usd": signed_ofi})
    engine.observe(signal_bps=1.0, timestamp_s=100, research_state={"book_events": rows})
    impact = engine.kyle_impact(exit_notional=10000.0)
    assert impact.ready is True
    assert impact.lambda_bps_per_usd is not None and impact.lambda_bps_per_usd > 0
    assert impact.expected_exit_impact_bps is not None and impact.expected_exit_impact_bps > 0


def test_vpin_toxicity_widens_stop_for_one_sided_tape(monkeypatch):
    _relax_warmup(monkeypatch)
    engine = DynamicProtectionPlanBuilder("SILVER")
    trades = [{"timestamp_s": 100 + i, "signed_notional_usd": 1000.0} for i in range(40)]
    engine.observe(signal_bps=1.0, timestamp_s=100, research_state={"trade_events": trades})
    toxicity = engine.vpin()
    assert toxicity.ready is True
    assert toxicity.vpin is not None and toxicity.vpin > 0.90
    assert toxicity.stop_multiplier > 1.0


def test_dynamic_plan_uses_signal_edge_cost_and_liquidation_schedule(monkeypatch):
    _relax_warmup(monkeypatch)
    engine = DynamicProtectionPlanBuilder("GOLD")
    for i in range(18):
        engine.observe(signal_bps=10.0 * (0.92 ** i), timestamp_s=100.0 + i)
    books = []
    px = 4500.0
    for i in range(20):
        ofi = 1000.0 if i % 2 == 0 else -900.0
        px *= 1 + (ofi * 0.000001) / 10000.0
        books.append({"timestamp_s": 200 + i, "microprice": px, "signed_ofi_usd": ofi})
    trades = [{"timestamp_s": 200 + i, "signed_notional_usd": (1000.0 if i % 5 else -1000.0)} for i in range(40)]
    engine.observe(signal_bps=4.0, timestamp_s=300.0, research_state={"book_events": books, "trade_events": trades})
    plan = engine.build_plan(
        direction=Direction.LONG,
        entry_price=4500.0,
        volatility_price=12.5,
        gross_edge_bps=8.0,
        execution_cost_bps=2.5,
        protection_type="VENUE_NATIVE_BRACKET",
        asset_class="commodity",
        position_notional=10000.0,
        quantity=3.0,
    )
    assert plan.protection_feasible is True
    assert plan.stop_price < plan.entry_price < plan.target_price
    assert plan.diagnostics["edge_scalar_rr"] >= 1.15
    assert plan.diagnostics["signal_decay"]["ready"] is True
    assert plan.diagnostics["vpin"]["ready"] is True
    assert plan.diagnostics["almgren_chriss"]["ready"] is True


def test_dynamic_plan_uses_spread_tick_and_policy_floors_for_silver(monkeypatch):
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_SIGNAL_DECAY_READY", False, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_TOXICITY_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_FOR_DELTA", False, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_ASSET_MIN_STOP_BPS", {"SILVER": 45.0}, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_VENUE_ASSET_MIN_STOP_BPS", {"hyperliquid:SILVER": 60.0}, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_ASSET_MIN_TARGET_BPS", {"SILVER": 100.0}, raising=False)
    engine = DynamicProtectionPlanBuilder("SILVER")
    plan = engine.build_plan(
        direction=Direction.LONG,
        entry_price=30.0,
        volatility_price=0.01,
        gross_edge_bps=12.0,
        execution_cost_bps=2.0,
        protection_type="VENUE_NATIVE_BRACKET",
        asset_class="commodity",
        position_notional=5000.0,
        quantity=100.0,
        market_state={
            "asset_id": "SILVER",
            "venue": "hyperliquid",
            "spread_bps": 10.0,
            "price_tick": 0.01,
            "near_touch_depth_usd": 20000.0,
            "policy_min_rr": 2.20,
            "policy_max_rr": 5.50,
        },
    )
    stop_distance = plan.entry_price - plan.stop_price
    target_distance = plan.target_price - plan.entry_price
    assert stop_distance >= 30.0 * 60.0 / 10000.0 - 1e-9
    assert target_distance / stop_distance >= 2.20
    assert plan.diagnostics["market_geometry"]["spread_floor_distance"] >= 30.0 * 60.0 / 10000.0


def test_option_greek_exit_diagnostics_fires_on_delta_iv_and_theta(monkeypatch):
    monkeypatch.setattr(dp.config, "DYNAMIC_OPTION_EXIT_MIN_ABS_DELTA", 0.10, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_OPTION_EXIT_IV_COLLAPSE_ABS", 0.02, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_OPTION_CHEAP_VRP_THRESHOLD", -0.10, raising=False)
    monkeypatch.setattr(dp.config, "DYNAMIC_OPTION_CHEAP_VRP_THETA_LIMIT", 0.12, raising=False)
    result = DynamicProtectionPlanBuilder.option_exit_diagnostics(
        abs_delta=0.08,
        current_iv=0.10,
        entry_iv=0.13,
        theta_to_premium_per_day=0.13,
        dte=3.0,
        vrp=-0.14,
    )
    assert result.exit_required is True
    assert "option_delta_exposure_collapsed" in result.reasons
    assert "option_iv_collapse" in result.reasons
    assert "option_theta_carry_structurally_excessive" in result.reasons


def test_delta_plan_fails_closed_until_kyle_and_vpin_are_observed(monkeypatch):
    _relax_warmup(monkeypatch)
    monkeypatch.setattr(dp.config, "DYNAMIC_PROTECTION_REQUIRE_KYLE_READY_FOR_DELTA", True, raising=False)
    engine = DynamicProtectionPlanBuilder("GOLD")
    for i in range(12):
        engine.observe(signal_bps=8.0 * (0.92 ** i), timestamp_s=100.0 + i)
    plan = engine.build_plan(
        direction=Direction.LONG,
        entry_price=4500.0,
        volatility_price=10.0,
        gross_edge_bps=8.0,
        execution_cost_bps=2.5,
        protection_type="VENUE_NATIVE_BRACKET",
        asset_class="commodity",
        position_notional=10000.0,
        quantity=2.0,
    )
    assert plan.protection_feasible is False
    assert any("kyle_lambda_warmup" in reason for reason in plan.reasons)
