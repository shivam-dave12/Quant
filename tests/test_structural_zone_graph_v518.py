"""V518 regression: one structural zone graph governs BTC, SILVER and NIFTY geometry."""
from __future__ import annotations

import os
import time
from types import SimpleNamespace

import pytest

os.environ.setdefault("DELTA_API_KEY", "test")
os.environ.setdefault("DELTA_SECRET_KEY", "test")
os.environ.setdefault("GROWW_API_KEY", "test")
os.environ.setdefault("GROWW_SECRET_KEY", "test")

from strategy.entry_engine import EntryEngine, EntryType, _FVG, _Thesis, _TrendContext
from strategy.liquidity_map import LiquidityMapSnapshot, LiquidityPool, PoolSide, PoolStatus, PoolTarget, SweepResult


def _snap(*, bsl=None, ssl=None, sweeps=None):
    return LiquidityMapSnapshot(
        bsl_pools=list(bsl or []), ssl_pools=list(ssl or []), primary_target=None,
        recent_sweeps=list(sweeps or []), swept_bsl_levels=[], swept_ssl_levels=[],
        nearest_bsl_atr=999.0, nearest_ssl_atr=999.0, timestamp=time.time(),
    )


def _target(price: float, side: PoolSide, tf: str, significance: float, *, sources=None, htf_count=1):
    pool = LiquidityPool(price, side, tf, status=PoolStatus.DETECTED, created_at=time.time(), htf_count=htf_count)
    direction = "long" if side is PoolSide.BSL else "short"
    return PoolTarget(pool, abs(price - 100.0), direction, significance, list(sources or [tf]))


def _long_thesis(*, sweep=None, fvg=None, anchor=99.0):
    ctx = _TrendContext(1, 0.8, 0.0, 0.0, 1.0, 0.72)
    return _Thesis(
        sweep_key=("long", 1), side="long", sweep=sweep, formed_at=time.time(),
        context_4h=ctx, context_15m=ctx, context_path="LIQUIDITY_TRANSFER_CONFIRMED",
        context_delivery_score=0.78, mss_level=100.0, displacement_atr=1.8,
        fvg=fvg or _FVG("long", 100.0, 101.0, 22, 1.8),
        entry_type=EntryType.LIQUIDITY_RAID_REVERSAL, invalidation_anchor=anchor,
        evidence_score=0.78, structural_origin="RAID_WICK",
        market_phase="LIQUIDITY_TRANSFER_CONFIRMED", auction_control_side="long",
        auction_control_score=0.72, execution_posture="AGGRESSIVE", auction_risk_scalar=1.0,
        market_state_thesis="controller=long",
    )


def test_btc_and_silver_are_on_the_same_zone_graph_authority_as_the_rebuilt_core():
    btc = EntryEngine(instrument=SimpleNamespace(asset_id="BTC", primary_exchange="delta"))
    silver = EntryEngine(instrument=SimpleNamespace(asset_id="SILVER", primary_exchange="delta"))
    assert btc._zone_graph_profile is True and silver._zone_graph_profile is True
    assert btc._desk_profile == "BTC_AUCTION_CONTROL_ZONE_GRAPH"
    assert silver._desk_profile == "SILVER_AUCTION_CONTROL_ZONE_GRAPH"


def test_entry_zone_graph_selects_funded_fvg_order_block_and_labels_micro_gap_noise():
    now = time.time()
    swept = LiquidityPool(99.0, PoolSide.SSL, "5m", status=PoolStatus.SWEPT, created_at=now - 20)
    sweep = SweepResult(swept, 19, 99.20, 0.8, 1.3, 0.90, "long", now - 10)
    thesis = _long_thesis(sweep=sweep, anchor=99.20)
    rows = [{"o": 99.7, "h": 99.9, "l": 99.5, "c": 99.8} for _ in range(35)]
    # Origin candle of thesis FVG: bearish OB overlapping the executable FVG.
    rows[20] = {"o": 101.20, "h": 101.25, "l": 99.85, "c": 100.20}
    rows[21] = {"o": 100.20, "h": 102.20, "l": 100.10, "c": 102.00}
    rows[22] = {"o": 101.20, "h": 102.10, "l": 101.00, "c": 101.60}
    # Tiny un-funded gap: visible but should be classified as noise.
    rows[28] = {"o": 101.80, "h": 102.00, "l": 101.70, "c": 101.95}
    rows[29] = {"o": 101.99, "h": 102.08, "l": 101.98, "c": 102.04}
    rows[30] = {"o": 102.05, "h": 102.18, "l": 102.04, "c": 102.10}
    engine = EntryEngine(instrument=SimpleNamespace(asset_id="BTC", primary_exchange="delta"))
    selected, plan = engine._rank_entry_zones(thesis, _snap(sweeps=[sweep]), 100.50, 1.0, rows, [])
    assert selected.low == pytest.approx(100.0) and selected.high == pytest.approx(101.0)
    assert plan["selected"]["order_block_overlap"] > 0.0
    assert any(row["classification"] == "NOISE" for row in plan["candidates"])
    assert engine.analysis_info["entry_zone_selection_model"] == "STRUCTURAL_ZONE_GRAPH_FVG_OB_RAID_ORIGIN"


def test_liquidity_protected_stop_sits_beyond_relevant_ssl_cluster_not_remote_noise():
    thesis = _long_thesis(anchor=99.0)
    nearby = _target(98.92, PoolSide.SSL, "15m", 4.0, sources=["15m", "1h"], htf_count=2)
    remote = _target(95.0, PoolSide.SSL, "4h", 8.0, sources=["4h"])
    engine = EntryEngine(instrument=SimpleNamespace(asset_id="SILVER", primary_exchange="delta"))
    engine._atr_pctile = 0.5
    plan = engine._liquidity_protected_stop(thesis, 100.50, _snap(ssl=[nearby, remote]), 1.0)
    assert plan is not None
    assert plan.price < nearby.pool.price  # stop is no longer a target at the nearer SSL pool
    assert plan.price > remote.pool.price  # distant liquidity is telemetry, not uncontrolled risk widening
    assert len(plan.selected_pools) == 1 and len(plan.noise_pools) == 1
    assert plan.model == "LIQUIDITY_PROTECTED_INVALIDATION_CLUSTER"


def test_tp_authority_prefers_multitimeframe_liquidity_concentration_over_isolated_noise():
    engine = EntryEngine(instrument=SimpleNamespace(asset_id="BTC", primary_exchange="delta"))
    engine.set_structural_delivery_policy(min_rr=1.0, max_rr_reference=3.2)
    engine._thesis = _long_thesis()
    concentrated = _target(104.0, PoolSide.BSL, "15m", 4.0, sources=["15m", "1h"], htf_count=2)
    companion = _target(104.18, PoolSide.BSL, "1h", 3.0, sources=["15m", "1h"], htf_count=2)
    isolated = _target(106.0, PoolSide.BSL, "15m", 0.25, sources=["15m"], htf_count=1)
    selected = engine._select_liquidity_target("long", 100.0, 98.0, _snap(bsl=[concentrated, companion, isolated]), 1.0)
    assert selected is not None
    chosen = selected[0]
    assert chosen.pool.price == pytest.approx(104.0) or chosen.pool.price == pytest.approx(104.18)
    candidates = engine.pool_plan_info["candidates"]
    isolated_row = next(row for row in candidates if row["pool_price"] == pytest.approx(106.0))
    assert isolated_row["classification"] == "NOISE"
    assert isolated_row["noise_reason"] == "ISOLATED_LOW_MASS_POOL"
    assert engine.analysis_info["target_structural_liquidity_mass"] > isolated_row["structural_liquidity_mass"]
    assert engine.analysis_info["target_selection_authority"] == "LIQUIDITY_CONCENTRATION_GRAPH_NET_R_RANK"


def test_tp_path_accounts_for_opposing_fvg_order_block_impedance_without_hard_block():
    engine = EntryEngine(instrument=SimpleNamespace(asset_id="SILVER", primary_exchange="delta"))
    engine.set_structural_delivery_policy(min_rr=1.0, max_rr_reference=4.0)
    engine._thesis = _long_thesis()
    target = _target(105.5, PoolSide.BSL, "1h", 5.0, sources=["15m", "1h"], htf_count=2)
    rows = [{"o": 104.0, "h": 104.2, "l": 103.8, "c": 104.0} for _ in range(12)]
    # Short displacement leaves bearish FVG between entry and TP, with bullish OB overlap.
    rows[7] = {"o": 103.0, "h": 103.8, "l": 102.8, "c": 103.6}
    rows[8] = {"o": 103.5, "h": 103.6, "l": 101.8, "c": 102.0}
    rows[9] = {"o": 102.5, "h": 102.7, "l": 101.9, "c": 102.1}
    selected = engine._select_liquidity_target("long", 100.0, 98.0, _snap(bsl=[target]), 1.0, candles_5m=rows, candles_15m=[])
    assert selected is not None
    info = engine.analysis_info
    assert info["target_path_imbalance_count"] >= 1
    assert info["target_path_imbalance_penalty"] < 1.0


def _trend(step: float, n: int):
    rows = []
    px = 90.0
    for _ in range(n):
        o, c = px, px + step
        rows.append({"o": o, "h": max(o, c) + 0.2, "l": min(o, c) - 0.2, "c": c})
        px = c
    return rows


def _continuation_bars():
    rows = [{"o": 99.20, "h": 99.70, "l": 99.00, "c": 99.45} for _ in range(34)]
    for i in range(8, 20):
        rows[i] = {"o": 99.30, "h": 100.00, "l": 99.10, "c": 99.60}
    rows[20] = {"o": 99.40, "h": 99.70, "l": 99.25, "c": 99.50}
    rows[21] = {"o": 99.40, "h": 100.00, "l": 99.30, "c": 99.80}
    rows[22] = {"o": 100.20, "h": 103.20, "l": 100.10, "c": 103.00}
    rows[23] = {"o": 101.30, "h": 103.40, "l": 101.00, "c": 103.10}
    for i in range(24, 33):
        rows[i] = {"o": 103.00, "h": 104.30, "l": 102.90, "c": 104.00}
    rows[33] = {"o": 100.60, "h": 100.80, "l": 100.30, "c": 100.50}
    return rows


@pytest.mark.parametrize("asset, expected_profile", [
    ("BTC", "BTC_AUCTION_CONTROL_ZONE_GRAPH"),
    ("SILVER", "SILVER_AUCTION_CONTROL_ZONE_GRAPH"),
])
def test_delta_desks_emit_entry_sl_tp_zone_authority_end_to_end(asset, expected_profile):
    now = time.time()
    final_pool = _target(110.0, PoolSide.BSL, "1h", 6.0, sources=["15m", "1h"], htf_count=2)
    stop_pool = _target(99.20, PoolSide.SSL, "15m", 3.0, sources=["15m"], htf_count=1)
    engine = EntryEngine(instrument=SimpleNamespace(asset_id=asset, primary_exchange="delta"))
    engine.set_structural_delivery_policy(min_rr=1.0, max_rr_reference=8.0)
    engine.update(
        _snap(bsl=[final_pool], ssl=[stop_pool]), 100.50, 1.0, now,
        candles_5m=_continuation_bars(), candles_15m=_trend(0.55, 33), candles_4h=_trend(1.40, 28),
    )
    signal = engine.get_signal()
    info = engine.analysis_info
    assert signal is not None
    assert info["execution_profile"] == expected_profile
    assert info["entry_zone_selection_model"] == "STRUCTURAL_ZONE_GRAPH_FVG_OB_RAID_ORIGIN"
    assert info["stop_selection_model"] == "LIQUIDITY_PROTECTED_INVALIDATION_CLUSTER"
    assert info["target_selection_authority"] == "LIQUIDITY_CONCENTRATION_GRAPH_NET_R_RANK"
