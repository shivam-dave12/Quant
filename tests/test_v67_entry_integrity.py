import time
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
