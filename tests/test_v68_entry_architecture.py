import time
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
    assert qs._active_institutional_size_mult < 1.0


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
    engine._processed_sweeps[key] = time.time() - 222.0
    engine.mark_pre_order_rejected(sig, cooldown_sec=30.0)
    assert engine._processed_sweeps[key] > time.time()
