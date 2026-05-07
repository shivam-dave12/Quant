"""Runtime-log regressions for entry-flow integrity.

Keeps the same strategy criteria, but prevents duplicate application of the
same post-sweep/SL-liquidity surface that can delay entries or over-widen stops.
"""

import inspect

from strategy.direction_engine import DirectionEngine
from strategy.entry_engine import EntryEngine


def test_direction_engine_merges_duplicate_same_cluster_sweep_without_restart():
    engine = DirectionEngine()
    engine.on_sweep(
        swept_pool_price=404.2,
        pool_type="SSL",
        price=404.75,
        atr=1.0,
        now=1_000.0,
        quality=0.85,
    )
    state = engine._ps_state
    assert state is not None
    state.rev_evidence = 72.0
    state.cont_evidence = 40.9

    engine.on_sweep(
        swept_pool_price=404.2,
        pool_type="SSL",
        price=404.50,
        atr=1.0,
        now=1_031.0,
        quality=1.00,
    )

    assert engine._ps_state is state
    assert engine._ps_state.entered_at == 1_000.0
    assert engine._ps_state.rev_evidence == 72.0
    assert engine._ps_state.cont_evidence == 40.9
    assert engine._ps_state_quality == 1.00


def test_refined_entry_path_uses_single_liquidity_sl_pass():
    src = inspect.getsource(EntryEngine._evaluate_pending_refined_entry)
    before_envelope = src.split("sl, sl_reason = self._apply_institutional_sl_envelope", 1)[0]
    assert "_push_sl_behind_pools" not in before_envelope


def test_entry_engine_processed_sweep_key_ignores_timestamp_churn_same_level():
    from types import SimpleNamespace
    import time
    from strategy.liquidity_map import SweepResult
    from strategy.liquidity_map import PoolSide

    engine = EntryEngine()
    side = PoolSide.SSL
    pool_1 = SimpleNamespace(price=197.2, side=side, timeframe="1d")
    pool_2 = SimpleNamespace(price=197.2, side=side, timeframe="1d")
    first = SweepResult(
        pool=pool_1,
        sweep_candle_idx=0,
        wick_extreme=196.2,
        rejection_pct=0.8,
        volume_ratio=1.0,
        quality=0.92,
        direction="long",
        detected_at=1_000.0,
    )
    refreshed = SweepResult(
        pool=pool_2,
        sweep_candle_idx=0,
        wick_extreme=196.2,
        rejection_pct=0.8,
        volume_ratio=1.0,
        quality=0.92,
        direction="long",
        detected_at=1_043.0,
    )

    assert engine._sweep_key(first) == engine._sweep_key(refreshed)
    engine._processed_sweeps[engine._sweep_key(first)] = time.time() + 120.0
    assert engine._is_processed(refreshed, time.time())


def test_entry_engine_processed_sweep_key_keeps_timeframes_distinct():
    from types import SimpleNamespace
    from strategy.liquidity_map import SweepResult
    from strategy.liquidity_map import PoolSide

    def sweep(tf: str):
        return SweepResult(
            pool=SimpleNamespace(price=404.2, side=PoolSide.SSL, timeframe=tf),
            sweep_candle_idx=0,
            wick_extreme=403.5,
            rejection_pct=0.7,
            volume_ratio=1.0,
            quality=0.75,
            direction="long",
            detected_at=1_000.0,
        )

    engine = EntryEngine()
    assert engine._sweep_key(sweep("15m")) != engine._sweep_key(sweep("1d"))


def test_tp_audit_reports_unreachable_micro_target_as_payoff_geometry_not_probability():
    from strategy.liquidity_map import LiquidityMapSnapshot, LiquidityPool, PoolSide, PoolStatus, PoolTarget
    from strategy.liquidity_pool_selector import select_tp_with_report
    import time

    now = time.time()
    entry = 100.0
    atr = 2.0
    pool = LiquidityPool(
        price=100.55,
        side=PoolSide.BSL,
        timeframe="15m",
        status=PoolStatus.CONFIRMED,
        touches=2,
        created_at=now,
        last_touch=now,
        ob_aligned=True,
        fvg_aligned=True,
        htf_count=2,
    )
    target = PoolTarget(
        pool=pool,
        distance_atr=abs(pool.price - entry) / atr,
        direction="above",
        significance=100.0,
        tf_sources=["15m", "1h"],
    )
    snap = LiquidityMapSnapshot(
        bsl_pools=[target], ssl_pools=[], primary_target=None, recent_sweeps=[],
        swept_bsl_levels=[], swept_ssl_levels=[], nearest_bsl_atr=0.3, nearest_ssl_atr=1.0,
        timestamp=now,
    )

    tp, target, score, report = select_tp_with_report(
        snap, "long", entry=entry, sl=98.0, atr=atr, min_rr=2.2, posterior_prob=0.95
    )

    assert tp is None
    assert report.candidates
    assert "durable payoff floor" in report.candidates[0].reason
    assert "probability not evaluated" in report.candidates[0].reason


def test_tp_probability_criterion_is_reachable_for_reasonable_rr_liquidity_target():
    from strategy.liquidity_map import LiquidityMapSnapshot, LiquidityPool, PoolSide, PoolStatus, PoolTarget
    from strategy.liquidity_pool_selector import select_tp_with_report
    import time

    now = time.time()
    entry = 100.0
    atr = 2.0
    pool = LiquidityPool(
        price=104.8,
        side=PoolSide.BSL,
        timeframe="1h",
        status=PoolStatus.CONFIRMED,
        touches=2,
        created_at=now,
        last_touch=now,
        ob_aligned=True,
        fvg_aligned=True,
        htf_count=3,
    )
    target = PoolTarget(
        pool=pool,
        distance_atr=abs(pool.price - entry) / atr,
        direction="above",
        significance=120.0,
        tf_sources=["15m", "1h", "4h"],
    )
    snap = LiquidityMapSnapshot(
        bsl_pools=[target], ssl_pools=[], primary_target=None, recent_sweeps=[],
        swept_bsl_levels=[], swept_ssl_levels=[], nearest_bsl_atr=2.4, nearest_ssl_atr=1.0,
        timestamp=now,
    )

    tp, selected_target, score, report = select_tp_with_report(
        snap, "long", entry=entry, sl=99.0, atr=atr, min_rr=2.2, posterior_prob=0.95
    )

    assert tp is not None
    assert selected_target is target
    assert report.selected is not None
    assert report.selected.delivery_prob >= report.selected.required_delivery_prob
    assert report.selected.rr >= report.selected.required_rr
