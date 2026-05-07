import time
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
