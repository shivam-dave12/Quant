import types
import pytest

from strategy.entry_engine import EntryEngine
from strategy.liquidity_map import LiquidityPool, LiquidityMapSnapshot, PoolSide, PoolStatus, PoolTarget


def _target(price, pool_side, *, entry=100.0, atr=2.0, sig=80.0, tf="15m"):
    pool = LiquidityPool(
        price=float(price),
        side=pool_side,
        timeframe=tf,
        status=PoolStatus.CONFIRMED,
        touches=4,
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


def _snapshot():
    return LiquidityMapSnapshot(
        bsl_pools=[_target(104.0, PoolSide.BSL)],
        ssl_pools=[_target(96.0, PoolSide.SSL)],
        primary_target=None,
        recent_sweeps=[],
        swept_bsl_levels=[],
        swept_ssl_levels=[],
        nearest_bsl_atr=2.0,
        nearest_ssl_atr=2.0,
        timestamp=0.0,
    )


def _engine():
    e = EntryEngine.__new__(EntryEngine)
    e._ict = None
    e._htf = None
    e.htf_engine = None
    e._current_quant_posterior = types.MethodType(lambda self: 0.90, e)
    return e


def test_candidate_sl_tp_frontier_rejects_far_stop_that_destroys_rr():
    engine = _engine()
    snap = _snapshot()

    efficient_frontier, efficient_tp = engine._tp_frontier_for_candidate_sl(
        snap, "long", 100.0, 2.0, 99.5, 1.5
    )
    far_frontier, far_tp = engine._tp_frontier_for_candidate_sl(
        snap, "long", 100.0, 2.0, 90.0, 1.5
    )

    assert efficient_tp is not None
    assert efficient_frontier > 0.0
    assert far_tp is None or far_frontier < efficient_frontier * 0.45


def test_pool_audit_format_reports_real_tp_ev_fields():
    engine = _engine()
    report = {
        "role": "TP",
        "side": "long",
        "summary": "sample",
        "candidates": [{
            "pool_side": "BSL",
            "pool_price": 104.0,
            "timeframe": "15m",
            "rr": 2.5,
            "delivery_prob": 0.62,
            "required_delivery_prob": 0.43,
            "expected_value_r": 0.57,
            "selection_ev": 1.23,
            "eligible": True,
            "reason": "eligible",
        }],
    }
    text = engine._format_pool_plan(report)
    assert "P=0.620/0.430" in text
    assert "EV_R=0.570" in text
    assert "frontier=1.230" in text
