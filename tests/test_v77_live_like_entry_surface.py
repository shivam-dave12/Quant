import time
from types import SimpleNamespace

import pytest

from strategy.expected_utility import build_target_surface
from strategy.liquidity_map import (
    LiquidityMapSnapshot,
    LiquidityPool,
    PoolSide,
    PoolStatus,
    PoolTarget,
)


def test_expected_utility_surface_uses_pool_target_significance():
    now = time.time()
    pool = LiquidityPool(
        price=91.0,
        side=PoolSide.SSL,
        timeframe="1h",
        status=PoolStatus.CONFIRMED,
        touches=3,
        created_at=now,
        last_touch=now,
        ob_aligned=True,
        fvg_aligned=True,
        htf_count=2,
    )
    target = PoolTarget(
        pool=pool,
        distance_atr=4.5,
        direction="below",
        significance=220.0,
        tf_sources=["15m", "1h"],
    )
    snapshot = LiquidityMapSnapshot(
        bsl_pools=[],
        ssl_pools=[target],
        primary_target=None,
        recent_sweeps=[],
        swept_bsl_levels=[],
        swept_ssl_levels=[],
        nearest_bsl_atr=1.0,
        nearest_ssl_atr=1.0,
        timestamp=now,
    )

    surface = build_target_surface(
        side="short",
        entry=100.0,
        stop=103.75,
        atr=2.0,
        snapshot=snapshot,
        flow=SimpleNamespace(tick_flow=-0.6, cvd_trend=-0.4),
        ict=SimpleNamespace(
            structure_15m="bearish",
            structure_4h="bearish",
            dealing_range_pd=0.68,
        ),
        fee_bps=8.0,
        slippage_bps=0.0,
        posterior_prob=0.95,
    )

    assert surface.best is not None
    assert surface.best.pool_significance > 100.0
    assert surface.best.expected_value_r > 0.0
    assert surface.has_positive_edge is True
    assert surface.best.rr == pytest.approx(2.2266666667)
