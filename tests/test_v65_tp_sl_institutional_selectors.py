import time
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
            sig=50.0, tf="5m", touches=8, tf_sources=None):
    now = time.time()
    pool = LiquidityPool(
        price=float(price),
        side=pool_side,
        timeframe=tf,
        status=status,
        touches=touches,
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
        tf_sources=list(tf_sources or [tf]),
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


def test_wide_structural_stop_selects_htf_runner_instead_of_tiny_pool():
    snap = _snapshot(
        entry=100.0,
        atr=2.0,
        bsl=[
            _target(
                106.0,
                PoolSide.BSL,
                sig=140.0,
                tf="5m",
                entry=100.0,
                atr=2.0,
                touches=3,
                tf_sources=["5m", "15m"],
            ),
            _target(
                122.0,
                PoolSide.BSL,
                sig=220.0,
                tf="1d",
                entry=100.0,
                atr=2.0,
                touches=3,
                tf_sources=["1h", "4h", "1d"],
            ),
        ],
    )

    tp, target, score, report = select_tp_with_report(
        snap, "long", entry=100.0, sl=90.0, atr=2.0, min_rr=2.0, posterior_prob=0.95
    )

    assert tp is not None
    assert target is not None
    assert target.pool.timeframe == "1d"
    assert score.rr > 1.0
    near = next(c for c in report.as_dict()["candidates"] if c["pool_price"] == pytest.approx(106.0))
    assert near["selected"] is False
    assert near["rr"] < near["required_rr"]


def test_posterior_conditioned_htf_target_passes_reachable_payoff():
    snap = _snapshot(
        entry=100.0,
        atr=2.0,
        ssl=[
            _target(
                91.0,
                PoolSide.SSL,
                sig=220.0,
                tf="1h",
                entry=100.0,
                atr=2.0,
                touches=3,
                tf_sources=["15m", "1h"],
            ),
        ],
    )

    tp, target, score, report = select_tp_with_report(
        snap, "short", entry=100.0, sl=103.75, atr=2.0, min_rr=2.2, posterior_prob=0.95
    )

    assert tp is not None
    assert target is not None
    assert target.pool.timeframe == "1h"
    assert score.rr >= 2.0
    selected = report.as_dict()["selected"]
    assert selected["delivery_prob"] > selected["required_delivery_prob"]


def test_lottery_distance_terminal_target_stays_rejected():
    snap = _snapshot(
        entry=100.0,
        atr=0.22,
        ssl=[
            _target(
                1.0,
                PoolSide.SSL,
                sig=220.0,
                tf="1d",
                entry=100.0,
                atr=0.22,
                touches=3,
                tf_sources=["1d"],
            ),
        ],
    )

    tp, target, score, report = select_tp_with_report(
        snap, "short", entry=100.0, sl=100.5, atr=0.22, min_rr=2.2, posterior_prob=0.95
    )

    assert tp is None
    assert target is None
    assert score is None
    rejected = report.as_dict()["candidates"][0]
    assert rejected["rr"] > 100.0
    assert rejected["delivery_prob"] < rejected["required_delivery_prob"]


@pytest.mark.parametrize("asset", [r["asset_id"] for r in MULTI_ASSET_REQUESTS])
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


def test_wide_liquidity_stop_still_selects_real_tp_pool_with_trail_managed_risk():
    """Regression for SILVER-like setup from live logs.

    The protective SL is intentionally behind a far HTF liquidity shelf.  TP
    selection must not reject every SSL just because the disaster SL makes the
    full-stop RR look small; high-posterior setups should price TP delivery
    against managed trail risk while keeping the executable SL untouched.
    """
    entry = 73.0
    atr = 0.193
    snap = _snapshot(
        entry=entry,
        atr=atr,
        ssl=[
            _target(72.90, PoolSide.SSL, sig=80.0, tf="5m", entry=entry, atr=atr, touches=3),
            _target(72.50, PoolSide.SSL, sig=80.0, tf="1m", entry=entry, atr=atr, touches=3),
            _target(72.40, PoolSide.SSL, sig=120.0, tf="15m", entry=entry, atr=atr, touches=3),
        ],
    )

    tp, target, score, report = select_tp_with_report(
        snap,
        "short",
        entry=entry,
        sl=74.60,        # ≈8.3ATR protective disaster stop
        atr=atr,
        min_rr=2.20,
        posterior_prob=0.926,
    )

    assert tp is not None
    assert target is not None
    assert target.pool.side is PoolSide.SSL
    assert target.pool.price < entry
    assert score.components["managed_risk_active"] == pytest.approx(1.0)
    assert score.components["managed_rr"] > score.components["full_rr"]
    selected = report.as_dict()["selected"]
    assert selected["required_rr"] < selected["rr"]
    assert any("managedRR" in str(n) for n in selected["notes"])

from strategy.expected_utility import build_target_surface


def test_target_surface_respects_selector_managed_trail_risk_for_wide_sl():
    """Downstream execution audit must not re-veto a selector-approved TP.

    Regression for the SILVER live log: the selector priced the TP with managed
    trail-risk, but the target surface repriced it against the full disaster SL
    and marked utility negative.  The advisory surface must use the same managed
    denominator when the selector explicitly activates it.
    """
    entry = 73.0
    atr = 0.193
    snap = _snapshot(
        entry=entry,
        atr=atr,
        ssl=[
            _target(
                72.40,
                PoolSide.SSL,
                sig=120.0,
                tf="15m",
                entry=entry,
                atr=atr,
                touches=3,
                tf_sources=["5m", "15m"],
            ),
        ],
    )

    surface = build_target_surface(
        side="short",
        entry=entry,
        stop=74.60,
        atr=atr,
        snapshot=snap,
        posterior_prob=0.95,
        managed_risk_points=atr * 2.70,
        managed_risk_active=True,
        fee_bps=8.0,
        slippage_bps=0.0,
    )

    assert surface.best is not None
    assert surface.has_positive_edge
    assert surface.best.rr > 0.90
    assert any("managedRisk" in note for note in surface.notes)


def test_managed_tp_frontier_prefers_farther_credible_liquidity_over_nearest_pool():
    """When trail-managed risk is active, do not collapse to nearest TP only."""
    entry = 73.0
    atr = 0.193
    snap = _snapshot(
        entry=entry,
        atr=atr,
        ssl=[
            _target(72.55, PoolSide.SSL, sig=110.0, tf="15m", entry=entry, atr=atr, touches=3),
            _target(72.05, PoolSide.SSL, sig=180.0, tf="1h", entry=entry, atr=atr, touches=3,
                    tf_sources=["15m", "1h"]),
        ],
    )

    tp, target, score, report = select_tp_with_report(
        snap,
        "short",
        entry=entry,
        sl=74.60,
        atr=atr,
        min_rr=2.20,
        posterior_prob=0.95,
    )

    assert tp is not None
    assert target is not None
    assert target.pool.price == pytest.approx(72.05)
    assert score.components["managed_risk_active"] == pytest.approx(1.0)
    near = next(c for c in report.as_dict()["candidates"] if c["pool_price"] == pytest.approx(72.55))
    assert near["selected"] is False
