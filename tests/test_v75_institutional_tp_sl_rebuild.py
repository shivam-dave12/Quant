import time
import pytest

from core.instruments import (
    AssetClass,
    ExchangeInstrument,
    ExchangeName,
    TradableInstrument,
    instrument_scope,
)
from strategy.liquidity_map import (
    LiquidityMapSnapshot,
    LiquidityPool,
    PoolSide,
    PoolStatus,
    PoolTarget,
)
from strategy.liquidity_pool_selector import select_sl_with_report, select_tp_with_report


def _instrument(asset_id: str, asset_class: AssetClass) -> TradableInstrument:
    ei = ExchangeInstrument(
        exchange=ExchangeName.DELTA,
        symbol=f"{asset_id}USD",
        ws_symbol=f"{asset_id}USD",
        display_symbol=f"{asset_id}USD",
        asset_id=asset_id,
        asset_class=asset_class,
    )
    return TradableInstrument(
        asset_id=asset_id,
        display_name=asset_id,
        asset_class=asset_class,
        primary_exchange=ExchangeName.DELTA,
        by_exchange={ExchangeName.DELTA: ei},
    )


def _target(price: float, side: PoolSide, *, entry: float = 100.0, atr: float = 2.0,
            tf: str = "5m", touches: int = 8, ob: bool = True, fvg: bool = True,
            htf_count: int = 2, status: PoolStatus = PoolStatus.CONFIRMED,
            sig_boost: float = 0.0) -> PoolTarget:
    now = time.time()
    pool = LiquidityPool(
        price=float(price),
        side=side,
        timeframe=tf,
        status=status,
        touches=touches,
        created_at=now,
        last_touch=now,
        ob_aligned=ob,
        fvg_aligned=fvg,
        htf_count=htf_count,
    )
    distance_atr = abs(float(price) - float(entry)) / max(float(atr), 1e-9)
    return PoolTarget(
        pool=pool,
        distance_atr=distance_atr,
        direction="above" if float(price) > float(entry) else "below",
        significance=float(pool.significance + sig_boost),
        tf_sources=[tf],
    )


def _snapshot(*, bsl=None, ssl=None, nearest_bsl_atr=1.0, nearest_ssl_atr=1.0) -> LiquidityMapSnapshot:
    return LiquidityMapSnapshot(
        bsl_pools=list(bsl or []),
        ssl_pools=list(ssl or []),
        primary_target=None,
        recent_sweeps=[],
        swept_bsl_levels=[],
        swept_ssl_levels=[],
        nearest_bsl_atr=nearest_bsl_atr,
        nearest_ssl_atr=nearest_ssl_atr,
        timestamp=time.time(),
        market_profile={},
    )


def test_btc_full_tp_rejects_micro_pool_and_selects_external_liquidity():
    snap = _snapshot(
        bsl=[
            _target(101.20, PoolSide.BSL, entry=100.0, atr=2.0, tf="5m", sig_boost=50.0),
            _target(108.00, PoolSide.BSL, entry=100.0, atr=2.0, tf="4h", sig_boost=80.0, htf_count=4),
        ],
        ssl=[_target(96.0, PoolSide.SSL, entry=100.0, atr=2.0)],
    )

    with instrument_scope(_instrument("BTC", AssetClass.CRYPTO)):
        tp, target, score, report = select_tp_with_report(
            snap, "long", entry=100.0, sl=99.0, atr=2.0, min_rr=1.5, posterior_prob=0.70
        )

    assert tp is not None
    assert target.pool.price == pytest.approx(108.0)
    assert score.components["asset_id"] == "BTC"
    assert score.components["delivery_prob"] > 0.0
    assert any("too close for BTC full TP" in c["reason"] for c in report.as_dict()["candidates"])


def test_gold_terminal_liquidity_can_pass_when_quality_and_payoff_are_valid():
    snap = _snapshot(
        bsl=[_target(115.0, PoolSide.BSL, entry=100.0, atr=2.0, tf="1d", sig_boost=100.0, htf_count=5)],
        ssl=[_target(96.0, PoolSide.SSL, entry=100.0, atr=2.0)],
    )

    with instrument_scope(_instrument("GOLD", AssetClass.COMMODITY)):
        tp, target, score, report = select_tp_with_report(
            snap, "long", entry=100.0, sl=98.8, atr=2.0, min_rr=2.0, posterior_prob=0.65
        )

    assert tp is not None, report.summary
    assert target.pool.timeframe == "1d"
    assert score.components["asset_id"] == "GOLD"
    assert score.components["delivery_prob"] >= score.components["required_delivery_prob"]


def test_btc_sl_clearance_is_wider_than_equity_for_same_liquidity_cluster():
    snap = _snapshot(
        bsl=[_target(104.0, PoolSide.BSL, entry=100.0, atr=2.0, tf="4h", touches=9, sig_boost=80.0, htf_count=4)],
        ssl=[],
    )

    with instrument_scope(_instrument("BTC", AssetClass.CRYPTO)):
        btc_sl, _, btc_pick, _ = select_sl_with_report(
            snap, "short", entry=100.0, atr=2.0, invalidation_price=101.0,
            max_buffer_atr=2.0, min_risk=1.0,
        )
    with instrument_scope(_instrument("AMZN", AssetClass.EQUITY)):
        eq_sl, _, eq_pick, _ = select_sl_with_report(
            snap, "short", entry=100.0, atr=2.0, invalidation_price=101.0,
            max_buffer_atr=2.0, min_risk=1.0,
        )

    assert btc_pick.buffer_atr > eq_pick.buffer_atr
    assert btc_sl > eq_sl


def test_equity_selector_rejects_fake_moonshot_terminal_pool_from_log_pattern():
    """Regression for the bad v75 behaviour: SPY/GOOGL-like 80-1000ATR TP.

    These pools may remain useful as map context/runners, but must not be the
    single full-position TP because huge RR can make raw EV look positive.
    """
    snap = _snapshot(
        ssl=[_target(312.3, PoolSide.SSL, entry=400.8, atr=0.3315, tf="4h", sig_boost=120.0, htf_count=5)],
        bsl=[_target(401.5, PoolSide.BSL, entry=400.8, atr=0.3315, tf="15m", sig_boost=40.0)],
    )

    with instrument_scope(_instrument("GOOGL", AssetClass.EQUITY)):
        tp, target, score, report = select_tp_with_report(
            snap, "short", entry=400.8, sl=401.8, atr=0.3315,
            min_rr=1.5, posterior_prob=0.75,
        )

    assert tp is None
    assert target is None
    assert score is None
    assert "too far for GOOGL full-position TP" in report.summary


def test_btc_and_silver_reject_extreme_terminal_pool_as_full_tp():
    btc_snap = _snapshot(
        bsl=[_target(82772.8, PoolSide.BSL, entry=79819.0, atr=76.4, tf="4h", sig_boost=120.0, htf_count=5)],
        ssl=[_target(79470.5, PoolSide.SSL, entry=79819.0, atr=76.4, tf="1h", sig_boost=80.0)],
    )
    with instrument_scope(_instrument("BTC", AssetClass.CRYPTO)):
        btc_tp, _, _, btc_report = select_tp_with_report(
            btc_snap, "long", entry=79819.0, sl=79357.4, atr=76.4,
            min_rr=1.5, posterior_prob=0.70,
        )
    assert btc_tp is None
    assert "too far for BTC full-position TP" in btc_report.summary

    silver_snap = _snapshot(
        ssl=[_target(65.6, PoolSide.SSL, entry=72.58, atr=0.1715, tf="4h", sig_boost=120.0, htf_count=5)],
        bsl=[_target(74.4, PoolSide.BSL, entry=72.58, atr=0.1715, tf="4h", sig_boost=80.0)],
    )
    with instrument_scope(_instrument("SILVER", AssetClass.COMMODITY)):
        sil_tp, _, _, sil_report = select_tp_with_report(
            silver_snap, "short", entry=72.58, sl=74.6, atr=0.1715,
            min_rr=1.5, posterior_prob=0.70,
        )
    assert sil_tp is None
    assert "too far for SILVER full-position TP" in sil_report.summary


def test_gold_accepted_auction_can_use_reasonable_htf_bsl_from_log_pattern():
    """Regression for v76: accepted GOLD long was rejected by treating HTF BSL
    as first-sweep probability only, despite reasonable distance and payoff.
    """
    entry = 4712.2
    sl = 4690.6
    atr = (entry - sl) / 5.31
    snap = _snapshot(
        bsl=[
            _target(4754.2, PoolSide.BSL, entry=entry, atr=atr, tf="4h", sig_boost=120.0, htf_count=5),
            _target(4723.6, PoolSide.BSL, entry=entry, atr=atr, tf="1h", sig_boost=80.0, htf_count=3),
            _target(4727.7, PoolSide.BSL, entry=entry, atr=atr, tf="15m", sig_boost=80.0, htf_count=2),
        ],
        ssl=[],
        nearest_bsl_atr=0.1,
        nearest_ssl_atr=0.3,
    )

    with instrument_scope(_instrument("GOLD", AssetClass.COMMODITY)):
        tp, target, score, report = select_tp_with_report(
            snap, "long", entry=entry, sl=sl, atr=atr,
            min_rr=2.0, posterior_prob=0.88,
        )

    assert tp is not None, report.summary
    assert target.pool.price == pytest.approx(4754.2)
    assert score.components["delivery_prob"] >= score.components["required_delivery_prob"]
    assert score.rr >= 1.8
